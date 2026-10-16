package feed

import (
	"context"
	"errors"

	"github.com/21strive/redifu"
)

// FeedPage is what the handler hands back to the transport layer.
type FeedPage struct {
	Posts   []*Post
	Cursor  redifu.Cursor // Cursor.String() goes to the client; ParseCursor reads it back
	HasMore bool
}

// maxTopUps caps the database reads one request spends extending the feed. One is
// enough unless the index lost its end while the client was deep in the feed.
const maxTopUps = 3

// GetPostFeed is the read path: the page that follows cursor, the zero Cursor for the
// first page.
//
// Nothing in here resolves a relation by hand. By the time Fetch returns, every post
// has post.Author filled in, and every author has author.Organisation filled in — two
// levels, one batched read per level, each distinct key read once for the whole page.
func (s *Store) GetPostFeed(ctx context.Context, userRandId string, cursor redifu.Cursor) (*FeedPage, error) {
	perPage := s.PostFeed.GetItemPerPage()

	// The page after the cursor, read by position: a post deleted, or moved out of this
	// feed, since the client read it does not send the client back to page one.
	output := s.PostFeed.FetchFrom(cursor).WithParams(userRandId).Exec(ctx)

	// The index holds a prefix of the table. A page that runs past its end is topped up
	// from where the index really ends — its Tail — never from the client's cursor, which
	// may lie beyond it after a purge or an expiry. A cold index has no tail, so the
	// first top-up loads the head.
	for topUps := 0; output.Error() == nil && !output.HasMore(); topUps++ {
		end, err := s.PostFeed.ReachedEnd(ctx, userRandId)
		if err != nil {
			return nil, err
		}
		if end {
			break
		}
		if topUps == maxTopUps {
			// Still short of the client's position. HasMore stays true, so the client
			// asks again and the next request carries on from here.
			return &FeedPage{Posts: output.Items(), Cursor: output.NextCursor(), HasMore: true}, nil
		}

		tail, _, err := s.PostFeed.Tail(ctx, userRandId)
		if err != nil {
			return nil, err
		}
		// One row more than the page still needs, so HasMore is read from the index.
		if err := s.SeedPostFeed(ctx, userRandId, tail, perPage+1-int64(len(output.Items()))); err != nil {
			return nil, err
		}
		output = s.PostFeed.FetchFrom(cursor).WithParams(userRandId).Exec(ctx)
	}

	if output.Error() != nil {
		return nil, output.Error()
	}

	// Drive "load more" from HasMore, not from len(Posts). HasMore is read from the
	// index before any post is loaded, so posts that have expired out of Base in the
	// middle of a feed cannot be mistaken for the end of it. output.Dangling() counts
	// those; a steadily non-zero value means itemTTL is too short next to indexTTL.
	return &FeedPage{
		Posts:   output.Items(),
		Cursor:  output.NextCursor(),
		HasMore: output.HasMore(),
	}, nil
}

// GetPost is the single-item read. It resolves the same two levels as the feed does,
// because the relations live on PostBase rather than on one index.
func (s *Store) GetPost(ctx context.Context, randId string) (*Post, error) {
	post, err := s.PostBase.Get(ctx, randId)
	if errors.Is(err, redifu.ErrNotFound) {
		// Cold key. Load it from the database and write it back; the relations
		// resolve on the next read without any extra work here.
		return s.loadPostFromDB(ctx, randId)
	}
	if err != nil {
		return nil, err
	}

	// A relation whose key has expired comes back nil while the rest of the fetch
	// succeeds. Guard for it rather than dereferencing blind.
	if post.Author == nil {
		// e.g. fall back to the database, or render without the author block
		_ = post.AuthorRandId
	}

	return post, nil
}

func (s *Store) loadPostFromDB(ctx context.Context, randId string) (*Post, error) {
	post := newPost()
	row := s.db.QueryRowContext(ctx, `
		SELECT randid, title, body, published, author_randid
		FROM posts WHERE randid = $1`, randId)

	if err := row.Scan(&post.RandId, &post.Title, &post.Body, &post.Published, &post.AuthorRandId); err != nil {
		return nil, err
	}

	if err := s.PostBase.Set(ctx, post); err != nil {
		return nil, err
	}

	// Read it back so the relations are resolved on the way out.
	return s.PostBase.Get(ctx, randId)
}
