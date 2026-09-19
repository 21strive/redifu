package feed

import (
	"context"
	"errors"

	"github.com/21strive/redifu"
)

// FeedPage is what the handler hands back to the transport layer.
type FeedPage struct {
	Posts   []*Post
	Cursor  string // pass back as lastRandIds on the next call
	HasMore bool
	Reset   bool // the client's cursor was stale; it should start over
}

// GetPostFeed is the read path.
//
// Nothing in here resolves a relation by hand. By the time Fetch returns, every post
// has post.Author filled in, and every author has author.Organisation filled in — two
// levels, one batched read per level, each distinct key read once for the whole page.
func (s *Store) GetPostFeed(ctx context.Context, userRandId string, lastRandIds []string) (*FeedPage, error) {
	needsSeed, err := s.PostFeed.RequiresSeeding(ctx, int64(len(lastRandIds)), userRandId)
	if err != nil {
		return nil, err
	}
	if needsSeed {
		if err := s.SeedPostFeed(ctx, userRandId, 0, ""); err != nil {
			return nil, err
		}
	}

	output := s.PostFeed.Fetch(lastRandIds).WithParams(userRandId).Exec(ctx)

	// None of the ids the client sent are in the index any more — it expired
	// mid-scroll, was purged, or those posts were removed. redifu refuses to quietly
	// serve page one here, because the client would receive items it already has with
	// no way to tell.
	if errors.Is(output.Error(), redifu.ErrResetPagination) {
		if err := s.SeedPostFeed(ctx, userRandId, 0, ""); err != nil {
			return nil, err
		}
		output = s.PostFeed.Fetch(nil).WithParams(userRandId).Exec(ctx)
		if output.Error() != nil {
			return nil, output.Error()
		}
		return &FeedPage{
			Posts:   output.Items(),
			Cursor:  output.ValidLastId(),
			HasMore: output.HasMore(),
			Reset:   true,
		}, nil
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
		Cursor:  output.ValidLastId(),
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
