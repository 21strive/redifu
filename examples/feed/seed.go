package feed

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/21strive/redifu"
)

// seedQuery selects the post and, in the same pass, the two entities behind it.
//
// Joining that far is a choice: it warms account:* and organisation:* so the feed's
// relations resolve from Redis on the very first fetch. Join only as deep as the
// relations you actually need filled — anything you do not warm comes back nil until
// something else writes that key.
const seedQuery = `
	SELECT p.randid, p.title, p.body, p.published, p.author_randid,
	       a.randid, a.name, a.handle, a.organisation_randid,
	       o.randid, o.name
	FROM posts p
	JOIN accounts a      ON a.randid = p.author_randid
	JOIN organisations o ON o.randid = a.organisation_randid
	WHERE p.feed_user_randid = $1`

// feedKey is what the index sorts by, as SQL: Published, which redifu scores in Unix
// milliseconds (SetSortingReference("Published") in store.go), then the randid in byte
// order, which is how Redis orders members that share a score. The ORDER BY and the
// keyset both use it; comparing the raw column, or the randid under the database's
// default collation, would skip or repeat rows at page boundaries.
const feedKey = `date_trunc('milliseconds', p.published), p.randid COLLATE "C"`

// scanFeedRow reads one joined row into three separate entities.
//
// Note what it does NOT do: it never touches post.Author or account.Organisation. The
// scanner's job is the randIds; assigning the entity is the Relation's job, on fetch.
// Assigning it here would be assigning a snapshot that stops updating.
func scanFeedRow(rows *sql.Rows) (*Post, *Account, *Organisation, error) {
	post := newPost()
	account := newAccount()
	organisation := newOrganisation()

	if err := rows.Scan(
		&post.RandId, &post.Title, &post.Body, &post.Published, &post.AuthorRandId,
		&account.RandId, &account.Name, &account.Handle, &account.OrganisationRandId,
		&organisation.RandId, &organisation.Name,
	); err != nil {
		return nil, nil, nil, err
	}

	return post, account, organisation, nil
}

// SeedPostFeed loads up to limit posts that follow `after` in the feed's order — the
// newest posts for the zero Cursor — and records whether they reach the end of the table.
//
// GetPostFeed calls it with the feed's Tail, so the index only ever grows from where it
// really ends: it always holds a prefix of the table, and a walk never skips a row.
func (s *Store) SeedPostFeed(ctx context.Context, userRandId string, after redifu.Cursor, limit int64) error {
	query := seedQuery
	args := []interface{}{userRandId}

	if !after.IsZero() {
		// Strictly after the anchor, in exactly the index's order. The anchor's score is
		// the one the index stores, so it is turned back into a timestamp rather than
		// read from the database.
		query += ` AND (` + feedKey + `) < ($2, $3)`
		args = append(args, time.UnixMilli(int64(after.Score)).UTC(), after.Member)
	}

	query += fmt.Sprintf(` ORDER BY date_trunc('milliseconds', p.published) DESC, p.randid COLLATE "C" DESC LIMIT %d`, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// One MULTI/EXEC: Redis applies the seed whole, so a reader never sees half of it (and, once
	// the feed has a generation, no other generation's seed interleaves with it).
	pipe := s.redis.TxPipeline()

	// The same author appears on many rows. Deduplicating is not needed for
	// correctness — same key, same value — but it keeps the pipeline small.
	seenAccount := map[string]bool{}
	seenOrganisation := map[string]bool{}

	var posts []*Post

	for rows.Next() {
		post, account, organisation, errScan := scanFeedRow(rows)
		if errScan != nil {
			return errScan
		}

		// The post is fully selected by this query, so write it outright.
		if err := s.PostBase.WithPipeline(pipe).Set(ctx, post); err != nil {
			return err
		}
		posts = append(posts, post)

		// The joined entities are NOT fully selected — this query never asked for
		// a.bio or o.plan. SetIfAbsent warms a cold key and refreshes a warm one's
		// TTL, but never replaces a complete Account with the four columns this
		// particular join happened to need. Use Set here only if you select every
		// column of the joined table.
		if !seenAccount[account.RandId] {
			seenAccount[account.RandId] = true
			if err := s.AccountBase.WithPipeline(pipe).SetIfAbsent(ctx, account); err != nil {
				return err
			}
		}
		if !seenOrganisation[organisation.RandId] {
			seenOrganisation[organisation.RandId] = true
			if err := s.OrganisationBase.WithPipeline(pipe).SetIfAbsent(ctx, organisation); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	count := int64(len(posts))

	if !after.IsZero() {
		// A top-up. Extend appends the rows only while the index still ends at `after` —
		// if it was purged or seeded again from its head since Tail was read, it writes
		// nothing and the handler reads again — and marks the end of the table when the
		// rows ran out. It renews the index TTL with that marker.
		if _, err := s.PostFeed.Extend(ctx, pipe, after, posts, count < limit, userRandId); err != nil {
			return err
		}
		_, err = pipe.Exec(ctx)
		return err
	}

	// A head seed starts the index over. A :lastpage marker can outlive the index it
	// described (an evicted index leaves it), and left in place it would end the feed at
	// this head.
	if err := s.PostFeed.UnmarkLastPage(ctx, pipe, userRandId); err != nil {
		return err
	}
	for _, post := range posts {
		if err := s.PostFeed.IngestItem(ctx, pipe, post, true, userRandId); err != nil {
			return err
		}
	}

	// State markers, so the next request knows whether there is more to load.
	switch {
	case count == 0:
		// Nothing in the table for this feed.
		if err := s.PostFeed.MarkEmpty(ctx, pipe, userRandId); err != nil {
			return err
		}
	case count < limit:
		// The table ran out before the limit: the index now reaches its end.
		if err := s.PostFeed.MarkLastPage(ctx, pipe, userRandId); err != nil {
			return err
		}
	}

	if err := s.PostFeed.SetExpiration(ctx, pipe, userRandId); err != nil {
		return err
	}

	// One round-trip for the whole page. Nothing above executed the pipeline —
	// IngestItem and the marker calls only enqueue.
	_, err = pipe.Exec(ctx)
	return err
}
