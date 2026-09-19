package feed

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// seedQuery selects the post and, in the same pass, the two entities behind it.
//
// Joining that far is a choice: it warms account:* and organisation:* so the feed's
// relations resolve from Redis on the very first fetch. Join only as deep as the
// relations you actually need filled — anything you do not warm comes back nil until
// something else writes that key.
//
// ORDER BY published matches SetSortingReference("Published"). If the two disagree,
// the sorted set is ordered by one field and paginated by another.
const seedQuery = `
	SELECT p.randid, p.title, p.body, p.published, p.author_randid,
	       a.randid, a.name, a.handle, a.organisation_randid,
	       o.randid, o.name
	FROM posts p
	JOIN accounts a      ON a.randid = p.author_randid
	JOIN organisations o ON o.randid = a.organisation_randid
	WHERE p.feed_user_randid = $1`

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

// SeedPostFeed fills one user's feed from the database.
//
// subtraction is how many items Redis already holds for the page being stitched; it
// comes off the SQL LIMIT so the page reaches exactly itemPerPage. Pass 0 on a
// first-page seed.
func (s *Store) SeedPostFeed(ctx context.Context, userRandId string, subtraction int64, lastRandId string) error {
	query := seedQuery
	args := []interface{}{userRandId}

	if lastRandId != "" {
		// Read the cursor's score from the same column the index is sorted by.
		var cursorPublished time.Time
		row := s.db.QueryRowContext(ctx, `SELECT published FROM posts WHERE randid = $1`, lastRandId)
		if err := row.Scan(&cursorPublished); err != nil {
			return err
		}
		query += ` AND p.published < $2`
		args = append(args, cursorPublished)
	}

	query += fmt.Sprintf(" ORDER BY p.published DESC LIMIT %d", s.PostFeed.GetItemPerPage()-subtraction)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	pipe := s.redis.Pipeline()

	// The same author appears on many rows. Deduplicating is not needed for
	// correctness — same key, same value — but it keeps the pipeline small.
	seenAccount := map[string]bool{}
	seenOrganisation := map[string]bool{}

	var count int64

	for rows.Next() {
		post, account, organisation, errScan := scanFeedRow(rows)
		if errScan != nil {
			return errScan
		}

		// The post is fully selected by this query, so write it outright.
		if err := s.PostBase.WithPipeline(pipe).Set(ctx, post); err != nil {
			return err
		}
		if err := s.PostFeed.IngestItem(ctx, pipe, post, true, userRandId); err != nil {
			return err
		}
		count++

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

	// State markers, so the next request knows whether this feed needs seeding again.
	isFirstPage := lastRandId == ""
	perPage := s.PostFeed.GetItemPerPage()

	switch {
	case isFirstPage && count == 0:
		if err := s.PostFeed.MarkEmpty(ctx, pipe, userRandId); err != nil {
			return err
		}
	case isFirstPage && count < perPage:
		if err := s.PostFeed.MarkFirstPage(ctx, pipe, userRandId); err != nil {
			return err
		}
	case !isFirstPage && subtraction+count < perPage:
		if err := s.PostFeed.MarkLastPage(ctx, pipe, userRandId); err != nil {
			return err
		}
	}

	if isFirstPage {
		if err := s.PostFeed.SetExpiration(ctx, pipe, userRandId); err != nil {
			return err
		}
	}

	// One round-trip for the whole page. Nothing above executed the pipeline —
	// IngestItem and the marker calls only enqueue.
	_, err = pipe.Exec(ctx)
	return err
}
