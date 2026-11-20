// Package outbox is a worked example of the outbox package: a cache of posts, one list per author,
// that follows every write to the posts table — whoever makes it — without the write path ever
// touching Redis.
//
// The four steps of an adoption:
//
//  1. migrate.go installs the outbox's schema (outbox.Schema) with the application's migrations;
//  2. migrate.go then watches the table: SELECT redifu_watch('posts', 'randid', 'author_uuid');
//  3. cache.go builds the Base and the List on the store, and the handler that applies the
//     queued changes of posts;
//  4. cache.go runs the store: one per process, its drain loop in the background, and reads that
//     fall back to SQL whenever the cache cannot answer.
//
// write.go is the write path: SQL and nothing else.
package outbox

import (
	"context"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
)

// Post is one row of posts. Its randid is the key column the table is watched by, so it is what a
// queued change names and what every index stores.
type Post struct {
	*redifu.Record
	AuthorUUID string `json:"authorUuid" db:"author_uuid"`
	Title      string `json:"title" db:"title"`
	// Score orders the post in an index: when it was created, in milliseconds. The loader sets it
	// before every index write (outbox.Scored); Redis keeps it in the index, not in the value.
	Score int64 `json:"-" db:"-"`
}

// SetScore sets what the next index write orders the post by.
func (p *Post) SetScore(score int64) { p.Score = score }

// postColumns is what a post is loaded with, everywhere: the seeds, the handler and the reads
// that go to SQL.
const postColumns = `uuid, randid, author_uuid, title, created_at, updated_at`

// byAuthorOrder is the order of an author's list, in SQL as the index keeps it: the score (the
// creation in milliseconds) and then the member byte by byte, both descending.
const byAuthorOrder = `ORDER BY date_trunc('milliseconds', created_at) DESC, randid COLLATE "C" DESC`

// loadPosts runs one SELECT of posts through q — the primary, or the transaction of a seed or a
// drain — and scores what it read.
func loadPosts(ctx context.Context, q sqlx.QueryerContext, where string, args ...any) ([]*Post, error) {
	var posts []*Post
	if err := sqlx.SelectContext(ctx, q, &posts, `SELECT `+postColumns+` FROM posts `+where, args...); err != nil {
		return nil, err
	}
	for _, post := range posts {
		post.SetScore(post.CreatedAt.UnixMilli())
	}
	return posts, nil
}
