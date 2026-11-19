package outbox

import (
	"context"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
)

// The write path writes to the database, and only there: no Base.Set, no AddItem after it. The
// row trigger queues every change in the transaction of the write — so a rolled-back write queues
// nothing — and the next read of the author's list, or the drain loop, applies it. A worker, a
// script or psql writing the same table reaches the cache the same way.

// CreatePost writes a new post and returns its randid.
func CreatePost(ctx context.Context, db sqlx.ExecerContext, author, title string) (string, error) {
	randID := redifu.RandId()
	_, err := db.ExecContext(ctx, `INSERT INTO posts (randid, author_uuid, title) VALUES ($1, $2, $3)`, randID, author, title)
	return randID, err
}

// RenamePost changes a post's title.
func RenamePost(ctx context.Context, db sqlx.ExecerContext, randID, title string) error {
	_, err := db.ExecContext(ctx, `UPDATE posts SET title = $2, updated_at = now() WHERE randid = $1`, randID, title)
	return err
}

// MovePost gives a post to another author: the queue has it for both, and each author's list
// follows.
func MovePost(ctx context.Context, db sqlx.ExecerContext, randID, author string) error {
	_, err := db.ExecContext(ctx, `UPDATE posts SET author_uuid = $2, updated_at = now() WHERE randid = $1`, randID, author)
	return err
}

// DeletePost deletes a post.
func DeletePost(ctx context.Context, db sqlx.ExecerContext, randID string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM posts WHERE randid = $1`, randID)
	return err
}
