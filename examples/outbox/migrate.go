package outbox

import (
	"context"

	"github.com/21strive/redifu/outbox"
	"github.com/jmoiron/sqlx"
)

// createPosts is the example's table.
const createPosts = `CREATE TABLE IF NOT EXISTS posts (
    uuid        uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    randid      text        NOT NULL UNIQUE,
    author_uuid text        NOT NULL,
    title       text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
)`

// Migrate is what the application's migrations do for the cache, in order:
//
//  1. install the outbox: outbox.Schema, the same for every application and safe to run again;
//  2. watch each cached table, naming its key column and its owner: here every post belongs to its
//     author, so the queue of a post is the queue of its author. A table without an owner of its
//     own names an empty spec (every row is global) or its parent's
//     (parent:<table>:<fk>:<owner column>).
//
// redifu_watch may run again at any time; a table watched later turns its cache on without a
// restart (the store re-reads the watched tables every minute).
func Migrate(ctx context.Context, db sqlx.ExecerContext) error {
	for _, statement := range []string{
		outbox.Schema,
		createPosts,
		`SELECT redifu_watch('posts', 'randid', 'author_uuid')`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
