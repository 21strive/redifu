package outboxfeed

import (
	"context"

	"github.com/21strive/redifu/outbox"
	"github.com/jmoiron/sqlx"
)

// The example's tables. A relation is a randid column, as in examples/feed.
const (
	createOrganisations = `CREATE TABLE IF NOT EXISTS organisations (
    uuid       uuid        PRIMARY KEY,
    randid     text        NOT NULL UNIQUE,
    name       text        NOT NULL,
    plan       text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
)`
	createAccounts = `CREATE TABLE IF NOT EXISTS accounts (
    uuid                uuid        PRIMARY KEY,
    randid              text        NOT NULL UNIQUE,
    organisation_randid text        NOT NULL REFERENCES organisations (randid),
    name                text        NOT NULL,
    handle              text        NOT NULL,
    bio                 text        NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
)`
	createPosts = `CREATE TABLE IF NOT EXISTS posts (
    uuid          uuid        PRIMARY KEY,
    randid        text        NOT NULL UNIQUE,
    author_randid text        NOT NULL REFERENCES accounts (randid),
    title         text        NOT NULL,
    body          text        NOT NULL DEFAULT '',
    published     timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
)`
)

// Migrate is what the application's migrations do for the cache: install the outbox (outbox.Schema,
// the same for every application and safe to run again), then watch every cached table — its key
// column, and the owner whose queue and lock a change of a row joins:
//
//   - a post is its author's: the author's feed is the view it changes;
//   - an account is its own owner, the same owner as its feed, so a read of the feed applies a
//     change of its author together with the changes of its posts;
//   - an organisation is shared by every account in it, so its owner is the empty string: global
//     data. A read that resolves one applies the global queue as well (read.go).
//
// redifu_watch may run again at any time.
func Migrate(ctx context.Context, db sqlx.ExecerContext) error {
	for _, statement := range []string{
		outbox.Schema,
		createOrganisations,
		createAccounts,
		createPosts,
		`SELECT redifu_watch('organisations', 'randid', '')`,
		`SELECT redifu_watch('accounts', 'randid', 'randid')`,
		`SELECT redifu_watch('posts', 'randid', 'author_randid')`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
