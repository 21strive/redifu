package outboxfeed

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// The write path writes to the database, and only there: no Base.Set, no AddItem, no Purge after
// it. The row triggers queue every change in the transaction of the write — a rolled-back write
// queues nothing — and the next read of an affected feed, or the drain loop, applies it. A worker, a
// script or psql writing these tables reaches the cache the same way.
//
// Set this against examples/feed/write.go, where every write updates Redis itself next to the
// database write its caller makes, and a rename reads the organisation out of Base first.

// CreateOrganisation inserts an organisation built by NewOrganisation.
func CreateOrganisation(ctx context.Context, db sqlx.ExecerContext, organisation *Organisation) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO organisations (uuid, randid, name, plan, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		organisation.UUID, organisation.RandId, organisation.Name, organisation.Plan,
		organisation.CreatedAt, organisation.UpdatedAt)
	return err
}

// CreateAccount inserts an account built by NewAccount.
func CreateAccount(ctx context.Context, db sqlx.ExecerContext, account *Account) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO accounts (uuid, randid, organisation_randid, name, handle, bio, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		account.UUID, account.RandId, account.OrganisationRandId, account.Name, account.Handle, account.Bio,
		account.CreatedAt, account.UpdatedAt)
	return err
}

// CreatePost inserts a post built by NewPost. It reaches its author's feed on the next read of it.
func CreatePost(ctx context.Context, db sqlx.ExecerContext, post *Post) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO posts (uuid, randid, author_randid, title, body, published, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		post.UUID, post.RandId, post.AuthorRandId, post.Title, post.Body, post.Published,
		post.CreatedAt, post.UpdatedAt)
	return err
}

// UpdatePost changes a post's contents.
func UpdatePost(ctx context.Context, db sqlx.ExecerContext, randId, title, body string) error {
	_, err := db.ExecContext(ctx,
		`UPDATE posts SET title = $2, body = $3, updated_at = now() WHERE randid = $1`, randId, title, body)
	return err
}

// RenameOrganisation is the payoff of examples/feed, kept: one UPDATE, two levels down the chain,
// and every post of every account in the organisation shows the new name on its next read.
func RenameOrganisation(ctx context.Context, db sqlx.ExecerContext, randId, name string) error {
	_, err := db.ExecContext(ctx,
		`UPDATE organisations SET name = $2, updated_at = now() WHERE randid = $1`, randId, name)
	return err
}

// MoveAccount points an account at another organisation: the account's value changes, no feed does.
func MoveAccount(ctx context.Context, db sqlx.ExecerContext, randId, organisationRandId string) error {
	_, err := db.ExecContext(ctx,
		`UPDATE accounts SET organisation_randid = $2, updated_at = now() WHERE randid = $1`, randId, organisationRandId)
	return err
}

// DeletePost deletes a post; the handler takes it out of its Base and its author's feed.
func DeletePost(ctx context.Context, db sqlx.ExecerContext, randId string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM posts WHERE randid = $1`, randId)
	return err
}
