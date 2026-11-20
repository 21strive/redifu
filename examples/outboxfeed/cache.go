package outboxfeed

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/21strive/redifu"
	"github.com/21strive/redifu/outbox"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// The generation of each entity's Base: what one stored value holds. Change it whenever the struct
// or the columns it is loaded with (load.go) change; a build with the new value reloads what an
// older build wrote instead of reading it.
const (
	organisationGeneration = "organisation 1"
	accountGeneration      = "account 1"
	postGeneration         = "post 1"
)

// entityTTL is the TTL of every Base: longer than outbox.ListTTL, the feed's, so a post outlives
// the feed it is in, and an author and an organisation the posts that point at them.
const entityTTL = 2 * time.Hour

// postsPerPage is the feed's page size when a read asks for none.
const postsPerPage = 20

// Cache is everything the example builds on one store, once per process.
type Cache struct {
	Store *outbox.Store

	// The Bases of the related entities, behind readers of their own (cachedFeed says why).
	Organisations *outbox.Detail[*Organisation]
	Accounts      *outbox.Detail[*Account]

	Posts *redifu.Base[*Post]
	Feed  *outbox.Feed[*Post]

	db *sqlx.DB
}

// NewCache builds the cache. db is the primary: the queue and the seeds must see a write as soon as
// it commits. revision and builtAt name the build (its commit, and when it was built in Unix
// seconds), so that of two builds sharing the cache during a rolling deploy the newer takes it.
func NewCache(db *sqlx.DB, rdb redis.UniversalClient, revision string, builtAt int64, logger *slog.Logger) (*Cache, error) {
	store := outbox.NewStore(db, rdb, true).
		WithBuild(revision, builtAt).
		WithLogger(logger)

	// A Base per entity, each with its generation, built through the store so that it takes the
	// lease's epoch and its key format is registered for Purge.
	organisations, err := outbox.NewBase[*Organisation](store, "organisation:%s", entityTTL, organisationGeneration)
	if err != nil {
		return nil, fmt.Errorf("organisation base: %w", err)
	}
	accounts, err := outbox.NewBase[*Account](store, "account:%s", entityTTL, accountGeneration)
	if err != nil {
		return nil, fmt.Errorf("account base: %w", err)
	}
	posts, err := outbox.NewBase[*Post](store, "post:%s", entityTTL, postGeneration)
	if err != nil {
		return nil, fmt.Errorf("post base: %w", err)
	}

	// The relations of examples/feed, each on the Base of the entity that owns the randId, so a feed
	// read and a single Posts.Get resolve the same chain. The outbox changes nothing here: it keeps
	// every Base current, and a relation reads whatever its Base holds.
	organisationRelation, err := redifu.Relate(organisations,
		func(a *Account) string { return a.OrganisationRandId },
		func(a *Account, o *Organisation) { a.Organisation = o },
	)
	if err != nil {
		return nil, fmt.Errorf("organisation relation: %w", err)
	}
	accounts.AddRelation(organisationRelation)

	authorRelation, err := redifu.Relate(accounts,
		func(p *Post) string { return p.AuthorRandId },
		func(p *Post, a *Account) { p.Author = a },
	)
	if err != nil {
		return nil, fmt.Errorf("author relation: %w", err)
	}
	posts.AddRelation(authorRelation)

	// An author's feed: one Timeline per author, seeded from its head on the first read and
	// extended from its tail whenever a page runs past it. Its tables are every table its posts and
	// their relations come from; it is served only while all three are watched.
	feed, err := outbox.NewFeed[*Post](store, posts, "feed:author:%s", postsPerPage, redifu.Descending,
		[]string{"posts", "accounts", "organisations"},
		func(ctx context.Context, tx *sqlx.Tx, params []string, after redifu.Cursor, limit int64) ([]*Post, error) {
			return loadFeed(ctx, tx, params[0], after, limit)
		},
		func(params []string) string { return params[0] }, // an author's feed is the author's to lock
	)
	if err != nil {
		return nil, fmt.Errorf("feed: %w", err)
	}

	cache := &Cache{
		Store: store,
		Organisations: outbox.NewDetail[*Organisation](store, organisations, []string{"organisations"},
			func(*Organisation) string { return "" }, // shared: queued under the global owner
			loadOrganisations,
		),
		Accounts: outbox.NewDetail[*Account](store, accounts, []string{"accounts", "organisations"},
			func(a *Account) string { return a.RandId }, // an account is its own owner
			loadAccounts,
		),
		Posts: posts,
		Feed:  feed,
		db:    db,
	}
	store.Register(handler{organisations: organisations, accounts: accounts, posts: posts, feed: feed})
	return cache, nil
}

// Run drains the queue in the background until ctx ends: in every process that writes these
// tables, so the queue stays short even when nobody reads.
func (c *Cache) Run(ctx context.Context) {
	c.Store.RunDrainLoop(ctx, time.Second)
}

// handler applies the queued changes of the three tables. It reads through batch.Tx — the drain's
// snapshot, under the owner's lock — and only enqueues into batch.Pipe; the store sends a batch in
// one MULTI/EXEC.
type handler struct {
	organisations *redifu.Base[*Organisation]
	accounts      *redifu.Base[*Account]
	posts         *redifu.Base[*Post]
	feed          *outbox.Feed[*Post]
}

func (h handler) Tables() []string { return []string{"organisations", "accounts", "posts"} }

func (h handler) Apply(batch *outbox.Batch, table string, changes []outbox.Change) error {
	switch table {
	case "organisations":
		return refresh(batch, h.organisations, changes, loadOrganisations, newOrganisation)
	case "accounts":
		return refresh(batch, h.accounts, changes, loadAccounts, newAccount)
	case "posts":
		return h.applyPosts(batch, changes)
	}
	return nil
}

// refresh brings organisations and accounts in line: each changed row reloaded and written into its
// Base, or deleted from it with the row. Nothing else moves. These entities are reached through
// relations, never through an index, so a rename is one key, and every post that points at it
// resolves the new value on its next read — no feed purged, no page seeded again.
func refresh[E redifu.Blueprint](batch *outbox.Batch, base *redifu.Base[E], changes []outbox.Change,
	load func(context.Context, sqlx.QueryerContext, []string) ([]E, error),
	named func(randId string) E,
) error {
	keys, _ := outbox.ChangeKeys(changes)
	current, err := load(batch.Ctx, batch.Tx, keys)
	if err != nil {
		return err
	}
	byKey := make(map[string]E, len(current))
	for _, item := range current {
		byKey[item.GetRandId()] = item
	}
	for _, key := range keys {
		item, exists := byKey[key]
		if !exists { // deleted
			if err := base.WithPipeline(batch.Pipe).Del(batch.Ctx, named(key)); err != nil {
				return err
			}
			continue
		}
		if err := base.WithPipeline(batch.Pipe).Set(batch.Ctx, item); err != nil {
			return err
		}
	}
	return nil
}

// applyPosts reloads every post that changed, writes it into its Base (or deletes it), and moves it
// between the authors' feeds.
func (h handler) applyPosts(batch *outbox.Batch, changes []outbox.Change) error {
	keys, first := outbox.ChangeKeys(changes)
	current, err := loadPosts(batch.Ctx, batch.Tx, `WHERE p.`+byRandIds, strings.Join(keys, ","))
	if err != nil {
		return err
	}
	byKey := make(map[string]*Post, len(current))
	for _, post := range current {
		byKey[post.RandId] = post
	}
	for _, key := range keys {
		// The row before this batch's first change of the post is what Redis holds: the feed it is
		// in. A post whose author changed — a backfill, an admin in psql — leaves that feed.
		old, post := first[key].Old(), byKey[key]
		if old != nil && (post == nil || old.Str("author_randid") != post.AuthorRandId) {
			if err := h.feed.Remove(batch.Ctx, batch.Pipe, newPost(key), old.Str("author_randid")); err != nil {
				return err
			}
		}
		if post == nil { // deleted
			if err := h.posts.WithPipeline(batch.Pipe).Del(batch.Ctx, newPost(key)); err != nil {
				return err
			}
			continue
		}
		if err := h.posts.WithPipeline(batch.Pipe).Set(batch.Ctx, post); err != nil {
			return err
		}
		// Into the author's feed when the post falls in the part of it the index holds. A cold feed,
		// or a post older than the index's tail, is loaded by the seed that reaches it.
		if err := h.feed.Add(batch.Ctx, batch.Pipe, post, post.AuthorRandId); err != nil {
			return err
		}
	}
	return nil
}
