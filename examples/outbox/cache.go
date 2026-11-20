package outbox

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/21strive/redifu"
	"github.com/21strive/redifu/outbox"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// postGeneration is the generation of the posts' Base: what one value holds. Change it whenever
// Post or postColumns changes; a build with the new value reloads what an older one wrote instead
// of reading it.
const postGeneration = "post 1"

// postTTL is the TTL of a post's value: longer than outbox.ListTTL, so a post outlives the lists
// it is in.
const postTTL = 2 * time.Hour

// Cache is the example's cache: the store, the posts' Base and one list per author.
type Cache struct {
	Store    *outbox.Store
	Posts    *redifu.Base[*Post]
	ByAuthor *outbox.List[*Post]
	db       *sqlx.DB
}

// NewCache builds the cache, once per process. db is the primary: the queue and the seeds must see
// a write as soon as it commits. revision and builtAt name the build (its commit, and when it was
// built in Unix seconds): during a rolling deploy two builds share the cache, and the newer one
// takes it over. ("", -1 falls back to a fingerprint of the binary and its modification time.)
func NewCache(db *sqlx.DB, rdb redis.UniversalClient, revision string, builtAt int64, logger *slog.Logger) (*Cache, error) {
	store := outbox.NewStore(db, rdb, true).
		WithBuild(revision, builtAt).
		WithLogger(logger)

	posts, err := outbox.NewBase[*Post](store, "post:%s", postTTL, postGeneration)
	if err != nil {
		return nil, err
	}
	byAuthor, err := outbox.NewList[*Post](store, posts, "post:author:%s", redifu.Descending, []string{"posts"},
		func(ctx context.Context, tx *sqlx.Tx, params []string) ([]*Post, error) {
			return loadPosts(ctx, tx, `WHERE author_uuid = $1`, params[0])
		},
		func(params []string) string { return params[0] }, // an author's list is the author's to lock
	)
	if err != nil {
		return nil, err
	}
	store.Register(postHandler{posts: posts, byAuthor: byAuthor})
	return &Cache{Store: store, Posts: posts, ByAuthor: byAuthor, db: db}, nil
}

// Run drains the queue in the background until ctx ends: in every process that writes posts, so
// the queue stays short even when nobody reads.
func (c *Cache) Run(ctx context.Context) {
	c.Store.RunDrainLoop(ctx, time.Second)
}

// PostsByAuthor reads an author's posts, newest first. The cache answers when it can — the author's
// queued changes applied first, so a post written a moment ago is there — and SQL otherwise.
func (c *Cache) PostsByAuthor(ctx context.Context, author string) ([]*Post, error) {
	if posts, ok := c.ByAuthor.Read(ctx, author); ok {
		return posts, nil
	}
	return loadPosts(ctx, c.db, `WHERE author_uuid = $1 `+byAuthorOrder, author)
}

// postHandler applies the queued changes of posts: it reloads every post that changed, writes it
// into the Base (or deletes it), and moves it between the authors' lists. It reads through
// batch.Tx — the drain's snapshot, under the author's lock — and only enqueues into batch.Pipe.
type postHandler struct {
	posts    *redifu.Base[*Post]
	byAuthor *outbox.List[*Post]
}

func (h postHandler) Tables() []string { return []string{"posts"} }

func (h postHandler) Apply(batch *outbox.Batch, table string, changes []outbox.Change) error {
	keys, first := outbox.ChangeKeys(changes)
	// The keys as one text parameter (a randid never holds a comma); with lib/pq,
	// randid = ANY($1) and pq.Array(keys) does the same.
	current, err := loadPosts(batch.Ctx, batch.Tx, `WHERE randid = ANY(string_to_array($1, ','))`, strings.Join(keys, ","))
	if err != nil {
		return err
	}
	byKey := make(map[string]*Post, len(current))
	for _, post := range current {
		byKey[post.RandId] = post
	}
	for _, key := range keys {
		// The row before this batch's first change of it is what Redis holds: the list it is in.
		old, post := first[key].Old(), byKey[key]
		stub := &Post{Record: &redifu.Record{RandId: key}}
		if old != nil && (post == nil || old.Str("author_uuid") != post.AuthorUUID) {
			if err := h.byAuthor.Remove(batch.Ctx, batch.Pipe, stub, old.Str("author_uuid")); err != nil {
				return err
			}
		}
		if post == nil { // deleted
			if err := h.posts.WithPipeline(batch.Pipe).Del(batch.Ctx, stub); err != nil {
				return err
			}
			continue
		}
		if err := h.posts.WithPipeline(batch.Pipe).Set(batch.Ctx, post); err != nil {
			return err
		}
		// Into the author's list — once it is seeded; a cold list loads the post when it is.
		if err := h.byAuthor.Add(batch.Ctx, batch.Pipe, post, post.AuthorUUID); err != nil {
			return err
		}
	}
	return nil
}
