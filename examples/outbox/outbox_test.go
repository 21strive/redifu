package outbox

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// testDatabase is an empty database for the example: a fresh namespace of the database
// REDIFU_TEST_DSN names, which every connection of the returned pool searches first, dropped at
// the end. The example installs the outbox into it from scratch, as an application's first
// migration would. Skips without REDIFU_TEST_DSN.
func testDatabase(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("REDIFU_TEST_DSN")
	if dsn == "" {
		t.Skip("REDIFU_TEST_DSN not set")
	}
	admin, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	namespace := fmt.Sprintf("zz_example_%d", time.Now().UnixNano())
	admin.MustExec(`CREATE SCHEMA ` + namespace)
	t.Cleanup(func() { admin.MustExec(`DROP SCHEMA ` + namespace + ` CASCADE`) })

	scoped := dsn + " search_path=" + namespace
	if strings.Contains(dsn, "://") {
		separator := "?"
		if strings.Contains(dsn, "?") {
			separator = "&"
		}
		scoped = dsn + separator + "search_path=" + namespace
	}
	db, err := sqlx.Connect("postgres", scoped)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func titles(posts []*Post) string {
	out := make([]string, 0, len(posts))
	for _, post := range posts {
		out = append(out, post.Title)
	}
	return strings.Join(out, ",")
}

// The whole adoption, end to end: the schema installed by the migration, the table watched, a
// write path that only writes SQL, and reads that see every write as soon as it committed.
func TestThePostsCacheFollowsEveryWrite(t *testing.T) {
	db := testDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate again: %v", err)
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cache, err := NewCache(db, rdb, "example-build", 1763500000, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	loop, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { cache.Run(loop); close(done) }()
	t.Cleanup(func() { stop(); <-done })

	// served reads the author's list from the cache — failing if it cannot answer — and checks it
	// against SQL, order included.
	served := func(author string) string {
		t.Helper()
		posts, ok := cache.ByAuthor.Read(ctx, author)
		if !ok {
			t.Fatalf("%s's posts were not served from the cache", author)
		}
		fromSQL, err := loadPosts(ctx, db, `WHERE author_uuid = $1 `+byAuthorOrder, author)
		if err != nil {
			t.Fatal(err)
		}
		if titles(posts) != titles(fromSQL) {
			t.Fatalf("%s: the cache says %q, the database %q", author, titles(posts), titles(fromSQL))
		}
		return titles(posts)
	}
	create := func(author, title string) string {
		t.Helper()
		randID, err := CreatePost(ctx, db, author, title)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // one post per millisecond: the order shows the newest first
		return randID
	}

	first := create("a1", "First")
	second := create("a1", "Second")
	create("a1", "Third")
	create("a2", "Elsewhere")
	if got := served("a1"); got != "Third,Second,First" {
		t.Fatalf("a1 = %q", got)
	}
	if got := served("a2"); got != "Elsewhere" {
		t.Fatalf("a2 = %q", got)
	}

	// Every write after the seed is in the next read: no write touched Redis.
	if err := RenamePost(ctx, db, first, "First, edited"); err != nil {
		t.Fatal(err)
	}
	if got := served("a1"); got != "Third,Second,First, edited" {
		t.Fatalf("a1 after a rename = %q", got)
	}
	if err := MovePost(ctx, db, second, "a2"); err != nil {
		t.Fatal(err)
	}
	if got := served("a1"); got != "Third,First, edited" {
		t.Fatalf("a1 after a move = %q", got)
	}
	if got := served("a2"); got != "Elsewhere,Second" {
		t.Fatalf("a2 after a move = %q", got)
	}
	if err := DeletePost(ctx, db, first); err != nil {
		t.Fatal(err)
	}
	if got := served("a1"); got != "Third" {
		t.Fatalf("a1 after a delete = %q", got)
	}
	if exists, _ := cache.Posts.Exists(ctx, first); exists {
		t.Fatal("a deleted post is still in the Base")
	}

	// A write that rolls back queues nothing.
	tx := db.MustBegin()
	if _, err := CreatePost(ctx, tx, "a1", "Rolled back"); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	if got := served("a1"); got != "Third" {
		t.Fatalf("a1 after a rolled-back insert = %q", got)
	}

	// With nobody reading, the drain loop applies the queue in the background.
	late := create("a3", "Late")
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		var queued int
		if err := db.Get(&queued, `SELECT count(*) FROM redifu_outbox`); err != nil {
			t.Fatal(err)
		}
		if queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d changes still queued: the drain loop did not apply them", queued)
		}
	}
	if post, err := cache.Posts.Get(ctx, late); err != nil || post.Title != "Late" {
		t.Fatalf("the drain loop did not write the post: %+v, %v", post, err)
	}

	// Redis flushed: every list seeds itself again.
	mr.FlushAll()
	if got := served("a2"); got != "Elsewhere,Second" {
		t.Fatalf("a2 after a flush = %q", got)
	}
	if posts, err := cache.PostsByAuthor(ctx, "a3"); err != nil || titles(posts) != "Late" {
		t.Fatalf("a3 = %q, %v", titles(posts), err)
	}
}
