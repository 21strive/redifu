package outboxfeed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/21strive/redifu"
	"github.com/21strive/redifu/outbox"
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
	namespace := fmt.Sprintf("zz_outboxfeed_%d", time.Now().UnixNano())
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

// pageSize is small, so that a walk of a feed crosses several pages and extends the feed from its
// tail on the way.
const pageSize = 3

// describe renders a page for comparison: every post with its author and the author's organisation,
// then the cursor and HasMore.
func describe(page *FeedPage) string {
	posts := make([]string, 0, len(page.Posts))
	for _, post := range page.Posts {
		author, organisation := "<nil>", "<nil>"
		if post.Author != nil {
			author = post.Author.Name
			if post.Author.Organisation != nil {
				organisation = post.Author.Organisation.Name
			}
		}
		posts = append(posts, post.Title+"/"+author+"/"+organisation)
	}
	return fmt.Sprintf("[%s] cursor=%q more=%v", strings.Join(posts, ", "), page.Cursor, page.HasMore)
}

// The whole adoption, end to end: the schema installed by the migration, three tables watched, a
// write path that only writes SQL — and an author's feed, read page by page, that resolves its
// relations two levels down and sees every write as soon as it committed.
func TestTheFeedFollowsEveryWrite(t *testing.T) {
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

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// served reads one page from the cache. While the drain loop applies an owner's queue it holds
	// that owner's lock, and the store answers "not now" — an application then reads SQL. The test
	// asks again, until the cache answers.
	served := func(author string, after redifu.Cursor) *FeedPage {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if page, ok := cache.cachedFeed(ctx, author, after, pageSize); ok {
				return page
			}
		}
		t.Fatalf("the feed of %s was not served from the cache", author)
		return nil
	}

	// walk reads an author's whole feed from the cache, page by page, and checks every page — its
	// posts, their authors and organisations, its cursor — against the database's answer. It returns
	// the titles, pages apart by "|", and every "author @ organisation" the posts resolved to.
	walk := func(author *Account) (string, string) {
		t.Helper()
		var pages []string
		resolved := map[string]bool{}
		after := redifu.Cursor{}
		for {
			page := served(author.RandId, after)
			fromSQL, err := cache.feedFromSQL(ctx, author.RandId, after, pageSize)
			must(err)
			if describe(page) != describe(fromSQL) {
				t.Fatalf("%s, the page after %v:\n the cache    %s\n the database %s", author.Name, after, describe(page), describe(fromSQL))
			}
			titles := make([]string, 0, len(page.Posts))
			for _, post := range page.Posts {
				if post.Author == nil || post.Author.Organisation == nil {
					t.Fatalf("%q came back without its author or the author's organisation", post.Title)
				}
				titles = append(titles, post.Title)
				resolved[post.Author.Name+" @ "+post.Author.Organisation.Name] = true
			}
			pages = append(pages, strings.Join(titles, ","))
			if !page.HasMore {
				break
			}
			if after, err = outbox.DecodeCursor(page.Cursor); err != nil {
				t.Fatal(err)
			}
		}
		names := make([]string, 0, len(resolved))
		for name := range resolved {
			names = append(names, name)
		}
		sort.Strings(names)
		return strings.Join(pages, "|"), strings.Join(names, ", ")
	}
	expect := func(author *Account, wantPages, wantResolved string) {
		t.Helper()
		if pages, resolved := walk(author); pages != wantPages || resolved != wantResolved {
			t.Fatalf("%s: %q, resolved to %q; want %q, resolved to %q", author.Name, pages, resolved, wantPages, wantResolved)
		}
	}

	// Two organisations with an author each: seven posts by Ada an hour apart, one by Bob.
	acme, globex := NewOrganisation("Acme", "pro"), NewOrganisation("Globex", "free")
	must(CreateOrganisation(ctx, db, acme))
	must(CreateOrganisation(ctx, db, globex))
	ada := NewAccount(acme.RandId, "Ada", "@ada", "Writes about compilers.")
	bob := NewAccount(globex.RandId, "Bob", "@bob", "")
	must(CreateAccount(ctx, db, ada))
	must(CreateAccount(ctx, db, bob))
	start := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	publish := func(author *Account, title string, hour int) *Post {
		t.Helper()
		post := NewPost(author.RandId, title, "body")
		post.Published = start.Add(time.Duration(hour) * time.Hour)
		must(CreatePost(ctx, db, post))
		return post
	}
	posts := map[string]*Post{}
	for hour := 1; hour <= 7; hour++ {
		title := fmt.Sprintf("p%d", hour)
		posts[title] = publish(ada, title, hour)
	}
	publish(bob, "b1", 3)

	// The first read of a feed seeds its head; reading on extends it from its tail.
	expect(ada, "p7,p6,p5|p4,p3,p2|p1", "Ada @ Acme")
	expect(bob, "b1", "Bob @ Globex")

	// One UPDATE, two levels down the chain, and every post shows it on the next read: the
	// organisation is one key, and nothing was purged or seeded again.
	must(RenameOrganisation(ctx, db, acme.RandId, "Acme Corp"))
	expect(ada, "p7,p6,p5|p4,p3,p2|p1", "Ada @ Acme Corp")

	// The author points at another organisation: the account's value changes, the feed does not.
	must(MoveAccount(ctx, db, ada.RandId, globex.RandId))
	expect(ada, "p7,p6,p5|p4,p3,p2|p1", "Ada @ Globex")

	// A new post enters at the head; an edited one keeps its place.
	posts["p8"] = publish(ada, "p8", 8)
	must(UpdatePost(ctx, db, posts["p4"].RandId, "p4-edited", "body, edited"))
	expect(ada, "p8,p7,p6|p5,p4-edited,p3|p2,p1", "Ada @ Globex")

	// A write from outside the application — a backfill in psql — reaches the cache the same way:
	// p2 changes author, leaves Ada's feed and enters Bob's.
	db.MustExec(`UPDATE posts SET author_randid = $1, updated_at = now() WHERE randid = $2`, bob.RandId, posts["p2"].RandId)
	expect(ada, "p8,p7,p6|p5,p4-edited,p3|p1", "Ada @ Globex")
	expect(bob, "b1,p2", "Bob @ Globex")

	// A delete takes the post out of its feed and its Base.
	must(DeletePost(ctx, db, posts["p1"].RandId))
	expect(ada, "p8,p7,p6|p5,p4-edited,p3", "Ada @ Globex")
	if exists, err := cache.Posts.Exists(ctx, posts["p1"].RandId); err != nil || exists {
		t.Fatalf("the deleted post is still in its Base (%v)", err)
	}

	// A write that rolls back queues nothing.
	tx := db.MustBegin()
	must(CreatePost(ctx, tx, NewPost(ada.RandId, "rolled-back", "body")))
	must(tx.Rollback())
	expect(ada, "p8,p7,p6|p5,p4-edited,p3", "Ada @ Globex")

	// With nobody reading, the drain loop applies the queue in the background: a new author and
	// their first post land in Redis, and a single read resolves the chain from what it wrote.
	cy := NewAccount(acme.RandId, "Cy", "@cy", "")
	must(CreateAccount(ctx, db, cy))
	late := publish(cy, "c1", 9)
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		var queued int
		must(db.Get(&queued, `SELECT count(*) FROM redifu_outbox`))
		if queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d changes still queued: the drain loop did not apply them", queued)
		}
	}
	post, err := cache.Posts.Get(ctx, late.RandId)
	if err != nil || post.Title != "c1" || post.Author == nil || post.Author.Name != "Cy" ||
		post.Author.Organisation == nil || post.Author.Organisation.Name != "Acme Corp" {
		t.Fatalf("the drain loop did not write the post and its author: %+v, %v", post, err)
	}

	// Redis flushed: every feed seeds itself again, and the related entities are loaded again.
	mr.FlushAll()
	expect(ada, "p8,p7,p6|p5,p4-edited,p3", "Ada @ Globex")
	expect(bob, "b1,p2", "Bob @ Globex")
	expect(cy, "c1", "Cy @ Acme Corp")

	// The public read gives the page whoever answers it; an author nobody knows has no posts, and a
	// cursor no feed handed out is the client's error.
	first, err := cache.AuthorFeed(ctx, ada.RandId, "", pageSize)
	must(err)
	fromSQL, err := cache.feedFromSQL(ctx, ada.RandId, redifu.Cursor{}, pageSize)
	must(err)
	if describe(first) != describe(fromSQL) {
		t.Fatalf("AuthorFeed gave %s, the database %s", describe(first), describe(fromSQL))
	}
	if nobody, err := cache.AuthorFeed(ctx, "nobody", "", pageSize); err != nil || len(nobody.Posts) != 0 || nobody.HasMore {
		t.Fatalf("an unknown author's feed: %+v, %v", nobody, err)
	}
	if _, err := cache.AuthorFeed(ctx, ada.RandId, "not a cursor", pageSize); !errors.Is(err, outbox.ErrBadCursor) {
		t.Fatalf("a malformed cursor answered %v", err)
	}
}
