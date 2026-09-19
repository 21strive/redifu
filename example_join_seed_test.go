package redifu

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// A three-level shape, the way a consumer would declare it.
//
//	Article -> Writer -> Publisher
//
// Each of the three is an entity in its own right. Each gets its own Base, is
// updated through its own Base, and is stored exactly once no matter how many
// articles or writers point at it.
// ---------------------------------------------------------------------------

type Publisher struct {
	*Record
	Name string `json:"name"`
	Tier string `json:"tier"`
}

type Writer struct {
	*Record
	Name            string     `json:"name"`
	Bio             string     `json:"bio"`
	PublisherRandId string     `json:"publisherRandId"`
	Publisher       *Publisher `json:"-"`
}

type Article struct {
	*Record
	Title        string  `json:"title"`
	Body         string  `json:"body"`
	WriterRandId string  `json:"writerRandId"`
	Writer       *Writer `json:"-"`
}

func newPublisher(t *testing.T, name, tier string) *Publisher {
	t.Helper()
	publisher := &Publisher{}
	InitRecord(publisher)
	publisher.Name, publisher.Tier = name, tier
	return publisher
}

func newWriter(t *testing.T, name, bio string) *Writer {
	t.Helper()
	writer := &Writer{}
	InitRecord(writer)
	writer.Name, writer.Bio = name, bio
	writer.Publisher = nil
	return writer
}

func newArticle(t *testing.T, title string, createdAt time.Time) *Article {
	t.Helper()
	article := &Article{}
	InitRecord(article)
	article.Title = title
	article.SetCreatedAt(createdAt)
	article.Writer = nil
	return article
}

// newsroom is the wiring a consumer does once, at startup.
type newsroom struct {
	client        redis.UniversalClient
	articleBase   *Base[*Article]
	writerBase    *Base[*Writer]
	publisherBase *Base[*Publisher]
	feed          *Timeline[*Article]
}

func newNewsroom(t *testing.T, client redis.UniversalClient) *newsroom {
	t.Helper()

	articleBase := mustBase[*Article](t, client, "article:%s", baseTTL)
	writerBase := mustBase[*Writer](t, client, "writer:%s", baseTTL)
	publisherBase := mustBase[*Publisher](t, client, "publisher:%s", baseTTL)

	// The relation is declared on the Base of the entity that owns the pointer, not
	// on a list. A Writer's publisher is a fact about the Writer; it should not depend
	// on which feed the writer happened to be fetched through.
	writerBase.AddRelation(mustRelation(t, publisherBase,
		func(w *Writer) string { return w.PublisherRandId },
		func(w *Writer, p *Publisher) { w.Publisher = p },
	))
	articleBase.AddRelation(mustRelation(t, writerBase,
		func(a *Article) string { return a.WriterRandId },
		func(a *Article, w *Writer) { a.Writer = w },
	))

	feed, err := NewTimeline[*Article](client, articleBase, "feed:%s", 20, Descending, indexTTL)
	if err != nil {
		t.Fatalf("NewTimeline: %v", err)
	}

	return &newsroom{
		client:        client,
		articleBase:   articleBase,
		writerBase:    writerBase,
		publisherBase: publisherBase,
		feed:          feed,
	}
}

// joinRow is one row of
//
//	SELECT a.randid, a.title, a.body, a.writer_randid, a.created_at,
//	       w.randid, w.name, w.publisher_randid,
//	       p.randid, p.name
//	FROM articles a
//	JOIN writers w    ON w.randid = a.writer_randid
//	JOIN publishers p ON p.randid = w.publisher_randid
//	WHERE a.section = $1
//	ORDER BY a.created_at DESC
//
// Note what the SELECT does *not* contain: w.bio and p.tier. A join written for a
// feed almost never selects every column of every joined table, and that omission is
// the thing that decides between Set and SetIfAbsent below.
type joinRow struct {
	article   *Article
	writer    *Writer
	publisher *Publisher
}

// seedFeed is the consumer's seeder. One pass over the rows, one pipeline, every
// entity written to its own Base.
func (n *newsroom) seedFeed(ctx context.Context, section string, rows []joinRow) error {
	pipe := n.client.Pipeline()

	// The same writer appears on many rows. Deduplicating is not required for
	// correctness — the same key with the same value is idempotent — but it keeps the
	// pipeline small when the joined payload is large.
	seenWriter := map[string]bool{}
	seenPublisher := map[string]bool{}

	for _, row := range rows {
		// The article is fully selected, so it is written outright.
		if err := n.articleBase.WithPipeline(pipe).Set(ctx, row.article); err != nil {
			return err
		}
		if err := n.feed.IngestItem(ctx, pipe, row.article, true, section); err != nil {
			return err
		}

		// The joined entities are only partially selected, so they are written with
		// SetIfAbsent: warm the key if nothing is there, refresh its TTL either way,
		// and never overwrite a complete Writer already in Redis with the two columns
		// this particular join happened to need.
		if !seenWriter[row.writer.GetRandId()] {
			seenWriter[row.writer.GetRandId()] = true
			if err := n.writerBase.WithPipeline(pipe).SetIfAbsent(ctx, row.writer); err != nil {
				return err
			}
		}
		if !seenPublisher[row.publisher.GetRandId()] {
			seenPublisher[row.publisher.GetRandId()] = true
			if err := n.publisherBase.WithPipeline(pipe).SetIfAbsent(ctx, row.publisher); err != nil {
				return err
			}
		}
	}

	if err := n.feed.SetExpiration(ctx, pipe, section); err != nil {
		return err
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (n *newsroom) fixtureRows(t *testing.T) []joinRow {
	t.Helper()

	publisher := newPublisher(t, "Gazette", "standard")
	writer := newWriter(t, "Ada", "") // bio not selected by the join
	writer.PublisherRandId = publisher.GetRandId()

	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]joinRow, 0, 3)
	for i := 0; i < 3; i++ {
		article := newArticle(t, []string{"first", "second", "third"}[i], base.Add(time.Duration(i)*time.Hour))
		article.WriterRandId = writer.GetRandId()
		// Every row carries its own copy of the joined columns, exactly as a SQL
		// driver would hand them back.
		rowWriter := newWriter(t, writer.Name, "")
		rowWriter.RandId = writer.GetRandId()
		rowWriter.PublisherRandId = publisher.GetRandId()
		rowPublisher := newPublisher(t, publisher.Name, "")
		rowPublisher.RandId = publisher.GetRandId()

		rows = append(rows, joinRow{article: article, writer: rowWriter, publisher: rowPublisher})
	}
	return rows
}

// ---------------------------------------------------------------------------

func TestSeedingAJoinStoresEachEntityInItsOwnBase(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	room := newNewsroom(t, client)

	rows := room.fixtureRows(t)
	if err := room.seedFeed(ctx, "tech", rows); err != nil {
		t.Fatalf("seedFeed: %v", err)
	}

	output := room.feed.Fetch(nil).WithParams("tech").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	if len(output.Items()) != 3 {
		t.Fatalf("fetched %d articles, want 3", len(output.Items()))
	}

	for _, article := range output.Items() {
		if article.Writer == nil {
			t.Fatal("article came back without its writer")
		}
		if article.Writer.Publisher == nil {
			t.Fatal("the writer came back without its publisher — the second level did not resolve")
		}
		if article.Writer.Publisher.Name != "Gazette" {
			t.Fatalf("publisher name = %q", article.Writer.Publisher.Name)
		}
	}

	// Three articles, one writer key, one publisher key. The joined entities were not
	// copied into the articles.
	writerCount, err := client.Keys(ctx, "writer:*").Result()
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(writerCount) != 1 {
		t.Fatalf("%d writer keys for 3 articles by one writer, want 1", len(writerCount))
	}
}

// This is the question the pattern exists to answer: the joined entity has its own
// Base, so it is updated through that Base, and every article reflects the change
// without any feed being purged or reseeded.
func TestUpdatingAJoinedEntityThroughItsOwnBase(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	room := newNewsroom(t, client)

	rows := room.fixtureRows(t)
	if err := room.seedFeed(ctx, "tech", rows); err != nil {
		t.Fatalf("seedFeed: %v", err)
	}

	// Two levels down, one write, no feed touched.
	publisher, err := room.publisherBase.Get(ctx, rows[0].publisher.GetRandId())
	if err != nil {
		t.Fatalf("Get publisher: %v", err)
	}
	publisher.Name = "The Gazette"
	publisher.Tier = "premium"
	if err := room.publisherBase.Set(ctx, publisher); err != nil {
		t.Fatalf("Set publisher: %v", err)
	}

	output := room.feed.Fetch(nil).WithParams("tech").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	for _, article := range output.Items() {
		if article.Writer.Publisher.Name != "The Gazette" {
			t.Fatalf("publisher name = %q, want the updated value on every article", article.Writer.Publisher.Name)
		}
		if article.Writer.Publisher.Tier != "premium" {
			t.Fatalf("publisher tier = %q, want the updated value", article.Writer.Publisher.Tier)
		}
	}

	// And a single-item read, with no index involved at all, resolves the same chain.
	article, errGet := room.articleBase.Get(ctx, rows[0].article.GetRandId())
	if errGet != nil {
		t.Fatalf("Get article: %v", errGet)
	}
	if article.Writer.Publisher.Name != "The Gazette" {
		t.Fatal("a single-item read did not resolve the chain")
	}
}

// The reason the seeder above uses SetIfAbsent for joined entities: a join selects the
// columns the feed needs, not the whole table, so writing it with Set would overwrite
// the complete entity with a partial one.
func TestSetWouldOverwriteACompleteEntityWithAPartialJoinRow(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	room := newNewsroom(t, client)

	// The writer already exists in full — written earlier by its own loader.
	rows := room.fixtureRows(t)
	full := newWriter(t, "Ada", "Covers distributed systems.")
	full.RandId = rows[0].writer.GetRandId()
	full.PublisherRandId = rows[0].publisher.GetRandId()
	if err := room.writerBase.Set(ctx, full); err != nil {
		t.Fatalf("Set writer: %v", err)
	}

	// Now a feed is seeded from a join that never selected w.bio.
	if err := room.seedFeed(ctx, "tech", rows); err != nil {
		t.Fatalf("seedFeed: %v", err)
	}

	stored, err := room.writerBase.Get(ctx, full.GetRandId())
	if err != nil {
		t.Fatalf("Get writer: %v", err)
	}
	if stored.Bio != "Covers distributed systems." {
		t.Fatalf("Bio = %q — the join row overwrote a column it never selected", stored.Bio)
	}

	// What Set would have done instead, for contrast.
	if err := room.writerBase.Set(ctx, rows[0].writer); err != nil {
		t.Fatalf("Set writer: %v", err)
	}
	clobbered, errGet := room.writerBase.Get(ctx, full.GetRandId())
	if errGet != nil {
		t.Fatalf("Get writer: %v", errGet)
	}
	if clobbered.Bio != "" {
		t.Fatal("expected Set to overwrite the entity with the partial join row")
	}
}
