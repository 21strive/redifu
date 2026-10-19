package redifu

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Interleavings of two generations that the first round of generation scenarios did not drive:
// a drop between two steps of a flow, a self-heal that meets another generation, a top-up whose
// index changed under it, a TimeSeries whose ranges must go with its index. Every scenario runs
// on miniredis here and on a real server in TestRealRedis.

func TestTimeSeriesGenerationSegmentsGoWithTheIndex(t *testing.T) {
	scenarioTimeSeriesGenerationSegmentsGoWithTheIndex(t, miniClient)
}

func newGenerationSeries(t *testing.T, client redis.UniversalClient, base *Base[*Booking], generation string) *TimeSeries[*Booking] {
	t.Helper()
	series := mustTimeSeries[*Booking](t, client, base, "cal:%s", indexTTL)
	if err := series.SetSortingReference("StartAt"); err != nil {
		t.Fatalf("SetSortingReference: %v", err)
	}
	if err := series.SetGeneration(generation); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	return series
}

func seedSeriesRange(t *testing.T, client redis.UniversalClient, base *Base[*Booking], series *TimeSeries[*Booking], from, to time.Time, bookings ...*Booking) {
	t.Helper()
	ctx := context.Background()
	pipe := client.TxPipeline()
	for _, booking := range bookings {
		if err := base.WithPipeline(pipe).Set(ctx, booking); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := series.IngestItem(ctx, pipe, booking, "o1"); err != nil {
			t.Fatalf("IngestItem: %v", err)
		}
	}
	if err := series.AddSegment(ctx, pipe, from, to, "o1"); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

// Another generation drops the index — a Count, a RemoveItem, a stamp — and the series' generation
// then seeds a different range. The ranges it seeded before must read as gaps: they went with the
// index, so the restamped index cannot claim them.
func scenarioTimeSeriesGenerationSegmentsGoWithTheIndex(t *testing.T, newClient clientFactory) {
	for _, dropper := range []string{"Count", "RemoveItem", "SetExpiration"} {
		t.Run(dropper, func(t *testing.T) {
			client := newClient(t)
			ctx := context.Background()
			base := mustBase[*Booking](t, client, "booking:%s", baseTTL)
			a, b := newGenerationSeries(t, client, base, "a"), newGenerationSeries(t, client, base, "b")

			jan := newBooking(t, "jan", jan1.Add(24*time.Hour))
			seedSeriesRange(t, client, base, b, jan1, feb1, jan)
			if !keyExists(t, client, "{cal:o1}:segments") || keyExists(t, client, "cal:o1:segments") {
				t.Fatal("with a generation the segment store is {cal:o1}:segments, under the index's hash tag")
			}

			switch dropper {
			case "Count":
				if _, err := a.Count(ctx, "o1"); err != nil {
					t.Fatalf("Count: %v", err)
				}
			case "RemoveItem":
				if err := a.RemoveItem(ctx, jan, "o1"); !errors.Is(err, ErrNotIngested) {
					t.Fatalf("a RemoveItem on b's series = %v, want ErrNotIngested", err)
				}
			case "SetExpiration":
				pipe := client.TxPipeline()
				if err := a.SetExpiration(ctx, pipe, "o1"); err != nil {
					t.Fatalf("SetExpiration: %v", err)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					t.Fatalf("Exec: %v", err)
				}
				if _, err := b.Count(ctx, "o1"); err != nil { // b drops a's stamp in turn
					t.Fatalf("Count: %v", err)
				}
			}
			if keyExists(t, client, "{cal:o1}:segments") {
				t.Fatal("the drop must take the segment store with the index")
			}

			feb := newBooking(t, "feb", feb1.Add(24*time.Hour))
			seedSeriesRange(t, client, base, b, feb1, mar1, feb)

			items, gap, err := b.Fetch(jan1, feb1).WithParams("o1").Exec(ctx)
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if !gap || len(items) != 0 {
				t.Fatalf("January after the drop = %d items, gap %v: it must read as a gap", len(items), gap)
			}
			items, gap, err = b.Fetch(feb1, mar1).WithParams("o1").Exec(ctx)
			if err != nil || gap || len(items) != 1 || items[0].Name != "feb" {
				t.Fatalf("February = %v, gap %v, %v", items, gap, err)
			}
			if segments, err := b.GetSegments(ctx, "o1"); err != nil || len(segments) != 1 {
				t.Fatalf("segments = %v, %v; want February only", segments, err)
			}
		})
	}
}

func TestTimeSeriesGenerationAddItemAndRemove(t *testing.T) {
	scenarioTimeSeriesGenerationAddItemAndRemove(t, miniClient)
}

// AddItem decides coverage and writes in one script; Remove of a segment is behind the guard; the
// TTLs of the store, the index and the marker move together.
func scenarioTimeSeriesGenerationAddItemAndRemove(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	base := mustBase[*Booking](t, client, "booking:%s", baseTTL)
	a, b := newGenerationSeries(t, client, base, "a"), newGenerationSeries(t, client, base, "b")

	seedSeriesRange(t, client, base, a, jan1, feb1)
	if err := a.AddItem(ctx, newBooking(t, "edge", feb1), "o1"); err != nil {
		t.Fatalf("AddItem on the upper bound of a seeded range = %v", err)
	}
	if err := a.AddItem(ctx, newBooking(t, "later", feb1.Add(time.Hour)), "o1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("AddItem outside every range = %v, want ErrNotIngested", err)
	}
	if items, gap, err := a.Fetch(jan1, feb1).WithParams("o1").Exec(ctx); err != nil || gap || len(items) != 1 {
		t.Fatalf("Fetch = %v, gap %v, %v", items, gap, err)
	}
	for _, key := range []string{"{cal:o1}", "{cal:o1}:segments", "{cal:o1}:gen"} {
		if ttl := client.PTTL(ctx, key).Val(); ttl <= 0 || ttl > indexTTL {
			t.Fatalf("PTTL %s = %v, want the index TTL", key, ttl)
		}
	}

	if err := b.AddItem(ctx, newBooking(t, "b", jan1.Add(time.Hour)), "o1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("b AddItem into a's series = %v, want ErrNotIngested", err)
	}
	for _, key := range []string{"{cal:o1}", "{cal:o1}:segments", "{cal:o1}:gen"} {
		if keyExists(t, client, key) {
			t.Fatalf("%s survived another generation's write", key)
		}
	}

	seedSeriesRange(t, client, base, a, jan1, feb1)
	if err := a.Remove(ctx, jan1, "o1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if count, err := a.CountSegments(ctx, "o1"); err != nil || count != 0 {
		t.Fatalf("CountSegments after Remove = %d, %v", count, err)
	}
	seedSeriesRange(t, client, base, a, jan1, feb1)
	if err := b.Remove(ctx, jan1, "o1"); err != nil {
		t.Fatalf("b Remove: %v", err)
	}
	if exists, err := a.Exists(ctx, jan1, "o1"); err != nil || exists {
		t.Fatalf("a's segment after b's Remove = %v, %v: b's guard must drop a's store", exists, err)
	}
}

func TestGenerationSelfHealPastTheUnpackLimit(t *testing.T) {
	scenarioGenerationSelfHealPastTheUnpackLimit(t, miniClient)
}

// Lua's unpack takes a few thousand values: a self-heal of more dangling members than that must
// still remove them all.
func scenarioGenerationSelfHealPastTheUnpackLimit(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, "posts:of:%s", indexTTL)
	if err := sorted.SetGeneration("g"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	const n = 9000
	pipe := client.TxPipeline()
	if err := sorted.SetExpiration(ctx, pipe, "o1"); err != nil { // stamps the marker
		t.Fatalf("SetExpiration: %v", err)
	}
	members := make([]redis.Z, 0, n)
	for i := 0; i < n; i++ {
		// Never written to Base: every member dangles. Added raw: n stamp scripts are slow on
		// miniredis, and the stamp is the only part of a seed this scenario needs.
		members = append(members, redis.Z{Score: float64(i), Member: fmt.Sprint("m", i)})
	}
	pipe.ZAdd(ctx, "{posts:of:o1}", members...)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	items, dangling, err := sorted.Fetch(Descending).WithParams("o1").ExecReport(ctx)
	if err != nil || len(items) != 0 || dangling != n {
		t.Fatalf("ExecReport = %d items, %d dangling, %v", len(items), dangling, err)
	}
	if left := client.ZCard(ctx, "{posts:of:o1}").Val(); left != 0 {
		t.Fatalf("self-heal left %d of %d dangling members", left, n)
	}
}

func TestGenerationSelfHealKeepsAnotherGenerationsValues(t *testing.T) {
	scenarioGenerationSelfHealKeepsAnotherGenerationsValues(t, miniClient)
}

// The recommended configuration — Base generation = shape, index generation = build — during an
// overlap: the older build rewrites a shared entity in its shape. The newer build's read counts
// that member as dangling but keeps it, and the list is whole again once the entity is rewritten.
func scenarioGenerationSelfHealKeepsAnotherGenerationsValues(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	newerBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	olderBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	if err := newerBase.SetGeneration("shape-2"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	if err := olderBase.SetGeneration("shape-1"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	feed := mustTimeline[*Post](t, client, newerBase, feedKeyFormat, 10, Descending, indexTTL)
	list := mustSorted[*Post](t, client, newerBase, "list:%s", indexTTL)
	for _, index := range []interface{ SetGeneration(string) error }{feed, list} {
		if err := index.SetGeneration("commit-2"); err != nil {
			t.Fatalf("SetGeneration: %v", err)
		}
	}

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]*Post, 4)
	pipe := client.TxPipeline()
	for i := range rows {
		rows[i] = newPost(t, fmt.Sprint("r", i), start.Add(time.Duration(len(rows)-i)*time.Minute))
		_ = newerBase.WithPipeline(pipe).Set(ctx, rows[i])
		_ = feed.IngestItem(ctx, pipe, rows[i], true, "u1")
		_ = list.IngestItem(ctx, pipe, rows[i], true, "l1")
	}
	_ = feed.MarkLastPage(ctx, pipe, "u1")
	_ = feed.SetExpiration(ctx, pipe, "u1")
	_ = list.SetExpiration(ctx, pipe, "l1")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if err := olderBase.Set(ctx, rows[1]); err != nil { // the older build's seed of another view
		t.Fatalf("Set: %v", err)
	}
	out := feed.FetchFrom(Cursor{}).WithParams("u1").Exec(ctx)
	if out.Error() != nil || len(out.Items()) != 3 || out.Dangling() != 1 {
		t.Fatalf("feed read = %d items, %d dangling, %v; want 3 and 1", len(out.Items()), out.Dangling(), out.Error())
	}
	items, dangling, err := list.Fetch(Descending).WithParams("l1").ExecReport(ctx)
	if err != nil || len(items) != 3 || dangling != 1 {
		t.Fatalf("list read = %d items, %d dangling, %v", len(items), dangling, err)
	}
	if count := client.ZCard(ctx, "{feed:u1}").Val() + client.ZCard(ctx, "{list:l1}").Val(); count != 8 {
		t.Fatalf("the indexes hold %d members after the reads, want all 8: the item exists", count)
	}

	if err := newerBase.Set(ctx, rows[1]); err != nil {
		t.Fatalf("Set: %v", err)
	}
	out = feed.FetchFrom(Cursor{}).WithParams("u1").Exec(ctx)
	if out.Error() != nil || len(out.Items()) != 4 || out.Dangling() != 0 {
		t.Fatalf("feed read after the rewrite = %d items, %d dangling, %v", len(out.Items()), out.Dangling(), out.Error())
	}
}

func TestGenerationSelfHealLeavesAForeignIndexAlone(t *testing.T) {
	scenarioGenerationSelfHealLeavesAForeignIndexAlone(t, miniClient)
}

// A read's self-heal runs after its read. When another generation reseeded the index in between,
// the heal must leave that seed alone.
func scenarioGenerationSelfHealLeavesAForeignIndexAlone(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "s", "a")
	b := newGenerationStack(t, client, "s", "b")
	seedGenerationSorted(t, client, b, "o1", newPost(t, "1", time.Now().UTC()))

	keys, err := a.sorted.sortedSetClient.generationKeys([]string{"o1"})
	if err != nil {
		t.Fatalf("generationKeys: %v", err)
	}
	healed, err := generationHealScript.Eval(ctx, client, keys, "gone", "a").Int64()
	if err != nil || healed != 0 {
		t.Fatalf("a's heal on b's index = %d, %v; want 0", healed, err)
	}
	if !keyExists(t, client, "{posts:of:o1}") || client.Get(ctx, "{posts:of:o1}:gen").Val() != "b" {
		t.Fatal("a's heal dropped b's index")
	}
}

func TestGenerationRemoveItemOnAColdIndex(t *testing.T) {
	scenarioGenerationRemoveItemOnAColdIndex(t, miniClient)
}

// RemoveItem on an index that is not there at all is no error, with or without a generation; on
// another generation's index it drops it and reports ErrNotIngested.
func scenarioGenerationRemoveItemOnAColdIndex(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	post := newPost(t, "p", time.Now().UTC())
	for _, generation := range []string{"", "g"} {
		sorted := mustSorted[*Post](t, client, base, "posts:of:%s", indexTTL)
		timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)
		if err := sorted.SetGeneration(generation); err != nil {
			t.Fatalf("SetGeneration: %v", err)
		}
		if err := timeline.SetGeneration(generation); err != nil {
			t.Fatalf("SetGeneration: %v", err)
		}
		if err := sorted.RemoveItem(ctx, post, "cold"); err != nil {
			t.Fatalf("generation %q: Sorted.RemoveItem on a cold index = %v", generation, err)
		}
		if err := timeline.RemoveItem(ctx, post, "cold"); err != nil {
			t.Fatalf("generation %q: Timeline.RemoveItem on a cold index = %v", generation, err)
		}
		pipe := client.Pipeline()
		if err := sorted.WithPipeline(pipe).RemoveItem(ctx, post, "cold"); err != nil {
			t.Fatalf("WithPipeline RemoveItem: %v", err)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}

	a := newGenerationStack(t, client, "s", "a")
	b := newGenerationStack(t, client, "s", "b")
	seedGenerationSorted(t, client, b, "o1", post)
	if err := a.sorted.RemoveItem(ctx, post, "o1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("RemoveItem on another generation's index = %v, want ErrNotIngested", err)
	}
	if keyExists(t, client, "{posts:of:o1}") {
		t.Fatal("the foreign index survived")
	}
}

func TestTimelineRequiresSeedingWithAGenerationWritesNothing(t *testing.T) {
	scenarioTimelineRequiresSeedingWithAGenerationWritesNothing(t, miniClient)
}

// The deprecated check stays a check with a generation: one MULTI/EXEC, no marker written.
func scenarioTimelineRequiresSeedingWithAGenerationWritesNothing(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)
	if err := timeline.SetGeneration("g"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	needs, err := timeline.RequiresSeeding(ctx, 0, "u1")
	if err != nil || !needs {
		t.Fatalf("RequiresSeeding on a cold index = %v, %v", needs, err)
	}
	if keys := client.Keys(ctx, "*").Val(); len(keys) != 0 {
		t.Fatalf("the check wrote %v", keys)
	}

	pipe := client.TxPipeline()
	post := newPost(t, "p", time.Now().UTC())
	_ = base.WithPipeline(pipe).Set(ctx, post)
	_ = timeline.IngestItem(ctx, pipe, post, true, "u1")
	_ = timeline.MarkLastPage(ctx, pipe, "u1")
	_ = timeline.SetExpiration(ctx, pipe, "u1")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if needs, err := timeline.RequiresSeeding(ctx, 1, "u1"); err != nil || needs {
		t.Fatalf("RequiresSeeding on a seeded index = %v, %v", needs, err)
	}
}

func TestSortedFetchAnswersErrNotSeededAfterADrop(t *testing.T) {
	scenarioSortedFetchAnswersErrNotSeededAfterADrop(t, miniClient)
}

// The documented Sorted flow checks, seeds and reads in separate round trips. Another generation
// dropping the index in between must not make the read answer an empty list.
func scenarioSortedFetchAnswersErrNotSeededAfterADrop(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "s", "a")
	b := newGenerationStack(t, client, "s", "b")
	seedGenerationSorted(t, client, a, "o1", newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC()))
	if requiresSeeding(t, a.sorted, "o1") {
		t.Fatal("a's seed must be warm")
	}
	if _, err := b.sorted.Count(ctx, "o1"); err != nil {
		t.Fatalf("Count: %v", err)
	}
	items, dangling, err := a.sorted.Fetch(Descending).WithParams("o1").ExecReport(ctx)
	if !errors.Is(err, ErrNotSeeded) || len(items) != 0 || dangling != 0 {
		t.Fatalf("a Fetch after b's drop = %d items, %d dangling, %v; want ErrNotSeeded", len(items), dangling, err)
	}
	if _, err := a.sorted.Fetch(Descending).WithParams("o1").WithRankRange(0, 0).Exec(ctx); !errors.Is(err, ErrNotSeeded) {
		t.Fatalf("a rank-range Fetch of a cold index = %v, want ErrNotSeeded", err)
	}

	// A list confirmed empty is this generation's: it reads as empty, not as ErrNotSeeded.
	pipe := client.TxPipeline()
	_ = a.sorted.MarkEmpty(ctx, pipe, "empty")
	_ = a.sorted.SetExpiration(ctx, pipe, "empty")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if items, err := a.sorted.Fetch(Descending).WithParams("empty").Exec(ctx); err != nil || len(items) != 0 {
		t.Fatalf("Fetch of a list marked empty = %v, %v", items, err)
	}
}

func TestTimelineExtend(t *testing.T) {
	scenarioTimelineExtend(t, miniClient)
}

// Extend appends a top-up only to the index whose tail it continues: not to an index another
// generation dropped, nor to one that was purged and seeded again from its head.
func scenarioTimelineExtend(t *testing.T, newClient clientFactory) {
	for _, generation := range []string{"", "a"} {
		t.Run("generation="+generation, func(t *testing.T) {
			client := newClient(t)
			ctx := context.Background()
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 5, Descending, indexTTL)
			if err := timeline.SetGeneration(generation); err != nil {
				t.Fatalf("SetGeneration: %v", err)
			}
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rows := make([]*Post, 12) // rows[0] is the newest
			for i := range rows {
				rows[i] = newPost(t, fmt.Sprint("r", i), start.Add(time.Duration(len(rows)-i)*time.Minute))
			}
			headSeed := func(n int) {
				pipe := client.TxPipeline()
				_ = timeline.UnmarkLastPage(ctx, pipe, "u1")
				for _, row := range rows[:n] {
					_ = base.WithPipeline(pipe).Set(ctx, row)
					_ = timeline.IngestItem(ctx, pipe, row, true, "u1")
				}
				_ = timeline.HasData(ctx, pipe, "u1")
				_ = timeline.SetExpiration(ctx, pipe, "u1")
				if _, err := pipe.Exec(ctx); err != nil {
					t.Fatalf("Exec: %v", err)
				}
			}
			extend := func(after Cursor, from, to int, end bool) bool {
				pipe := client.TxPipeline()
				for _, row := range rows[from:to] {
					_ = base.WithPipeline(pipe).Set(ctx, row)
				}
				extension, err := timeline.Extend(ctx, pipe, after, rows[from:to], end, "u1")
				if err != nil {
					t.Fatalf("Extend: %v", err)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					t.Fatalf("Exec: %v", err)
				}
				applied, err := extension.Applied()
				if err != nil {
					t.Fatalf("Applied: %v", err)
				}
				return applied
			}
			titles := func() []string {
				out := timeline.FetchAll().WithParams("u1").Exec(ctx)
				if out.Error() != nil {
					t.Fatalf("FetchAll: %v", out.Error())
				}
				ids := make([]string, 0, len(out.Items()))
				for _, item := range out.Items() {
					ids = append(ids, item.Title)
				}
				return ids
			}

			headSeed(5)
			tail, ok, err := timeline.Tail(ctx, "u1")
			if err != nil || !ok || tail.Member != rows[4].GetRandId() {
				t.Fatalf("Tail = %v, %v, %v", tail, ok, err)
			}
			if !extend(tail, 5, 8, false) {
				t.Fatal("an extension from the tail must apply")
			}
			if got := strings.Join(titles(), ","); got != "r0,r1,r2,r3,r4,r5,r6,r7" {
				t.Fatalf("after the extension: %s", got)
			}
			if extend(tail, 8, 10, false) {
				t.Fatal("an extension from a tail the index no longer ends at must not apply")
			}

			// The index is purged and seeded again from its head while a top-up was in flight.
			if err := timeline.Purge(ctx, "u1"); err != nil {
				t.Fatalf("Purge: %v", err)
			}
			headSeed(3)
			stale, _, _ := timeline.Tail(ctx, "u1")
			if extend(tail, 5, 8, false) {
				t.Fatal("an extension of the index's previous life must not apply")
			}
			if got := strings.Join(titles(), ","); got != "r0,r1,r2" {
				t.Fatalf("the reseeded index was changed: %s", got)
			}
			if !extend(stale, 3, 12, true) {
				t.Fatal("an extension to the end must apply")
			}
			if end, err := timeline.ReachedEnd(ctx, "u1"); err != nil || !end {
				t.Fatalf("ReachedEnd after the last extension = %v, %v", end, err)
			}
			if got := len(titles()); got != len(rows) {
				t.Fatalf("the index holds %d rows, want %d", got, len(rows))
			}

			if generation == "" {
				return
			}
			// Another generation reads the feed between the tail and the top-up: its check drops
			// the index, and the top-up must not stamp what is left with rows from the middle.
			other := mustTimeline[*Post](t, client, base, feedKeyFormat, 5, Descending, indexTTL)
			if err := other.SetGeneration("b"); err != nil {
				t.Fatalf("SetGeneration: %v", err)
			}
			if err := timeline.Purge(ctx, "u1"); err != nil {
				t.Fatalf("Purge: %v", err)
			}
			headSeed(5)
			tail, _, _ = timeline.Tail(ctx, "u1")
			if out := other.FetchFrom(Cursor{}).WithParams("u1").Exec(ctx); out.Error() != nil {
				t.Fatalf("other read: %v", out.Error())
			}
			if extend(tail, 5, 11, false) {
				t.Fatal("an extension onto an index another generation dropped must not apply")
			}
			for _, key := range []string{"{feed:u1}", "{feed:u1}:gen", "{feed:u1}:lastpage"} {
				if keyExists(t, client, key) {
					t.Fatalf("the refused extension left %s", key)
				}
			}

			// The other generation seeds the same head: the tail matches, the generation does not.
			pipe := client.TxPipeline()
			for _, row := range rows[:5] {
				_ = other.IngestItem(ctx, pipe, row, true, "u1")
			}
			_ = other.SetExpiration(ctx, pipe, "u1")
			if _, err := pipe.Exec(ctx); err != nil {
				t.Fatalf("Exec: %v", err)
			}
			if extend(tail, 5, 11, true) {
				t.Fatal("an extension onto another generation's index must not apply, even at its tail")
			}
			if count := client.ZCard(ctx, "{feed:u1}").Val(); count != 5 || keyExists(t, client, "{feed:u1}:lastpage") {
				t.Fatalf("the other generation's index was changed: %d members", count)
			}
		})
	}
}

func TestBaseWithoutGenerationMissesAPrefixedValue(t *testing.T) {
	scenarioBaseWithoutGenerationMissesAPrefixedValue(t, miniClient)
}

// A Base without a generation that meets a value with a generation prefix — a build that turned
// generations on for this key format — reads it as missing instead of failing the whole read.
func scenarioBaseWithoutGenerationMissesAPrefixedValue(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	newer := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	if err := newer.SetGeneration("shape1"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	older := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	p1, p2 := newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC())
	if err := older.Set(ctx, p1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := newer.Set(ctx, p2); err != nil {
		t.Fatalf("Set: %v", err)
	}
	items, err := older.GetMany(ctx, []string{p1.GetRandId(), p2.GetRandId()})
	if err != nil || len(items) != 1 || items[p1.GetRandId()] == nil {
		t.Fatalf("GetMany = %v, %v; want p1 only", items, err)
	}
	if _, err := older.Get(ctx, p2.GetRandId()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get of a prefixed value without a generation = %v, want ErrNotFound", err)
	}
}

func TestSetGenerationWhileInUse(t *testing.T) {
	scenarioSetGenerationWhileInUse(t, miniClient)
}

// SetGeneration may move a Base and an index to a new generation while other goroutines use them:
// what the old generation wrote is then a miss and a cold index, and nothing races (go test -race).
func scenarioSetGenerationWhileInUse(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	stack := newGenerationStack(t, client, "s1", "b1")
	post := newPost(t, "p", time.Now().UTC())
	seedGenerationSorted(t, client, stack, "o1", post)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = stack.posts.Get(ctx, post.GetRandId())
				_, _, _ = stack.sorted.Fetch(Descending).WithParams("o1").ExecReport(ctx)
				_, _ = stack.sorted.RequiresSeeding(ctx, "o1")
			}
		}()
	}
	for i := 0; i < 20; i++ {
		_ = stack.posts.SetGeneration(fmt.Sprintf("s%d", i%2+1))
		_ = stack.sorted.SetGeneration(fmt.Sprintf("b%d", i%2+1))
	}
	close(stop)
	wg.Wait()

	if err := stack.posts.SetGeneration("s3"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	if err := stack.sorted.SetGeneration("b3"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	if _, err := stack.posts.Get(ctx, post.GetRandId()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after the move = %v, want ErrNotFound", err)
	}
	if !requiresSeeding(t, stack.sorted, "o1") {
		t.Fatal("the index of the previous generation must need seeding")
	}
	seedGenerationSorted(t, client, stack, "o1", post)
	if items, err := stack.sorted.Fetch(Descending).WithParams("o1").Exec(ctx); err != nil || len(items) != 1 {
		t.Fatalf("Fetch after the reseed = %v, %v", items, err)
	}
	if got := client.Get(ctx, "{posts:of:o1}:gen").Val(); got != "b3" {
		t.Fatalf("marker = %q, want b3", got)
	}
}
