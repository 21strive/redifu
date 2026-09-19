package redifu

import (
	"context"
	"errors"
	"testing"
	"time"
)

func seedSeries(t *testing.T, series *TimeSeries[*Post], base *Base[*Post], from, to time.Time, keyParam string, posts ...*Post) {
	t.Helper()
	ctx := context.Background()
	pipe := series.sorted.client.Pipeline()
	for _, post := range posts {
		if err := base.WithPipeline(pipe).Set(ctx, post); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := series.IngestItem(ctx, pipe, post, keyParam); err != nil {
			t.Fatalf("IngestItem: %v", err)
		}
	}
	if err := series.AddSegment(ctx, pipe, from, to, keyParam); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if err := series.SetExpiration(ctx, pipe, keyParam); err != nil {
		t.Fatalf("SetExpiration: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

// A caller's pipeline used to be ignored here: the item was written through a second
// pipeline that executed immediately, behind the caller's back.
func TestTimeSeriesAddItemHonoursTheCallersPipeline(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	series := mustTimeSeries[*Post](t, client, base, feedKeyFormat, indexTTL)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	seedSeries(t, series, base, from, to, "u1", newPost(t, "seeded", from.Add(time.Minute)))

	fresh := newPost(t, "fresh", from.Add(2*time.Minute))

	pipe := client.Pipeline()
	if err := series.WithPipeline(pipe).AddItem(ctx, fresh, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	// Nothing may have reached Redis yet — the caller owns the pipeline.
	count, err := series.Count(ctx, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("index holds %d items before Exec, want 1 — the write escaped the caller's pipeline", count)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	count, err = series.Count(ctx, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("index holds %d items after Exec, want 2", count)
	}
}

func TestTimeSeriesAcceptsAnItemOnASegmentBoundary(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	series := mustTimeSeries[*Post](t, client, base, feedKeyFormat, indexTTL)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	seedSeries(t, series, base, from, to, "u1", newPost(t, "seeded", from.Add(time.Minute)))

	// Exactly on the lower bound of a range that plainly covers it.
	onBoundary := newPost(t, "on the boundary", from)
	if err := series.AddItem(ctx, onBoundary, "u1"); err != nil {
		t.Fatalf("AddItem on a segment boundary: %v", err)
	}

	count, err := series.Count(ctx, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("index holds %d items, want 2", count)
	}
}

func TestTimeSeriesReportsAnItemOutsideEverySegment(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	series := mustTimeSeries[*Post](t, client, base, feedKeyFormat, indexTTL)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	seedSeries(t, series, base, from, to, "u1", newPost(t, "seeded", from.Add(time.Minute)))

	outside := newPost(t, "next week", from.Add(7*24*time.Hour))
	if err := series.AddItem(ctx, outside, "u1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("AddItem = %v, want ErrNotIngested rather than a silent drop", err)
	}
}

func TestTimeSeriesSortingReferenceCanBeSet(t *testing.T) {
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	series := mustTimeSeries[*Post](t, client, base, feedKeyFormat, indexTTL)

	if err := series.SetSortingReference("UpdatedAt"); err != nil {
		t.Fatalf("SetSortingReference: %v", err)
	}
	if err := series.SetSortingReference("Nope"); err == nil {
		t.Fatal("an unknown sorting reference was accepted")
	}
}

func TestTimeSeriesPurgeAlsoDropsTheSeededRanges(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	series := mustTimeSeries[*Post](t, client, base, feedKeyFormat, indexTTL)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	post := newPost(t, "seeded", from.Add(time.Minute))
	seedSeries(t, series, base, from, to, "u1", post)

	if err := series.Purge(ctx, "u1"); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	segments, err := series.CountSegments(ctx, "u1")
	if err != nil {
		t.Fatalf("CountSegments: %v", err)
	}
	if segments != 0 {
		t.Fatalf("%d segments survived the purge — the series would claim to hold ranges it no longer has", segments)
	}

	count, errCount := series.Count(ctx, "u1")
	if errCount != nil {
		t.Fatalf("Count: %v", errCount)
	}
	if count != 0 {
		t.Fatalf("index holds %d items after a purge", count)
	}

	// Purge never deletes entities.
	if _, errGet := base.Get(ctx, post.GetRandId()); errGet != nil {
		t.Fatalf("Purge deleted an item key: %v", errGet)
	}

	// The range now reads as a gap again, so the next fetch reseeds it.
	_, needsSeeding, errFetch := series.Fetch(from, to).WithParams("u1").Exec(ctx)
	if errFetch != nil {
		t.Fatalf("Fetch: %v", errFetch)
	}
	if !needsSeeding {
		t.Fatal("a purged range did not report that it needs seeding")
	}
}

func TestTimeSeriesFetchReturnsSeededData(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	series := mustTimeSeries[*Post](t, client, base, feedKeyFormat, indexTTL)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	seedSeries(t, series, base, from, to, "u1",
		newPost(t, "first", from.Add(time.Minute)),
		newPost(t, "second", from.Add(2*time.Minute)),
	)

	items, needsSeeding, err := series.Fetch(from, to).WithParams("u1").Exec(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if needsSeeding {
		t.Fatal("a fully seeded range reported a gap")
	}
	if len(items) != 2 {
		t.Fatalf("fetched %d items, want 2", len(items))
	}
	if items[0].Title != "second" {
		t.Fatalf("first item = %q, want the most recent", items[0].Title)
	}
}
