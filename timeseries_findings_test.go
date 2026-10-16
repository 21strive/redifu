package redifu

// B–D of the BE plan §8: TimeSeries took no item into a range seeded while it was empty
// (B), let its index expire before its segments (C), and read int64 scores as
// milliseconds whatever they counted (D).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Booking is a row whose time is a time.Time, like the createdAt of a Post.
type Booking struct {
	*Record
	Name    string    `json:"name"`
	StartAt time.Time `json:"startAt"`
}

// BookingSeconds carries its time as Unix seconds, like bookings.start_time.
type BookingSeconds struct {
	*Record
	StartTime int64 `json:"startTime"`
}

func newBooking(t *testing.T, name string, at time.Time) *Booking {
	t.Helper()
	booking := &Booking{}
	InitRecord(booking)
	booking.Name = name
	booking.StartAt = at
	return booking
}

var (
	jan1 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb1 = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	mar1 = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
)

func bookingSeries(t *testing.T, newClient clientFactory) (*TimeSeries[*Booking], *Base[*Booking], context.Context) {
	t.Helper()
	client := newClient(t)
	base := mustBase[*Booking](t, client, "booking:%s", baseTTL)
	series := mustTimeSeries[*Booking](t, client, base, "cal:%s", indexTTL)
	if err := series.SetSortingReference("StartAt"); err != nil {
		t.Fatalf("SetSortingReference: %v", err)
	}
	return series, base, context.Background()
}

// seedEmptyRange records a range as seeded with no rows in it, the way the documented
// seeder does when the database returns nothing.
func seedEmptyRange(t *testing.T, series *TimeSeries[*Booking], from, to time.Time, owner string) {
	t.Helper()
	ctx := context.Background()
	pipe := series.redis.Pipeline()
	if err := series.AddSegment(ctx, pipe, from, to, owner); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

// ---------------------------------------------------------------------------
// B — the first item of a range seeded while it was empty
// ---------------------------------------------------------------------------

func TestTimeSeriesTakesTheFirstItemOfAnEmptySeededRange(t *testing.T) {
	scenarioTimeSeriesTakesTheFirstItemOfAnEmptySeededRange(t, miniClient)
}

func scenarioTimeSeriesTakesTheFirstItemOfAnEmptySeededRange(t *testing.T, newClient clientFactory) {
	series, _, ctx := bookingSeries(t, newClient)
	seedEmptyRange(t, series, jan1, feb1, "owner1")

	first := newBooking(t, "first", jan1.Add(14*24*time.Hour))
	if err := series.AddItem(ctx, first, "owner1"); err != nil {
		t.Fatalf("AddItem into a seeded, empty January = %v, want it ingested", err)
	}

	items, needsSeed, err := series.Fetch(jan1, feb1).WithParams("owner1").Exec(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if needsSeed || len(items) != 1 || items[0].GetRandId() != first.GetRandId() {
		t.Fatalf("Fetch January = %d items, needsSeed = %v; want the first booking and no seed", len(items), needsSeed)
	}

	// The ZADD created the sorted set, so it carried the TTL with it.
	ttl, err := series.redis.TTL(ctx, indexKey("cal:owner1")).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > indexTTL {
		t.Fatalf("index TTL = %v, want it set (0 < ttl <= %v)", ttl, indexTTL)
	}
}

func TestTimeSeriesFirstItemThroughTheCallersPipeline(t *testing.T) {
	ctx := context.Background()
	series, _, _ := bookingSeries(t, miniClient)
	seedEmptyRange(t, series, jan1, feb1, "owner1")

	first := newBooking(t, "first", jan1.Add(time.Hour))
	pipe := series.redis.Pipeline()
	if err := series.WithPipeline(pipe).AddItem(ctx, first, "owner1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	// Nothing may have reached Redis yet: the caller owns the pipeline.
	if count, _ := series.Count(ctx, "owner1"); count != 0 {
		t.Fatalf("index holds %d items before Exec, want 0", count)
	}
	if exists, _ := series.sorted.baseClient.Exists(ctx, first.GetRandId()); exists {
		t.Fatal("the item was written to Base before the caller executed the pipeline")
	}

	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if count, _ := series.Count(ctx, "owner1"); count != 1 {
		t.Fatalf("index holds %d items after Exec, want 1", count)
	}
}

func TestTimeSeriesStoresAnUncoveredItemInBaseOnly(t *testing.T) {
	ctx := context.Background()
	series, base, _ := bookingSeries(t, miniClient)
	seedEmptyRange(t, series, jan1, feb1, "owner1")

	march := newBooking(t, "march", mar1.Add(time.Hour))
	if err := series.AddItem(ctx, march, "owner1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("AddItem outside every segment = %v, want ErrNotIngested", err)
	}
	if count, _ := series.Count(ctx, "owner1"); count != 0 {
		t.Fatalf("index holds %d items, want 0", count)
	}

	// As AddItem does on every other collection: the entity is stored, only not indexed.
	stored, err := base.Get(ctx, march.GetRandId())
	if err != nil || stored.Name != "march" {
		t.Fatalf("Base.Get = %v, %v; want the item stored", stored, err)
	}

	// A series with no segments at all is not seeded.
	other := newBooking(t, "other", jan1.Add(time.Hour))
	if err := series.AddItem(ctx, other, "owner2"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("AddItem into an unseeded series = %v, want ErrNotIngested", err)
	}
}

// ---------------------------------------------------------------------------
// C — the index expires with its segments, not before them
// ---------------------------------------------------------------------------

// seedRange is the documented TimeSeries seeder for one gap: the rows, then the segment
// that records the range. setExpiration replays the previous documentation, which applied
// the index TTL only with the first non-empty seed.
func seedRange(t *testing.T, series *TimeSeries[*Booking], base *Base[*Booking], from, to time.Time, owner string, setExpiration bool, rows ...*Booking) {
	t.Helper()
	ctx := context.Background()
	pipe := series.redis.Pipeline()
	for _, row := range rows {
		if err := base.WithPipeline(pipe).Set(ctx, row); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := series.IngestItem(ctx, pipe, row, owner); err != nil {
			t.Fatalf("IngestItem: %v", err)
		}
	}
	if setExpiration {
		if err := series.SetExpiration(ctx, pipe, owner); err != nil {
			t.Fatalf("SetExpiration: %v", err)
		}
	}
	if err := series.AddSegment(ctx, pipe, from, to, owner); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

func TestTimeSeriesIndexOutlivesItsSegments(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "documented seeder"
		if legacy {
			name = "seeder that sets the index TTL on its first seed only"
		}
		t.Run(name, func(t *testing.T) {
			server, client := newTestRedis(t)
			series, base, ctx := bookingSeries(t, func(*testing.T) redis.UniversalClient { return client })

			seedRange(t, series, base, jan1, feb1, "owner1", legacy, newBooking(t, "jan", jan1.Add(48*time.Hour)))
			server.FastForward(25 * time.Hour)
			seedRange(t, series, base, feb1, mar1, "owner1", false, newBooking(t, "feb", feb1.Add(48*time.Hour)))

			// 49 hours after the first seed: past its original index TTL, inside the TTL
			// the second AddSegment gave both keys.
			server.FastForward(24 * time.Hour)

			items, needsSeed, err := series.Fetch(jan1, mar1).WithParams("owner1").Exec(ctx)
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if needsSeed || len(items) != 2 {
				t.Fatalf("Fetch January–February = %d items, needsSeed = %v; want both bookings — the index expired before its segments", len(items), needsSeed)
			}

			// And once both expire, the range reads as a gap again instead of as empty.
			server.FastForward(indexTTL)
			items, needsSeed, err = series.Fetch(jan1, mar1).WithParams("owner1").Exec(ctx)
			if err != nil || !needsSeed || len(items) != 0 {
				t.Fatalf("after both expired: %d items, needsSeed = %v, %v; want a gap to seed", len(items), needsSeed, err)
			}
		})
	}
}

func TestTimeSeriesAddSegmentAlignsTheTwoTTLs(t *testing.T) {
	scenarioTimeSeriesAddSegmentAlignsTheTwoTTLs(t, miniClient)
}

func scenarioTimeSeriesAddSegmentAlignsTheTwoTTLs(t *testing.T, newClient clientFactory) {
	series, base, ctx := bookingSeries(t, newClient)
	seedRange(t, series, base, jan1, feb1, "owner1", false, newBooking(t, "jan", jan1.Add(time.Hour)))

	indexTTLLeft, err := series.redis.TTL(ctx, indexKey("cal:owner1")).Result()
	if err != nil {
		t.Fatalf("TTL index: %v", err)
	}
	segmentsTTLLeft, err := series.redis.TTL(ctx, "cal:owner1:segments").Result()
	if err != nil {
		t.Fatalf("TTL segments: %v", err)
	}
	if indexTTLLeft <= 0 || indexTTLLeft < segmentsTTLLeft {
		t.Fatalf("index TTL %v, segments TTL %v: the index must not expire first", indexTTLLeft, segmentsTTLLeft)
	}
}

func TestTimeSeriesIndexGetsATTLWhateverTheSeedOrder(t *testing.T) {
	ctx := context.Background()
	series, base, _ := bookingSeries(t, miniClient)

	// AddSegment enqueued before the rows: its EXPIRE runs before the sorted set exists.
	pipe := series.redis.Pipeline()
	if err := series.AddSegment(ctx, pipe, jan1, feb1, "owner1"); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	row := newBooking(t, "jan", jan1.Add(time.Hour))
	if err := base.WithPipeline(pipe).Set(ctx, row); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := series.IngestItem(ctx, pipe, row, "owner1"); err != nil {
		t.Fatalf("IngestItem: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	ttl, err := series.redis.TTL(ctx, indexKey("cal:owner1")).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("index TTL = %v (%v), want one — the index would never expire", ttl, err)
	}
}
