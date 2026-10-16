package redifu

// B–D of the BE plan §8: TimeSeries took no item into a range seeded while it was empty
// (B), let its index expire before its segments (C), and read int64 scores as
// milliseconds whatever they counted (D).

import (
	"context"
	"errors"
	"testing"
	"time"
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
