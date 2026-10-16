package redifu

// J — which items AddItem places into a Timeline. The index holds a prefix of the data in
// the timeline's order, ending at its tail: an item enters when it sorts at or before the
// tail, and anywhere once :lastpage says the index reaches the end of the data.

import (
	"context"
	"errors"
	"testing"
	"time"
)

type ingestCase struct {
	name string
	post *Post
	want bool // entered the index
}

// ingestFixture seeds five posts one minute apart, at 00:00 .. 00:04, under randIds
// "m0" .. "m4", plus ties at both ends so that the member order matters.
func ingestFixture(t *testing.T, newClient clientFactory, direction string, lastPage bool) (*Timeline[*Post], context.Context) {
	t.Helper()
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 20, direction, indexTTL)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	posts := make([]*Post, 0, 5)
	for i, id := range []string{"m0", "m1", "m2", "m3", "m4"} {
		posts = append(posts, postWithId(t, id, start.Add(time.Duration(i)*time.Minute)))
	}
	seedTimeline(t, client, base, timeline, "u1", posts...)

	if lastPage {
		pipe := client.Pipeline()
		if err := timeline.MarkLastPage(ctx, pipe, "u1"); err != nil {
			t.Fatalf("MarkLastPage: %v", err)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}
	return timeline, ctx
}

func runIngestCases(t *testing.T, newClient clientFactory, direction string, lastPage bool, cases []ingestCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			timeline, ctx := ingestFixture(t, newClient, direction, lastPage)

			err := timeline.AddItem(ctx, tc.post, "u1")
			entered := err == nil
			if err != nil && !errors.Is(err, ErrNotIngested) {
				t.Fatalf("AddItem: %v", err)
			}
			if entered != tc.want {
				t.Fatalf("AddItem entered = %v (err %v), want %v", entered, err, tc.want)
			}

			present := false
			for _, item := range timeline.FetchAll().WithParams("u1").Exec(ctx).Items() {
				if item.GetRandId() == tc.post.GetRandId() {
					present = true
				}
			}
			if present != tc.want {
				t.Fatalf("index holds the item = %v, want %v", present, tc.want)
			}
		})
	}
}

func TestTimelineIngestDescending(t *testing.T) { scenarioTimelineIngestDescending(t, miniClient) }

func scenarioTimelineIngestDescending(t *testing.T, newClient clientFactory) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tail := start // m0 is the oldest, the tail of a descending timeline

	t.Run("window ends at the tail", func(t *testing.T) {
		runIngestCases(t, newClient, Descending, false, []ingestCase{
			{"newer than the head", postWithId(t, "new", start.Add(time.Hour)), true},
			{"between members", postWithId(t, "mid", start.Add(90*time.Second)), true},
			{"the tail itself again", postWithId(t, "m0", tail), true},
			// ZREVRANGE orders equal scores by member descending: "m1" sorts before m0,
			// "a" after it.
			{"tied with the tail, sorting before it", postWithId(t, "m1x", tail), true},
			{"tied with the tail, sorting after it", postWithId(t, "a", tail), false},
			{"older than the tail", postWithId(t, "old", start.Add(-time.Hour)), false},
		})
	})

	t.Run("lastpage: the window is everything", func(t *testing.T) {
		runIngestCases(t, newClient, Descending, true, []ingestCase{
			{"newer than the head", postWithId(t, "new", start.Add(time.Hour)), true},
			{"tied with the tail, sorting after it", postWithId(t, "a", tail), true},
			// The case J is about: a row moving into a status timeline that already
			// holds every row of that status.
			{"older than the tail", postWithId(t, "old", start.Add(-time.Hour)), true},
		})
	})
}

func TestTimelineIngestAscending(t *testing.T) { scenarioTimelineIngestAscending(t, miniClient) }

func scenarioTimelineIngestAscending(t *testing.T, newClient clientFactory) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tail := start.Add(4 * time.Minute) // m4 is the newest, the tail of an ascending timeline

	t.Run("window ends at the tail", func(t *testing.T) {
		runIngestCases(t, newClient, Ascending, false, []ingestCase{
			// The head of an ascending timeline is its oldest item: an even older one
			// sorts before it and belongs. The original script refused every item here
			// unless a page marker was set.
			{"older than the head", postWithId(t, "old", start.Add(-time.Hour)), true},
			{"between members", postWithId(t, "mid", start.Add(90*time.Second)), true},
			{"the tail itself again", postWithId(t, "m4", tail), true},
			// ZRANGE orders equal scores by member ascending: "a" sorts before m4, "z"
			// after it.
			{"tied with the tail, sorting before it", postWithId(t, "a", tail), true},
			{"tied with the tail, sorting after it", postWithId(t, "z", tail), false},
			{"newer than the tail", postWithId(t, "new", start.Add(time.Hour)), false},
		})
	})

	t.Run("lastpage: the window is everything", func(t *testing.T) {
		runIngestCases(t, newClient, Ascending, true, []ingestCase{
			{"tied with the tail, sorting after it", postWithId(t, "z", tail), true},
			{"newer than the tail", postWithId(t, "new", start.Add(time.Hour)), true},
		})
	})
}

func TestTimelineIngestIntoAnEmptyIndex(t *testing.T) {
	scenarioTimelineIngestIntoAnEmptyIndex(t, miniClient)
}

func scenarioTimelineIngestIntoAnEmptyIndex(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)

	for _, direction := range []string{Descending, Ascending} {
		timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 20, direction, indexTTL)
		user := "empty-" + direction

		// A :lastpage left behind by an index that no longer exists does not make an
		// empty index "complete": nothing is added until the head is seeded.
		pipe := client.Pipeline()
		if err := timeline.MarkLastPage(ctx, pipe, user); err != nil {
			t.Fatalf("MarkLastPage: %v", err)
		}
		if err := timeline.MarkEmpty(ctx, pipe, user); err != nil {
			t.Fatalf("MarkEmpty: %v", err)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}

		post := newPost(t, "first", time.Now())
		if err := timeline.AddItem(ctx, post, user); !errors.Is(err, ErrNotIngested) {
			t.Fatalf("%s: AddItem into an empty index = %v, want ErrNotIngested", direction, err)
		}
		if count, _ := timeline.Count(ctx, user); count != 0 {
			t.Fatalf("%s: the empty index holds %d members", direction, count)
		}
		// The confirmed-empty marker is gone, so the next read seeds the head — and
		// finds this item in the database.
		if end, _ := timeline.ReachedEnd(ctx, user); end {
			t.Fatalf("%s: the index still claims to be complete after an item arrived", direction)
		}
	}
}

func TestTimelineIngestKeepsTheFirstPageMarkerRule(t *testing.T) {
	for _, direction := range []string{Descending, Ascending} {
		t.Run(direction, func(t *testing.T) {
			ctx := context.Background()
			_, client := newTestRedis(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 3, direction, indexTTL)

			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			seedTimeline(t, client, base, timeline, "u1",
				postWithId(t, "a", start),
				postWithId(t, "b", start.Add(time.Minute)),
			)
			pipe := client.Pipeline()
			if err := timeline.MarkFirstPage(ctx, pipe, "u1"); err != nil {
				t.Fatalf("MarkFirstPage: %v", err)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				t.Fatalf("Exec: %v", err)
			}

			// Inside the window for both directions: between the two members.
			between := start.Add(30 * time.Second)

			// Two members, a page of three: the marker stays.
			if err := timeline.AddItem(ctx, postWithId(t, "c", between), "u1"); err != nil {
				t.Fatalf("AddItem: %v", err)
			}
			if first, _ := timeline.IsFirstPage(ctx, "u1"); !first {
				t.Fatal("the first-page marker was dropped before the index outgrew a page")
			}

			// A full page, and one more: the marker goes, as it always did.
			if err := timeline.AddItem(ctx, postWithId(t, "d", between), "u1"); err != nil {
				t.Fatalf("AddItem: %v", err)
			}
			if first, _ := timeline.IsFirstPage(ctx, "u1"); first {
				t.Fatal("the first-page marker survived the index outgrowing a page")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// J through the documented flow: the BE's status timeline
// ---------------------------------------------------------------------------

func TestAnOldItemJoinsAFullySeededTimeline(t *testing.T) {
	scenarioAnOldItemJoinsAFullySeededTimeline(t, miniClient)
}

func scenarioAnOldItemJoinsAFullySeededTimeline(t *testing.T, newClient clientFactory) {
	store, ctx := newFeedStore(t, newClient(t), Descending, 30)
	store.walk(ctx) // loads every row and marks the end

	// A booking from long ago moves into this status: the row joins the table and the
	// write path adds it to the index.
	old := newPost(t, "moved back", time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC))
	store.table.rows = append(store.table.rows, old)
	if err := store.timeline.AddItem(ctx, old, store.user); err != nil {
		t.Fatalf("AddItem into a timeline that holds every row = %v, want it ingested", err)
	}

	before := store.table.queries
	equalIds(t, "walk", store.walk(ctx), store.table.order(t, store.timeline))
	if store.table.queries != before {
		t.Fatalf("the walk ran %d queries — the index should already hold the moved row", store.table.queries-before)
	}
}

func TestAnOldItemWaitsForTheTopUpOfAPartialTimeline(t *testing.T) {
	scenarioAnOldItemWaitsForTheTopUpOfAPartialTimeline(t, miniClient)
}

func scenarioAnOldItemWaitsForTheTopUpOfAPartialTimeline(t *testing.T, newClient clientFactory) {
	store, ctx := newFeedStore(t, newClient(t), Descending, 30)
	if _, err := store.page(ctx, Cursor{}); err != nil { // the head only: 21 of 30 rows
		t.Fatalf("first page: %v", err)
	}

	// The same move into a timeline whose index stops short of the end: rows between its
	// tail and the moved row are not loaded yet, so the row waits for the top-up.
	old := newPost(t, "moved back", time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC))
	store.table.rows = append(store.table.rows, old)
	if err := store.timeline.AddItem(ctx, old, store.user); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("AddItem past the tail of a partial index = %v, want ErrNotIngested", err)
	}

	equalIds(t, "walk", store.walk(ctx), store.table.order(t, store.timeline))
}
