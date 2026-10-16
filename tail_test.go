package redifu

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// N — Tail: where the index ends, in the timeline's own order
// ---------------------------------------------------------------------------

func TestTailIsTheLastMemberInTheTimelinesOrder(t *testing.T) {
	scenarioTailIsTheLastMemberInTheTimelinesOrder(t, miniClient)
}

func scenarioTailIsTheLastMemberInTheTimelinesOrder(t *testing.T, newClient clientFactory) {
	lowest := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	highest := lowest.Add(time.Hour)

	cases := []struct {
		direction string
		want      Cursor
	}{
		// Descending ends at the lowest score, and among the members tied there at the
		// smallest one: ZREVRANGE orders ties by member descending.
		{Descending, Cursor{Score: float64(lowest.UnixMilli()), Member: "B"}},
		// Ascending ends at the highest score and the largest member tied there.
		{Ascending, Cursor{Score: float64(highest.UnixMilli()), Member: "z"}},
	}

	for _, tc := range cases {
		t.Run(tc.direction, func(t *testing.T) {
			ctx := context.Background()
			client := newClient(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, tc.direction, indexTTL)

			// Ties at both ends, with members whose byte order differs from a
			// case-insensitive one.
			posts := []*Post{
				postWithId(t, "b", lowest),
				postWithId(t, "B", lowest),
				postWithId(t, "c", lowest),
				postWithId(t, "middle", lowest.Add(30*time.Minute)),
				postWithId(t, "Z", highest),
				postWithId(t, "z", highest),
				postWithId(t, "a", highest),
			}
			seedTimeline(t, client, base, timeline, "u1", posts...)

			tail, found, err := timeline.Tail(ctx, "u1")
			if err != nil {
				t.Fatalf("Tail: %v", err)
			}
			if !found {
				t.Fatal("Tail reported an empty index")
			}
			if tail != tc.want {
				t.Fatalf("Tail = %+v, want %+v", tail, tc.want)
			}

			// It is exactly where a walk of the whole index ends.
			order := timelineOrder(posts, tc.direction)
			if tail.Member != order[len(order)-1] {
				t.Fatalf("Tail().Member = %s, a walk ends at %s", tail.Member, order[len(order)-1])
			}
			if end := timeline.FetchAll().WithParams("u1").Exec(ctx).NextCursor(); end != tail {
				t.Fatalf("FetchAll ends at %+v, Tail = %+v", end, tail)
			}

			// Nothing follows it.
			after := timeline.FetchFrom(tail).WithParams("u1").Exec(ctx)
			if after.Error() != nil || len(after.Items()) != 0 || after.HasMore() {
				t.Fatalf("FetchFrom(Tail): %d items, HasMore = %v, err = %v", len(after.Items()), after.HasMore(), after.Error())
			}
		})
	}
}

func TestTailOfAnEmptyIndex(t *testing.T) { scenarioTailOfAnEmptyIndex(t, miniClient) }

func scenarioTailOfAnEmptyIndex(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)

	for _, direction := range []string{Descending, Ascending} {
		timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, direction, indexTTL)
		tail, found, err := timeline.Tail(ctx, "nobody")
		if err != nil {
			t.Fatalf("%s: Tail: %v", direction, err)
		}
		if found || !tail.IsZero() {
			t.Fatalf("%s: Tail of an empty index = %+v, %v; want the zero Cursor and false", direction, tail, found)
		}
	}
}

func TestTailFollowsTheIndexAsItChanges(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	posts := seedPosts(t, 3) // posts[0] is the oldest
	seedTimeline(t, client, base, timeline, "u1", posts...)

	tail, _, err := timeline.Tail(ctx, "u1")
	if err != nil || tail.Member != posts[0].GetRandId() {
		t.Fatalf("Tail = %+v (%v), want the oldest post", tail, err)
	}

	if err := timeline.RemoveItem(ctx, posts[0], "u1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}
	tail, _, err = timeline.Tail(ctx, "u1")
	if err != nil || tail.Member != posts[1].GetRandId() {
		t.Fatalf("after removing the tail, Tail = %+v (%v), want the next oldest", tail, err)
	}
}

func TestTailIsOneRoundTrip(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)
	seedTimeline(t, client, base, timeline, "u1", seedPosts(t, 5)...)

	counter := newVerbCounter(client)
	if _, _, err := timeline.Tail(ctx, "u1"); err != nil {
		t.Fatalf("Tail: %v", err)
	}

	total := 0
	for _, verb := range []string{"zrange", "zrevrange", "zcard", "zscore", "eval", "get", "exists"} {
		total += counter.count(verb)
	}
	if total != 1 || counter.count("zrange") != 1 {
		t.Fatalf("Tail issued %d commands (%d ZRANGE), want one ZRANGE", total, counter.count("zrange"))
	}
}
