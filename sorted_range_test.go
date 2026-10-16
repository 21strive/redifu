package redifu

// F — Sorted reads a slice of the collection by rank, or a page of a score range.

import (
	"context"
	"strings"
	"testing"
	"time"
)

// sortedFixture seeds ten posts one minute apart: "s0" at 00:00 .. "s9" at 00:09.
func sortedFixture(t *testing.T, newClient clientFactory) (*Sorted[*Post], []*Post, context.Context) {
	t.Helper()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	posts := make([]*Post, 0, 10)
	for i := 0; i < 10; i++ {
		posts = append(posts, postWithId(t, "s"+string(rune('0'+i)), start.Add(time.Duration(i)*time.Minute)))
	}
	seedSorted(t, client, base, sorted, "u1", posts...)
	return sorted, posts, context.Background()
}

func idsOf(items []*Post) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.GetRandId())
	}
	return ids
}

func scoreOf(post *Post) int64 { return post.GetCreatedAt().UnixMilli() }

func TestSortedRankRange(t *testing.T) { scenarioSortedRankRange(t, miniClient) }

func scenarioSortedRankRange(t *testing.T, newClient clientFactory) {
	sorted, _, ctx := sortedFixture(t, newClient)

	cases := []struct {
		direction   string
		start, stop int64
		want        []string
	}{
		{Ascending, 0, 2, []string{"s0", "s1", "s2"}},
		{Ascending, 3, 5, []string{"s3", "s4", "s5"}},
		{Descending, 0, 2, []string{"s9", "s8", "s7"}},
		{Descending, 3, 5, []string{"s6", "s5", "s4"}},
		{Descending, -3, -1, []string{"s2", "s1", "s0"}},
		{Ascending, 8, 20, []string{"s8", "s9"}},
		{Ascending, 10, 20, nil},
		{Descending, 0, -1, []string{"s9", "s8", "s7", "s6", "s5", "s4", "s3", "s2", "s1", "s0"}},
	}

	for _, tc := range cases {
		items, err := sorted.Fetch(tc.direction).WithParams("u1").WithRankRange(tc.start, tc.stop).Exec(ctx)
		if err != nil {
			t.Fatalf("%s %d..%d: %v", tc.direction, tc.start, tc.stop, err)
		}
		equalIds(t, tc.direction+" rank range", idsOf(items), tc.want)
	}
}

func TestSortedScoreRangeWithLimit(t *testing.T) { scenarioSortedScoreRangeWithLimit(t, miniClient) }

func scenarioSortedScoreRangeWithLimit(t *testing.T, newClient clientFactory) {
	sorted, posts, ctx := sortedFixture(t, newClient)
	lower, upper := scoreOf(posts[2]), scoreOf(posts[7]) // s2 .. s7, both inclusive

	cases := []struct {
		name          string
		direction     string
		offset, count int64
		want          []string
	}{
		{"first page ascending", Ascending, 0, 2, []string{"s2", "s3"}},
		{"second page ascending", Ascending, 2, 2, []string{"s4", "s5"}},
		{"first page descending", Descending, 0, 2, []string{"s7", "s6"}},
		{"second page descending", Descending, 2, 2, []string{"s5", "s4"}},
		{"last, short page", Descending, 4, 4, []string{"s3", "s2"}},
		{"past the range", Ascending, 6, 2, nil},
		{"negative count: the rest", Ascending, 3, -1, []string{"s5", "s6", "s7"}},
		{"zero count: nothing", Ascending, 0, 0, nil},
	}

	for _, tc := range cases {
		items, err := sorted.Fetch(tc.direction).WithParams("u1").WithRange(lower, upper).WithLimit(tc.offset, tc.count).Exec(ctx)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		equalIds(t, tc.name, idsOf(items), tc.want)
	}

	// Without WithRange the limit pages the whole collection.
	items, err := sorted.Fetch(Descending).WithParams("u1").WithLimit(1, 3).Exec(ctx)
	if err != nil {
		t.Fatalf("whole collection: %v", err)
	}
	equalIds(t, "whole collection", idsOf(items), []string{"s8", "s7", "s6"})
}

func TestSortedRangeOptionsCombine(t *testing.T) {
	sorted, posts, ctx := sortedFixture(t, miniClient)

	// The last range wins.
	items, err := sorted.Fetch(Ascending).WithParams("u1").
		WithRange(scoreOf(posts[5]), scoreOf(posts[9])).
		WithRankRange(0, 1).
		Exec(ctx)
	if err != nil {
		t.Fatalf("score then rank: %v", err)
	}
	equalIds(t, "score then rank", idsOf(items), []string{"s0", "s1"})

	items, err = sorted.Fetch(Ascending).WithParams("u1").
		WithRankRange(0, 1).
		WithRange(scoreOf(posts[5]), scoreOf(posts[6])).
		Exec(ctx)
	if err != nil {
		t.Fatalf("rank then score: %v", err)
	}
	equalIds(t, "rank then score", idsOf(items), []string{"s5", "s6"})

	// A rank range is already a limit.
	if _, err := sorted.Fetch(Ascending).WithParams("u1").WithRankRange(0, 5).WithLimit(0, 2).Exec(ctx); err == nil ||
		!strings.Contains(err.Error(), "rank range") {
		t.Fatalf("rank range with a limit = %v, want an error", err)
	}

	// A negative offset is refused rather than silently returning nothing.
	if _, err := sorted.Fetch(Ascending).WithParams("u1").WithLimit(-1, 2).Exec(ctx); err == nil {
		t.Fatal("a negative offset was accepted")
	}

	// A builder can be executed twice.
	builder := sorted.Fetch(Descending).WithParams("u1").WithRankRange(0, 2)
	first, errFirst := builder.Exec(ctx)
	second, errSecond := builder.Exec(ctx)
	if errFirst != nil || errSecond != nil {
		t.Fatalf("Exec twice: %v, %v", errFirst, errSecond)
	}
	equalIds(t, "second Exec", idsOf(second), idsOf(first))
}

func TestSortedLimitOfZeroAsksRedisNothing(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)
	seedSorted(t, client, base, sorted, "u1", seedPosts(t, 4)...)

	counter := newVerbCounter(client)
	items, err := sorted.Fetch(Ascending).WithParams("u1").WithLimit(0, 0).Exec(ctx)
	if err != nil || len(items) != 0 {
		t.Fatalf("WithLimit(0, 0) = %d items, %v; want none", len(items), err)
	}
	if got := counter.count("zrangebyscore"); got != 0 {
		t.Fatalf("issued %d ZRANGEBYSCORE — a LIMIT of 0 0 is dropped by the driver and would read everything", got)
	}
}

func TestSortedRankRangeResolvesRelationsAndRunsTheProcessor(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	postBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	postBase.AddRelation(relateAuthor(t, accountBase))
	sorted := mustSorted[*Post](t, client, postBase, feedKeyFormat, indexTTL)

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("Set account: %v", err)
	}
	posts := seedPosts(t, 4)
	for _, post := range posts {
		post.AuthorRandId = author.GetRandId()
	}
	seedSorted(t, client, postBase, sorted, "u1", posts...)

	items, err := sorted.Fetch(Descending).WithParams("u1").WithRankRange(1, 2).
		WithProcessor(func(p **Post, _ []interface{}) { (*p).Title = "processed" }).
		Exec(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("fetched %d items, want 2", len(items))
	}
	for _, item := range items {
		if item.Author == nil || item.Author.Name != "Ada" || item.Title != "processed" {
			t.Fatal("a rank-range read skipped the relation or the processor")
		}
	}
}
