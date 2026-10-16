package redifu

// O — one Timeline index serves more than one page size.

import (
	"context"
	"fmt"
	"testing"
)

func pageIds(output *FetchOutput[*Post]) []string {
	ids := make([]string, 0, len(output.Items()))
	for _, item := range output.Items() {
		ids = append(ids, item.GetRandId())
	}
	return ids
}

func TestTimelineWithLimitOverridesThePageSize(t *testing.T) {
	scenarioTimelineWithLimitOverridesThePageSize(t, miniClient)
}

func scenarioTimelineWithLimitOverridesThePageSize(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 3, Descending, indexTTL)

	posts := seedPosts(t, 12)
	seedTimeline(t, client, base, timeline, "u1", posts...)
	order := timelineOrder(posts, Descending)

	// Position cursors: 5, 5, then the last 2.
	first := timeline.FetchFrom(Cursor{}).WithParams("u1").WithLimit(5).Exec(ctx)
	equalIds(t, "first page of 5", pageIds(first), order[0:5])
	if !first.HasMore() || first.NextCursor().Member != order[4] {
		t.Fatalf("HasMore = %v, NextCursor = %+v; want true and %s", first.HasMore(), first.NextCursor(), order[4])
	}
	second := timeline.FetchFrom(first.NextCursor()).WithParams("u1").WithLimit(5).Exec(ctx)
	equalIds(t, "second page of 5", pageIds(second), order[5:10])
	if !second.HasMore() {
		t.Fatal("HasMore = false with two items left")
	}
	last := timeline.FetchFrom(second.NextCursor()).WithParams("u1").WithLimit(5).Exec(ctx)
	equalIds(t, "last page", pageIds(last), order[10:12])
	if last.HasMore() || last.Position() != LastPage {
		t.Fatalf("HasMore = %v, Position = %s on the last page", last.HasMore(), last.Position())
	}

	// Exactly the page size left: no more after it.
	exact := timeline.FetchFrom(second.NextCursor()).WithParams("u1").WithLimit(2).Exec(ctx)
	if len(exact.Items()) != 2 || exact.HasMore() {
		t.Fatalf("exactly n left: %d items, HasMore = %v; want 2 and false", len(exact.Items()), exact.HasMore())
	}

	// randId cursors take it too.
	byId := timeline.Fetch([]string{order[1]}).WithParams("u1").WithLimit(4).Exec(ctx)
	equalIds(t, "randId cursor, page of 4", pageIds(byId), order[2:6])

	// Below 1 keeps itemPerPage.
	for _, n := range []int64{0, -5} {
		output := timeline.FetchFrom(Cursor{}).WithParams("u1").WithLimit(n).Exec(ctx)
		equalIds(t, fmt.Sprintf("WithLimit(%d)", n), pageIds(output), order[0:3])
	}

	// FetchAll ignores it.
	all := timeline.FetchAll().WithParams("u1").WithLimit(2).Exec(ctx)
	if len(all.Items()) != 12 || all.HasMore() {
		t.Fatalf("FetchAll with a limit: %d items, HasMore = %v; want all 12", len(all.Items()), all.HasMore())
	}
}

func TestDocumentedTimelineFlowWithAnotherPageSize(t *testing.T) {
	scenarioDocumentedTimelineFlowWithAnotherPageSize(t, miniClient)
}

func scenarioDocumentedTimelineFlowWithAnotherPageSize(t *testing.T, newClient clientFactory) {
	for _, pageSize := range []int64{7, 100} {
		for _, rows := range []int{0, 7, 8, 50, 120} {
			t.Run(fmt.Sprintf("pageSize=%d/rows=%d", pageSize, rows), func(t *testing.T) {
				store, ctx := newFeedStore(t, newClient(t), Descending, rows)
				store.pageSize = pageSize
				want := store.table.order(t, store.timeline)

				equalIds(t, "first walk", store.walk(ctx), want)
				first := store.table.queries
				equalIds(t, "second walk", store.walk(ctx), want)
				if store.table.queries != first {
					t.Fatalf("the second walk ran %d queries, want 0", store.table.queries-first)
				}
			})
		}
	}
}

func TestOneIndexServesTwoPageSizes(t *testing.T) {
	ctx := context.Background()
	store, _ := newFeedStore(t, miniClient(t), Descending, 150)
	want := store.table.order(t, store.timeline)

	// One client scrolls 20 at a time, another 100 at a time, against the same index.
	store.pageSize = 20
	equalIds(t, "walk by 20", store.walk(ctx), want)

	before := store.table.queries
	store.pageSize = 100
	equalIds(t, "walk by 100", store.walk(ctx), want)
	if store.table.queries != before {
		t.Fatalf("the walk by 100 ran %d queries over an index the walk by 20 had filled", store.table.queries-before)
	}

	// Switching page size mid-scroll neither skips nor repeats.
	var seen []string
	cursor := Cursor{}
	for i, size := range []int64{20, 100, 7, 50} {
		store.pageSize = size
		page, err := store.page(ctx, cursor)
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		seen = append(seen, page.ids...)
		if !page.hasMore {
			break
		}
		cursor = page.next
	}
	equalIds(t, "mixed page sizes", seen, want[:len(seen)])
	if len(seen) != 150 {
		t.Fatalf("mixed page sizes covered %d of 150 rows", len(seen))
	}
}
