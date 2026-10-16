package redifu

// M — a Sorted or Page read says how many of its members had lost their item.

import (
	"context"
	"testing"
)

func TestSortedExecReportCountsDanglingMembers(t *testing.T) {
	scenarioSortedExecReportCountsDanglingMembers(t, miniClient)
}

func scenarioSortedExecReportCountsDanglingMembers(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)

	posts := seedPosts(t, 5)
	seedSorted(t, client, base, sorted, "u1", posts...)

	// One item's key expires while the index still points at it.
	if err := client.Del(ctx, "post:"+posts[2].GetRandId()).Err(); err != nil {
		t.Fatalf("Del: %v", err)
	}

	items, dangling, err := sorted.Fetch(Descending).WithParams("u1").ExecReport(ctx)
	if err != nil {
		t.Fatalf("ExecReport: %v", err)
	}
	if len(items) != 4 || dangling != 1 {
		t.Fatalf("ExecReport = %d items, %d dangling; want 4 and 1", len(items), dangling)
	}
	for _, item := range items {
		if item.GetRandId() == posts[2].GetRandId() {
			t.Fatal("the expired item came back")
		}
	}

	// Self-healing removed the member, as Exec always did: the next read is whole.
	if count, _ := sorted.Count(ctx, "u1"); count != 4 {
		t.Fatalf("index holds %d members after the read, want 4", count)
	}
	items, dangling, err = sorted.Fetch(Descending).WithParams("u1").ExecReport(ctx)
	if err != nil || len(items) != 4 || dangling != 0 {
		t.Fatalf("second ExecReport = %d items, %d dangling, %v; want 4, 0", len(items), dangling, err)
	}
}

func TestSortedExecReportCountsOnlyTheRangeItRead(t *testing.T) {
	scenarioSortedExecReportCountsOnlyTheRangeItRead(t, miniClient)
}

func scenarioSortedExecReportCountsOnlyTheRangeItRead(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)

	posts := seedPosts(t, 6) // posts[i] scored i minutes after the first
	seedSorted(t, client, base, sorted, "u1", posts...)
	for _, gone := range []*Post{posts[1], posts[4]} {
		if err := client.Del(ctx, "post:"+gone.GetRandId()).Err(); err != nil {
			t.Fatalf("Del: %v", err)
		}
	}

	// Ranks 0..2 ascending hold posts 0, 1, 2: one of them is gone.
	items, dangling, err := sorted.Fetch(Ascending).WithParams("u1").WithRankRange(0, 2).ExecReport(ctx)
	if err != nil || len(items) != 2 || dangling != 1 {
		t.Fatalf("rank range: %d items, %d dangling, %v; want 2, 1", len(items), dangling, err)
	}

	// The score range of posts 3..5 holds the other one.
	lower, upper := scoreOf(posts[3]), scoreOf(posts[5])
	items, dangling, err = sorted.Fetch(Ascending).WithParams("u1").WithRange(lower, upper).ExecReport(ctx)
	if err != nil || len(items) != 2 || dangling != 1 {
		t.Fatalf("score range: %d items, %d dangling, %v; want 2, 1", len(items), dangling, err)
	}
}

func TestSortedExecReportWithSelfHealOff(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)
	sorted.SetSelfHeal(false)

	posts := seedPosts(t, 5)
	seedSorted(t, client, base, sorted, "u1", posts...)
	server.Del("post:" + posts[0].GetRandId())

	for i := 0; i < 2; i++ {
		items, dangling, err := sorted.Fetch(Ascending).WithParams("u1").ExecReport(ctx)
		if err != nil || len(items) != 4 || dangling != 1 {
			t.Fatalf("read %d: %d items, %d dangling, %v; want 4 and 1 every time", i, len(items), dangling, err)
		}
	}
	if count, _ := sorted.Count(ctx, "u1"); count != 5 {
		t.Fatalf("index holds %d members, want 5 with self-healing off", count)
	}
}

func TestSortedExecIsExecReportWithoutTheCount(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)

	posts := seedPosts(t, 5)
	seedSorted(t, client, base, sorted, "u1", posts...)
	server.Del("post:" + posts[3].GetRandId())

	items, err := sorted.Fetch(Descending).WithParams("u1").Exec(ctx)
	if err != nil || len(items) != 4 {
		t.Fatalf("Exec = %d items, %v; want the 4 that exist", len(items), err)
	}

	// Nothing at all: no items and nothing dangling.
	items, dangling, err := sorted.Fetch(Descending).WithParams("nobody").ExecReport(ctx)
	if err != nil || items != nil || dangling != 0 {
		t.Fatalf("empty collection: %v items, %d dangling, %v", items, dangling, err)
	}
}

func TestPageExecReportCountsDanglingMembers(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	page := mustPage[*Post](t, client, base, feedKeyFormat, 3, Descending, indexTTL)

	posts := seedPosts(t, 3)
	pipe := client.Pipeline()
	for _, post := range posts {
		if err := base.WithPipeline(pipe).Set(ctx, post); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := page.IngestItem(ctx, pipe, post, 1, "u1"); err != nil {
			t.Fatalf("IngestItem: %v", err)
		}
	}
	if err := page.AddPage(ctx, pipe, 1, "u1"); err != nil {
		t.Fatalf("AddPage: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	server.Del("post:" + posts[1].GetRandId())

	items, dangling, err := page.Fetch(1).WithParams("u1").ExecReport(ctx)
	if err != nil || len(items) != 2 || dangling != 1 {
		t.Fatalf("Page ExecReport = %d items, %d dangling, %v; want 2 and 1", len(items), dangling, err)
	}
}
