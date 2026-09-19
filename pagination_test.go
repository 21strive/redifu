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

// verbCounter counts commands by verb only, which is what matters when the argument
// is a Lua script body.
type verbCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newVerbCounter(client redis.UniversalClient) *verbCounter {
	counter := &verbCounter{counts: map[string]int{}}
	client.AddHook(counter)
	return counter
}

func (c *verbCounter) record(cmd redis.Cmder) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[strings.ToLower(cmd.Name())]++
}

func (c *verbCounter) count(verb string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[verb]
}

func (c *verbCounter) DialHook(next redis.DialHook) redis.DialHook { return next }

func (c *verbCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.record(cmd)
		return next(ctx, cmd)
	}
}

func (c *verbCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			c.record(cmd)
		}
		return next(ctx, cmds)
	}
}

func seedPosts(t *testing.T, count int) []*Post {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	posts := make([]*Post, 0, count)
	for i := 0; i < count; i++ {
		posts = append(posts, newPost(t, fmt.Sprintf("post %d", i), base.Add(time.Duration(i)*time.Minute)))
	}
	return posts
}

// ---------------------------------------------------------------------------
// an expired item in the middle of a feed is not the end of the feed
// ---------------------------------------------------------------------------

func TestExpiredItemsDoNotLookLikeTheEndOfTheFeed(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 3, Descending, indexTTL)

	posts := seedPosts(t, 7)
	seedTimeline(t, client, base, timeline, "u1", posts...)

	// Two items on the first page expire out of Base while the index keeps pointing
	// at them. Descending order puts posts 6, 5, 4 on page one.
	server.Del("post:" + posts[6].GetRandId())
	server.Del("post:" + posts[5].GetRandId())

	output := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}

	if len(output.Items()) != 1 {
		t.Fatalf("hydrated %d items, want 1", len(output.Items()))
	}
	if output.Dangling() != 2 {
		t.Fatalf("Dangling() = %d, want 2", output.Dangling())
	}
	if !output.HasMore() {
		t.Fatal("HasMore() = false — a short page was mistaken for the end of the feed")
	}
	if output.Position() == LastPage {
		t.Fatal("Position() = LastPage on a page that has four more items behind it")
	}
}

func TestDanglingMembersAreRemovedFromTheIndex(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)

	posts := seedPosts(t, 4)
	seedSorted(t, client, base, sorted, "u1", posts...)

	server.Del("post:" + posts[0].GetRandId())

	if _, err := sorted.Fetch(Descending).WithParams("u1").Exec(ctx); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	count, err := sorted.Count(ctx, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Fatalf("index holds %d members, want 3 — the dangling member was not cleaned up", count)
	}

	if !server.Exists("post:" + posts[1].GetRandId()) {
		t.Fatal("self-healing deleted an item that still exists")
	}
}

func TestSelfHealCanBeSwitchedOff(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)
	sorted.SetSelfHeal(false)

	posts := seedPosts(t, 4)
	seedSorted(t, client, base, sorted, "u1", posts...)
	server.Del("post:" + posts[0].GetRandId())

	if _, err := sorted.Fetch(Descending).WithParams("u1").Exec(ctx); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	count, err := sorted.Count(ctx, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 4 {
		t.Fatalf("index holds %d members, want 4 with self-healing off", count)
	}
}

// ---------------------------------------------------------------------------
// cursors
// ---------------------------------------------------------------------------

func TestCursorPagesDoNotOverlapOrSkip(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	posts := seedPosts(t, 5)
	seedTimeline(t, client, base, timeline, "u1", posts...)

	seen := make([]string, 0, 5)
	cursor := []string{}

	for page := 0; page < 5; page++ {
		output := timeline.Fetch(cursor).WithParams("u1").Exec(ctx)
		if output.Error() != nil {
			t.Fatalf("page %d: %v", page, output.Error())
		}
		for _, fetched := range output.Items() {
			seen = append(seen, fetched.Title)
		}
		if !output.HasMore() {
			break
		}
		cursor = []string{output.ValidLastId()}
	}

	want := []string{"post 4", "post 3", "post 2", "post 1", "post 0"}
	if len(seen) != len(want) {
		t.Fatalf("walked %d items %v, want %d", len(seen), seen, len(want))
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("item %d = %q, want %q (full walk: %v)", i, seen[i], want[i], seen)
		}
	}
}

func TestCursorHandlesItemsSharingOneScore(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	// Five posts written in the same millisecond: every member shares one score, so
	// a score cursor has to break the tie by member rather than skip the whole group.
	sameInstant := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	posts := make([]*Post, 0, 5)
	for i := 0; i < 5; i++ {
		posts = append(posts, newPost(t, fmt.Sprintf("tied %d", i), sameInstant))
	}
	seedTimeline(t, client, base, timeline, "u1", posts...)

	seen := map[string]bool{}
	cursor := []string{}

	for page := 0; page < 6; page++ {
		output := timeline.Fetch(cursor).WithParams("u1").Exec(ctx)
		if output.Error() != nil {
			t.Fatalf("page %d: %v", page, output.Error())
		}
		for _, fetched := range output.Items() {
			if seen[fetched.GetRandId()] {
				t.Fatalf("item %s served twice", fetched.Title)
			}
			seen[fetched.GetRandId()] = true
		}
		if !output.HasMore() {
			break
		}
		cursor = []string{output.ValidLastId()}
	}

	if len(seen) != 5 {
		t.Fatalf("walked %d of 5 tied items", len(seen))
	}
}

func TestAVanishedCursorAsksTheClientToRestart(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	posts := seedPosts(t, 5)
	seedTimeline(t, client, base, timeline, "u1", posts...)

	first := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if first.Error() != nil {
		t.Fatalf("first page: %v", first.Error())
	}
	cursor := first.ValidLastId()

	// The cursor leaves the index — removed, or aged out of the window.
	if err := timeline.RemoveItem(ctx, posts[3], "u1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}

	output := timeline.Fetch([]string{cursor}).WithParams("u1").Exec(ctx)
	if !errors.Is(output.Error(), ErrResetPagination) {
		t.Fatalf("Fetch = %v, want ErrResetPagination — serving page one again would repeat items the client already has", output.Error())
	}
}

func TestOlderCursorCandidateIsUsedWhenTheNewestIsGone(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	posts := seedPosts(t, 5)
	seedTimeline(t, client, base, timeline, "u1", posts...)

	// posts[3] is the newest cursor, posts[4] the fallback behind it.
	if err := timeline.RemoveItem(ctx, posts[3], "u1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}

	output := timeline.Fetch([]string{posts[4].GetRandId(), posts[3].GetRandId()}).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	if len(output.Items()) == 0 {
		t.Fatal("fell back to no page at all")
	}
	if output.Items()[0].Title != "post 2" {
		t.Fatalf("first item = %q, want %q", output.Items()[0].Title, "post 2")
	}
}

// ---------------------------------------------------------------------------
// ingest is one atomic command instead of a read-decide-write sequence
// ---------------------------------------------------------------------------

func TestIngestCostsOneCommandAndNoClientSideReads(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 20, Descending, indexTTL)

	posts := seedPosts(t, 3)
	seedTimeline(t, client, base, timeline, "u1", posts...)

	counter := newVerbCounter(client)

	fresh := newPost(t, "fresh", time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	if err := timeline.AddItem(ctx, fresh, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	for _, verb := range []string{"zcard", "zrange", "zrevrange", "get"} {
		if got := counter.count(verb); got != 0 {
			t.Fatalf("AddItem issued %d %s from the client — the decision should happen inside Redis", got, verb)
		}
	}
	if got := counter.count("eval"); got != 1 {
		t.Fatalf("AddItem issued %d EVAL, want exactly 1", got)
	}
}

func TestConcurrentAddsCannotOverfillAPage(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 3, Descending, indexTTL)

	posts := seedPosts(t, 3)
	seedTimeline(t, client, base, timeline, "u1", posts...)

	pipe := client.Pipeline()
	if err := timeline.MarkFirstPage(ctx, pipe, "u1"); err != nil {
		t.Fatalf("MarkFirstPage: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	var wait sync.WaitGroup
	for i := 0; i < 20; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			fresh := newPost(t, fmt.Sprintf("concurrent %d", i), time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC).Add(time.Duration(i)*time.Second))
			_ = timeline.AddItem(ctx, fresh, "u1")
		}(i)
	}
	wait.Wait()

	// Every add is a single atomic script, so the marker transition happens exactly
	// once no matter how many writers race for it.
	isFirstPage, err := timeline.IsFirstPage(ctx, "u1")
	if err != nil {
		t.Fatalf("IsFirstPage: %v", err)
	}
	if isFirstPage {
		t.Fatal("the first-page marker survived a full page of concurrent adds")
	}
}
