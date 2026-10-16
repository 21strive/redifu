package redifu

// A — the Timeline flow CLAUDE.consumer.md documents, driven end to end against tables of
// different sizes. The handler and the seeder below are the documented ones with the
// database replaced by feedTable, which answers the same keyset query and counts it.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// feedTable stands in for the posts table. after is the documented keyset query: the
// rows strictly after a cursor in the timeline's order — score then member, both in the
// timeline's direction — LIMIT limit, the head of the table for the zero Cursor.
type feedTable struct {
	rows    []*Post
	queries int
}

func (table *feedTable) ordered(t *testing.T, timeline *Timeline[*Post]) ([]*Post, []float64) {
	t.Helper()
	type scored struct {
		post  *Post
		score float64
	}
	all := make([]scored, 0, len(table.rows))
	for _, row := range table.rows {
		score, err := timeline.Score(row)
		if err != nil {
			t.Fatalf("Score: %v", err)
		}
		all = append(all, scored{row, score})
	}
	descending := timeline.GetDirection() == Descending
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i], all[j]
		if a.score != b.score {
			return (a.score > b.score) == descending
		}
		return (a.post.GetRandId() > b.post.GetRandId()) == descending
	})

	posts := make([]*Post, 0, len(all))
	scores := make([]float64, 0, len(all))
	for _, entry := range all {
		posts = append(posts, entry.post)
		scores = append(scores, entry.score)
	}
	return posts, scores
}

func (table *feedTable) after(t *testing.T, timeline *Timeline[*Post], after Cursor, limit int64) []*Post {
	t.Helper()
	table.queries++

	posts, scores := table.ordered(t, timeline)
	descending := timeline.GetDirection() == Descending

	start := 0
	if !after.IsZero() {
		start = len(posts)
		for i, post := range posts {
			score, member := scores[i], post.GetRandId()
			var past bool
			if descending {
				past = score < after.Score || (score == after.Score && member < after.Member)
			} else {
				past = score > after.Score || (score == after.Score && member > after.Member)
			}
			if past {
				start = i
				break
			}
		}
	}

	end := start + int(limit)
	if end > len(posts) {
		end = len(posts)
	}
	return posts[start:end]
}

func (table *feedTable) order(t *testing.T, timeline *Timeline[*Post]) []string {
	t.Helper()
	posts, _ := table.ordered(t, timeline)
	ids := make([]string, 0, len(posts))
	for _, post := range posts {
		ids = append(ids, post.GetRandId())
	}
	return ids
}

func (table *feedTable) remove(randId string) {
	kept := table.rows[:0]
	for _, row := range table.rows {
		if row.GetRandId() != randId {
			kept = append(kept, row)
		}
	}
	table.rows = kept
}

type feedStore struct {
	t        *testing.T
	client   redis.UniversalClient
	base     *Base[*Post]
	timeline *Timeline[*Post]
	table    *feedTable
	user     string
}

// seed is the documented seeder: up to limit rows that follow after (the head of the
// table for the zero Cursor), with the markers that record whether they reach the end.
func (s *feedStore) seed(ctx context.Context, after Cursor, limit int64) error {
	rows := s.table.after(s.t, s.timeline, after, limit)

	pipe := s.client.Pipeline()
	if after.IsZero() {
		// A head seed starts the index over: a :lastpage marker can outlive the index
		// it described.
		if err := s.timeline.UnmarkLastPage(ctx, pipe, s.user); err != nil {
			return err
		}
	}
	for _, row := range rows {
		if err := s.base.WithPipeline(pipe).Set(ctx, row); err != nil {
			return err
		}
		if err := s.timeline.IngestItem(ctx, pipe, row, true, s.user); err != nil {
			return err
		}
	}

	count := int64(len(rows))
	switch {
	case after.IsZero() && count == 0:
		if err := s.timeline.MarkEmpty(ctx, pipe, s.user); err != nil {
			return err
		}
	case count < limit:
		if err := s.timeline.MarkLastPage(ctx, pipe, s.user); err != nil {
			return err
		}
	}
	if after.IsZero() {
		if err := s.timeline.SetExpiration(ctx, pipe, s.user); err != nil {
			return err
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}

// maxTopUps caps the database reads one request spends extending the index.
const maxTopUps = 3

type feedPage struct {
	ids     []string
	next    Cursor
	lastId  string
	hasMore bool
	reset   bool
}

func pageOf(output *FetchOutput[*Post], hasMore bool) feedPage {
	ids := make([]string, 0, len(output.Items()))
	for _, item := range output.Items() {
		ids = append(ids, item.GetRandId())
	}
	return feedPage{ids: ids, next: output.NextCursor(), lastId: output.ValidLastId(), hasMore: hasMore}
}

// topUp is the documented loop: while the page has run past the end of the index and the
// index does not hold the end of the table, load what follows its tail and read again.
func (s *feedStore) topUp(ctx context.Context, output *FetchOutput[*Post], read func() *FetchOutput[*Post]) (feedPage, error) {
	perPage := s.timeline.GetItemPerPage()

	for topUps := 0; output.Error() == nil && !output.HasMore(); topUps++ {
		end, err := s.timeline.ReachedEnd(ctx, s.user)
		if err != nil {
			return feedPage{}, err
		}
		if end {
			break
		}
		if topUps == maxTopUps {
			return pageOf(output, true), nil
		}

		tail, _, err := s.timeline.Tail(ctx, s.user)
		if err != nil {
			return feedPage{}, err
		}
		if err := s.seed(ctx, tail, perPage+1-int64(len(output.Items()))); err != nil {
			return feedPage{}, err
		}
		output = read()
	}
	if output.Error() != nil {
		return feedPage{}, output.Error()
	}
	return pageOf(output, output.HasMore()), nil
}

// page is the documented handler for position cursors.
func (s *feedStore) page(ctx context.Context, cursor Cursor) (feedPage, error) {
	read := func() *FetchOutput[*Post] {
		return s.timeline.FetchFrom(cursor).WithParams(s.user).Exec(ctx)
	}
	return s.topUp(ctx, read(), read)
}

// pageByRandId is the documented handler for clients that send randId cursors.
func (s *feedStore) pageByRandId(ctx context.Context, lastRandIds []string) (feedPage, error) {
	output := s.timeline.Fetch(lastRandIds).WithParams(s.user).Exec(ctx)
	reset := false
	if errors.Is(output.Error(), ErrResetPagination) {
		lastRandIds, reset = nil, true
		output = s.timeline.Fetch(nil).WithParams(s.user).Exec(ctx)
	}

	read := func() *FetchOutput[*Post] {
		return s.timeline.Fetch(lastRandIds).WithParams(s.user).Exec(ctx)
	}
	page, err := s.topUp(ctx, output, read)
	page.reset = reset
	return page, err
}

// feedRows builds a table of n posts, three to a millisecond, so that page boundaries
// fall inside groups of equal scores.
func feedRows(t *testing.T, n int) []*Post {
	t.Helper()
	newest := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]*Post, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, newPost(t, fmt.Sprintf("p%02d", i), newest.Add(-time.Duration(i/3)*time.Minute)))
	}
	return rows
}

func newFeedStore(t *testing.T, client redis.UniversalClient, direction string, rows int) (*feedStore, context.Context) {
	t.Helper()
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 20, direction, indexTTL)
	return &feedStore{
		t:        t,
		client:   client,
		base:     base,
		timeline: timeline,
		table:    &feedTable{rows: feedRows(t, rows)},
		user:     "u1",
	}, context.Background()
}

// walk scrolls the whole feed like a client: follow the cursor while HasMore.
func (s *feedStore) walk(ctx context.Context) []string {
	s.t.Helper()
	var seen []string
	cursor := Cursor{}
	for i := 0; i < 50; i++ {
		page, err := s.page(ctx, cursor)
		if err != nil {
			s.t.Fatalf("page %d: %v", i, err)
		}
		seen = append(seen, page.ids...)
		if !page.hasMore {
			return seen
		}
		cursor = page.next
	}
	s.t.Fatal("the feed did not end")
	return nil
}

func (s *feedStore) walkByRandId(ctx context.Context) []string {
	s.t.Helper()
	var seen []string
	var cursor []string
	for i := 0; i < 50; i++ {
		page, err := s.pageByRandId(ctx, cursor)
		if err != nil {
			s.t.Fatalf("page %d: %v", i, err)
		}
		if page.reset {
			s.t.Fatalf("page %d: the client was sent back to page one", i)
		}
		seen = append(seen, page.ids...)
		if !page.hasMore {
			return seen
		}
		cursor = []string{page.lastId}
	}
	s.t.Fatal("the feed did not end")
	return nil
}

// ---------------------------------------------------------------------------
// the documented flow scrolls every table exactly once, then from Redis alone
// ---------------------------------------------------------------------------

func TestDocumentedTimelineFlowWalksEveryRowOnce(t *testing.T) {
	scenarioDocumentedTimelineFlowWalksEveryRowOnce(t, miniClient)
}

func scenarioDocumentedTimelineFlowWalksEveryRowOnce(t *testing.T, newClient clientFactory) {
	for _, direction := range []string{Descending, Ascending} {
		for _, rows := range []int{0, 7, 20, 21, 40, 50} {
			t.Run(fmt.Sprintf("%s/rows=%d", direction, rows), func(t *testing.T) {
				store, ctx := newFeedStore(t, newClient(t), direction, rows)
				want := store.table.order(t, store.timeline)

				equalIds(t, "first walk", store.walk(ctx), want)
				first := store.table.queries

				equalIds(t, "second walk", store.walk(ctx), want)
				if store.table.queries != first {
					t.Fatalf("the second walk ran %d queries, want 0 — it should be served from Redis", store.table.queries-first)
				}
				t.Logf("%d queries for the first walk, 0 for the second", first)
			})
		}
	}
}

func TestDocumentedTimelineFlowWithRandIdCursors(t *testing.T) {
	scenarioDocumentedTimelineFlowWithRandIdCursors(t, miniClient)
}

func scenarioDocumentedTimelineFlowWithRandIdCursors(t *testing.T, newClient clientFactory) {
	for _, rows := range []int{0, 7, 20, 21, 40, 50} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			store, ctx := newFeedStore(t, newClient(t), Descending, rows)
			want := store.table.order(t, store.timeline)

			equalIds(t, "first walk", store.walkByRandId(ctx), want)
			first := store.table.queries

			equalIds(t, "second walk", store.walkByRandId(ctx), want)
			if store.table.queries != first {
				t.Fatalf("the second walk ran %d queries, want 0", store.table.queries-first)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// the index changes while a client is scrolling
// ---------------------------------------------------------------------------

func TestDocumentedTimelineFlowWithANewItemMidScroll(t *testing.T) {
	scenarioDocumentedTimelineFlowWithANewItemMidScroll(t, miniClient)
}

func scenarioDocumentedTimelineFlowWithANewItemMidScroll(t *testing.T, newClient clientFactory) {
	store, ctx := newFeedStore(t, newClient(t), Descending, 50)
	older := store.table.order(t, store.timeline)

	first, err := store.page(ctx, Cursor{})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}

	// A new post lands at the head while the reader is on page one.
	fresh := newPost(t, "fresh", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	store.table.rows = append(store.table.rows, fresh)
	if err := store.timeline.AddItem(ctx, fresh, store.user); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	seen := append([]string(nil), first.ids...)
	cursor := first.next
	for first.hasMore {
		page, err := store.page(ctx, cursor)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		seen = append(seen, page.ids...)
		if !page.hasMore {
			break
		}
		cursor = page.next
	}
	equalIds(t, "the scroll in progress", seen, older)

	head, err := store.page(ctx, Cursor{})
	if err != nil {
		t.Fatalf("fresh first page: %v", err)
	}
	if head.ids[0] != fresh.GetRandId() {
		t.Fatalf("a fresh first page starts with %s, want the new post", head.ids[0])
	}
}

func TestDocumentedTimelineFlowSurvivesARemovalMidScroll(t *testing.T) {
	scenarioDocumentedTimelineFlowSurvivesARemovalMidScroll(t, miniClient)
}

func scenarioDocumentedTimelineFlowSurvivesARemovalMidScroll(t *testing.T, newClient clientFactory) {
	store, ctx := newFeedStore(t, newClient(t), Descending, 50)
	order := store.table.order(t, store.timeline)

	first, err := store.page(ctx, Cursor{})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}

	// The post the client's cursor points at changes status: it leaves the table this
	// feed reads, and its index.
	gone := first.next.Member
	for _, row := range store.table.rows {
		if row.GetRandId() == gone {
			if err := store.timeline.RemoveItem(ctx, row, store.user); err != nil {
				t.Fatalf("RemoveItem: %v", err)
			}
		}
	}
	store.table.remove(gone)

	seen := append([]string(nil), first.ids...)
	cursor := first.next
	for {
		page, err := store.page(ctx, cursor)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if page.reset {
			t.Fatal("the client was sent back to page one")
		}
		seen = append(seen, page.ids...)
		if !page.hasMore {
			break
		}
		cursor = page.next
	}
	equalIds(t, "the scroll across the removal", seen, order)
}

func TestDocumentedTimelineFlowRecoversAnIndexLostMidScroll(t *testing.T) {
	scenarioDocumentedTimelineFlowRecoversAnIndexLostMidScroll(t, miniClient)
}

func scenarioDocumentedTimelineFlowRecoversAnIndexLostMidScroll(t *testing.T, newClient clientFactory) {
	store, ctx := newFeedStore(t, newClient(t), Descending, 50)
	order := store.table.order(t, store.timeline)

	var seen []string
	cursor := Cursor{}
	for i := 0; i < 2; i++ {
		page, err := store.page(ctx, cursor)
		if err != nil {
			t.Fatalf("page %d: %v", i, err)
		}
		seen = append(seen, page.ids...)
		cursor = page.next
	}

	// The index is lost while the client is forty rows deep: purged, evicted, expired.
	if err := store.timeline.Purge(ctx, store.user); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	// The next page is still the one after the client's cursor. The index is rebuilt
	// from its head and extended from its tail until it covers that page, never seeded
	// from the cursor itself, which would leave a hole behind it.
	for {
		page, err := store.page(ctx, cursor)
		if err != nil {
			t.Fatalf("page after the loss: %v", err)
		}
		seen = append(seen, page.ids...)
		if !page.hasMore {
			break
		}
		cursor = page.next
	}
	equalIds(t, "the scroll across the loss", seen, order)

	// And the rebuilt index is whole: a fresh walk reads it without the database.
	before := store.table.queries
	equalIds(t, "a fresh walk", store.walk(ctx), order)
	if store.table.queries != before {
		t.Fatalf("a fresh walk after the recovery ran %d queries — the index has a hole", store.table.queries-before)
	}
}

func TestDocumentedTimelineFlowCapsTheTopUpsOfOneRequest(t *testing.T) {
	ctx := context.Background()
	store, _ := newFeedStore(t, miniClient(t), Descending, 200)
	order := store.table.order(t, store.timeline)

	// A cursor 150 rows deep against a cold index: one request may not rebuild all of
	// it, but it must not end the feed or skip anything either.
	deep := order[149]
	var deepScore float64
	for _, row := range store.table.rows {
		if row.GetRandId() == deep {
			deepScore, _ = store.timeline.Score(row)
		}
	}
	cursor := Cursor{Score: deepScore, Member: deep}

	var seen []string
	requests := 0
	for {
		requests++
		before := store.table.queries
		page, err := store.page(ctx, cursor)
		if err != nil {
			t.Fatalf("request %d: %v", requests, err)
		}
		if store.table.queries-before > maxTopUps {
			t.Fatalf("one request ran %d queries, cap is %d", store.table.queries-before, maxTopUps)
		}
		seen = append(seen, page.ids...)
		if !page.hasMore {
			break
		}
		cursor = page.next
		if requests > 40 {
			t.Fatal("the feed did not end")
		}
	}
	equalIds(t, "the rows after the deep cursor", seen, order[150:])
}

func TestDocumentedTimelineFlowIgnoresAStaleLastPageMarker(t *testing.T) {
	server, client := newTestRedis(t)
	store, ctx := newFeedStore(t, client, Descending, 50)
	order := store.table.order(t, store.timeline)

	// Page one seeds the head, which sets the index TTL.
	first, err := store.page(ctx, Cursor{})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}

	// An hour later the client scrolls to the end; the top-up that reaches it sets the
	// :lastpage marker, with a TTL of its own.
	server.FastForward(time.Hour)
	cursor := first.next
	for {
		page, err := store.page(ctx, cursor)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		if !page.hasMore {
			break
		}
		cursor = page.next
	}

	// The index expires; the marker, set an hour later, has not yet.
	server.FastForward(indexTTL - 30*time.Minute)
	marker := indexKey("feed:u1") + markerLastPage
	if server.Exists(indexKey("feed:u1")) || !server.Exists(marker) {
		t.Fatalf("setup: index exists = %v, marker exists = %v; want the index gone and the marker alive",
			server.Exists(indexKey("feed:u1")), server.Exists(marker))
	}

	end, err := store.timeline.ReachedEnd(ctx, store.user)
	if err != nil {
		t.Fatalf("ReachedEnd: %v", err)
	}
	if end {
		t.Fatal("ReachedEnd trusted a :lastpage marker on an index that holds nothing")
	}

	// The whole feed is still there; the head seed cleared the stale marker.
	equalIds(t, "a walk after the expiry", store.walk(ctx), order)
}

// ---------------------------------------------------------------------------
// ReachedEnd
// ---------------------------------------------------------------------------

func TestReachedEnd(t *testing.T) { scenarioReachedEnd(t, miniClient) }

func scenarioReachedEnd(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	check := func(label, user string, want bool) {
		t.Helper()
		got, err := timeline.ReachedEnd(ctx, user)
		if err != nil {
			t.Fatalf("%s: ReachedEnd: %v", label, err)
		}
		if got != want {
			t.Fatalf("%s: ReachedEnd = %v, want %v", label, got, want)
		}
	}
	mark := func(user string, marks ...func(context.Context, redis.Pipeliner, ...string) error) {
		t.Helper()
		pipe := client.Pipeline()
		for _, markIt := range marks {
			if err := markIt(ctx, pipe, user); err != nil {
				t.Fatalf("mark: %v", err)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}

	check("a cold index", "cold", false)

	mark("empty", timeline.MarkEmpty)
	check("an index confirmed empty", "empty", true)

	seedTimeline(t, client, base, timeline, "partial", seedPosts(t, 3)...)
	check("members and no marker", "partial", false)

	seedTimeline(t, client, base, timeline, "whole", seedPosts(t, 3)...)
	mark("whole", timeline.MarkLastPage)
	check("members and :lastpage", "whole", true)

	mark("stale", timeline.MarkLastPage)
	check(":lastpage on an index that holds nothing", "stale", false)

	seedTimeline(t, client, base, timeline, "first", seedPosts(t, 3)...)
	mark("first", timeline.MarkFirstPage)
	check(":firstpage says nothing about the end", "first", false)
}
