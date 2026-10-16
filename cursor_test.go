package redifu

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"
)

// postWithId builds a post whose randId the test chooses, so that ties between members
// sharing one score have a known order.
func postWithId(t *testing.T, randId string, createdAt time.Time) *Post {
	t.Helper()
	post := newPost(t, randId, createdAt)
	post.RandId = randId
	return post
}

// timelineOrder sorts posts the way the index holds them: ZREVRANGE order (score, then
// member, both descending) for a descending timeline, ZRANGE order for an ascending one.
// Go compares strings byte by byte, which is how Redis orders equal-score members.
func timelineOrder(posts []*Post, direction string) []string {
	sorted := append([]*Post(nil), posts...)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		as, bs := a.GetCreatedAt().UnixMilli(), b.GetCreatedAt().UnixMilli()
		if direction == Descending {
			if as != bs {
				return as > bs
			}
			return a.GetRandId() > b.GetRandId()
		}
		if as != bs {
			return as < bs
		}
		return a.GetRandId() < b.GetRandId()
	})
	ids := make([]string, 0, len(sorted))
	for _, post := range sorted {
		ids = append(ids, post.GetRandId())
	}
	return ids
}

// walkFrom scrolls a whole timeline with position cursors, the way a client would.
func walkFrom(t *testing.T, timeline *Timeline[*Post], keyParam string) []string {
	t.Helper()
	ctx := context.Background()

	var seen []string
	cursor := Cursor{}
	for page := 0; page < 100; page++ {
		output := timeline.FetchFrom(cursor).WithParams(keyParam).Exec(ctx)
		if output.Error() != nil {
			t.Fatalf("page %d: %v", page, output.Error())
		}
		for _, item := range output.Items() {
			seen = append(seen, item.GetRandId())
		}
		if !output.HasMore() {
			return seen
		}
		cursor = output.NextCursor()
	}
	t.Fatal("the walk did not end")
	return nil
}

func equalIds(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d ids %v, want %d %v", label, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: position %d = %s, want %s\n got  %v\n want %v", label, i, got[i], want[i], got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// L — a position cursor walks the index in its exact order
// ---------------------------------------------------------------------------

func TestFetchFromWalksEveryPageOnce(t *testing.T) {
	scenarioFetchFromWalksEveryPageOnce(t, miniClient)
}

func scenarioFetchFromWalksEveryPageOnce(t *testing.T, newClient clientFactory) {
	for _, direction := range []string{Descending, Ascending} {
		t.Run(direction, func(t *testing.T) {
			client := newClient(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 3, direction, indexTTL)

			posts := seedPosts(t, 7)
			seedTimeline(t, client, base, timeline, "u1", posts...)

			equalIds(t, "walk", walkFrom(t, timeline, "u1"), timelineOrder(posts, direction))
		})
	}
}

func TestFetchFromOrdersTiesByMemberByteWise(t *testing.T) {
	scenarioFetchFromOrdersTiesByMemberByteWise(t, miniClient)
}

func scenarioFetchFromOrdersTiesByMemberByteWise(t *testing.T, newClient clientFactory) {
	// Upper- and lower-case members: byte order puts every capital before every
	// lower-case letter, which most locales' collation does not.
	ids := []string{"b2", "A1", "a1", "Z9", "B2", "z0", "m5", "M5"}
	sameInstant := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, direction := range []string{Descending, Ascending} {
		t.Run(direction, func(t *testing.T) {
			client := newClient(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 3, direction, indexTTL)

			posts := make([]*Post, 0, len(ids)+2)
			for _, id := range ids {
				posts = append(posts, postWithId(t, id, sameInstant))
			}
			// One neighbour on each side of the tie group.
			posts = append(posts,
				postWithId(t, "older", sameInstant.Add(-time.Minute)),
				postWithId(t, "newer", sameInstant.Add(time.Minute)),
			)
			seedTimeline(t, client, base, timeline, "u1", posts...)

			equalIds(t, "walk", walkFrom(t, timeline, "u1"), timelineOrder(posts, direction))
		})
	}
}

func TestFetchFromContinuesAfterARemovedCursor(t *testing.T) {
	scenarioFetchFromContinuesAfterARemovedCursor(t, miniClient)
}

func scenarioFetchFromContinuesAfterARemovedCursor(t *testing.T, newClient clientFactory) {
	for _, direction := range []string{Descending, Ascending} {
		t.Run(direction, func(t *testing.T) {
			ctx := context.Background()
			client := newClient(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, direction, indexTTL)

			posts := seedPosts(t, 6)
			seedTimeline(t, client, base, timeline, "u1", posts...)
			order := timelineOrder(posts, direction)

			first := timeline.FetchFrom(Cursor{}).WithParams("u1").Exec(ctx)
			if first.Error() != nil {
				t.Fatalf("first page: %v", first.Error())
			}
			cursor := first.NextCursor()
			if cursor.Member != order[1] {
				t.Fatalf("NextCursor().Member = %s, want %s", cursor.Member, order[1])
			}

			// The item the client's cursor points at leaves the index: deleted, or moved
			// to another status.
			for _, post := range posts {
				if post.GetRandId() == cursor.Member {
					if err := timeline.RemoveItem(ctx, post, "u1"); err != nil {
						t.Fatalf("RemoveItem: %v", err)
					}
				}
			}

			// A randId cursor can no longer be resolved...
			byId := timeline.Fetch([]string{cursor.Member}).WithParams("u1").Exec(ctx)
			if !errors.Is(byId.Error(), ErrResetPagination) {
				t.Fatalf("Fetch by randId = %v, want ErrResetPagination", byId.Error())
			}

			// ...but the position still is.
			next := timeline.FetchFrom(cursor).WithParams("u1").Exec(ctx)
			if next.Error() != nil {
				t.Fatalf("FetchFrom after the cursor left the index: %v", next.Error())
			}
			got := make([]string, 0, 2)
			for _, item := range next.Items() {
				got = append(got, item.GetRandId())
			}
			equalIds(t, "page after the removed cursor", got, order[2:4])
			if !next.HasMore() || next.Position() != MiddlePage {
				t.Fatalf("HasMore = %v, Position = %s, want true and %s", next.HasMore(), next.Position(), MiddlePage)
			}
		})
	}
}

func TestFetchFromInsideATieGroupWithoutItsMember(t *testing.T) {
	scenarioFetchFromInsideATieGroupWithoutItsMember(t, miniClient)
}

func scenarioFetchFromInsideATieGroupWithoutItsMember(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	sameInstant := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	score := float64(sameInstant.UnixMilli())

	cases := []struct {
		direction string
		want      []string
	}{
		// ZREVRANGE order of the tie group is e, d, b, a: "c" sits between d and b.
		{Descending, []string{"b", "a", "older"}},
		// ZRANGE order is a, b, d, e: "c" sits between b and d.
		{Ascending, []string{"d", "e", "newer"}},
	}

	for _, tc := range cases {
		t.Run(tc.direction, func(t *testing.T) {
			client := newClient(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 3, tc.direction, indexTTL)

			seedTimeline(t, client, base, timeline, "u1",
				postWithId(t, "a", sameInstant),
				postWithId(t, "b", sameInstant),
				postWithId(t, "d", sameInstant),
				postWithId(t, "e", sameInstant),
				postWithId(t, "older", sameInstant.Add(-time.Minute)),
				postWithId(t, "newer", sameInstant.Add(time.Minute)),
			)

			// "c" was never a member; the position alone decides the page.
			output := timeline.FetchFrom(Cursor{Score: score, Member: "c"}).WithParams("u1").Exec(ctx)
			if output.Error() != nil {
				t.Fatalf("FetchFrom: %v", output.Error())
			}
			got := make([]string, 0, 3)
			for _, item := range output.Items() {
				got = append(got, item.GetRandId())
			}
			equalIds(t, "page after (score, c)", got, tc.want)
		})
	}
}

func TestFetchFromWithAScoreOnlyCursor(t *testing.T) {
	scenarioFetchFromWithAScoreOnlyCursor(t, miniClient)
}

func scenarioFetchFromWithAScoreOnlyCursor(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		direction string
		want      []string
	}{
		// An empty member sorts below every member: after every member at that score
		// in a descending timeline, so the page holds only what is older...
		{Descending, []string{"older"}},
		// ...and before them in an ascending one, so the page starts at that score.
		{Ascending, []string{"x", "y", "newer"}},
	}

	for _, tc := range cases {
		t.Run(tc.direction, func(t *testing.T) {
			client := newClient(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 5, tc.direction, indexTTL)

			seedTimeline(t, client, base, timeline, "u1",
				postWithId(t, "x", at),
				postWithId(t, "y", at),
				postWithId(t, "older", at.Add(-time.Minute)),
				postWithId(t, "newer", at.Add(time.Minute)),
			)

			output := timeline.FetchFrom(Cursor{Score: float64(at.UnixMilli())}).WithParams("u1").Exec(ctx)
			if output.Error() != nil {
				t.Fatalf("FetchFrom: %v", output.Error())
			}
			got := make([]string, 0, 3)
			for _, item := range output.Items() {
				got = append(got, item.GetRandId())
			}
			equalIds(t, "page after a score-only cursor", got, tc.want)
		})
	}
}

func TestFetchFromAtTheEndOfTheIndex(t *testing.T) {
	scenarioFetchFromAtTheEndOfTheIndex(t, miniClient)
}

func scenarioFetchFromAtTheEndOfTheIndex(t *testing.T, newClient clientFactory) {
	for _, direction := range []string{Descending, Ascending} {
		t.Run(direction, func(t *testing.T) {
			ctx := context.Background()
			client := newClient(t)
			base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
			timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, direction, indexTTL)

			posts := seedPosts(t, 5)
			seedTimeline(t, client, base, timeline, "u1", posts...)
			order := timelineOrder(posts, direction)
			scoreOf := map[string]float64{}
			for _, post := range posts {
				scoreOf[post.GetRandId()] = float64(post.GetCreatedAt().UnixMilli())
			}
			at := func(i int) Cursor { return Cursor{Score: scoreOf[order[i]], Member: order[i]} }

			// Three members left after the cursor: a full page and one more.
			output := timeline.FetchFrom(at(1)).WithParams("u1").Exec(ctx)
			if !output.HasMore() || len(output.Items()) != 2 {
				t.Fatalf("3 left: %d items, HasMore = %v, want 2 and true", len(output.Items()), output.HasMore())
			}

			// Exactly one page left.
			output = timeline.FetchFrom(at(2)).WithParams("u1").Exec(ctx)
			if output.HasMore() || len(output.Items()) != 2 || output.Position() != LastPage {
				t.Fatalf("2 left: %d items, HasMore = %v, Position = %s, want 2, false, %s",
					len(output.Items()), output.HasMore(), output.Position(), LastPage)
			}

			// The cursor is the last member: an empty page that ends where it started.
			last := at(4)
			output = timeline.FetchFrom(last).WithParams("u1").Exec(ctx)
			if output.Error() != nil {
				t.Fatalf("FetchFrom: %v", output.Error())
			}
			if len(output.Items()) != 0 || output.HasMore() || output.Position() != LastPage {
				t.Fatalf("at the end: %d items, HasMore = %v, Position = %s", len(output.Items()), output.HasMore(), output.Position())
			}
			if output.NextCursor() != last {
				t.Fatalf("NextCursor() = %+v on an empty page, want the cursor it was read after %+v", output.NextCursor(), last)
			}

			// Past every member.
			beyond := Cursor{Score: scoreOf[order[4]] - 1, Member: "zzz"}
			if direction == Ascending {
				beyond.Score = scoreOf[order[4]] + 1
			}
			output = timeline.FetchFrom(beyond).WithParams("u1").Exec(ctx)
			if output.Error() != nil || len(output.Items()) != 0 || output.HasMore() {
				t.Fatalf("beyond the end: %d items, HasMore = %v, err = %v", len(output.Items()), output.HasMore(), output.Error())
			}
		})
	}
}

func TestNextCursorAgreesAcrossFetchPaths(t *testing.T) {
	scenarioNextCursorAgreesAcrossFetchPaths(t, miniClient)
}

func scenarioNextCursorAgreesAcrossFetchPaths(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	// An empty index: the first page has no position to report.
	empty := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if !empty.NextCursor().IsZero() {
		t.Fatalf("NextCursor() of an empty first page = %+v, want the zero Cursor", empty.NextCursor())
	}

	posts := seedPosts(t, 5)
	seedTimeline(t, client, base, timeline, "u1", posts...)

	first := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	cursor := first.NextCursor()
	if cursor.Member != first.ValidLastId() {
		t.Fatalf("NextCursor().Member = %s, ValidLastId() = %s", cursor.Member, first.ValidLastId())
	}
	stored, err := client.ZScore(ctx, indexKey("feed:u1"), cursor.Member).Result()
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}
	if cursor.Score != stored {
		t.Fatalf("NextCursor().Score = %v, the index holds %v", cursor.Score, stored)
	}

	// The next page is the same whichever cursor reads it.
	byPosition := timeline.FetchFrom(cursor).WithParams("u1").Exec(ctx)
	byRandId := timeline.Fetch([]string{first.ValidLastId()}).WithParams("u1").Exec(ctx)
	if byPosition.Error() != nil || byRandId.Error() != nil {
		t.Fatalf("FetchFrom: %v, Fetch: %v", byPosition.Error(), byRandId.Error())
	}
	if byPosition.NextCursor() != byRandId.NextCursor() {
		t.Fatalf("NextCursor() differs: %+v by position, %+v by randId", byPosition.NextCursor(), byRandId.NextCursor())
	}
	for i := range byPosition.Items() {
		if byPosition.Items()[i].GetRandId() != byRandId.Items()[i].GetRandId() {
			t.Fatal("the two cursors read different pages")
		}
	}
	if byPosition.IsFirstPage() || byPosition.Position() != MiddlePage {
		t.Fatalf("IsFirstPage = %v, Position = %s on a second page", byPosition.IsFirstPage(), byPosition.Position())
	}

	// A randId cursor that is the last member reports its own position on the empty page.
	order := timelineOrder(posts, Descending)
	tail := timeline.Fetch([]string{order[4]}).WithParams("u1").Exec(ctx)
	if tail.Error() != nil || len(tail.Items()) != 0 {
		t.Fatalf("Fetch after the last member: %d items, err = %v", len(tail.Items()), tail.Error())
	}
	if tail.NextCursor().Member != order[4] || tail.NextCursor().Score != float64(posts[0].GetCreatedAt().UnixMilli()) {
		t.Fatalf("NextCursor() = %+v, want the cursor member and its score", tail.NextCursor())
	}

	// FetchAll ends at the last member.
	all := timeline.FetchAll().WithParams("u1").Exec(ctx)
	if all.NextCursor().Member != order[4] {
		t.Fatalf("FetchAll NextCursor().Member = %s, want %s", all.NextCursor().Member, order[4])
	}
}

func TestTimelineScoreIsWhatTheIndexStores(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	posts := mustTimeline[*Post](t, client, postBase, feedKeyFormat, 10, Descending, indexTTL)

	post := newPost(t, "timed", time.Date(2026, 1, 1, 12, 30, 45, 678_912_345, time.UTC))
	seedTimeline(t, client, postBase, posts, "u1", post)

	score, err := posts.Score(post)
	if err != nil {
		t.Fatalf("Score: %v", err)
	}
	stored, err := client.ZScore(ctx, indexKey("feed:u1"), post.GetRandId()).Result()
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}
	if score != stored || score != float64(post.GetCreatedAt().UnixMilli()) {
		t.Fatalf("Score = %v, index = %v, want the Unix milliseconds of createdAt", score, stored)
	}

	// An int64 sorting reference is stored as it is.
	docBase := mustBase[*Doc](t, client, "doc:%s", baseTTL)
	docs := mustTimeline[*Doc](t, client, docBase, "docs:%s", 10, Descending, indexTTL)
	if err := docs.SetSortingReference("Ranking"); err != nil {
		t.Fatalf("SetSortingReference: %v", err)
	}
	doc := newDoc(t, "ranked", time.Now())
	doc.Ranking = 1_234_567
	pipe := client.Pipeline()
	if err := docs.IngestItem(ctx, pipe, doc, true, "u1"); err != nil {
		t.Fatalf("IngestItem: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if score, err := docs.Score(doc); err != nil || score != 1_234_567 {
		t.Fatalf("Score = %v (%v), want 1234567", score, err)
	}
}

func TestFetchFromIsOneScriptAndNoClientSideReads(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	posts := seedPosts(t, 6)
	seedTimeline(t, client, base, timeline, "u1", posts...)
	cursor := timeline.FetchFrom(Cursor{}).WithParams("u1").Exec(ctx).NextCursor()

	counter := newVerbCounter(client)
	if output := timeline.FetchFrom(cursor).WithParams("u1").Exec(ctx); output.Error() != nil {
		t.Fatalf("FetchFrom: %v", output.Error())
	}

	if got := counter.count("eval"); got != 1 {
		t.Fatalf("issued %d EVAL, want exactly 1", got)
	}
	for _, verb := range []string{"zscore", "zcount", "zrank", "zrevrank", "zrange", "zrevrange", "zrangebyscore", "zrevrangebyscore"} {
		if got := counter.count(verb); got != 0 {
			t.Fatalf("issued %d %s from the client — the position is resolved inside Redis", got, verb)
		}
	}
}

func TestFetchFromResolvesRelationsAndRunsTheProcessor(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	postBase.AddRelation(relateAuthor(t, accountBase))
	timeline := mustTimeline[*Post](t, client, postBase, feedKeyFormat, 2, Descending, indexTTL)

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("Set account: %v", err)
	}
	posts := seedPosts(t, 4)
	for _, post := range posts {
		post.AuthorRandId = author.GetRandId()
	}
	seedTimeline(t, client, postBase, timeline, "u1", posts...)

	cursor := timeline.FetchFrom(Cursor{}).WithParams("u1").Exec(ctx).NextCursor()

	var processed []interface{}
	output := timeline.FetchFrom(cursor).
		WithParams("u1").
		WithProcessor(func(p **Post, args []interface{}) {
			processed = args
			(*p).Title = "processed"
		}, "viewer").
		Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("FetchFrom: %v", output.Error())
	}
	if len(output.Items()) != 2 {
		t.Fatalf("fetched %d items, want 2", len(output.Items()))
	}
	for _, item := range output.Items() {
		if item.Author == nil || item.Author.Name != "Ada" {
			t.Fatal("FetchFrom did not resolve the relation")
		}
		if item.Title != "processed" {
			t.Fatal("FetchFrom did not run the processor")
		}
	}
	if len(processed) != 1 || processed[0] != "viewer" {
		t.Fatalf("processor received %#v, want [viewer]", processed)
	}
}

func TestFetchFromReportsDanglingMembersAndHasMoreFromTheIndex(t *testing.T) {
	scenarioFetchFromReportsDanglingMembersAndHasMoreFromTheIndex(t, miniClient)
}

func scenarioFetchFromReportsDanglingMembersAndHasMoreFromTheIndex(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)

	posts := seedPosts(t, 6)
	seedTimeline(t, client, base, timeline, "u1", posts...)
	order := timelineOrder(posts, Descending)

	cursor := timeline.FetchFrom(Cursor{}).WithParams("u1").Exec(ctx).NextCursor()
	client.Del(ctx, "post:"+order[2])
	client.Del(ctx, "post:"+order[3])

	output := timeline.FetchFrom(cursor).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("FetchFrom: %v", output.Error())
	}
	if len(output.Items()) != 0 || output.Dangling() != 2 {
		t.Fatalf("%d items, Dangling() = %d, want 0 and 2", len(output.Items()), output.Dangling())
	}
	if !output.HasMore() {
		t.Fatal("HasMore() = false — two expired items were mistaken for the end of the feed")
	}
	if output.NextCursor().Member != order[3] {
		t.Fatalf("NextCursor().Member = %s, want %s: the cursor moves past expired members", output.NextCursor().Member, order[3])
	}
}

func TestFetchFromRejectsAScoreThatIsNotANumber(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 2, Descending, indexTTL)
	seedTimeline(t, client, base, timeline, "u1", seedPosts(t, 3)...)

	output := timeline.FetchFrom(Cursor{Score: math.NaN(), Member: "x"}).WithParams("u1").Exec(ctx)
	if !errors.Is(output.Error(), ErrInvalidCursor) {
		t.Fatalf("FetchFrom(NaN) = %v, want ErrInvalidCursor", output.Error())
	}
}

func TestCursorRoundTripsThroughItsString(t *testing.T) {
	cases := []Cursor{
		{},
		{Score: 1767225600123, Member: "aB3dE5fG7hJ9kL1m"},
		{Score: 1.5, Member: "half"},
		{Score: -42, Member: "negative"},
		{Score: 5, Member: ""},
		{Score: 7, Member: "has:colons:inside"},
		{Score: float64(maxExactScore), Member: "largest exact"},
	}

	for _, cursor := range cases {
		token := cursor.String()
		parsed, err := ParseCursor(token)
		if err != nil {
			t.Fatalf("ParseCursor(%q): %v", token, err)
		}
		if parsed != cursor {
			t.Fatalf("round trip of %+v through %q gave %+v", cursor, token, parsed)
		}
	}

	if token := (Cursor{}).String(); token != "" {
		t.Fatalf("the zero Cursor renders as %q, want the empty string", token)
	}
	if token := (Cursor{Score: 1767225600123, Member: "m"}).String(); token != "1767225600123:m" {
		t.Fatalf("String() = %q, want plain digits for an integer score", token)
	}
	if (Cursor{Score: 5}).IsZero() {
		t.Fatal("a cursor with a score is not the zero Cursor, even without a member")
	}

	for _, bad := range []string{"no-separator", "x:member", "NaN:member", ":member"} {
		if _, err := ParseCursor(bad); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("ParseCursor(%q) = %v, want ErrInvalidCursor", bad, err)
		}
	}
}

func TestFetchFromWalksAThousandTies(t *testing.T) {
	scenarioFetchFromWalksAThousandTies(t, miniClient)
}

func scenarioFetchFromWalksAThousandTies(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 50, Descending, indexTTL)

	sameInstant := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	posts := make([]*Post, 0, 1000)
	for i := 0; i < 1000; i++ {
		posts = append(posts, postWithId(t, fmt.Sprintf("tie%04d", i), sameInstant))
	}
	seedTimeline(t, client, base, timeline, "u1", posts...)

	equalIds(t, "walk", walkFrom(t, timeline, "u1"), timelineOrder(posts, Descending))
}
