package redifu

// R1–R3 of the BE plan docs/plans/redifu-key-generations.md: a Base and an index carry a
// generation, and two instances of different generations share one Redis without reading each
// other's values or indexes.

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// generationStack is one build's view of the same key formats: a Base of accounts, a Base of
// posts, and a Sorted index of posts per owner.
type generationStack struct {
	accounts *Base[*Account]
	posts    *Base[*Post]
	sorted   *Sorted[*Post]
}

func newGenerationStack(t *testing.T, client redis.UniversalClient, baseGeneration, indexGeneration string) generationStack {
	t.Helper()
	accounts := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	posts := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	if err := accounts.SetGeneration(baseGeneration); err != nil {
		t.Fatalf("accounts.SetGeneration: %v", err)
	}
	if err := posts.SetGeneration(baseGeneration); err != nil {
		t.Fatalf("posts.SetGeneration: %v", err)
	}
	sorted := mustSorted[*Post](t, client, posts, "posts:of:%s", indexTTL)
	if err := sorted.SetGeneration(indexGeneration); err != nil {
		t.Fatalf("sorted.SetGeneration: %v", err)
	}
	return generationStack{accounts: accounts, posts: posts, sorted: sorted}
}

// seedSorted is a consumer seed of a whole list, in one MULTI/EXEC.
func seedGenerationSorted(t *testing.T, client redis.UniversalClient, stack generationStack, owner string, posts ...*Post) {
	t.Helper()
	ctx := context.Background()
	pipe := client.TxPipeline()
	for _, post := range posts {
		if err := stack.posts.WithPipeline(pipe).Set(ctx, post); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if err := stack.sorted.IngestItem(ctx, pipe, post, true, owner); err != nil {
			t.Fatalf("IngestItem: %v", err)
		}
	}
	if len(posts) == 0 {
		if err := stack.sorted.MarkEmpty(ctx, pipe, owner); err != nil {
			t.Fatalf("MarkEmpty: %v", err)
		}
	} else if err := stack.sorted.HasData(ctx, pipe, owner); err != nil {
		t.Fatalf("HasData: %v", err)
	}
	if err := stack.sorted.SetExpiration(ctx, pipe, owner); err != nil {
		t.Fatalf("SetExpiration: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed Exec: %v", err)
	}
}

func postIds(posts []*Post) []string {
	ids := make([]string, 0, len(posts))
	for _, post := range posts {
		ids = append(ids, post.GetRandId())
	}
	sort.Strings(ids)
	return ids
}

func sortedIds(posts ...*Post) []string { return postIds(posts) }

func requiresSeeding(t *testing.T, sorted *Sorted[*Post], owner string) bool {
	t.Helper()
	needs, err := sorted.RequiresSeeding(context.Background(), owner)
	if err != nil {
		t.Fatalf("RequiresSeeding: %v", err)
	}
	return needs
}

func keyExists(t *testing.T, client redis.UniversalClient, key string) bool {
	t.Helper()
	count, err := client.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("EXISTS %s: %v", key, err)
	}
	return count == 1
}

// ---------------------------------------------------------------------------
// R1 — Base
// ---------------------------------------------------------------------------

func TestBaseGenerationReadsOnlyItsOwnValues(t *testing.T) {
	scenarioBaseGenerationReadsOnlyItsOwnValues(t, miniClient)
}

func scenarioBaseGenerationReadsOnlyItsOwnValues(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "shape-a", "build-a")
	b := newGenerationStack(t, client, "shape-b", "build-b")

	post := newPost(t, "hello", time.Now().UTC())
	if err := a.posts.Set(ctx, post); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := a.posts.Get(ctx, post.GetRandId())
	if err != nil || got.Title != "hello" {
		t.Fatalf("own generation: got %v, %v", got, err)
	}
	if _, err := b.posts.Get(ctx, post.GetRandId()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other generation: Get = %v, want ErrNotFound", err)
	}
	many, err := b.posts.GetMany(ctx, []string{post.GetRandId()})
	if err != nil || len(many) != 0 {
		t.Fatalf("other generation: GetMany = %v, %v, want nothing", many, err)
	}
	if exists, err := b.posts.Exists(ctx, post.GetRandId()); err != nil || exists {
		t.Fatalf("other generation: Exists = %v, %v", exists, err)
	}
	if exists, err := a.posts.Exists(ctx, post.GetRandId()); err != nil || !exists {
		t.Fatalf("own generation: Exists = %v, %v", exists, err)
	}

	// The value carries the generation in front of the JSON the item encodes to.
	raw, err := client.Get(ctx, "post:"+post.GetRandId()).Result()
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if !strings.HasPrefix(raw, "shape-a\x1f{") {
		t.Fatalf("stored value %q does not start with the generation", raw)
	}

	// b writes its own value under the same key; a no longer reads it.
	post.Title = "rewritten"
	if err := b.posts.Set(ctx, post); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	if _, err := a.posts.Get(ctx, post.GetRandId()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after b wrote: a.Get = %v, want ErrNotFound", err)
	}
	if got, err := b.posts.Get(ctx, post.GetRandId()); err != nil || got.Title != "rewritten" {
		t.Fatalf("b.Get = %v, %v", got, err)
	}
}

func TestBaseGenerationTreatsAnUnprefixedValueAsMissing(t *testing.T) {
	scenarioBaseGenerationTreatsAnUnprefixedValueAsMissing(t, miniClient)
}

func scenarioBaseGenerationTreatsAnUnprefixedValueAsMissing(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	plain := mustBase[*Post](t, client, postKeyFormat, baseTTL) // a build before generations
	generational := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	if err := generational.SetGeneration("g1"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}

	post := newPost(t, "legacy", time.Now().UTC())
	if err := plain.Set(ctx, post); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := plain.Get(ctx, post.GetRandId()); err != nil || got.Title != "legacy" {
		t.Fatalf("a Base without a generation reads its value as before: %v, %v", got, err)
	}
	if _, err := generational.Get(ctx, post.GetRandId()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unprefixed value: Get = %v, want ErrNotFound", err)
	}
	if many, err := generational.GetMany(ctx, []string{post.GetRandId()}); err != nil || len(many) != 0 {
		t.Fatalf("an unprefixed value: GetMany = %v, %v", many, err)
	}
	if exists, err := generational.Exists(ctx, post.GetRandId()); err != nil || exists {
		t.Fatalf("an unprefixed value: Exists = %v, %v", exists, err)
	}
}

func TestBaseGenerationSetIfAbsent(t *testing.T) {
	scenarioBaseGenerationSetIfAbsent(t, miniClient)
}

func scenarioBaseGenerationSetIfAbsent(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "a", "a")
	b := newGenerationStack(t, client, "b", "b")

	// Absent: written.
	post := newPost(t, "full", time.Now().UTC())
	if err := a.posts.SetIfAbsent(ctx, post); err != nil {
		t.Fatalf("SetIfAbsent: %v", err)
	}
	if got, err := a.posts.Get(ctx, post.GetRandId()); err != nil || got.Title != "full" {
		t.Fatalf("absent: Get = %v, %v", got, err)
	}

	// Present in this generation: kept (a stub never overwrites the full value), TTL renewed.
	if err := client.PExpire(ctx, "post:"+post.GetRandId(), time.Minute).Err(); err != nil {
		t.Fatalf("PEXPIRE: %v", err)
	}
	stub := &Post{Record: &Record{RandId: post.GetRandId()}, Title: "stub"}
	if err := a.posts.SetIfAbsent(ctx, stub); err != nil {
		t.Fatalf("SetIfAbsent stub: %v", err)
	}
	if got, err := a.posts.Get(ctx, post.GetRandId()); err != nil || got.Title != "full" {
		t.Fatalf("own value was overwritten: %v, %v", got, err)
	}
	if ttl := client.PTTL(ctx, "post:"+post.GetRandId()).Val(); ttl < baseTTL-time.Minute {
		t.Fatalf("TTL was not renewed: %v", ttl)
	}

	// Present in another generation: replaced, through a pipeline too.
	pipe := client.Pipeline()
	other := &Post{Record: &Record{RandId: post.GetRandId()}, Title: "b's"}
	if err := b.posts.WithPipeline(pipe).SetIfAbsent(ctx, other); err != nil {
		t.Fatalf("SetIfAbsent b: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if got, err := b.posts.Get(ctx, post.GetRandId()); err != nil || got.Title != "b's" {
		t.Fatalf("foreign value was kept: %v, %v", got, err)
	}
}

func TestBaseGenerationRelationOfAnotherGenerationIsNil(t *testing.T) {
	scenarioBaseGenerationRelationOfAnotherGenerationIsNil(t, miniClient)
}

func scenarioBaseGenerationRelationOfAnotherGenerationIsNil(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	accountsA := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	accountsB := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	posts := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	_ = accountsA.SetGeneration("a")
	_ = accountsB.SetGeneration("b")
	relation, err := Relate(accountsA, func(p *Post) string { return p.AuthorRandId }, func(p *Post, a *Account) { p.Author = a })
	if err != nil {
		t.Fatalf("Relate: %v", err)
	}
	posts.AddRelation(relation)

	author := newAccount(t, "ann")
	if err := accountsB.Set(ctx, author); err != nil { // written by the other generation
		t.Fatalf("Set: %v", err)
	}
	post := newPost(t, "p", time.Now().UTC())
	post.AuthorRandId = author.GetRandId()
	if err := posts.Set(ctx, post); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := posts.Get(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Author != nil {
		t.Fatalf("a related value of another generation resolved: %+v", got.Author)
	}

	if err := accountsA.Set(ctx, author); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	got, err = posts.Get(ctx, post.GetRandId())
	if err != nil || got.Author == nil || got.Author.Name != "ann" {
		t.Fatalf("own generation's relation did not resolve: %+v, %v", got, err)
	}
}

func TestSortedExecReportCountsAForeignValueAsDangling(t *testing.T) {
	scenarioSortedExecReportCountsAForeignValueAsDangling(t, miniClient)
}

func scenarioSortedExecReportCountsAForeignValueAsDangling(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	// Same index generation, different Base generations: the shape of the entity changed but
	// the rule of the list did not.
	a := newGenerationStack(t, client, "shape-a", "build")
	b := newGenerationStack(t, client, "shape-b", "build")
	a.sorted.SetSelfHeal(false)

	p1, p2 := newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC())
	seedGenerationSorted(t, client, a, "o1", p1, p2)
	if err := b.posts.Set(ctx, p2); err != nil { // b rewrote p2 in its own shape
		t.Fatalf("Set: %v", err)
	}

	items, dangling, err := a.sorted.Fetch(Descending).WithParams("o1").ExecReport(ctx)
	if err != nil {
		t.Fatalf("ExecReport: %v", err)
	}
	if dangling != 1 || len(items) != 1 || items[0].GetRandId() != p1.GetRandId() {
		t.Fatalf("got %d items, %d dangling; want p1 and one dangling", len(items), dangling)
	}
}

// ---------------------------------------------------------------------------
// R2 — Sorted
// ---------------------------------------------------------------------------

func TestSortedGenerationPingPongSeed(t *testing.T) {
	scenarioSortedGenerationPingPongSeed(t, miniClient)
}

func scenarioSortedGenerationPingPongSeed(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "shape", "build-a")
	b := newGenerationStack(t, client, "shape", "build-b")
	p1, p2, p3 := newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC()), newPost(t, "3", time.Now().UTC())

	if !requiresSeeding(t, a.sorted, "o1") || !requiresSeeding(t, b.sorted, "o1") {
		t.Fatal("a cold index must need seeding in both generations")
	}

	// a's rule puts p1 and p2 in the list.
	seedGenerationSorted(t, client, a, "o1", p1, p2)
	if requiresSeeding(t, a.sorted, "o1") {
		t.Fatal("a: its own seed must be warm")
	}
	if got, _ := client.Get(ctx, "{posts:of:o1}:gen").Result(); got != "build-a" {
		t.Fatalf("marker = %q, want build-a", got)
	}

	// b does not read a's index: it needs seeding, and the check dropped it.
	if !requiresSeeding(t, b.sorted, "o1") {
		t.Fatal("b: a's index must need seeding")
	}
	if keyExists(t, client, "{posts:of:o1}") || keyExists(t, client, "{posts:of:o1}:gen") {
		t.Fatal("b's check must drop a's index and its marker")
	}

	// b's rule puts only p3 in it.
	seedGenerationSorted(t, client, b, "o1", p3)
	items, err := b.sorted.Fetch(Descending).WithParams("o1").Exec(ctx)
	if err != nil {
		t.Fatalf("b Fetch: %v", err)
	}
	if got := postIds(items); strings.Join(got, ",") != strings.Join(sortedIds(p3), ",") {
		t.Fatalf("b reads %v, want only p3", got)
	}

	// And back: a's fetch reads nothing of b's — it answers ErrNotSeeded, never an empty list —
	// and a needs seeding again.
	items, err = a.sorted.Fetch(Descending).WithParams("o1").Exec(ctx)
	if !errors.Is(err, ErrNotSeeded) {
		t.Fatalf("a Fetch of b's index = %v, %v; want ErrNotSeeded", postIds(items), err)
	}
	if len(items) != 0 {
		t.Fatalf("a read b's index: %v", postIds(items))
	}
	if count, err := a.sorted.Count(ctx, "o1"); err != nil || count != 0 {
		t.Fatalf("a Count = %d, %v", count, err)
	}
	if !requiresSeeding(t, a.sorted, "o1") {
		t.Fatal("a: b's index must need seeding")
	}
	seedGenerationSorted(t, client, a, "o1", p1, p2)
	items, err = a.sorted.Fetch(Descending).WithParams("o1").Exec(ctx)
	if err != nil || strings.Join(postIds(items), ",") != strings.Join(sortedIds(p1, p2), ",") {
		t.Fatalf("a reseeded: %v, %v", postIds(items), err)
	}

	// A confirmed-empty list of one generation is not confirmed empty for the other.
	seedGenerationSorted(t, client, a, "o2")
	if requiresSeeding(t, a.sorted, "o2") {
		t.Fatal("a: its empty list must be warm")
	}
	if empty, err := a.sorted.IsEmpty(ctx, "o2"); err != nil || !empty {
		t.Fatalf("a IsEmpty = %v, %v", empty, err)
	}
	if empty, err := b.sorted.IsEmpty(ctx, "o2"); err != nil || empty {
		t.Fatalf("b IsEmpty = %v, %v (a's marker must not count)", empty, err)
	}
	if !requiresSeeding(t, b.sorted, "o2") {
		t.Fatal("b: a's empty list must need seeding")
	}
}

func TestSortedGenerationWritesAcrossGenerationsDropTheIndex(t *testing.T) {
	scenarioSortedGenerationWritesAcrossGenerationsDropTheIndex(t, miniClient)
}

func scenarioSortedGenerationWritesAcrossGenerationsDropTheIndex(t *testing.T, newClient clientFactory) {
	ctx := context.Background()
	p1, p2, p3 := newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC()), newPost(t, "3", time.Now().UTC())

	t.Run("AddItem", func(t *testing.T) {
		client := newClient(t)
		a := newGenerationStack(t, client, "shape", "build-a")
		b := newGenerationStack(t, client, "shape", "build-b")
		seedGenerationSorted(t, client, a, "o1", p1, p2)

		// The old build's drain adds an item to the new build's index: the index goes.
		if err := b.sorted.AddItem(ctx, p3, "o1"); !errors.Is(err, ErrNotIngested) {
			t.Fatalf("AddItem across generations = %v, want ErrNotIngested", err)
		}
		for _, key := range []string{"{posts:of:o1}", "{posts:of:o1}:blankpage", "{posts:of:o1}:gen"} {
			if keyExists(t, client, key) {
				t.Fatalf("%s survived a write of another generation", key)
			}
		}
		if !requiresSeeding(t, a.sorted, "o1") {
			t.Fatal("a must seed again")
		}
		// The item itself still reached Base: the next seed of either one reads it.
		if _, err := b.posts.Get(ctx, p3.GetRandId()); err != nil {
			t.Fatalf("p3 is not in Base: %v", err)
		}

		// Within one generation AddItem works as before.
		seedGenerationSorted(t, client, a, "o1", p1, p2)
		if err := a.sorted.AddItem(ctx, p3, "o1"); err != nil {
			t.Fatalf("AddItem own generation: %v", err)
		}
		items, _ := a.sorted.Fetch(Descending).WithParams("o1").Exec(ctx)
		if strings.Join(postIds(items), ",") != strings.Join(sortedIds(p1, p2, p3), ",") {
			t.Fatalf("a reads %v", postIds(items))
		}
	})

	t.Run("AddItemThroughAPipeline", func(t *testing.T) {
		client := newClient(t)
		a := newGenerationStack(t, client, "shape", "build-a")
		b := newGenerationStack(t, client, "shape", "build-b")
		seedGenerationSorted(t, client, a, "o1", p1)
		pipe := client.TxPipeline()
		if err := b.sorted.WithPipeline(pipe).AddItem(ctx, p2, "o1"); err != nil {
			t.Fatalf("AddItem: %v", err)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
		if keyExists(t, client, "{posts:of:o1}") {
			t.Fatal("a pipelined AddItem of another generation left the index")
		}
	})

	t.Run("RemoveItem", func(t *testing.T) {
		client := newClient(t)
		a := newGenerationStack(t, client, "shape", "build-a")
		b := newGenerationStack(t, client, "shape", "build-b")
		seedGenerationSorted(t, client, a, "o1", p1, p2)

		if err := b.sorted.RemoveItem(ctx, p1, "o1"); !errors.Is(err, ErrNotIngested) {
			t.Fatalf("RemoveItem across generations = %v, want ErrNotIngested", err)
		}
		if keyExists(t, client, "{posts:of:o1}") || keyExists(t, client, "{posts:of:o1}:gen") {
			t.Fatal("RemoveItem of another generation left the index")
		}

		seedGenerationSorted(t, client, a, "o1", p1, p2)
		if err := a.sorted.RemoveItem(ctx, p1, "o1"); err != nil {
			t.Fatalf("RemoveItem own generation: %v", err)
		}
		items, _ := a.sorted.Fetch(Descending).WithParams("o1").Exec(ctx)
		if strings.Join(postIds(items), ",") != strings.Join(sortedIds(p2), ",") {
			t.Fatalf("a reads %v", postIds(items))
		}
	})

	t.Run("AddItemIntoAnEmptyListOfAnotherGeneration", func(t *testing.T) {
		client := newClient(t)
		a := newGenerationStack(t, client, "shape", "build-a")
		b := newGenerationStack(t, client, "shape", "build-b")
		seedGenerationSorted(t, client, a, "o1") // confirmed empty by a
		if err := b.sorted.AddItem(ctx, p1, "o1"); !errors.Is(err, ErrNotIngested) {
			t.Fatalf("AddItem = %v", err)
		}
		if keyExists(t, client, "{posts:of:o1}:blankpage") {
			t.Fatal("a's empty marker survived")
		}
		if !requiresSeeding(t, a.sorted, "o1") {
			t.Fatal("a must seed again, the new item included")
		}
	})
}

func TestSortedGenerationSeedOverAForeignIndexStartsEmpty(t *testing.T) {
	scenarioSortedGenerationSeedOverAForeignIndexStartsEmpty(t, miniClient)
}

func scenarioSortedGenerationSeedOverAForeignIndexStartsEmpty(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "shape", "build-a")
	b := newGenerationStack(t, client, "shape", "build-b")
	p1, p2, p3 := newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC()), newPost(t, "3", time.Now().UTC())

	// b saw the index cold and seeds it while a's seed lands first: b's seed must not merge
	// into a's members.
	seedGenerationSorted(t, client, a, "o1", p1, p2)
	seedGenerationSorted(t, client, b, "o1", p3)
	members, err := client.ZRange(ctx, "{posts:of:o1}", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRANGE: %v", err)
	}
	if len(members) != 1 || members[0] != p3.GetRandId() {
		t.Fatalf("index holds %v, want only b's p3", members)
	}
	if got, _ := client.Get(ctx, "{posts:of:o1}:gen").Result(); got != "build-b" {
		t.Fatalf("marker = %q", got)
	}
}

func TestSortedGenerationPurgeDropsTheMarker(t *testing.T) {
	scenarioSortedGenerationPurgeDropsTheMarker(t, miniClient)
}

func scenarioSortedGenerationPurgeDropsTheMarker(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "shape", "build-a")
	seedGenerationSorted(t, client, a, "o1", newPost(t, "1", time.Now().UTC()))
	seedGenerationSorted(t, client, a, "o2")

	for _, owner := range []string{"o1", "o2"} {
		if err := a.sorted.Purge(ctx, owner); err != nil {
			t.Fatalf("Purge: %v", err)
		}
		for _, suffix := range []string{"", ":blankpage", ":gen"} {
			if key := "{posts:of:" + owner + "}" + suffix; keyExists(t, client, key) {
				t.Fatalf("%s survived Purge", key)
			}
		}
		if !requiresSeeding(t, a.sorted, owner) {
			t.Fatalf("%s: a purged index must need seeding", owner)
		}
	}
}

func TestSortedGenerationMarkerLivesWithTheIndex(t *testing.T) {
	scenarioSortedGenerationMarkerLivesWithTheIndex(t, miniClient)
}

func scenarioSortedGenerationMarkerLivesWithTheIndex(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	a := newGenerationStack(t, client, "shape", "build-a")
	seedGenerationSorted(t, client, a, "o1", newPost(t, "1", time.Now().UTC()))

	setTTL := client.PTTL(ctx, "{posts:of:o1}").Val()
	markerTTL := client.PTTL(ctx, "{posts:of:o1}:gen").Val()
	if setTTL <= 0 || markerTTL <= 0 || setTTL-markerTTL > time.Second || markerTTL-setTTL > time.Second {
		t.Fatalf("set TTL %v, marker TTL %v: they must be the index TTL together", setTTL, markerTTL)
	}

	// SetExpiration renews both.
	_ = client.PExpire(ctx, "{posts:of:o1}", time.Minute).Err()
	_ = client.PExpire(ctx, "{posts:of:o1}:gen", time.Minute).Err()
	pipe := client.TxPipeline()
	if err := a.sorted.SetExpiration(ctx, pipe, "o1"); err != nil {
		t.Fatalf("SetExpiration: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if client.PTTL(ctx, "{posts:of:o1}").Val() < indexTTL-time.Minute || client.PTTL(ctx, "{posts:of:o1}:gen").Val() < indexTTL-time.Minute {
		t.Fatal("SetExpiration did not renew the set and its marker")
	}

	// A marker gone on its own (evicted) drops the index on the next read.
	if err := client.Del(ctx, "{posts:of:o1}:gen").Err(); err != nil {
		t.Fatalf("DEL: %v", err)
	}
	if !requiresSeeding(t, a.sorted, "o1") {
		t.Fatal("an index without its marker must need seeding")
	}
	if keyExists(t, client, "{posts:of:o1}") {
		t.Fatal("an index without its marker must be dropped")
	}
}

// ---------------------------------------------------------------------------
// R2 — Timeline
// ---------------------------------------------------------------------------

func generationFeedStores(t *testing.T, newClient clientFactory, baseA, indexA, baseB, indexB string) (*feedStore, *feedStore, context.Context) {
	t.Helper()
	client := newClient(t)
	a, ctx := newFeedStore(t, client, Descending, 50)
	b, _ := newFeedStore(t, client, Descending, 0)
	b.table = a.table // one database
	for _, store := range []struct {
		s           *feedStore
		base, index string
	}{{a, baseA, indexA}, {b, baseB, indexB}} {
		if err := store.s.base.SetGeneration(store.base); err != nil {
			t.Fatalf("Base.SetGeneration: %v", err)
		}
		if err := store.s.timeline.SetGeneration(store.index); err != nil {
			t.Fatalf("Timeline.SetGeneration: %v", err)
		}
		store.s.tx = true
	}
	return a, b, ctx
}

func TestTimelineGenerationPingPongDocumentedFlow(t *testing.T) {
	scenarioTimelineGenerationPingPongDocumentedFlow(t, miniClient)
}

func scenarioTimelineGenerationPingPongDocumentedFlow(t *testing.T, newClient clientFactory) {
	for _, shapes := range []struct{ name, baseA, baseB string }{
		{"SameShape", "shape", "shape"},
		{"ShapeChanged", "shape-a", "shape-b"},
	} {
		t.Run(shapes.name, func(t *testing.T) {
			a, b, ctx := generationFeedStores(t, newClient, shapes.baseA, "build-a", shapes.baseB, "build-b")
			want := a.table.order(t, a.timeline)

			equalIds(t, "a walks", a.walk(ctx), want)
			queries := a.table.queries
			equalIds(t, "a walks warm", a.walk(ctx), want)
			if a.table.queries != queries {
				t.Fatalf("a's second walk read the database %d times", a.table.queries-queries)
			}

			// b reads the same feed: a's index is not b's, so b seeds it its own way...
			equalIds(t, "b walks", b.walk(ctx), want)
			if b.table.queries == queries {
				t.Fatal("b served a's index")
			}
			if got, _ := a.client.Get(ctx, "{feed:u1}:gen").Result(); got != "build-b" {
				t.Fatalf("marker = %q after b's walk", got)
			}

			// ...and a seeds it back.
			queries = a.table.queries
			equalIds(t, "a walks after b", a.walk(ctx), want)
			if a.table.queries == queries {
				t.Fatal("a served b's index")
			}

			// The randId flow resets a cursor handed out by the other generation.
			first, err := a.pageByRandId(ctx, nil)
			if err != nil {
				t.Fatalf("a first page: %v", err)
			}
			page, err := b.pageByRandId(ctx, []string{first.lastId})
			if err != nil {
				t.Fatalf("b page: %v", err)
			}
			if !page.reset {
				t.Fatal("a cursor into another generation's index must reset the client")
			}
		})
	}
}

func TestTimelineGenerationWritesAcrossGenerationsDropTheIndex(t *testing.T) {
	scenarioTimelineGenerationWritesAcrossGenerationsDropTheIndex(t, miniClient)
}

func scenarioTimelineGenerationWritesAcrossGenerationsDropTheIndex(t *testing.T, newClient clientFactory) {
	keys := []string{"{feed:u1}", "{feed:u1}:firstpage", "{feed:u1}:lastpage", "{feed:u1}:blankpage", "{feed:u1}:gen"}

	t.Run("AddItem", func(t *testing.T) {
		a, b, ctx := generationFeedStores(t, newClient, "shape", "build-a", "shape", "build-b")
		a.walk(ctx) // fully seeded, :lastpage set
		if !keyExists(t, a.client, "{feed:u1}:lastpage") {
			t.Fatal("the walk must reach the end of the table")
		}

		fresh := newPost(t, "fresh", time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
		a.table.rows = append(a.table.rows, fresh)
		if err := b.timeline.AddItem(ctx, fresh, "u1"); !errors.Is(err, ErrNotIngested) {
			t.Fatalf("AddItem across generations = %v, want ErrNotIngested", err)
		}
		for _, key := range keys {
			if keyExists(t, a.client, key) {
				t.Fatalf("%s survived a write of another generation", key)
			}
		}
		// a never serves a feed without the new item: it seeds again.
		equalIds(t, "a after b's write", a.walk(ctx), a.table.order(t, a.timeline))

		// Within one generation the new item enters as before.
		newer := newPost(t, "newer", time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC))
		a.table.rows = append(a.table.rows, newer)
		if err := a.timeline.AddItem(ctx, newer, "u1"); err != nil {
			t.Fatalf("AddItem own generation: %v", err)
		}
		queries := a.table.queries
		equalIds(t, "a after its own write", a.walk(ctx), a.table.order(t, a.timeline))
		if a.table.queries != queries {
			t.Fatal("an item added in the same generation must not cost a reseed")
		}
	})

	t.Run("RemoveItem", func(t *testing.T) {
		a, b, ctx := generationFeedStores(t, newClient, "shape", "build-a", "shape", "build-b")
		a.walk(ctx)
		gone := a.table.rows[3]
		a.table.remove(gone.GetRandId())
		if err := b.timeline.RemoveItem(ctx, gone, "u1"); !errors.Is(err, ErrNotIngested) {
			t.Fatalf("RemoveItem across generations = %v, want ErrNotIngested", err)
		}
		for _, key := range keys {
			if keyExists(t, a.client, key) {
				t.Fatalf("%s survived a removal of another generation", key)
			}
		}
		equalIds(t, "a after b's removal", a.walk(ctx), a.table.order(t, a.timeline))
	})

	t.Run("ReachedEndOfAForeignIndex", func(t *testing.T) {
		a, b, ctx := generationFeedStores(t, newClient, "shape", "build-a", "shape", "build-b")
		a.walk(ctx)
		end, err := b.timeline.ReachedEnd(ctx, "u1")
		if err != nil || end {
			t.Fatalf("ReachedEnd of another generation's index = %v, %v", end, err)
		}
		if tail, nonEmpty, err := b.timeline.Tail(ctx, "u1"); err != nil || nonEmpty {
			t.Fatalf("Tail of a dropped index = %v, %v, %v", tail, nonEmpty, err)
		}
	})
}

// ---------------------------------------------------------------------------
// R2 — Page and TimeSeries
// ---------------------------------------------------------------------------

func TestPageGeneration(t *testing.T) {
	scenarioPageGeneration(t, miniClient)
}

func scenarioPageGeneration(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	a := mustPage[*Post](t, client, base, "pages:%s", 2, Descending, indexTTL)
	b := mustPage[*Post](t, client, base, "pages:%s", 2, Descending, indexTTL)
	if err := a.SetGeneration("a"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	if err := b.SetGeneration("b"); err != nil {
		t.Fatalf("SetGeneration: %v", err)
	}
	p1, p2 := newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC())

	seedPage := func(page *Page[*Post], posts ...*Post) {
		pipe := client.TxPipeline()
		for _, post := range posts {
			_ = base.WithPipeline(pipe).Set(ctx, post)
			if err := page.IngestItem(ctx, pipe, post, 1, "o1"); err != nil {
				t.Fatalf("IngestItem: %v", err)
			}
		}
		_ = page.AddPage(ctx, pipe, 1, "o1")
		_ = page.SetExpiration(ctx, pipe, 1, "o1")
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}

	seedPage(a, p1, p2)
	if needs, err := a.RequiresSeeding(ctx, 1, "o1"); err != nil || needs {
		t.Fatalf("a RequiresSeeding = %v, %v", needs, err)
	}
	if items, err := b.Fetch(1).WithParams("o1").Exec(ctx); !errors.Is(err, ErrNotSeeded) || len(items) != 0 {
		t.Fatalf("b read a's page: %v, %v; want ErrNotSeeded", items, err)
	}
	if needs, err := b.RequiresSeeding(ctx, 1, "o1"); err != nil || !needs {
		t.Fatalf("b RequiresSeeding = %v, %v", needs, err)
	}
	seedPage(b, p2)
	if items, err := b.Fetch(1).WithParams("o1").Exec(ctx); err != nil || len(items) != 1 {
		t.Fatalf("b page = %v, %v", items, err)
	}
	if err := a.AddItem(ctx, p1, 1, "o1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("a AddItem into b's page = %v", err)
	}
	if keyExists(t, client, "{pages:o1:page:1}") {
		t.Fatal("a's write left b's page")
	}

	seedPage(a, p1, p2)
	if err := a.Purge(ctx, "o1"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	for _, key := range []string{"{pages:o1:page:1}", "{pages:o1:page:1}:gen", "pages:o1:page-index"} {
		if keyExists(t, client, key) {
			t.Fatalf("%s survived Purge", key)
		}
	}
}

func TestTimeSeriesGeneration(t *testing.T) {
	scenarioTimeSeriesGeneration(t, miniClient)
}

func scenarioTimeSeriesGeneration(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	base := mustBase[*Booking](t, client, "booking:%s", baseTTL)
	newSeries := func(generation string) *TimeSeries[*Booking] {
		series := mustTimeSeries[*Booking](t, client, base, "cal:%s", indexTTL)
		if err := series.SetSortingReference("StartAt"); err != nil {
			t.Fatalf("SetSortingReference: %v", err)
		}
		if err := series.SetGeneration(generation); err != nil {
			t.Fatalf("SetGeneration: %v", err)
		}
		return series
	}
	a, b := newSeries("a"), newSeries("b")
	seed := func(series *TimeSeries[*Booking], from, to time.Time, bookings ...*Booking) {
		pipe := client.TxPipeline()
		for _, booking := range bookings {
			_ = base.WithPipeline(pipe).Set(ctx, booking)
			if err := series.IngestItem(ctx, pipe, booking, "o1"); err != nil {
				t.Fatalf("IngestItem: %v", err)
			}
		}
		if err := series.AddSegment(ctx, pipe, from, to, "o1"); err != nil {
			t.Fatalf("AddSegment: %v", err)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}
	fetch := func(series *TimeSeries[*Booking], from, to time.Time) ([]*Booking, bool) {
		items, gap, err := series.Fetch(from, to).WithParams("o1").Exec(ctx)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		return items, gap
	}

	b1 := newBooking(t, "b1", jan1.Add(24*time.Hour))
	seed(a, jan1, feb1, b1)
	if items, gap := fetch(a, jan1, feb1); gap || len(items) != 1 {
		t.Fatalf("a: %d items, gap %v", len(items), gap)
	}
	if count, err := a.CountSegments(ctx, "o1"); err != nil || count != 1 {
		t.Fatalf("CountSegments = %d, %v (the gen field is not a segment)", count, err)
	}

	// b does not read a's segments: the range is a gap, and a's segments are gone.
	if _, gap := fetch(b, jan1, feb1); !gap {
		t.Fatal("b read a's segments")
	}
	if _, gap := fetch(a, jan1, feb1); !gap {
		t.Fatal("b's read must drop a's segments")
	}

	// An empty range seeded by a answers empty for a and is a gap for b.
	seed(a, feb1, mar1)
	if items, gap := fetch(a, feb1, mar1); gap || len(items) != 0 {
		t.Fatalf("a empty range: %d items, gap %v", len(items), gap)
	}
	if _, gap := fetch(b, feb1, mar1); !gap {
		t.Fatal("b read a's empty range")
	}

	// AddItem of another generation: the segments are not b's, so the item is not ingested
	// and a's segments go.
	seed(a, jan1, feb1, b1)
	b2 := newBooking(t, "b2", jan1.Add(48*time.Hour))
	if err := b.AddItem(ctx, b2, "o1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("b AddItem = %v", err)
	}
	if _, gap := fetch(a, jan1, feb1); !gap {
		t.Fatal("a still claims a range b wrote into")
	}

	// a's segments with an index that another generation took: the range is not seeded.
	seed(a, jan1, feb1, b1)
	if err := client.Set(ctx, "{cal:o1}:gen", "b", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if _, gap := fetch(a, jan1, feb1); !gap {
		t.Fatal("a answered from an index that is not its own")
	}
	if keyExists(t, client, "cal:o1:segments") {
		t.Fatal("a's segments must go with the index")
	}

	// Within one generation AddItem works as before.
	seed(a, jan1, feb1, b1)
	if err := a.AddItem(ctx, b2, "o1"); err != nil {
		t.Fatalf("a AddItem: %v", err)
	}
	if items, gap := fetch(a, jan1, feb1); gap || len(items) != 2 {
		t.Fatalf("a: %d items, gap %v", len(items), gap)
	}

	if err := a.Purge(ctx, "o1"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	for _, key := range []string{"{cal:o1}", "{cal:o1}:gen", "cal:o1:segments"} {
		if keyExists(t, client, key) {
			t.Fatalf("%s survived Purge", key)
		}
	}
}

// ---------------------------------------------------------------------------
// without a generation nothing changes
// ---------------------------------------------------------------------------

func TestWithoutGenerationNothingChanges(t *testing.T) {
	scenarioWithoutGenerationNothingChanges(t, miniClient)
}

func scenarioWithoutGenerationNothingChanges(t *testing.T, newClient clientFactory) {
	client := newClient(t)
	ctx := context.Background()
	stack := newGenerationStack(t, client, "", "")
	posts := []*Post{newPost(t, "1", time.Now().UTC()), newPost(t, "2", time.Now().UTC())}
	seedGenerationSorted(t, client, stack, "o1", posts...)
	seedGenerationSorted(t, client, stack, "o2")
	if err := stack.sorted.AddItem(ctx, newPost(t, "3", time.Now().UTC()), "o1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}
	if err := stack.sorted.RemoveItem(ctx, posts[0], "o1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}

	feed, ctx := newFeedStore(t, client, Descending, 30)
	feed.walk(ctx)

	series := mustTimeSeries[*Booking](t, client, mustBase[*Booking](t, client, "booking:%s", baseTTL), "cal:%s", indexTTL)
	_ = series.SetSortingReference("StartAt")
	pipe := client.Pipeline()
	_ = series.IngestItem(ctx, pipe, newBooking(t, "b", jan1.Add(time.Hour)), "o1")
	_ = series.AddSegment(ctx, pipe, jan1, feb1, "o1")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("KEYS: %v", err)
	}
	for _, key := range keys {
		if strings.HasSuffix(key, markerGeneration) {
			t.Fatalf("%s written without a generation", key)
		}
		if kind := client.Type(ctx, key).Val(); kind == "string" {
			if value := client.Get(ctx, key).Val(); strings.Contains(value, generationSeparator) {
				t.Fatalf("%s holds a generation prefix: %q", key, value)
			}
		}
	}
	if fields := client.HKeys(ctx, "cal:o1:segments").Val(); len(fields) != 1 || fields[0] == "gen" {
		t.Fatalf("segments hold %v", fields)
	}
}

func TestSetGenerationValidates(t *testing.T) {
	_, client := newTestRedis(t)
	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, "posts:of:%s", indexTTL)
	timeline := mustTimeline[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)
	page := mustPage[*Post](t, client, base, "pages:%s", 10, Descending, indexTTL)
	series := mustTimeSeries[*Post](t, client, base, "series:%s", indexTTL)
	setters := map[string]func(string) error{
		"Base": base.SetGeneration, "Sorted": sorted.SetGeneration, "Timeline": timeline.SetGeneration,
		"Page": page.SetGeneration, "TimeSeries": series.SetGeneration,
	}
	for name, set := range setters {
		for _, bad := range []string{"a\x1fb", "line\nbreak", strings.Repeat("x", maxGenerationLength+1)} {
			if err := set(bad); err == nil {
				t.Fatalf("%s.SetGeneration(%q) accepted", name, bad)
			}
		}
		for _, good := range []string{"", "0f3c9e1", "dev", strings.Repeat("x", maxGenerationLength)} {
			if err := set(good); err != nil {
				t.Fatalf("%s.SetGeneration(%q): %v", name, good, err)
			}
		}
	}
	_ = base.SetGeneration("g")
	_ = sorted.SetGeneration("g")
	_ = timeline.SetGeneration("g")
	_ = page.SetGeneration("g")
	_ = series.SetGeneration("g")
	if base.Generation() != "g" || sorted.Generation() != "g" || timeline.Generation() != "g" || page.Generation() != "g" || series.Generation() != "g" {
		t.Fatal("Generation does not report what SetGeneration set")
	}
}
