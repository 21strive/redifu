package redifu

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/21strive/item"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const (
	postKeyFormat    = "post:%s"
	accountKeyFormat = "account:%s"
	feedKeyFormat    = "feed:%s"
	baseTTL          = 7 * 24 * time.Hour
	indexTTL         = 2 * 24 * time.Hour
)

type Account struct {
	*item.Foundation
	Name string `json:"name"`
}

type Post struct {
	*item.Foundation
	Title string `json:"title"`

	AuthorRandId string   `json:"authorRandId"`
	Author       *Account `json:"-"`

	EditorRandId string   `json:"editorRandId"`
	Editor       *Account `json:"-"`
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newAccount(t *testing.T, name string) *Account {
	t.Helper()
	account := &Account{Foundation: &item.Foundation{}}
	item.InitItem(account)
	account.Name = name
	return account
}

func newPost(t *testing.T, title string, createdAt time.Time) *Post {
	t.Helper()
	post := &Post{Foundation: &item.Foundation{}}
	item.InitItem(post)
	post.Title = title
	post.SetCreatedAt(createdAt)
	post.Author = nil
	post.Editor = nil
	return post
}

func newTestRedis(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

// seedTimeline mirrors what a consumer seeder does: write each item to Base, ingest it
// with seed = true, apply the collection TTL, execute once.
func seedTimeline(t *testing.T, client redis.UniversalClient, base *Base[*Post], timeline *Timeline[*Post], keyParam string, posts ...*Post) {
	t.Helper()
	ctx := context.Background()
	pipe := client.Pipeline()
	for _, post := range posts {
		if err := base.WithPipeline(pipe).Set(ctx, post); err != nil {
			t.Fatalf("seed Set: %v", err)
		}
		if err := timeline.IngestItem(ctx, pipe, post, true, keyParam); err != nil {
			t.Fatalf("seed IngestItem: %v", err)
		}
	}
	timeline.SetExpiration(ctx, pipe, keyParam)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed Exec: %v", err)
	}
}

func seedSorted(t *testing.T, client redis.UniversalClient, base *Base[*Post], sorted *Sorted[*Post], keyParam string, posts ...*Post) {
	t.Helper()
	ctx := context.Background()
	pipe := client.Pipeline()
	for _, post := range posts {
		if err := base.WithPipeline(pipe).Set(ctx, post); err != nil {
			t.Fatalf("seed Set: %v", err)
		}
		if err := sorted.IngestItem(ctx, pipe, post, true, keyParam); err != nil {
			t.Fatalf("seed IngestItem: %v", err)
		}
	}
	sorted.SetExpiration(ctx, pipe, keyParam)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed Exec: %v", err)
	}
}

// commandCounter records every command the client issues, pipelined or not.
type commandCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCommandCounter(client redis.UniversalClient) *commandCounter {
	counter := &commandCounter{counts: map[string]int{}}
	client.AddHook(counter)
	return counter
}

func (c *commandCounter) record(cmd redis.Cmder) {
	args := cmd.Args()
	if len(args) < 2 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[fmt.Sprintf("%v %v", args[0], args[1])]++
}

func (c *commandCounter) count(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[key]
}

func (c *commandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }

func (c *commandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.record(cmd)
		return next(ctx, cmd)
	}
}

func (c *commandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			c.record(cmd)
		}
		return next(ctx, cmds)
	}
}

// ---------------------------------------------------------------------------
// 02 — SetIfAbsent: AddItem refreshes the TTL of an item Base already holds
// ---------------------------------------------------------------------------

func TestAddItemRefreshesTTLOfExistingItem(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, base, feedKeyFormat, 20, Descending, indexTTL)

	post := newPost(t, "old post", time.Now())
	if err := base.Set(ctx, post); err != nil {
		t.Fatalf("seed item: %v", err)
	}

	// The item has been sitting in Base for six days when it is added to an index.
	server.FastForward(6 * 24 * time.Hour)

	if err := timeline.AddItem(ctx, post, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	itemTTL := server.TTL("post:" + post.GetRandId())
	if itemTTL != baseTTL {
		t.Fatalf("item TTL = %v, want a full %v", itemTTL, baseTTL)
	}
	if itemTTL <= indexTTL {
		t.Fatalf("item TTL %v must outlive the index TTL %v", itemTTL, indexTTL)
	}
}

func TestAddItemDoesNotOverwriteExistingPayload(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, base, feedKeyFormat, 20, Descending, indexTTL)

	post := newPost(t, "full value", time.Now())
	seedTimeline(t, client, base, timeline, "u1", post)

	// A stub carrying only identity and sorting field, as an event payload would.
	stub := &Post{Foundation: &item.Foundation{}}
	stub.RandId = post.GetRandId()
	stub.SetCreatedAt(post.GetCreatedAt())

	if err := timeline.AddItem(ctx, stub, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	stored, err := base.Get(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Title != "full value" {
		t.Fatalf("stored title = %q, want the value Base already held", stored.Title)
	}
}

func TestAddItemWritesItemThatDoesNotExistYet(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	sorted := NewSorted[*Post](client, base, feedKeyFormat, indexTTL)

	post := newPost(t, "brand new", time.Now())
	if err := sorted.AddItem(ctx, post, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	stored, err := base.Get(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Title != "brand new" {
		t.Fatalf("stored title = %q, want the value passed to AddItem", stored.Title)
	}
	if ttl := server.TTL("post:" + post.GetRandId()); ttl != baseTTL {
		t.Fatalf("item TTL = %v, want %v", ttl, baseTTL)
	}
}

func TestAddItemWithCallerPipelineDoesNotExecute(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, base, feedKeyFormat, 20, Descending, indexTTL)

	post := newPost(t, "pipelined", time.Now())

	pipe := client.Pipeline()
	if err := timeline.WithPipeline(pipe).AddItem(ctx, post, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	if server.Exists("post:" + post.GetRandId()) {
		t.Fatal("item was written before the caller executed the pipeline")
	}

	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !server.Exists("post:" + post.GetRandId()) {
		t.Fatal("item missing after the caller executed the pipeline")
	}
}

// An item older than (Base TTL - index TTL) that is added to a fresh index used to keep
// its remaining lifetime and die while that index was still alive.
func TestIndexNeverOutlivesItsItems(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, base, feedKeyFormat, 20, Descending, indexTTL)

	now := time.Now()
	stale := newPost(t, "written six days ago", now)
	if err := base.Set(ctx, stale); err != nil {
		t.Fatalf("seed stale item: %v", err)
	}

	server.FastForward(6 * 24 * time.Hour)

	// A fresh index is seeded now; it will live two more days.
	anchor := newPost(t, "anchor", now.Add(-time.Hour))
	seedTimeline(t, client, base, timeline, "u1", anchor)

	if err := timeline.AddItem(ctx, stale, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	// One day on: the stale item's original TTL would have run out here, while the
	// index still has a day left.
	server.FastForward(24*time.Hour + time.Minute)

	output := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	if len(output.Items()) != 2 {
		t.Fatalf("fetched %d items, want 2 — the index is holed", len(output.Items()))
	}
}

// ---------------------------------------------------------------------------
// 03 — RemoveItem, Purge and Base.Del
// ---------------------------------------------------------------------------

func TestRemoveItemKeepsItemInOtherIndexes(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, base, feedKeyFormat, 20, Descending, indexTTL)

	post := newPost(t, "shared", time.Now())
	seedTimeline(t, client, base, timeline, "u1", post)
	seedTimeline(t, client, base, timeline, "u2", post)

	if err := timeline.RemoveItem(ctx, post, "u1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}

	kept := timeline.Fetch(nil).WithParams("u2").Exec(ctx)
	if kept.Error() != nil {
		t.Fatalf("Fetch u2: %v", kept.Error())
	}
	if len(kept.Items()) != 1 {
		t.Fatalf("u2 has %d items, want 1 — RemoveItem holed another index", len(kept.Items()))
	}
	if !server.Exists("post:" + post.GetRandId()) {
		t.Fatal("RemoveItem deleted the item key")
	}

	removed := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if len(removed.Items()) != 0 {
		t.Fatalf("u1 has %d items, want 0", len(removed.Items()))
	}
}

func TestRemoveItemLeavesBaseKeyUntouched(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	sorted := NewSorted[*Post](client, base, feedKeyFormat, indexTTL)

	post := newPost(t, "kept", time.Now())
	seedSorted(t, client, base, sorted, "u1", post)

	before := server.TTL("post:" + post.GetRandId())
	if err := sorted.RemoveItem(ctx, post, "u1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}

	if !server.Exists("post:" + post.GetRandId()) {
		t.Fatal("RemoveItem deleted the item key")
	}
	if after := server.TTL("post:" + post.GetRandId()); after != before {
		t.Fatalf("item TTL changed from %v to %v", before, after)
	}
	if count := sorted.Count(ctx, "u1"); count != 0 {
		t.Fatalf("index still holds %d members", count)
	}
}

func TestPurgeDropsIndexAndMarkersButKeepsItems(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, base, feedKeyFormat, 20, Descending, indexTTL)

	post := newPost(t, "kept", time.Now())
	seedTimeline(t, client, base, timeline, "u1", post)

	pipe := client.Pipeline()
	timeline.MarkFirstPage(ctx, pipe, "u1")
	timeline.MarkLastPage(ctx, pipe, "u1")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("mark: %v", err)
	}

	if err := timeline.Purge(ctx, "u1"); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	if server.Exists("feed:u1") {
		t.Fatal("Purge left the index behind")
	}
	for _, marker := range []string{"feed:u1:firstpage", "feed:u1:lastpage", "feed:u1:blankpage"} {
		if server.Exists(marker) {
			t.Fatalf("Purge left marker %s behind", marker)
		}
	}
	if !server.Exists("post:" + post.GetRandId()) {
		t.Fatal("Purge deleted an item key")
	}

	needsSeeding, err := timeline.RequiresSeeding(ctx, 0, "u1")
	if err != nil {
		t.Fatalf("RequiresSeeding: %v", err)
	}
	if !needsSeeding {
		t.Fatal("collection does not require seeding after Purge")
	}
}

func TestSortedPurgeClearsBlankPageMarker(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	sorted := NewSorted[*Post](client, base, feedKeyFormat, indexTTL)

	pipe := client.Pipeline()
	sorted.MarkEmpty(ctx, pipe, "u1")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("MarkEmpty: %v", err)
	}

	needsSeeding, err := sorted.RequiresSeeding(ctx, "u1")
	if err != nil {
		t.Fatalf("RequiresSeeding: %v", err)
	}
	if needsSeeding {
		t.Fatal("a collection confirmed empty should not require seeding yet")
	}

	if err := sorted.Purge(ctx, "u1"); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	needsSeeding, err = sorted.RequiresSeeding(ctx, "u1")
	if err != nil {
		t.Fatalf("RequiresSeeding after purge: %v", err)
	}
	if !needsSeeding {
		t.Fatal("collection still counts as empty after Purge — the blankpage marker survived")
	}
}

func TestDeletingAnEntityIsBaseDelAlongsideRemoveItem(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	base := NewBase[*Post](client, postKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, base, feedKeyFormat, 20, Descending, indexTTL)

	post := newPost(t, "doomed", time.Now())
	seedTimeline(t, client, base, timeline, "u1", post)

	pipe := client.Pipeline()
	if err := timeline.WithPipeline(pipe).RemoveItem(ctx, post, "u1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}
	if err := base.WithPipeline(pipe).Del(ctx, post); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if server.Exists("post:" + post.GetRandId()) {
		t.Fatal("item key survived Base.Del")
	}
	count, errZCard := client.ZCard(ctx, "feed:u1").Result()
	if errZCard != nil {
		t.Fatalf("ZCard: %v", errZCard)
	}
	if count != 0 {
		t.Fatalf("index still holds %d members", count)
	}
}

// ---------------------------------------------------------------------------
// 01 — Relation
// ---------------------------------------------------------------------------

func relateAuthor(t *testing.T, accountBase *Base[*Account]) Relation[*Post] {
	t.Helper()
	relation, err := Relate(accountBase,
		func(p *Post) string { return p.AuthorRandId },
		func(p *Post, a *Account) { p.Author = a },
	)
	if err != nil {
		t.Fatalf("Relate: %v", err)
	}
	return relation
}

func TestRelationResolvesOnFetch(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := NewBase[*Post](client, postKeyFormat, baseTTL)
	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, postBase, feedKeyFormat, 20, Descending, indexTTL)
	timeline.AddRelation(relateAuthor(t, accountBase))

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("seed account: %v", err)
	}

	post := newPost(t, "hello", time.Now())
	post.AuthorRandId = author.GetRandId()
	seedTimeline(t, client, postBase, timeline, "u1", post)

	output := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	if len(output.Items()) != 1 {
		t.Fatalf("fetched %d items, want 1", len(output.Items()))
	}

	fetched := output.Items()[0]
	if fetched.Author == nil {
		t.Fatal("Author was not resolved")
	}
	if fetched.Author.Name != "Ada" {
		t.Fatalf("Author.Name = %q, want %q", fetched.Author.Name, "Ada")
	}
	if fetched.AuthorRandId != author.GetRandId() {
		t.Fatalf("AuthorRandId = %q, want it to survive the fetch", fetched.AuthorRandId)
	}
}

func TestFetchedItemIsSafeToWriteBack(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	postBase := NewBase[*Post](client, postKeyFormat, baseTTL)
	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, postBase, feedKeyFormat, 20, Descending, indexTTL)
	timeline.AddRelation(relateAuthor(t, accountBase))

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("seed account: %v", err)
	}

	post := newPost(t, "before", time.Now())
	post.AuthorRandId = author.GetRandId()
	seedTimeline(t, client, postBase, timeline, "u1", post)

	fetched := timeline.Fetch(nil).WithParams("u1").Exec(ctx).Items()[0]
	fetched.Title = "after"
	if err := postBase.Set(ctx, fetched); err != nil {
		t.Fatalf("write back: %v", err)
	}

	stored, _ := server.Get("post:" + post.GetRandId())
	if strings.Contains(stored, "\"name\"") {
		t.Fatalf("write-back baked the related entity into the item key: %s", stored)
	}

	again := timeline.Fetch(nil).WithParams("u1").Exec(ctx).Items()[0]
	if again.Title != "after" {
		t.Fatalf("Title = %q, want %q", again.Title, "after")
	}
	if again.Author == nil {
		t.Fatal("relation broke after a write-back")
	}
}

func TestRelationReadsSharedEntityOnce(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := NewBase[*Post](client, postKeyFormat, baseTTL)
	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, postBase, feedKeyFormat, 20, Descending, indexTTL)
	timeline.AddRelation(relateAuthor(t, accountBase))

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("seed account: %v", err)
	}

	now := time.Now()
	posts := make([]*Post, 0, 3)
	for i := 0; i < 3; i++ {
		post := newPost(t, fmt.Sprintf("post %d", i), now.Add(time.Duration(i)*time.Minute))
		post.AuthorRandId = author.GetRandId()
		posts = append(posts, post)
	}
	seedTimeline(t, client, postBase, timeline, "u1", posts...)

	counter := newCommandCounter(client)
	output := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	if len(output.Items()) != 3 {
		t.Fatalf("fetched %d items, want 3", len(output.Items()))
	}

	reads := counter.count("get account:" + author.GetRandId())
	if reads != 1 {
		t.Fatalf("read the shared account %d times, want 1", reads)
	}
	for _, fetched := range output.Items() {
		if fetched.Author == nil || fetched.Author.Name != "Ada" {
			t.Fatal("a post came back without its author")
		}
	}
}

func TestTwoRelationsOfTheSameType(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := NewBase[*Post](client, postKeyFormat, baseTTL)
	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, postBase, feedKeyFormat, 20, Descending, indexTTL)

	editorRelation, err := Relate(accountBase,
		func(p *Post) string { return p.EditorRandId },
		func(p *Post, a *Account) { p.Editor = a },
	)
	if err != nil {
		t.Fatalf("Relate editor: %v", err)
	}
	timeline.AddRelation(relateAuthor(t, accountBase), editorRelation)

	author := newAccount(t, "Ada")
	editor := newAccount(t, "Grace")
	for _, account := range []*Account{author, editor} {
		if err := accountBase.Set(ctx, account); err != nil {
			t.Fatalf("seed account: %v", err)
		}
	}

	post := newPost(t, "hello", time.Now())
	post.AuthorRandId = author.GetRandId()
	post.EditorRandId = editor.GetRandId()
	seedTimeline(t, client, postBase, timeline, "u1", post)

	fetched := timeline.Fetch(nil).WithParams("u1").Exec(ctx).Items()[0]
	if fetched.Author == nil || fetched.Author.Name != "Ada" {
		t.Fatal("Author not resolved")
	}
	if fetched.Editor == nil || fetched.Editor.Name != "Grace" {
		t.Fatal("Editor not resolved")
	}
}

func TestMissingRelatedEntityLeavesFieldNil(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := NewBase[*Post](client, postKeyFormat, baseTTL)
	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, postBase, feedKeyFormat, 20, Descending, indexTTL)
	timeline.AddRelation(relateAuthor(t, accountBase))

	post := newPost(t, "orphan", time.Now())
	post.AuthorRandId = "gone"
	seedTimeline(t, client, postBase, timeline, "u1", post)

	output := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch must not fail when a related entity is missing: %v", output.Error())
	}
	if len(output.Items()) != 1 {
		t.Fatalf("fetched %d items, want 1", len(output.Items()))
	}
	if output.Items()[0].Author != nil {
		t.Fatal("Author should be nil when the related key is gone")
	}
}

func TestUpdatingRelatedEntityShowsEverywhere(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := NewBase[*Post](client, postKeyFormat, baseTTL)
	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)
	timeline := NewTimeline[*Post](client, postBase, feedKeyFormat, 20, Descending, indexTTL)
	timeline.AddRelation(relateAuthor(t, accountBase))

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("seed account: %v", err)
	}

	now := time.Now()
	first := newPost(t, "one", now)
	second := newPost(t, "two", now.Add(time.Minute))
	first.AuthorRandId = author.GetRandId()
	second.AuthorRandId = author.GetRandId()
	seedTimeline(t, client, postBase, timeline, "u1", first, second)

	author.Name = "Ada Lovelace"
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("update account: %v", err)
	}

	for _, fetched := range timeline.Fetch(nil).WithParams("u1").Exec(ctx).Items() {
		if fetched.Author.Name != "Ada Lovelace" {
			t.Fatalf("Author.Name = %q, want the updated value", fetched.Author.Name)
		}
	}
}

func TestRelateRejectsValueItemType(t *testing.T) {
	_, client := newTestRedis(t)

	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)

	_, err := Relate(accountBase,
		func(p Post) string { return p.AuthorRandId },
		func(p Post, a *Account) { p.Author = a },
	)
	if err == nil {
		t.Fatal("Relate accepted a value item type; it must require a pointer")
	}
	if !strings.Contains(err.Error(), "pointer") {
		t.Fatalf("error = %q, want it to explain the pointer requirement", err)
	}
}

func TestRelationOnSortedAndPage(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := NewBase[*Post](client, postKeyFormat, baseTTL)
	accountBase := NewBase[*Account](client, accountKeyFormat, baseTTL)
	sorted := NewSorted[*Post](client, postBase, feedKeyFormat, indexTTL)
	sorted.AddRelation(relateAuthor(t, accountBase))

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("seed account: %v", err)
	}

	post := newPost(t, "hello", time.Now())
	post.AuthorRandId = author.GetRandId()
	seedSorted(t, client, postBase, sorted, "u1", post)

	items, err := sorted.Fetch(Descending).WithParams("u1").Exec(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("fetched %d items, want 1", len(items))
	}
	if items[0].Author == nil || items[0].Author.Name != "Ada" {
		t.Fatal("Sorted did not resolve its relation")
	}
}
