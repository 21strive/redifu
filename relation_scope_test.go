package redifu

import (
	"context"
	"testing"
	"time"
)

// Base relations and index relations are additive, not exclusive.
func TestBaseAndIndexRelationsResolveTogether(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, postBase, feedKeyFormat, 20, Descending, indexTTL)

	// Author on the Base: resolves on every read of a Post, anywhere.
	postBase.AddRelation(mustRelation(t, accountBase,
		func(p *Post) string { return p.AuthorRandId },
		func(p *Post, a *Account) { p.Author = a },
	))
	// Editor on the index only: resolves in this feed, nowhere else.
	timeline.AddRelation(mustRelation(t, accountBase,
		func(p *Post) string { return p.EditorRandId },
		func(p *Post, a *Account) { p.Editor = a },
	))

	author, editor := newAccount(t, "Ada"), newAccount(t, "Linus")
	for _, account := range []*Account{author, editor} {
		if err := accountBase.Set(ctx, account); err != nil {
			t.Fatalf("Set account: %v", err)
		}
	}

	post := newPost(t, "hello", time.Now())
	post.AuthorRandId = author.GetRandId()
	post.EditorRandId = editor.GetRandId()
	seedTimeline(t, client, postBase, timeline, "u1", post)

	// Through the index: both resolve.
	viaIndex := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if viaIndex.Error() != nil {
		t.Fatalf("Fetch: %v", viaIndex.Error())
	}
	fetched := viaIndex.Items()[0]
	if fetched.Author == nil || fetched.Author.Name != "Ada" {
		t.Fatal("base relation did not resolve through the index")
	}
	if fetched.Editor == nil || fetched.Editor.Name != "Linus" {
		t.Fatal("index relation did not resolve through the index")
	}

	// Through Base.Get: only the base relation. The index one is scoped to that index.
	direct, err := postBase.Get(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if direct.Author == nil {
		t.Fatal("base relation did not resolve on Base.Get")
	}
	if direct.Editor != nil {
		t.Fatal("an index-scoped relation leaked into Base.Get")
	}
}

// Registering the same relation on both the Base and an index is the natural mistake
// to make while moving relations onto Base. It is kept once rather than doubling the
// reads that relation performs.
func TestRegisteringTheSameRelationTwiceStillReadsItOnce(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	postBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	timeline := mustTimeline[*Post](t, client, postBase, feedKeyFormat, 20, Descending, indexTTL)

	authorRelation := mustRelation(t, accountBase,
		func(p *Post) string { return p.AuthorRandId },
		func(p *Post, a *Account) { p.Author = a },
	)
	postBase.AddRelation(authorRelation)
	timeline.AddRelation(authorRelation) // the mistake

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("Set: %v", err)
	}
	post := newPost(t, "hello", time.Now())
	post.AuthorRandId = author.GetRandId()
	seedTimeline(t, client, postBase, timeline, "u1", post)

	counter := newCommandCounter(client)
	output := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	if output.Items()[0].Author == nil {
		t.Fatal("relation did not resolve")
	}

	if reads := counter.reads("account:" + author.GetRandId()); reads != 1 {
		t.Fatalf("read the shared account %d times, want 1", reads)
	}
}
