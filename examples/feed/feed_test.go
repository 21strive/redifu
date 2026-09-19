package feed

import (
	"context"
	"testing"
	"time"

	"github.com/21strive/redifu"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// The SQL in seed.go needs a database, but everything else in this package can be
// driven against an in-memory Redis. This exercises the wiring in store.go, the
// two-level relation chain, and the read and write paths that do not touch the DB.
func newTestStore(t *testing.T) (*Store, redis.UniversalClient) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	store, err := NewStore(nil, client)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return store, client
}

// seedByHand does what SeedPostFeed does, minus the SQL.
func seedByHand(t *testing.T, store *Store, client redis.UniversalClient, userRandId string, posts ...*Post) {
	t.Helper()
	ctx := context.Background()
	pipe := client.Pipeline()

	for _, post := range posts {
		if err := store.PostBase.WithPipeline(pipe).Set(ctx, post); err != nil {
			t.Fatalf("Set post: %v", err)
		}
		if err := store.PostFeed.IngestItem(ctx, pipe, post, true, userRandId); err != nil {
			t.Fatalf("IngestItem: %v", err)
		}
	}
	if err := store.PostFeed.MarkFirstPage(ctx, pipe, userRandId); err != nil {
		t.Fatalf("MarkFirstPage: %v", err)
	}
	if err := store.PostFeed.SetExpiration(ctx, pipe, userRandId); err != nil {
		t.Fatalf("SetExpiration: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

func fixture(t *testing.T, store *Store) (*Organisation, *Account, []*Post) {
	t.Helper()
	ctx := context.Background()

	organisation := newOrganisation()
	redifu.InitRecord(organisation)
	organisation.Name, organisation.Plan = "Acme", "pro"

	account := newAccount()
	redifu.InitRecord(account)
	account.Name, account.Handle, account.Bio = "Ada", "@ada", "Writes about compilers."
	account.OrganisationRandId = organisation.GetRandId()

	if err := store.OrganisationBase.Set(ctx, organisation); err != nil {
		t.Fatalf("Set organisation: %v", err)
	}
	if err := store.AccountBase.Set(ctx, account); err != nil {
		t.Fatalf("Set account: %v", err)
	}

	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	posts := make([]*Post, 0, 3)
	for i, title := range []string{"first", "second", "third"} {
		post := NewDraftPost(title, "body", account.GetRandId())
		post.Published = base.Add(time.Duration(i) * time.Hour)
		posts = append(posts, post)
	}

	return organisation, account, posts
}

func TestFeedResolvesBothRelationLevels(t *testing.T) {
	ctx := context.Background()
	store, client := newTestStore(t)
	_, _, posts := fixture(t, store)
	seedByHand(t, store, client, "u1", posts...)

	page, err := store.GetPostFeed(ctx, "u1", nil)
	if err != nil {
		t.Fatalf("GetPostFeed: %v", err)
	}
	if len(page.Posts) != 3 {
		t.Fatalf("got %d posts, want 3", len(page.Posts))
	}
	// Sorted by Published, descending — the sorting reference set in store.go.
	if page.Posts[0].Title != "third" {
		t.Fatalf("first post = %q, want the most recently published", page.Posts[0].Title)
	}

	for _, post := range page.Posts {
		if post.Author == nil {
			t.Fatal("post came back without its author")
		}
		if post.Author.Organisation == nil {
			t.Fatal("author came back without its organisation")
		}
		if post.Author.Organisation.Name != "Acme" {
			t.Fatalf("organisation = %q", post.Author.Organisation.Name)
		}
		// The scanner never assigned these; the Relation did, on fetch.
		if post.AuthorRandId == "" {
			t.Fatal("the randId was cleared — it is the only pointer that exists")
		}
	}
}

func TestSinglePostReadResolvesTheSameChain(t *testing.T) {
	ctx := context.Background()
	store, client := newTestStore(t)
	_, _, posts := fixture(t, store)
	seedByHand(t, store, client, "u1", posts...)

	post, err := store.GetPost(ctx, posts[0].GetRandId())
	if err != nil {
		t.Fatalf("GetPost: %v", err)
	}
	if post.Author == nil || post.Author.Organisation == nil {
		t.Fatal("a single-item read did not resolve the chain the feed resolves")
	}
	if post.Author.Bio != "Writes about compilers." {
		t.Fatalf("Bio = %q — the full account was not the one that resolved", post.Author.Bio)
	}
}

func TestRenamingTheOrganisationReachesEveryPost(t *testing.T) {
	ctx := context.Background()
	store, client := newTestStore(t)
	organisation, _, posts := fixture(t, store)
	seedByHand(t, store, client, "u1", posts...)

	if err := store.RenameOrganisation(ctx, organisation.GetRandId(), "Acme Corp"); err != nil {
		t.Fatalf("RenameOrganisation: %v", err)
	}

	page, err := store.GetPostFeed(ctx, "u1", nil)
	if err != nil {
		t.Fatalf("GetPostFeed: %v", err)
	}
	for _, post := range page.Posts {
		if post.Author.Organisation.Name != "Acme Corp" {
			t.Fatalf("organisation = %q, want the renamed value on every post", post.Author.Organisation.Name)
		}
	}
}

func TestCreatePostEntersASeededFeed(t *testing.T) {
	ctx := context.Background()
	store, client := newTestStore(t)
	_, account, posts := fixture(t, store)
	seedByHand(t, store, client, "u1", posts...)

	fresh := NewDraftPost("fourth", "body", account.GetRandId())
	fresh.Published = time.Date(2026, 4, 1, 5, 0, 0, 0, time.UTC)

	if err := store.CreatePost(ctx, fresh, []string{"u1"}); err != nil {
		t.Fatalf("CreatePost: %v", err)
	}

	page, err := store.GetPostFeed(ctx, "u1", nil)
	if err != nil {
		t.Fatalf("GetPostFeed: %v", err)
	}
	if len(page.Posts) != 4 {
		t.Fatalf("got %d posts, want 4", len(page.Posts))
	}
	if page.Posts[0].Title != "fourth" {
		t.Fatalf("first post = %q, want the new one at the head", page.Posts[0].Title)
	}
	if page.Posts[0].Author == nil {
		t.Fatal("the new post came back without its author")
	}
}

func TestDeletePostRemovesEntityAndIndexMember(t *testing.T) {
	ctx := context.Background()
	store, client := newTestStore(t)
	_, _, posts := fixture(t, store)
	seedByHand(t, store, client, "u1", posts...)

	if err := store.DeletePost(ctx, posts[0], []string{"u1"}); err != nil {
		t.Fatalf("DeletePost: %v", err)
	}

	count, err := store.PostFeed.Count(ctx, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("feed holds %d members, want 2", count)
	}

	present, errExists := store.PostBase.Exists(ctx, posts[0].GetRandId())
	if errExists != nil {
		t.Fatalf("Exists: %v", errExists)
	}
	if present {
		t.Fatal("the entity key survived DeletePost")
	}

	// Removing an item clears the page markers, so the feed no longer knows where its
	// boundaries are and asks to be rebuilt on the next read. GetPostFeed would call
	// SeedPostFeed here — which is why this test checks the state directly instead.
	needsSeed, errSeed := store.PostFeed.RequiresSeeding(ctx, 0, "u1")
	if errSeed != nil {
		t.Fatalf("RequiresSeeding: %v", errSeed)
	}
	if !needsSeed {
		t.Fatal("a feed that lost an item did not ask to be reseeded")
	}
}
