package redifu

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/21strive/item"
)

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

type Org struct {
	*item.Foundation
	Name string `json:"name"`
}

type Member struct {
	*item.Foundation
	Name      string `json:"name"`
	OrgRandId string `json:"orgRandId"`
	Org       *Org   `json:"-"`
}

type Doc struct {
	*item.Foundation
	Title       string  `json:"title"`
	OwnerRandId string  `json:"ownerRandId"`
	Owner       *Member `json:"-"`
	Ranking     int64   `json:"ranking"`
}

// LeakyPost is what invariant 3 exists to prevent: the related entity is not excluded
// from JSON, so writing a fetched post back to Base would bake a copy of the account
// into the post's own key.
type LeakyPost struct {
	*item.Foundation
	AuthorRandId string   `json:"authorRandId"`
	Author       *Account `json:"author"`
}

// Node relates to itself, which is legitimate and must terminate.
type Node struct {
	*item.Foundation
	Name         string `json:"name"`
	ParentRandId string `json:"parentRandId"`
	Parent       *Node  `json:"-"`
}

func newOrg(t *testing.T, name string) *Org {
	t.Helper()
	org := &Org{Foundation: &item.Foundation{}}
	item.InitItem(org)
	org.Name = name
	return org
}

func newMember(t *testing.T, name string) *Member {
	t.Helper()
	member := &Member{Foundation: &item.Foundation{}}
	item.InitItem(member)
	member.Name = name
	member.Org = nil
	return member
}

func newDoc(t *testing.T, title string, createdAt time.Time) *Doc {
	t.Helper()
	doc := &Doc{Foundation: &item.Foundation{}}
	item.InitItem(doc)
	doc.Title = title
	doc.SetCreatedAt(createdAt)
	doc.Owner = nil
	return doc
}

func newNode(t *testing.T, name string) *Node {
	t.Helper()
	node := &Node{Foundation: &item.Foundation{}}
	item.InitItem(node)
	node.Name = name
	node.Parent = nil
	return node
}

func mustRelation[P any, R item.Blueprint](t *testing.T, base *Base[R], get func(P) string, set func(P, R)) Relation[P] {
	t.Helper()
	relation, err := Relate(base, get, set)
	if err != nil {
		t.Fatalf("Relate: %v", err)
	}
	return relation
}

// ---------------------------------------------------------------------------
// invariant 3 is now enforced, not merely documented
// ---------------------------------------------------------------------------

func TestRelateRejectsAFieldThatIsNotTaggedJSONDash(t *testing.T) {
	_, client := newTestRedis(t)
	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)

	_, err := Relate(accountBase,
		func(p *LeakyPost) string { return p.AuthorRandId },
		func(p *LeakyPost, a *Account) { p.Author = a },
	)

	if !errors.Is(err, ErrRelationNotTransient) {
		t.Fatalf("Relate = %v, want ErrRelationNotTransient", err)
	}
	if !strings.Contains(err.Error(), `json:"-"`) {
		t.Fatalf("error should say how to fix it, got: %v", err)
	}
}

func TestRelateAcceptsAProperlyTaggedField(t *testing.T) {
	_, client := newTestRedis(t)
	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)

	if _, err := Relate(accountBase,
		func(p *Post) string { return p.AuthorRandId },
		func(p *Post, a *Account) { p.Author = a },
	); err != nil {
		t.Fatalf("Relate on a json:\"-\" field: %v", err)
	}
}

// ---------------------------------------------------------------------------
// relations resolve on the entity, not only through an index
// ---------------------------------------------------------------------------

func TestBaseGetResolvesRelations(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	orgBase := mustBase[*Org](t, client, "org:%s", baseTTL)
	memberBase := mustBase[*Member](t, client, "member:%s", baseTTL)
	memberBase.AddRelation(mustRelation(t, orgBase,
		func(m *Member) string { return m.OrgRandId },
		func(m *Member, o *Org) { m.Org = o },
	))

	org := newOrg(t, "Acme")
	if err := orgBase.Set(ctx, org); err != nil {
		t.Fatalf("Set org: %v", err)
	}
	member := newMember(t, "Ada")
	member.OrgRandId = org.GetRandId()
	if err := memberBase.Set(ctx, member); err != nil {
		t.Fatalf("Set member: %v", err)
	}

	fetched, err := memberBase.Get(ctx, member.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fetched.Org == nil {
		t.Fatal("Base.Get returned an item with an unresolved relation")
	}
	if fetched.Org.Name != "Acme" {
		t.Fatalf("Org.Name = %q, want %q", fetched.Org.Name, "Acme")
	}
}

func TestBaseGetManyResolvesRelations(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	orgBase := mustBase[*Org](t, client, "org:%s", baseTTL)
	memberBase := mustBase[*Member](t, client, "member:%s", baseTTL)
	memberBase.AddRelation(mustRelation(t, orgBase,
		func(m *Member) string { return m.OrgRandId },
		func(m *Member, o *Org) { m.Org = o },
	))

	org := newOrg(t, "Acme")
	if err := orgBase.Set(ctx, org); err != nil {
		t.Fatalf("Set org: %v", err)
	}

	randIds := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		member := newMember(t, fmt.Sprintf("member %d", i))
		member.OrgRandId = org.GetRandId()
		if err := memberBase.Set(ctx, member); err != nil {
			t.Fatalf("Set member: %v", err)
		}
		randIds = append(randIds, member.GetRandId())
	}

	fetched, err := memberBase.GetMany(ctx, randIds)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(fetched) != 3 {
		t.Fatalf("fetched %d members, want 3", len(fetched))
	}
	for randId, member := range fetched {
		if member.Org == nil {
			t.Fatalf("member %s came back without its org", randId)
		}
	}
}

func TestRelationsAreTransitive(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	orgBase := mustBase[*Org](t, client, "org:%s", baseTTL)
	memberBase := mustBase[*Member](t, client, "member:%s", baseTTL)
	docBase := mustBase[*Doc](t, client, "doc:%s", baseTTL)

	memberBase.AddRelation(mustRelation(t, orgBase,
		func(m *Member) string { return m.OrgRandId },
		func(m *Member, o *Org) { m.Org = o },
	))
	docBase.AddRelation(mustRelation(t, memberBase,
		func(d *Doc) string { return d.OwnerRandId },
		func(d *Doc, m *Member) { d.Owner = m },
	))

	org := newOrg(t, "Acme")
	member := newMember(t, "Ada")
	member.OrgRandId = org.GetRandId()
	doc := newDoc(t, "spec", time.Now())
	doc.OwnerRandId = member.GetRandId()

	for _, write := range []func() error{
		func() error { return orgBase.Set(ctx, org) },
		func() error { return memberBase.Set(ctx, member) },
		func() error { return docBase.Set(ctx, doc) },
	} {
		if err := write(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	timeline, errTimeline := NewTimeline[*Doc](client, docBase, "docs:%s", 20, Descending, indexTTL)
	if errTimeline != nil {
		t.Fatalf("NewTimeline: %v", errTimeline)
	}

	pipe := client.Pipeline()
	if err := timeline.IngestItem(ctx, pipe, doc, true, "u1"); err != nil {
		t.Fatalf("IngestItem: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	output := timeline.Fetch(nil).WithParams("u1").Exec(ctx)
	if output.Error() != nil {
		t.Fatalf("Fetch: %v", output.Error())
	}
	fetched := output.Items()[0]

	if fetched.Owner == nil {
		t.Fatal("doc came back without its owner")
	}
	if fetched.Owner.Org == nil {
		t.Fatal("the owner came back without its org — relations did not nest")
	}
	if fetched.Owner.Org.Name != "Acme" {
		t.Fatalf("nested org name = %q, want %q", fetched.Owner.Org.Name, "Acme")
	}
}

func TestUpdatingADeeplyRelatedEntityShowsEverywhere(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	orgBase := mustBase[*Org](t, client, "org:%s", baseTTL)
	memberBase := mustBase[*Member](t, client, "member:%s", baseTTL)
	docBase := mustBase[*Doc](t, client, "doc:%s", baseTTL)

	memberBase.AddRelation(mustRelation(t, orgBase,
		func(m *Member) string { return m.OrgRandId },
		func(m *Member, o *Org) { m.Org = o },
	))
	docBase.AddRelation(mustRelation(t, memberBase,
		func(d *Doc) string { return d.OwnerRandId },
		func(d *Doc, m *Member) { d.Owner = m },
	))

	org := newOrg(t, "Acme")
	member := newMember(t, "Ada")
	member.OrgRandId = org.GetRandId()
	if err := orgBase.Set(ctx, org); err != nil {
		t.Fatalf("Set org: %v", err)
	}
	if err := memberBase.Set(ctx, member); err != nil {
		t.Fatalf("Set member: %v", err)
	}

	for i := 0; i < 3; i++ {
		doc := newDoc(t, fmt.Sprintf("doc %d", i), time.Now())
		doc.OwnerRandId = member.GetRandId()
		if err := docBase.Set(ctx, doc); err != nil {
			t.Fatalf("Set doc: %v", err)
		}
	}

	// One write, two levels down.
	org.Name = "Acme Corp"
	if err := orgBase.Set(ctx, org); err != nil {
		t.Fatalf("rename org: %v", err)
	}

	fetched, err := memberBase.Get(ctx, member.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fetched.Org.Name != "Acme Corp" {
		t.Fatalf("Org.Name = %q, want the updated value", fetched.Org.Name)
	}
}

func TestSelfReferentialRelationTerminatesAtTheDepthLimit(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	nodeBase := mustBase[*Node](t, client, "node:%s", baseTTL)
	if err := nodeBase.SetRelationDepth(3); err != nil {
		t.Fatalf("SetRelationDepth: %v", err)
	}
	nodeBase.AddRelation(mustRelation(t, nodeBase,
		func(n *Node) string { return n.ParentRandId },
		func(n *Node, p *Node) { n.Parent = p },
	))

	// A cycle: a is its own grandparent.
	a, b := newNode(t, "a"), newNode(t, "b")
	a.ParentRandId = b.GetRandId()
	b.ParentRandId = a.GetRandId()
	if err := nodeBase.Set(ctx, a); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := nodeBase.Set(ctx, b); err != nil {
		t.Fatalf("Set b: %v", err)
	}

	done := make(chan *Node, 1)
	go func() {
		fetched, err := nodeBase.Get(ctx, a.GetRandId())
		if err != nil {
			done <- nil
			return
		}
		done <- fetched
	}()

	select {
	case fetched := <-done:
		if fetched == nil {
			t.Fatal("Get failed on a cyclic relation")
		}
		if fetched.Parent == nil || fetched.Parent.Name != "b" {
			t.Fatal("first level of the cycle did not resolve")
		}
		if fetched.Parent.Parent == nil || fetched.Parent.Parent.Name != "a" {
			t.Fatal("second level of the cycle did not resolve")
		}
		// Depth 3 means three levels of resolution and then a stop.
		if fetched.Parent.Parent.Parent != nil && fetched.Parent.Parent.Parent.Parent != nil {
			t.Fatal("the cycle did not stop at the depth limit")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("resolving a cyclic relation did not terminate")
	}
}

// ---------------------------------------------------------------------------
// a shared entity survives as long as something is reading it
// ---------------------------------------------------------------------------

func TestReadingAnItemExtendsItsTTL(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	account := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, account); err != nil {
		t.Fatalf("Set: %v", err)
	}

	server.FastForward(6 * 24 * time.Hour)

	if _, err := accountBase.Get(ctx, account.GetRandId()); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if ttl := server.TTL("account:" + account.GetRandId()); ttl != baseTTL {
		t.Fatalf("TTL after a read = %v, want a full %v", ttl, baseTTL)
	}
}

func TestResolvingARelationExtendsTheSharedEntityTTL(t *testing.T) {
	ctx := context.Background()
	server, client := newTestRedis(t)

	postBase := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	postBase.AddRelation(mustRelation(t, accountBase,
		func(p *Post) string { return p.AuthorRandId },
		func(p *Post, a *Account) { p.Author = a },
	))

	author := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, author); err != nil {
		t.Fatalf("Set account: %v", err)
	}
	post := newPost(t, "hello", time.Now())
	post.AuthorRandId = author.GetRandId()
	if err := postBase.Set(ctx, post); err != nil {
		t.Fatalf("Set post: %v", err)
	}

	server.FastForward(6 * 24 * time.Hour)

	fetched, err := postBase.Get(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fetched.Author == nil {
		t.Fatal("relation did not resolve")
	}

	if ttl := server.TTL("account:" + author.GetRandId()); ttl != baseTTL {
		t.Fatalf("shared account TTL = %v, want a full %v after being read", ttl, baseTTL)
	}
}

func TestTouchOnReadCanBeSwitchedOff(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	accountBase := mustBase[*Account](t, client, accountKeyFormat, baseTTL)
	accountBase.SetTouchOnRead(false)

	account := newAccount(t, "Ada")
	if err := accountBase.Set(ctx, account); err != nil {
		t.Fatalf("Set: %v", err)
	}

	counter := newCommandCounter(client)
	if _, err := accountBase.Get(ctx, account.GetRandId()); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got := counter.count("getex account:" + account.GetRandId()); got != 0 {
		t.Fatalf("issued %d GETEX with touch-on-read off", got)
	}
	if got := counter.count("get account:" + account.GetRandId()); got != 1 {
		t.Fatalf("issued %d GET, want 1", got)
	}
}

// MinimalTarget renders to very little JSON, so the untagged-field check cannot rely
// on the related entity being bulky enough to notice.
type MinimalTarget struct {
	*item.Foundation
}

type LeakyHolder struct {
	*item.Foundation
	TargetRandId string         `json:"targetRandId"`
	Target       *MinimalTarget `json:"target"`
}

type TidyHolder struct {
	*item.Foundation
	TargetRandId string         `json:"targetRandId"`
	Target       *MinimalTarget `json:"-"`
}

func TestUntaggedFieldIsCaughtEvenWhenTheRelatedEntityIsTiny(t *testing.T) {
	_, client := newTestRedis(t)
	targetBase := mustBase[*MinimalTarget](t, client, "target:%s", baseTTL)

	if _, err := Relate(targetBase,
		func(h *LeakyHolder) string { return h.TargetRandId },
		func(h *LeakyHolder, m *MinimalTarget) { h.Target = m },
	); !errors.Is(err, ErrRelationNotTransient) {
		t.Fatalf("Relate = %v, want ErrRelationNotTransient", err)
	}

	if _, err := Relate(targetBase,
		func(h *TidyHolder) string { return h.TargetRandId },
		func(h *TidyHolder, m *MinimalTarget) { h.Target = m },
	); err != nil {
		t.Fatalf("Relate on a tagged field: %v", err)
	}
}
