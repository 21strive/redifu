package outbox

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21strive/redifu"
	"github.com/alicebob/miniredis/v2"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// Generations (NewBase, NewSorted, NewTimeline), Purge, and two builds on one Redis and one outbox.

func TestRegisterRefusesOneFormatUnderTwoGenerations(t *testing.T) {
	mr := miniredis.RunT(t)
	store := NewStore(nil, redis.NewClient(&redis.Options{Addr: mr.Addr()}), true).WithBuild("build-1", 0)
	if _, err := NewBase[*unitEntity](store, "thing:%s", EntityTTL, "s1"); err != nil {
		t.Fatalf("first Base: %v", err)
	}
	if _, err := NewBase[*unitEntity](store, "thing:%s", EntityTTL, "s1"); err != nil {
		t.Fatalf("a second Base of the same format and generation: %v", err)
	}
	if _, err := NewBase[*unitEntity](store, "thing:%s", EntityTTL, "s2"); err == nil {
		t.Fatal("one format under two generations was accepted")
	}
	if _, err := NewBase[*unitEntity](store, "other:%s", EntityTTL, ""); err == nil {
		t.Fatal("a Base without a generation was accepted")
	}
	base, _ := NewBase[*unitEntity](store, "thing:%s", EntityTTL, "s1")
	if _, err := NewSorted[*unitEntity](store, base, "thing:%s", ListTTL); err == nil {
		t.Fatal("an index under an item's format was accepted")
	}
	sorted, err := NewSorted[*unitEntity](store, base, "thing:list:%s", ListTTL)
	if err != nil {
		t.Fatalf("NewSorted: %v", err)
	}
	if sorted.Generation() != "build-1" || base.Generation() != "s1" {
		t.Fatalf("generations: index %q, base %q", sorted.Generation(), base.Generation())
	}
	formats := store.Formats()
	if len(formats) != 2 || formats[0].Format != "thing:%s" || formats[1].Format != "thing:list:%s" || !formats[1].Index {
		t.Fatalf("Formats = %+v", formats)
	}
}

// A binary without a revision (this test binary) takes a fingerprint of its own file: stable for the
// binary, different for any other build.
func TestBuildGenerationWithoutARevisionFingerprintsTheBinary(t *testing.T) {
	got := BuildGeneration()
	if !strings.HasPrefix(got, "bin-") || len(got) != len("bin-")+16 {
		t.Fatalf("BuildGeneration in a test binary = %q, want bin-<16 hex>", got)
	}
	if again := executableGeneration(); again != got {
		t.Fatalf("the fingerprint moved: %q then %q", got, again)
	}
	if store := NewStore(nil, nil, true).BuildGeneration(); store != got {
		t.Fatalf("a store's build generation = %q", store)
	}
	if order := BuildOrder(); order <= 0 {
		t.Fatalf("BuildOrder without BUILD_TIME = %d, want the binary's modification time", order)
	}
}

// purgeFormats is a keyspace the cache shares with other data: the cache's keys, the keys of the
// release before generations, and everything else in the same Redis.
func purgeFormats() []KeyFormat {
	return []KeyFormat{
		{Format: "post:%s", Generation: "s1"},
		{Format: "post:feed:%s:%s", Index: true, Generation: "b"},
		{Format: "post:month:%s:%s", Index: true, Generation: "b"},
		{Format: "post_search:%s", Generation: "b"},
		{Format: "coupon:%s", Generation: "s2"},
		{Format: "coupon:list:%s", Index: true, Generation: "b"},
		{Format: "view_page:%s", Generation: "b"},
		{Format: "view_epoch:%s", Generation: "s3"},
		{Format: "tag:list", Index: true, Generation: "b"},
		{Format: "message:inbox:%s:%s", Index: true, Generation: "b"},
	}
}

func TestPurgeMatchersTakeTheCacheAndNothingElse(t *testing.T) {
	matchers := purgeMatchers(purgeFormats(), `v[0-9]+`, []string{"post_search_gen:%s"})
	deleted := map[string]string{
		"post:0b6c2f8e-1":                      "post:%s",
		"post:0b6c2f8e-1:blank":                "post:%s",
		"{post:feed:o1:ALL}":                   "post:feed:%s:%s",
		"{post:feed:o1:ALL}:lastpage":          "post:feed:%s:%s",
		"{post:feed:o1:ALL}:gen":               "post:feed:%s:%s",
		"{post:month:o1:2026-10}:blankpage":    "post:month:%s:%s",
		"post_search:o1|7|abc":                 "post_search:%s",
		"coupon:7a1e":                          "coupon:%s",
		"{coupon:list:o1}":                     "coupon:list:%s",
		"view_page:o1|ab12":                    "view_page:%s",
		"view_epoch:o1":                        "view_epoch:%s",
		"{tag:list}":                           "tag:list",
		"{tag:list}:gen":                       "tag:list",
		"{message:inbox:o1:owner}":             "message:inbox:%s:%s",
		"post:v3:0b6c2f8e-1":                   "legacy post:%s",
		"{post:feed:v3:o1:ALL}:firstpage":      "legacy post:feed:%s:%s",
		"post_search:v3:o1:7:abc":              "legacy post_search:%s",
		"view_page:v1:o1:ab12":                 "legacy view_page:%s",
		"view_epoch:v1:o1":                     "legacy view_epoch:%s",
		"{tag:list:v3}":                        "legacy tag:list",
		"{message:inbox:v3:o1:m1}":             "legacy message:inbox:%s:%s",
		"post_search_gen:v3:o1":                "legacy post_search_gen:%s",
		"coupon:v3:7a1e:blank":                 "legacy coupon:%s",
		"{coupon:list:v3:o1}:blankpage":        "legacy coupon:list:%s",
		"{post:month:v3:o1:2026-10}:blankpage": "legacy post:month:%s:%s",
		"{post:feed:v12:o1:INCOMING}:lastpage": "legacy post:feed:%s:%s",
		// The release before generations had no :gen marker, but a stray one goes too.
		"{message:inbox:v3:o1:owner}:gen": "legacy message:inbox:%s:%s",
	}
	kept := []string{
		"hits:o1:2026-10-19:v:visit", "hits:dirty", "hits:rl:o1:29123", "stats:ov:o1:ab", "stats:hits:o1:cd",
		"oauth_state:tok", "geo:route:car:1,2:3,4", "geo:region:1,2",
		"quota:o1:3:1:x:100000", "quota:version:o1",
		"coupon:check:rejections:ana:29123", "post_search_gen:o1",
		"post:feed:o1:ALL",            // an index format without its hash tag
		"{post:0b6c2f8e-1}",           // an item format inside a hash tag
		"post:a:b",                    // a parameter never spans segments
		"view_page:o1:ab12",           // the old separator, without the version
		"{post:feed:o1:ALL}:nextpage", // not a marker
		"coupon:list:o1",              // an index without its tag
		"posts:x", "xpost:1", "post", "", "{}",
	}
	for key, want := range deleted {
		if got := match(matchers, key); got != want {
			t.Errorf("%q: matched %q, want %q", key, got, want)
		}
	}
	for _, key := range kept {
		if got := match(matchers, key); got != "" {
			t.Errorf("%q is not the cache's but matched %q", key, got)
		}
	}
}

func TestPurgeDeletesTheCacheAndCountsTheRest(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	store := NewStore(nil, rdb, true).WithLegacyFormats(`v[0-9]+`, "post_search_gen:%s")
	for _, format := range purgeFormats() {
		if err := store.register(format.Format, format.Index, format.Generation, func(string) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	cache := []string{"post:u1", "post:u1:blank", "{post:feed:o1:ALL}", "{post:feed:o1:ALL}:gen",
		"post_search:o1|1|h", "view_page:o1|h", "{tag:list}", "post:v3:u1", "view_epoch:v1:o1",
		"{post:feed:v3:o1:ALL}:lastpage", "post_search_gen:v3:o1"}
	other := []string{"hits:o1:2026-10-19:v:visit", "hits:dirty", "stats:ov:o1:h", "oauth_state:t",
		"geo:route:car:a:b", "post_search_gen:o1", "coupon:check:rejections:ana:1"}
	for _, key := range append(append([]string{}, cache...), other...) {
		if err := mr.Set(key, "x"); err != nil {
			t.Fatal(err)
		}
	}
	// a few hundred more, so that SCAN takes several steps
	for i := 0; i < 1200; i++ {
		_ = mr.Set("post:bulk-"+itoa(i), "x")
		_ = mr.Set("hits:bulk:"+itoa(i), "x")
	}

	dry, err := store.Purge(ctx, rdb, true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if dry.Total() != int64(len(cache)+1200) || dry.Scanned != int64(len(cache)+len(other)+2400) || len(mr.Keys()) != len(cache)+len(other)+2400 {
		t.Fatalf("dry run: %d to delete of %d scanned, %d keys left", dry.Total(), dry.Scanned, len(mr.Keys()))
	}

	report, err := store.Purge(ctx, rdb, false)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	left := mr.Keys()
	sort.Strings(left)
	want := append([]string{}, other...)
	for i := 0; i < 1200; i++ {
		want = append(want, "hits:bulk:"+itoa(i))
	}
	sort.Strings(want)
	if strings.Join(left, ",") != strings.Join(want, ",") {
		t.Fatalf("left after the purge: %d keys, want the %d others", len(left), len(want))
	}
	if report.Deleted["legacy post:%s"] != 1 || report.Deleted["post:%s"] != 1202 || report.Kept["hits"] != 1202 {
		t.Fatalf("report: %+v", report)
	}
	if again, _ := store.Purge(ctx, rdb, false); again.Total() != 0 {
		t.Fatalf("a second purge deleted %d keys", again.Total())
	}
	if lines := report.Lines(); len(lines) < 3 || !strings.HasPrefix(lines[0], "scanned ") {
		t.Fatalf("lines: %v", lines)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// ---------------------------------------------------------------- two builds on one Redis and one outbox

// buildStack is one binary's cache over the unit table: its own store (build generation and
// order), Base (shape), List and Detail, and its own drain handler, on the database and Redis of the
// fixture. Two of them stand for the old and the new task of a rolling deploy. hot, when set, is an
// index only this build has (the rows with sort < 10): what a deploy adding a cached view brings.
type buildStack struct {
	store  *Store
	base   *redifu.Base[*unitEntity]
	list   *List[*unitEntity]
	hot    *List[*unitEntity]
	detail *Detail[*unitEntity]
	loads  atomic.Int64 // database reads of the seeds and the detail loads
}

func newBuildStack(t *testing.T, f *unitFixture, build string, order int64, shape string, withHot bool) *buildStack {
	t.Helper()
	s := &buildStack{store: NewStore(f.db, f.rdb, true).WithBuild(build, order)}
	query := `SELECT uuid, randid, created_at, updated_at, user_uuid, name, sort, hidden FROM ` + f.table
	var err error
	if s.base, err = NewBase[*unitEntity](s.store, "unit:%s", EntityTTL, shape); err != nil {
		t.Fatal(err)
	}
	owner := func(params []string) string { return params[0] }
	if s.list, err = NewList[*unitEntity](s.store, s.base, "unit:list:%s", redifu.Ascending, []string{f.table},
		func(ctx context.Context, tx *sqlx.Tx, params []string) ([]*unitEntity, error) {
			s.loads.Add(1)
			return loadUnitItems(ctx, tx, query+` WHERE user_uuid = $1 ORDER BY sort, uuid COLLATE "C"`, params[0])
		}, owner); err != nil {
		t.Fatal(err)
	}
	if withHot {
		if s.hot, err = NewList[*unitEntity](s.store, s.base, "unit:hot:%s", redifu.Ascending, []string{f.table},
			func(ctx context.Context, tx *sqlx.Tx, params []string) ([]*unitEntity, error) {
				s.loads.Add(1)
				return loadUnitItems(ctx, tx, query+` WHERE user_uuid = $1 AND sort < 10 ORDER BY sort, uuid COLLATE "C"`, params[0])
			}, owner); err != nil {
			t.Fatal(err)
		}
	}
	s.detail = NewDetail[*unitEntity](s.store, s.base, []string{f.table}, func(e *unitEntity) string { return e.UserUUID },
		func(ctx context.Context, q sqlx.QueryerContext, keys []string) ([]*unitEntity, error) {
			s.loads.Add(1)
			return loadUnitItems(ctx, q, query+` WHERE uuid = ANY($1)`, pqArray(keys))
		})
	// The fixture's handler, pointed at this stack's Base and List, and one for the hot list.
	s.store.Register(unitHandler{f: &unitFixture{db: f.db, table: f.table, base: s.base, list: s.list}})
	if withHot {
		s.store.Register(hotHandler{table: f.table, hot: s.hot})
	}
	return s
}

// hotHandler keeps the hot list: a row is a member while its sort is below 10.
type hotHandler struct {
	table string
	hot   *List[*unitEntity]
}

func (h hotHandler) Tables() []string { return []string{h.table} }

func (h hotHandler) Apply(batch *Batch, table string, changes []Change) error {
	for _, change := range changes {
		var rows []*unitEntity
		rows, err := loadUnitItems(batch.Ctx, batch.Tx, `SELECT uuid, randid, created_at, updated_at, user_uuid, name, sort, hidden FROM `+h.table+` WHERE uuid = $1`, change.Key)
		if err != nil {
			return err
		}
		if old := change.Old(); old != nil && old.Int("sort") < 10 {
			stub := &unitEntity{unitItem: &unitItem{Record: &redifu.Record{UUID: change.Key}}}
			if err := h.hot.Remove(batch.Ctx, batch.Pipe, stub, old.Str("user_uuid")); err != nil {
				return err
			}
		}
		if len(rows) == 1 && rows[0].Sort < 10 {
			if err := h.hot.Add(batch.Ctx, batch.Pipe, rows[0], rows[0].UserUUID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *buildStack) readList(t *testing.T, list *List[*unitEntity], owner string) (string, bool) {
	t.Helper()
	items, ok := list.Read(context.Background(), owner)
	return names(items), ok
}

func (s *buildStack) read(t *testing.T, owner string) string {
	t.Helper()
	got, ok := s.readList(t, s.list, owner)
	if !ok {
		t.Fatalf("read %s: not served from the cache", owner)
	}
	return got
}

// everyCheck makes every lease check go to Redis, so a takeover shows in the next call.
func everyCheck(t *testing.T) {
	previous := leaseCheck
	leaseCheck = 0
	t.Cleanup(func() { leaseCheck = previous })
}

// Two builds of one shape: the newer build takes the cache over at its first read, keeps the
// entities the older one wrote (no reload), seeds its own indexes, and from then on is the only one
// that drains: the older build reads SQL and its drains are fenced.
func TestTwoBuildsOfOneShapeShareEntitiesAndNeverIndexes(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	old, next := newBuildStack(t, f, "build-old", 1, shape, false), newBuildStack(t, f, "build-new", 2, shape, false)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2)`)

	if got := old.read(t, "o1"); got != "A,B" {
		t.Fatalf("old cold = %q", got)
	}
	if holder, _ := f.mr.Get(leaseBuildKey); leaseBuildOf(holder) != "1|build-old" {
		t.Fatalf("lease = %q", holder)
	}

	// The newer build reads the older one's entity as its own: no database read.
	loads := next.loads.Load()
	found, ok := next.detail.Get(f.ctx, "a")
	if !ok || found["a"] == nil || found["a"].Name.String != "A" || next.loads.Load() != loads {
		t.Fatalf("the new build reloaded the old one's entity: %v, %v, %d loads", found, ok, next.loads.Load()-loads)
	}
	if holder, _ := f.mr.Get(leaseBuildKey); leaseBuildOf(holder) != "2|build-new" {
		t.Fatalf("lease after the new build's read = %q", holder)
	}
	// Its index it seeds itself.
	if got := next.read(t, "o1"); got != "A,B" || next.loads.Load() != loads+1 {
		t.Fatalf("new = %q after %d loads", got, next.loads.Load()-loads)
	}
	if marker, _ := f.mr.Get("{unit:list:o1}:gen"); marker != withEpoch("build-new", next.store.Epoch()) {
		t.Fatalf("index marker = %q", marker)
	}
	// Warm within its build.
	loads = next.loads.Load()
	if got := next.read(t, "o1"); got != "A,B" || next.loads.Load() != loads {
		t.Fatalf("new warm = %q after %d loads", got, next.loads.Load()-loads)
	}

	// The older build no longer uses the cache: its reads go to SQL, its drains are fenced.
	if _, ok := old.readList(t, old.list, "o1"); ok {
		t.Fatal("the old build served a read after the new one took the cache over")
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('c','o1','C',3)`)
	if done, err := old.store.DrainOwner(f.ctx, "o1"); err != nil || done {
		t.Fatalf("old drain after the takeover: done %v, %v", done, err)
	}
	var queued int
	f.db.Get(&queued, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table)
	if queued == 0 {
		t.Fatal("the old build's fenced drain took the queue")
	}
	if got := next.read(t, "o1"); got != "A,B,C" {
		t.Fatalf("new after the insert = %q", got)
	}
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	f.exec(t, `UPDATE TBL SET user_uuid = 'o2' WHERE uuid = 'b'`)
	if got := next.read(t, "o1"); got != "A2,C" {
		t.Fatalf("new after rename and move = %q", got)
	}
	if got := next.read(t, "o2"); got != "B" {
		t.Fatalf("new o2 = %q", got)
	}
	f.db.Get(&queued, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table)
	if queued != 0 {
		t.Fatalf("%d outbox rows left", queued)
	}

	// The newer build gone (its lease not renewed past its TTL), the older build takes over again.
	f.mr.FastForward(leaseTTL + time.Second)
	if got := old.read(t, "o1"); got != "A2,C" {
		t.Fatalf("old after the new build left = %q", got)
	}
	if holder, _ := f.mr.Get(leaseBuildKey); leaseBuildOf(holder) != "1|build-old" {
		t.Fatalf("lease = %q", holder)
	}
}

// Two builds of different shapes: the newer build reads every value the older one wrote as a
// miss and loads it again, so it never decodes the older shape.
func TestTwoShapesReloadEachOthersEntities(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	old := newBuildStack(t, f, "build-old", 1, Shape(unitItem{}, "unit columns"), false)
	next := newBuildStack(t, f, "build-new", 2, Shape(unitItem{}, "unit columns", "a column added"), false)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2)`)

	if got := old.read(t, "o1"); got != "A,B" {
		t.Fatalf("old cold = %q", got)
	}
	if _, err := next.base.Get(f.ctx, "a"); err != redifu.ErrNotFound {
		t.Fatalf("the new shape read the old one's value: %v", err)
	}
	loads := next.loads.Load()
	found, ok := next.detail.Get(f.ctx, "a")
	// A miss costs the detail its two reads (whose entity it is, then the entity under that owner's
	// lock): the old shape's value is not decoded.
	if !ok || found["a"] == nil || found["a"].Name.String != "A" || next.loads.Load() != loads+2 {
		t.Fatalf("detail of the old shape's entity: %v, %v, %d loads", found, ok, next.loads.Load()-loads)
	}
	if raw, _ := f.mr.Get("unit:a"); !strings.HasPrefix(raw, next.base.Generation()+"\x1f") {
		t.Fatalf("unit:a = %q, want the new shape's value", raw)
	}
	if got := next.read(t, "o1"); got != "A,B" {
		t.Fatalf("new = %q", got)
	}
	if raw, _ := f.mr.Get("unit:b"); !strings.HasPrefix(raw, next.base.Generation()+"\x1f") {
		t.Fatalf("unit:b = %q, want the new shape's value after the seed", raw)
	}
	f.exec(t, `UPDATE TBL SET name = 'B2' WHERE uuid = 'b'`)
	if got := next.read(t, "o1"); got != "A,B2" {
		t.Fatalf("new after rename = %q", got)
	}
}

// The gap the lease closes: an older build that drains applies its own rules, and an index
// it does not know — one the newer build added — never hears of the change. Without the lease the
// newer build serves that index stale.
func TestWithoutTheLeaseAnOlderBuildLeavesANewIndexStale(t *testing.T) {
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	old, next := newBuildStack(t, f, "build-old", 1, shape, false), newBuildStack(t, f, "build-new", 2, shape, true)
	old.store.lease, next.store.lease = nil, nil // both drain, as before the lease
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got, ok := next.readList(t, next.hot, "o1"); !ok || got != "A" {
		t.Fatalf("hot = %q, %v", got, ok)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('b','o1','B',2)`)
	if done, err := old.store.DrainOwner(f.ctx, "o1"); err != nil || !done {
		t.Fatalf("old drain: %v, %v", done, err)
	}
	if got, _ := next.readList(t, next.hot, "o1"); got != "A" {
		t.Fatalf("hot = %q: expected the stale list this test documents", got)
	}
}

// With the lease the older build cannot drain once the newer one runs, so the newer build applies
// every change by its own rules and its new index is never stale.
func TestTheLeaseKeepsANewIndexCurrent(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	old, next := newBuildStack(t, f, "build-old", 1, shape, false), newBuildStack(t, f, "build-new", 2, shape, true)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := old.read(t, "o1"); got != "A" {
		t.Fatalf("old = %q", got)
	}
	if got, ok := next.readList(t, next.hot, "o1"); !ok || got != "A" {
		t.Fatalf("hot = %q, %v", got, ok)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('b','o1','B',2)`)
	if done, err := old.store.DrainOwner(f.ctx, "o1"); err != nil || done {
		t.Fatalf("old drain was not fenced: %v, %v", done, err)
	}
	if got, ok := next.readList(t, next.hot, "o1"); !ok || got != "A,B" {
		t.Fatalf("hot = %q, %v", got, ok)
	}
	f.exec(t, `UPDATE TBL SET sort = 20 WHERE uuid = 'a'`)
	if n, err := old.store.DrainAll(f.ctx); err != nil || n != 0 {
		t.Fatalf("old background drain: %d, %v", n, err)
	}
	if got, ok := next.readList(t, next.hot, "o1"); !ok || got != "B" {
		t.Fatalf("hot after a leaves = %q, %v", got, ok)
	}
}

// A takeover between a drain's check and its write aborts the write and leaves the changes queued.
func TestATakeoverAbortsADrainInFlight(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	old := newBuildStack(t, f, "build-old", 1, shape, false)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := old.read(t, "o1"); got != "A" {
		t.Fatalf("old = %q", got)
	}
	// The newer build takes the lease while the old build's handler runs, before its MULTI/EXEC.
	next := newBuildStack(t, f, "build-new", 2, shape, false)
	old.store.Register(handlerFunc{table: f.table, apply: func(*Batch, string, []Change) error {
		if !next.store.holdsLease(f.ctx) {
			t.Error("the new build did not take the lease")
		}
		return nil
	}})
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	if done, err := old.store.DrainOwner(f.ctx, "o1"); err != nil || done {
		t.Fatalf("old drain = %v, %v: the takeover did not abort it", done, err)
	}
	var queued int
	f.db.Get(&queued, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table)
	if queued != 1 {
		t.Fatalf("%d rows queued after the aborted drain, want 1", queued)
	}
	if entity, _ := old.base.Get(f.ctx, "a"); entity == nil || entity.Name.String != "A" {
		t.Fatalf("the aborted drain wrote Redis: %+v", entity)
	}
	if got := next.read(t, "o1"); got != "A2" {
		t.Fatalf("new = %q", got)
	}
}

type handlerFunc struct {
	table string
	apply func(*Batch, string, []Change) error
}

func (h handlerFunc) Tables() []string { return []string{h.table} }
func (h handlerFunc) Apply(batch *Batch, table string, changes []Change) error {
	return h.apply(batch, table, changes)
}

// A one-off command (ObserveLease) of a newer build uses the cache only while its own build holds the
// lease and never takes it from the builds serving traffic.
func TestAnObservingProcessNeverTakesTheLease(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	serving := newBuildStack(t, f, "build-serving", 1, shape, false)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := serving.read(t, "o1"); got != "A" {
		t.Fatalf("serving = %q", got)
	}
	leaseObserveOnly.Store(true)
	t.Cleanup(func() { leaseObserveOnly.Store(false) })
	command := newBuildStack(t, f, "build-newer", 2, shape, false)
	if _, ok := command.readList(t, command.list, "o1"); ok {
		t.Fatal("an observing process of a newer build used the cache")
	}
	if holder, _ := f.mr.Get(leaseBuildKey); leaseBuildOf(holder) != "1|build-serving" {
		t.Fatalf("lease = %q: the observing process took it", holder)
	}
	same := newBuildStack(t, f, "build-serving", 1, shape, false)
	if got, ok := same.readList(t, same.list, "o1"); !ok || got != "A" {
		t.Fatalf("an observing process of the serving build: %q, %v", got, ok)
	}
}
