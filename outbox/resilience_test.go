package outbox

// What the engine does when Redis or the database misbehave under it: entities evicted from under
// an index, a seed or a drain landing while others read, a feed index outliving or outlived by its
// markers, an owner whose drain keeps failing, and a pool with no connection to spare. Same
// fixture as store_test.go (REDIFU_TEST_DSN; skips without it).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// baseKeyOf is the Redis key of the unit entity uuid (the Base writes it under the fixture's
// format, possibly hash-tagged).
func (f *unitFixture) baseKeyOf(t *testing.T, uuid string) string {
	t.Helper()
	for _, key := range f.mr.Keys() {
		if key == "unit:"+uuid {
			return key
		}
	}
	t.Fatalf("no Base key for %s among %v", uuid, f.mr.Keys())
	return ""
}

// An entity evicted from under a seeded list (maxmemory with a volatile-* policy evicts any key
// with a TTL) must not quietly shorten the list: the read rebuilds the index from the database.
func TestListRebuildsWhenAMemberEntityIsGone(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2), ('c','o1','C',3), ('d','o1','D',4), ('e','o1','E',5)`)
	if got := f.read(t, "o1"); got != "A,B,C,D,E" {
		t.Fatalf("cold read = %q", got)
	}
	f.mr.Del(f.baseKeyOf(t, "c"))
	for i := 0; i < 2; i++ {
		if got := f.read(t, "o1"); got != "A,B,C,D,E" {
			t.Fatalf("read %d after the eviction = %q, want every member", i+1, got)
		}
	}
	if n, ok := f.list.Count(f.ctx, "o1"); !ok || n != 5 {
		t.Fatalf("count after the eviction = %d %v", n, ok)
	}
}

// The calendar's month buckets: same rule, a member evicted from Base stays on the calendar.
func TestBucketsRebuildAMonthWhenAMemberEntityIsGone(t *testing.T) {
	f := newUnitFixture(t)
	month := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix()
	buckets, err := NewBuckets[*unitEntity](f.store, f.base, "unit:month:%s:%s", []string{f.table},
		func(ctx context.Context, tx *sqlx.Tx, owner string, from, to int64) ([]*unitEntity, error) {
			return loadUnitItems(ctx, tx, `SELECT uuid, randid, created_at, updated_at, user_uuid, name, sort, hidden FROM `+f.table+
				` WHERE user_uuid = $1 AND sort >= $2 AND sort < $3 ORDER BY sort, uuid COLLATE "C"`, owner, from, to)
		})
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"a", "b", "c", "d"} {
		f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ($1,'o1',$2,$3)`, id, strings.ToUpper(id), month+int64(i)*86400)
	}
	read := func() string {
		t.Helper()
		items, ok := buckets.Read(f.ctx, "o1", month, month+31*86400-1)
		if !ok {
			t.Fatal("month not served from the cache")
		}
		return names(items)
	}
	if got := read(); got != "A,B,C,D" {
		t.Fatalf("cold read = %q", got)
	}
	f.mr.Del(f.baseKeyOf(t, "b"))
	if got := read(); got != "A,B,C,D" {
		t.Fatalf("after the eviction = %q, want every member of the month", got)
	}
}

// pipelineRecorder records, for every pipeline the client sends, whether it is a MULTI/EXEC
// transaction and which commands it holds.
type pipelineRecorder struct {
	mu   sync.Mutex
	sent [][]string
}

func (r *pipelineRecorder) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (r *pipelineRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (r *pipelineRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		var names []string
		for _, cmd := range cmds {
			names = append(names, strings.ToLower(cmd.Name()))
		}
		r.mu.Lock()
		r.sent = append(r.sent, names)
		r.mu.Unlock()
		return next(ctx, cmds)
	}
}

// writing returns the pipelines that wrote (a ZADD, SET or EVAL among their commands).
func (r *pipelineRecorder) writing() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]string
	for _, names := range r.sent {
		joined := " " + strings.Join(names, " ") + " "
		if strings.Contains(joined, " zadd ") || strings.Contains(joined, " set ") || strings.Contains(joined, " eval ") {
			out = append(out, names)
		}
	}
	return out
}

// A seed and a drain reach Redis as one MULTI/EXEC each: a plain pipeline over 16 KB is read by
// Redis in chunks with other clients' commands in between, and a reader that came in the middle
// would take the part it saw for the whole list.
func TestSeedAndDrainReachRedisAsOneTransaction(t *testing.T) {
	f := newUnitFixture(t)
	recorder := &pipelineRecorder{}
	f.rdb.AddHook(recorder)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2)`)
	if got := f.read(t, "o1"); got != "A,B" { // seed
		t.Fatalf("cold read = %q", got)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('c','o1','C',3)`)
	if got := f.read(t, "o1"); got != "A,B,C" { // drain
		t.Fatalf("after the insert = %q", got)
	}
	writes := recorder.writing()
	if len(writes) < 2 {
		t.Fatalf("%d writing pipelines, want the seed and the drain: %v", len(writes), writes)
	}
	for _, names := range writes {
		if names[0] != "multi" || names[len(names)-1] != "exec" {
			t.Fatalf("a writing pipeline outside MULTI/EXEC: %v", names)
		}
	}
}

// A feed whose index expires while the :lastpage marker of its last top-up lives on must still
// walk every row: the next head seed drops the stale marker, and top-ups renew the index's TTL.
func TestFeedKeepsItsEndAfterTheIndexExpires(t *testing.T) {
	f := newFeedFixture(t, 10)
	f.insert(t, "o1", "INCOMING", 120, false)
	want := f.sqlOrder(t, "user_uuid = 'o1'")

	if _, ok := f.feed.Read(f.ctx, []string{"o1"}, redifu.Cursor{}); !ok { // head seed at T0
		t.Fatal("head not served")
	}
	step := ListTTL / 48
	f.mr.FastForward(step)
	if got := f.walk(t, f.feed, "o1"); len(got) != len(want) { // top-ups to the end at T0+step
		t.Fatalf("first walk: %d of %d rows", len(got), len(want))
	}
	f.mr.FastForward(ListTTL - step/2) // past the head seed's TTL, inside the top-ups'
	if got := f.walk(t, f.feed, "o1"); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("walk at T0+ListTTL+step/2: %d of %d rows", len(got), len(want))
	}

	// The index alone gone (evicted) under a live :lastpage marker.
	for _, key := range f.mr.Keys() {
		if strings.Contains(key, "unit:feed:o1") && !strings.Contains(key, ":lastpage") && !strings.Contains(key, ":blankpage") && !strings.Contains(key, ":firstpage") {
			f.mr.Del(key)
		}
	}
	if got := f.walk(t, f.feed, "o1"); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("walk after the index alone was evicted: %d of %d rows", len(got), len(want))
	}
}

// failingHandler fails every change of the owners it is told to fail, and counts its calls.
type failingHandler struct {
	table string
	fail  func(owner string) bool
	calls *atomic.Int64
}

func (h failingHandler) Tables() []string { return []string{h.table} }

func (h failingHandler) Apply(batch *Batch, table string, changes []Change) error {
	h.calls.Add(1)
	if h.fail(batch.Owner) {
		return errors.New("poisoned row")
	}
	return nil
}

// newWatchedTable creates a second watched table like the fixture's (dropped at the end).
func (f *unitFixture) newWatchedTable(t *testing.T) string {
	t.Helper()
	table := fmt.Sprintf("zz_unit_other_%d", time.Now().UnixNano())
	f.exec(t, fmt.Sprintf(`CREATE TABLE %s (uuid varchar PRIMARY KEY, user_uuid varchar NOT NULL)`, table))
	t.Cleanup(func() {
		f.db.Exec(fmt.Sprintf(`DROP TABLE %s`, table))
		f.db.Exec(`DELETE FROM redifu_outbox WHERE table_name = $1`, table)
	})
	f.exec(t, `SELECT redifu_watch($1::regclass, 'uuid', 'user_uuid')`, table)
	f.store.refreshWatched(f.ctx)
	return table
}

// A view across accounts (owner "*") depends only on the queues of its own tables, and an owner
// whose drain just failed is not retried by every read of the view.
func TestServeAllOwnersAppliesOnlyItsTablesAndBacksOff(t *testing.T) {
	f := newUnitFixture(t)
	other := f.newWatchedTable(t)
	var otherCalls, viewCalls atomic.Int64
	f.store.Register(failingHandler{table: other, fail: func(string) bool { return true }, calls: &otherCalls})
	f.store.Register(failingHandler{table: f.table, fail: func(owner string) bool { return owner == "bad" }, calls: &viewCalls})

	f.exec(t, fmt.Sprintf(`INSERT INTO %s (uuid, user_uuid) VALUES ('x','poisoned')`, other))
	for i := 0; i < 3; i++ {
		if !f.store.Serve(f.ctx, "*", f.table) {
			t.Fatalf("read %d of the view: a poisoned row of another table sent it to SQL", i+1)
		}
	}
	if n := otherCalls.Load(); n != 0 {
		t.Fatalf("the view ran the other table's drain %d times", n)
	}

	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('b1','bad','B',1)`)
	if f.store.Serve(f.ctx, "*", f.table) {
		t.Fatal("served while an owner of the view's table cannot be drained")
	}
	calls := viewCalls.Load()
	for i := 0; i < 3; i++ {
		if f.store.Serve(f.ctx, "*", f.table) {
			t.Fatal("served during the failing owner's pause")
		}
	}
	if viewCalls.Load() != calls {
		t.Fatalf("the failing drain ran %d more times during its pause", viewCalls.Load()-calls)
	}
}

// The background drain takes at most 200 owners a round; owners whose drain keeps failing wait out
// their pause instead of filling every round, so the others still drain.
func TestDrainAllLetsOtherOwnersPastFailingOnes(t *testing.T) {
	f := newUnitFixture(t)
	other := f.newWatchedTable(t)
	var calls atomic.Int64
	f.store.Register(failingHandler{table: other, fail: func(owner string) bool { return strings.HasPrefix(owner, "bad") }, calls: &calls})
	f.exec(t, fmt.Sprintf(`INSERT INTO %s (uuid, user_uuid) SELECT 'r' || i, 'bad' || i FROM generate_series(1, 201) AS i`, other))
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('g','good','G',1)`)

	if _, err := f.store.DrainAll(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DrainAll(f.ctx); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := f.db.Get(&left, `SELECT count(*) FROM redifu_outbox WHERE owner_uuid = 'good'`); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("the healthy owner's queue was not drained behind %d failing owners", 201)
	}
}

// A pool with no connection to spare (a request holds it in its transaction, and asks the cache)
// sends the read to SQL at once instead of waiting for a connection that only its own transaction
// could give back.
func TestStoreGivesUpOnABusyPool(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	small, err := sqlx.Connect("postgres", os.Getenv("REDIFU_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { small.Close() })
	small.SetMaxOpenConns(1)
	store := NewStore(small, f.rdb, true)
	store.Register(unitHandler{f: f})
	if !store.Ready(f.ctx) || !store.Serve(f.ctx, "o1", f.table) {
		t.Fatal("not served with a free pool")
	}

	tx := small.MustBegin() // the request's own transaction holds the only connection
	defer tx.Rollback()
	start := time.Now()
	if store.Serve(f.ctx, "o1", f.table) {
		t.Fatal("served without a connection")
	}
	if seeded, err := store.Seed(f.ctx, "o1", func(*sqlx.Tx, redis.Pipeliner) error { return nil }); seeded || err != nil {
		t.Fatalf("seed without a connection: seeded=%v err=%v", seeded, err)
	}
	if took := time.Since(start); took > redifuConnWait {
		t.Fatalf("gave up after %s", took)
	}
	tx.Rollback()
	if !store.Serve(f.ctx, "o1", f.table) {
		t.Fatal("not served once the connection was back")
	}
}

// A seed never runs ahead of its owner's queue. A read checks the queue, a write commits, and the
// read's seed then loads the written row while the write is still queued; when the write is reverted
// before the drain, the drain sees old row = current row and leaves the seeded state in Redis. So a
// seed with queued changes is refused (the read goes to SQL), and a seed reads one snapshot.
func TestSeedWaitsForTheOwnersQueue(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := f.read(t, "o1"); got != "A" {
		t.Fatalf("o1 = %q", got)
	}
	if got := f.read(t, "o2"); got != "" {
		t.Fatalf("o2 = %q", got)
	}
	if err := f.list.Sorted().Purge(f.ctx, "o2"); err != nil { // o2's list cold again
		t.Fatal(err)
	}
	f.exec(t, `UPDATE TBL SET user_uuid = 'o2' WHERE uuid = 'a'`) // queued for o1 and o2
	if f.list.Seed(f.ctx, "o2") {                                 // a read's seed after its queue check
		t.Fatal("seeded o2 with its queue not applied")
	}
	f.exec(t, `UPDATE TBL SET user_uuid = 'o1' WHERE uuid = 'a'`) // reverted before any drain
	if got := f.read(t, "o2"); got != "" {
		t.Fatalf("o2 after the move and its revert = %q, want empty", got)
	}
	if got := f.read(t, "o1"); got != "A" {
		t.Fatalf("o1 after the move and its revert = %q", got)
	}
}

// midDrainWriter commits a write from another connection while a drain of its table is under way
// (between the drain's DELETE of the queue and the handler's reload), once.
type midDrainWriter struct {
	f     *unitFixture
	query string
	done  *atomic.Bool
}

func (w midDrainWriter) Tables() []string { return []string{w.f.table} }

func (w midDrainWriter) Apply(batch *Batch, table string, changes []Change) error {
	if w.done.CompareAndSwap(false, true) {
		_, err := w.f.db.Exec(strings.ReplaceAll(w.query, "TBL", w.f.table))
		return err
	}
	return nil
}

// A drain reads one snapshot too: a write committed while a drain runs is applied by the next drain
// only, from the state the first one wrote. Moving a row o1 → o2, then o2 → o3 during o1's drain,
// then o3 → o2: o3's list must not keep it.
func TestDrainReadsOneSnapshot(t *testing.T) {
	f := newUnitFixture(t)
	var done atomic.Bool
	handler := f.store.handlers[f.table]
	f.store.handlers[f.table] = append([]Handler{midDrainWriter{f: f, query: `UPDATE TBL SET user_uuid = 'o3' WHERE uuid = 'a'`, done: &done}}, handler...)
	done.Store(true) // not yet
	// o3's list holds another row: an empty list is never added to (its next read seeds it).
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('z','o3','Z',2)`)
	for _, owner := range []string{"o1", "o2", "o3"} {
		f.read(t, owner) // every list seeded
	}
	f.exec(t, `UPDATE TBL SET user_uuid = 'o2' WHERE uuid = 'a'`)
	done.Store(false) // o1's drain moves the row on to o3 while it runs
	if ok, err := f.store.DrainOwner(f.ctx, "o1"); !ok || err != nil {
		t.Fatalf("drain o1: %v %v", ok, err)
	}
	f.exec(t, `UPDATE TBL SET user_uuid = 'o2' WHERE uuid = 'a'`)
	for owner, want := range map[string]string{"o1": "", "o2": "A", "o3": "Z"} {
		if got := f.read(t, owner); got != want {
			t.Fatalf("%s = %q, want %q", owner, got, want)
		}
	}
}
