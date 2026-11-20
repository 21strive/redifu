package outbox

// Tests against a real PostgreSQL (the outbox triggers are the point) and miniredis. They run
// when REDIFU_TEST_DSN names a database the test may create tables in, e.g.
//
//	createdb redifu_outbox_unit
//	REDIFU_TEST_DSN="host=/var/run/postgresql dbname=redifu_outbox_unit sslmode=disable" go test ./outbox/
//
// and skip otherwise. The fixture installs Schema into that database (installSchema).

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/21strive/redifu"
	"github.com/alicebob/miniredis/v2"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type unitItem struct {
	*redifu.Record
	UserUUID string         `db:"user_uuid"`
	Name     sql.NullString `db:"name"`
	Sort     int64          `db:"sort"`
	Hidden   string         `json:"-" db:"hidden"`
}

type unitEntity struct {
	*unitItem
	Score int64 `json:"-"`
}

func (e *unitEntity) GetRandId() string            { return e.UUID }
func (e *unitEntity) SetScore(score int64)         { e.Score = score }
func (e *unitEntity) MarshalJSON() ([]byte, error) { return Encode(e.unitItem) }
func (e *unitEntity) UnmarshalJSON(data []byte) error {
	e.unitItem = &unitItem{}
	return Decode(data, e.unitItem)
}

type unitFixture struct {
	db    *sqlx.DB
	mr    *miniredis.Miniredis
	rdb   *redis.Client
	store *Store
	base  *redifu.Base[*unitEntity]
	list  *List[*unitEntity]
	ctx   context.Context
	table string
}

func loadUnitItems(ctx context.Context, q sqlx.QueryerContext, query string, args ...any) ([]*unitEntity, error) {
	var rows []*unitItem
	if err := sqlx.SelectContext(ctx, q, &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]*unitEntity, 0, len(rows))
	for _, row := range rows {
		out = append(out, &unitEntity{unitItem: row, Score: row.Sort})
	}
	return out, nil
}

type unitHandler struct{ f *unitFixture }

func (h unitHandler) Tables() []string { return []string{h.f.table} }

func (h unitHandler) Apply(batch *Batch, table string, changes []Change) error {
	keys := map[string]bool{}
	for _, change := range changes {
		keys[change.Key] = true
	}
	var list []string
	for key := range keys {
		list = append(list, key)
	}
	current, err := loadUnitItems(batch.Ctx, batch.Tx, fmt.Sprintf(`SELECT uuid, randid, created_at, updated_at, user_uuid, name, sort, hidden FROM %s WHERE uuid = ANY($1)`, h.f.table), pqArray(list))
	if err != nil {
		return err
	}
	byKey := map[string]*unitEntity{}
	for _, item := range current {
		byKey[item.UUID] = item
	}
	first := map[string]Change{}
	for _, change := range changes {
		if _, seen := first[change.Key]; !seen {
			first[change.Key] = change
		}
	}
	for key, change := range first {
		old := change.Old()
		now := byKey[key]
		if old != nil && (now == nil || old.Str("user_uuid") != now.UserUUID) {
			stub := &unitEntity{unitItem: &unitItem{Record: &redifu.Record{UUID: key}}}
			if err := h.f.list.Remove(batch.Ctx, batch.Pipe, stub, old.Str("user_uuid")); err != nil {
				return err
			}
		}
		if now == nil {
			stub := &unitEntity{unitItem: &unitItem{Record: &redifu.Record{UUID: key}}}
			if err := h.f.base.WithPipeline(batch.Pipe).Del(batch.Ctx, stub); err != nil {
				return err
			}
			continue
		}
		if err := h.f.base.WithPipeline(batch.Pipe).Set(batch.Ctx, now); err != nil {
			return err
		}
		if err := h.f.list.Add(batch.Ctx, batch.Pipe, now, now.UserUUID); err != nil {
			return err
		}
	}
	return nil
}

func pqArray(values []string) any { return "{" + strings.Join(values, ",") + "}" }

func newUnitFixture(t *testing.T) *unitFixture {
	t.Helper()
	dsn := os.Getenv("REDIFU_TEST_DSN")
	if dsn == "" {
		t.Skip("REDIFU_TEST_DSN not set")
	}
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	installSchema(t, db)
	table := fmt.Sprintf("zz_unit_%d", time.Now().UnixNano())
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (uuid varchar PRIMARY KEY, randid varchar DEFAULT 'r', created_at timestamptz DEFAULT now(), updated_at timestamp DEFAULT now(), user_uuid varchar NOT NULL, name text, sort bigint NOT NULL, hidden text DEFAULT 'h')`, table)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Exec(fmt.Sprintf(`DROP TABLE %s`, table))
		db.Exec(`DELETE FROM redifu_outbox WHERE table_name = $1`, table)
	})
	if _, err := db.Exec(`SELECT redifu_watch($1::regclass, 'uuid', 'user_uuid')`, table); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	f := &unitFixture{db: db, mr: mr, rdb: rdb, ctx: context.Background(), table: table}
	f.store = NewStore(db, rdb, true)
	f.base, err = NewBase[*unitEntity](f.store, "unit:%s", EntityTTL, Shape(unitEntity{}))
	if err != nil {
		t.Fatal(err)
	}
	f.list, err = NewList[*unitEntity](f.store, f.base, "unit:list:%s", redifu.Ascending, []string{table},
		func(ctx context.Context, tx *sqlx.Tx, params []string) ([]*unitEntity, error) {
			return loadUnitItems(ctx, tx, fmt.Sprintf(`SELECT uuid, randid, created_at, updated_at, user_uuid, name, sort, hidden FROM %s WHERE user_uuid = $1 ORDER BY sort, uuid COLLATE "C"`, table), params[0])
		},
		func(params []string) string { return params[0] })
	if err != nil {
		t.Fatal(err)
	}
	f.store.Register(unitHandler{f: f})
	return f
}

func (f *unitFixture) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := f.db.Exec(strings.ReplaceAll(query, "TBL", f.table), args...); err != nil {
		t.Fatal(err)
	}
}

func names(items []*unitEntity) string {
	var out []string
	for _, item := range items {
		out = append(out, item.Name.String)
	}
	return strings.Join(out, ",")
}

func (f *unitFixture) read(t *testing.T, owner string) string {
	t.Helper()
	items, ok := f.list.Read(f.ctx, owner)
	if !ok {
		t.Fatalf("read %s: not served from the cache", owner)
	}
	return names(items)
}

func TestStoreListFollowsEveryWrite(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2), ('c','o2','C',1)`)

	if got := f.read(t, "o1"); got != "A,B" {
		t.Fatalf("cold read = %q", got)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('d','o1','D',0)`)
	if got := f.read(t, "o1"); got != "D,A,B" {
		t.Fatalf("after insert = %q", got)
	}
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	if got := f.read(t, "o1"); got != "D,A2,B" {
		t.Fatalf("after rename = %q", got)
	}
	f.exec(t, `UPDATE TBL SET sort = 9 WHERE uuid = 'd'`)
	if got := f.read(t, "o1"); got != "A2,B,D" {
		t.Fatalf("after reorder = %q", got)
	}
	f.exec(t, `UPDATE TBL SET user_uuid = 'o2' WHERE uuid = 'b'`)
	if got := f.read(t, "o1"); got != "A2,D" {
		t.Fatalf("o1 after move = %q", got)
	}
	if got := f.read(t, "o2"); got != "C,B" {
		t.Fatalf("o2 after move = %q", got)
	}
	f.exec(t, `DELETE FROM TBL WHERE uuid = 'a'`)
	if got := f.read(t, "o1"); got != "D" {
		t.Fatalf("after delete = %q", got)
	}
	if exists, _ := f.base.Exists(f.ctx, "a"); exists {
		t.Fatal("deleted entity still in Base")
	}
	// Hidden (json:"-") survives the cache: the codec writes columns, not the API shape.
	items, _ := f.list.Read(f.ctx, "o1")
	if items[0].Hidden != "h" {
		t.Fatalf("json:\"-\" column lost: %q", items[0].Hidden)
	}

	// A flush in the middle: everything seeds again.
	f.mr.FlushAll()
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('e','o1','E',1)`)
	if got := f.read(t, "o1"); got != "E,D" {
		t.Fatalf("after flush = %q", got)
	}

	// An owner whose list is empty, then gets its first member.
	if got := f.read(t, "o3"); got != "" {
		t.Fatalf("empty = %q", got)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('f','o3','F',1)`)
	if got := f.read(t, "o3"); got != "F" {
		t.Fatalf("first member = %q", got)
	}
	var left int
	f.db.Get(&left, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table)
	if left != 0 {
		t.Fatalf("%d outbox rows left", left)
	}
}

func TestStoreReadFallsBackWhileTheOwnerIsLocked(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := f.read(t, "o1"); got != "A" {
		t.Fatalf("read = %q", got)
	}
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)

	// Another drain of o1 holds the lock: the read must not answer from Redis (it would be
	// stale), it says no and the caller reads SQL.
	tx := f.db.MustBegin()
	var locked bool
	tx.Get(&locked, `SELECT pg_try_advisory_xact_lock(hashtextextended('redifu:' || $1, 0))`, "o1")
	if !locked {
		t.Fatal("could not take the lock")
	}
	if _, ok := f.list.Read(f.ctx, "o1"); ok {
		t.Fatal("served while the owner's queue could not be applied")
	}
	tx.Rollback()
	f.store.drainFailed.Delete("o1")
	if got := f.read(t, "o1"); got != "A2" {
		t.Fatalf("after the lock = %q", got)
	}
}

func TestStoreReadsOffAndUnwatchedTablesGoToSQL(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	off := NewStore(f.db, f.rdb, false)
	list, _ := NewList[*unitEntity](off, f.base, "unit:list:%s", redifu.Ascending, []string{f.table}, f.list.load, f.list.owner)
	if _, ok := list.Read(f.ctx, "o1"); ok {
		t.Fatal("reads off but served from Redis")
	}
	f.exec(t, `DROP TRIGGER redifu_capture ON TBL`)
	f.store.checkedAt.Store(0) // re-read the watched tables now
	if _, ok := f.list.Read(f.ctx, "o1"); ok {
		t.Fatal("table not watched but served from Redis")
	}
}

func TestStoreRedisDownGoesToSQLAndRecovers(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := f.read(t, "o1"); got != "A" {
		t.Fatalf("read = %q", got)
	}
	f.mr.SetError("LOADING")
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	if _, ok := f.list.Read(f.ctx, "o1"); ok {
		t.Fatal("served with Redis failing")
	}
	f.mr.SetError("")
	f.store.redisDownUntil.Store(0)
	f.store.drainFailed.Delete("o1")
	if got := f.read(t, "o1"); got != "A2" {
		t.Fatalf("after recovery = %q (the queued change must still be applied)", got)
	}
}

func TestStoreConcurrentDrainsConverge(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	f.read(t, "o1")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			f.db.Exec(fmt.Sprintf(`UPDATE %s SET name = $1, sort = $2 WHERE uuid = 'a'`, f.table), fmt.Sprintf("A%d", i), i)
		}(i)
		go func() {
			defer wg.Done()
			f.store.DrainAll(f.ctx)
		}()
	}
	wg.Wait()
	f.store.drainFailed.Delete("o1")
	var want struct {
		Name string `db:"name"`
	}
	f.db.Get(&want, fmt.Sprintf(`SELECT name FROM %s WHERE uuid = 'a'`, f.table))
	if got := f.read(t, "o1"); got != want.Name {
		t.Fatalf("converged to %q, database has %q", got, want.Name)
	}
}

// Within one read scope an owner's queue is applied once: a write committed after the scope's
// first check is not applied by the same scope (its read is the snapshot of that check), and the
// next scope applies it.
func TestReadScopeAppliesTheQueueOnce(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	scope := NewReadScope(f.ctx)
	items, ok := f.list.Read(scope, "o1")
	if !ok || names(items) != "A" {
		t.Fatalf("first read in the scope: ok=%v %q", ok, names(items))
	}
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	if ok, drained := f.store.ServeOwner(scope, "o1", f.table); !ok || drained {
		t.Fatalf("second serve in the scope: ok=%v drained=%v, want served without a drain", ok, drained)
	}
	var queued int
	f.db.Get(&queued, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table)
	if queued != 1 {
		t.Fatalf("%d outbox rows, want the update still queued", queued)
	}
	if got := f.read(t, "o1"); got != "A2" {
		t.Fatalf("a new read = %q, want the update", got)
	}
	// A scope that never served an owner still checks it, and a failed check is not remembered.
	other := NewReadScope(f.ctx)
	tx := f.db.MustBegin()
	var locked bool
	tx.Get(&locked, `SELECT pg_try_advisory_xact_lock(hashtextextended('redifu:' || $1, 0))`, "o1")
	f.exec(t, `UPDATE TBL SET name = 'A3' WHERE uuid = 'a'`)
	if ok, _ := f.store.ServeOwner(other, "o1", f.table); ok {
		t.Fatal("served while the owner's lock was held")
	}
	tx.Rollback()
	f.store.drainFailed.Delete("o1")
	if items, ok := f.list.Read(other, "o1"); !ok || names(items) != "A3" {
		t.Fatalf("after the lock, in the same scope: ok=%v %q", ok, names(items))
	}
}

func TestStoreServeManyAppliesEveryQueueWithOneCheck(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o2','B',1)`)
	if got := f.read(t, "o1"); got != "A" {
		t.Fatalf("o1 = %q", got)
	}
	if got := f.read(t, "o2"); got != "B" {
		t.Fatalf("o2 = %q", got)
	}
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	served := f.store.ServeMany(f.ctx, []string{"o1", "o2", "o3"}, f.table)
	if !served["o1"] || !served["o2"] || !served["o3"] {
		t.Fatalf("served = %v", served)
	}
	var left int
	f.db.Get(&left, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table)
	if left != 0 {
		t.Fatalf("%d outbox rows left: o1's queue was not applied", left)
	}
	items, _, _ := f.list.Sorted().Fetch(redifu.Ascending).WithParams("o1").ExecReport(f.ctx)
	if names(items) != "A2" {
		t.Fatalf("o1 in Redis = %q", names(items))
	}

	// An owner whose queue cannot be applied now (its lock is held) is left out; the others are not.
	f.exec(t, `UPDATE TBL SET name = 'A3' WHERE uuid = 'a'`)
	tx := f.db.MustBegin()
	defer tx.Rollback()
	var locked bool
	tx.Get(&locked, `SELECT pg_try_advisory_xact_lock(hashtextextended('redifu:' || $1, 0))`, "o1")
	if !locked {
		t.Fatal("could not take the lock")
	}
	served = f.store.ServeMany(f.ctx, []string{"o1", "o2"}, f.table)
	if served["o1"] || !served["o2"] {
		t.Fatalf("with o1 locked: served = %v", served)
	}
	off := NewStore(f.db, f.rdb, false)
	if served := off.ServeMany(f.ctx, []string{"o2"}, f.table); len(served) != 0 {
		t.Fatalf("reads off: served = %v", served)
	}
}

func TestListCountFollowsMembershipWithoutReadingMembers(t *testing.T) {
	f := newUnitFixture(t)
	count := func(owner string) int64 {
		t.Helper()
		n, ok := f.list.Count(f.ctx, owner)
		if !ok {
			t.Fatalf("count %s: not served from the cache", owner)
		}
		return n
	}
	if n := count("o1"); n != 0 {
		t.Fatalf("empty owner = %d", n)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2)`)
	if n := count("o1"); n != 2 {
		t.Fatalf("after two inserts = %d", n)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('c','o1','C',3)`)
	f.exec(t, `UPDATE TBL SET user_uuid = 'o2' WHERE uuid = 'a'`)
	if n := count("o1"); n != 2 {
		t.Fatalf("after insert + move = %d", n)
	}
	if n := count("o2"); n != 1 {
		t.Fatalf("o2 after the move = %d", n)
	}
	f.exec(t, `DELETE FROM TBL WHERE user_uuid = 'o1'`)
	if n := count("o1"); n != 0 {
		t.Fatalf("after deleting every member = %d", n)
	}
	f.mr.FlushAll()
	if n := count("o2"); n != 1 {
		t.Fatalf("after a flush = %d", n)
	}
	served := f.store.ServeMany(f.ctx, []string{"o2"}, f.table)
	if n, ok := f.list.CountServed(f.ctx, "o2"); !served["o2"] || !ok || n != 1 {
		t.Fatalf("CountServed = %d %v", n, ok)
	}
}

func TestBatchLocalIsOneValuePerBatch(t *testing.T) {
	batch := &Batch{later: map[string]func() error{}}
	set := batch.Local("keys", func() any { return map[string]bool{} }).(map[string]bool)
	set["a"] = true
	again := batch.Local("keys", func() any { t.Fatal("init called twice"); return nil }).(map[string]bool)
	if !again["a"] {
		t.Fatal("a second Local did not return the first value")
	}
	other := &Batch{later: map[string]func() error{}}
	if fresh := other.Local("keys", func() any { return map[string]bool{} }).(map[string]bool); fresh["a"] {
		t.Fatal("two batches share a value")
	}
}
