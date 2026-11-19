package outbox

// The parts of a drain the other tests leave alone: a view across owners (SeedView and
// Batch.LockGlobal), the steps of a batch (Local, Later, AfterExec), the background drain loop and
// the backlog. Same fixture as store_test.go (REDIFU_TEST_DSN; skips without it).

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

const unitColumns = `uuid, randid, created_at, updated_at, user_uuid, name, sort, hidden`

// everyRowView is one Sorted index of every owner's rows of the fixture's table: a view across
// owners, seeded with SeedView and kept by a handler that takes the view's lock.
type everyRowView struct {
	f      *unitFixture
	sorted *redifu.Sorted[*unitEntity]
}

const everyRowLock = "unit:every"

func newEveryRowView(t *testing.T, f *unitFixture) *everyRowView {
	t.Helper()
	sorted, err := NewSorted[*unitEntity](f.store, f.base, "unit:every", ListTTL)
	if err != nil {
		t.Fatal(err)
	}
	if err := sorted.SetSortingReference("Score"); err != nil {
		t.Fatal(err)
	}
	view := &everyRowView{f: f, sorted: sorted}
	f.store.Register(view)
	return view
}

func (v *everyRowView) Tables() []string { return []string{v.f.table} }

func (v *everyRowView) Apply(batch *Batch, table string, changes []Change) error {
	if err := batch.LockGlobal(everyRowLock); err != nil {
		return err
	}
	keys, _ := ChangeKeys(changes)
	rows, err := loadUnitItems(batch.Ctx, batch.Tx, `SELECT `+unitColumns+` FROM `+table+` WHERE uuid = ANY($1)`, textArray(keys))
	if err != nil {
		return err
	}
	current := map[string]*unitEntity{}
	for _, row := range rows {
		current[row.UUID] = row
	}
	for _, key := range keys {
		if row, ok := current[key]; ok {
			if err := v.f.base.WithPipeline(batch.Pipe).Set(batch.Ctx, row); err != nil {
				return err
			}
			if err := v.sorted.WithPipeline(batch.Pipe).AddItem(batch.Ctx, row); err != nil {
				return err
			}
			continue
		}
		stub := &unitEntity{unitItem: &unitItem{Record: &redifu.Record{UUID: key}}}
		if err := v.sorted.WithPipeline(batch.Pipe).RemoveItem(batch.Ctx, stub); err != nil {
			return err
		}
	}
	return nil
}

func (v *everyRowView) seed(ctx context.Context) (bool, error) {
	return v.f.store.SeedView(ctx, everyRowLock, []string{v.f.table}, func(tx *sqlx.Tx, pipe redis.Pipeliner) error {
		rows, err := loadUnitItems(ctx, tx, `SELECT `+unitColumns+` FROM `+v.f.table+` ORDER BY sort, uuid COLLATE "C"`)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := v.f.base.WithPipeline(pipe).Set(ctx, row); err != nil {
				return err
			}
			if err := v.sorted.IngestItem(ctx, pipe, row, true); err != nil {
				return err
			}
		}
		if len(rows) == 0 {
			if err := v.sorted.MarkEmpty(ctx, pipe); err != nil {
				return err
			}
		} else if err := v.sorted.HasData(ctx, pipe); err != nil {
			return err
		}
		return v.sorted.SetExpiration(ctx, pipe)
	})
}

func (v *everyRowView) read(t *testing.T) string {
	t.Helper()
	items, dangling, err := v.sorted.Fetch(redifu.Ascending).ExecReport(v.f.ctx)
	if err != nil || dangling != 0 {
		t.Fatalf("read the view: %v, %d dangling", err, dangling)
	}
	return names(items)
}

// A view across owners is seeded only when no change of its tables is queued, whoever's, and never
// while a drain of it holds its lock; a drain that changes it waits for that lock.
func TestAViewAcrossOwnersSeedsAndDrainsUnderItsOwnLock(t *testing.T) {
	f := newUnitFixture(t)
	view := newEveryRowView(t, f)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o2','B',2)`)

	if seeded, err := view.seed(f.ctx); seeded || err != nil {
		t.Fatalf("seed with two owners' changes queued: %v, %v", seeded, err)
	}
	if !f.store.Serve(f.ctx, "*", f.table) {
		t.Fatal("the queues of the view's table were not applied")
	}
	if seeded, err := view.seed(f.ctx); !seeded || err != nil {
		t.Fatalf("seed with nothing queued: %v, %v", seeded, err)
	}
	if got := view.read(t); got != "A,B" {
		t.Fatalf("view = %q", got)
	}

	// While someone else holds the view's lock — a drain of it, here a transaction of the test —
	// a seed gives up at once, and a drain that changes the view waits, then applies the change.
	tx := f.db.MustBegin()
	var locked bool
	if err := tx.Get(&locked, `SELECT pg_try_advisory_xact_lock(hashtextextended('redifu:' || $1, 0))`, everyRowLock); err != nil || !locked {
		t.Fatalf("could not take the view's lock: %v", err)
	}
	if seeded, err := view.seed(f.ctx); seeded || err != nil {
		t.Fatalf("seed under a held view lock, nothing queued: %v, %v", seeded, err)
	}
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('c','o3','C',3)`)
	released := make(chan struct{})
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = tx.Rollback()
		close(released)
	}()
	start := time.Now()
	if !f.store.Serve(f.ctx, "*", f.table) {
		t.Fatal("the drain did not wait for the view's lock")
	}
	<-released
	if waited := time.Since(start); waited < 150*time.Millisecond {
		t.Fatalf("the drain applied the change after %s, while the view's lock was held", waited)
	}
	if got := view.read(t); got != "A,B,C" {
		t.Fatalf("view after the drain = %q", got)
	}
	f.exec(t, `DELETE FROM TBL WHERE uuid = 'a'`)
	if !f.store.Serve(f.ctx, "*", f.table) {
		t.Fatal("not served")
	}
	if got := view.read(t); got != "B,C" {
		t.Fatalf("view after a delete = %q", got)
	}
}

// satelliteHandler keeps one computed record per owner — how many rows it has — with a Later step
// that runs once per batch however many of its rows changed, and checks it after the batch's
// writes reached Redis (AfterExec).
type satelliteHandler struct {
	f          *unitFixture
	laterRuns  atomic.Int64
	afterSeen  atomic.Value // string
	chainedRan atomic.Bool
}

func (h *satelliteHandler) Tables() []string { return []string{h.f.table} }

func (h *satelliteHandler) Apply(batch *Batch, table string, changes []Change) error {
	keys := batch.Local("keys", func() any { return map[string]bool{} }).(map[string]bool)
	for _, change := range changes {
		keys[change.Key] = true
	}
	key := fmt.Sprintf("{unit:count:%s}", batch.Owner)
	batch.Later("count:"+batch.Owner, func() error {
		h.laterRuns.Add(1)
		var n int64
		if err := batch.Tx.GetContext(batch.Ctx, &n, `SELECT count(*) FROM `+table+` WHERE user_uuid = $1`, batch.Owner); err != nil {
			return err
		}
		batch.Pipe.Set(batch.Ctx, key, fmt.Sprintf("%d rows, %d changed", n, len(keys)), time.Hour)
		batch.Later("chained", func() error { h.chainedRan.Store(true); return nil }) // a Later step may queue another
		return nil
	})
	batch.AfterExec(func(ctx context.Context) error {
		value, err := h.f.rdb.Get(ctx, key).Result()
		h.afterSeen.Store(value)
		return err
	})
	return nil
}

func TestBatchStepsRunOnceAfterEveryTableAndAfterTheWrites(t *testing.T) {
	f := newUnitFixture(t)
	satellite := &satelliteHandler{f: f}
	f.store.Register(satellite)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2), ('c','o1','C',3)`)
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	if done, err := f.store.DrainOwner(f.ctx, "o1"); !done || err != nil {
		t.Fatalf("drain: %v, %v", done, err)
	}
	if n := satellite.laterRuns.Load(); n != 1 {
		t.Fatalf("the Later step ran %d times for one batch", n)
	}
	if !satellite.chainedRan.Load() {
		t.Fatal("a Later step queued by a Later step did not run")
	}
	if got := satellite.afterSeen.Load(); got != "3 rows, 3 changed" {
		t.Fatalf("AfterExec saw %v: it must run once the batch's writes are in Redis", got)
	}
}

// The background drain applies the queue without any read asking for it, and a pass whose handler
// panics leaves the changes queued for the next pass.
func TestTheDrainLoopAppliesTheQueueAndOutlivesAPanickingHandler(t *testing.T) {
	f := newUnitFixture(t)
	logged := &capturedLog{}
	f.store.WithLogger(slog.New(logged))
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := f.read(t, "o1"); got != "A" {
		t.Fatalf("read = %q", got)
	}
	var panicked atomic.Bool
	f.store.Register(handlerFunc{table: f.table, apply: func(*Batch, string, []Change) error {
		if panicked.CompareAndSwap(false, true) {
			panic("a handler bug")
		}
		return nil
	}})
	before, _, err := f.store.Backlog(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('b','o2','B',1)`)
	rows, age, err := f.store.Backlog(f.ctx)
	if err != nil || rows != before+2 || (before == 0 && (age <= 0 || age > time.Minute)) {
		t.Fatalf("backlog: %d rows (%d before), oldest %s, %v", rows, before, age, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.store.RunDrainLoop(ctx, 10*time.Millisecond); close(done) }()
	defer func() { cancel(); <-done }()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		var queued int
		if err := f.db.Get(&queued, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table); err != nil {
			t.Fatal(err)
		}
		if queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d changes still queued; logged %q", queued, logged.messages())
		}
	}
	if !panicked.Load() || logged.attrs("background pass panicked") == nil {
		t.Fatalf("the panicking pass was not logged: %q", logged.messages())
	}
	if !strings.Contains(logged.attrs("background pass panicked")["error"], "a handler bug") {
		t.Fatalf("logged %v", logged.attrs("background pass panicked"))
	}
	entity, err := f.base.Get(f.ctx, "a")
	if err != nil || entity.Name.String != "A2" {
		t.Fatalf("Base after the background drain: %+v, %v", entity, err)
	}
	if exists, _ := f.base.Exists(f.ctx, "b"); !exists {
		t.Fatal("the background drain did not apply o2's insert")
	}
}
