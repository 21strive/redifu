package outbox

import (
	"strings"
	"testing"
	"time"
)

// leaseBuildOf is the build part of the lease's value ("<order>|<generation>"), without its epoch.
func leaseBuildOf(value string) string {
	if strings.Count(value, "|") < 2 {
		return value
	}
	return value[:strings.LastIndex(value, "|")]
}

// leaseEpochOf is the epoch part of the lease's value.
func leaseEpochOf(value string) string {
	if strings.Count(value, "|") < 2 {
		return ""
	}
	return value[strings.LastIndex(value, "|")+1:]
}

// The newest build loses the lease — its tasks all down for longer than the lease's TTL while the
// older build still runs, a crash-looping rollout — the older build takes over and drains by its
// own rules (it has no hot list), then the newest build comes back and takes the lease again. Its
// hot index still carries its marker from before; the downgrade's new epoch is what keeps it from
// being served.
func TestABuildThatRegainsTheLeaseStartsAFreshEpoch(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	old, next := newBuildStack(t, f, "build-old", 1, shape, false), newBuildStack(t, f, "build-new", 2, shape, true)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got, ok := next.readList(t, next.hot, "o1"); !ok || got != "A" {
		t.Fatalf("hot = %q, %v", got, ok)
	}
	first, _ := f.mr.Get(leaseBuildKey)
	if leaseBuildOf(first) != "2|build-new" || leaseEpochOf(first) == "" {
		t.Fatalf("lease = %q", first)
	}

	f.mr.FastForward(leaseTTL + time.Second)
	if got := old.read(t, "o1"); got != "A" {
		t.Fatalf("old = %q", got)
	}
	downgrade, _ := f.mr.Get(leaseBuildKey)
	if leaseBuildOf(downgrade) != "1|build-old" || leaseEpochOf(downgrade) == leaseEpochOf(first) {
		t.Fatalf("lease after the downgrade = %q: an older build taking over must start a new epoch", downgrade)
	}
	if old.store.Epoch() != leaseEpochOf(downgrade) {
		t.Fatalf("the old build's caches carry epoch %q, the lease %q", old.store.Epoch(), downgrade)
	}

	// A write while the old build holds the lease: its drain knows no hot list.
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('b','o1','B',2)`)
	if done, err := old.store.DrainOwner(f.ctx, "o1"); err != nil || !done {
		t.Fatalf("old drain: %v, %v", done, err)
	}

	got, ok := next.readList(t, next.hot, "o1")
	upgrade, _ := f.mr.Get(leaseBuildKey)
	if leaseBuildOf(upgrade) != "2|build-new" || leaseEpochOf(upgrade) != leaseEpochOf(downgrade) {
		t.Fatalf("lease after the new build came back = %q: an upgrade keeps the epoch", upgrade)
	}
	if !ok || got != "A,B" {
		t.Fatalf("the new build came back to its own hot index: %q (served %v), SQL says A,B", got, ok)
	}
	if got := next.read(t, "o1"); got != "A,B" {
		t.Fatalf("new list = %q", got)
	}
}

// A deploy (an upgrade) keeps the epoch, so the entities the two builds share stay warm; so does a
// build whose processes were all gone for a while with nobody taking its place.
func TestAnUpgradeAndAResumeKeepTheEpoch(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	old, next := newBuildStack(t, f, "build-old", 1, shape, false), newBuildStack(t, f, "build-new", 2, shape, false)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := old.read(t, "o1"); got != "A" {
		t.Fatalf("old = %q", got)
	}
	epoch := old.store.Epoch()
	if epoch == "" {
		t.Fatal("the first holder carries no epoch")
	}
	loads := next.loads.Load()
	if found, ok := next.detail.Get(f.ctx, "a"); !ok || found["a"] == nil || next.loads.Load() != loads {
		t.Fatalf("the upgrade did not find the old build's entity warm: %v, %v", found, ok)
	}
	if next.store.Epoch() != epoch {
		t.Fatalf("epoch after the upgrade = %q, want %q", next.store.Epoch(), epoch)
	}

	f.mr.FastForward(leaseTTL + time.Second) // every process of the new build gone; nobody else ran
	if got := next.read(t, "o1"); got != "A" {
		t.Fatalf("new after the pause = %q", got)
	}
	if value, _ := f.mr.Get(leaseBuildKey); leaseEpochOf(value) != epoch || next.store.Epoch() != epoch {
		t.Fatalf("lease after the resume = %q, caches %q: the epoch must stay", value, next.store.Epoch())
	}

	// A flushed Redis has no holder: whoever takes it next starts a new epoch.
	f.mr.FlushAll()
	if got := next.read(t, "o1"); got != "A" {
		t.Fatalf("new after the flush = %q", got)
	}
	if next.store.Epoch() == epoch {
		t.Fatal("a takeover of a flushed lease kept the old epoch")
	}
}

// A drain's handlers run their SQL with no Redis connection taken from the pool: the batch's writes
// are recorded and sent in one fenced round trip after them.
func TestADrainHoldsNoRedisConnectionWhileItsHandlersRun(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	stack := newBuildStack(t, f, "build-1", 1, shape, false)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := stack.read(t, "o1"); got != "A" {
		t.Fatalf("read = %q", got)
	}
	var inUse []uint32
	stack.store.Register(handlerFunc{table: f.table, apply: func(*Batch, string, []Change) error {
		stats := f.rdb.PoolStats()
		inUse = append(inUse, stats.TotalConns-stats.IdleConns)
		return nil
	}})
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	if done, err := stack.store.DrainOwner(f.ctx, "o1"); err != nil || !done {
		t.Fatalf("drain = %v, %v", done, err)
	}
	if len(inUse) != 1 || inUse[0] != 0 {
		t.Fatalf("Redis connections in use while the handlers ran: %v, want none", inUse)
	}
	if got := stack.read(t, "o1"); got != "A2" {
		t.Fatalf("read after the drain = %q", got)
	}
}

// Redis failing at the batch's commit (the WATCH itself included) is a Redis failure: the reads
// pause, the changes stay queued.
func TestADrainWhoseCommitFailsPausesTheReads(t *testing.T) {
	everyCheck(t)
	f := newUnitFixture(t)
	shape := Shape(unitItem{}, "unit columns")
	stack := newBuildStack(t, f, "build-1", 1, shape, false)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1)`)
	if got := stack.read(t, "o1"); got != "A" {
		t.Fatalf("read = %q", got)
	}
	stack.store.Register(handlerFunc{table: f.table, apply: func(*Batch, string, []Change) error {
		f.mr.Close() // Redis goes away between the handlers and the commit
		return nil
	}})
	f.exec(t, `UPDATE TBL SET name = 'A2' WHERE uuid = 'a'`)
	if done, err := stack.store.DrainOwner(f.ctx, "o1"); err == nil || done {
		t.Fatalf("drain with Redis gone = %v, %v", done, err)
	}
	if !stack.store.redisDown() {
		t.Fatal("a failed commit did not pause the reads")
	}
	var queued int
	f.db.Get(&queued, `SELECT count(*) FROM redifu_outbox WHERE table_name = $1`, f.table)
	if queued != 1 {
		t.Fatalf("%d rows queued after the failed commit, want 1", queued)
	}
}
