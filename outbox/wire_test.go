package outbox

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// TestWireContract pins what every build sharing one database and one Redis must agree on — a
// build on this package and a build on the code it came from, side by side during a rolling
// deploy: the SQL of the advisory locks and the limits it sets, the isolation of seeds and drains,
// the lease's keys, value and TTL, the package's numbers, and the error texts a log filter may
// match. Every expected value is a literal on purpose: changing one changes the contract, and
// needs every build at once.
func TestWireContract(t *testing.T) {
	ctx := context.Background()

	t.Run("owner lock", func(t *testing.T) {
		const lock = `SELECT pg_try_advisory_xact_lock(hashtextextended('redifu:' || $1, 0))
  FROM (SELECT set_config('statement_timeout', '15000', true),
               set_config('idle_in_transaction_session_timeout', '30000', true)) AS limits`
		if redifuLockSQL != lock {
			t.Fatalf("the owner lock's SQL changed:\n%s", redifuLockSQL)
		}
		// A seed, a view's seed and a drain each open a REPEATABLE READ transaction and send that
		// lock first; the recording database answers that the lock is taken, so each stops there.
		db, sent := recordingDB(t)
		store := NewStore(db, nil, true)
		noop := func(*sqlx.Tx, redis.Pipeliner) error { return nil }
		if seeded, err := store.Seed(ctx, "o1", noop); seeded || err != nil {
			t.Fatalf("seed under a taken lock: %v, %v", seeded, err)
		}
		if seeded, err := store.SeedView(ctx, "view", []string{"t"}, noop); seeded || err != nil {
			t.Fatalf("view seed under a taken lock: %v, %v", seeded, err)
		}
		if done, err := store.DrainOwner(ctx, "o1"); done || err != nil {
			t.Fatalf("drain under a taken lock: %v, %v", done, err)
		}
		want := []string{
			"BEGIN Repeatable Read", "QUERY " + lock + " [o1]", "ROLLBACK",
			"BEGIN Repeatable Read", "QUERY " + lock + " [view]", "ROLLBACK",
			"BEGIN Repeatable Read", "QUERY " + lock + " [o1]", "ROLLBACK",
		}
		assertSent(t, *sent, want)
	})

	t.Run("view lock", func(t *testing.T) {
		db, sent := recordingDB(t)
		tx, err := db.Beginx()
		if err != nil {
			t.Fatal(err)
		}
		batch := &Batch{Ctx: ctx, Tx: tx}
		for i := 0; i < 2; i++ { // the second call takes nothing more
			if err := batch.LockGlobal("view"); err != nil {
				t.Fatal(err)
			}
		}
		_ = tx.Rollback()
		assertSent(t, *sent, []string{
			"BEGIN Default",
			"EXEC SET LOCAL lock_timeout = '5s' []",
			"EXEC SELECT pg_advisory_xact_lock(hashtextextended('redifu:' || $1, 0)) [view]",
			"ROLLBACK",
		})
	})

	t.Run("lease", func(t *testing.T) {
		if leaseBuildKey != "{redifu:lease}:build" || leaseAliveKey != "{redifu:lease}:alive" {
			t.Fatalf("lease keys %q, %q", leaseBuildKey, leaseAliveKey)
		}
		if leaseTTL != 30*time.Second || leaseCheck != 250*time.Millisecond {
			t.Fatalf("lease TTL %v, check %v", leaseTTL, leaseCheck)
		}
		if got := withEpoch("4b5e8d8", "ab12"); got != "4b5e8d8@ab12" || withEpoch("4b5e8d8", "") != "4b5e8d8" {
			t.Fatalf("a cache's generation with the lease's epoch = %q", got)
		}
		mr := miniredis.RunT(t)
		store := NewStore(nil, redis.NewClient(&redis.Options{Addr: mr.Addr()}), true).WithBuild("4b5e8d8", 1763500000)
		if !store.holdsLease(ctx) {
			t.Fatal("a first build did not take a free lease")
		}
		holder, _ := mr.Get("{redifu:lease}:build")
		if !regexp.MustCompile(`^1763500000\|4b5e8d8\|[0-9a-z]+$`).MatchString(holder) {
			t.Fatalf("{redifu:lease}:build = %q, want <build time>|<generation>|<epoch>", holder)
		}
		if alive, _ := mr.Get("{redifu:lease}:alive"); alive != "1763500000|4b5e8d8" || mr.TTL("{redifu:lease}:alive") != 30*time.Second {
			t.Fatalf("{redifu:lease}:alive = %q, TTL %v", alive, mr.TTL("{redifu:lease}:alive"))
		}
		if mr.TTL("{redifu:lease}:build") != 0 {
			t.Fatal("{redifu:lease}:build has a TTL")
		}
	})

	t.Run("numbers", func(t *testing.T) {
		for name, value := range map[string]struct{ got, want any }{
			"rows per drain batch":       {redifuDrainBatch, 500},
			"owners per drain round":     {redifuDrainOwners, 200},
			"drain backoff":              {redifuDrainRetry, 5 * time.Second},
			"Redis pause":                {redifuRedisPause, 5 * time.Second},
			"connection wait":            {redifuConnWait, time.Second},
			"trigger recheck":            {redifuWatchRecheck, time.Minute},
			"backlog check":              {redifuBacklogCheck, time.Minute},
			"backlog age that is logged": {redifuBacklogAge, time.Minute},
			"list TTL":                   {ListTTL, time.Hour},
		} {
			if value.got != value.want {
				t.Errorf("%s = %v, want %v", name, value.got, value.want)
			}
		}
		if redifuSnapshot.Isolation != sql.LevelRepeatableRead || redifuSnapshot.ReadOnly {
			t.Errorf("seeds and drains run %v", redifuSnapshot)
		}
	})

	t.Run("error texts", func(t *testing.T) {
		for err, want := range map[error]string{
			errPoolBusy:      "redifucache: database pool busy",
			ErrNotCached:     "redifucache: not served from the cache",
			ErrBadCursor:     "redifucache: malformed cursor",
			errLeaseLost:     "redifucache: another build holds the cache lease",
			errLeaseMissing:  "redifucache: the cache lease is gone",
			errRecorderSends: "redifucache: a recording pipeline sends nothing",
		} {
			if err.Error() != want {
				t.Errorf("%q, want %q", err.Error(), want)
			}
		}
	})
}

func assertSent(t *testing.T, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("sent to the database:\n got %q\nwant %q", got, want)
	}
}

// recordingDB is a database that records every statement it is sent, with its arguments, and
// answers every query with one row holding false (a lock that is taken).
func recordingDB(t *testing.T) (*sqlx.DB, *[]string) {
	t.Helper()
	sent := &[]string{}
	db := sqlx.NewDb(sql.OpenDB(recordingConnector{sent: sent}), "postgres")
	t.Cleanup(func() { _ = db.Close() })
	return db, sent
}

type recordingConnector struct{ sent *[]string }

func (c recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return recordingConn(c), nil
}
func (c recordingConnector) Driver() driver.Driver { return recordingDriver{} }

type recordingDriver struct{}

func (recordingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("the recording database opens through its connector")
}

type recordingConn struct{ sent *[]string }

func (c recordingConn) record(kind, query string, args []driver.NamedValue) {
	values := make([]any, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	*c.sent = append(*c.sent, fmt.Sprintf("%s %s %v", kind, query, values))
}

func (c recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("the recording database prepares nothing")
}
func (c recordingConn) Close() error { return nil }
func (c recordingConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c recordingConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	*c.sent = append(*c.sent, "BEGIN "+sql.IsolationLevel(opts.Isolation).String())
	return recordingTx(c), nil
}

func (c recordingConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.record("EXEC", query, args)
	return driver.RowsAffected(0), nil
}

func (c recordingConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.record("QUERY", query, args)
	return &takenLock{}, nil
}

type recordingTx struct{ sent *[]string }

func (tx recordingTx) Commit() error   { *tx.sent = append(*tx.sent, "COMMIT"); return nil }
func (tx recordingTx) Rollback() error { *tx.sent = append(*tx.sent, "ROLLBACK"); return nil }

// takenLock is one row with one column, false.
type takenLock struct{ done bool }

func (r *takenLock) Columns() []string { return []string{"locked"} }
func (r *takenLock) Close() error      { return nil }
func (r *takenLock) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = false
	return nil
}
