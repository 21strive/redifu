package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// Store is the machinery every redifu-cached repository shares:
//
//   - The database queues every write to a cached table in redifu_outbox (row triggers: Schema,
//     and redifu_watch per table), in the transaction of the write. Nothing in the write paths
//     knows the cache exists.
//   - Drain applies the queue of one owner to Redis under that owner's advisory lock, through
//     the handler registered for each table: reload the rows that changed, Base.Set or Del
//     them, move them between indexes, recompute satellites.
//   - Serve is asked before a read is answered from Redis: it applies the owner's queue first
//     (or says no, and the read goes to SQL), so a read that starts after a write committed
//     sees it, whichever process wrote it.
//   - Seed fills a cold index under the same lock, so a seed and a drain of one owner never
//     interleave.
//
// Redis is never the source of truth: an error anywhere sends the read to SQL, and a flushed
// Redis seeds itself again.
type Store struct {
	rdb   redis.UniversalClient
	db    *sqlx.DB
	reads bool

	handlers map[string][]Handler

	ready     atomic.Bool // redifu_outbox exists
	watchedMu sync.RWMutex
	watched   map[string]bool // tables with the capture trigger
	checkedAt atomic.Int64

	redisDownUntil atomic.Int64
	drainFailed    sync.Map // owner → time.Time until which reads of the owner skip the cache
	poolBusyAt     atomic.Int64

	// build is the generation of the indexes and computed records (generation.go); formats are
	// the key formats the caches registered (NewBase, NewSorted, NewTimeline), and caches every
	// Base and index they built, so that a new epoch reaches them all; epoch is the lease's epoch
	// the caches carry; lease is this build's hold on the cache (lease.go).
	build     string
	formatsMu sync.Mutex
	formats   map[string]KeyFormat
	caches    []generationHolder
	epoch     atomic.Pointer[string]
	lease     *buildLease

	// order is the build's order in the lease, as WithBuild gave it (negative: none); buildOnce
	// settles build, order and lease, with their fallbacks, the first time one is needed
	// (settleBuild), and settled says it has.
	order     int64
	buildOnce sync.Once
	settled   bool

	// The application's parts: the logger (WithLogger), what runs each pass of the drain loop
	// (WithRecover), and the key formats from before generations that Purge also deletes
	// (WithLegacyFormats).
	logger        *slog.Logger
	recoverPass   func(loop string, pass func()) bool
	legacyVersion string
	legacyRaw     []string
}

// redifuDrainBatch bounds the rows one drain transaction takes; a longer queue takes several.
const redifuDrainBatch = 500

// redifuDrainOwners bounds the owners one round of the background drain takes (DrainAll).
const redifuDrainOwners = 200

// redifuBacklogCheck is how often the drain loop reads the queue's backlog, and redifuBacklogAge
// the age of its oldest row past which the loop logs it.
const (
	redifuBacklogCheck = time.Minute
	redifuBacklogAge   = time.Minute
)

// redifuRedisPause is how long reads skip Redis after it failed, so an outage costs one slow
// read per process every few seconds instead of a timeout on every request.
const redifuRedisPause = 5 * time.Second

// redifuWatchRecheck is how often the store re-reads which tables carry the capture trigger: a
// migration applied while the process runs turns the cache of those tables on (or a dropped
// trigger off) without a restart.
const redifuWatchRecheck = time.Minute

// redifuConnWait bounds how long the cache waits for a connection of the primary's pool. A request
// that holds a write transaction and then asks the cache (a write path that reads a cached view
// inside its transaction) needs a second connection of the same pool; when as many requests as the
// pool has connections do that at once, a wait without a bound hangs them all. The cache gives up
// instead and the read goes to SQL (the read pool), so the transactions finish.
const redifuConnWait = time.Second

// errPoolBusy: no connection of the primary's pool within redifuConnWait.
var errPoolBusy = errors.New("redifucache: database pool busy")

// redifuSnapshot is the isolation of seeds and drain batches: REPEATABLE READ, so every statement of
// one reads the snapshot its lock statement took. Redis is then written as the database stood at
// that instant, and every write committed after it is still queued, where the next drain finds it
// with an old row equal to what Redis holds — the handlers move entities between indexes from that
// old row. With READ COMMITTED a write that landed between two statements was read but stayed
// queued, and a later revert of it looked like no change (the entity stayed in the wrong index).
var redifuSnapshot = &sql.TxOptions{Isolation: sql.LevelRepeatableRead}

// isSerializationFailure reports a REPEATABLE READ conflict (SQLSTATE 40001). The driver's error
// reports its SQLSTATE through SQLState() — lib/pq's *pq.Error and pgx's *pgconn.PgError both do —
// so the package imports no driver.
func isSerializationFailure(err error) bool {
	var coded interface{ SQLState() string }
	return errors.As(err, &coded) && coded.SQLState() == "40001"
}

// NewStore builds the store. db is the primary: the queue and the seeds must see a write
// as soon as it commits. reads false answers every read from SQL while the queue is still
// applied, so Redis stays current and reads can be turned back on at any time.
//
// The application's parts follow as chained calls, all before the first cache is built on the
// store: WithBuild, WithLogger, WithRecover and WithLegacyFormats.
func NewStore(db *sqlx.DB, rdb redis.UniversalClient, reads bool) *Store {
	return &Store{
		rdb:      rdb,
		db:       db,
		reads:    reads,
		handlers: map[string][]Handler{},
		watched:  map[string]bool{},
		order:    -1,
		formats:  map[string]KeyFormat{},
	}
}

// WithLogger sets the logger the store writes to. Without one it logs to slog.Default(), as that
// stands when a line is written.
func (s *Store) WithLogger(logger *slog.Logger) *Store {
	s.logger = logger
	return s
}

// WithRecover sets what runs each pass of the drain loop (RunDrainLoop), given the loop's name
// ("redifu_drain"): it must keep a panic in the pass from ending the process, and report whether
// the pass returned. Without one a panic is recovered and logged at ERROR with its stack.
func (s *Store) WithRecover(recoverPass func(loop string, pass func()) bool) *Store {
	s.recoverPass = recoverPass
	return s
}

// log is the store's logger (WithLogger), slog.Default() without one.
func (s *Store) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

// runPass runs one pass of a background loop through WithRecover's function, or recoverAndLog.
func (s *Store) runPass(loop string, pass func()) bool {
	if s.recoverPass != nil {
		return s.recoverPass(loop, pass)
	}
	return recoverAndLog(s.log(), loop, pass)
}

// recoverAndLog runs pass, one pass of a background loop, and keeps a panic inside it from ending
// the process: the panic is logged at ERROR with its stack, under an error text that names the loop
// and the panic, and the caller's loop goes on with its next pass. It reports whether pass returned.
func recoverAndLog(logger *slog.Logger, loop string, pass func()) (finished bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Error("background pass panicked",
				"error", fmt.Sprintf("%s: pass panicked: %v", loop, recovered),
				"loop", loop,
				"stack", string(debug.Stack()),
			)
		}
	}()
	pass()
	return true
}

// Client is the Redis client of the cache (short timeouts).
func (s *Store) Client() redis.UniversalClient { return s.rdb }

// DB is the primary the cache loads from.
func (s *Store) DB() *sqlx.DB { return s.db }

// conn takes one connection of the primary's pool for the cache's own work: at once when one is
// free, never after a wait longer than redifuConnWait, and not at all while the pool is visibly
// exhausted (errPoolBusy; the caller then reads SQL). The caller closes it.
func (s *Store) conn(ctx context.Context) (*sqlx.Conn, error) {
	if stats := s.db.Stats(); stats.MaxOpenConnections > 0 && stats.InUse >= stats.MaxOpenConnections {
		s.poolBusy()
		return nil, errPoolBusy
	}
	wait, cancel := context.WithTimeout(ctx, redifuConnWait)
	defer cancel()
	conn, err := s.db.Connx(wait)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			s.poolBusy()
			return nil, errPoolBusy
		}
		return nil, err
	}
	return conn, nil
}

// poolBusy logs, at most once a minute, that reads went to SQL for want of a connection.
func (s *Store) poolBusy() {
	now := time.Now().UnixNano()
	last := s.poolBusyAt.Load()
	if now-last < int64(time.Minute) || !s.poolBusyAt.CompareAndSwap(last, now) {
		return
	}
	s.log().Warn("redifu: the database pool is busy, cached reads go to SQL", "wait", redifuConnWait.String())
}

// Query runs fn on one connection of the primary's pool, taken as conn takes it. A cached reader
// that loads outside an owner's lock (to learn whose a key is) goes through it.
func (s *Store) Query(ctx context.Context, fn func(q sqlx.QueryerContext) error) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return fn(conn)
}

// IsPoolBusy reports whether err is the cache giving up on a connection (see conn): not a failure
// worth logging, the read just goes to SQL.
func IsPoolBusy(err error) bool { return errors.Is(err, errPoolBusy) }

// Handler brings Redis in line with the database for the changes queued on its tables.
type Handler interface {
	// Tables are the tables whose changes the handler applies.
	Tables() []string
	// Apply works through the changes of one table, oldest first. It reloads what it needs
	// through batch.Tx (the drain transaction, which holds the owner's lock) and enqueues the
	// Redis writes into batch.Pipe; work that depends on several tables (a satellite) goes
	// through batch.Later.
	Apply(batch *Batch, table string, changes []Change) error
}

// Register adds a handler. Called while the repositories are built, before any traffic. Several
// handlers may follow one table (an owner's own views and a list across owners may both follow
// one); each is called with the table's changes.
func (s *Store) Register(handler Handler) {
	for _, table := range handler.Tables() {
		s.handlers[table] = append(s.handlers[table], handler)
	}
}

// Change is one queued change, a row of redifu_outbox.
type Change struct {
	ID     int64   `db:"id"`
	Owner  string  `db:"owner_uuid"`
	Table  string  `db:"table_name"`
	Key    string  `db:"row_key"`
	Op     string  `db:"op"`
	OldRow *string `db:"old_row"`
}

// Old is the row before the change (nil for an INSERT), as to_jsonb rendered it.
func (c Change) Old() Row {
	if c.OldRow == nil {
		return nil
	}
	var row Row
	if err := json.Unmarshal([]byte(*c.OldRow), &row); err != nil {
		return nil
	}
	return row
}

// Row is a row as to_jsonb renders it.
type Row map[string]json.RawMessage

func (r Row) raw(column string) (json.RawMessage, bool) {
	if r == nil {
		return nil, false
	}
	value, ok := r[column]
	if !ok || string(value) == "null" {
		return nil, false
	}
	return value, true
}

// Str is a text column ("" for NULL or absent).
func (r Row) Str(column string) string {
	value, ok := r.raw(column)
	if !ok {
		return ""
	}
	var text string
	if err := json.Unmarshal(value, &text); err == nil {
		return text
	}
	return strings.Trim(string(value), `"`)
}

// Int is an integer column (0 for NULL or absent).
func (r Row) Int(column string) int64 {
	value, ok := r.raw(column)
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(strings.Trim(string(value), `"`), 10, 64)
	if err != nil {
		f, errFloat := strconv.ParseFloat(strings.Trim(string(value), `"`), 64)
		if errFloat != nil {
			return 0
		}
		return int64(f)
	}
	return n
}

// Bool is a boolean column (false for NULL or absent).
func (r Row) Bool(column string) bool {
	value, ok := r.raw(column)
	return ok && string(value) == "true"
}

// Null reports whether the column is NULL or absent.
func (r Row) Null(column string) bool {
	_, ok := r.raw(column)
	return !ok
}

// Time is a timestamp or timestamptz column (zero for NULL). to_jsonb writes a timestamptz with
// its offset and a timestamp without one, which is read as UTC.
func (r Row) Time(column string) time.Time {
	text := r.Str(column)
	if text == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

// Batch is one drain transaction of one owner.
type Batch struct {
	Ctx   context.Context
	Tx    *sqlx.Tx
	Owner string
	Pipe  redis.Pipeliner
	store *Store

	later      map[string]func() error
	laterOrder []string
	afterExec  []func(context.Context) error
	globals    map[string]bool
	locals     map[string]any
}

// Local is the batch's own value under key, made by init the first time it is asked for: what a
// handler gathers over several tables of one batch (the keys a satellite must recompute, read by
// its Later step). It lives as long as the batch.
func (b *Batch) Local(key string, init func() any) any {
	if b.locals == nil {
		b.locals = map[string]any{}
	}
	value, ok := b.locals[key]
	if !ok {
		value = init()
		b.locals[key] = value
	}
	return value
}

// Later queues fn once per key, to run after every table of the batch was applied (a satellite
// recomputed once however many of its rows changed).
func (b *Batch) Later(key string, fn func() error) {
	if _, queued := b.later[key]; queued {
		return
	}
	b.later[key] = fn
	b.laterOrder = append(b.laterOrder, key)
}

// LockGlobal takes, for the rest of the drain transaction, the lock of a view shared by every owner
// (a list across owners): a seed of that view (SeedView with the same name) then never interleaves
// with a drain that changes it. It waits up to five seconds.
func (b *Batch) LockGlobal(name string) error {
	if b.globals == nil {
		b.globals = map[string]bool{}
	}
	if b.globals[name] {
		return nil
	}
	if _, err := b.Tx.ExecContext(b.Ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return err
	}
	if _, err := b.Tx.ExecContext(b.Ctx, `SELECT pg_advisory_xact_lock(hashtextextended('redifu:' || $1, 0))`, name); err != nil {
		return err
	}
	b.globals[name] = true
	return nil
}

// AfterExec queues fn to run once the pipeline was executed (a Purge, which runs its own pipeline,
// must not run before the writes it follows).
func (b *Batch) AfterExec(fn func(context.Context) error) {
	b.afterExec = append(b.afterExec, fn)
}

// redifuLockSQL takes the advisory lock of one owner's cache (drains and seeds of that owner take
// it) and bounds the transaction that holds it, in one round trip.
const redifuLockSQL = `SELECT pg_try_advisory_xact_lock(hashtextextended('redifu:' || $1, 0))
  FROM (SELECT set_config('statement_timeout', '15000', true),
               set_config('idle_in_transaction_session_timeout', '30000', true)) AS limits`

// redifuDrainRetry is how long a read leaves an owner to SQL after a drain of that owner failed,
// so a failing row costs one attempt every few seconds instead of one per request.
const redifuDrainRetry = 5 * time.Second

// Ready reports whether the outbox exists (Schema was installed) and refreshes the set of watched
// tables now and then.
func (s *Store) Ready(ctx context.Context) bool {
	last := s.checkedAt.Load()
	if last == 0 || time.Since(time.Unix(0, last)) > redifuWatchRecheck {
		s.refreshWatched(ctx)
	}
	return s.ready.Load()
}

func (s *Store) refreshWatched(ctx context.Context) {
	s.checkedAt.Store(time.Now().UnixNano())
	var exists bool
	var tables []string
	err := s.Query(ctx, func(q sqlx.QueryerContext) error {
		if err := sqlx.GetContext(ctx, q, &exists, `SELECT to_regclass('redifu_outbox') IS NOT NULL`); err != nil {
			return fmt.Errorf("cannot check the outbox: %w", err)
		}
		if !exists {
			return nil
		}
		if err := sqlx.SelectContext(ctx, q, &tables,
			`SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE t.tgname = 'redifu_capture' AND NOT t.tgisinternal`); err != nil {
			return fmt.Errorf("cannot read the watched tables: %w", err)
		}
		return nil
	})
	if err != nil {
		if !IsPoolBusy(err) {
			s.log().Error("redifu: cannot refresh the watched tables", "error", err.Error())
		}
		return
	}
	s.ready.Store(exists)
	if !exists {
		return
	}
	watched := make(map[string]bool, len(tables))
	for _, table := range tables {
		watched[table] = true
	}
	s.watchedMu.Lock()
	s.watched = watched
	s.watchedMu.Unlock()
}

// Watches reports whether every table carries the capture trigger. A cache built on a table the
// database does not watch would never hear of its writes, so such a read always goes to SQL.
func (s *Store) Watches(tables ...string) bool {
	s.watchedMu.RLock()
	defer s.watchedMu.RUnlock()
	for _, table := range tables {
		if !s.watched[table] {
			return false
		}
	}
	return true
}

// RedisFailed records a Redis error seen by a read: reads skip Redis for a few seconds.
func (s *Store) RedisFailed(err error) {
	if err == nil || errors.Is(err, redis.Nil) {
		return
	}
	s.redisDownUntil.Store(time.Now().Add(redifuRedisPause).UnixNano())
	s.log().Warn("redifu: redis failed, reads go to SQL for a while", "error", err.Error(), "pause", redifuRedisPause.String())
}

func (s *Store) redisDown() bool {
	return time.Now().UnixNano() < s.redisDownUntil.Load()
}

// Enabled reports whether reads built on tables may use the cache at all: reads on, the outbox
// present, every table watched, Redis not in its pause after a failure, and this build holding the
// cache's lease (an older build reads SQL while a newer one runs).
func (s *Store) Enabled(ctx context.Context, tables ...string) bool {
	return s != nil && s.reads && !s.redisDown() && s.Ready(ctx) && s.Watches(tables...) && s.holdsLease(ctx)
}

// Serve reports whether a read of owner's views (built on tables) may be answered from Redis
// now. It applies the owner's queued changes first; when they cannot be applied right now (a
// drain of the same owner holds the lock, or it failed) the answer is no and the read goes to SQL.
// owner "*" stands for every owner (views across owners).
func (s *Store) Serve(ctx context.Context, owner string, tables ...string) bool {
	ok, _ := s.ServeOwner(ctx, owner, tables...)
	return ok
}

// ServeOwner is Serve that also reports whether it applied queued changes (a caller that read
// entities before asking must read them again).
func (s *Store) ServeOwner(ctx context.Context, owner string, tables ...string) (bool, bool) {
	if !s.Enabled(ctx, tables...) {
		return false, false
	}
	// Within one read (NewReadScope) an owner's queue is applied once. "*" applies only the queues
	// that touch tables, so it counts for those tables alone.
	key := owner
	if owner == "*" {
		key = "*:" + strings.Join(tables, ",")
	}
	scope := readScopeOf(ctx)
	if scope.has(key) {
		return true, false
	}
	ok, drained := s.serveOwner(ctx, owner, tables)
	if ok {
		scope.mark(key)
	}
	return ok, drained
}

// inBackoff reports whether a drain of owner failed a moment ago: its reads go to SQL until
// redifuDrainRetry has passed, instead of running the failing drain again on every request.
func (s *Store) inBackoff(owner string) bool {
	until, failed := s.drainFailed.Load(owner)
	return failed && time.Now().Before(until.(time.Time))
}

func (s *Store) serveOwner(ctx context.Context, owner string, tables []string) (bool, bool) {
	if owner == "*" {
		// A view across owners only depends on the queues that touch its own tables.
		owners, err := s.pendingOwners(ctx, 0, tables, nil)
		if err != nil {
			if !IsPoolBusy(err) {
				s.log().Error("redifu: pending check failed", "error", err.Error())
			}
			return false, false
		}
		for _, pending := range owners {
			if s.inBackoff(pending) {
				return false, false
			}
			if done, err := s.DrainOwner(ctx, pending); err != nil || !done {
				return false, true
			}
		}
		return true, len(owners) > 0
	}
	var pending bool
	if err := s.Query(ctx, func(q sqlx.QueryerContext) error {
		return sqlx.GetContext(ctx, q, &pending, `SELECT EXISTS (SELECT 1 FROM redifu_outbox WHERE owner_uuid = $1)`, owner)
	}); err != nil {
		if !IsPoolBusy(err) {
			s.log().Error("redifu: pending check failed", "error", err.Error())
		}
		return false, false
	}
	if !pending {
		return true, false
	}
	if s.inBackoff(owner) {
		return false, false
	}
	done, err := s.DrainOwner(ctx, owner)
	return err == nil && done, true
}

// ServeMany is Serve for several owners at once (a page that shows something of each of several
// owners): one query finds which of them have queued changes, those queues are applied, and the
// answer is the set of owners whose reads may be answered from Redis now. An owner missing from it
// is read from SQL.
func (s *Store) ServeMany(ctx context.Context, owners []string, tables ...string) map[string]bool {
	served := make(map[string]bool, len(owners))
	if len(owners) == 0 || !s.Enabled(ctx, tables...) {
		return served
	}
	var pending []string
	if err := s.Query(ctx, func(q sqlx.QueryerContext) error {
		return sqlx.SelectContext(ctx, q, &pending, `SELECT DISTINCT owner_uuid FROM redifu_outbox WHERE owner_uuid = ANY($1)`, textArray(owners))
	}); err != nil {
		if !IsPoolBusy(err) {
			s.log().Error("redifu: pending check failed", "error", err.Error())
		}
		return served
	}
	queued := make(map[string]bool, len(pending))
	for _, owner := range pending {
		queued[owner] = true
	}
	for _, owner := range owners {
		if !queued[owner] {
			served[owner] = true
			continue
		}
		if s.inBackoff(owner) {
			continue
		}
		if done, err := s.DrainOwner(ctx, owner); err == nil && done {
			served[owner] = true
		}
	}
	return served
}

// Seed runs fn under owner's lock with the transaction that holds it: fn loads the rows through
// tx and enqueues the Redis writes into pipe, which runs before the lock is released. It returns
// false, having written nothing, when another drain or seed of the owner holds the lock, the pool
// has no connection for it (conn), or the owner has queued changes (the read goes to SQL; the next
// read applies them first).
//
// tx reads one snapshot (redifuSnapshot) in which the owner's queue is empty: what the seed writes is
// what the next drain's old rows say Redis holds. pipe is a MULTI/EXEC transaction: Redis applies a
// seed whole, so a reader never sees an index half filled (a plain pipeline over 16 KB is read in
// chunks, with other clients' commands in between, and a reader would take the part it saw for
// the whole list).
func (s *Store) Seed(ctx context.Context, owner string, fn func(tx *sqlx.Tx, pipe redis.Pipeliner) error) (bool, error) {
	return s.seed(ctx, owner, `SELECT EXISTS (SELECT 1 FROM redifu_outbox WHERE owner_uuid = $1)`, owner, fn)
}

// SeedView is Seed for a view across owners (a list of every owner's rows): name is the view's
// lock (the one its drains take with batch.LockGlobal), and the seed waits until no change of
// tables is queued, whoever's.
func (s *Store) SeedView(ctx context.Context, name string, tables []string, fn func(tx *sqlx.Tx, pipe redis.Pipeliner) error) (bool, error) {
	return s.seed(ctx, name, `SELECT EXISTS (SELECT 1 FROM redifu_outbox WHERE table_name = ANY($1))`, textArray(tables), fn)
}

func (s *Store) seed(ctx context.Context, lock, pendingSQL string, pendingArg any, fn func(tx *sqlx.Tx, pipe redis.Pipeliner) error) (bool, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		if IsPoolBusy(err) {
			return false, nil
		}
		return false, err
	}
	defer func() { _ = conn.Close() }()
	tx, err := conn.BeginTxx(ctx, redifuSnapshot)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var locked bool
	if err := tx.GetContext(ctx, &locked, redifuLockSQL, lock); err != nil {
		return false, err
	}
	if !locked {
		return false, nil
	}
	var pending bool
	if err := tx.GetContext(ctx, &pending, pendingSQL, pendingArg); err != nil {
		return false, err
	}
	if pending {
		return false, nil
	}
	pipe := s.rdb.TxPipeline()
	if err := fn(tx, pipe); err != nil {
		return false, err
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		s.RedisFailed(err)
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// DrainOwner applies owner's queue. It reports false when another drain or seed of the owner
// holds the lock (the queue is then theirs to apply).
func (s *Store) DrainOwner(ctx context.Context, owner string) (bool, error) {
	for {
		taken, locked, err := s.drainBatch(ctx, owner)
		if err != nil {
			s.drainFailed.Store(owner, time.Now().Add(redifuDrainRetry))
			s.log().Error("redifu: drain failed", "owner", owner, "error", err.Error())
			return false, err
		}
		if !locked {
			return false, nil
		}
		if taken < redifuDrainBatch {
			return true, nil
		}
	}
}

func (s *Store) drainBatch(ctx context.Context, owner string) (int, bool, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		if IsPoolBusy(err) {
			return 0, false, nil // like a busy lock: the reads go to SQL, a later drain applies the queue
		}
		return 0, false, err
	}
	defer func() { _ = conn.Close() }()
	tx, err := conn.BeginTxx(ctx, redifuSnapshot)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback() }()

	var locked bool
	if err := tx.GetContext(ctx, &locked, redifuLockSQL, owner); err != nil {
		return 0, false, err
	}
	if !locked {
		return 0, false, nil
	}

	var changes []Change
	if err := tx.SelectContext(ctx, &changes, `
		DELETE FROM redifu_outbox
		 WHERE id IN (SELECT id FROM redifu_outbox WHERE owner_uuid = $1 ORDER BY id LIMIT $2)
		RETURNING id, owner_uuid, table_name, row_key, op, old_row::text AS old_row`, owner, redifuDrainBatch); err != nil {
		if isSerializationFailure(err) {
			// Another drain of the owner committed between this one's snapshot and its lock: its rows
			// are gone already. Like a busy lock, the queue is left to a later drain.
			return 0, false, nil
		}
		return 0, true, err
	}
	if len(changes) == 0 {
		return 0, true, tx.Commit()
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].ID < changes[j].ID })

	// The handlers reload what changed and enqueue their writes into a recording pipeline; the writes
	// then go in one MULTI/EXEC for the whole batch, like a seed — a reader sees the batch applied
	// whole or not at all (an entity moved between two indexes is never in neither) — behind the
	// lease's fence (fence.go): a takeover before it runs aborts it, and the rollback below leaves the
	// changes queued for the holder.
	pipe := record()
	batch, errApply := s.applyBatch(ctx, tx, owner, pipe, changes)
	if errApply != nil {
		return 0, true, errApply
	}
	errApply = s.commitBatch(ctx, pipe.Commands(ctx))
	if errApply == nil {
		for _, fn := range batch.afterExec {
			if err := fn(ctx); err != nil {
				errApply = redisWriteError{err}
				break
			}
		}
	}
	var redisErr redisWriteError
	switch {
	case errors.Is(errApply, errLeaseLost), errors.Is(errApply, errLeaseMissing), errors.Is(errApply, redis.TxFailedErr):
		s.leaseLost()
		return 0, false, nil // like a busy lock: the holder's drain applies the queue
	case errors.As(errApply, &redisErr):
		s.RedisFailed(redisErr.err)
		return 0, true, redisErr.err
	case errApply != nil:
		return 0, true, errApply
	}
	if err := tx.Commit(); err != nil {
		return 0, true, err
	}
	s.drainFailed.Delete(owner)
	return len(changes), true, nil
}

// redisWriteError is a Redis failure of a drain's write (the read pause applies), as opposed to a
// handler's failure.
type redisWriteError struct{ err error }

func (e redisWriteError) Error() string { return e.err.Error() }

func (e redisWriteError) Unwrap() error { return e.err }

// applyBatch runs the handlers of one drain batch over its changes, enqueueing their writes into
// pipe (recorded: the caller sends them, behind the fence), and returns the batch for its AfterExec
// steps.
func (s *Store) applyBatch(ctx context.Context, tx *sqlx.Tx, owner string, pipe redis.Pipeliner, changes []Change) (*Batch, error) {
	batch := &Batch{Ctx: ctx, Tx: tx, Owner: owner, Pipe: pipe, store: s, later: map[string]func() error{}}
	byTable := map[string][]Change{}
	var tableOrder []string
	for _, change := range changes {
		if _, ok := byTable[change.Table]; !ok {
			tableOrder = append(tableOrder, change.Table)
		}
		byTable[change.Table] = append(byTable[change.Table], change)
	}
	for _, table := range tableOrder {
		// A table this binary does not cache has no handler: nothing in Redis to correct.
		for _, handler := range s.handlers[table] {
			if err := handler.Apply(batch, table, byTable[table]); err != nil {
				return nil, fmt.Errorf("%s: %w", table, err)
			}
		}
	}
	for i := 0; i < len(batch.laterOrder); i++ { // a Later step may queue another
		if err := batch.later[batch.laterOrder[i]](); err != nil {
			return nil, fmt.Errorf("%s: %w", batch.laterOrder[i], err)
		}
	}
	return batch, nil
}

// pendingOwners lists the owners with queued changes, oldest first (limit 0 = all): only changes
// of tables when tables is set, and none of the owners in skip.
func (s *Store) pendingOwners(ctx context.Context, limit int, tables, skip []string) ([]string, error) {
	query := `SELECT owner_uuid FROM redifu_outbox`
	var conditions []string
	var args []any
	if len(tables) > 0 {
		args = append(args, textArray(tables))
		conditions = append(conditions, fmt.Sprintf("table_name = ANY($%d)", len(args)))
	}
	if len(skip) > 0 {
		args = append(args, textArray(skip))
		conditions = append(conditions, fmt.Sprintf("owner_uuid <> ALL($%d)", len(args)))
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += ` GROUP BY owner_uuid ORDER BY min(id)`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	var owners []string
	err := s.Query(ctx, func(q sqlx.QueryerContext) error {
		return sqlx.SelectContext(ctx, q, &owners, query, args...)
	})
	return owners, err
}

// backoffOwners lists the owners whose drain failed a moment ago (inBackoff), forgetting the ones
// whose pause is over.
func (s *Store) backoffOwners() []string {
	now := time.Now()
	var owners []string
	s.drainFailed.Range(func(key, value any) bool {
		if now.Before(value.(time.Time)) {
			owners = append(owners, key.(string))
		} else {
			s.drainFailed.Delete(key)
		}
		return true
	})
	return owners
}

// DrainAll applies the queue of every owner it can lock now (the background drain). It returns
// how many owners it applied. An owner whose drain failed a moment ago waits out its pause, and
// does not take the place of the others in the round.
func (s *Store) DrainAll(ctx context.Context) (int, error) {
	// Only the build holding the lease drains (lease.go); the check also renews it.
	if !s.Ready(ctx) || !s.holdsLease(ctx) {
		return 0, nil
	}
	owners, err := s.pendingOwners(ctx, redifuDrainOwners, nil, s.backoffOwners())
	if err != nil {
		if IsPoolBusy(err) {
			return 0, nil
		}
		return 0, err
	}
	drained := 0
	for _, owner := range owners {
		done, err := s.DrainOwner(ctx, owner)
		if err != nil {
			continue // logged; the next round retries
		}
		if done {
			drained++
		}
	}
	return drained, nil
}

// Backlog reports the queue length and the age of its oldest row (health).
func (s *Store) Backlog(ctx context.Context) (int64, time.Duration, error) {
	var row struct {
		Count  int64      `db:"count"`
		Oldest *time.Time `db:"oldest"`
	}
	if err := s.Query(ctx, func(q sqlx.QueryerContext) error {
		return sqlx.GetContext(ctx, q, &row, `SELECT count(*) AS count, min(created_at) AS oldest FROM redifu_outbox`)
	}); err != nil {
		return 0, 0, err
	}
	if row.Oldest == nil {
		return row.Count, 0, nil
	}
	return row.Count, time.Since(*row.Oldest), nil
}

// RunDrainLoop drains every interval until ctx ends, and logs a backlog that grows old (a
// handler failing on some row, Redis gone).
func (s *Store) RunDrainLoop(ctx context.Context, interval time.Duration) {
	logger := s.log()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastHealth := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// The loop may run in every process of the application (a server and a worker): a
			// panic in a pass is logged and the next tick drains again.
			s.runPass("redifu_drain", func() {
				if _, err := s.DrainAll(ctx); err != nil {
					logger.Error("redifu: background drain failed", "error", err.Error())
				}
				if time.Since(lastHealth) > redifuBacklogCheck {
					lastHealth = time.Now()
					if count, age, err := s.Backlog(ctx); err == nil && age > redifuBacklogAge {
						logger.Error("redifu: outbox backlog is old", "rows", count, "oldest", age.String())
					}
				}
			})
		}
	}
}
