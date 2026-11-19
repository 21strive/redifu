package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// Scored is an entity that carries the score its next index write uses. redifu reads the score
// from a struct field (SetSortingReference); the cache entities keep it in a field of their own,
// set right before each ingest, because one entity sits in indexes scored differently.
type Scored interface {
	redifu.Blueprint
	SetScore(score int64)
}

// List is one whole list in one redifu Sorted index: every member of a view (an owner's
// items, an owner's categories), read in full and filtered or sliced in Go. Membership
// changes come from the drain (AddItem / RemoveItem); content changes only rewrite Base.
type List[E Scored] struct {
	store  *Store
	base   *redifu.Base[E]
	sorted *redifu.Sorted[E]
	dir    string
	tables []string
	load   func(ctx context.Context, tx *sqlx.Tx, params []string) ([]E, error)
	owner  func(params []string) string
}

// ListTTL is the TTL of a list index (and of a Buckets month and a Feed). Give the Base of its
// entities a TTL at least as long: a seed writes every member into Base again, so each member then
// outlives its index. With a shorter one the index points at entities that are gone, and every
// read of it purges and seeds it again.
const ListTTL = time.Hour

// NewList builds a list over base. keyFormat names the index (one %s per param); dir is
// redifu.Ascending or Descending; tables are the tables the list's members and relations come
// from (all must be watched for the list to be served); load returns every member for params,
// complete and scored; owner names the owner whose lock and queue cover params.
func NewList[E Scored](store *Store, base *redifu.Base[E], keyFormat, dir string, tables []string,
	load func(ctx context.Context, tx *sqlx.Tx, params []string) ([]E, error),
	owner func(params []string) string,
) (*List[E], error) {
	sorted, err := NewSorted[E](store, base, keyFormat, ListTTL)
	if err != nil {
		return nil, err
	}
	if err := sorted.SetSortingReference("Score"); err != nil {
		return nil, err
	}
	// A member whose entity is gone (evicted) must make the read rebuild the index, never quietly
	// leave the list: the reads below count dangling members and purge.
	sorted.SetSelfHeal(false)
	return &List[E]{store: store, base: base, sorted: sorted, dir: dir, tables: tables, load: load, owner: owner}, nil
}

// Sorted is the index (drain handlers add and remove through it).
func (l *List[E]) Sorted() *redifu.Sorted[E] { return l.sorted }

// Read returns the whole list for params from Redis, seeding it first when it is cold. ok is
// false when the cache cannot answer right now — reads off, the owner's queue not applied, Redis
// down, the lock busy — and the caller then reads SQL.
func (l *List[E]) Read(ctx context.Context, params ...string) ([]E, bool) {
	owner := l.owner(params)
	if !l.store.Serve(ctx, owner, l.tables...) {
		return nil, false
	}
	return l.read(ctx, params)
}

// ReadServed is Read for a caller that already applied the owner's queue within the same call
// (Serve, ServeOwner, or a Detail read of entities of that owner): it skips the queue check, so
// one answer built from an entity read and a list of the same owner costs one check, not two.
// The guarantee of Serve holds for the call as a whole, because its check happened after the
// call started. The list's tables must still all be watched.
func (l *List[E]) ReadServed(ctx context.Context, params ...string) ([]E, bool) {
	if !l.store.Enabled(ctx, l.tables...) {
		return nil, false
	}
	return l.read(ctx, params)
}

func (l *List[E]) read(ctx context.Context, params []string) ([]E, bool) {
	for attempt := 0; attempt < 2; attempt++ {
		needs, err := l.sorted.RequiresSeeding(ctx, params...)
		if err != nil {
			l.store.RedisFailed(err)
			return nil, false
		}
		if needs && !l.Seed(ctx, params...) {
			return nil, false
		}
		items, dangling, err := l.sorted.Fetch(l.dir).WithParams(params...).ExecReport(ctx)
		if errors.Is(err, redifu.ErrNotSeeded) {
			continue // the index stopped being this build's after the check: seed it again
		}
		if err != nil {
			l.store.RedisFailed(err)
			return nil, false
		}
		count, err := l.sorted.Count(ctx, params...)
		if err != nil {
			l.store.RedisFailed(err)
			return nil, false
		}
		if dangling == 0 && int64(len(items)) == count && (count > 0 || l.isMarkedEmpty(ctx, params)) {
			return items, true
		}
		// Members whose Base key is gone (evicted), or an index that expired between the
		// seed and the read: rebuild it once from the database.
		if err := l.sorted.Purge(ctx, params...); err != nil {
			l.store.RedisFailed(err)
			return nil, false
		}
	}
	return nil, false
}

// Count is how many members the list has for params, seeding a cold index first: one ZCARD when
// warm, no member read. ok false: the caller reads SQL.
func (l *List[E]) Count(ctx context.Context, params ...string) (int64, bool) {
	if !l.store.Serve(ctx, l.owner(params), l.tables...) {
		return 0, false
	}
	return l.CountServed(ctx, params...)
}

// CountServed is Count for a caller that has just had the owner's queue applied (Store.ServeMany).
func (l *List[E]) CountServed(ctx context.Context, params ...string) (int64, bool) {
	for attempt := 0; attempt < 2; attempt++ {
		count, err := l.sorted.Count(ctx, params...)
		if err != nil {
			l.store.RedisFailed(err)
			return 0, false
		}
		if count > 0 {
			return count, true
		}
		empty, err := l.sorted.IsEmpty(ctx, params...)
		if err != nil {
			l.store.RedisFailed(err)
			return 0, false
		}
		if empty {
			return 0, true
		}
		// No member and no empty marker: cold (never seeded, expired, or its last member left).
		if !l.Seed(ctx, params...) {
			return 0, false
		}
	}
	return 0, false
}

// ReadPage returns the members of ranks start..stop (0-based, both included, in the list's order)
// and the size of the whole list, seeding the index first when it is cold: a page without a
// filter decodes only its own entities. ok is false when the cache cannot answer (see Read).
func (l *List[E]) ReadPage(ctx context.Context, start, stop int64, params ...string) ([]E, int64, bool) {
	owner := l.owner(params)
	if start < 0 || stop < start || !l.store.Serve(ctx, owner, l.tables...) {
		return nil, 0, false
	}
	for attempt := 0; attempt < 2; attempt++ {
		needs, err := l.sorted.RequiresSeeding(ctx, params...)
		if err != nil {
			l.store.RedisFailed(err)
			return nil, 0, false
		}
		if needs && !l.Seed(ctx, params...) {
			return nil, 0, false
		}
		count, err := l.sorted.Count(ctx, params...)
		if err != nil {
			l.store.RedisFailed(err)
			return nil, 0, false
		}
		if count == 0 && !l.isMarkedEmpty(ctx, params) {
			// The index expired between the seed check and the count: seed it again.
			if err := l.sorted.Purge(ctx, params...); err != nil {
				l.store.RedisFailed(err)
				return nil, 0, false
			}
			continue
		}
		if start >= count {
			return nil, count, true
		}
		items, dangling, err := l.sorted.Fetch(l.dir).WithParams(params...).WithRankRange(start, stop).ExecReport(ctx)
		if errors.Is(err, redifu.ErrNotSeeded) {
			continue // the index stopped being this build's after the count: seed it again
		}
		if err != nil {
			l.store.RedisFailed(err)
			return nil, 0, false
		}
		want := stop - start + 1
		if rest := count - start; rest < want {
			want = rest
		}
		if dangling == 0 && int64(len(items)) == want {
			return items, count, true
		}
		// A member whose Base key is gone, or an index that changed under the read: rebuild it
		// once from the database.
		if err := l.sorted.Purge(ctx, params...); err != nil {
			l.store.RedisFailed(err)
			return nil, 0, false
		}
	}
	return nil, 0, false
}

func (l *List[E]) isMarkedEmpty(ctx context.Context, params []string) bool {
	empty, err := l.sorted.IsEmpty(ctx, params...)
	return err == nil && empty
}

// Seed fills the index for params from the database under the owner's lock. It reports false
// when the lock was busy or something failed (nothing is then trusted).
func (l *List[E]) Seed(ctx context.Context, params ...string) bool {
	seeded, err := l.store.Seed(ctx, l.owner(params), func(tx *sqlx.Tx, pipe redis.Pipeliner) error {
		items, err := l.load(ctx, tx, params)
		if err != nil {
			return err
		}
		return l.Fill(ctx, pipe, items, params...)
	})
	return err == nil && seeded
}

// Fill enqueues a complete list into an empty index: every item into Base (Set: a seed heals a
// stale key) and into the index, the empty marker when there is none, and the index TTL.
func (l *List[E]) Fill(ctx context.Context, pipe redis.Pipeliner, items []E, params ...string) error {
	for _, item := range items {
		if err := l.base.WithPipeline(pipe).Set(ctx, item); err != nil {
			return err
		}
		if err := l.sorted.IngestItem(ctx, pipe, item, true, params...); err != nil {
			return err
		}
	}
	if len(items) == 0 {
		if err := l.sorted.MarkEmpty(ctx, pipe, params...); err != nil {
			return err
		}
	} else if err := l.sorted.HasData(ctx, pipe, params...); err != nil {
		return err
	}
	return l.sorted.SetExpiration(ctx, pipe, params...)
}

// Add enqueues item into the index for params (it enters only when the index is seeded; a cold
// index picks it up when it is seeded). The caller sets the item's score first.
func (l *List[E]) Add(ctx context.Context, pipe redis.Pipeliner, item E, params ...string) error {
	return l.sorted.WithPipeline(pipe).AddItem(ctx, item, params...)
}

// Remove enqueues item's removal from the index for params.
func (l *List[E]) Remove(ctx context.Context, pipe redis.Pipeliner, item E, params ...string) error {
	return l.sorted.WithPipeline(pipe).RemoveItem(ctx, item, params...)
}

// ErrNotCached is what a cached read returns when the caller must read SQL instead.
var ErrNotCached = errors.New("redifucache: not served from the cache")
