package outbox

import (
	"context"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// Detail reads entities by key straight from their Base (a detail page, a batch of related
// rows), loading the ones Redis does not hold under their owner's lock. The owner of a key is
// only known once the entity is read, so the owner's queue is applied after the first read and
// the keys are read again when that applied anything.
type Detail[E redifu.Blueprint] struct {
	store  *Store
	base   *redifu.Base[E]
	tables []string
	owner  func(E) string
	load   func(ctx context.Context, q sqlx.QueryerContext, keys []string) ([]E, error)
}

// NewDetail builds the reader. owner names an entity's owner; load reads entities by key from
// the database through q (the primary, or the lock-holding transaction).
func NewDetail[E redifu.Blueprint](store *Store, base *redifu.Base[E], tables []string,
	owner func(E) string,
	load func(ctx context.Context, q sqlx.QueryerContext, keys []string) ([]E, error),
) *Detail[E] {
	return &Detail[E]{store: store, base: base, tables: tables, owner: owner, load: load}
}

// Base is the entity store of the reader.
func (d *Detail[E]) Base() *redifu.Base[E] { return d.base }

// Get returns the entities that exist for keys, by key. ok is false when the cache cannot answer
// (the caller reads SQL). A key the database does not have is simply absent from the map.
func (d *Detail[E]) Get(ctx context.Context, keys ...string) (map[string]E, bool) {
	if len(keys) == 0 {
		return map[string]E{}, true
	}
	if !d.store.Enabled(ctx, d.tables...) {
		return nil, false
	}
	found, err := d.base.GetMany(ctx, keys)
	if err != nil {
		d.store.RedisFailed(err)
		return nil, false
	}

	// Apply the queue of every owner seen; when that changed anything, read again.
	reread := false
	owners := map[string]bool{}
	for _, item := range found {
		owners[d.owner(item)] = true
	}
	for owner := range owners {
		ok, drained := d.store.ServeOwner(ctx, owner, d.tables...)
		if !ok {
			return nil, false
		}
		reread = reread || drained
	}
	if reread {
		if found, err = d.base.GetMany(ctx, keys); err != nil {
			d.store.RedisFailed(err)
			return nil, false
		}
	}

	var missing []string
	for _, key := range keys {
		if _, ok := found[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return found, true
	}

	// Learn the owners of the missing keys from the database, then load them again under each
	// owner's lock (a drain of that owner may be applying a newer version right now).
	var loaded []E
	if err := d.store.Query(ctx, func(q sqlx.QueryerContext) error {
		var errLoad error
		loaded, errLoad = d.load(ctx, q, missing)
		return errLoad
	}); err != nil {
		return nil, false
	}
	byOwner := map[string][]string{}
	for _, item := range loaded {
		owner := d.owner(item)
		byOwner[owner] = append(byOwner[owner], item.GetRandId())
	}
	for owner, ownerKeys := range byOwner {
		if !d.store.Serve(ctx, owner, d.tables...) {
			return nil, false
		}
		var fresh []E
		seeded, err := d.store.Seed(ctx, owner, func(tx *sqlx.Tx, pipe redis.Pipeliner) error {
			var errLoad error
			fresh, errLoad = d.load(ctx, tx, ownerKeys)
			if errLoad != nil {
				return errLoad
			}
			for _, item := range fresh {
				if errSet := d.base.WithPipeline(pipe).Set(ctx, item); errSet != nil {
					return errSet
				}
			}
			return nil
		})
		if err != nil || !seeded {
			return nil, false
		}
	}
	// Read them back through Base so their relations resolve like every other read.
	again, err := d.base.GetMany(ctx, missing)
	if err != nil {
		d.store.RedisFailed(err)
		return nil, false
	}
	for key, item := range again {
		found[key] = item
	}
	return found, true
}

// Put writes entities the caller loaded under owner's lock (a seed of a list does this for its
// members already); exported for handlers that rebuild an entity.
func (d *Detail[E]) Put(ctx context.Context, pipe redis.Pipeliner, items ...E) error {
	for _, item := range items {
		if err := d.base.WithPipeline(pipe).Set(ctx, item); err != nil {
			return err
		}
	}
	return nil
}
