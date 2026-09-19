package redifu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

// Base is the single source of truth for an entity: one Redis key per item, written
// once and read by every index and every relation that points at it.
type Base[T item.Blueprint] struct {
	client        redis.UniversalClient
	keys          *keyBuilder
	timeToLive    time.Duration
	relations     []Relation[T]
	relationDepth int
	touchOnRead   bool
}

type BaseWithPipeline[T item.Blueprint] struct {
	baseClient *Base[T]
	pipe       redis.Pipeliner
}

func (bw *BaseWithPipeline[T]) Set(ctx context.Context, item T, keyParams ...string) error {
	return bw.baseClient.set(ctx, bw.pipe, item, keyParams...)
}

func (bw *BaseWithPipeline[T]) SetIfAbsent(ctx context.Context, item T) error {
	return bw.baseClient.setIfAbsent(ctx, bw.pipe, item)
}

func (bw *BaseWithPipeline[T]) Del(ctx context.Context, item T, keyParams ...string) error {
	return bw.baseClient.del(ctx, bw.pipe, item, keyParams...)
}

func (bw *BaseWithPipeline[T]) MarkAsMissing(ctx context.Context, keyParams ...string) error {
	return bw.baseClient.markAsMissing(ctx, bw.pipe, keyParams...)
}

// NewBase builds the item store. itemKeyFormat must take exactly one %s — an item is
// addressed by exactly one randId, and that is what lets any relation anywhere point
// at it with nothing but that id.
func NewBase[T item.Blueprint](client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) (*Base[T], error) {
	base := &Base[T]{}
	if err := base.Init(client, itemKeyFormat, timeToLive); err != nil {
		return nil, err
	}
	return base, nil
}

func (cr *Base[T]) Init(client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) error {
	if client == nil {
		return errors.New("redifu: base client must not be nil")
	}

	keys, err := newKeyBuilder(itemKeyFormat)
	if err != nil {
		return err
	}
	if keys.arity != 1 {
		return fmt.Errorf("redifu: item key format %q takes %d parameters — Base takes exactly one %%s, the item's randId", itemKeyFormat, keys.arity)
	}

	cr.client = client
	cr.keys = keys
	cr.timeToLive = timeToLive
	cr.relationDepth = DefaultRelationDepth
	// A shared entity is the most-read key in the system and, without this, the most
	// likely to expire underneath everything that points at it. Reading it keeps it
	// alive. Requires Redis 6.2 (GETEX); call SetTouchOnRead(false) on anything older.
	cr.touchOnRead = true
	return nil
}

// AddRelation registers a relation on the entity itself, so it is resolved by every
// read of this Base — Get, GetMany, and any index built on top of it — and by any
// other entity that relates to this one. This is where relations belong: a Post's
// author should not depend on which index the Post happened to be fetched through.
func (cr *Base[T]) AddRelation(relations ...Relation[T]) {
	cr.relations = append(cr.relations, relations...)
}

func (cr *Base[T]) GetRelations() []Relation[T] {
	return cr.relations
}

// SetRelationDepth caps how many levels of nested relations a read follows. Resolution
// stops at the limit rather than failing, which is what lets a self-referential
// relation (a manager who is also an employee) terminate.
func (cr *Base[T]) SetRelationDepth(depth int) error {
	if depth < 1 {
		return fmt.Errorf("redifu: relation depth must be at least 1, got %d", depth)
	}
	cr.relationDepth = depth
	return nil
}

// SetTouchOnRead controls whether reading an item extends its TTL. On by default.
func (cr *Base[T]) SetTouchOnRead(touch bool) {
	cr.touchOnRead = touch
}

func (cr *Base[T]) TimeToLive() time.Duration {
	return cr.timeToLive
}

func (cr *Base[T]) itemKey(keyParams []string) (string, error) {
	return cr.keys.build(keyParams)
}

// Get reads one item and resolves its relations. A key that does not exist returns
// ErrNotFound, never the driver's redis.Nil.
func (cr *Base[T]) Get(ctx context.Context, keyParams ...string) (T, error) {
	var nilItem T

	key, errKey := cr.itemKey(keyParams)
	if errKey != nil {
		return nilItem, errKey
	}

	var result *redis.StringCmd
	if cr.touchOnRead && cr.timeToLive > 0 {
		result = cr.client.GetEx(ctx, key, cr.timeToLive)
	} else {
		result = cr.client.Get(ctx, key)
	}

	if result.Err() != nil {
		if errors.Is(result.Err(), redis.Nil) {
			return nilItem, ErrNotFound
		}
		return nilItem, result.Err()
	}

	var fetchedItem T
	if errUnmarshal := json.Unmarshal([]byte(result.Val()), &fetchedItem); errUnmarshal != nil {
		return nilItem, errUnmarshal
	}

	if err := resolveRelations(ctx, cr.client, cr.relations, []T{fetchedItem}, cr.relationDepth); err != nil {
		return nilItem, err
	}

	return fetchedItem, nil
}

// GetMany fetches every randId in one round-trip, resolves relations for the whole
// batch, and returns the items that exist, keyed by randId. A key that is missing is
// simply absent from the map — that is not an error, since an index may outlive an
// individual item.
func (cr *Base[T]) GetMany(ctx context.Context, randIds []string) (map[string]T, error) {
	items, err := cr.getManyRaw(ctx, randIds)
	if err != nil {
		return nil, err
	}

	if len(cr.relations) > 0 && len(items) > 0 {
		values := make([]T, 0, len(items))
		for _, value := range items {
			values = append(values, value)
		}
		if errRelation := resolveRelations(ctx, cr.client, cr.relations, values, cr.relationDepth); errRelation != nil {
			return nil, errRelation
		}
	}

	return items, nil
}

// getManyRaw is GetMany without relation resolution. Indexes use it so that the
// entity's own relations and the index's extra relations are resolved together, in
// one pass, instead of the batch being walked twice.
func (cr *Base[T]) getManyRaw(ctx context.Context, randIds []string) (map[string]T, error) {
	if len(randIds) == 0 {
		return map[string]T{}, nil
	}

	pipe := cr.client.Pipeline()
	resolve := cr.stageGetMany(ctx, pipe, randIds)

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	return resolve()
}

// stageGetMany enqueues one read per randId into the caller's pipeline and returns a
// resolver that reads the replies once the caller has executed it. A pipeline of GETs
// rather than MGET keeps this correct on Redis Cluster, where the keys may live in
// different slots.
func (cr *Base[T]) stageGetMany(ctx context.Context, pipe redis.Pipeliner, randIds []string) func() (map[string]T, error) {
	commands := make(map[string]*redis.StringCmd, len(randIds))
	var keyErr error

	for _, randId := range randIds {
		if _, staged := commands[randId]; staged {
			continue
		}
		key, err := cr.itemKey([]string{randId})
		if err != nil {
			keyErr = err
			break
		}
		if cr.touchOnRead && cr.timeToLive > 0 {
			commands[randId] = pipe.GetEx(ctx, key, cr.timeToLive)
		} else {
			commands[randId] = pipe.Get(ctx, key)
		}
	}

	return func() (map[string]T, error) {
		if keyErr != nil {
			return nil, keyErr
		}

		items := make(map[string]T, len(commands))

		for randId, command := range commands {
			value, err := command.Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				return nil, err
			}

			var fetchedItem T
			if errUnmarshal := json.Unmarshal([]byte(value), &fetchedItem); errUnmarshal != nil {
				return nil, errUnmarshal
			}

			items[randId] = fetchedItem
		}

		return items, nil
	}
}

func (cr *Base[T]) Set(ctx context.Context, item T, keyParam ...string) error {
	return cr.set(ctx, nil, item, keyParam...)
}

func (cr *Base[T]) set(ctx context.Context, pipe redis.Pipeliner, item T, keyParam ...string) error {
	if len(keyParam) > 1 {
		return fmt.Errorf("redifu: Set takes at most one key parameter, got %d", len(keyParam))
	}

	params := keyParam
	if len(params) == 0 {
		params = []string{item.GetRandId()}
	}

	key, errKey := cr.itemKey(params)
	if errKey != nil {
		return errKey
	}

	itemInByte, errorMarshalJson := json.Marshal(item)
	if errorMarshalJson != nil {
		return errorMarshalJson
	}

	valueAsString := string(itemInByte)

	if pipe != nil {
		pipe.Set(ctx, key, valueAsString, cr.timeToLive)
	} else {
		setRes := cr.client.Set(ctx, key, valueAsString, cr.timeToLive)
		if setRes.Err() != nil {
			return setRes.Err()
		}
	}

	return cr.unmarkMissing(ctx, pipe, params...)
}

// SetIfAbsent writes the item only when its key does not exist yet, but always refreshes
// the key's TTL. AddItem uses it so that placing an existing item into an index can never
// leave that index pointing at a key which expires before the index itself, while an item
// passed in as a stub can never overwrite the full value already held in Base.
func (cr *Base[T]) SetIfAbsent(ctx context.Context, item T) error {
	return cr.setIfAbsent(ctx, nil, item)
}

func (cr *Base[T]) setIfAbsent(ctx context.Context, pipe redis.Pipeliner, item T) error {
	params := []string{item.GetRandId()}

	key, errKey := cr.itemKey(params)
	if errKey != nil {
		return errKey
	}

	itemInByte, errorMarshalJson := json.Marshal(item)
	if errorMarshalJson != nil {
		return errorMarshalJson
	}

	valueAsString := string(itemInByte)

	if pipe != nil {
		pipe.SetNX(ctx, key, valueAsString, cr.timeToLive)
		pipe.Expire(ctx, key, cr.timeToLive)
	} else {
		setRes := cr.client.SetNX(ctx, key, valueAsString, cr.timeToLive)
		if setRes.Err() != nil {
			return setRes.Err()
		}
		expireRes := cr.client.Expire(ctx, key, cr.timeToLive)
		if expireRes.Err() != nil {
			return expireRes.Err()
		}
	}

	return cr.unmarkMissing(ctx, pipe, params...)
}

func (cr *Base[T]) Del(ctx context.Context, item T, keyParam ...string) error {
	return cr.del(ctx, nil, item, keyParam...)
}

func (cr *Base[T]) del(ctx context.Context, pipe redis.Pipeliner, item T, keyParam ...string) error {
	if len(keyParam) > 1 {
		return fmt.Errorf("redifu: Del takes at most one key parameter, got %d", len(keyParam))
	}

	params := keyParam
	if len(params) == 0 {
		params = []string{item.GetRandId()}
	}

	key, errKey := cr.itemKey(params)
	if errKey != nil {
		return errKey
	}

	if pipe != nil {
		pipe.Del(ctx, key)
		return nil
	}

	delRes := cr.client.Del(ctx, key)
	return delRes.Err()
}

func (cr *Base[T]) WithPipeline(pipe redis.Pipeliner) *BaseWithPipeline[T] {
	return &BaseWithPipeline[T]{
		baseClient: cr,
		pipe:       pipe,
	}
}

func (cr *Base[T]) missingKey(keyParams []string) (string, error) {
	key, err := cr.keys.build(keyParams)
	if err != nil {
		return "", err
	}
	return key + ":blank", nil
}

// MarkAsMissing records that the database has no such item, so that a repeated lookup
// for it does not hit the database again.
func (cr *Base[T]) MarkAsMissing(ctx context.Context, keyParams ...string) error {
	return cr.markAsMissing(ctx, nil, keyParams...)
}

func (cr *Base[T]) markAsMissing(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	key, errKey := cr.missingKey(keyParams)
	if errKey != nil {
		return errKey
	}

	if pipe != nil {
		pipe.Set(ctx, key, 1, cr.timeToLive)
		return nil
	}

	return cr.client.Set(ctx, key, 1, cr.timeToLive).Err()
}

func (cr *Base[T]) IsMissing(ctx context.Context, keyParams ...string) (bool, error) {
	key, errKey := cr.missingKey(keyParams)
	if errKey != nil {
		return false, errKey
	}

	getBlank := cr.client.Get(ctx, key)
	if getBlank.Err() != nil {
		if errors.Is(getBlank.Err(), redis.Nil) {
			return false, nil
		}
		return false, getBlank.Err()
	}

	return getBlank.Val() == "1", nil
}

func (cr *Base[T]) UnmarkMissing(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.unmarkMissing(ctx, pipe, keyParams...)
}

func (cr *Base[T]) unmarkMissing(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	key, errKey := cr.missingKey(keyParams)
	if errKey != nil {
		return errKey
	}

	if pipe != nil {
		pipe.Del(ctx, key)
		return nil
	}

	return cr.client.Del(ctx, key).Err()
}

// Exists reports whether the item key is present in Base.
func (cr *Base[T]) Exists(ctx context.Context, keyParams ...string) (bool, error) {
	key, errKey := cr.itemKey(keyParams)
	if errKey != nil {
		return false, errKey
	}

	count, err := cr.client.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	return count > 0, nil
}
