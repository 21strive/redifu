package redifu

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type SortedSet[T item.Blueprint] struct {
	client redis.UniversalClient
	keys   *keyBuilder
	// selfHeal removes index members whose item key no longer exists. Without it an
	// index decays into a set of dangling randIds that quietly shortens every page.
	selfHeal bool
}

func NewSortedSet[T item.Blueprint](client redis.UniversalClient, sortedSetKeyFormat string) (*SortedSet[T], error) {
	sorted := &SortedSet[T]{}
	if err := sorted.Init(client, sortedSetKeyFormat); err != nil {
		return nil, err
	}
	return sorted, nil
}

func (cr *SortedSet[T]) Init(client redis.UniversalClient, sortedSetKeyFormat string) error {
	if client == nil {
		return errors.New("redifu: sorted set client must not be nil")
	}
	keys, err := newKeyBuilder(sortedSetKeyFormat)
	if err != nil {
		return err
	}
	cr.client = client
	cr.keys = keys
	cr.selfHeal = true
	return nil
}

// key returns the collection key, hash-tagged so the sorted set and its markers share
// a Redis Cluster slot.
func (cr *SortedSet[T]) key(keyParams []string) (string, error) {
	return cr.keys.buildTagged(keyParams)
}

func (cr *SortedSet[T]) markerKey(suffix string, keyParams []string) (string, error) {
	key, err := cr.key(keyParams)
	if err != nil {
		return "", err
	}
	return key + suffix, nil
}

func (cr *SortedSet[T]) SetSelfHeal(selfHeal bool) {
	cr.selfHeal = selfHeal
}

func (cr *SortedSet[T]) SetItem(ctx context.Context, pipe redis.Pipeliner, score float64, item T, keyParams ...string) error {
	key, err := cr.key(keyParams)
	if err != nil {
		return err
	}

	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: item.GetRandId()})
	return nil
}

func (cr *SortedSet[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, timeToLive time.Duration, keyParams ...string) error {
	key, err := cr.key(keyParams)
	if err != nil {
		return err
	}

	pipe.Expire(ctx, key, timeToLive)
	return nil
}

func (cr *SortedSet[T]) RemoveItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	key, err := cr.key(keyParams)
	if err != nil {
		return err
	}

	pipe.ZRem(ctx, key, item.GetRandId())
	return nil
}

// Count returns the number of members in the index. It reports Redis failures instead
// of folding them into a zero: a zero that actually means "Redis is down" reads as
// "this collection is empty", which sends every caller to the database at once and
// makes ingest drop items on the floor.
func (cr *SortedSet[T]) Count(ctx context.Context, keyParams ...string) (int64, error) {
	key, err := cr.key(keyParams)
	if err != nil {
		return 0, err
	}

	return cr.client.ZCard(ctx, key).Result()
}

func (cr *SortedSet[T]) Delete(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	key, err := cr.key(keyParams)
	if err != nil {
		return err
	}

	pipe.Del(ctx, key)
	return nil
}

func (cr *SortedSet[T]) LowestScore(ctx context.Context, keyParams ...string) (float64, error) {
	return cr.edgeScore(ctx, true, keyParams)
}

func (cr *SortedSet[T]) HighestScore(ctx context.Context, keyParams ...string) (float64, error) {
	return cr.edgeScore(ctx, false, keyParams)
}

func (cr *SortedSet[T]) edgeScore(ctx context.Context, lowest bool, keyParams []string) (float64, error) {
	key, errKey := cr.key(keyParams)
	if errKey != nil {
		return 0, errKey
	}

	start, stop := int64(0), int64(0)
	if !lowest {
		start, stop = -1, -1
	}

	result, err := cr.client.ZRangeWithScores(ctx, key, start, stop).Result()
	if err != nil {
		return 0, fmt.Errorf("redifu: failed to read edge score: %w", err)
	}
	if len(result) == 0 {
		return 0, ErrNotFound
	}

	return result[0].Score, nil
}

// rangeMembers reads the randIds the index holds for this range. It is deliberately
// separate from hydration so that Timeline's cursor script, which produces members by
// a different route, shares the same item-loading path.
func (cr *SortedSet[T]) rangeMembers(
	ctx context.Context,
	direction string,
	start int64,
	stop int64,
	byScore bool,
	keyParams []string,
) ([]string, error) {
	if direction != Ascending && direction != Descending {
		return nil, fmt.Errorf("redifu: direction must be %q or %q, got %q", Ascending, Descending, direction)
	}

	key, errKey := cr.key(keyParams)
	if errKey != nil {
		return nil, errKey
	}

	var result *redis.StringSliceCmd
	if byScore {
		reqRange := redis.ZRangeBy{
			Min: formatScore(float64(start)),
			Max: formatScore(float64(stop)),
		}
		if direction == Descending {
			result = cr.client.ZRevRangeByScore(ctx, key, &reqRange)
		} else {
			result = cr.client.ZRangeByScore(ctx, key, &reqRange)
		}
	} else {
		if direction == Descending {
			result = cr.client.ZRevRange(ctx, key, start, stop)
		} else {
			result = cr.client.ZRange(ctx, key, start, stop)
		}
	}

	if result.Err() != nil {
		return nil, result.Err()
	}

	return result.Val(), nil
}

// hydrate turns a list of randIds into items: one batched Base read, then one batched
// read per relation. It returns the items it could resolve and how many members the
// index held that Base no longer does.
//
// Those dangling members matter twice over. They are removed from the index here, so a
// collection converges instead of decaying, and the count is reported to the caller,
// so Timeline can tell "this is the end of the feed" apart from "three items in the
// middle of this page have expired".
func (cr *SortedSet[T]) hydrate(
	ctx context.Context,
	baseClient *Base[T],
	members []string,
	relations []Relation[T],
	relationDepth int,
	processor func(item *T, args []interface{}),
	processorArgs []interface{},
	keyParams []string,
) ([]T, int, error) {
	if len(members) == 0 {
		return nil, 0, nil
	}

	fetchedItems, errGetMany := baseClient.getManyRaw(ctx, members)
	if errGetMany != nil {
		return nil, 0, errGetMany
	}

	items := make([]T, 0, len(members))
	dangling := make([]interface{}, 0)

	for _, randId := range members {
		fetchedItem, found := fetchedItems[randId]
		if !found {
			dangling = append(dangling, randId)
			continue
		}
		items = append(items, fetchedItem)
	}

	if len(dangling) > 0 && cr.selfHeal {
		// Best effort. The read succeeded; failing it because the cleanup did not
		// would trade a working page for a housekeeping error.
		if key, err := cr.key(keyParams); err == nil {
			cr.client.ZRem(ctx, key, dangling...)
		}
	}

	if len(items) == 0 {
		return nil, len(dangling), nil
	}

	if errRelation := resolveRelations(ctx, cr.client, relations, items, relationDepth); errRelation != nil {
		return nil, len(dangling), errRelation
	}

	if processor != nil {
		for i := range items {
			processor(&items[i], processorArgs)
		}
	}

	return items, len(dangling), nil
}

// Fetch reads a range of the index and returns the hydrated items.
func (cr *SortedSet[T]) Fetch(
	ctx context.Context,
	baseClient *Base[T],
	direction string,
	processor func(item *T, args []interface{}),
	processorArgs []interface{},
	relations []Relation[T],
	relationDepth int,
	start int64,
	stop int64,
	byScore bool,
	keyParams ...string) ([]T, error) {
	members, err := cr.rangeMembers(ctx, direction, start, stop, byScore, keyParams)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	items, _, errHydrate := cr.hydrate(ctx, baseClient, members, relations, relationDepth, processor, processorArgs, keyParams)
	if errHydrate != nil {
		return nil, errHydrate
	}

	return items, nil
}
