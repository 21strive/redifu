package redifu

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type SortedSet[T Blueprint] struct {
	client redis.UniversalClient
	keys   *keyBuilder
	// selfHeal removes index members whose item key no longer exists. Without it an
	// index decays into a set of dangling randIds that quietly shortens every page.
	selfHeal bool
	// markers are the suffixes of the collection's state markers, in the order the
	// generation scripts take them: :blankpage for a Sorted; :blankpage and :segments (the
	// segment store) for a TimeSeries; :firstpage, :lastpage and :blankpage for a Timeline.
	markers []string
	// gen is the index generation its owner set (Sorted.SetGeneration,
	// Timeline.SetGeneration), none by default. The reads below check it; the writes are made
	// by the owner, which knows the TTL a stamp needs.
	gen generationCell
}

func NewSortedSet[T Blueprint](client redis.UniversalClient, sortedSetKeyFormat string) (*SortedSet[T], error) {
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
	cr.markers = []string{markerBlankPage}
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

	if generation := cr.generation(); generation != "" {
		var count *redis.IntCmd
		if _, err := cr.guarded(ctx, generation, keyParams, func(pipe redis.Pipeliner) error {
			count = pipe.ZCard(ctx, key)
			return nil
		}); err != nil {
			return 0, err
		}
		return count.Result()
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

	var result []redis.Z
	var err error
	if generation := cr.generation(); generation != "" {
		var edge *redis.ZSliceCmd
		if _, err = cr.guarded(ctx, generation, keyParams, func(pipe redis.Pipeliner) error {
			edge = pipe.ZRangeWithScores(ctx, key, start, stop)
			return nil
		}); err == nil {
			result, err = edge.Result()
		}
	} else {
		result, err = cr.client.ZRangeWithScores(ctx, key, start, stop).Result()
	}
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
// a different route, shares the same item-loading path. With a generation, an index that is
// not this generation's answers ErrNotSeeded (the check has just dropped it).
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

	read := func(cmd redis.Cmdable) *redis.StringSliceCmd {
		if byScore {
			reqRange := redis.ZRangeBy{
				Min: formatScore(float64(start)),
				Max: formatScore(float64(stop)),
			}
			if direction == Descending {
				return cmd.ZRevRangeByScore(ctx, key, &reqRange)
			}
			return cmd.ZRangeByScore(ctx, key, &reqRange)
		}
		if direction == Descending {
			return cmd.ZRevRange(ctx, key, start, stop)
		}
		return cmd.ZRange(ctx, key, start, stop)
	}

	if generation := cr.generation(); generation != "" {
		return cr.guardedMembers(ctx, generation, keyParams, read)
	}

	result := read(cr.client)
	if result.Err() != nil {
		return nil, result.Err()
	}

	return result.Val(), nil
}

// guardedMembers runs one member read behind the generation check, answering ErrNotSeeded for an
// index that was not this generation's.
func (cr *SortedSet[T]) guardedMembers(ctx context.Context, generation string, keyParams []string, read func(cmd redis.Cmdable) *redis.StringSliceCmd) ([]string, error) {
	var result *redis.StringSliceCmd
	own, err := cr.guarded(ctx, generation, keyParams, func(pipe redis.Pipeliner) error {
		result = read(pipe)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !own {
		return nil, ErrNotSeeded
	}
	return result.Result()
}

// scoreRangeMembers reads the members of a score range with a LIMIT: ZRANGEBYSCORE, or
// ZREVRANGEBYSCORE for Descending. lower and upper are rendered bounds ("-inf", "+inf"
// or a formatted score). A count of zero answers without a query — go-redis drops a
// LIMIT of 0 0, which would return the whole range — and a negative count reads
// everything after offset.
func (cr *SortedSet[T]) scoreRangeMembers(
	ctx context.Context,
	direction string,
	lower string,
	upper string,
	offset int64,
	count int64,
	keyParams []string,
) ([]string, error) {
	if direction != Ascending && direction != Descending {
		return nil, fmt.Errorf("redifu: direction must be %q or %q, got %q", Ascending, Descending, direction)
	}
	if offset < 0 {
		return nil, fmt.Errorf("redifu: limit offset must not be negative, got %d", offset)
	}
	if count == 0 {
		return nil, nil
	}

	key, errKey := cr.key(keyParams)
	if errKey != nil {
		return nil, errKey
	}

	bounds := redis.ZRangeBy{Min: lower, Max: upper, Offset: offset, Count: count}
	read := func(cmd redis.Cmdable) *redis.StringSliceCmd {
		if direction == Descending {
			return cmd.ZRevRangeByScore(ctx, key, &bounds)
		}
		return cmd.ZRangeByScore(ctx, key, &bounds)
	}

	if generation := cr.generation(); generation != "" {
		return cr.guardedMembers(ctx, generation, keyParams, read)
	}

	result := read(cr.client)
	if result.Err() != nil {
		return nil, result.Err()
	}

	return result.Val(), nil
}

// rangeWithScores reads members by rank together with the score the index holds for
// each. Timeline needs the score of a page's last member to hand back a Cursor, and it
// has to be the stored score, not one recomputed from an item that may have changed. With a
// generation, an index that is not this generation's reads as a cold one (nothing): a Timeline
// read of it seeds.
func (cr *SortedSet[T]) rangeWithScores(
	ctx context.Context,
	direction string,
	start int64,
	stop int64,
	keyParams []string,
) ([]string, []float64, error) {
	if direction != Ascending && direction != Descending {
		return nil, nil, fmt.Errorf("redifu: direction must be %q or %q, got %q", Ascending, Descending, direction)
	}

	key, errKey := cr.key(keyParams)
	if errKey != nil {
		return nil, nil, errKey
	}

	read := func(cmd redis.Cmdable) *redis.ZSliceCmd {
		if direction == Descending {
			return cmd.ZRevRangeWithScores(ctx, key, start, stop)
		}
		return cmd.ZRangeWithScores(ctx, key, start, stop)
	}

	var result *redis.ZSliceCmd
	if generation := cr.generation(); generation != "" {
		if _, err := cr.guarded(ctx, generation, keyParams, func(pipe redis.Pipeliner) error {
			result = read(pipe)
			return nil
		}); err != nil {
			return nil, nil, err
		}
	} else {
		result = read(cr.client)
	}
	if result.Err() != nil {
		return nil, nil, result.Err()
	}

	members := make([]string, 0, len(result.Val()))
	scores := make([]float64, 0, len(result.Val()))
	for _, entry := range result.Val() {
		member, ok := entry.Member.(string)
		if !ok {
			return nil, nil, fmt.Errorf("redifu: unexpected member type %T", entry.Member)
		}
		members = append(members, member)
		scores = append(scores, entry.Score)
	}

	return members, scores, nil
}

// hydrate turns a list of randIds into items: one batched Base read, then one batched
// read per relation. It returns the items it could resolve and how many members the
// index held that Base no longer does.
//
// Those dangling members matter twice over. They are removed from the index here, so a
// collection converges instead of decaying, and the count is reported to the caller,
// so Timeline can tell "this is the end of the feed" apart from "three items in the
// middle of this page have expired".
//
// With generations, a member whose item holds a value of another generation counts as dangling
// too — this read cannot serve it — but is not removed: the item exists, and the index is right
// to hold it. A consumer that needs the whole list reseeds it (its seed writes this generation's
// values); removing the member would leave a hole that no later read refills.
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

	fetchedItems, foreign, errGetMany := baseClient.getManyReport(ctx, members)
	if errGetMany != nil {
		return nil, 0, errGetMany
	}

	items := make([]T, 0, len(members))
	dangling := 0
	gone := make([]interface{}, 0)

	for _, randId := range members {
		fetchedItem, found := fetchedItems[randId]
		if !found {
			dangling++
			if !foreign[randId] {
				gone = append(gone, randId)
			}
			continue
		}
		items = append(items, fetchedItem)
	}

	if len(gone) > 0 && cr.selfHeal {
		// Best effort. The read succeeded; failing it because the cleanup did not
		// would trade a working page for a housekeeping error. With a generation the
		// removal applies only to an index that is still this generation's: one that has
		// become another generation's since it was read is not this read's to change.
		if generation := cr.generation(); generation != "" {
			if keys, err := cr.generationKeys(keyParams); err == nil {
				args := make([]interface{}, 0, len(gone)+1)
				args = append(args, gone...)
				_ = generationHealScript.Eval(ctx, cr.client, keys, append(args, generation)...).Err()
			}
		} else if key, err := cr.key(keyParams); err == nil {
			cr.client.ZRem(ctx, key, gone...)
		}
	}

	if len(items) == 0 {
		return nil, dangling, nil
	}

	if errRelation := resolveRelations(ctx, cr.client, relations, items, relationDepth); errRelation != nil {
		return nil, dangling, errRelation
	}

	if processor != nil {
		for i := range items {
			processor(&items[i], processorArgs)
		}
	}

	return items, dangling, nil
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
