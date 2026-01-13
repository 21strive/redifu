package redifu

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type SortedSet[T item.Blueprint] struct {
	client             redis.UniversalClient
	sortedSetKeyFormat string
}

func NewSortedSet[T item.Blueprint](client redis.UniversalClient, sortedSetKeyFormat string) *SortedSet[T] {
	sorted := &SortedSet[T]{}
	sorted.Init(client, sortedSetKeyFormat)
	return sorted
}

func (cr *SortedSet[T]) Init(client redis.UniversalClient, sortedSetKeyFormat string) {
	cr.client = client
	cr.sortedSetKeyFormat = sortedSetKeyFormat
}

func (cr *SortedSet[T]) SetItem(ctx context.Context, pipe redis.Pipeliner, score float64, item T, keyParams ...string) {
	var key string
	if keyParams == nil {
		key = cr.sortedSetKeyFormat
	} else {
		key = joinParam(cr.sortedSetKeyFormat, keyParams)
	}

	sortedSetMember := redis.Z{
		Score:  score,
		Member: item.GetRandId(),
	}

	pipe.ZAdd(
		ctx,
		key,
		sortedSetMember)
}

func (cr *SortedSet[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, timeToLive time.Duration, keyParams ...string) {
	var key string
	if keyParams == nil {
		key = cr.sortedSetKeyFormat
	} else {
		key = joinParam(cr.sortedSetKeyFormat, keyParams)
	}

	pipe.Expire(
		ctx,
		key,
		timeToLive,
	)
}

func (cr *SortedSet[T]) RemoveItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	key := joinParam(cr.sortedSetKeyFormat, keyParams)

	removeFromSortedSet := pipe.ZRem(
		ctx,
		key,
		item.GetRandId(),
	)
	if removeFromSortedSet.Err() != nil {
		return removeFromSortedSet.Err()
	}

	return nil
}

func (cr *SortedSet[T]) Count(ctx context.Context, keyParams ...string) int64 {
	key := joinParam(cr.sortedSetKeyFormat, keyParams)

	getTotalItemSortedSet := cr.client.ZCard(ctx, key)
	if getTotalItemSortedSet.Err() != nil {
		return 0
	}

	return getTotalItemSortedSet.Val()
}

func (cr *SortedSet[T]) Delete(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	key := joinParam(cr.sortedSetKeyFormat, keyParams)

	removeSortedSet := pipe.Del(ctx, key)
	if removeSortedSet.Err() != nil {
		return removeSortedSet.Err()
	}

	return nil
}

func (cr *SortedSet[T]) LowestScore(ctx context.Context, keyParams ...string) (float64, error) {
	key := joinParam(cr.sortedSetKeyFormat, keyParams)

	result, err := cr.client.ZRangeWithScores(ctx, key, 0, 0).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get lowest score: %w", err)
	}

	if len(result) == 0 {
		return 0, fmt.Errorf("sorted set is empty")
	}

	return result[0].Score, nil
}

func (cr *SortedSet[T]) HighestScore(ctx context.Context, keyParams ...string) (float64, error) {
	key := joinParam(cr.sortedSetKeyFormat, keyParams)

	result, err := cr.client.ZRangeWithScores(ctx, key, -1, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get highest score: %w", err)
	}

	if len(result) == 0 {
		return 0, fmt.Errorf("sorted set is empty")
	}

	return result[0].Score, nil
}

func (cr *SortedSet[T]) Fetch(
	ctx context.Context,
	baseClient *Base[T],
	direction string,
	processor func(item *T, args []interface{}),
	processorArgs []interface{},
	relation map[string]Relation,
	start int64,
	stop int64,
	byScore bool,
	keyParams ...string) ([]T, error) {
	var items []T
	if direction == "" {
		return nil, errors.New("must set direction!")
	}
	sortedSetKey := joinParam(cr.sortedSetKeyFormat, keyParams)

	var result *redis.StringSliceCmd
	if byScore {
		reqRange := redis.ZRangeBy{
			Min: strconv.FormatInt(start, 10),
			Max: strconv.FormatInt(stop, 10),
		}
		if direction == Descending {
			result = cr.client.ZRevRangeByScore(ctx, sortedSetKey, &reqRange)
		} else {
			result = cr.client.ZRangeByScore(ctx, sortedSetKey, &reqRange)
		}
	} else {
		if direction == Descending {
			result = cr.client.ZRevRange(ctx, sortedSetKey, start, stop)
		} else {
			result = cr.client.ZRange(ctx, sortedSetKey, start, stop)
		}
	}

	if result.Err() != nil {
		return nil, result.Err()
	}
	listRandIds := result.Val()

	for i := 0; i < len(listRandIds); i++ {
		fetchedItem, err := baseClient.Get(ctx, listRandIds[i])
		if err != nil {
			continue
		}

		if relation != nil {
			for _, relationFormat := range relation {
				v := reflect.ValueOf(fetchedItem)

				if v.Kind() == reflect.Ptr {
					v = v.Elem()
				}

				relationRandIdField := v.FieldByName(relationFormat.GetRandIdAttribute())
				if !relationRandIdField.IsValid() {
					continue
				}

				relationRandId := relationRandIdField.String()
				if relationRandId == "" {
					continue
				}

				relationItem, errGet := relationFormat.GetByRandId(ctx, relationRandId)
				if errGet != nil {
					continue
				}

				relationAttrField := v.FieldByName(relationFormat.GetItemAttribute())
				if !relationAttrField.IsValid() || !relationAttrField.CanSet() {
					continue
				}

				relationAttrField.Set(reflect.ValueOf(relationItem))
				if relationRandIdField.CanSet() {
					relationRandIdField.SetString("")
				}
			}
		}
		if processor != nil {
			processor(&fetchedItem, processorArgs)
		}

		items = append(items, fetchedItem)
	}

	return items, nil
}
