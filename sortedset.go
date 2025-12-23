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

func (cr *SortedSet[T]) Init(client redis.UniversalClient, sortedSetKeyFormat string) {
	cr.client = client
	cr.sortedSetKeyFormat = sortedSetKeyFormat
}

func (cr *SortedSet[T]) SetSortedSet(pipe redis.Pipeliner, ctx context.Context, param []string, score float64, item T) {
	var key string
	if param == nil {
		key = cr.sortedSetKeyFormat
	} else {
		key = joinParam(cr.sortedSetKeyFormat, param)
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

func (cr *SortedSet[T]) SetExpiration(pipe redis.Pipeliner, ctx context.Context, param []string, timeToLive time.Duration) {
	var key string
	if param == nil {
		key = cr.sortedSetKeyFormat
	} else {
		key = joinParam(cr.sortedSetKeyFormat, param)
	}

	pipe.Expire(
		ctx,
		key,
		timeToLive,
	)
}

func (cr *SortedSet[T]) DeleteFromSortedSet(pipe redis.Pipeliner, ctx context.Context, param []string, item T) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

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

func (cr *SortedSet[T]) TotalItemOnSortedSet(param []string) int64 {
	key := joinParam(cr.sortedSetKeyFormat, param)

	getTotalItemSortedSet := cr.client.ZCard(context.TODO(), key)
	if getTotalItemSortedSet.Err() != nil {
		return 0
	}

	return getTotalItemSortedSet.Val()
}

func (cr *SortedSet[T]) DeleteSortedSet(pipe redis.Pipeliner, ctx context.Context, param []string) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeSortedSet := pipe.Del(ctx, key)
	if removeSortedSet.Err() != nil {
		return removeSortedSet.Err()
	}

	return nil
}

func (cr *SortedSet[T]) LowestScore(param []string) (float64, error) {
	key := joinParam(cr.sortedSetKeyFormat, param)

	result, err := cr.client.ZRangeWithScores(context.TODO(), key, 0, 0).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get lowest score: %w", err)
	}

	if len(result) == 0 {
		return 0, fmt.Errorf("sorted set is empty")
	}

	return result[0].Score, nil
}

func (cr *SortedSet[T]) HighestScore(param []string) (float64, error) {
	key := joinParam(cr.sortedSetKeyFormat, param)

	result, err := cr.client.ZRangeWithScores(context.TODO(), key, -1, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get highest score: %w", err)
	}

	if len(result) == 0 {
		return 0, fmt.Errorf("sorted set is empty")
	}

	return result[0].Score, nil
}

func (cr *SortedSet[T]) Fetch(
	baseClient *Base[T],
	param []string,
	direction string,
	processor func(item *T, args []interface{}),
	processorArgs []interface{},
	relation map[string]Relation,
	start int64,
	stop int64,
	byScore bool) ([]T, error) {
	var items []T

	if direction == "" {
		return nil, errors.New("must set direction!")
	}

	sortedSetKey := joinParam(cr.sortedSetKeyFormat, param)

	var result *redis.StringSliceCmd
	if byScore {
		reqRange := redis.ZRangeBy{
			Min: strconv.FormatInt(start, 10),
			Max: strconv.FormatInt(stop, 10),
		}
		if direction == Descending {
			result = cr.client.ZRevRangeByScore(context.TODO(), sortedSetKey, &reqRange)
		} else {
			result = cr.client.ZRangeByScore(context.TODO(), sortedSetKey, &reqRange)
		}
	} else {
		if direction == Descending {
			result = cr.client.ZRevRange(context.TODO(), sortedSetKey, start, stop)
		} else {
			result = cr.client.ZRange(context.TODO(), sortedSetKey, start, stop)
		}
	}

	if result.Err() != nil {
		return nil, result.Err()
	}
	listRandIds := result.Val()

	for i := 0; i < len(listRandIds); i++ {
		item, err := baseClient.Get(listRandIds[i])
		if err != nil {
			continue
		}

		if relation != nil {
			for _, relationFormat := range relation {
				v := reflect.ValueOf(item)

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

				relationItem, errGet := relationFormat.GetByRandId(relationRandId)
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
			processor(&item, processorArgs)
		}

		items = append(items, item)
	}

	return items, nil
}

func NewSortedSet[T item.Blueprint](client redis.UniversalClient, sortedSetKeyFormat string, timeToLive time.Duration) *SortedSet[T] {
	sorted := &SortedSet[T]{}
	sorted.Init(client, sortedSetKeyFormat)
	return sorted
}
