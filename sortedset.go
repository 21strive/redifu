package redifu

import (
	"context"
	"fmt"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type SortedSet[T item.Blueprint] struct {
	client             redis.UniversalClient
	sortedSetKeyFormat string
	timeToLive         time.Duration
}

func (cr *SortedSet[T]) Init(client redis.UniversalClient, sortedSetKeyFormat string, timeToLive time.Duration) {
	cr.client = client
	cr.sortedSetKeyFormat = sortedSetKeyFormat
	cr.timeToLive = timeToLive
}

func (cr *SortedSet[T]) SetSortedSet(param []string, score float64, item T) error {
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

	setSortedSet := cr.client.ZAdd(
		context.TODO(),
		key,
		sortedSetMember)
	if setSortedSet.Err() != nil {
		return setSortedSet.Err()
	}

	setExpire := cr.client.Expire(
		context.TODO(),
		key,
		cr.timeToLive,
	)
	if !setExpire.Val() {
		return setExpire.Err()
	}

	return nil
}

func (cr *SortedSet[T]) DeleteFromSortedSet(param []string, item T) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeFromSortedSet := cr.client.ZRem(
		context.TODO(),
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

func (cr *SortedSet[T]) DeleteSortedSet(param []string) error {
	key := joinParam(cr.sortedSetKeyFormat, param)

	removeSortedSet := cr.client.Del(context.TODO(), key)
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

func NewSortedSet[T item.Blueprint](client redis.UniversalClient, sortedSetKeyFormat string, timeToLive time.Duration) *SortedSet[T] {
	sorted := &SortedSet[T]{}
	sorted.Init(client, sortedSetKeyFormat, timeToLive)
	return sorted
}
