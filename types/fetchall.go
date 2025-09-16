package types

import (
	"context"
	"errors"
	"time"

	"github.com/21strive/item"
	"github.com/21strive/redifu/definition"
	"github.com/21strive/redifu/helper"
	"github.com/redis/go-redis/v9"
)

func FetchAll[T item.Blueprint](redisClient redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T], param []string, direction string, timeToLive time.Duration, processor func(item *T, args []interface{}), processorArgs []interface{}) ([]T, error) {
	var items []T
	var extendTTL bool

	if direction == "" {
		return nil, errors.New("must set direction!")
	}

	sortedSetKey := helper.JoinParam(sortedSetClient.sortedSetKeyFormat, param)

	var result *redis.StringSliceCmd
	if direction == definition.Descending {
		result = redisClient.ZRevRange(context.TODO(), sortedSetKey, 0, -1)
	} else {
		result = redisClient.ZRange(context.TODO(), sortedSetKey, 0, -1)
	}

	if result.Err() != nil {
		return nil, result.Err()
	}
	listRandIds := result.Val()

	for i := 0; i < len(listRandIds); i++ {
		if !extendTTL {
			extendTTL = true
		}

		item, err := baseClient.Get(listRandIds[i])
		if err != nil {
			continue
		}

		if processor != nil {
			processor(&item, processorArgs)
		}

		items = append(items, item)
	}

	if extendTTL {
		redisClient.Expire(context.TODO(), sortedSetKey, timeToLive)
	}

	return items, nil
}
