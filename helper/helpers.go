package helper

import (
	"context"
	"errors"
	"fmt"
	"github.com/21strive/item"
	"github.com/21strive/redifu/definition"
	"github.com/21strive/redifu/types"
	"github.com/redis/go-redis/v9"
	"reflect"
	"time"
)

func FetchAll[T item.Blueprint](redisClient redis.UniversalClient, baseClient *types.Base[T], sortedSetClient *types.SortedSet[T], param []string, direction string, timeToLive time.Duration, processor func(item *T, args []interface{}), processorArgs []interface{}) ([]T, error) {
	var items []T
	var extendTTL bool

	if direction == "" {
		return nil, errors.New("must set direction!")
	}

	sortedSetKey := JoinParam(sortedSetClient.sortedSetKeyFormat, param)

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

func GetItemScore[T item.Blueprint](item T, sortingReference string) (float64, error) {
	if sortingReference == "" || sortingReference == "createdAt" {
		if scorer, ok := interface{}(item).(interface{ GetCreatedAt() time.Time }); ok {
			return float64(scorer.GetCreatedAt().UnixMilli()), nil
		}
	}

	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return 0, errors.New("getItemScore: item must be a struct or pointer to struct")
	}

	field := val.FieldByName(sortingReference)
	if !field.IsValid() {
		return 0, fmt.Errorf("getItemScore: field %s not found in item", sortingReference)
	}

	switch field.Type() {
	case reflect.TypeOf(time.Time{}):
		return float64(field.Interface().(time.Time).UnixMilli()), nil
	case reflect.TypeOf(&time.Time{}):
		if field.IsNil() {
			return 0, errors.New("getItemScore: time field is nil")
		}
		return float64(field.Interface().(*time.Time).UnixMilli()), nil
	case reflect.TypeOf(int64(0)):
		return float64(field.Interface().(int64)), nil
	default:
		return 0, fmt.Errorf("getItemScore: field %s is not a time.Time", sortingReference)
	}
}

func JoinParam(keyFormat string, param []string) string {
	interfaces := make([]interface{}, len(param))
	for i, v := range param {
		interfaces[i] = v
	}
	sortedSetKey := fmt.Sprintf(keyFormat, interfaces...)
	return sortedSetKey
}
