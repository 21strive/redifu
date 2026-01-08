package redifu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
	"time"
)

type Base[T item.Blueprint] struct {
	client        redis.UniversalClient
	itemKeyFormat string
	timeToLive    time.Duration
}

func (cr *Base[T]) Init(client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) {
	cr.client = client
	cr.itemKeyFormat = itemKeyFormat
	cr.timeToLive = timeToLive
}

func (cr *Base[T]) Get(ctx context.Context, keyParam string) (T, error) {
	var nilItem T
	key := fmt.Sprintf(cr.itemKeyFormat, keyParam)

	result := cr.client.Get(ctx, key)
	if result.Err() != nil {
		if errors.Is(result.Err(), redis.Nil) {
			return nilItem, redis.Nil
		}
		return nilItem, result.Err()
	}

	var fetchedItem T
	errorUnmarshal := json.Unmarshal([]byte(result.Val()), &fetchedItem)
	if errorUnmarshal != nil {
		return nilItem, errorUnmarshal
	}

	return fetchedItem, nil
}

func (cr *Base[T]) Set(ctx context.Context, pipe redis.Pipeliner, item T, keyParam ...string) error {
	if len(keyParam) > 1 {
		return errors.New("only accept one keyParam")
	}
	var key string
	if keyParam != nil {
		key = fmt.Sprintf(cr.itemKeyFormat, keyParam[0])
	} else {
		key = fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())
	}

	itemInByte, errorMarshalJson := json.Marshal(item)
	if errorMarshalJson != nil {
		return errorMarshalJson
	}

	valueAsString := string(itemInByte)
	pipe.Set(
		ctx,
		key,
		valueAsString,
		cr.timeToLive,
	)

	if keyParam != nil {
		cr.DelBlank(ctx, pipe, keyParam...)
	} else {
		cr.DelBlank(ctx, pipe, item.GetRandId())
	}

	return nil
}

func (cr *Base[T]) Upsert(ctx context.Context, item T, keyParam ...string) error {
	pipeline := cr.client.Pipeline()
	errSet := cr.Set(ctx, pipeline, item, keyParam...)
	if errSet != nil {
		return errSet
	}
	_, errPipe := pipeline.Exec(ctx)
	return errPipe
}

func (cr *Base[T]) Del(ctx context.Context, pipe redis.Pipeliner, item T, keyParam ...string) error {
	if len(keyParam) > 1 {
		return errors.New("only accept one keyParam")
	}
	var key string
	if keyParam != nil {
		key = fmt.Sprintf(cr.itemKeyFormat, keyParam[0])
	} else {
		key = fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())
	}

	deleteRedis := pipe.Del(
		ctx,
		key,
	)
	if deleteRedis.Err() != nil {
		return deleteRedis.Err()
	}

	return nil
}

func (cr *Base[T]) SetBlank(ctx context.Context, keyParam ...string) error {
	key := fmt.Sprintf(cr.itemKeyFormat, keyParam)
	key = key + ":blank"

	setBlank := cr.client.Set(
		ctx,
		key,
		1,
		cr.timeToLive,
	)

	if setBlank.Err() != nil {
		return setBlank.Err()
	}
	return nil
}

func (cr *Base[T]) IsBlank(ctx context.Context, keyParam ...string) (bool, error) {
	key := fmt.Sprintf(cr.itemKeyFormat, keyParam)
	key = key + ":blank"

	getBlank := cr.client.Get(ctx, key)
	if getBlank.Err() != nil {
		if errors.Is(getBlank.Err(), redis.Nil) {
			return false, nil
		}
		return false, getBlank.Err()
	}

	if getBlank.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Base[T]) DelBlank(ctx context.Context, pipe redis.Pipeliner, keyParam ...string) {
	key := fmt.Sprintf(cr.itemKeyFormat, keyParam)
	key = key + ":blank"

	pipe.Del(ctx, key)
}

func NewBase[T item.Blueprint](client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) *Base[T] {
	base := &Base[T]{}
	base.Init(client, itemKeyFormat, timeToLive)
	return base
}
