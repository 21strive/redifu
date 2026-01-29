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

type BaseWithPipeline[T item.Blueprint] struct {
	baseClient *Base[T]
	pipe       redis.Pipeliner
}

func (bw *BaseWithPipeline[T]) Set(ctx context.Context, item T, keyParams ...string) error {
	return bw.baseClient.set(ctx, bw.pipe, item, keyParams...)
}

func (bw *BaseWithPipeline[T]) Del(ctx context.Context, item T, keyParams ...string) error {
	return bw.baseClient.del(ctx, bw.pipe, item, keyParams...)
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

func (cr *Base[T]) Set(ctx context.Context, item T, keyParam ...string) error {
	return cr.set(ctx, nil, item, keyParam...)
}

func (cr *Base[T]) set(ctx context.Context, pipe redis.Pipeliner, item T, keyParam ...string) error {
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

	if pipe != nil {
		pipe.Set(
			ctx,
			key,
			valueAsString,
			cr.timeToLive,
		)
	} else {
		delRes := cr.client.Set(
			ctx,
			key,
			valueAsString,
			cr.timeToLive,
		)
		if delRes.Err() != nil {
			return delRes.Err()
		}
	}

	if keyParam != nil {
		cr.UnmarkMissing(ctx, pipe, keyParam...)
	} else {
		errUnmarkMissing := cr.UnmarkMissing(ctx, pipe, item.GetRandId())
		if errUnmarkMissing != nil {
			return errUnmarkMissing
		}
	}

	return nil
}

func (cr *Base[T]) Del(ctx context.Context, item T, keyParam ...string) error {
	return cr.del(ctx, nil, item, keyParam...)
}

func (cr *Base[T]) del(ctx context.Context, pipe redis.Pipeliner, item T, keyParam ...string) error {
	if len(keyParam) > 1 {
		return errors.New("only accept one keyParam")
	}
	var key string
	if keyParam != nil {
		key = fmt.Sprintf(cr.itemKeyFormat, keyParam[0])
	} else {
		key = fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())
	}

	if pipe != nil {
		pipe.Del(
			ctx,
			key,
		)
	} else {
		delRes := cr.client.Del(ctx, key)
		if delRes.Err() != nil {
			return delRes.Err()
		}
	}

	return nil
}

func (cr *Base[T]) WithPipeline(pipe redis.Pipeliner) *BaseWithPipeline[T] {
	return &BaseWithPipeline[T]{
		baseClient: cr,
		pipe:       pipe,
	}
}

func (cr *Base[T]) MarkAsMissing(ctx context.Context, keyParam ...string) error {
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

func (cr *Base[T]) IsMissing(ctx context.Context, keyParam ...string) (bool, error) {
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

func (cr *Base[T]) UnmarkMissing(ctx context.Context, pipe redis.Pipeliner, keyParam ...string) error {
	key := fmt.Sprintf(cr.itemKeyFormat, keyParam)
	key = key + ":blank"

	if pipe != nil {
		pipe.Del(ctx, key)
	} else {
		delRes := cr.client.Del(ctx, key)
		if delRes.Err() != nil {
			return delRes.Err()
		}
	}

	return nil
}

func (cr *Base[T]) Exists(ctx context.Context, keyParam ...string) error {
	return cr.UnmarkMissing(ctx, nil, keyParam...)
}

func NewBase[T item.Blueprint](client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) *Base[T] {
	base := &Base[T]{}
	base.Init(client, itemKeyFormat, timeToLive)
	return base
}
