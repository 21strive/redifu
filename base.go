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

func (cr *Base[T]) Get(param string) (T, error) {
	var nilItem T
	key := fmt.Sprintf(cr.itemKeyFormat, param)

	result := cr.client.Get(context.TODO(), key)
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return nilItem, redis.Nil
		}
		return nilItem, result.Err()
	}

	var item T
	errorUnmarshal := json.Unmarshal([]byte(result.Val()), &item)
	if errorUnmarshal != nil {
		return nilItem, errorUnmarshal
	}

	return item, nil
}

func (cr *Base[T]) Set(pipe redis.Pipeliner, ctx context.Context, item T, param ...string) error {
	if len(param) > 1 {
		return errors.New("only accept one param")
	}
	var key string
	if param != nil {
		key = fmt.Sprintf(cr.itemKeyFormat, param[0])
	} else {
		key = fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())
	}

	itemInByte, errorMarshalJson := json.Marshal(item)
	if errorMarshalJson != nil {
		return errorMarshalJson
	}

	valueAsString := string(itemInByte)
	setRedis := pipe.Set(
		ctx,
		key,
		valueAsString,
		cr.timeToLive,
	)
	if setRedis.Err() != nil {
		return setRedis.Err()
	}

	if param != nil {
		cr.DelBlank(pipe, param...)
	} else {
		cr.DelBlank(pipe, item.GetRandId())
	}

	return nil
}

func (cr *Base[T]) Del(pipe redis.Pipeliner, ctx context.Context, item T, param ...string) error {
	if len(param) > 1 {
		return errors.New("only accept one param")
	}
	var key string
	if param != nil {
		key = fmt.Sprintf(cr.itemKeyFormat, param[0])
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

func (cr *Base[T]) SetBlank(param ...string) error {
	key := fmt.Sprintf(cr.itemKeyFormat, param)
	key = key + ":blank"

	setBlank := cr.client.Set(
		context.TODO(),
		key,
		1,
		cr.timeToLive,
	)

	if setBlank.Err() != nil {
		return setBlank.Err()
	}
	return nil
}

func (cr *Base[T]) IsBlank(param ...string) (bool, error) {
	key := fmt.Sprintf(cr.itemKeyFormat, param)
	key = key + ":blank"

	getBlank := cr.client.Get(context.TODO(), key)
	if getBlank.Err() != nil {
		if getBlank.Err() == redis.Nil {
			return false, nil
		}
		return false, getBlank.Err()
	}

	if getBlank.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Base[T]) DelBlank(pipe redis.Pipeliner, param ...string) error {
	key := fmt.Sprintf(cr.itemKeyFormat, param)
	key = key + ":blank"

	delBlank := pipe.Del(context.TODO(), key)
	if delBlank.Err() != nil {
		return delBlank.Err()
	}
	return nil
}

func NewBase[T item.Blueprint](client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) *Base[T] {
	base := &Base[T]{}
	base.Init(client, itemKeyFormat, timeToLive)
	return base
}
