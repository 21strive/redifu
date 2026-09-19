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

func (bw *BaseWithPipeline[T]) SetIfAbsent(ctx context.Context, item T) error {
	return bw.baseClient.setIfAbsent(ctx, bw.pipe, item)
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

// GetMany fetches every randId in one round-trip and returns the items that exist,
// keyed by randId. A key that is missing is simply absent from the map — that is not
// an error, since an index may outlive an individual item.
func (cr *Base[T]) GetMany(ctx context.Context, randIds []string) (map[string]T, error) {
	if len(randIds) == 0 {
		return map[string]T{}, nil
	}

	pipe := cr.client.Pipeline()
	resolve := cr.stageGetMany(ctx, pipe, randIds)

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	return resolve()
}

// stageGetMany enqueues one GET per randId into the caller's pipeline and returns a
// resolver that reads the replies once the caller has executed it. A pipeline of GETs
// rather than MGET keeps this correct on Redis Cluster, where the keys may live in
// different slots.
func (cr *Base[T]) stageGetMany(ctx context.Context, pipe redis.Pipeliner, randIds []string) func() (map[string]T, error) {
	commands := make(map[string]*redis.StringCmd, len(randIds))

	for _, randId := range randIds {
		if _, staged := commands[randId]; staged {
			continue
		}
		commands[randId] = pipe.Get(ctx, fmt.Sprintf(cr.itemKeyFormat, randId))
	}

	return func() (map[string]T, error) {
		items := make(map[string]T, len(commands))

		for randId, command := range commands {
			value, err := command.Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				return nil, err
			}

			var fetchedItem T
			if errUnmarshal := json.Unmarshal([]byte(value), &fetchedItem); errUnmarshal != nil {
				return nil, errUnmarshal
			}

			items[randId] = fetchedItem
		}

		return items, nil
	}
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

// SetIfAbsent writes the item only when its key does not exist yet, but always refreshes
// the key's TTL. AddItem uses it so that placing an existing item into an index can never
// leave that index pointing at a key which expires before the index itself, while an item
// passed in as a stub can never overwrite the full value already held in Base.
func (cr *Base[T]) SetIfAbsent(ctx context.Context, item T) error {
	return cr.setIfAbsent(ctx, nil, item)
}

func (cr *Base[T]) setIfAbsent(ctx context.Context, pipe redis.Pipeliner, item T) error {
	key := fmt.Sprintf(cr.itemKeyFormat, item.GetRandId())

	itemInByte, errorMarshalJson := json.Marshal(item)
	if errorMarshalJson != nil {
		return errorMarshalJson
	}

	valueAsString := string(itemInByte)

	if pipe != nil {
		pipe.SetNX(ctx, key, valueAsString, cr.timeToLive)
		pipe.Expire(ctx, key, cr.timeToLive)
	} else {
		setRes := cr.client.SetNX(ctx, key, valueAsString, cr.timeToLive)
		if setRes.Err() != nil {
			return setRes.Err()
		}
		expireRes := cr.client.Expire(ctx, key, cr.timeToLive)
		if expireRes.Err() != nil {
			return expireRes.Err()
		}
	}

	return cr.UnmarkMissing(ctx, pipe, item.GetRandId())
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
	key := joinParam(cr.itemKeyFormat, keyParam)
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
	key := joinParam(cr.itemKeyFormat, keyParam)
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
	key := joinParam(cr.itemKeyFormat, keyParam)
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
