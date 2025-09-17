package types

import (
	"context"
	"errors"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type RequestLimiter[T item.Blueprint] struct {
	client     redis.UniversalClient
	name       string
	throughput int64
	duration   time.Duration
	processor  func(string) error
	errHandler func(error, string)
}

func (eq *RequestLimiter[T]) Init(client redis.UniversalClient, name string, throughput int64, duration time.Duration, processor func(string) error, errHandler func(error, string)) {
	eq.client = client
	eq.name = name
	eq.throughput = throughput
	eq.duration = duration
	eq.processor = processor
	eq.errHandler = errHandler
}

func (eq *RequestLimiter[T]) Add(ctx context.Context, item T) error {
	cmd := eq.client.LPush(ctx, eq.name, item.GetRandId())
	if cmd.Err() != nil {
		return cmd.Err()
	}

	return cmd.Err()
}

func (eq *RequestLimiter[T]) Count(ctx context.Context) (int64, error) {
	return eq.client.LLen(ctx, eq.name).Result()
}

func (eq *RequestLimiter[T]) Worker(ctx context.Context) {
	ticker := time.NewTicker(eq.duration)
	defer ticker.Stop()

	for range ticker.C {
		randId, err := eq.client.RPop(ctx, eq.name).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			eq.errHandler(err, "")
			continue
		}

		go func(randId string) {
			err := eq.processor(randId)
			if err != nil {
				errMsg := errors.New("Invocation RandId: " + randId + " failed: " + err.Error())
				eq.errHandler(errMsg, randId)
				errPush := eq.client.LPush(ctx, eq.name, randId)
				if errPush.Err() != nil {
					eq.errHandler(errors.New("Failed to push back randId: "+randId+" error: "+errPush.Err().Error()), randId)
				}
			}
		}(randId)
	}
}
