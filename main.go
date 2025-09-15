package redifu

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/21strive/item"
	sqlSeeder "github.com/21strive/redifu/seeder/sql"
	"github.com/21strive/redifu/types"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
	"time"
)

func NewBase[T item.Blueprint](client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) *types.Base[T] {
	base := &types.Base[T]{}
	base.Init(client, itemKeyFormat, timeToLive)
	return base
}

func NewSortedSet[T item.Blueprint](client redis.UniversalClient, sortedSetKeyFormat string, timeToLive time.Duration) *types.SortedSet[T] {
	return &types.SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: sortedSetKeyFormat,
		timeToLive:         timeToLive,
	}
}

func NewTimelineWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, itemPerPage int64, direction string, sortingReference string, timeToLive time.Duration) *types.Timeline[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := types.SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
		timeToLive:         timeToLive,
	}

	return &types.Timeline[T]{
		client:           client,
		baseClient:       baseClient,
		sortedSetClient:  &sortedSetClient,
		itemPerPage:      itemPerPage,
		direction:        direction,
		sortingReference: sortingReference,
	}
}

func NewTimeline[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) *types.Timeline[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := types.SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
		timeToLive:         timeToLive,
	}

	return &types.Timeline[T]{
		client:          client,
		baseClient:      baseClient,
		sortedSetClient: &sortedSetClient,
		itemPerPage:     itemPerPage,
		direction:       direction,
	}
}

func NewTimelineSQLSeeder[T types.SQLItemBlueprint](db *sql.DB, baseClient *types.Base[T], paginateClient *types.Timeline[T]) *sqlSeeder.TimelineSQLSeeder[T] {
	return sqlSeeder.NewTimelineSQLSeeder(db, baseClient, paginateClient)
}

func NewSortedWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, sortingReference string, timeToLive time.Duration) *types.Sorted[T] {
	sortedSetClient := &types.SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
		timeToLive:         timeToLive,
	}

	return &types.Sorted[T]{
		client:           client,
		baseClient:       baseClient,
		sortedSetClient:  sortedSetClient,
		sortingReference: sortingReference,
	}
}

func NewSorted[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, timeToLive time.Duration) *types.Sorted[T] {
	sortedSetClient := &types.SortedSet[T]{
		client:             client,
		sortedSetKeyFormat: keyFormat,
		timeToLive:         timeToLive,
	}

	return &types.Sorted[T]{
		client:          client,
		baseClient:      baseClient,
		sortedSetClient: sortedSetClient,
	}
}
