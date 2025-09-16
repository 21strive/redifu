package redifu

import (
	"database/sql"
	"time"

	"github.com/21strive/item"
	"github.com/21strive/redifu/definition"
	sqlSeeder "github.com/21strive/redifu/seeder/sql"
	"github.com/21strive/redifu/types"
	"github.com/redis/go-redis/v9"
)

func NewBase[T item.Blueprint](client redis.UniversalClient, itemKeyFormat string, timeToLive time.Duration) *types.Base[T] {
	base := &types.Base[T]{}
	base.Init(client, itemKeyFormat, timeToLive)
	return base
}

func NewSortedSet[T item.Blueprint](client redis.UniversalClient, sortedSetKeyFormat string, timeToLive time.Duration) *types.SortedSet[T] {
	sorted := &types.SortedSet[T]{}
	sorted.Init(client, sortedSetKeyFormat, timeToLive)
	return sorted
}

func NewTimelineWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, itemPerPage int64, direction string, sortingReference string, timeToLive time.Duration) *types.Timeline[T] {
	if direction != definition.Ascending && direction != definition.Descending {
		direction = definition.Descending
	}

	sortedSetClient := types.SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	timeline := &types.Timeline[T]{}
	timeline.Init(client, baseClient, &sortedSetClient, itemPerPage, direction)
	timeline.SetSortingReference(sortingReference)
	return timeline
}

func NewTimeline[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) *types.Timeline[T] {
	if direction != definition.Ascending && direction != definition.Descending {
		direction = definition.Descending
	}

	sortedSetClient := &types.SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	timeline := &types.Timeline[T]{}
	timeline.Init(client, baseClient, sortedSetClient, itemPerPage, direction)
	return timeline
}

func NewTimelineSQLSeeder[T types.SQLItemBlueprint](db *sql.DB, baseClient *types.Base[T], paginateClient *types.Timeline[T]) *sqlSeeder.TimelineSQLSeeder[T] {
	return sqlSeeder.NewTimelineSQLSeeder(db, baseClient, paginateClient)
}

func NewSortedWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, sortingReference string, timeToLive time.Duration) *types.Sorted[T] {
	sortedSetClient := &types.SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	sorted := &types.Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	sorted.SetSortingReference(sortingReference)
	return sorted
}

func NewSorted[T item.Blueprint](client redis.UniversalClient, baseClient *types.Base[T], keyFormat string, timeToLive time.Duration) *types.Sorted[T] {
	sortedSetClient := &types.SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	sorted := &types.Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	return sorted
}
