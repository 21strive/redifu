package redifu

import (
	"context"
	"errors"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

var ResetPagination = errors.New("reset pagination")

type Timeline[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	itemPerPage      int64
	direction        string
	sortingReference string
	relation         map[string]Relation
	timeToLive       time.Duration
}

func NewTimeline[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) *Timeline[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat)

	timeline := &Timeline[T]{
		relation: make(map[string]Relation), // Initialize the map
	}
	timeline.Init(client, baseClient, sortedSetClient, itemPerPage, direction, timeToLive)
	return timeline
}

func (cr *Timeline[T]) Init(client redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T], itemPerPage int64, direction string, timeToLive time.Duration) {
	cr.client = client
	cr.baseClient = baseClient
	cr.sortedSetClient = sortedSetClient
	cr.itemPerPage = itemPerPage
	cr.direction = direction
	cr.timeToLive = timeToLive
}

func (cr *Timeline[T]) AddRelation(identifier string, relationBase Relation) {
	if cr.relation == nil {
		cr.relation = make(map[string]Relation)
	}
	cr.relation[identifier] = relationBase
}

func (cr *Timeline[T]) GetRelation() map[string]Relation {
	return cr.relation
}

func (cr *Timeline[T]) SetSortingReference(sortingReference string) {
	cr.sortingReference = sortingReference
}

func (cr *Timeline[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	cr.sortedSetClient.SetExpiration(ctx, pipe, cr.timeToLive, keyParams...)
}

func (cr *Timeline[T]) GetItemPerPage() int64 {
	return cr.itemPerPage
}

func (cr *Timeline[T]) GetDirection() string {
	return cr.direction
}

func (cr *Timeline[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	_, errGet := cr.baseClient.Get(ctx, item.GetRandId())
	if errGet != nil && errGet != redis.Nil {
		return errGet
	}
	pipe := cr.client.Pipeline()

	if errGet == redis.Nil {
		errSet := cr.baseClient.Set(ctx, pipe, item)
		if errSet != nil {
			return errSet
		}
	}

	errIngest := cr.IngestItem(ctx, pipe, item, false, keyParams...)
	if errIngest != nil {
		return errIngest
	}
	_, errPipe := pipe.Exec(ctx)
	return errPipe
}

func (cr *Timeline[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, seed bool, keyParams ...string) error {
	if cr.direction == "" {
		return errors.New("must set direction!")
	}

	score, err := getItemScore(item, cr.sortingReference)
	if err != nil {
		return err
	}

	isFirstPage, err := cr.IsFirstPage(ctx, keyParams...)
	if err != nil {
		return err
	}

	isLastPage, err := cr.IsLastPage(ctx, keyParams...)
	if err != nil {
		return err
	}

	if !seed {
		isBlankPage, errGet := cr.IsBlankPage(ctx, keyParams...)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			cr.DelBlankPage(ctx, pipe, keyParams...)
		}

		if cr.direction == Descending {
			elementCount := cr.sortedSetClient.Count(ctx, keyParams...)
			if elementCount > 0 {
				lowestScore, err := cr.sortedSetClient.LowestScore(ctx, keyParams...)
				if err != nil {
					return err
				}

				if score >= lowestScore {
					if elementCount == cr.itemPerPage && isFirstPage {
						cr.DelFirstPage(ctx, pipe, keyParams...)
					}
					cr.sortedSetClient.SetItem(ctx, pipe, score, item, keyParams...)
				}
			}
		} else if cr.direction == Ascending {
			elementCount := cr.sortedSetClient.Count(ctx, keyParams...)
			if elementCount > 0 {
				highestScore, err := cr.sortedSetClient.HighestScore(ctx, keyParams...)
				if err != nil {
					return err
				}

				if score <= highestScore {
					if elementCount == cr.itemPerPage && isFirstPage {
						cr.DelFirstPage(ctx, pipe, keyParams...)
					}
					if isFirstPage || isLastPage {
						cr.sortedSetClient.SetItem(ctx, pipe, score, item, keyParams...)
					}
				}
			}
		}
	} else {
		cr.sortedSetClient.SetItem(ctx, pipe, score, item, keyParams...)
	}

	return nil
}

func (cr *Timeline[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	pipe := cr.client.Pipeline()

	errDelBase := cr.baseClient.Del(ctx, pipe, item)
	if errDelBase != nil {
		return errDelBase
	}

	err := cr.sortedSetClient.RemoveItem(ctx, pipe, item, keyParams...)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}

	isFirstPage, errFirstPage := cr.IsFirstPage(ctx, keyParams...)
	if errFirstPage != nil {
		return errFirstPage
	}
	if isFirstPage {
		numItem := cr.sortedSetClient.Count(ctx, keyParams...) // O(log(n))
		if numItem == 0 {
			cr.DelFirstPage(ctx, pipe, keyParams...)
		}
	}

	isLastPage, errLastPage := cr.IsLastPage(ctx, keyParams...)
	if errLastPage != nil {
		return errLastPage
	}
	if isLastPage {
		numItem := cr.sortedSetClient.Count(ctx, keyParams...)
		if numItem == 0 {
			cr.DelLastPage(ctx, pipe, keyParams...)
		}
	}

	_, errPipe := pipe.Exec(ctx)
	return errPipe
}

func (cr *Timeline[T]) IsFirstPage(ctx context.Context, keyParams ...string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	firstPageKey := sortedSetKey + ":firstpage"

	getFirstPageKey := cr.client.Get(ctx, firstPageKey)
	if getFirstPageKey.Err() != nil {
		if getFirstPageKey.Err() == redis.Nil {
			return false, nil
		} else {
			return false, getFirstPageKey.Err()
		}
	}

	if getFirstPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Timeline[T]) SetFirstPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	firstPageKey := sortedSetKey + ":firstpage"

	pipe.Set(
		ctx,
		firstPageKey,
		1,
		cr.timeToLive,
	)
}

func (cr *Timeline[T]) DelFirstPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	firstPageKey := sortedSetKey + ":firstpage"

	pipe.Del(ctx, firstPageKey)
}

func (cr *Timeline[T]) IsLastPage(ctx context.Context, keyParams ...string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":lastpage"

	getLastPageKey := cr.client.Get(ctx, lastPageKey)
	if getLastPageKey.Err() != nil {
		if getLastPageKey.Err() == redis.Nil {
			return false, nil
		} else {
			return false, getLastPageKey.Err()
		}
	}

	if getLastPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Timeline[T]) SetLastPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":lastpage"

	pipe.Set(
		ctx,
		lastPageKey,
		1,
		cr.timeToLive,
	)
}

func (cr *Timeline[T]) DelLastPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":lastpage"

	pipe.Del(ctx, lastPageKey)
}

func (cr *Timeline[T]) IsBlankPage(ctx context.Context, keyParams ...string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	blankPageKey := sortedSetKey + ":blankpage"

	getLastPageKey := cr.client.Get(ctx, blankPageKey)
	if getLastPageKey.Err() != nil {
		if getLastPageKey.Err() == redis.Nil {
			return false, nil
		} else {
			return false, getLastPageKey.Err()
		}
	}

	if getLastPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Timeline[T]) SetBlankPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":blankpage"

	pipe.Set(
		ctx,
		lastPageKey,
		1,
		cr.timeToLive,
	)
}

func (cr *Timeline[T]) DelBlankPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":blankpage"

	pipe.Del(ctx, lastPageKey)
}

func (cr *Timeline[T]) Fetch(ctx context.Context, lastRandId []string) *timelineFetchBuilder[T] {
	return &timelineFetchBuilder[T]{
		mainCtx:       ctx,
		timeline:      cr,
		lastRandIds:   lastRandId,
		params:        nil,
		processor:     nil,
		processorArgs: nil,
		fetchAll:      false,
	}
}

func (cr *Timeline[T]) FetchAll(ctx context.Context) *timelineFetchBuilder[T] {
	return &timelineFetchBuilder[T]{
		mainCtx:       ctx,
		timeline:      cr,
		lastRandIds:   nil,
		params:        nil,
		processor:     nil,
		processorArgs: nil,
		fetchAll:      true,
	}
}

func (cr *Timeline[T]) Remove(ctx context.Context) *timelineRemovalBuilder[T] {
	return &timelineRemovalBuilder[T]{
		mainCtx:  ctx,
		timeline: cr,
		params:   nil,
		purge:    false,
	}
}

func (cr *Timeline[T]) Purge(ctx context.Context) *timelineRemovalBuilder[T] {
	return &timelineRemovalBuilder[T]{
		mainCtx:  ctx,
		timeline: cr,
		params:   nil,
		purge:    true,
	}
}

type timelineFetchBuilder[T item.Blueprint] struct {
	mainCtx       context.Context
	timeline      *Timeline[T]
	lastRandIds   []string
	params        []string
	processor     func(*T, []interface{})
	processorArgs []interface{}
	fetchAll      bool
}

func (b *timelineFetchBuilder[T]) WithParams(params ...string) *timelineFetchBuilder[T] {
	b.params = params
	return b
}

func (b *timelineFetchBuilder[T]) WithProcessor(processor func(*T, []interface{}), args ...interface{}) *timelineFetchBuilder[T] {
	b.processor = processor
	b.processorArgs = args
	return b
}

func (b *timelineFetchBuilder[T]) Exec() ([]T, string, string, error) {
	var items []T
	var validLastRandId string
	var position string

	// safety net
	if b.timeline.direction == "" {
		return nil, validLastRandId, position, errors.New("must set direction!")
	}

	sortedSetKey := joinParam(b.timeline.sortedSetClient.sortedSetKeyFormat, b.params)
	start := int64(0)
	stop := b.timeline.itemPerPage - 1

	for i := len(b.lastRandIds) - 1; i >= 0; i-- {
		count, errZCard := b.timeline.client.ZCard(b.mainCtx, sortedSetKey).Result()
		if errZCard != nil {
			return nil, validLastRandId, position, errZCard
		}
		if count == 0 {
			return nil, validLastRandId, position, ResetPagination
		}

		item, err := b.timeline.baseClient.Get(b.mainCtx, b.lastRandIds[i])
		if err != nil {
			continue
		}

		var rank *redis.IntCmd
		if b.timeline.direction == Descending {
			rank = b.timeline.client.ZRevRank(b.mainCtx, sortedSetKey, item.GetRandId())
		} else {
			rank = b.timeline.client.ZRank(b.mainCtx, sortedSetKey, item.GetRandId())
		}

		if rank.Err() == nil {
			validLastRandId = item.GetRandId()
			start = rank.Val() + 1
			stop = start + b.timeline.itemPerPage - 1
			break
		}
	}

	var listRandIds []string

	if b.fetchAll {
		start = 0
		stop = -1
	}

	items, errFetch := b.timeline.sortedSetClient.Fetch(
		b.mainCtx,
		b.timeline.baseClient,
		b.timeline.direction,
		b.processor,
		b.processorArgs,
		b.timeline.relation,
		start,
		stop,
		false,
		b.params...)
	if errFetch != nil {
		return nil, validLastRandId, position, errFetch
	}

	if start == 0 {
		position = FirstPage
	} else if int64(len(listRandIds)) < b.timeline.itemPerPage {
		position = LastPage
	} else {
		position = MiddlePage
	}

	return items, validLastRandId, position, nil
}

func (b *Timeline[T]) RequiresSeeding(ctx context.Context, totalItems int64, keyParams ...string) (bool, error) {
	sortedSetKey := joinParam(b.sortedSetClient.sortedSetKeyFormat, keyParams)
	count, errZCard := b.client.ZCard(ctx, sortedSetKey).Result()
	if errZCard != nil {
		return false, errZCard
	}
	if count == 0 {
		pipeline := b.client.Pipeline()

		b.DelLastPage(ctx, pipeline, keyParams...)
		b.DelFirstPage(ctx, pipeline, keyParams...)

		_, errPipe := pipeline.Exec(ctx)
		if errPipe != nil {
			return false, errPipe
		}
	}

	isBlankPage, err := b.IsBlankPage(ctx, keyParams...)
	if err != nil {
		return false, err
	}

	isFirstPage, err := b.IsFirstPage(ctx, keyParams...)
	if err != nil {
		return false, err
	}

	isLastPage, err := b.IsLastPage(ctx, keyParams...)
	if err != nil {
		return false, err
	}

	if !isBlankPage && !isFirstPage && !isLastPage && totalItems < b.itemPerPage {
		return true, nil
	} else {
		return false, nil
	}
}

type timelineRemovalBuilder[T item.Blueprint] struct {
	mainCtx  context.Context
	timeline *Timeline[T]
	params   []string
	purge    bool
}

func (t *timelineRemovalBuilder[T]) WithParams(params ...string) *timelineRemovalBuilder[T] {
	t.params = params
	return t
}

func (t *timelineRemovalBuilder[T]) Exec() error {
	pipe := t.timeline.client.Pipeline()

	if t.purge {
		items, _, _, err := t.timeline.FetchAll(t.mainCtx).WithParams(t.params...).Exec()
		if err != nil {
			return err
		}

		for _, item := range items {
			t.timeline.baseClient.Del(t.mainCtx, pipe, item)
		}
	}

	err := t.timeline.sortedSetClient.Delete(t.mainCtx, pipe, t.params...)
	if err != nil {
		return err
	}

	t.timeline.DelFirstPage(t.mainCtx, pipe, t.params...)
	t.timeline.DelLastPage(t.mainCtx, pipe, t.params...)
	t.timeline.DelBlankPage(t.mainCtx, pipe, t.params...)

	_, errPipe := pipe.Exec(t.mainCtx)
	return errPipe
}
