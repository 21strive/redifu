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

func (cr *Timeline[T]) SetExpiration(pipe redis.Pipeliner, pipeCtx context.Context, sortedSetParam []string) {
	cr.sortedSetClient.SetExpiration(pipe, pipeCtx, sortedSetParam, cr.timeToLive)
}

func (cr *Timeline[T]) GetItemPerPage() int64 {
	return cr.itemPerPage
}

func (cr *Timeline[T]) GetDirection() string {
	return cr.direction
}

func (cr *Timeline[T]) AddItem(item T, keyParam []string) error {
	_, errGet := cr.baseClient.Get(item.GetRandId())
	if errGet != nil && errGet != redis.Nil {
		return errGet
	}

	pipeCtx := context.Background()
	pipe := cr.client.Pipeline()

	if errGet == redis.Nil {
		errSet := cr.baseClient.Set(pipe, pipeCtx, item)
		if errSet != nil {
			return errSet
		}
	}

	errIngest := cr.IngestItem(pipe, pipeCtx, item, keyParam, false)
	if errIngest != nil {
		return errIngest
	}
	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
}

func (cr *Timeline[T]) IngestItem(pipe redis.Pipeliner, pipeCtx context.Context, item T, keyParam []string, seed bool) error {
	if cr.direction == "" {
		return errors.New("must set direction!")
	}

	score, err := getItemScore(item, cr.sortingReference)
	if err != nil {
		return err
	}

	isFirstPage, err := cr.IsFirstPage(keyParam)
	if err != nil {
		return err
	}

	isLastPage, err := cr.IsLastPage(keyParam)
	if err != nil {
		return err
	}

	if !seed {
		isBlankPage, errGet := cr.IsBlankPage(keyParam)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			cr.DelBlankPage(pipe, pipeCtx, keyParam)
		}

		if cr.direction == Descending {
			if cr.sortedSetClient.Count(keyParam) > 0 {
				lowestScore, err := cr.sortedSetClient.LowestScore(keyParam)
				if err != nil {
					return err
				}

				if score >= lowestScore {
					if cr.sortedSetClient.Count(keyParam) == cr.itemPerPage && isFirstPage {
						cr.DelFirstPage(pipe, pipeCtx, keyParam)
					}
					cr.sortedSetClient.SetItem(pipe, pipeCtx, keyParam, score, item)
				}
			}
		} else if cr.direction == Ascending {
			if cr.sortedSetClient.Count(keyParam) > 0 {
				highestScore, err := cr.sortedSetClient.HighestScore(keyParam)
				if err != nil {
					return err
				}

				if score <= highestScore {
					if cr.sortedSetClient.Count(keyParam) == cr.itemPerPage && isFirstPage {
						cr.DelFirstPage(pipe, pipeCtx, keyParam)
					}
					if isFirstPage || isLastPage {
						cr.sortedSetClient.SetItem(pipe, pipeCtx, keyParam, score, item)
					}
				}
			}
		}
	} else {
		cr.sortedSetClient.SetItem(pipe, pipeCtx, keyParam, score, item)
	}

	return nil
}

func (cr *Timeline[T]) RemoveItem(item T, param []string) error {
	pipeCtx := context.Background()
	pipe := cr.client.Pipeline()

	errDelBase := cr.baseClient.Del(pipe, pipeCtx, item)
	if errDelBase != nil {
		return errDelBase
	}

	err := cr.sortedSetClient.RemoveItem(pipe, pipeCtx, param, item)
	if err != nil {
		return err
	}

	isFirstPage, errFirstPage := cr.IsFirstPage(param)
	if errFirstPage != nil {
		return errFirstPage
	}
	if isFirstPage {
		numItem := cr.sortedSetClient.Count(param) // O(log(n))
		if numItem == 0 {
			cr.DelFirstPage(pipe, pipeCtx, param)
		}
	}

	isLastPage, errLastPage := cr.IsLastPage(param)
	if errLastPage != nil {
		return errLastPage
	}
	if isLastPage {
		numItem := cr.sortedSetClient.Count(param)
		if numItem == 0 {
			cr.DelLastPage(pipe, pipeCtx, param)
		}
	}

	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
}

func (cr *Timeline[T]) IsFirstPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":firstpage"

	getFirstPageKey := cr.client.Get(context.TODO(), firstPageKey)
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

func (cr *Timeline[T]) SetFirstPage(pipe redis.Pipeliner, ctx context.Context, param []string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":firstpage"

	pipe.Set(
		ctx,
		firstPageKey,
		1,
		cr.timeToLive,
	)
}

func (cr *Timeline[T]) DelFirstPage(pipe redis.Pipeliner, ctx context.Context, param []string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":firstpage"

	pipe.Del(ctx, firstPageKey)
}

func (cr *Timeline[T]) IsLastPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	getLastPageKey := cr.client.Get(context.TODO(), lastPageKey)
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

func (cr *Timeline[T]) SetLastPage(pipe redis.Pipeliner, ctx context.Context, param []string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	pipe.Set(
		ctx,
		lastPageKey,
		1,
		cr.timeToLive,
	)
}

func (cr *Timeline[T]) DelLastPage(pipe redis.Pipeliner, ctx context.Context, param []string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	pipe.Del(ctx, lastPageKey)
}

func (cr *Timeline[T]) IsBlankPage(param []string) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	blankPageKey := sortedSetKey + ":blankpage"

	getLastPageKey := cr.client.Get(context.TODO(), blankPageKey)
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

func (cr *Timeline[T]) SetBlankPage(pipe redis.Pipeliner, ctx context.Context, param []string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	pipe.Set(
		ctx,
		lastPageKey,
		1,
		cr.timeToLive,
	)
}

func (cr *Timeline[T]) DelBlankPage(pipe redis.Pipeliner, ctx context.Context, param []string) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	pipe.Del(ctx, lastPageKey)
}

func (cr *Timeline[T]) Fetch(ctx context.Context, lastRandId []string) *TimelineFetchBuilder[T] {
	return &TimelineFetchBuilder[T]{
		mainCtx:       ctx,
		timeline:      cr,
		lastRandIds:   lastRandId,
		params:        nil,
		processor:     nil,
		processorArgs: nil,
		fetchAll:      false,
	}
}

func (cr *Timeline[T]) FetchAll(ctx context.Context) *TimelineFetchBuilder[T] {
	return &TimelineFetchBuilder[T]{
		mainCtx:       ctx,
		timeline:      cr,
		lastRandIds:   nil,
		params:        nil,
		processor:     nil,
		processorArgs: nil,
		fetchAll:      true,
	}
}

func (cr *Timeline[T]) RequiresSeeding(ctx context.Context, totalItems int64) *TimelineRequiresSeedingBuilder[T] {
	return &TimelineRequiresSeedingBuilder[T]{
		mainCtx:    ctx,
		timeline:   cr,
		totalItems: totalItems,
		params:     nil,
	}
}

func (cr *Timeline[T]) Remove(ctx context.Context) *TimelineRemovalBuilder[T] {
	return &TimelineRemovalBuilder[T]{
		mainCtx:  ctx,
		timeline: cr,
		params:   nil,
		purge:    false,
	}
}

func (cr *Timeline[T]) Purge(ctx context.Context) *TimelineRemovalBuilder[T] {
	return &TimelineRemovalBuilder[T]{
		mainCtx:  ctx,
		timeline: cr,
		params:   nil,
		purge:    true,
	}
}

type TimelineFetchBuilder[T item.Blueprint] struct {
	mainCtx       context.Context
	timeline      *Timeline[T]
	lastRandIds   []string
	params        []string
	processor     func(*T, []interface{})
	processorArgs []interface{}
	fetchAll      bool
}

func (b *TimelineFetchBuilder[T]) WithParams(params ...string) *TimelineFetchBuilder[T] {
	b.params = params
	return b
}

func (b *TimelineFetchBuilder[T]) WithProcessor(processor func(*T, []interface{}), args ...interface{}) *TimelineFetchBuilder[T] {
	b.processor = processor
	b.processorArgs = args
	return b
}

func (b *TimelineFetchBuilder[T]) Exec() ([]T, string, string, error) {
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

		item, err := b.timeline.baseClient.Get(b.lastRandIds[i])
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
		b.params,
		b.timeline.direction,
		b.processor,
		b.processorArgs,
		b.timeline.relation,
		start,
		stop,
		false)
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

type TimelineRequiresSeedingBuilder[T item.Blueprint] struct {
	mainCtx    context.Context
	timeline   *Timeline[T]
	totalItems int64
	params     []string
}

func (b *TimelineRequiresSeedingBuilder[T]) WithParams(params ...string) *TimelineRequiresSeedingBuilder[T] {
	b.params = params
	return b
}

func (b *TimelineRequiresSeedingBuilder[T]) Exec() (bool, error) {
	sortedSetKey := joinParam(b.timeline.sortedSetClient.sortedSetKeyFormat, b.params)
	count, errZCard := b.timeline.client.ZCard(b.mainCtx, sortedSetKey).Result()
	if errZCard != nil {
		return false, errZCard
	}
	if count == 0 {
		pipeCtx := context.Background()
		pipeline := b.timeline.client.Pipeline()

		b.timeline.DelLastPage(pipeline, pipeCtx, b.params)
		b.timeline.DelFirstPage(pipeline, pipeCtx, b.params)

		_, errPipe := pipeline.Exec(pipeCtx)
		if errPipe != nil {
			return false, errPipe
		}
	}

	isBlankPage, err := b.timeline.IsBlankPage(b.params)
	if err != nil {
		return false, err
	}

	isFirstPage, err := b.timeline.IsFirstPage(b.params)
	if err != nil {
		return false, err
	}

	isLastPage, err := b.timeline.IsLastPage(b.params)
	if err != nil {
		return false, err
	}

	if !isBlankPage && !isFirstPage && !isLastPage && b.totalItems < b.timeline.itemPerPage {
		return true, nil
	} else {
		return false, nil
	}
}

type TimelineRemovalBuilder[T item.Blueprint] struct {
	mainCtx  context.Context
	timeline *Timeline[T]
	params   []string
	purge    bool
}

func (t *TimelineRemovalBuilder[T]) WithParams(params ...string) *TimelineRemovalBuilder[T] {
	t.params = params
	return t
}

func (t *TimelineRemovalBuilder[T]) Exec() error {
	pipeCtx := context.Background()
	pipe := t.timeline.client.Pipeline()

	if t.purge {
		items, _, _, err := t.timeline.FetchAll(t.mainCtx).WithParams(t.params...).Exec()
		if err != nil {
			return err
		}

		for _, item := range items {
			t.timeline.baseClient.Del(pipe, pipeCtx, item)
		}
	}

	err := t.timeline.sortedSetClient.Delete(pipe, pipeCtx, t.params)
	if err != nil {
		return err
	}

	t.timeline.DelFirstPage(pipe, pipeCtx, t.params)
	t.timeline.DelLastPage(pipe, pipeCtx, t.params)
	t.timeline.DelBlankPage(pipe, pipeCtx, t.params)

	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
}
