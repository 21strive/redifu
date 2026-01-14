package redifu

import (
	"context"
	"errors"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type Sorted[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	sortingReference string
	relation         map[string]Relation
	timeToLive       time.Duration
}

func NewSorted[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, timeToLive time.Duration) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat)

	sorted := &Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient, timeToLive)
	return sorted
}

func (srtd *Sorted[T]) Init(client redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T], timeToLive time.Duration) {
	srtd.client = client
	srtd.baseClient = baseClient
	srtd.sortedSetClient = sortedSetClient
	srtd.timeToLive = timeToLive
}

func (cr *Sorted[T]) AddRelation(identifier string, relationBase Relation) {
	if cr.relation == nil {
		cr.relation = make(map[string]Relation)
	}
	cr.relation[identifier] = relationBase
}

func (cr *Sorted[T]) GetRelation() map[string]Relation {
	return cr.relation
}

func (srtd *Sorted[T]) SetSortingReference(sortingReference string) {
	srtd.sortingReference = sortingReference
}

func (srtd *Sorted[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	srtd.sortedSetClient.SetExpiration(ctx, pipe, srtd.timeToLive, keyParams...)
}

func (srtd *Sorted[T]) Count(ctx context.Context, keyParams ...string) int64 {
	return srtd.sortedSetClient.Count(ctx, keyParams...)
}

func (srtd *Sorted[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	_, errGet := srtd.baseClient.Get(ctx, item.GetRandId())
	if errGet != nil && errors.Is(errGet, redis.Nil) {
		return errGet
	}

	pipe := srtd.client.Pipeline()

	if errors.Is(errGet, redis.Nil) {
		errSet := srtd.baseClient.Set(ctx, pipe, item)
		if errSet != nil {
			return errSet
		}
	}

	errIngest := srtd.IngestItem(ctx, pipe, item, false, keyParams...)
	if errIngest != nil {
		return errIngest
	}

	_, errPipe := pipe.Exec(ctx)
	return errPipe
}

func (srtd *Sorted[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, seed bool, keyParams ...string) error {
	score, err := getItemScore(item, srtd.sortingReference)
	if err != nil {
		return err
	}

	if !seed {
		isBlankPage, errGet := srtd.IsEmpty(ctx, keyParams...)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			errDelBlankPage := srtd.HasData(ctx, pipe, keyParams...)
			if errDelBlankPage != nil {
				return errDelBlankPage
			}
		}

		if srtd.sortedSetClient.Count(ctx, keyParams...) > 0 {
			srtd.sortedSetClient.SetItem(ctx, pipe, score, item, keyParams...)
		}
	} else {
		srtd.sortedSetClient.SetItem(ctx, pipe, score, item, keyParams...)
	}

	return nil
}

func (srtd *Sorted[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	pipe := srtd.client.Pipeline()
	errDelBase := srtd.baseClient.Del(ctx, pipe, item)
	if errDelBase != nil {
		return errDelBase
	}

	errDel := srtd.sortedSetClient.RemoveItem(ctx, pipe, item, keyParams...)
	if errDel != nil {
		return errDel
	}
	_, errPipe := pipe.Exec(ctx)
	return errPipe
}

func (srtd *Sorted[T]) Fetch(ctx context.Context, direction string) *sortedFetchBuilder[T] {
	return &sortedFetchBuilder[T]{
		mainCtx:   ctx,
		direction: direction,
		sorted:    srtd,
	}
}

func (srtd *Sorted[T]) MarkEmpty(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":blankpage"

	pipe.Set(
		ctx,
		lastPageKey,
		1,
		srtd.timeToLive,
	)
}

func (srtd *Sorted[T]) HasData(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":blankpage"

	pipe.Del(ctx, lastPageKey)
	return nil
}

func (srtd *Sorted[T]) IsEmpty(ctx context.Context, keyParams ...string) (bool, error) {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, keyParams)
	lastPageKey := sortedSetKey + ":blankpage"

	getLastPageKey := srtd.client.Get(ctx, lastPageKey)
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

func (srtd *Sorted[T]) RequiresSeeding(ctx context.Context, keyParams ...string) (bool, error) {
	isBlankPage, err := srtd.IsEmpty(ctx, keyParams...)
	if err != nil {
		return false, err
	}

	if !isBlankPage {
		if srtd.sortedSetClient.Count(ctx, keyParams...) > 0 {
			return false, nil
		}
		return true, nil
	} else {
		return false, nil
	}
}

func (srtd *Sorted[T]) Remove(ctx context.Context) *sortedRemoveBuilder[T] {
	return &sortedRemoveBuilder[T]{
		srtd:      srtd,
		mainCtx:   ctx,
		keyParams: nil,
		purge:     false,
	}
}

func (srtd *Sorted[T]) Purge(ctx context.Context) *sortedRemoveBuilder[T] {
	return &sortedRemoveBuilder[T]{
		srtd:      srtd,
		mainCtx:   ctx,
		keyParams: nil,
		purge:     true,
	}
}

type sortedFetchBuilder[T item.Blueprint] struct {
	mainCtx       context.Context
	direction     string
	sorted        *Sorted[T]
	keyParams     []string
	processor     func(*T, []interface{})
	processorArgs []interface{}
	byScore       bool
	lowerbound    int64
	upperbound    int64
}

func (s *sortedFetchBuilder[T]) WithParams(params ...string) *sortedFetchBuilder[T] {
	s.keyParams = params
	return s
}

func (s *sortedFetchBuilder[T]) WithProcessor(processor func(*T, []interface{}), processorArgs ...interface{}) *sortedFetchBuilder[T] {
	s.processor = processor
	s.processorArgs = processorArgs
	return s
}

func (s *sortedFetchBuilder[T]) WithRange(lowerbound int64, upperbound int64) *sortedFetchBuilder[T] {
	s.byScore = true
	s.lowerbound = lowerbound
	s.upperbound = upperbound
	return s
}

func (s *sortedFetchBuilder[T]) Exec() ([]T, error) {
	if !s.byScore {
		return s.sorted.sortedSetClient.Fetch(
			s.mainCtx,
			s.sorted.baseClient,
			s.direction,
			s.processor,
			s.processorArgs,
			s.sorted.relation,
			0, -1, false, s.keyParams...)
	} else {
		return s.sorted.sortedSetClient.Fetch(
			s.mainCtx,
			s.sorted.baseClient,
			s.direction,
			s.processor,
			s.processorArgs,
			s.sorted.relation,
			s.lowerbound,
			s.upperbound, true, s.keyParams...)
	}
}

type sortedRemoveBuilder[T item.Blueprint] struct {
	srtd      *Sorted[T]
	mainCtx   context.Context
	keyParams []string
	purge     bool
}

func (s *sortedRemoveBuilder[T]) WithParams(params ...string) *sortedRemoveBuilder[T] {
	s.keyParams = params
	return s
}

func (s *sortedRemoveBuilder[T]) Exec() error {
	pipe := s.srtd.client.Pipeline()

	if s.purge {
		fetchedItems, err := s.srtd.Fetch(s.mainCtx, Ascending).WithParams(s.keyParams...).Exec()
		if err != nil {
			return err
		}

		for _, fetchedItem := range fetchedItems {
			errDelItem := s.srtd.baseClient.Del(s.mainCtx, pipe, fetchedItem)
			if errDelItem != nil {
				return errDelItem
			}
		}
	}

	err := s.srtd.sortedSetClient.Delete(s.mainCtx, pipe, s.keyParams...)
	if err != nil {
		return err
	}

	_, errPipe := pipe.Exec(s.mainCtx)
	return errPipe
}
