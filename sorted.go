package redifu

import (
	"context"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type SortedWithPipeline[T item.Blueprint] struct {
	sorted   *Sorted[T]
	pipeline redis.Pipeliner
}

func (sw *SortedWithPipeline[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	return sw.sorted.addItem(ctx, sw.pipeline, item, keyParams...)
}

func (sw *SortedWithPipeline[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return sw.sorted.removeItem(ctx, sw.pipeline, item, keyParams...)
}

type Sorted[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	sortingReference string
	relations        []Relation[T]
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

func (cr *Sorted[T]) AddRelation(relations ...Relation[T]) {
	cr.relations = append(cr.relations, relations...)
}

func (cr *Sorted[T]) GetRelations() []Relation[T] {
	return cr.relations
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

func (srtd *Sorted[T]) WithPipeline(pipe redis.Pipeliner) *SortedWithPipeline[T] {
	return &SortedWithPipeline[T]{
		sorted:   srtd,
		pipeline: pipe,
	}
}

func (srtd *Sorted[T]) addItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	var selfPipe bool
	if pipe == nil {
		pipe = srtd.client.Pipeline()
		selfPipe = true
	}

	// Writes the item only if Base does not hold it yet, but always refreshes its TTL —
	// an index must never outlive the items it points at.
	errSet := srtd.baseClient.WithPipeline(pipe).SetIfAbsent(ctx, item)
	if errSet != nil {
		return errSet
	}

	errIngest := srtd.IngestItem(ctx, pipe, item, false, keyParams...)
	if errIngest != nil {
		return errIngest
	}

	if selfPipe {
		_, errPipe := pipe.Exec(ctx)
		return errPipe
	}

	return nil
}

func (srtd *Sorted[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	return srtd.addItem(ctx, nil, item, keyParams...)
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

func (srtd *Sorted[T]) removeItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	var selfPipe bool
	if pipe == nil {
		selfPipe = true
		pipe = srtd.client.Pipeline()
	}

	// Only the index is touched. The item stays in Base, because it may well be a member
	// of other collections; deleting the entity itself is Base.Del's job.
	errDel := srtd.sortedSetClient.RemoveItem(ctx, pipe, item, keyParams...)
	if errDel != nil {
		return errDel
	}

	if selfPipe {
		_, errPipe := pipe.Exec(ctx)
		return errPipe
	}

	return nil
}

func (srtd *Sorted[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return srtd.removeItem(ctx, nil, item, keyParams...)
}

func (srtd *Sorted[T]) Fetch(direction string) *sortedFetchBuilder[T] {
	return &sortedFetchBuilder[T]{
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

// Purge invalidates the collection: the sorted set and its state marker are dropped so
// the next fetch seeds it again from the database. Item keys are left alone.
func (srtd *Sorted[T]) Purge(ctx context.Context, keyParams ...string) error {
	pipe := srtd.client.Pipeline()

	err := srtd.sortedSetClient.Delete(ctx, pipe, keyParams...)
	if err != nil {
		return err
	}

	// Without this the collection stays flagged as confirmed-empty and RequiresSeeding
	// keeps returning false, so a purge would silently kill it instead of rebuilding it.
	errHasData := srtd.HasData(ctx, pipe, keyParams...)
	if errHasData != nil {
		return errHasData
	}

	_, errPipe := pipe.Exec(ctx)
	return errPipe
}

type sortedFetchBuilder[T item.Blueprint] struct {
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

func (s *sortedFetchBuilder[T]) Exec(ctx context.Context) ([]T, error) {
	if !s.byScore {
		return s.sorted.sortedSetClient.Fetch(
			ctx,
			s.sorted.baseClient,
			s.direction,
			s.processor,
			s.processorArgs,
			s.sorted.relations,
			0, -1, false, s.keyParams...)
	} else {
		return s.sorted.sortedSetClient.Fetch(
			ctx,
			s.sorted.baseClient,
			s.direction,
			s.processor,
			s.processorArgs,
			s.sorted.relations,
			s.lowerbound,
			s.upperbound, true, s.keyParams...)
	}
}
