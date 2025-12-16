package redifu

import (
	"context"
	"errors"
	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
	"time"
)

type Sorted[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	sortingReference string
	relation         map[string]Relation
	timeToLive       time.Duration
}

func (srtd *Sorted[T]) Init(client redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T]) {
	srtd.client = client
	srtd.baseClient = baseClient
	srtd.sortedSetClient = sortedSetClient
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

func (srtd *Sorted[T]) AddItem(item T, sortedSetParam []string) error {
	_, errGet := srtd.baseClient.Get(item.GetRandId())
	if errGet != nil && errGet != redis.Nil {
		return errGet
	}

	pipeCtx := context.Background()
	pipe := srtd.client.Pipeline()

	if errors.Is(errGet, redis.Nil) {
		errSet := srtd.baseClient.Set(pipe, pipeCtx, item)
		if errSet != nil {
			return errSet
		}
	}

	errIngest := srtd.IngestItem(pipe, pipeCtx, item, sortedSetParam, false)
	if errIngest != nil {
		return errIngest
	}

	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
}

func (srtd *Sorted[T]) IngestItem(pipe redis.Pipeliner, pipeCtx context.Context, item T, sortedSetParam []string, seed bool) error {
	score, err := getItemScore(item, srtd.sortingReference)
	if err != nil {
		return err
	}

	if !seed {
		isBlankPage, errGet := srtd.IsBlankPage(sortedSetParam)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			srtd.DelBlankPage(pipe, pipeCtx, sortedSetParam)
		}

		if srtd.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
			return srtd.sortedSetClient.SetSortedSet(pipe, pipeCtx, sortedSetParam, score, item)
		}
	} else {
		return srtd.sortedSetClient.SetSortedSet(pipe, pipeCtx, sortedSetParam, score, item)
	}

	return nil
}

func (srtd *Sorted[T]) SetExpiration(pipe redis.Pipeliner, pipeCtx context.Context, sortedSetParam []string) error {
	return srtd.sortedSetClient.SetExpiration(pipe, pipeCtx, sortedSetParam, srtd.timeToLive)
}

func (srtd *Sorted[T]) RemoveItem(item T, sortedSetParam []string) error {
	pipe := srtd.client.Pipeline()
	errDel := srtd.sortedSetClient.DeleteFromSortedSet(pipe, context.Background(), sortedSetParam, item)
	if errDel != nil {
		return errDel
	}
	_, errPipe := pipe.Exec(context.Background())
	return errPipe
}

func (srtd *Sorted[T]) Fetch(param []string, direction string, processor func(item *T, args []interface{}), processorArgs []interface{}) ([]T, error) {
	return fetchAll[T](srtd.client, srtd.baseClient, srtd.sortedSetClient, param, direction, processor, processorArgs, srtd.relation)
}

func (srtd *Sorted[T]) SetBlankPage(pipe redis.Pipeliner, pipeCtx context.Context, param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	setLastPageKey := pipe.Set(
		pipeCtx,
		lastPageKey,
		1,
		srtd.timeToLive,
	)

	if setLastPageKey.Err() != nil {
		return setLastPageKey.Err()
	}
	return nil
}

func (srtd *Sorted[T]) DelBlankPage(pipe redis.Pipeliner, pipeCtx context.Context, param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	delLastPageKey := pipe.Del(pipeCtx, lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
}

func (srtd *Sorted[T]) IsBlankPage(param []string) (bool, error) {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	getLastPageKey := srtd.client.Get(context.TODO(), lastPageKey)
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

func (srtd *Sorted[T]) RequiresSeeding(param []string) (bool, error) {
	isBlankPage, err := srtd.IsBlankPage(param)
	if err != nil {
		return false, err
	}

	if !isBlankPage {
		if srtd.sortedSetClient.TotalItemOnSortedSet(param) > 0 {
			return false, nil
		}
		return true, nil
	} else {
		return false, nil
	}
}

func (srtd *Sorted[T]) RemoveSorted(param []string) error {
	pipe := srtd.client.Pipeline()
	err := srtd.sortedSetClient.DeleteSortedSet(pipe, context.Background(), param)
	if err != nil {
		return err
	}
	_, errPipe := pipe.Exec(context.Background())
	return errPipe
}

func (srtd *Sorted[T]) PurgeSorted(param []string) error {
	pipeCtx := context.Background()
	pipe := srtd.client.Pipeline()

	items, err := srtd.Fetch(param, Descending, nil, nil)
	if err != nil {
		return err
	}

	for _, item := range items {
		srtd.baseClient.Del(pipe, pipeCtx, item)
	}

	err = srtd.sortedSetClient.DeleteSortedSet(pipe, pipeCtx, param)
	if err != nil {
		return err
	}

	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
}

func NewSorted[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, timeToLive time.Duration) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat)

	sorted := &Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	return sorted
}

func NewSortedWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, sortingReference string, timeToLive time.Duration) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat)

	sorted := &Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	sorted.SetSortingReference(sortingReference)
	return sorted
}
