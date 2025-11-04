package redifu

import (
	"context"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type Segment struct {
	*item.Foundation `json:",inline" bson:",inline"`
	Start            float64
	End              float64
}

func (segment *Segment) SetStart(start float64) {
	segment.Start = start
}

func (segment *Segment) SetEnd(end float64) {
	segment.End = end
}

func NewSegment(start float64, end float64) Segment {
	segment := &Segment{}
	item.InitItem(segment)
	return *segment
}

type SegmentManager[T item.Blueprint] struct {
	client             redis.UniversalClient
	baseClient         *Base[Segment]
	sortedSetClient    *SortedSet[Segment]
	segmentDesignation string
}

func (sm *SegmentManager[T]) AddSegment(start float64, end float64) {
	segment := NewSegment(start, end)
	segment.SetStart(start)
	segment.SetEnd(end)

	totalItemOnSortedSet := sm.sortedSetClient.TotalItemOnSortedSet([]string{sm.segmentDesignation})

	sm.baseClient.Set(segment)
	sm.sortedSetClient.SetSortedSet([]string{sm.segmentDesignation}, float64(totalItemOnSortedSet+1), segment)
}

func (sm *SegmentManager[T]) IsWithinSegment(start float64, end float64) *Segment {
	keySegmentList := joinParam(sm.sortedSetClient.sortedSetKeyFormat, []string{sm.segmentDesignation})

	segments, errFetchSegments := sm.client.ZRange(context.TODO(), keySegmentList, 0, -1).Result()
	if errFetchSegments != nil {
		return nil
	}

	for _, segmentRandId := range segments {
		segment, err := sm.baseClient.Get(segmentRandId)
		if err != nil {
			continue
		}

		if segment.Start <= start && segment.End >= end {
			return &segment
		}
	}
	return nil
}

func NewSegmentManager[T item.Blueprint](client redis.UniversalClient, designation string) *SegmentManager[T] {
	baseClient := &Base[Segment]{
		client:        client,
		itemKeyFormat: "segment:%s",
	}

	sortedSetClient := &SortedSet[Segment]{
		client:             client,
		sortedSetKeyFormat: "segments:%s",
	}

	return &SegmentManager[T]{
		segmentDesignation: designation,
		client:             client,
		baseClient:         baseClient,
		sortedSetClient:    sortedSetClient,
	}
}

type Sorted[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	sortingReference string
	relation         map[string]Relation
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

func (srtd *Sorted[T]) AddItem(item T, sortedSetParam []string) {
	srtd.IngestItem(item, sortedSetParam, false)
}

func (srtd *Sorted[T]) IngestItem(item T, sortedSetParam []string, seed bool) error {
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
			srtd.DelBlankPage(sortedSetParam)
		}

		if srtd.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
			return srtd.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
		}
	} else {
		return srtd.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
	}

	return nil
}

func (srtd *Sorted[T]) RemoveItem(item T, sortedSetParam []string) error {
	return srtd.sortedSetClient.DeleteFromSortedSet(sortedSetParam, item)
}

func (srtd *Sorted[T]) Fetch(param []string, direction string, processor func(item *T, args []interface{}), processorArgs []interface{}) ([]T, error) {
	return fetchAll[T](srtd.client, srtd.baseClient, srtd.sortedSetClient, param, direction, srtd.baseClient.timeToLive, processor, processorArgs, srtd.relation)
}

func (srtd *Sorted[T]) SetBlankPage(param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	setLastPageKey := srtd.client.Set(
		context.TODO(),
		lastPageKey,
		1,
		srtd.sortedSetClient.timeToLive,
	)

	if setLastPageKey.Err() != nil {
		return setLastPageKey.Err()
	}
	return nil
}

func (srtd *Sorted[T]) DelBlankPage(param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	delLastPageKey := srtd.client.Del(context.TODO(), lastPageKey)
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
	err := srtd.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	return nil
}

func (srtd *Sorted[T]) PurgeSorted(param []string) error {
	items, err := srtd.Fetch(param, Descending, nil, nil)
	if err != nil {
		return err
	}

	for _, item := range items {
		srtd.baseClient.Del(item)
	}

	err = srtd.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	return nil
}

func NewSorted[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, timeToLive time.Duration) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	sorted := &Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	return sorted
}

func NewSortedWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, sortingReference string, timeToLive time.Duration) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	sorted := &Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	sorted.SetSortingReference(sortingReference)
	return sorted
}
