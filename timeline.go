package redifu

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type Timeline[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	itemPerPage      int64
	direction        string
	sortingReference string
	relation         map[string]Relation
}

func (cr *Timeline[T]) Init(client redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T], itemPerPage int64, direction string) {
	cr.client = client
	cr.baseClient = baseClient
	cr.sortedSetClient = sortedSetClient
	cr.itemPerPage = itemPerPage
	cr.direction = direction
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

func (cr *Timeline[T]) GetItemPerPage() int64 {
	return cr.itemPerPage
}

func (cr *Timeline[T]) GetDirection() string {
	return cr.direction
}

func (cr *Timeline[T]) AddItem(item T, sortedSetParam []string) error {
	return cr.IngestItem(item, sortedSetParam, false)
}

func (cr *Timeline[T]) IngestItem(item T, sortedSetParam []string, seed bool) error {
	if cr.direction == "" {
		return errors.New("must set direction!")
	}

	score, err := getItemScore(item, cr.sortingReference)
	if err != nil {
		return err
	}

	isFirstPage, err := cr.IsFirstPage(sortedSetParam)
	if err != nil {
		return err
	}

	isLastPage, err := cr.IsLastPage(sortedSetParam)
	if err != nil {
		return err
	}

	if !seed {
		isBlankPage, errGet := cr.IsBlankPage(sortedSetParam)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			cr.DelBlankPage(sortedSetParam)
		}

		if cr.direction == Descending {
			if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
				lowestScore, err := cr.sortedSetClient.LowestScore(sortedSetParam)
				if err != nil {
					return err
				}

				if score >= lowestScore {
					if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) == cr.itemPerPage && isFirstPage {
						cr.DelFirstPage(sortedSetParam)
					}
					return cr.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
				}
			}
		} else if cr.direction == Ascending {
			if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
				highestScore, err := cr.sortedSetClient.HighestScore(sortedSetParam)
				if err != nil {
					return err
				}

				if score <= highestScore {
					if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) == cr.itemPerPage && isFirstPage {
						return cr.DelFirstPage(sortedSetParam)
					}
					if isFirstPage || isLastPage {
						return cr.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
					}
				}
			}
		}
	} else {
		return cr.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
	}

	return nil
}

func (cr *Timeline[T]) RemoveItem(item T, param []string) error {
	err := cr.sortedSetClient.DeleteFromSortedSet(param, item)
	if err != nil {
		return err
	}

	isFirstPage, errFirstPage := cr.IsFirstPage(param)
	if errFirstPage != nil {
		return errFirstPage
	}
	if isFirstPage {
		numItem := cr.sortedSetClient.TotalItemOnSortedSet(param) // O(log(n))
		if numItem == 0 {
			errRemFirstPage := cr.DelFirstPage(param)
			if errRemFirstPage != nil {
				return errRemFirstPage
			}
		}
	}

	isLastPage, errLastPage := cr.IsLastPage(param)
	if errLastPage != nil {
		return errLastPage
	}
	if isLastPage {
		numItem := cr.sortedSetClient.TotalItemOnSortedSet(param)
		if numItem == 0 {
			errRemLastPage := cr.DelLastPage(param)
			if errRemLastPage != nil {
				return errRemLastPage
			}
		}
	}

	return nil
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

	cr.client.Expire(context.TODO(), firstPageKey, cr.sortedSetClient.timeToLive)

	if getFirstPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Timeline[T]) SetFirstPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":firstpage"

	setFirstPageKey := cr.client.Set(
		context.TODO(),
		firstPageKey,
		1,
		cr.sortedSetClient.timeToLive,
	)

	if setFirstPageKey.Err() != nil {
		return setFirstPageKey.Err()
	}
	return nil
}

func (cr *Timeline[T]) DelFirstPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	firstPageKey := sortedSetKey + ":firstpage"

	setFirstPageKey := cr.client.Del(context.TODO(), firstPageKey)
	if setFirstPageKey.Err() != nil {
		return setFirstPageKey.Err()
	}

	return nil
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

	cr.client.Expire(context.TODO(), lastPageKey, cr.sortedSetClient.timeToLive)

	if getLastPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Timeline[T]) SetLastPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	setLastPageKey := cr.client.Set(
		context.TODO(),
		lastPageKey,
		1,
		cr.sortedSetClient.timeToLive,
	)

	if setLastPageKey.Err() != nil {
		return setLastPageKey.Err()
	}
	return nil
}

func (cr *Timeline[T]) DelLastPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":lastpage"

	delLastPageKey := cr.client.Del(context.TODO(), lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
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

	cr.client.Expire(context.TODO(), blankPageKey, cr.sortedSetClient.timeToLive)

	if getLastPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (cr *Timeline[T]) SetBlankPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	setLastPageKey := cr.client.Set(
		context.TODO(),
		lastPageKey,
		1,
		cr.sortedSetClient.timeToLive,
	)

	if setLastPageKey.Err() != nil {
		return setLastPageKey.Err()
	}
	return nil
}

func (cr *Timeline[T]) DelBlankPage(param []string) error {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	delLastPageKey := cr.client.Del(context.TODO(), lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
}

func (cr *Timeline[T]) Fetch(
	param []string,
	lastRandIds []string,
	processor func(item *T, args []interface{}),
	processorArgs []interface{},
) ([]T, string, string, error) {
	var items []T
	var validLastRandId string
	var position string

	// safety net
	if cr.direction == "" {
		return nil, validLastRandId, position, errors.New("must set direction!")
	}

	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	start := int64(0)
	stop := cr.itemPerPage - 1

	for i := len(lastRandIds) - 1; i >= 0; i-- {
		item, err := cr.baseClient.Get(lastRandIds[i])
		if err != nil {
			continue
		}

		var rank *redis.IntCmd
		if cr.direction == Descending {
			rank = cr.client.ZRevRank(context.TODO(), sortedSetKey, item.GetRandId())
		} else {
			rank = cr.client.ZRank(context.TODO(), sortedSetKey, item.GetRandId())
		}

		if rank.Err() == nil {
			validLastRandId = item.GetRandId()
			start = rank.Val() + 1
			stop = start + cr.itemPerPage - 1
			break
		}
	}

	var listRandIds []string
	var result *redis.StringSliceCmd
	if cr.direction == Descending {
		result = cr.client.ZRevRange(context.TODO(), sortedSetKey, start, stop)
	} else {
		result = cr.client.ZRange(context.TODO(), sortedSetKey, start, stop)
	}
	if result.Err() != nil {
		return nil, validLastRandId, position, result.Err()
	}
	listRandIds = result.Val()

	cr.client.Expire(context.TODO(), sortedSetKey, cr.sortedSetClient.timeToLive)

	for i := 0; i < len(listRandIds); i++ {
		item, err := cr.baseClient.Get(listRandIds[i])
		if err != nil {
			continue
		}

		if cr.relation != nil {
			for _, relationFormat := range cr.relation {
				v := reflect.ValueOf(&item)

				if v.Kind() == reflect.Ptr {
					v = v.Elem()
				}

				relationRandIdField := v.FieldByName(relationFormat.GetRandIdAttribute())
				if !relationRandIdField.IsValid() {
					continue
				}

				relationRandId := relationRandIdField.String()
				if relationRandId == "" {
					continue
				}

				relationItem, errGet := relationFormat.GetByRandId(relationRandId)
				if errGet != nil {
					continue
				}

				relationAttrField := v.FieldByName(relationFormat.GetItemAttribute())
				if !relationAttrField.IsValid() || !relationAttrField.CanSet() {
					continue
				}

				relationAttrField.Set(reflect.ValueOf(relationItem))
				if relationRandIdField.CanSet() {
					relationRandIdField.SetString("")
				}
			}
		}
		if processor != nil {
			processor(&item, processorArgs)
		}

		items = append(items, item)
		validLastRandId = listRandIds[i]
	}

	if start == 0 {
		position = FirstPage
	} else if int64(len(listRandIds)) < cr.itemPerPage {
		position = LastPage
	} else {
		position = MiddlePage
	}

	return items, validLastRandId, position, nil
}

func (cr *Timeline[T]) FetchAll(param []string, processor func(item *T, args []interface{}), processorArgs []interface{}) ([]T, error) {
	return fetchAll(cr.client, cr.baseClient, cr.sortedSetClient, param, cr.direction, cr.sortedSetClient.timeToLive, processor, processorArgs, cr.relation)
}

func (cr *Timeline[T]) RequriesSeeding(param []string, totalItems int64) (bool, error) {
	isBlankPage, err := cr.IsBlankPage(param)
	if err != nil {
		return false, err
	}

	isFirstPage, err := cr.IsFirstPage(param)
	if err != nil {
		return false, err
	}

	isLastPage, err := cr.IsLastPage(param)
	if err != nil {
		return false, err
	}

	if !isBlankPage && !isFirstPage && !isLastPage && totalItems < cr.itemPerPage {
		return true, nil
	} else {
		return false, nil
	}
}

func (cr *Timeline[T]) RemovePagination(param []string) error {
	err := cr.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	err = cr.DelFirstPage(param)
	if err != nil {
		return err
	}

	err = cr.DelLastPage(param)
	if err != nil {
		return err
	}

	err = cr.DelBlankPage(param)
	if err != nil {
		return err
	}

	return nil
}

func (cr *Timeline[T]) PurgePagination(param []string) error {
	items, err := cr.FetchAll(param, nil, nil)
	if err != nil {
		return err
	}

	for _, item := range items {
		cr.baseClient.Del(item)
	}

	err = cr.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	err = cr.DelFirstPage(param)
	if err != nil {
		return err
	}

	err = cr.DelLastPage(param)
	if err != nil {
		return err
	}

	err = cr.DelBlankPage(param)
	if err != nil {
		return err
	}

	return nil
}

func NewTimeline[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) *Timeline[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	timeline := &Timeline[T]{
		relation: make(map[string]Relation), // Initialize the map
	}
	timeline.Init(client, baseClient, sortedSetClient, itemPerPage, direction)
	return timeline
}

func NewTimelineWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, sortingReference string, timeToLive time.Duration) *Timeline[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	timeline := &Timeline[T]{}
	timeline.Init(client, baseClient, &sortedSetClient, itemPerPage, direction)
	timeline.SetSortingReference(sortingReference)
	return timeline
}
