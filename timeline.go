package redifu

import (
	"context"
	"errors"
	"reflect"
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

func (cr *Timeline[T]) AddItem(item T, sortedSetParam []string) error {
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

	errIngest := cr.IngestItem(pipe, pipeCtx, item, sortedSetParam, false)
	if errIngest != nil {
		return errIngest
	}
	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
}

func (cr *Timeline[T]) IngestItem(pipe redis.Pipeliner, pipeCtx context.Context, item T, sortedSetParam []string, seed bool) error {
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
			cr.DelBlankPage(pipe, pipeCtx, sortedSetParam)
		}

		if cr.direction == Descending {
			if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
				lowestScore, err := cr.sortedSetClient.LowestScore(sortedSetParam)
				if err != nil {
					return err
				}

				if score >= lowestScore {
					if cr.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) == cr.itemPerPage && isFirstPage {
						cr.DelFirstPage(pipe, pipeCtx, sortedSetParam)
					}
					cr.sortedSetClient.SetSortedSet(pipe, pipeCtx, sortedSetParam, score, item)
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
						cr.DelFirstPage(pipe, pipeCtx, sortedSetParam)
					}
					if isFirstPage || isLastPage {
						cr.sortedSetClient.SetSortedSet(pipe, pipeCtx, sortedSetParam, score, item)
					}
				}
			}
		}
	} else {
		cr.sortedSetClient.SetSortedSet(pipe, pipeCtx, sortedSetParam, score, item)
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

	err := cr.sortedSetClient.DeleteFromSortedSet(pipe, pipeCtx, param, item)
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
			cr.DelFirstPage(pipe, pipeCtx, param)
		}
	}

	isLastPage, errLastPage := cr.IsLastPage(param)
	if errLastPage != nil {
		return errLastPage
	}
	if isLastPage {
		numItem := cr.sortedSetClient.TotalItemOnSortedSet(param)
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
		count, errZCard := cr.client.ZCard(context.TODO(), sortedSetKey).Result()
		if errZCard != nil {
			return nil, validLastRandId, position, errZCard
		}
		if count == 0 {
			return nil, validLastRandId, position, ResetPagination
		}

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

	for i := 0; i < len(listRandIds); i++ {
		item, err := cr.baseClient.Get(listRandIds[i])
		if err != nil {
			continue
		}

		if cr.relation != nil {
			for _, relationFormat := range cr.relation {
				v := reflect.ValueOf(item)

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
	return fetchAll(cr.client, cr.baseClient, cr.sortedSetClient, param, cr.direction, processor, processorArgs, cr.relation)
}

func (cr *Timeline[T]) RequiresSeeding(param []string, totalItems int64) (bool, error) {
	sortedSetKey := joinParam(cr.sortedSetClient.sortedSetKeyFormat, param)
	count, errZCard := cr.client.ZCard(context.TODO(), sortedSetKey).Result()
	if errZCard != nil {
		return false, errZCard
	}
	if count == 0 {
		pipeCtx := context.Background()
		pipeline := cr.client.Pipeline()

		cr.DelLastPage(pipeline, pipeCtx, param)
		cr.DelFirstPage(pipeline, pipeCtx, param)

		_, errPipe := pipeline.Exec(pipeCtx)
		if errPipe != nil {
			return false, errPipe
		}
	}

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
	pipeCtx := context.Background()
	pipe := cr.client.Pipeline()

	err := cr.sortedSetClient.DeleteSortedSet(pipe, pipeCtx, param)
	if err != nil {
		return err
	}

	cr.DelFirstPage(pipe, pipeCtx, param)
	cr.DelLastPage(pipe, pipeCtx, param)
	cr.DelBlankPage(pipe, pipeCtx, param)

	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
}

func (cr *Timeline[T]) PurgePagination(param []string) error {
	items, err := cr.FetchAll(param, nil, nil)
	if err != nil {
		return err
	}

	pipeCtx := context.Background()
	pipe := cr.client.Pipeline()

	for _, item := range items {
		cr.baseClient.Del(pipe, pipeCtx, item)
	}

	err = cr.sortedSetClient.DeleteSortedSet(pipe, pipeCtx, param)
	if err != nil {
		return err
	}

	cr.DelFirstPage(pipe, pipeCtx, param)
	cr.DelLastPage(pipe, pipeCtx, param)
	cr.DelBlankPage(pipe, pipeCtx, param)

	_, errPipe := pipe.Exec(pipeCtx)
	return errPipe
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

func NewTimelineWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, sortingReference string, timeToLive time.Duration) *Timeline[T] {
	if direction != Ascending && direction != Descending {
		direction = Descending
	}

	sortedSetClient := SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat)

	timeline := &Timeline[T]{}
	timeline.Init(client, baseClient, &sortedSetClient, itemPerPage, direction, timeToLive)
	timeline.SetSortingReference(sortingReference)
	return timeline
}
