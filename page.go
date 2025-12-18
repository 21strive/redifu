package redifu

import (
	"context"
	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type Page[T item.Blueprint] struct {
	client             redis.UniversalClient
	pageIndexKeyFormat string
	sorted             *Sorted[T]
	direction          string
	processor          func(item *T, args []interface{})
	itemPerPage        int64
}

func (p *Page[T]) SetItemPerPage(itemPerPage int64) {
	p.itemPerPage = itemPerPage
}

func (p *Page[T]) GetItemPerPage() int64 {
	return p.itemPerPage
}

func (p *Page[T]) GetSorted() *Sorted[T] {
	return p.sorted
}

func (p *Page[T]) Fetch(param []string, page int64, processorArg []interface{}) {
	param = append(param, strconv.FormatInt(page, 10))
	p.sorted.Fetch(param, p.direction, p.processor, processorArg)
}

func (p *Page[T]) IsPageBlank(param []string, page int64) (bool, error) {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.IsBlankPage(param)
}

func (p *Page[T]) SetPageBlank(pipe redis.Pipeliner, pipeCtx context.Context, param []string, page int64) error {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.SetBlankPage(pipe, pipeCtx, param)
}

func (p *Page[T]) AddPage(param []string, page int64) error {
	key := joinParam(p.pageIndexKeyFormat, param)
	member := redis.Z{
		Score:  float64(page),
		Member: strconv.FormatInt(page, 10),
	}

	errAdd := p.client.ZAdd(context.TODO(), key, member)
	if errAdd.Err() != nil {
		return errAdd.Err()
	}

	return nil
}

func (p *Page[T]) PurgeAll(param []string) error {
	key := joinParam(p.pageIndexKeyFormat, param)

	result := p.client.ZRange(context.TODO(), key, 0, -1)
	if result.Err() != nil {
		return result.Err()
	}

	for _, member := range result.Val() {
		newParam := append(param, member)
		errPurge := p.sorted.PurgeSorted(newParam)
		if errPurge != nil {
			return errPurge
		}
	}

	delPageIndex := p.client.Del(context.TODO(), key)
	if delPageIndex.Err() != nil {
		return delPageIndex.Err()
	}

	return nil
}

func NewPage[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, timeToLive time.Duration) *Page[T] {
	adjustedKeyFormat := keyFormat + ":page:%s"
	pageIndexKeyFormat := keyFormat + ":pageindex"
	sorted := NewSorted[T](client, baseClient, adjustedKeyFormat, timeToLive)

	return &Page[T]{
		client:             client,
		sorted:             sorted,
		pageIndexKeyFormat: pageIndexKeyFormat,
	}
}
