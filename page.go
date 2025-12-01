package redifu

import (
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

func (p *Page[T]) Fetch(param []string, page int64, processorArg []interface{}) {
	param = append(param, strconv.FormatInt(page, 10))
	p.sorted.Fetch(param, p.direction, p.processor, processorArg)
}

func (p *Page[T]) IsPageBlank(param []string, page int64) (bool, error) {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.IsBlankPage(param)
}

func (p *Page[T]) SetPageBlank(param []string, page int64) error {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.SetBlankPage(param)
}

func (p *Page[T]) PurgeAll(param []string) error {
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
