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
	relation           map[string]Relation
	itemPerPage        int64
}

func NewPage[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) *Page[T] {
	adjustedKeyFormat := keyFormat + ":page:%s"
	pageIndexKeyFormat := keyFormat + ":page-index"
	sorted := NewSorted[T](client, baseClient, adjustedKeyFormat, timeToLive)

	page := &Page[T]{}
	page.Init(client, sorted, pageIndexKeyFormat, direction, itemPerPage)
	return page
}

func (p *Page[T]) Init(client redis.UniversalClient, sortedClient *Sorted[T], pageIndexKeyFormat string, direction string, itemPerPage int64) {
	p.client = client
	p.sorted = sortedClient
	p.pageIndexKeyFormat = pageIndexKeyFormat
	p.direction = direction
	p.itemPerPage = itemPerPage
}

func (p *Page[T]) SetSortingReference(sortingReference string) {
	p.sorted.SetSortingReference(sortingReference)
}

func (p *Page[T]) SetExpiration(pipe redis.Pipeliner, pipeCtx context.Context, page int64, param []string) {
	param = append(param, strconv.FormatInt(page, 10))
	p.sorted.SetExpiration(pipe, pipeCtx, param)
}

func (p *Page[T]) AddRelation(identifier string, relationBase Relation) {
	if p.relation == nil {
		p.relation = make(map[string]Relation)
	}
	p.relation[identifier] = relationBase
}

func (p *Page[T]) GetRelation() map[string]Relation {
	return p.relation
}

func (p *Page[T]) GetItemPerPage() int64 {
	return p.itemPerPage
}

func (p *Page[T]) GetSorted() *Sorted[T] {
	return p.sorted
}

func (p *Page[T]) Fetch(param []string, page int64, processor func(item *T, args []interface{}), processorArg []interface{}) ([]T, error) {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.Fetch(param, p.direction, processor, processorArg)
}

// TODO: Get Total Items

func (p *Page[T]) SetBlankPage(pipe redis.Pipeliner, pipeCtx context.Context, page int64, param []string) {
	param = append(param, strconv.FormatInt(page, 10))
	p.sorted.SetBlankPage(pipe, pipeCtx, param)
}

func (p *Page[T]) RequiresSeeding(param []string, page int64) (bool, error) {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.RequiresSeeding(param)
}

func (p *Page[T]) IngestItem(pipe redis.Pipeliner, pipeCtx context.Context, item T, page int64, param []string) error {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.IngestItem(pipe, pipeCtx, item, param, true)
}

func (p *Page[T]) AddPage(pipe redis.Pipeliner, pipeCtx context.Context, page int64, param []string) {
	key := joinParam(p.pageIndexKeyFormat, param)
	member := redis.Z{
		Score:  float64(page),
		Member: strconv.FormatInt(page, 10),
	}

	pipe.ZAdd(pipeCtx, key, member)
	pipe.Expire(pipeCtx, key, p.sorted.timeToLive)
}

func (p *Page[T]) PurgeAll(param []string) error {
	key := joinParam(p.pageIndexKeyFormat, param)

	result := p.client.ZRange(context.TODO(), key, 0, -1)
	if result.Err() != nil {
		return result.Err()
	}

	for _, member := range result.Val() {
		newParam := append(param, member)
		errPurge := p.sorted.Purge(newParam)
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
