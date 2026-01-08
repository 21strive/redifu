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

func (p *Page[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, page int64, param []string) {
	param = append(param, strconv.FormatInt(page, 10))
	p.sorted.SetExpiration(ctx, pipe, param)
}

func (p *Page[T]) SetBlankPage(ctx context.Context, pipe redis.Pipeliner, page int64, param []string) {
	param = append(param, strconv.FormatInt(page, 10))
	p.sorted.SetBlankPage(ctx, pipe, param)
}

func (p *Page[T]) AddPage(ctx context.Context, pipe redis.Pipeliner, page int64, param []string) {
	key := joinParam(p.pageIndexKeyFormat, param)
	member := redis.Z{
		Score:  float64(page),
		Member: strconv.FormatInt(page, 10),
	}

	pipe.ZAdd(ctx, key, member)
	pipe.Expire(ctx, key, p.sorted.timeToLive)
}

func (p *Page[T]) AddRelation(identifier string, relationBase Relation) {
	if p.relation == nil {
		p.relation = make(map[string]Relation)
	}
	p.relation[identifier] = relationBase
}

func (p *Page[T]) IngestItem(pipe redis.Pipeliner, pipeCtx context.Context, item T, page int64, param []string) error {
	param = append(param, strconv.FormatInt(page, 10))
	return p.sorted.IngestItem(pipe, pipeCtx, item, param, true)
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

func (p *Page[T]) Fetch(page int64) *PageFetchBuilder[T] {
	return &PageFetchBuilder[T]{
		page:          p,
		pageNumber:    page,
		params:        nil,
		processor:     nil,
		processorArgs: nil,
	}
}

func (f *Page[T]) RequiresSeeding(page int64, params ...string) (bool, error) {
	param := append(params, strconv.FormatInt(page, 10))
	return f.sorted.RequiresSeeding(param)
}

func (p *Page[T]) Purge(ctx context.Context, params ...string) error {
	key := joinParam(p.pageIndexKeyFormat, params)

	result := p.client.ZRange(ctx, key, 0, -1)
	if result.Err() != nil {
		return result.Err()
	}

	for _, member := range result.Val() {
		newParam := append(params, member)
		errPurge := p.sorted.Purge(newParam)
		if errPurge != nil {
			return errPurge
		}
	}

	delPageIndex := p.client.Del(ctx, key)
	if delPageIndex.Err() != nil {
		return delPageIndex.Err()
	}

	return nil
}

type PageFetchBuilder[T item.Blueprint] struct {
	mainCtx       context.Context
	page          *Page[T]
	pageNumber    int64
	params        []string
	processor     func(*T, []interface{})
	processorArgs []interface{}
}

func (f *PageFetchBuilder[T]) WithParams(params ...string) *PageFetchBuilder[T] {
	f.params = params
	return f
}

func (f *PageFetchBuilder[T]) WithProcessor(processor func(*T, []interface{}), processorArgs ...interface{}) *PageFetchBuilder[T] {
	f.processor = processor
	f.processorArgs = processorArgs
	return f
}

func (f *PageFetchBuilder[T]) Exec() ([]T, error) {
	f.params = append(f.params, strconv.FormatInt(f.pageNumber, 10))
	return f.page.sorted.Fetch(f.mainCtx, f.page.direction).
		WithParams(f.params...).
		WithProcessor(f.processor, f.processorArgs).Exec()
}
