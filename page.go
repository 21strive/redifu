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

func (p *Page[T]) SetBlankPage(pipe redis.Pipeliner, pipeCtx context.Context, page int64, param []string) {
	param = append(param, strconv.FormatInt(page, 10))
	p.sorted.SetBlankPage(pipe, pipeCtx, param)
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

func (p *Page[T]) RequiresSeeding(page int64) *PageRequiresSeedingBuilder[T] {
	return &PageRequiresSeedingBuilder[T]{
		page:       p,
		pageNumber: page,
		params:     nil,
	}
}

func (p *Page[T]) Purge() *PagePurgeBuilder[T] {
	return &PagePurgeBuilder[T]{
		page:   p,
		params: nil,
	}
}

type PageFetchBuilder[T item.Blueprint] struct {
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
	param := append(f.params, strconv.FormatInt(f.pageNumber, 10))
	return f.page.sorted.Fetch(param, f.page.direction, f.processor, f.processorArgs)
}

type PageRequiresSeedingBuilder[T item.Blueprint] struct {
	page       *Page[T]
	pageNumber int64
	params     []string
}

func (f *PageRequiresSeedingBuilder[T]) WithParams(params ...string) *PageRequiresSeedingBuilder[T] {
	f.params = params
	return f
}

func (f *PageRequiresSeedingBuilder[T]) Exec() (bool, error) {
	param := append(f.params, strconv.FormatInt(f.pageNumber, 10))
	return f.page.sorted.RequiresSeeding(param)
}

type PagePurgeBuilder[T item.Blueprint] struct {
	page   *Page[T]
	params []string
}

func (f *PagePurgeBuilder[T]) WithParams(params ...string) *PagePurgeBuilder[T] {
	f.params = params
	return f
}

func (f *PagePurgeBuilder[T]) Exec(ctx context.Context) error {
	key := joinParam(f.page.pageIndexKeyFormat, f.params)

	result := f.page.client.ZRange(ctx, key, 0, -1)
	if result.Err() != nil {
		return result.Err()
	}

	for _, member := range result.Val() {
		newParam := append(f.params, member)
		errPurge := f.page.sorted.Purge(newParam)
		if errPurge != nil {
			return errPurge
		}
	}

	delPageIndex := f.page.client.Del(context.TODO(), key)
	if delPageIndex.Err() != nil {
		return delPageIndex.Err()
	}

	return nil
}
