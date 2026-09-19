package redifu

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type PageWithPipeline[T item.Blueprint] struct {
	page *Page[T]
	pipe redis.Pipeliner
}

func (pw *PageWithPipeline[T]) AddItem(ctx context.Context, item T, page int64, keyParams ...string) error {
	_, err := pw.page.addItem(ctx, pw.pipe, item, page, keyParams...)
	return err
}

func (pw *PageWithPipeline[T]) RemoveItem(ctx context.Context, item T, page int64, keyParams ...string) error {
	return pw.page.sorted.removeItem(ctx, pw.pipe, item, pw.page.pageParams(page, keyParams)...)
}

type Page[T item.Blueprint] struct {
	client        redis.UniversalClient
	pageIndexKeys *keyBuilder
	sorted        *Sorted[T]
	direction     string
	itemPerPage   int64
}

func NewPage[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) (*Page[T], error) {
	keys, err := newKeyBuilder(keyFormat)
	if err != nil {
		return nil, err
	}

	sorted, errSorted := NewSorted[T](client, baseClient, keyFormat+":page:%s", timeToLive)
	if errSorted != nil {
		return nil, errSorted
	}

	page := &Page[T]{}
	if errInit := page.Init(client, sorted, keys.suffixed(":page-index"), direction, itemPerPage); errInit != nil {
		return nil, errInit
	}
	return page, nil
}

func (p *Page[T]) Init(client redis.UniversalClient, sortedClient *Sorted[T], pageIndexKeys *keyBuilder, direction string, itemPerPage int64) error {
	if client == nil {
		return errors.New("redifu: page client must not be nil")
	}
	if itemPerPage < 1 {
		return errors.New("redifu: itemPerPage must be at least 1")
	}
	if direction != Ascending && direction != Descending {
		return errors.New("redifu: direction must be redifu.Ascending or redifu.Descending")
	}

	p.client = client
	p.sorted = sortedClient
	p.pageIndexKeys = pageIndexKeys
	p.direction = direction
	p.itemPerPage = itemPerPage
	return nil
}

// pageParams appends the page number to the caller's key parameters without touching
// the caller's slice. Spreading a variadic argument hands the callee the caller's own
// backing array, so appending in place could overwrite a value the caller still holds.
func (p *Page[T]) pageParams(page int64, keyParams []string) []string {
	return appendParams(keyParams, strconv.FormatInt(page, 10))
}

func (p *Page[T]) SetSortingReference(sortingReference string) error {
	return p.sorted.SetSortingReference(sortingReference)
}

func (p *Page[T]) SetSelfHeal(selfHeal bool) {
	p.sorted.SetSelfHeal(selfHeal)
}

func (p *Page[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, page int64, keyParams ...string) error {
	return p.sorted.SetExpiration(ctx, pipe, p.pageParams(page, keyParams)...)
}

func (p *Page[T]) MarkEmpty(ctx context.Context, pipe redis.Pipeliner, page int64, keyParams ...string) error {
	return p.sorted.MarkEmpty(ctx, pipe, p.pageParams(page, keyParams)...)
}

func (p *Page[T]) IsEmpty(ctx context.Context, page int64, keyParams ...string) (bool, error) {
	return p.sorted.IsEmpty(ctx, p.pageParams(page, keyParams)...)
}

func (p *Page[T]) Count(ctx context.Context, page int64, keyParams ...string) (int64, error) {
	return p.sorted.Count(ctx, p.pageParams(page, keyParams)...)
}

func (p *Page[T]) AddPage(ctx context.Context, pipe redis.Pipeliner, page int64, keyParams ...string) error {
	key, err := p.pageIndexKeys.build(keyParams)
	if err != nil {
		return err
	}

	pipe.ZAdd(ctx, key, redis.Z{
		Score:  float64(page),
		Member: strconv.FormatInt(page, 10),
	})
	pipe.Expire(ctx, key, p.sorted.timeToLive)
	return nil
}

func (p *Page[T]) AddRelation(relations ...Relation[T]) {
	p.sorted.AddRelation(relations...)
}

func (p *Page[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, page int64, keyParams ...string) error {
	return p.sorted.IngestItem(ctx, pipe, item, true, p.pageParams(page, keyParams)...)
}

func (p *Page[T]) WithPipeline(pipe redis.Pipeliner) *PageWithPipeline[T] {
	return &PageWithPipeline[T]{page: p, pipe: pipe}
}

func (p *Page[T]) addItem(ctx context.Context, pipe redis.Pipeliner, item T, page int64, keyParams ...string) (bool, error) {
	return p.sorted.addItem(ctx, pipe, item, p.pageParams(page, keyParams)...)
}

// AddItem places an item into one numbered page, storing it in Base if it is not there
// yet and refreshing its TTL either way. It returns ErrNotIngested when that page has
// not been seeded, matching Sorted and Timeline; Page previously had no way at all to
// take a new item without a full reseed.
func (p *Page[T]) AddItem(ctx context.Context, item T, page int64, keyParams ...string) error {
	ingested, err := p.addItem(ctx, nil, item, page, keyParams...)
	if err != nil {
		return err
	}
	if !ingested {
		return ErrNotIngested
	}
	return nil
}

// RemoveItem drops the item from one numbered page. The item itself stays in Base.
func (p *Page[T]) RemoveItem(ctx context.Context, item T, page int64, keyParams ...string) error {
	return p.sorted.RemoveItem(ctx, item, p.pageParams(page, keyParams)...)
}

func (p *Page[T]) GetRelations() []Relation[T] {
	return p.sorted.GetRelations()
}

func (p *Page[T]) GetItemPerPage() int64 {
	return p.itemPerPage
}

func (p *Page[T]) GetSorted() *Sorted[T] {
	return p.sorted
}

func (p *Page[T]) Fetch(page int64) *pageFetchBuilder[T] {
	return &pageFetchBuilder[T]{
		page:       p,
		pageNumber: page,
	}
}

func (p *Page[T]) RequiresSeeding(ctx context.Context, page int64, keyParams ...string) (bool, error) {
	return p.sorted.RequiresSeeding(ctx, p.pageParams(page, keyParams)...)
}

func (p *Page[T]) Purge(ctx context.Context, keyParams ...string) error {
	key, errKey := p.pageIndexKeys.build(keyParams)
	if errKey != nil {
		return errKey
	}

	result := p.client.ZRange(ctx, key, 0, -1)
	if result.Err() != nil {
		return result.Err()
	}

	for _, member := range result.Val() {
		if errPurge := p.sorted.Purge(ctx, appendParams(keyParams, member)...); errPurge != nil {
			return errPurge
		}
	}

	return p.client.Del(ctx, key).Err()
}

type pageFetchBuilder[T item.Blueprint] struct {
	page          *Page[T]
	pageNumber    int64
	params        []string
	processor     func(*T, []interface{})
	processorArgs []interface{}
}

func (f *pageFetchBuilder[T]) WithParams(keyParams ...string) *pageFetchBuilder[T] {
	f.params = appendParams(nil, keyParams...)
	return f
}

func (f *pageFetchBuilder[T]) WithProcessor(processor func(*T, []interface{}), processorArgs ...interface{}) *pageFetchBuilder[T] {
	f.processor = processor
	f.processorArgs = processorArgs
	return f
}

func (f *pageFetchBuilder[T]) Exec(ctx context.Context) ([]T, error) {
	// processorArgs is forwarded with ... — without it the consumer's arguments arrive
	// wrapped in one more layer of slice than they passed.
	return f.page.sorted.Fetch(f.page.direction).
		WithParams(f.page.pageParams(f.pageNumber, f.params)...).
		WithProcessor(f.processor, f.processorArgs...).
		Exec(ctx)
}
