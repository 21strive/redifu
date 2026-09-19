package redifu

import (
	"context"
	"errors"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type FetchOutput[T item.Blueprint] struct {
	items       []T
	validLastId string
	position    string
	firstPage   bool
	hasMore     bool
	dangling    int
	error       error
}

func (f FetchOutput[T]) Items() []T {
	return f.items
}

// ValidLastId is the cursor to hand back for the next page.
func (f FetchOutput[T]) ValidLastId() string {
	return f.validLastId
}

// Position is FirstPage, MiddlePage or LastPage. A page that is both the first and the
// last reports FirstPage, so use IsFirstPage and HasMore when you need both facts.
func (f FetchOutput[T]) Position() string {
	return f.position
}

// IsFirstPage reports whether this page was read without a cursor.
func (f FetchOutput[T]) IsFirstPage() bool {
	return f.firstPage
}

// HasMore reports whether the index holds further items after this page. It is read
// from the index itself, not from how many items came back, so items that have expired
// out of Base in the middle of a feed cannot be mistaken for the end of it.
func (f FetchOutput[T]) HasMore() bool {
	return f.hasMore
}

// Dangling is how many members of this page pointed at item keys that no longer exist.
// A non-zero value is normal when item TTL is shorter than index TTL; a persistently
// high one means the two TTLs are badly matched.
func (f FetchOutput[T]) Dangling() int {
	return f.dangling
}

func (f FetchOutput[T]) Error() error {
	return f.error
}

type TimelineWithPipeline[T item.Blueprint] struct {
	timeline *Timeline[T]
	pipeline redis.Pipeliner
}

// AddItem enqueues the write into the caller's pipeline. Whether the item actually
// entered the index is only known once the caller executes, so unlike Timeline.AddItem
// this cannot report ErrNotIngested.
func (t TimelineWithPipeline[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	_, err := t.timeline.addItem(ctx, t.pipeline, item, keyParams...)
	return err
}

func (t TimelineWithPipeline[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return t.timeline.removeItem(ctx, t.pipeline, item, keyParams...)
}

type Timeline[T item.Blueprint] struct {
	client          redis.UniversalClient
	baseClient      *Base[T]
	sortedSetClient *SortedSet[T]
	itemPerPage     int64
	direction       string
	scorer          *scorer[T]
	relations       []Relation[T]
	timeToLive      time.Duration
}

func NewTimeline[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) (*Timeline[T], error) {
	sortedSetClient := &SortedSet[T]{}
	if err := sortedSetClient.Init(client, keyFormat); err != nil {
		return nil, err
	}

	timeline := &Timeline[T]{}
	if err := timeline.Init(client, baseClient, sortedSetClient, itemPerPage, direction, timeToLive); err != nil {
		return nil, err
	}
	return timeline, nil
}

func (cr *Timeline[T]) Init(client redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T], itemPerPage int64, direction string, timeToLive time.Duration) error {
	if client == nil {
		return errors.New("redifu: timeline client must not be nil")
	}
	if baseClient == nil {
		return errors.New("redifu: timeline base client must not be nil")
	}
	if itemPerPage < 1 {
		return errors.New("redifu: itemPerPage must be at least 1")
	}
	if direction != Ascending && direction != Descending {
		return errors.New("redifu: direction must be redifu.Ascending or redifu.Descending")
	}

	defaultScorer, err := newScorer[T]("")
	if err != nil {
		return err
	}

	cr.client = client
	cr.baseClient = baseClient
	cr.sortedSetClient = sortedSetClient
	cr.itemPerPage = itemPerPage
	cr.direction = direction
	cr.scorer = defaultScorer
	cr.timeToLive = timeToLive
	return nil
}

func (cr *Timeline[T]) AddRelation(relations ...Relation[T]) {
	cr.relations = append(cr.relations, relations...)
}

func (cr *Timeline[T]) GetRelations() []Relation[T] {
	return combineRelations(cr.baseClient, cr.relations)
}

// SetSortingReference chooses the struct field used as the sorted-set score, resolving
// and type-checking it once, here, rather than on every write.
func (cr *Timeline[T]) SetSortingReference(sortingReference string) error {
	resolved, err := newScorer[T](sortingReference)
	if err != nil {
		return err
	}
	cr.scorer = resolved
	return nil
}

func (cr *Timeline[T]) SetSelfHeal(selfHeal bool) {
	cr.sortedSetClient.SetSelfHeal(selfHeal)
}

func (cr *Timeline[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.sortedSetClient.SetExpiration(ctx, pipe, cr.timeToLive, keyParams...)
}

func (cr *Timeline[T]) Count(ctx context.Context, keyParams ...string) (int64, error) {
	return cr.sortedSetClient.Count(ctx, keyParams...)
}

func (cr *Timeline[T]) GetItemPerPage() int64 {
	return cr.itemPerPage
}

func (cr *Timeline[T]) GetDirection() string {
	return cr.direction
}

func (cr *Timeline[T]) WithPipeline(pipe redis.Pipeliner) *TimelineWithPipeline[T] {
	return &TimelineWithPipeline[T]{
		timeline: cr,
		pipeline: pipe,
	}
}

func (cr *Timeline[T]) addItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) (bool, error) {
	var selfPipe bool
	if pipe == nil {
		pipe = cr.client.Pipeline()
		selfPipe = true
	}

	// Writes the item only if Base does not hold it yet, but always refreshes its TTL —
	// an index must never outlive the items it points at.
	if err := cr.baseClient.WithPipeline(pipe).SetIfAbsent(ctx, item); err != nil {
		return false, err
	}

	ingested, errIngest := cr.ingest(ctx, pipe, item, false, keyParams...)
	if errIngest != nil {
		return false, errIngest
	}

	if !selfPipe {
		return false, nil
	}

	if _, errPipe := pipe.Exec(ctx); errPipe != nil && !errors.Is(errPipe, redis.Nil) {
		return false, errPipe
	}

	placed, errPlaced := ingested.Int64()
	if errPlaced != nil {
		return false, errPlaced
	}

	return placed == 1, nil
}

// AddItem stores the item in Base if it is not there yet, always refreshes its TTL, and
// places it into the index when it belongs in the window this timeline currently holds.
//
// It returns ErrNotIngested when the item was stored but not indexed — the collection
// is not seeded, or the item sorts outside the page in hand. That is an outcome rather
// than a failure, but it is reported instead of swallowed.
func (cr *Timeline[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	ingested, err := cr.addItem(ctx, nil, item, keyParams...)
	if err != nil {
		return err
	}
	if !ingested {
		return ErrNotIngested
	}
	return nil
}

// IngestItem places an item into the index. Pass seed = true while seeding. With
// seed = false the window check, the marker updates and the write happen inside Redis
// as one atomic command: one enqueued command instead of five round-trips, and two
// concurrent writers can no longer both decide the page has room.
func (cr *Timeline[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, seed bool, keyParams ...string) error {
	_, err := cr.ingest(ctx, pipe, item, seed, keyParams...)
	return err
}

func (cr *Timeline[T]) ingest(ctx context.Context, pipe redis.Pipeliner, item T, seed bool, keyParams ...string) (*redis.Cmd, error) {
	if pipe == nil {
		return nil, errors.New("redifu: IngestItem requires a pipeline")
	}

	score, err := cr.scorer.score(item)
	if err != nil {
		return nil, err
	}

	if seed {
		if errSet := cr.sortedSetClient.SetItem(ctx, pipe, score, item, keyParams...); errSet != nil {
			return nil, errSet
		}
		return nil, nil
	}

	key, errKey := cr.sortedSetClient.key(keyParams)
	if errKey != nil {
		return nil, errKey
	}

	return timelineIngestScript.Eval(
		ctx, pipe,
		[]string{key, key + markerFirstPage, key + markerLastPage, key + markerBlankPage},
		formatScore(score), item.GetRandId(), cr.itemPerPage, cr.direction,
	), nil
}

func (cr *Timeline[T]) removeItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	var selfPipe bool
	if pipe == nil {
		pipe = cr.client.Pipeline()
		selfPipe = true
	}

	// Only the index is touched. The item stays in Base, because it may well be a member
	// of other collections; deleting the entity itself is Base.Del's job.
	if err := cr.sortedSetClient.RemoveItem(ctx, pipe, item, keyParams...); err != nil {
		return err
	}

	// Removing an item invalidates "nothing exists before/after this page". Both marks
	// are simply dropped: DEL is idempotent, so reading them first only cost two
	// round-trips to learn something that did not change the outcome.
	if err := cr.UnmarkFirstPage(ctx, pipe, keyParams...); err != nil {
		return err
	}
	if err := cr.UnmarkLastPage(ctx, pipe, keyParams...); err != nil {
		return err
	}

	if selfPipe {
		_, errPipe := pipe.Exec(ctx)
		return errPipe
	}

	return nil
}

func (cr *Timeline[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return cr.removeItem(ctx, nil, item, keyParams...)
}

func (cr *Timeline[T]) marker(ctx context.Context, suffix string, keyParams []string) (bool, error) {
	key, err := cr.sortedSetClient.markerKey(suffix, keyParams)
	if err != nil {
		return false, err
	}
	return markerIsSet(ctx, cr.client, key)
}

func (cr *Timeline[T]) setMarker(ctx context.Context, pipe redis.Pipeliner, suffix string, keyParams []string) error {
	key, err := cr.sortedSetClient.markerKey(suffix, keyParams)
	if err != nil {
		return err
	}
	pipe.Set(ctx, key, 1, cr.timeToLive)
	return nil
}

func (cr *Timeline[T]) clearMarker(ctx context.Context, pipe redis.Pipeliner, suffix string, keyParams []string) error {
	key, err := cr.sortedSetClient.markerKey(suffix, keyParams)
	if err != nil {
		return err
	}
	pipe.Del(ctx, key)
	return nil
}

func (cr *Timeline[T]) IsFirstPage(ctx context.Context, keyParams ...string) (bool, error) {
	return cr.marker(ctx, markerFirstPage, keyParams)
}

func (cr *Timeline[T]) MarkFirstPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.setMarker(ctx, pipe, markerFirstPage, keyParams)
}

func (cr *Timeline[T]) UnmarkFirstPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.clearMarker(ctx, pipe, markerFirstPage, keyParams)
}

func (cr *Timeline[T]) IsLastPage(ctx context.Context, keyParams ...string) (bool, error) {
	return cr.marker(ctx, markerLastPage, keyParams)
}

func (cr *Timeline[T]) MarkLastPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.setMarker(ctx, pipe, markerLastPage, keyParams)
}

func (cr *Timeline[T]) UnmarkLastPage(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.clearMarker(ctx, pipe, markerLastPage, keyParams)
}

func (cr *Timeline[T]) IsEmpty(ctx context.Context, keyParams ...string) (bool, error) {
	return cr.marker(ctx, markerBlankPage, keyParams)
}

func (cr *Timeline[T]) MarkEmpty(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.setMarker(ctx, pipe, markerBlankPage, keyParams)
}

func (cr *Timeline[T]) HasData(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.clearMarker(ctx, pipe, markerBlankPage, keyParams)
}

func (cr *Timeline[T]) Fetch(lastRandId []string) *timelineFetchBuilder[T] {
	return &timelineFetchBuilder[T]{
		timeline:    cr,
		lastRandIds: appendParams(nil, lastRandId...),
	}
}

func (cr *Timeline[T]) FetchAll() *timelineFetchBuilder[T] {
	return &timelineFetchBuilder[T]{
		timeline: cr,
		fetchAll: true,
	}
}

// Purge invalidates the collection: the sorted set and all three state markers are
// dropped so the next fetch seeds it again from the database. Item keys are left alone.
func (cr *Timeline[T]) Purge(ctx context.Context, keyParams ...string) error {
	pipe := cr.client.Pipeline()

	if err := cr.sortedSetClient.Delete(ctx, pipe, keyParams...); err != nil {
		return err
	}
	if err := cr.UnmarkFirstPage(ctx, pipe, keyParams...); err != nil {
		return err
	}
	if err := cr.UnmarkLastPage(ctx, pipe, keyParams...); err != nil {
		return err
	}
	if err := cr.HasData(ctx, pipe, keyParams...); err != nil {
		return err
	}

	_, errPipe := pipe.Exec(ctx)
	return errPipe
}

type timelineFetchBuilder[T item.Blueprint] struct {
	timeline      *Timeline[T]
	lastRandIds   []string
	params        []string
	processor     func(*T, []interface{})
	processorArgs []interface{}
	fetchAll      bool
}

func (b *timelineFetchBuilder[T]) WithParams(params ...string) *timelineFetchBuilder[T] {
	b.params = appendParams(nil, params...)
	return b
}

func (b *timelineFetchBuilder[T]) WithProcessor(processor func(*T, []interface{}), args ...interface{}) *timelineFetchBuilder[T] {
	b.processor = processor
	b.processorArgs = args
	return b
}

// cursorPage asks Redis for the page that follows one of the supplied cursors. The
// candidates are tried newest first; the first one still present in the index wins.
func (b *timelineFetchBuilder[T]) cursorPage(ctx context.Context, key string, limit int64) ([]string, bool, error) {
	for i := len(b.lastRandIds) - 1; i >= 0; i-- {
		cursor := b.lastRandIds[i]
		if cursor == "" {
			continue
		}

		result, err := timelineCursorScript.Eval(
			ctx, b.timeline.client,
			[]string{key},
			cursor, limit, b.timeline.direction,
		).StringSlice()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				// This cursor is no longer a member of the index. Try an older one.
				continue
			}
			return nil, false, err
		}

		return result, true, nil
	}

	return nil, false, nil
}

func (b *timelineFetchBuilder[T]) Exec(ctx context.Context) *FetchOutput[T] {
	key, errKey := b.timeline.sortedSetClient.key(b.params)
	if errKey != nil {
		return &FetchOutput[T]{error: errKey}
	}

	// One extra member tells us whether a further page exists without a second query.
	limit := b.timeline.itemPerPage
	var members []string
	var usedCursor bool

	switch {
	case b.fetchAll:
		found, err := b.timeline.sortedSetClient.rangeMembers(ctx, b.timeline.direction, 0, -1, false, b.params)
		if err != nil {
			return &FetchOutput[T]{error: err}
		}
		members = found

	case len(b.lastRandIds) > 0:
		found, resolved, err := b.cursorPage(ctx, key, limit+1)
		if err != nil {
			return &FetchOutput[T]{error: err}
		}
		if !resolved {
			// Every cursor the client held has left the index — it was purged, it
			// expired, or those items were removed. Restarting is the only correct
			// answer; silently serving page one would repeat items the client has.
			return &FetchOutput[T]{error: ErrResetPagination}
		}
		members = found
		usedCursor = true

	default:
		found, err := b.timeline.sortedSetClient.rangeMembers(ctx, b.timeline.direction, 0, limit, false, b.params)
		if err != nil {
			return &FetchOutput[T]{error: err}
		}
		members = found
	}

	// hasMore is decided by what the index holds, before any item is loaded. Deriving
	// it from the number of hydrated items would report the end of the feed whenever a
	// few items in the middle of a page had expired out of Base.
	hasMore := !b.fetchAll && int64(len(members)) > limit
	if hasMore {
		members = members[:limit]
	}

	items, dangling, errHydrate := b.timeline.sortedSetClient.hydrate(
		ctx,
		b.timeline.baseClient,
		members,
		combineRelations(b.timeline.baseClient, b.timeline.relations),
		b.timeline.baseClient.relationDepth,
		b.processor,
		b.processorArgs,
		b.params,
	)
	if errHydrate != nil {
		return &FetchOutput[T]{error: errHydrate}
	}

	position := MiddlePage
	switch {
	case !usedCursor:
		position = FirstPage
	case !hasMore:
		position = LastPage
	}

	validLastId := ""
	if len(members) > 0 {
		validLastId = members[len(members)-1]
	}

	return &FetchOutput[T]{
		items:       items,
		validLastId: validLastId,
		position:    position,
		firstPage:   !usedCursor,
		hasMore:     hasMore,
		dangling:    dangling,
	}
}

func (b *Timeline[T]) RequiresSeeding(ctx context.Context, totalItems int64, keyParams ...string) (bool, error) {
	count, errCount := b.sortedSetClient.Count(ctx, keyParams...)
	if errCount != nil {
		return false, errCount
	}

	if count == 0 {
		// NOTE: There's a potential race between checking count and unmarking pages,
		// but the impact is minimal - worst case is page markers are cleaned when
		// they shouldn't be, which will be corrected on the next seeding check.
		pipeline := b.client.Pipeline()

		if err := b.UnmarkLastPage(ctx, pipeline, keyParams...); err != nil {
			return false, err
		}
		if err := b.UnmarkFirstPage(ctx, pipeline, keyParams...); err != nil {
			return false, err
		}

		if _, errPipe := pipeline.Exec(ctx); errPipe != nil {
			return false, errPipe
		}
	}

	isBlankPage, err := b.IsEmpty(ctx, keyParams...)
	if err != nil {
		return false, err
	}

	isFirstPage, err := b.IsFirstPage(ctx, keyParams...)
	if err != nil {
		return false, err
	}

	isLastPage, err := b.IsLastPage(ctx, keyParams...)
	if err != nil {
		return false, err
	}

	return !isBlankPage && !isFirstPage && !isLastPage && totalItems < b.itemPerPage, nil
}
