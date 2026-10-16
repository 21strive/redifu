package redifu

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type FetchOutput[T Blueprint] struct {
	items       []T
	validLastId string
	nextCursor  Cursor
	position    string
	firstPage   bool
	hasMore     bool
	dangling    int
	error       error
}

func (f FetchOutput[T]) Items() []T {
	return f.items
}

// ValidLastId is the randId cursor to hand back for the next page, for Fetch.
func (f FetchOutput[T]) ValidLastId() string {
	return f.validLastId
}

// NextCursor is the position this page ends at: the score and member of its last member,
// as the index holds them. Hand it to the client and pass it to FetchFrom for the next
// page. It is read from the index, so a member whose item has expired out of Base still
// moves the cursor past it.
//
// An empty page ends where it started — at the cursor it was read after, or the zero
// Cursor for a first page — so following NextCursor never moves a client backwards.
func (f FetchOutput[T]) NextCursor() Cursor {
	return f.nextCursor
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

type TimelineWithPipeline[T Blueprint] struct {
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

type Timeline[T Blueprint] struct {
	client          redis.UniversalClient
	baseClient      *Base[T]
	sortedSetClient *SortedSet[T]
	itemPerPage     int64
	direction       string
	scorer          *scorer[T]
	relations       []Relation[T]
	timeToLive      time.Duration
}

func NewTimeline[T Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, itemPerPage int64, direction string, timeToLive time.Duration) (*Timeline[T], error) {
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

// Score returns the score this timeline stores for item: its sorting reference as the
// sorted set holds it — Unix milliseconds for a time field, the value itself for an
// int64. Build cursors and database keysets from it, so that they compare exactly what
// the index sorts by.
func (cr *Timeline[T]) Score(item T) (float64, error) {
	return cr.scorer.score(item)
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

// Tail returns the position of the last member of the index in this timeline's order:
// for a Descending timeline the lowest score and, among the members sharing it, the
// smallest member — the last element ZREVRANGE returns; for an Ascending one the last
// element of ZRANGE. It reports false when the index holds nothing. One round trip.
//
// It is where a top-up seed continues from. A page that ran past the end of the index
// ends at the tail, but a client's cursor can lie beyond it — the index may have been
// purged or have expired while the client was deep in the feed — and seeding from such
// a cursor would leave a gap between the tail and the cursor that no later read fills.
func (cr *Timeline[T]) Tail(ctx context.Context, keyParams ...string) (Cursor, bool, error) {
	key, err := cr.sortedSetClient.key(keyParams)
	if err != nil {
		return Cursor{}, false, err
	}

	// ZRANGE order is the reverse of ZREVRANGE order, member ties included, so the last
	// element of a descending read is the first element of an ascending one.
	rank := int64(0)
	if cr.direction == Ascending {
		rank = -1
	}

	result, err := cr.client.ZRangeWithScores(ctx, key, rank, rank).Result()
	if err != nil {
		return Cursor{}, false, err
	}
	if len(result) == 0 {
		return Cursor{}, false, nil
	}

	member, ok := result[0].Member.(string)
	if !ok {
		return Cursor{}, false, fmt.Errorf("redifu: unexpected member type %T", result[0].Member)
	}
	return Cursor{Score: result[0].Score, Member: member}, true, nil
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

// ReachedEnd reports whether the index already holds the end of the data, so that a
// page running past its last member needs no top-up: the collection was confirmed empty
// (MarkEmpty), or it holds members and a seed found nothing after them (MarkLastPage).
// One round trip.
//
// A :lastpage marker on an index that holds nothing is ignored. It is left over from
// the index's previous life — markers set by a later top-up outlive an index whose TTL
// was set by its head seed — and trusting it would end the feed at whatever the next
// head seed loads.
func (cr *Timeline[T]) ReachedEnd(ctx context.Context, keyParams ...string) (bool, error) {
	key, err := cr.sortedSetClient.key(keyParams)
	if err != nil {
		return false, err
	}

	// The markers hang off the hash-tagged collection key, so all three reads land on
	// one node even on a cluster.
	pipe := cr.client.Pipeline()
	count := pipe.ZCard(ctx, key)
	lastPage := pipe.Get(ctx, key+markerLastPage)
	blankPage := pipe.Get(ctx, key+markerBlankPage)
	if _, errPipe := pipe.Exec(ctx); errPipe != nil && !errors.Is(errPipe, redis.Nil) {
		return false, errPipe
	}

	members, errCount := count.Result()
	if errCount != nil {
		return false, errCount
	}

	isSet := func(marker *redis.StringCmd) (bool, error) {
		value, errMarker := marker.Result()
		if errors.Is(errMarker, redis.Nil) {
			return false, nil
		}
		return value == "1", errMarker
	}

	if members == 0 {
		return isSet(blankPage)
	}
	return isSet(lastPage)
}

func (cr *Timeline[T]) HasData(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return cr.clearMarker(ctx, pipe, markerBlankPage, keyParams)
}

// Fetch reads the page that follows a randId cursor: the first of lastRandId, newest
// first, that is still a member of the index. When none of them is, it returns
// ErrResetPagination. An empty lastRandId reads the first page.
func (cr *Timeline[T]) Fetch(lastRandId []string) *timelineFetchBuilder[T] {
	return &timelineFetchBuilder[T]{
		timeline:    cr,
		lastRandIds: appendParams(nil, lastRandId...),
	}
}

// FetchFrom reads the page that follows cursor in this timeline's order — score
// descending then member descending for a Descending timeline (ZREVRANGE order), score
// ascending then member ascending for an Ascending one (ZRANGE order) — whether or not
// the cursor's member is still in the index. An item that left the index does not move
// the position, so the client carries on where it was instead of starting over: unlike
// Fetch, FetchFrom never returns ErrResetPagination.
//
// The zero Cursor reads the first page. An empty Member sorts below every member that
// shares the cursor's score, so Cursor{Score: s} reads the items scored below s on a
// Descending timeline — everything older than s — and from s upwards on an Ascending one.
//
// Everything else — the one-extra-member HasMore check, hydration, relations, the
// processor, self-healing — is the same as Fetch.
func (cr *Timeline[T]) FetchFrom(cursor Cursor) *timelineFetchBuilder[T] {
	return &timelineFetchBuilder[T]{
		timeline:     cr,
		position:     cursor,
		fromPosition: true,
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

type timelineFetchBuilder[T Blueprint] struct {
	timeline      *Timeline[T]
	lastRandIds   []string
	position      Cursor
	fromPosition  bool
	pageSize      int64
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

// WithLimit reads a page of n items for this fetch instead of the timeline's itemPerPage,
// with the same one-extra-member HasMore check, so one index serves a per_page of 20 and
// of 100 without a Timeline per page size. An n below 1 keeps itemPerPage; FetchAll
// ignores it. Bound n yourself when it comes from a client.
//
// A top-up after a page read this way loads n + 1 - len(items) rows: the page size of
// the read it completes, not itemPerPage.
func (b *timelineFetchBuilder[T]) WithLimit(n int64) *timelineFetchBuilder[T] {
	b.pageSize = n
	return b
}

// cursorPage asks Redis for the page that follows one of the supplied randId cursors.
// The candidates are tried newest first; the first one still present in the index wins,
// and is returned as a Cursor so an empty page can still report where it ended.
func (b *timelineFetchBuilder[T]) cursorPage(ctx context.Context, key string, limit int64) ([]string, []float64, Cursor, bool, error) {
	for i := len(b.lastRandIds) - 1; i >= 0; i-- {
		candidate := b.lastRandIds[i]
		if candidate == "" {
			continue
		}

		result, err := timelineCursorScript.Eval(
			ctx, b.timeline.client,
			[]string{key},
			candidate, limit, b.timeline.direction,
		).StringSlice()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				// This cursor is no longer a member of the index. Try an older one.
				continue
			}
			return nil, nil, Cursor{}, false, err
		}
		if len(result) == 0 {
			return nil, nil, Cursor{}, false, errors.New("redifu: cursor script returned no cursor score")
		}

		cursorScore, errScore := strconv.ParseFloat(result[0], 64)
		if errScore != nil {
			return nil, nil, Cursor{}, false, fmt.Errorf("redifu: unreadable cursor score %q: %w", result[0], errScore)
		}

		members, scores, errPairs := scoredMembers(result[1:])
		if errPairs != nil {
			return nil, nil, Cursor{}, false, errPairs
		}

		return members, scores, Cursor{Score: cursorScore, Member: candidate}, true, nil
	}

	return nil, nil, Cursor{}, false, nil
}

// positionPage asks Redis for the page that follows the builder's Cursor, in one script.
func (b *timelineFetchBuilder[T]) positionPage(ctx context.Context, key string, limit int64) ([]string, []float64, error) {
	if math.IsNaN(b.position.Score) {
		return nil, nil, fmt.Errorf("%w: the score is not a number", ErrInvalidCursor)
	}

	result, err := timelinePositionScript.Eval(
		ctx, b.timeline.client,
		[]string{key},
		formatScore(b.position.Score), b.position.Member, limit, b.timeline.direction,
	).StringSlice()
	if err != nil {
		return nil, nil, err
	}

	return scoredMembers(result)
}

func (b *timelineFetchBuilder[T]) Exec(ctx context.Context) *FetchOutput[T] {
	key, errKey := b.timeline.sortedSetClient.key(b.params)
	if errKey != nil {
		return &FetchOutput[T]{error: errKey}
	}

	// One extra member tells us whether a further page exists without a second query.
	limit := b.timeline.itemPerPage
	if b.pageSize >= 1 {
		limit = b.pageSize
	}
	var members []string
	var scores []float64
	var usedCursor bool
	// after is the position this page was read after. An empty page ends there.
	var after Cursor

	switch {
	case b.fetchAll:
		found, foundScores, err := b.timeline.sortedSetClient.rangeWithScores(ctx, b.timeline.direction, 0, -1, b.params)
		if err != nil {
			return &FetchOutput[T]{error: err}
		}
		members, scores = found, foundScores

	case b.fromPosition && !b.position.IsZero():
		found, foundScores, err := b.positionPage(ctx, key, limit+1)
		if err != nil {
			return &FetchOutput[T]{error: err}
		}
		members, scores = found, foundScores
		usedCursor = true
		after = b.position

	case len(b.lastRandIds) > 0:
		found, foundScores, resolved, ok, err := b.cursorPage(ctx, key, limit+1)
		if err != nil {
			return &FetchOutput[T]{error: err}
		}
		if !ok {
			// Every cursor the client held has left the index — it was purged, it
			// expired, or those items were removed. Restarting is the only correct
			// answer; silently serving page one would repeat items the client has.
			return &FetchOutput[T]{error: ErrResetPagination}
		}
		members, scores = found, foundScores
		usedCursor = true
		after = resolved

	default:
		found, foundScores, err := b.timeline.sortedSetClient.rangeWithScores(ctx, b.timeline.direction, 0, limit, b.params)
		if err != nil {
			return &FetchOutput[T]{error: err}
		}
		members, scores = found, foundScores
	}

	// hasMore is decided by what the index holds, before any item is loaded. Deriving
	// it from the number of hydrated items would report the end of the feed whenever a
	// few items in the middle of a page had expired out of Base.
	hasMore := !b.fetchAll && int64(len(members)) > limit
	if hasMore {
		members = members[:limit]
		scores = scores[:limit]
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
	nextCursor := after
	if len(members) > 0 {
		validLastId = members[len(members)-1]
		nextCursor = Cursor{Score: scores[len(scores)-1], Member: validLastId}
	}

	return &FetchOutput[T]{
		items:       items,
		validLastId: validLastId,
		nextCursor:  nextCursor,
		position:    position,
		firstPage:   !usedCursor,
		hasMore:     hasMore,
		dangling:    dangling,
	}
}

// RequiresSeeding is the seeding check of the original Timeline flow, kept for existing
// callers. It cannot drive a feed: the handler it was documented with seeded the first
// page only, so "load more" stopped after page one, and it reports a warm index without
// markers as needing a seed on every first-page read.
//
// Deprecated: use the top-up flow — FetchFrom, then, while the page has no HasMore and
// the index has not ReachedEnd, seed from Tail and read again. CLAUDE.consumer.md has
// the handler.
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
