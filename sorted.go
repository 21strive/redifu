package redifu

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	markerBlankPage = ":blankpage"
	markerFirstPage = ":firstpage"
	markerLastPage  = ":lastpage"
)

// combineRelations merges the relations declared on the entity itself with the extra
// ones declared on one index. The entity's relations travel with it everywhere, which
// is the point: a Post's author does not depend on which index the Post came through.
//
// A relation registered in both places is kept once. Registering it twice is the
// natural mistake to make while moving relations onto Base, and without this it would
// silently double every read that relation performs.
func combineRelations[T Blueprint](baseClient *Base[T], extra []Relation[T]) []Relation[T] {
	if len(extra) == 0 {
		return baseClient.relations
	}
	if len(baseClient.relations) == 0 {
		return extra
	}

	combined := make([]Relation[T], 0, len(baseClient.relations)+len(extra))
	seen := make(map[Relation[T]]struct{}, len(baseClient.relations)+len(extra))

	for _, relation := range baseClient.relations {
		if _, duplicate := seen[relation]; duplicate {
			continue
		}
		seen[relation] = struct{}{}
		combined = append(combined, relation)
	}
	for _, relation := range extra {
		if _, duplicate := seen[relation]; duplicate {
			continue
		}
		seen[relation] = struct{}{}
		combined = append(combined, relation)
	}

	return combined
}

type SortedWithPipeline[T Blueprint] struct {
	sorted   *Sorted[T]
	pipeline redis.Pipeliner
}

// AddItem enqueues the write into the caller's pipeline. Whether the item actually
// entered the index is only known once the caller executes, so unlike Sorted.AddItem
// this cannot report ErrNotIngested.
func (sw *SortedWithPipeline[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	_, err := sw.sorted.addItem(ctx, sw.pipeline, item, keyParams...)
	return err
}

func (sw *SortedWithPipeline[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return sw.sorted.removeItem(ctx, sw.pipeline, item, keyParams...)
}

type Sorted[T Blueprint] struct {
	client          redis.UniversalClient
	baseClient      *Base[T]
	sortedSetClient *SortedSet[T]
	scorer          *scorer[T]
	relations       []Relation[T]
	timeToLive      time.Duration
}

func NewSorted[T Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, timeToLive time.Duration) (*Sorted[T], error) {
	sortedSetClient := &SortedSet[T]{}
	if err := sortedSetClient.Init(client, keyFormat); err != nil {
		return nil, err
	}

	sorted := &Sorted[T]{}
	if err := sorted.Init(client, baseClient, sortedSetClient, timeToLive); err != nil {
		return nil, err
	}
	return sorted, nil
}

func (srtd *Sorted[T]) Init(client redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T], timeToLive time.Duration) error {
	if client == nil {
		return errors.New("redifu: sorted client must not be nil")
	}
	if baseClient == nil {
		return errors.New("redifu: sorted base client must not be nil")
	}

	defaultScorer, err := newScorer[T]("")
	if err != nil {
		return err
	}

	srtd.client = client
	srtd.baseClient = baseClient
	srtd.sortedSetClient = sortedSetClient
	srtd.scorer = defaultScorer
	srtd.timeToLive = timeToLive
	return nil
}

func (cr *Sorted[T]) AddRelation(relations ...Relation[T]) {
	cr.relations = append(cr.relations, relations...)
}

func (cr *Sorted[T]) GetRelations() []Relation[T] {
	return combineRelations(cr.baseClient, cr.relations)
}

// SetSortingReference chooses the struct field used as the sorted-set score. The field
// is resolved and type-checked here, once, so a typo fails at startup instead of on
// the first write, and so reflection does not run on every add.
func (srtd *Sorted[T]) SetSortingReference(sortingReference string) error {
	resolved, err := newScorer[T](sortingReference)
	if err != nil {
		return err
	}
	srtd.scorer = resolved
	return nil
}

func (srtd *Sorted[T]) SetSelfHeal(selfHeal bool) {
	srtd.sortedSetClient.SetSelfHeal(selfHeal)
}

// SetGeneration stamps the index with a generation: an index whose {K}:gen marker holds another
// generation, or none, is treated as not there. Every operation that finds one drops the sorted
// set and its markers before it does its own work, in the same command, so:
//
//   - RequiresSeeding reports true, IsEmpty false, Count 0, and a fetch returns nothing;
//   - AddItem and RemoveItem report ErrNotIngested (the dropped index is seeded again on the
//     next read, this change included);
//   - a seed — IngestItem with seed = true, MarkEmpty, HasData, SetExpiration — starts from an
//     empty index and stamps the marker, with the index TTL.
//
// Use a generation that changes whenever the rule that decides the members or their scores may
// have changed — a build's commit is the safe choice — so that two builds sharing one Redis never
// serve each other's index: each one reseeds what the other seeded, and a write by either one
// removes the other's index rather than leaving it stale. "" turns it off again. The generation
// must be at most 128 bytes without control characters. Set it before the index is used.
//
// Run each seed in one MULTI/EXEC (a TxPipeline): its first write drops another generation's
// index, so a seed whose writes were interleaved with another generation's could otherwise end
// up holding members of both.
func (srtd *Sorted[T]) SetGeneration(generation string) error {
	if err := validateGeneration(generation); err != nil {
		return err
	}
	srtd.sortedSetClient.generation = generation
	return nil
}

// Generation is the index generation SetGeneration set, "" for none.
func (srtd *Sorted[T]) Generation() string {
	return srtd.sortedSetClient.generation
}

func (srtd *Sorted[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	if srtd.sortedSetClient.generation != "" {
		return srtd.sortedSetClient.stamp(ctx, pipe, srtd.timeToLive, "expire", nil, nil, keyParams)
	}
	return srtd.sortedSetClient.SetExpiration(ctx, pipe, srtd.timeToLive, keyParams...)
}

func (srtd *Sorted[T]) Count(ctx context.Context, keyParams ...string) (int64, error) {
	return srtd.sortedSetClient.Count(ctx, keyParams...)
}

func (srtd *Sorted[T]) WithPipeline(pipe redis.Pipeliner) *SortedWithPipeline[T] {
	return &SortedWithPipeline[T]{
		sorted:   srtd,
		pipeline: pipe,
	}
}

func (srtd *Sorted[T]) addItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) (bool, error) {
	var selfPipe bool
	if pipe == nil {
		pipe = srtd.client.Pipeline()
		selfPipe = true
	}

	// Writes the item only if Base does not hold it yet, but always refreshes its TTL —
	// an index must never outlive the items it points at.
	if err := srtd.baseClient.WithPipeline(pipe).SetIfAbsent(ctx, item); err != nil {
		return false, err
	}

	ingested, errIngest := srtd.ingest(ctx, pipe, item, false, keyParams...)
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
// places it into the index.
//
// It returns ErrNotIngested when the item was stored but deliberately not indexed,
// which happens when the collection has not been seeded from the database yet. That is
// an outcome, not a failure — the item will appear once the collection is seeded — but
// it is reported rather than swallowed, so a caller can tell the two apart.
func (srtd *Sorted[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	ingested, err := srtd.addItem(ctx, nil, item, keyParams...)
	if err != nil {
		return err
	}
	if !ingested {
		return ErrNotIngested
	}
	return nil
}

// IngestItem places an item into the index. Pass seed = true while seeding: the item
// is added unconditionally. With seed = false the whole read-decide-write decision runs
// inside Redis as one atomic command, so it costs a single enqueued command and cannot
// race another writer.
func (srtd *Sorted[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, seed bool, keyParams ...string) error {
	_, err := srtd.ingest(ctx, pipe, item, seed, keyParams...)
	return err
}

func (srtd *Sorted[T]) ingest(ctx context.Context, pipe redis.Pipeliner, item T, seed bool, keyParams ...string) (*redis.Cmd, error) {
	if pipe == nil {
		return nil, errors.New("redifu: IngestItem requires a pipeline")
	}

	score, err := srtd.scorer.score(item)
	if err != nil {
		return nil, err
	}

	generation := srtd.sortedSetClient.generation

	if seed {
		if generation != "" {
			if errStamp := srtd.sortedSetClient.stamp(ctx, pipe, srtd.timeToLive, "zadd", formatScore(score), item.GetRandId(), keyParams); errStamp != nil {
				return nil, errStamp
			}
			return nil, nil
		}
		if errSet := srtd.sortedSetClient.SetItem(ctx, pipe, score, item, keyParams...); errSet != nil {
			return nil, errSet
		}
		return nil, nil
	}

	if generation != "" {
		keys, errKeys := srtd.sortedSetClient.generationKeys(keyParams)
		if errKeys != nil {
			return nil, errKeys
		}
		return generationSortedIngestScript.Eval(ctx, pipe, keys, formatScore(score), item.GetRandId(), generation), nil
	}

	key, errKey := srtd.sortedSetClient.key(keyParams)
	if errKey != nil {
		return nil, errKey
	}

	return sortedIngestScript.Eval(
		ctx, pipe,
		[]string{key, key + markerBlankPage},
		formatScore(score), item.GetRandId(),
	), nil
}

func (srtd *Sorted[T]) removeItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	var selfPipe bool
	if pipe == nil {
		selfPipe = true
		pipe = srtd.client.Pipeline()
	}

	// Only the index is touched. The item stays in Base, because it may well be a member
	// of other collections; deleting the entity itself is Base.Del's job.
	var removed *redis.Cmd
	if srtd.sortedSetClient.generation != "" {
		cmd, err := srtd.sortedSetClient.removeMembers(ctx, pipe, false, []interface{}{item.GetRandId()}, keyParams)
		if err != nil {
			return err
		}
		removed = cmd
	} else if err := srtd.sortedSetClient.RemoveItem(ctx, pipe, item, keyParams...); err != nil {
		return err
	}

	if selfPipe {
		if _, errPipe := pipe.Exec(ctx); errPipe != nil {
			return errPipe
		}
		if removed != nil {
			// The index was another generation's and has been dropped rather than changed.
			if own, err := removed.Int64(); err != nil {
				return err
			} else if own == 0 {
				return ErrNotIngested
			}
		}
	}

	return nil
}

// RemoveItem drops the item from the index. The item itself stays in Base. With a generation,
// an index of another generation is dropped whole instead, and RemoveItem reports
// ErrNotIngested.
func (srtd *Sorted[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return srtd.removeItem(ctx, nil, item, keyParams...)
}

func (srtd *Sorted[T]) Fetch(direction string) *sortedFetchBuilder[T] {
	return &sortedFetchBuilder[T]{
		direction: direction,
		sorted:    srtd,
	}
}

func (srtd *Sorted[T]) MarkEmpty(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	if srtd.sortedSetClient.generation != "" {
		return srtd.sortedSetClient.stampMarker(ctx, pipe, srtd.timeToLive, markerBlankPage, true, keyParams)
	}

	key, err := srtd.sortedSetClient.markerKey(markerBlankPage, keyParams)
	if err != nil {
		return err
	}

	pipe.Set(ctx, key, 1, srtd.timeToLive)
	return nil
}

func (srtd *Sorted[T]) HasData(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	if srtd.sortedSetClient.generation != "" {
		return srtd.sortedSetClient.stampMarker(ctx, pipe, srtd.timeToLive, markerBlankPage, false, keyParams)
	}

	key, err := srtd.sortedSetClient.markerKey(markerBlankPage, keyParams)
	if err != nil {
		return err
	}

	pipe.Del(ctx, key)
	return nil
}

func (srtd *Sorted[T]) IsEmpty(ctx context.Context, keyParams ...string) (bool, error) {
	key, errKey := srtd.sortedSetClient.markerKey(markerBlankPage, keyParams)
	if errKey != nil {
		return false, errKey
	}

	if srtd.sortedSetClient.generation != "" {
		return srtd.sortedSetClient.guardedMarker(ctx, key, keyParams)
	}

	return markerIsSet(ctx, srtd.client, key)
}

func markerIsSet(ctx context.Context, client redis.UniversalClient, key string) (bool, error) {
	result := client.Get(ctx, key)
	if result.Err() != nil {
		if errors.Is(result.Err(), redis.Nil) {
			return false, nil
		}
		return false, result.Err()
	}
	return result.Val() == "1", nil
}

func (srtd *Sorted[T]) RequiresSeeding(ctx context.Context, keyParams ...string) (bool, error) {
	if srtd.sortedSetClient.generation != "" {
		// One MULTI/EXEC: an index of another generation is dropped by the check, and the
		// marker and the count read after it then say it needs seeding.
		key, err := srtd.sortedSetClient.key(keyParams)
		if err != nil {
			return false, err
		}
		var blank *redis.StringCmd
		var count *redis.IntCmd
		if _, err := srtd.sortedSetClient.guarded(ctx, keyParams, func(pipe redis.Pipeliner) error {
			blank = pipe.Get(ctx, key+markerBlankPage)
			count = pipe.ZCard(ctx, key)
			return nil
		}); err != nil {
			return false, err
		}
		if value, errBlank := blank.Result(); errBlank == nil && value == "1" {
			return false, nil
		} else if errBlank != nil && !errors.Is(errBlank, redis.Nil) {
			return false, errBlank
		}
		members, errCount := count.Result()
		if errCount != nil {
			return false, errCount
		}
		return members == 0, nil
	}

	isBlankPage, err := srtd.IsEmpty(ctx, keyParams...)
	if err != nil {
		return false, err
	}
	if isBlankPage {
		return false, nil
	}

	count, errCount := srtd.sortedSetClient.Count(ctx, keyParams...)
	if errCount != nil {
		return false, errCount
	}

	return count == 0, nil
}

// Purge invalidates the collection: the sorted set, its state marker and its generation
// marker are dropped so the next fetch seeds it again from the database. Item keys are left
// alone.
func (srtd *Sorted[T]) Purge(ctx context.Context, keyParams ...string) error {
	// Without the blank-page marker gone the collection stays flagged as confirmed-empty and
	// RequiresSeeding keeps returning false, so a purge would silently kill it instead of
	// rebuilding it.
	keys, err := srtd.sortedSetClient.generationKeys(keyParams)
	if err != nil {
		return err
	}
	return srtd.client.Del(ctx, keys...).Err()
}

type sortedFetchBuilder[T Blueprint] struct {
	direction     string
	sorted        *Sorted[T]
	keyParams     []string
	processor     func(*T, []interface{})
	processorArgs []interface{}
	byScore       bool
	lowerbound    int64
	upperbound    int64
	byRank        bool
	rankStart     int64
	rankStop      int64
	limited       bool
	offset        int64
	count         int64
}

func (s *sortedFetchBuilder[T]) WithParams(params ...string) *sortedFetchBuilder[T] {
	s.keyParams = appendParams(nil, params...)
	return s
}

func (s *sortedFetchBuilder[T]) WithProcessor(processor func(*T, []interface{}), processorArgs ...interface{}) *sortedFetchBuilder[T] {
	s.processor = processor
	s.processorArgs = processorArgs
	return s
}

// WithRange reads the members whose score lies between lowerbound and upperbound, both
// inclusive: ZRANGEBYSCORE, or ZREVRANGEBYSCORE for Descending. It replaces an earlier
// WithRankRange on the same builder.
func (s *sortedFetchBuilder[T]) WithRange(lowerbound int64, upperbound int64) *sortedFetchBuilder[T] {
	s.byScore = true
	s.byRank = false
	s.lowerbound = lowerbound
	s.upperbound = upperbound
	return s
}

// WithRankRange reads the members at ranks start through stop, both inclusive, in the
// fetch's direction: ZRANGE, or ZREVRANGE for Descending. A negative rank counts from
// the end, as in Redis, so WithRankRange(-10, -1) is the last ten. It replaces an earlier
// WithRange on the same builder.
//
// Ranks shift when a member leaves — including a dangling member that self-healing
// removes during a read — so pages cut by rank can skip an item at a boundary. When
// pages must stay put, store the position as the score and page with WithRange.
func (s *sortedFetchBuilder[T]) WithRankRange(start int64, stop int64) *sortedFetchBuilder[T] {
	s.byRank = true
	s.byScore = false
	s.rankStart = start
	s.rankStop = stop
	return s
}

// WithLimit skips the first offset members of a score range and returns at most count of
// them: ZRANGEBYSCORE ... LIMIT offset count. Without WithRange it applies to the whole
// collection. A negative count returns every member after offset; a count of zero
// returns none. A rank range is already a limit, so combining the two is an error.
func (s *sortedFetchBuilder[T]) WithLimit(offset int64, count int64) *sortedFetchBuilder[T] {
	s.limited = true
	s.offset = offset
	s.count = count
	return s
}

// members reads the randIds the fetch covers, in its direction.
func (s *sortedFetchBuilder[T]) members(ctx context.Context) ([]string, error) {
	set := s.sorted.sortedSetClient

	switch {
	case s.byRank && s.limited:
		return nil, errors.New("redifu: WithLimit applies to a score range — a rank range is already a limit")

	case s.byRank:
		return set.rangeMembers(ctx, s.direction, s.rankStart, s.rankStop, false, s.keyParams)

	case s.limited:
		lower, upper := "-inf", "+inf"
		if s.byScore {
			lower, upper = formatScore(float64(s.lowerbound)), formatScore(float64(s.upperbound))
		}
		return set.scoreRangeMembers(ctx, s.direction, lower, upper, s.offset, s.count, s.keyParams)

	case s.byScore:
		return set.rangeMembers(ctx, s.direction, s.lowerbound, s.upperbound, true, s.keyParams)

	default:
		return set.rangeMembers(ctx, s.direction, 0, -1, false, s.keyParams)
	}
}

// Exec returns the items the fetch covers. Members whose item has expired out of Base
// are left out — and removed from the index, unless self-healing is off — so the result
// can be shorter than the range. Use ExecReport to learn whether it is.
func (s *sortedFetchBuilder[T]) Exec(ctx context.Context) ([]T, error) {
	items, _, err := s.ExecReport(ctx)
	return items, err
}

// ExecReport is Exec, also reporting how many members of the range pointed at items that
// no longer exist in Base. Exec drops them without a trace, so "this list is shorter" and
// "this list is complete" looked the same. A non-zero count means the collection is
// missing items the database may still hold — their keys expired before the index did —
// so a consumer that needs the whole list (a full collection, a month of bookings)
// reseeds it. Self-healing removes those members either way, as Exec does.
func (s *sortedFetchBuilder[T]) ExecReport(ctx context.Context) ([]T, int, error) {
	members, err := s.members(ctx)
	if err != nil {
		return nil, 0, err
	}
	if len(members) == 0 {
		return nil, 0, nil
	}

	items, dangling, errHydrate := s.sorted.sortedSetClient.hydrate(
		ctx,
		s.sorted.baseClient,
		members,
		combineRelations(s.sorted.baseClient, s.sorted.relations),
		s.sorted.baseClient.relationDepth,
		s.processor,
		s.processorArgs,
		s.keyParams,
	)
	if errHydrate != nil {
		return nil, 0, errHydrate
	}

	return items, dangling, nil
}
