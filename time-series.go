package redifu

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type TimeSeriesWithPipeline[T Blueprint] struct {
	timeSeries *TimeSeries[T]
	pipe       redis.Pipeliner
}

func (t *TimeSeriesWithPipeline[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	_, err := t.timeSeries.addItem(ctx, t.pipe, item, keyParams...)
	return err
}

func (t *TimeSeriesWithPipeline[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return t.timeSeries.sorted.WithPipeline(t.pipe).RemoveItem(ctx, item, keyParams...)
}

type TimeSeries[T Blueprint] struct {
	redis      redis.UniversalClient
	segments   *keyBuilder
	timeToLive time.Duration
	sorted     *Sorted[T]
	// unit is what an int64 score counts since the Unix epoch. A time.Time sorting
	// reference is always scored in milliseconds.
	unit time.Duration
}

// markerSegments is the suffix of a TimeSeries' segment store when the series has a generation:
// the store then sits under the index's hash tag, as one of its markers, so that every check that
// drops the index drops the ranges it claimed with it.
const markerSegments = ":segments"

func NewTimeSeries[T Blueprint](
	client redis.UniversalClient,
	baseClient *Base[T],
	keyFormat string,
	timeToLive time.Duration,
) (*TimeSeries[T], error) {
	if client == nil {
		return nil, errors.New("redifu: time series client must not be nil")
	}

	keys, err := newKeyBuilder(keyFormat)
	if err != nil {
		return nil, err
	}

	sorted, errSorted := NewSorted[T](client, baseClient, keyFormat, timeToLive)
	if errSorted != nil {
		return nil, errSorted
	}
	// With a generation the segment store is the last marker before the generation marker (the
	// stamp's "segment" op writes KEYS[#KEYS - 1]).
	sorted.sortedSetClient.markers = []string{markerBlankPage, markerSegments}

	return &TimeSeries[T]{
		redis:      client,
		sorted:     sorted,
		segments:   keys.suffixed(":segments"),
		timeToLive: timeToLive,
		unit:       time.Millisecond,
	}, nil
}

func (s *TimeSeries[T]) AddRelation(relations ...Relation[T]) {
	s.sorted.AddRelation(relations...)
}

func (s *TimeSeries[T]) GetRelations() []Relation[T] {
	return s.sorted.GetRelations()
}

// SetSortingReference chooses the struct field used as the timestamp. TimeSeries reads
// this field to decide which seeded segment an item belongs to, but had no way to set
// it — every series was pinned to createdAt whatever the consumer intended.
//
// A time field is scored in milliseconds, so it is refused while SetScoreUnit has set
// another unit.
func (s *TimeSeries[T]) SetSortingReference(sortingReference string) error {
	resolved, err := newScorer[T](sortingReference)
	if err != nil {
		return err
	}
	if s.unit != time.Millisecond && resolved.kind != scoreFromInt64 {
		return fmt.Errorf("redifu: sorting reference %q is a time and is scored in milliseconds, but the score unit is %v — only an int64 reference can count another unit", sortingReference, s.unit)
	}
	s.sorted.scorer = resolved
	return nil
}

// SetScoreUnit declares what an int64 sorting reference counts since the Unix epoch:
// time.Millisecond (the default), time.Second or time.Microsecond. The series uses it
// to turn an item's score into the instant whose segment must cover it, and to turn the
// bounds of Fetch into a score range. Without it, a Unix-seconds field was read as
// milliseconds: every item landed in January 1970, outside every segment and every
// range.
//
// A time.Time sorting reference, createdAt included, is always scored in milliseconds.
// Another unit is refused while a time field is the sorting reference, and a series
// left on createdAt reports the mismatch on its first AddItem or Fetch.
func (s *TimeSeries[T]) SetScoreUnit(unit time.Duration) error {
	switch unit {
	case time.Second, time.Millisecond, time.Microsecond:
	default:
		return fmt.Errorf("redifu: score unit must be time.Second, time.Millisecond or time.Microsecond, got %v", unit)
	}
	if unit != time.Millisecond && (s.sorted.scorer.kind == scoreFromTime || s.sorted.scorer.kind == scoreFromTimePtr) {
		return fmt.Errorf("redifu: sorting reference %q is a time and is scored in milliseconds — set an int64 reference to count in %v", s.sorted.scorer.reference, unit)
	}
	s.unit = unit
	return nil
}

// checkUnit reports a score unit that cannot apply: one other than milliseconds on a
// series still scored by createdAt.
func (s *TimeSeries[T]) checkUnit() error {
	if s.unit != time.Millisecond && s.sorted.scorer.kind != scoreFromInt64 {
		return fmt.Errorf("redifu: score unit %v needs an int64 sorting reference — call SetSortingReference", s.unit)
	}
	return nil
}

// instantOf turns a score back into the instant it stands for.
func (s *TimeSeries[T]) instantOf(score float64) time.Time {
	value := int64(score)
	switch s.unit {
	case time.Second:
		return time.Unix(value, 0).UTC()
	case time.Microsecond:
		return time.UnixMicro(value).UTC()
	}
	return time.UnixMilli(value).UTC()
}

// scoreAt turns an instant into the score that stands for it, truncated to the unit.
func (s *TimeSeries[T]) scoreAt(instant time.Time) int64 {
	switch s.unit {
	case time.Second:
		return instant.Unix()
	case time.Microsecond:
		return instant.UnixMicro()
	}
	return instant.UnixMilli()
}

func (s *TimeSeries[T]) SetSelfHeal(selfHeal bool) {
	s.sorted.SetSelfHeal(selfHeal)
}

// SetGeneration stamps the series with a generation, as Sorted.SetGeneration does. Its segment
// store — which ranges are seeded — then lives under the index's hash tag ({K}:segments, instead of
// the untagged K:segments of a series without a generation) and counts as one of the index's
// markers: whatever drops an index of another generation drops the ranges it claimed in the same
// command, so they read as gaps and are seeded again, and a seed restamps the two together. Every
// segment read is behind the check, and AddItem decides coverage and writes in one script. "" turns
// it off again. It may be changed while the series is in use.
func (s *TimeSeries[T]) SetGeneration(generation string) error {
	return s.sorted.SetGeneration(generation)
}

// Generation is the generation SetGeneration set, "" for none.
func (s *TimeSeries[T]) Generation() string {
	return s.sorted.Generation()
}

// generation is the series' generation as one operation reads it.
func (s *TimeSeries[T]) generation() string {
	return s.sorted.sortedSetClient.generation()
}

func (s *TimeSeries[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) error {
	return s.sorted.SetExpiration(ctx, pipe, keyParams...)
}

func (s *TimeSeries[T]) Count(ctx context.Context, keyParams ...string) (int64, error) {
	return s.sorted.Count(ctx, keyParams...)
}

func (s *TimeSeries[T]) WithPipeline(pipe redis.Pipeliner) *TimeSeriesWithPipeline[T] {
	return &TimeSeriesWithPipeline[T]{
		timeSeries: s,
		pipe:       pipe,
	}
}

func (s *TimeSeries[T]) addItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) (bool, error) {
	if err := s.checkUnit(); err != nil {
		return false, err
	}

	itemScore, errGetScore := s.sorted.scorer.score(item)
	if errGetScore != nil {
		return false, errGetScore
	}

	key, errKey := s.sorted.sortedSetClient.key(keyParams)
	if errKey != nil {
		return false, errKey
	}

	generation := s.generation()
	if generation != "" {
		return s.addItemGeneration(ctx, pipe, generation, item, itemScore, keyParams)
	}

	// The one read this path makes outside the caller's pipeline, before anything is
	// enqueued. Without a generation the segment store sits under an untagged key, in another
	// cluster slot than the index, so no script could read it and write the index atomically.
	covered, errCovered := s.Covers(ctx, s.instantOf(itemScore), keyParams...)
	if errCovered != nil {
		return false, errCovered
	}

	// The caller's pipeline is used when there is one. This branch used to be
	// inverted: a caller-supplied pipeline was ignored and a second pipeline was
	// executed behind the caller's back, which broke both ordering and atomicity.
	var selfPipe bool
	if pipe == nil {
		pipe = s.redis.Pipeline()
		selfPipe = true
	}

	// Base holds the item whether or not the series indexes it, as with every other
	// collection: written only if absent, its TTL refreshed either way, so an index can
	// never outlive the items it points at.
	if err := s.sorted.baseClient.WithPipeline(pipe).SetIfAbsent(ctx, item); err != nil {
		return false, err
	}

	if covered {
		// A series is seeded where its segments say so, not where its sorted set holds
		// members: a range seeded while it had no rows leaves segments and no sorted set
		// at all. So a covered item is added whatever ZCARD says — the Sorted ingest
		// script, which waits for a non-empty set, dropped the first item of every such
		// range. This ZADD may be the one that creates the set, so the TTL is applied
		// too, and the index keeps living at least as long as its segments.
		pipe.ZAdd(ctx, key, redis.Z{Score: itemScore, Member: item.GetRandId()})
		if s.timeToLive > 0 {
			pipe.Expire(ctx, key, s.timeToLive)
		}
	}

	if selfPipe {
		if _, errPipe := pipe.Exec(ctx); errPipe != nil && !errors.Is(errPipe, redis.Nil) {
			return false, errPipe
		}
	}

	return covered, nil
}

// addItemGeneration is addItem with a generation: the segment store shares the index's slot, so
// one script checks the generation, decides whether a segment covers the item and places it.
// Through a caller's pipeline the outcome is only known after Exec, and false is returned.
func (s *TimeSeries[T]) addItemGeneration(ctx context.Context, pipe redis.Pipeliner, generation string, item T, itemScore float64, keyParams []string) (bool, error) {
	keys, err := s.sorted.sortedSetClient.generationKeys(keyParams)
	if err != nil {
		return false, err
	}

	var selfPipe bool
	if pipe == nil {
		pipe = s.redis.Pipeline()
		selfPipe = true
	}

	if err := s.sorted.baseClient.WithPipeline(pipe).SetIfAbsent(ctx, item); err != nil {
		return false, err
	}
	placed := generationTimeSeriesAddScript.Eval(ctx, pipe, keys,
		formatScore(itemScore), item.GetRandId(), s.instantOf(itemScore).UnixMilli(), ttlMillis(s.timeToLive), generation)

	if !selfPipe {
		return false, nil
	}
	if _, errPipe := pipe.Exec(ctx); errPipe != nil && !errors.Is(errPipe, redis.Nil) {
		return false, errPipe
	}
	own, err := placed.Int64()
	if err != nil {
		return false, err
	}
	return own == 1, nil
}

// AddItem stores the item in Base if it is not there yet, always refreshes its TTL, and
// places it into the series when its timestamp falls inside a seeded segment — also when
// that range was seeded while it held no rows. It returns ErrNotIngested when the
// timestamp is outside every seeded range, or the series is not seeded at all.
func (s *TimeSeries[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	ingested, err := s.addItem(ctx, nil, item, keyParams...)
	if err != nil {
		return err
	}
	if !ingested {
		return ErrNotIngested
	}
	return nil
}

// IngestItem places an item into the series while seeding. It also refreshes the index
// TTL, so that the sorted set has one whatever order a seeder enqueues its commands in:
// an EXPIRE queued by AddSegment before the first ZADD of a new series finds no key.
func (s *TimeSeries[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	if err := s.sorted.IngestItem(ctx, pipe, item, true, keyParams...); err != nil {
		return err
	}
	if s.generation() != "" {
		// The stamped expire also renews the generation marker with the index.
		return s.sorted.SetExpiration(ctx, pipe, keyParams...)
	}
	if s.timeToLive > 0 {
		key, err := s.sorted.sortedSetClient.key(keyParams)
		if err != nil {
			return err
		}
		pipe.Expire(ctx, key, s.timeToLive)
	}
	return nil
}

func (s *TimeSeries[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return s.sorted.RemoveItem(ctx, item, keyParams...)
}

// AddSegment stores a seeded segment range in the segment store.
// Validates that the new segment doesn't overlap with any existing segment.
// Segments are stored in a Redis hash: field = lowerbound, value = upperbound.
//
// It refreshes the TTL of the segment store and of the index together. The two used to
// drift apart: every AddSegment renewed the segments while the index kept the TTL of
// its first seed, so the index expired first and Fetch answered the ranges its segments
// still claimed with nothing, and no gap to seed. Enqueue it after the IngestItem calls
// of the range it records.
func (s *TimeSeries[T]) AddSegment(ctx context.Context, pipe redis.Pipeliner, lowerbound time.Time, upperbound time.Time, segmentStoreKeyParams ...string) error {
	if !lowerbound.Before(upperbound) {
		return fmt.Errorf("redifu: invalid range: lowerbound (%s) must be less than upperbound (%s)", lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339))
	}

	if err := s.validateNoOverlap(ctx, lowerbound, upperbound, segmentStoreKeyParams...); err != nil {
		return err
	}

	if generation := s.generation(); generation != "" {
		// A seed write: the stamp drops an index of another generation (its segments with it),
		// stamps the marker — even when the range holds no rows and no sorted set exists, the
		// marker is what says the range was seeded by this generation — records the range and
		// renews the TTL of the store and the index.
		return s.sorted.sortedSetClient.stamp(ctx, pipe, generation, s.timeToLive, "segment",
			strconv.FormatInt(lowerbound.UnixMilli(), 10), upperbound.UnixMilli(), segmentStoreKeyParams)
	}

	segmentStoreKey, errKey := s.segments.build(segmentStoreKeyParams)
	if errKey != nil {
		return errKey
	}
	indexKey, errIndexKey := s.sorted.sortedSetClient.key(segmentStoreKeyParams)
	if errIndexKey != nil {
		return errIndexKey
	}

	pipe.HSet(ctx, segmentStoreKey, strconv.FormatInt(lowerbound.UnixMilli(), 10), upperbound.UnixMilli())

	if s.timeToLive > 0 {
		pipe.Expire(ctx, segmentStoreKey, s.timeToLive)
		// The index goes with its segments, never before them. An index that outlives
		// its segments costs a reseed at worst; segments that outlive the index claim
		// ranges it no longer holds.
		pipe.Expire(ctx, indexKey, s.timeToLive)
	}

	return nil
}

func (s *TimeSeries[T]) allSegments(ctx context.Context, keyParams []string) ([][]int64, error) {
	var result map[string]string
	if generation := s.generation(); generation != "" {
		storeKey, errKey := s.sorted.sortedSetClient.markerKey(markerSegments, keyParams)
		if errKey != nil {
			return nil, errKey
		}
		var read *redis.MapStringStringCmd
		if _, err := s.sorted.sortedSetClient.guarded(ctx, generation, keyParams, func(pipe redis.Pipeliner) error {
			read = pipe.HGetAll(ctx, storeKey)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("redifu: failed to read segments: %w", err)
		}
		pairs, err := read.Result()
		if err != nil {
			return nil, fmt.Errorf("redifu: failed to read segments: %w", err)
		}
		result = pairs
	} else {
		segmentStoreKey, errKey := s.segments.build(keyParams)
		if errKey != nil {
			return nil, errKey
		}
		read, err := s.redis.HGetAll(ctx, segmentStoreKey).Result()
		if err != nil {
			return nil, fmt.Errorf("redifu: failed to read segments: %w", err)
		}
		result = read
	}

	return parseSegments(result), nil
}

// parseSegments turns a segment store (lowerbound → upperbound, in milliseconds) into ranges
// sorted by lower bound, skipping any field that is not one.
func parseSegments(result map[string]string) [][]int64 {
	segments := make([][]int64, 0, len(result))
	for lowerStr, upperStr := range result {
		lower, err1 := strconv.ParseInt(lowerStr, 10, 64)
		upper, err2 := strconv.ParseInt(upperStr, 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		segments = append(segments, []int64{lower, upper})
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i][0] < segments[j][0] })
	return segments
}

// validateNoOverlap ensures the new segment doesn't overlap with any existing segment.
func (s *TimeSeries[T]) validateNoOverlap(ctx context.Context, lowerbound time.Time, upperbound time.Time, keyParams ...string) error {
	segments, err := s.allSegments(ctx, keyParams)
	if err != nil {
		return err
	}

	lowerboundUnix := lowerbound.UnixMilli()
	upperboundUnix := upperbound.UnixMilli()

	for _, segment := range segments {
		if upperboundUnix > segment[0] && segment[1] > lowerboundUnix {
			return fmt.Errorf(
				"redifu: segment [%s, %s] overlaps with existing segment [%d, %d]",
				lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339), segment[0], segment[1],
			)
		}
	}

	return nil
}

// Scan retrieves all segments that intersect with the specified range, sorted by lower
// bound.
func (s *TimeSeries[T]) Scan(ctx context.Context, lowerbound time.Time, upperbound time.Time, keyParams ...string) (*[][]int64, error) {
	all, err := s.allSegments(ctx, keyParams)
	if err != nil {
		return nil, err
	}

	lowerboundUnix := lowerbound.UnixMilli()
	upperboundUnix := upperbound.UnixMilli()

	segments := make([][]int64, 0, len(all))
	for _, segment := range all {
		if segment[1] > lowerboundUnix && segment[0] < upperboundUnix {
			segments = append(segments, segment)
		}
	}

	return &segments, nil
}

// Covers reports whether a single instant falls inside a seeded segment. Scan cannot
// answer this: it compares ranges with strict inequalities, so a point query for an
// instant sitting exactly on a segment boundary intersects nothing and the item would
// be rejected from a range that does in fact cover it.
func (s *TimeSeries[T]) Covers(ctx context.Context, instant time.Time, keyParams ...string) (bool, error) {
	segments, err := s.allSegments(ctx, keyParams)
	if err != nil {
		return false, err
	}

	at := instant.UnixMilli()
	for _, segment := range segments {
		if segment[0] <= at && at <= segment[1] {
			return true, nil
		}
	}

	return false, nil
}

// FindGap identifies gaps between seeded segments within the specified range.
// Since segments are guaranteed non-overlapping (enforced by AddSegment), no merging
// is needed.
func (s *TimeSeries[T]) FindGap(ctx context.Context, lowerbound time.Time, upperbound time.Time, keyParams ...string) ([][]int64, error) {
	if !lowerbound.Before(upperbound) {
		return [][]int64{}, fmt.Errorf("redifu: invalid range: lowerbound (%s) must be less than upperbound (%s)", lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339))
	}

	segments, err := s.Scan(ctx, lowerbound, upperbound, keyParams...)
	if err != nil {
		return nil, err
	}

	lowerboundUnix := lowerbound.UnixMilli()
	upperboundUnix := upperbound.UnixMilli()

	if segments == nil || len(*segments) == 0 {
		return [][]int64{{lowerboundUnix, upperboundUnix}}, nil
	}

	return s.calculateGaps(*segments, lowerboundUnix, upperboundUnix), nil
}

func (s *TimeSeries[T]) calculateGaps(segments [][]int64, lowerbound, upperbound int64) [][]int64 {
	gaps := make([][]int64, 0)

	if len(segments) == 0 {
		return [][]int64{{lowerbound, upperbound}}
	}

	if segments[0][0] > lowerbound {
		gaps = append(gaps, []int64{lowerbound, segments[0][0]})
	}

	for i := 0; i < len(segments)-1; i++ {
		if segments[i][1] < segments[i+1][0] {
			gaps = append(gaps, []int64{segments[i][1], segments[i+1][0]})
		}
	}

	if segments[len(segments)-1][1] < upperbound {
		gaps = append(gaps, []int64{segments[len(segments)-1][1], upperbound})
	}

	return gaps
}

// Fetch retrieves data from seeded segments within the specified range.
func (s *TimeSeries[T]) Fetch(lowerbound time.Time, upperbound time.Time) *fetchTimeSeriesBuilder[T] {
	return &fetchTimeSeriesBuilder[T]{
		timeSeries: s,
		lowerbound: lowerbound,
		upperbound: upperbound,
	}
}

// Remove deletes a segment from the segment store by its lowerbound. With a generation it is
// behind the guard: a store of another generation is dropped with its index instead.
func (s *TimeSeries[T]) Remove(ctx context.Context, lowerbound time.Time, segmentStoreKeyParams ...string) error {
	field := strconv.FormatInt(lowerbound.UnixMilli(), 10)
	if generation := s.generation(); generation != "" {
		keys, err := s.sorted.sortedSetClient.generationKeys(segmentStoreKeyParams)
		if err != nil {
			return err
		}
		return generationRemoveSegmentScript.Eval(ctx, s.redis, keys, field, generation).Err()
	}

	segmentStoreKey, errKey := s.segments.build(segmentStoreKeyParams)
	if errKey != nil {
		return errKey
	}

	return s.redis.HDel(ctx, segmentStoreKey, field).Err()
}

// Purge invalidates the series: the sorted set, its blank-page marker and the record
// of which ranges have been seeded are all dropped, so the next fetch rebuilds from
// the database. Item keys are left alone. Without this, purging the sorted set alone
// left the segment store claiming ranges that no longer held any data. The sorted set's
// keys include the generation marker and the tagged segment store of a series with a
// generation; the untagged store is the one of a series without.
func (s *TimeSeries[T]) Purge(ctx context.Context, keyParams ...string) error {
	if err := s.sorted.Purge(ctx, keyParams...); err != nil {
		return err
	}

	segmentStoreKey, errKey := s.segments.build(keyParams)
	if errKey != nil {
		return errKey
	}

	return s.redis.Del(ctx, segmentStoreKey).Err()
}

// GetSegments retrieves all stored segments, sorted by lower bound.
func (s *TimeSeries[T]) GetSegments(ctx context.Context, keyParams ...string) ([][]int64, error) {
	return s.allSegments(ctx, keyParams)
}

// Exists checks if a segment with the given lowerbound exists.
func (s *TimeSeries[T]) Exists(ctx context.Context, lowerbound time.Time, keyParams ...string) (bool, error) {
	if s.generation() != "" {
		segments, err := s.allSegments(ctx, keyParams)
		if err != nil {
			return false, err
		}
		for _, segment := range segments {
			if segment[0] == lowerbound.UnixMilli() {
				return true, nil
			}
		}
		return false, nil
	}

	segmentStoreKey, errKey := s.segments.build(keyParams)
	if errKey != nil {
		return false, errKey
	}

	return s.redis.HExists(ctx, segmentStoreKey, strconv.FormatInt(lowerbound.UnixMilli(), 10)).Result()
}

// CountSegments returns the total number of segments stored.
func (s *TimeSeries[T]) CountSegments(ctx context.Context, keyParams ...string) (int64, error) {
	if s.generation() != "" {
		segments, err := s.allSegments(ctx, keyParams)
		if err != nil {
			return 0, err
		}
		return int64(len(segments)), nil
	}

	segmentStoreKey, errKey := s.segments.build(keyParams)
	if errKey != nil {
		return 0, errKey
	}

	return s.redis.HLen(ctx, segmentStoreKey).Result()
}

type fetchTimeSeriesBuilder[T Blueprint] struct {
	timeSeries    *TimeSeries[T]
	lowerbound    time.Time
	upperbound    time.Time
	keyParams     []string
	processor     func(item *T, args []interface{})
	processorArgs []interface{}
}

func (f *fetchTimeSeriesBuilder[T]) WithParams(keyParams ...string) *fetchTimeSeriesBuilder[T] {
	f.keyParams = appendParams(nil, keyParams...)
	return f
}

func (f *fetchTimeSeriesBuilder[T]) WithProcessor(processor func(item *T, args []interface{}), processorArgs ...interface{}) *fetchTimeSeriesBuilder[T] {
	f.processor = processor
	f.processorArgs = processorArgs
	return f
}

// Exec returns the items in range. The second return value reports that the range is
// not fully seeded, in which case no items are returned and the caller should seed the
// gaps first.
func (f *fetchTimeSeriesBuilder[T]) Exec(ctx context.Context) ([]T, bool, error) {
	if !f.lowerbound.Before(f.upperbound) {
		return nil, false, fmt.Errorf("redifu: invalid range: lowerbound (%s) must be less than upperbound (%s)", f.lowerbound.Format(time.RFC3339), f.upperbound.Format(time.RFC3339))
	}
	if err := f.timeSeries.checkUnit(); err != nil {
		return nil, false, err
	}

	if generation := f.timeSeries.generation(); generation != "" {
		return f.execGeneration(ctx, generation)
	}

	gaps, errFindGaps := f.timeSeries.FindGap(ctx, f.lowerbound, f.upperbound, f.keyParams...)
	if errFindGaps != nil {
		return nil, false, errFindGaps
	}
	if len(gaps) > 0 {
		return nil, true, nil
	}

	// processorArgs is forwarded with ... — without it the consumer's arguments arrive
	// wrapped in one more layer of slice than they passed. The bounds are turned into
	// scores in the series' unit; they used to be milliseconds whatever the scores were.
	result, errFetch := f.timeSeries.sorted.Fetch(Descending).
		WithParams(f.keyParams...).
		WithRange(f.timeSeries.scoreAt(f.lowerbound), f.timeSeries.scoreAt(f.upperbound)).
		WithProcessor(f.processor, f.processorArgs...).
		Exec(ctx)
	if errFetch != nil {
		return nil, false, errFetch
	}

	return result, false, nil
}

// execGeneration is Exec for a series with a generation. The segment store and the index share a
// slot, so the segments, the members and the check that both are this generation's are read in one
// MULTI/EXEC: an index of another generation (or none) is dropped with its segments by the check,
// and the range then reads as a gap, never as seeded and empty.
func (f *fetchTimeSeriesBuilder[T]) execGeneration(ctx context.Context, generation string) ([]T, bool, error) {
	series := f.timeSeries
	set := series.sorted.sortedSetClient
	key, err := set.key(f.keyParams)
	if err != nil {
		return nil, false, err
	}
	storeKey, err := set.markerKey(markerSegments, f.keyParams)
	if err != nil {
		return nil, false, err
	}

	var stored *redis.MapStringStringCmd
	var members *redis.StringSliceCmd
	own, err := set.guarded(ctx, generation, f.keyParams, func(pipe redis.Pipeliner) error {
		stored = pipe.HGetAll(ctx, storeKey)
		members = pipe.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
			Min: formatScore(float64(series.scoreAt(f.lowerbound))),
			Max: formatScore(float64(series.scoreAt(f.upperbound))),
		})
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !own {
		return nil, true, nil
	}

	pairs, err := stored.Result()
	if err != nil {
		return nil, false, fmt.Errorf("redifu: failed to read segments: %w", err)
	}
	lower, upper := f.lowerbound.UnixMilli(), f.upperbound.UnixMilli()
	intersecting := make([][]int64, 0)
	for _, segment := range parseSegments(pairs) {
		if segment[1] > lower && segment[0] < upper {
			intersecting = append(intersecting, segment)
		}
	}
	if len(series.calculateGaps(intersecting, lower, upper)) > 0 {
		return nil, true, nil
	}

	found, err := members.Result()
	if err != nil {
		return nil, false, err
	}
	if len(found) == 0 {
		return nil, false, nil
	}

	items, _, err := set.hydrate(
		ctx,
		series.sorted.baseClient,
		found,
		combineRelations(series.sorted.baseClient, series.sorted.relations),
		series.sorted.baseClient.relationDepth,
		f.processor,
		f.processorArgs,
		f.keyParams,
	)
	if err != nil {
		return nil, false, err
	}
	return items, false, nil
}

// generationTimeSeriesAddScript is AddItem for a series with a generation: behind the guard (an
// index of another generation is dropped, its segments with it), it places the item when a
// segment covers its instant — both bounds inclusive, as Covers — and renews the TTL of the index
// and of its generation marker. Coverage and the write are one atomic step.
//
//	KEYS sorted set, :blankpage, :segments, generation marker
//	ARGV score, member, instant in ms, TTL in ms, generation
//
// Returns 1 if it placed the item, 0 if no segment covers it or the index was dropped.
var generationTimeSeriesAddScript = redis.NewScript(generationGuard("0") + `
local at = tonumber(ARGV[3])
local segments = redis.call('HGETALL', KEYS[#KEYS - 1])
local covered = false
for i = 1, #segments, 2 do
  local lower, upper = tonumber(segments[i]), tonumber(segments[i + 1])
  if lower and upper and lower <= at and at <= upper then
    covered = true
    break
  end
end
if not covered then
  return 0
end
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
local ttl = tonumber(ARGV[4])
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
  redis.call('PEXPIRE', KEYS[#KEYS], ttl)
end
return 1
`)

// generationRemoveSegmentScript is Remove for a series with a generation: the segment's field is
// deleted from a store of this generation; a store of another generation is dropped with its index.
//
//	KEYS sorted set, :blankpage, :segments, generation marker   ARGV lowerbound, generation
var generationRemoveSegmentScript = redis.NewScript(generationGuard("0") + `
redis.call('HDEL', KEYS[#KEYS - 1], ARGV[1])
return 1
`)
