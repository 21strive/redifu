package redifu

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

type TimeSeriesWithPipeline[T item.Blueprint] struct {
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

type TimeSeries[T item.Blueprint] struct {
	redis      redis.UniversalClient
	segments   *keyBuilder
	timeToLive time.Duration
	sorted     *Sorted[T]
}

func NewTimeSeries[T item.Blueprint](
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

	return &TimeSeries[T]{
		redis:      client,
		sorted:     sorted,
		segments:   keys.suffixed(":segments"),
		timeToLive: timeToLive,
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
func (s *TimeSeries[T]) SetSortingReference(sortingReference string) error {
	return s.sorted.SetSortingReference(sortingReference)
}

func (s *TimeSeries[T]) SetSelfHeal(selfHeal bool) {
	s.sorted.SetSelfHeal(selfHeal)
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
	itemScore, errGetScore := s.sorted.scorer.score(item)
	if errGetScore != nil {
		return false, errGetScore
	}

	itemTime := time.UnixMilli(int64(itemScore)).UTC()

	covered, errCovered := s.Covers(ctx, itemTime, keyParams...)
	if errCovered != nil {
		return false, errCovered
	}
	if !covered {
		// The item falls outside every seeded range, so there is nothing for it to
		// join. It is reported rather than silently dropped.
		return false, nil
	}

	// The caller's pipeline is used when there is one. This branch used to be
	// inverted: a caller-supplied pipeline was ignored and a second pipeline was
	// executed behind the caller's back, which broke both ordering and atomicity.
	return s.sorted.addItem(ctx, pipe, item, keyParams...)
}

// AddItem places an item into the series if its timestamp falls inside a seeded
// segment. It returns ErrNotIngested when the timestamp is outside every seeded range,
// or when the series itself is not seeded.
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

func (s *TimeSeries[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	return s.sorted.IngestItem(ctx, pipe, item, true, keyParams...)
}

func (s *TimeSeries[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return s.sorted.RemoveItem(ctx, item, keyParams...)
}

// AddSegment stores a seeded segment range in the segment store.
// Validates that the new segment doesn't overlap with any existing segment.
// Segments are stored in a Redis hash: field = lowerbound, value = upperbound.
func (s *TimeSeries[T]) AddSegment(ctx context.Context, pipe redis.Pipeliner, lowerbound time.Time, upperbound time.Time, segmentStoreKeyParams ...string) error {
	if !lowerbound.Before(upperbound) {
		return fmt.Errorf("redifu: invalid range: lowerbound (%s) must be less than upperbound (%s)", lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339))
	}

	if err := s.validateNoOverlap(ctx, lowerbound, upperbound, segmentStoreKeyParams...); err != nil {
		return err
	}

	segmentStoreKey, errKey := s.segments.build(segmentStoreKeyParams)
	if errKey != nil {
		return errKey
	}

	pipe.HSet(ctx, segmentStoreKey, strconv.FormatInt(lowerbound.UnixMilli(), 10), upperbound.UnixMilli())

	if s.timeToLive > 0 {
		pipe.Expire(ctx, segmentStoreKey, s.timeToLive)
	}

	return nil
}

func (s *TimeSeries[T]) allSegments(ctx context.Context, keyParams []string) ([][]int64, error) {
	segmentStoreKey, errKey := s.segments.build(keyParams)
	if errKey != nil {
		return nil, errKey
	}

	result, err := s.redis.HGetAll(ctx, segmentStoreKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redifu: failed to read segments: %w", err)
	}

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
	return segments, nil
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

// Remove deletes a segment from the segment store by its lowerbound.
func (s *TimeSeries[T]) Remove(ctx context.Context, lowerbound time.Time, segmentStoreKeyParams ...string) error {
	segmentStoreKey, errKey := s.segments.build(segmentStoreKeyParams)
	if errKey != nil {
		return errKey
	}

	return s.redis.HDel(ctx, segmentStoreKey, strconv.FormatInt(lowerbound.UnixMilli(), 10)).Err()
}

// Purge invalidates the series: the sorted set, its blank-page marker and the record
// of which ranges have been seeded are all dropped, so the next fetch rebuilds from
// the database. Item keys are left alone. Without this, purging the sorted set alone
// left the segment store claiming ranges that no longer held any data.
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
	segmentStoreKey, errKey := s.segments.build(keyParams)
	if errKey != nil {
		return false, errKey
	}

	return s.redis.HExists(ctx, segmentStoreKey, strconv.FormatInt(lowerbound.UnixMilli(), 10)).Result()
}

// CountSegments returns the total number of segments stored.
func (s *TimeSeries[T]) CountSegments(ctx context.Context, keyParams ...string) (int64, error) {
	segmentStoreKey, errKey := s.segments.build(keyParams)
	if errKey != nil {
		return 0, errKey
	}

	return s.redis.HLen(ctx, segmentStoreKey).Result()
}

type fetchTimeSeriesBuilder[T item.Blueprint] struct {
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

	gaps, errFindGaps := f.timeSeries.FindGap(ctx, f.lowerbound, f.upperbound, f.keyParams...)
	if errFindGaps != nil {
		return nil, false, errFindGaps
	}
	if len(gaps) > 0 {
		return nil, true, nil
	}

	// processorArgs is forwarded with ... — without it the consumer's arguments arrive
	// wrapped in one more layer of slice than they passed.
	result, errFetch := f.timeSeries.sorted.Fetch(Descending).
		WithParams(f.keyParams...).
		WithRange(f.lowerbound.UnixMilli(), f.upperbound.UnixMilli()).
		WithProcessor(f.processor, f.processorArgs...).
		Exec(ctx)
	if errFetch != nil {
		return nil, false, errFetch
	}

	return result, false, nil
}
