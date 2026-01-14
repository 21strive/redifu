package redifu

import (
	"context"
	"fmt"
	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
	"sort"
	"strconv"
	"time"
)

type TimeSeries[T item.Blueprint] struct {
	redis                 redis.UniversalClient
	segmentStoreKeyFormat string
	timeToLive            time.Duration
	sorted                *Sorted[T]
}

func NewTimeSeries[T item.Blueprint](
	redis redis.UniversalClient,
	baseClient *Base[T],
	keyFormat string,
	timeToLive time.Duration,
) *TimeSeries[T] {
	sorted := NewSorted[T](redis, baseClient, keyFormat, timeToLive)
	segmentStoreKeyFormat := fmt.Sprintf("%s:segments", keyFormat)

	return &TimeSeries[T]{
		redis:                 redis,
		sorted:                sorted,
		segmentStoreKeyFormat: segmentStoreKeyFormat,
		timeToLive:            timeToLive,
	}
}

func (s *TimeSeries[T]) AddRelation(identifier string, relation Relation) {
	s.sorted.AddRelation(identifier, relation)
}

func (s *TimeSeries[T]) GetRelation() map[string]Relation {
	return s.sorted.GetRelation()
}

func (s *TimeSeries[T]) SetExpiration(ctx context.Context, pipe redis.Pipeliner, keyParams ...string) {
	s.sorted.SetExpiration(ctx, pipe, keyParams...)
}

func (s *TimeSeries[T]) Count(ctx context.Context, keyParams ...string) int64 {
	return s.sorted.Count(ctx, keyParams...)
}

func (s *TimeSeries[T]) AddItem(ctx context.Context, item T, keyParams ...string) error {
	itemScore, errGetScore := getItemScore(item, s.sorted.sortingReference)
	if errGetScore != nil {
		return errGetScore
	}

	// Convert item score to time for TimeSeries operations
	itemTime := time.UnixMilli(int64(itemScore)).UTC()
	segment, errScan := s.Scan(ctx, itemTime, itemTime, keyParams...)
	if errScan != nil {
		return errScan
	}
	if segment != nil && len(*segment) == 1 {
		errAdd := s.sorted.AddItem(ctx, item, keyParams...)
		if errAdd != nil {
			return errAdd
		}
		return nil
	}

	return nil
}

func (s *TimeSeries[T]) IngestItem(ctx context.Context, pipe redis.Pipeliner, item T, keyParams ...string) error {
	return s.sorted.IngestItem(ctx, pipe, item, true, keyParams...)
}

func (s *TimeSeries[T]) RemoveItem(ctx context.Context, item T, keyParams ...string) error {
	return s.sorted.RemoveItem(ctx, item, keyParams...)
}

// AddSegment stores a seeded segment range in the segment store
// Validates that the new segment doesn't overlap with any existing segment
// Segments are stored in Redis Hash: field = lowerbound (string), value = upperbound (string)
func (s *TimeSeries[T]) AddSegment(ctx context.Context, pipe redis.Pipeliner, lowerbound time.Time, upperbound time.Time, segmentStoreKeyParams ...string) error {
	if !lowerbound.Before(upperbound) {
		return fmt.Errorf("invalid range: lowerbound (%s) must be less than upperbound (%s)", lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339))
	}

	// Convert to Unix timestamps for internal storage
	lowerboundUnix := lowerbound.UnixMilli()
	upperboundUnix := upperbound.UnixMilli()

	// Validate no overlap with existing segments
	if err := s.validateNoOverlap(ctx, lowerbound, upperbound, segmentStoreKeyParams...); err != nil {
		return err
	}

	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, segmentStoreKeyParams)

	pipe.HSet(ctx, segmentStoreKey, fmt.Sprintf("%d", lowerboundUnix), upperboundUnix)

	if s.timeToLive > 0 {
		pipe.Expire(ctx, segmentStoreKey, s.timeToLive)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// validateNoOverlap ensures the new segment doesn't overlap with any existing segment
// Two segments [a,b] and [c,d] overlap if: b > c AND d > a
func (s *TimeSeries[T]) validateNoOverlap(ctx context.Context, lowerbound time.Time, upperbound time.Time, keyParams ...string) error {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParams)

	result, err := s.redis.HGetAll(ctx, segmentStoreKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	// Convert to Unix timestamps for comparison
	lowerboundUnix := lowerbound.UnixMilli()
	upperboundUnix := upperbound.UnixMilli()

	for lowerStr, upperStr := range result {
		existingLower, err1 := strconv.ParseInt(lowerStr, 10, 64)
		existingUpper, err2 := strconv.ParseInt(upperStr, 10, 64)

		if err1 != nil || err2 != nil {
			continue
		}

		// Check if segments overlap: upper > existingLower AND existingUpper > lower
		if upperboundUnix > existingLower && existingUpper > lowerboundUnix {
			return fmt.Errorf(
				"segment [%s, %s] overlaps with existing segment [%d, %d]",
				lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339), existingLower, existingUpper,
			)
		}
	}

	return nil
}

// Scan retrieves all segments that intersect with the specified range
// For inclusive intervals [a,b] and [c,d], they intersect if: upper > lowerbound AND lower < upperbound
// Returns segments sorted by lower bound
func (s *TimeSeries[T]) Scan(ctx context.Context, lowerbound time.Time, upperbound time.Time, keyParams ...string) (*[][]int64, error) {
	if !lowerbound.Before(upperbound) {
		return nil, fmt.Errorf("invalid range: lowerbound (%s) must be less than upperbound (%s)", lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339))
	}

	// Convert to Unix timestamps for comparison
	lowerboundUnix := lowerbound.UnixMilli()
	upperboundUnix := upperbound.UnixMilli()

	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParams)

	result, err := s.redis.HGetAll(ctx, segmentStoreKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get segments: %w", err)
	}

	segments := make([][]int64, 0, len(result))

	for lowerStr, upperStr := range result {
		lower, err1 := strconv.ParseInt(lowerStr, 10, 64)
		upper, err2 := strconv.ParseInt(upperStr, 10, 64)

		if err1 != nil || err2 != nil {
			continue
		}

		// Intersects if: upper > lowerbound AND lower < upperbound
		if upper > lowerboundUnix && lower < upperboundUnix {
			segments = append(segments, []int64{lower, upper})
		}
	}

	// Sort segments by lower bound
	sort.Slice(segments, func(i, j int) bool {
		return segments[i][0] < segments[j][0]
	})

	return &segments, nil
}

// FindGap identifies gaps between seeded segments within the specified range
// Since segments are guaranteed non-overlapping (enforced by AddSegment), no merging is needed
func (s *TimeSeries[T]) FindGap(ctx context.Context, lowerbound time.Time, upperbound time.Time, keyParams ...string) ([][]int64, error) {
	if !lowerbound.Before(upperbound) {
		return [][]int64{}, fmt.Errorf("invalid range: lowerbound (%s) must be less than upperbound (%s)", lowerbound.Format(time.RFC3339), upperbound.Format(time.RFC3339))
	}

	segments, err := s.Scan(ctx, lowerbound, upperbound, keyParams...)
	if err != nil {
		return nil, err
	}

	// Convert to Unix timestamps for gap calculation
	lowerboundUnix := lowerbound.UnixMilli()
	upperboundUnix := upperbound.UnixMilli()

	if segments == nil || len(*segments) == 0 {
		return [][]int64{{lowerboundUnix, upperboundUnix}}, nil
	}

	segs := *segments
	return s.calculateGaps(segs, lowerboundUnix, upperboundUnix), nil
}

// calculateGaps finds gaps between non-overlapping segments within the specified range
func (s *TimeSeries[T]) calculateGaps(segments [][]int64, lowerbound, upperbound int64) [][]int64 {
	gaps := make([][]int64, 0)

	if len(segments) == 0 {
		return [][]int64{{lowerbound, upperbound}}
	}

	// Gap before first segment
	if segments[0][0] > lowerbound {
		gaps = append(gaps, []int64{lowerbound, segments[0][0]})
	}

	// Gaps between consecutive segments
	for i := 0; i < len(segments)-1; i++ {
		currentUpper := segments[i][1]
		nextLower := segments[i+1][0]

		if currentUpper < nextLower {
			gaps = append(gaps, []int64{currentUpper, nextLower})
		}
	}

	// Gap after last segment
	if segments[len(segments)-1][1] < upperbound {
		gaps = append(gaps, []int64{segments[len(segments)-1][1], upperbound})
	}

	return gaps
}

// Fetch retrieves data from seeded segments within the specified range
func (s *TimeSeries[T]) Fetch(ctx context.Context, lowerbound time.Time, upperbound time.Time) *fetchTimeSeriesBuilder[T] {
	return &fetchTimeSeriesBuilder[T]{
		mainCtx:    ctx,
		timeSeries: s,
		lowerbound: lowerbound,
		upperbound: upperbound,
	}
}

// Remove deletes a segment from the segment store by its lowerbound
func (s *TimeSeries[T]) Remove(ctx context.Context, lowerbound time.Time, segmentStoreKeyParams ...string) error {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, segmentStoreKeyParams)
	return s.redis.HDel(ctx, segmentStoreKey, fmt.Sprintf("%d", lowerbound.UnixMilli())).Err()
}

// GetSegments retrieves all stored segments, sorted by lower bound
func (s *TimeSeries[T]) GetSegments(ctx context.Context, keyParams ...string) ([][]int64, error) {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParams)

	result, err := s.redis.HGetAll(ctx, segmentStoreKey).Result()
	if err != nil {
		return nil, err
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

	sort.Slice(segments, func(i, j int) bool {
		return segments[i][0] < segments[j][0]
	})

	return segments, nil
}

// Exists checks if a segment with the given lowerbound exists
func (s *TimeSeries[T]) Exists(ctx context.Context, lowerbound time.Time, keyParams ...string) (bool, error) {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParams)
	return s.redis.HExists(ctx, segmentStoreKey, fmt.Sprintf("%d", lowerbound.UnixMilli())).Result()
}

// Count returns the total number of segments stored
func (s *TimeSeries[T]) CountSegments(ctx context.Context, keyParams ...string) (int64, error) {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParams)
	return s.redis.HLen(ctx, segmentStoreKey).Result()
}

type fetchTimeSeriesBuilder[T item.Blueprint] struct {
	mainCtx       context.Context
	timeSeries    *TimeSeries[T]
	lowerbound    time.Time
	upperbound    time.Time
	keyParams     []string
	processor     func(item *T, args []interface{})
	processorArgs []interface{}
}

func (f *fetchTimeSeriesBuilder[T]) WithKeyParams(keyParams ...string) *fetchTimeSeriesBuilder[T] {
	f.keyParams = keyParams
	return f
}

func (f *fetchTimeSeriesBuilder[T]) WithProcessor(processor func(item *T, args []interface{}), processorArgs ...interface{}) *fetchTimeSeriesBuilder[T] {
	f.processor = processor
	f.processorArgs = processorArgs
	return f
}

func (f *fetchTimeSeriesBuilder[T]) Exec() ([]T, bool, error) {
	if !f.lowerbound.Before(f.upperbound) {
		return nil, false, fmt.Errorf("invalid range: lowerbound (%s) must be less than upperbound (%s)", f.lowerbound.Format(time.RFC3339), f.upperbound.Format(time.RFC3339))
	}

	gaps, errFindGaps := f.timeSeries.FindGap(f.mainCtx, f.lowerbound, f.upperbound, f.keyParams...)
	if errFindGaps != nil {
		return nil, false, errFindGaps
	}
	if len(gaps) > 0 {
		return nil, true, nil
	}

	// Convert to Unix timestamps for sorted set operations
	lowerboundUnix := f.lowerbound.UnixMilli()
	upperboundUnix := f.upperbound.UnixMilli()

	result, errFetch := f.timeSeries.sorted.Fetch(f.mainCtx, Descending).
		WithParams(f.keyParams...).
		WithRange(lowerboundUnix, upperboundUnix).
		WithProcessor(f.processor, f.processorArgs).
		Exec()
	if errFetch != nil {
		return nil, false, errFetch
	}

	return result, false, nil
}
