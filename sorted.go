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

type Segment[T item.Blueprint] struct {
	redis                 redis.UniversalClient
	segmentStoreKeyFormat string
	timeToLive            time.Duration
	sorted                *Sorted[T]
}

func NewSegment[T item.Blueprint](
	redis redis.UniversalClient,
	sorted *Sorted[T],
	segmentStoreKeyFormat string,
	ttl time.Duration,
) *Segment[T] {
	return &Segment[T]{
		redis:                 redis,
		sorted:                sorted,
		segmentStoreKeyFormat: segmentStoreKeyFormat,
		timeToLive:            ttl,
	}
}

// Add stores a seeded segment range in the segment store
// Validates that the new segment doesn't overlap with any existing segment
// Segments are stored in Redis Hash: field = lowerbound (string), value = upperbound (string)
func (s *Segment[T]) Add(lowerbound int64, upperbound int64, segmentStoreKeyParam []string) error {
	if lowerbound >= upperbound {
		return fmt.Errorf("invalid range: lowerbound (%d) must be less than upperbound (%d)", lowerbound, upperbound)
	}

	// Validate no overlap with existing segments
	if err := s.validateNoOverlap(lowerbound, upperbound, segmentStoreKeyParam); err != nil {
		return err
	}

	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, segmentStoreKeyParam)

	ctx := context.TODO()
	pipe := s.redis.Pipeline()

	pipe.HSet(ctx, segmentStoreKey, fmt.Sprintf("%d", lowerbound), upperbound)

	if s.timeToLive > 0 {
		pipe.Expire(ctx, segmentStoreKey, s.timeToLive)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// validateNoOverlap ensures the new segment doesn't overlap with any existing segment
// Two segments [a,b] and [c,d] overlap if: b >= c AND d >= a
func (s *Segment[T]) validateNoOverlap(lowerbound int64, upperbound int64, keyParam []string) error {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParam)

	result, err := s.redis.HGetAll(context.TODO(), segmentStoreKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	for lowerStr, upperStr := range result {
		existingLower, err1 := strconv.ParseInt(lowerStr, 10, 64)
		existingUpper, err2 := strconv.ParseInt(upperStr, 10, 64)

		if err1 != nil || err2 != nil {
			continue
		}

		// Check if segments overlap: upper >= existingLower AND existingUpper >= lower
		if upperbound >= existingLower && existingUpper >= lowerbound {
			return fmt.Errorf(
				"segment [%d, %d] overlaps with existing segment [%d, %d]",
				lowerbound, upperbound, existingLower, existingUpper,
			)
		}
	}

	return nil
}

// Scan retrieves all segments that intersect with the specified range
// For inclusive intervals [a,b] and [c,d], they intersect if: upper >= lowerbound AND lower <= upperbound
// Returns segments sorted by lower bound
func (s *Segment[T]) Scan(lowerbound int64, upperbound int64, keyParam []string) (*[][]int64, error) {
	if lowerbound >= upperbound {
		return nil, fmt.Errorf("invalid range: lowerbound (%d) must be less than upperbound (%d)", lowerbound, upperbound)
	}

	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParam)

	result, err := s.redis.HGetAll(context.TODO(), segmentStoreKey).Result()
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

		// Intersects if: upper >= lowerbound AND lower <= upperbound
		if upper >= lowerbound && lower <= upperbound {
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
// Since segments are guaranteed non-overlapping (enforced by Add), no merging is needed
func (s *Segment[T]) FindGap(lowerbound int64, upperbound int64, keyParam []string) ([][]int64, error) {
	if lowerbound >= upperbound {
		return [][]int64{}, fmt.Errorf("invalid range: lowerbound (%d) must be less than upperbound (%d)", lowerbound, upperbound)
	}

	segments, err := s.Scan(lowerbound, upperbound, keyParam)
	if err != nil {
		return nil, err
	}

	if segments == nil || len(*segments) == 0 {
		return [][]int64{{lowerbound, upperbound}}, nil
	}

	segs := *segments
	return s.calculateGaps(segs, lowerbound, upperbound), nil
}

// calculateGaps finds gaps between non-overlapping segments within the specified range
func (s *Segment[T]) calculateGaps(segments [][]int64, lowerbound, upperbound int64) [][]int64 {
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
// Optimized: Only filters first and last segments, middle segments are appended directly
// This reduces complexity from O(2N) to O(N + 2(N_edges))
func (s *Segment[T]) Fetch(lowerbound int64, upperbound int64, keyParam []string) ([]T, error) {
	if lowerbound >= upperbound {
		return nil, fmt.Errorf("invalid range: lowerbound (%d) must be less than upperbound (%d)", lowerbound, upperbound)
	}

	segments, err := s.Scan(lowerbound, upperbound, keyParam)
	if err != nil {
		return nil, err
	}

	if segments == nil || len(*segments) == 0 {
		return []T{}, nil
	}

	segs := *segments
	numSegments := len(segs)
	result := make([]T, 0)

	for i, segment := range segs {
		segmentLower := segment[0]
		segmentUpper := segment[1]

		// Fetch entire segment (FetchByScore only accepts segment boundaries)
		items, err := s.sorted.Fetch(keyParam, Descending, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch segment [%d-%d]: %w", segmentLower, segmentUpper, err)
		}

		// Handle different cases based on segment position
		switch {
		case numSegments == 1:
			// Single segment: filter both bounds
			for _, item := range items {
				score := item.GetCreatedAt().UnixMilli()
				if score >= lowerbound && score <= upperbound {
					result = append(result, item)
				}
			}

		case i == 0:
			// First segment: filter lower bound only
			for _, item := range items {
				if item.GetCreatedAt().UnixMilli() >= lowerbound {
					result = append(result, item)
				}
			}

		case i == numSegments-1:
			// Last segment: filter upper bound only
			for _, item := range items {
				if item.GetCreatedAt().UnixMilli() <= upperbound {
					result = append(result, item)
				}
			}

		default:
			// Middle segments: no filtering, append all
			result = append(result, items...)
		}
	}

	return result, nil
}

// Remove deletes a segment from the segment store by its lowerbound
func (s *Segment[T]) Remove(lowerbound int64, segmentStoreKeyParam []string) error {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, segmentStoreKeyParam)
	return s.redis.HDel(context.TODO(), segmentStoreKey, fmt.Sprintf("%d", lowerbound)).Err()
}

// GetAllSegments retrieves all stored segments, sorted by lower bound
func (s *Segment[T]) GetAllSegments(keyParam []string) ([][]int64, error) {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParam)

	result, err := s.redis.HGetAll(context.TODO(), segmentStoreKey).Result()
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
func (s *Segment[T]) Exists(lowerbound int64, keyParam []string) (bool, error) {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParam)
	return s.redis.HExists(context.TODO(), segmentStoreKey, fmt.Sprintf("%d", lowerbound)).Result()
}

// Count returns the total number of segments stored
func (s *Segment[T]) Count(keyParam []string) (int64, error) {
	segmentStoreKey := joinParam(s.segmentStoreKeyFormat, keyParam)
	return s.redis.HLen(context.TODO(), segmentStoreKey).Result()
}

// Helper functions
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

type Sorted[T item.Blueprint] struct {
	client           redis.UniversalClient
	baseClient       *Base[T]
	sortedSetClient  *SortedSet[T]
	sortingReference string
	relation         map[string]Relation
}

func (srtd *Sorted[T]) Init(client redis.UniversalClient, baseClient *Base[T], sortedSetClient *SortedSet[T]) {
	srtd.client = client
	srtd.baseClient = baseClient
	srtd.sortedSetClient = sortedSetClient
}

func (cr *Sorted[T]) AddRelation(identifier string, relationBase Relation) {
	if cr.relation == nil {
		cr.relation = make(map[string]Relation)
	}
	cr.relation[identifier] = relationBase
}

func (cr *Sorted[T]) GetRelation() map[string]Relation {
	return cr.relation
}

func (srtd *Sorted[T]) SetSortingReference(sortingReference string) {
	srtd.sortingReference = sortingReference
}

func (srtd *Sorted[T]) AddItem(item T, sortedSetParam []string) {
	srtd.IngestItem(item, sortedSetParam, false)
}

func (srtd *Sorted[T]) IngestItem(item T, sortedSetParam []string, seed bool) error {
	score, err := getItemScore(item, srtd.sortingReference)
	if err != nil {
		return err
	}

	if !seed {
		isBlankPage, errGet := srtd.IsBlankPage(sortedSetParam)
		if errGet != nil {
			return errGet
		}
		if isBlankPage {
			srtd.DelBlankPage(sortedSetParam)
		}

		if srtd.sortedSetClient.TotalItemOnSortedSet(sortedSetParam) > 0 {
			return srtd.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
		}
	} else {
		return srtd.sortedSetClient.SetSortedSet(sortedSetParam, score, item)
	}

	return nil
}

func (srtd *Sorted[T]) RemoveItem(item T, sortedSetParam []string) error {
	return srtd.sortedSetClient.DeleteFromSortedSet(sortedSetParam, item)
}

func (srtd *Sorted[T]) Fetch(param []string, direction string, processor func(item *T, args []interface{}), processorArgs []interface{}) ([]T, error) {
	return fetchAll[T](srtd.client, srtd.baseClient, srtd.sortedSetClient, param, direction, srtd.baseClient.timeToLive, processor, processorArgs, srtd.relation)
}

func (srtd *Sorted[T]) SetBlankPage(param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	setLastPageKey := srtd.client.Set(
		context.TODO(),
		lastPageKey,
		1,
		srtd.sortedSetClient.timeToLive,
	)

	if setLastPageKey.Err() != nil {
		return setLastPageKey.Err()
	}
	return nil
}

func (srtd *Sorted[T]) DelBlankPage(param []string) error {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	delLastPageKey := srtd.client.Del(context.TODO(), lastPageKey)
	if delLastPageKey.Err() != nil {
		return delLastPageKey.Err()
	}
	return nil
}

func (srtd *Sorted[T]) IsBlankPage(param []string) (bool, error) {
	sortedSetKey := joinParam(srtd.sortedSetClient.sortedSetKeyFormat, param)
	lastPageKey := sortedSetKey + ":blankpage"

	getLastPageKey := srtd.client.Get(context.TODO(), lastPageKey)
	if getLastPageKey.Err() != nil {
		if getLastPageKey.Err() == redis.Nil {
			return false, nil
		} else {
			return false, getLastPageKey.Err()
		}
	}

	if getLastPageKey.Val() == "1" {
		return true, nil
	}
	return false, nil
}

func (srtd *Sorted[T]) RequiresSeeding(param []string) (bool, error) {
	isBlankPage, err := srtd.IsBlankPage(param)
	if err != nil {
		return false, err
	}

	if !isBlankPage {
		if srtd.sortedSetClient.TotalItemOnSortedSet(param) > 0 {
			return false, nil
		}
		return true, nil
	} else {
		return false, nil
	}
}

func (srtd *Sorted[T]) RemoveSorted(param []string) error {
	err := srtd.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	return nil
}

func (srtd *Sorted[T]) PurgeSorted(param []string) error {
	items, err := srtd.Fetch(param, Descending, nil, nil)
	if err != nil {
		return err
	}

	for _, item := range items {
		srtd.baseClient.Del(item)
	}

	err = srtd.sortedSetClient.DeleteSortedSet(param)
	if err != nil {
		return err
	}

	return nil
}

func NewSorted[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, timeToLive time.Duration) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	sorted := &Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	return sorted
}

func NewSortedWithReference[T item.Blueprint](client redis.UniversalClient, baseClient *Base[T], keyFormat string, sortingReference string, timeToLive time.Duration) *Sorted[T] {
	sortedSetClient := &SortedSet[T]{}
	sortedSetClient.Init(client, keyFormat, timeToLive)

	sorted := &Sorted[T]{}
	sorted.Init(client, baseClient, sortedSetClient)
	sorted.SetSortingReference(sortingReference)
	return sorted
}
