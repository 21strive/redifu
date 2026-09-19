package redifu

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"
)

// maxExactScore is the largest integer a float64 holds without loss. Sorted-set
// scores are float64, so an int64 sorting reference beyond this point would reorder
// items silently — a Unix millisecond timestamp is nowhere near it, but a snowflake
// id is.
const maxExactScore = int64(1) << 53

type scoreKind int

const (
	scoreFromCreatedAt scoreKind = iota
	scoreFromTime
	scoreFromTimePtr
	scoreFromInt64
)

// scorer resolves the sorted-set score of an item. The struct field behind a
// sortingReference is located once, at SetSortingReference time, so a typo fails at
// startup instead of on the first write, and reflect.FieldByName does not run on
// every single add.
type scorer[T Blueprint] struct {
	reference string
	kind      scoreKind
	index     []int
}

// newScorer validates that sortingReference names a field of T that redifu can score
// by. An empty reference (or "createdAt") means GetCreatedAt, which Blueprint
// guarantees.
func newScorer[T Blueprint](sortingReference string) (*scorer[T], error) {
	if sortingReference == "" || sortingReference == "createdAt" {
		return &scorer[T]{reference: sortingReference, kind: scoreFromCreatedAt}, nil
	}

	var zero T
	itemType := reflect.TypeOf(&zero).Elem()
	for itemType.Kind() == reflect.Pointer {
		itemType = itemType.Elem()
	}
	if itemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("redifu: sorting reference %q needs a struct item type, got %s", sortingReference, itemType)
	}

	field, found := itemType.FieldByName(sortingReference)
	if !found {
		return nil, fmt.Errorf("redifu: sorting reference %q is not a field of %s", sortingReference, itemType)
	}
	if field.PkgPath != "" {
		return nil, fmt.Errorf("redifu: sorting reference %q is unexported on %s", sortingReference, itemType)
	}

	scored := &scorer[T]{reference: sortingReference, index: field.Index}
	switch field.Type {
	case reflect.TypeOf(time.Time{}):
		scored.kind = scoreFromTime
	case reflect.TypeOf(&time.Time{}):
		scored.kind = scoreFromTimePtr
	case reflect.TypeOf(int64(0)):
		scored.kind = scoreFromInt64
	default:
		return nil, fmt.Errorf("redifu: sorting reference %q is %s — must be time.Time, *time.Time or int64", sortingReference, field.Type)
	}

	return scored, nil
}

func (s *scorer[T]) score(scored T) (float64, error) {
	if s == nil || s.kind == scoreFromCreatedAt {
		return float64(scored.GetCreatedAt().UnixMilli()), nil
	}

	value := reflect.ValueOf(scored)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 0, errors.New("redifu: cannot score a nil item")
		}
		value = value.Elem()
	}

	field, err := value.FieldByIndexErr(s.index)
	if err != nil {
		return 0, fmt.Errorf("redifu: sorting reference %q is behind a nil embedded pointer: %w", s.reference, err)
	}

	switch s.kind {
	case scoreFromTime:
		return float64(field.Interface().(time.Time).UnixMilli()), nil
	case scoreFromTimePtr:
		if field.IsNil() {
			return 0, fmt.Errorf("redifu: sorting reference %q is nil on this item", s.reference)
		}
		return float64(field.Interface().(*time.Time).UnixMilli()), nil
	case scoreFromInt64:
		raw := field.Interface().(int64)
		if raw > maxExactScore || raw < -maxExactScore {
			return 0, fmt.Errorf("%w: %q = %d", ErrScoreOutOfRange, s.reference, raw)
		}
		return float64(raw), nil
	}

	return 0, fmt.Errorf("redifu: sorting reference %q cannot be scored", s.reference)
}

// formatScore renders a score for Lua and for ZRANGEBYSCORE bounds without the
// exponent notation strconv would otherwise produce for large values.
func formatScore(score float64) string {
	if score == math.Trunc(score) && math.Abs(score) < float64(maxExactScore) {
		return fmt.Sprintf("%d", int64(score))
	}
	return fmt.Sprintf("%f", score)
}
