package redifu

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/21strive/item"
	"github.com/redis/go-redis/v9"
)

func getItemScore[T item.Blueprint](item T, sortingReference string) (float64, error) {
	if sortingReference == "" || sortingReference == "createdAt" {
		if scorer, ok := interface{}(item).(interface{ GetCreatedAt() time.Time }); ok {
			return float64(scorer.GetCreatedAt().UnixMilli()), nil
		}
	}

	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return 0, errors.New("getItemScore: item must be a struct or pointer to struct")
	}

	field := val.FieldByName(sortingReference)
	if !field.IsValid() {
		return 0, fmt.Errorf("getItemScore: field %s not found in item", sortingReference)
	}

	switch field.Type() {
	case reflect.TypeOf(time.Time{}):
		return float64(field.Interface().(time.Time).UnixMilli()), nil
	case reflect.TypeOf(&time.Time{}):
		if field.IsNil() {
			return 0, errors.New("getItemScore: time field is nil")
		}
		return float64(field.Interface().(*time.Time).UnixMilli()), nil
	case reflect.TypeOf(int64(0)):
		return float64(field.Interface().(int64)), nil
	default:
		return 0, fmt.Errorf("getItemScore: field %s is not a time.Time", sortingReference)
	}
}

func joinParam(keyFormat string, param []string) string {
	interfaces := make([]interface{}, len(param))
	for i, v := range param {
		interfaces[i] = v
	}
	sortedSetKey := fmt.Sprintf(keyFormat, interfaces...)
	return sortedSetKey
}

// Relation resolves an entity that is stored once in its own Base and referenced from
// many parent items by randId. Implementations come from Relate; the interface is closed
// so that a relation can only be built through it.
type Relation[P any] interface {
	// stage enqueues the reads for these items into the caller's pipeline and returns a
	// function that writes the fetched entities into them once the pipeline has run.
	stage(ctx context.Context, pipe redis.Pipeliner, items []P) (func() error, error)
}

type relation[P any, R item.Blueprint] struct {
	base      *Base[R]
	getRandId func(P) string
	setItem   func(P, R)
}

func (rl *relation[P, R]) stage(ctx context.Context, pipe redis.Pipeliner, items []P) (func() error, error) {
	randIds := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))

	for _, parent := range items {
		randId := rl.getRandId(parent)
		if randId == "" {
			continue
		}
		if _, duplicate := seen[randId]; duplicate {
			continue
		}
		seen[randId] = struct{}{}
		randIds = append(randIds, randId)
	}

	if len(randIds) == 0 {
		return func() error { return nil }, nil
	}

	resolve := rl.base.stageGetMany(ctx, pipe, randIds)

	return func() error {
		fetchedItems, err := resolve()
		if err != nil {
			return err
		}

		for _, parent := range items {
			relatedItem, found := fetchedItems[rl.getRandId(parent)]
			if !found {
				// The related key has expired or been evicted. The parent is returned
				// with that field left empty rather than failing the whole fetch.
				continue
			}
			rl.setItem(parent, relatedItem)
		}

		return nil
	}, nil
}

// Relate declares that parent items of type P carry a randId pointing at an entity stored
// in base. Both accessors are ordinary functions, so the compiler checks them: renaming a
// field breaks the build here instead of silently resolving to nothing at runtime.
//
//	authorRelation, err := redifu.Relate(account.AccountBase,
//	    func(p *Post) string              { return p.AuthorRandId },
//	    func(p *Post, a *account.Account) { p.Author = a },
//	)
//
// P must be a pointer type — the setter has to mutate the item that was fetched.
func Relate[P any, R item.Blueprint](
	base *Base[R],
	getRandId func(P) string,
	setItem func(P, R),
) (Relation[P], error) {
	if base == nil {
		return nil, errors.New("redifu: relation base must not be nil")
	}
	if getRandId == nil {
		return nil, errors.New("redifu: relation randId accessor must not be nil")
	}
	if setItem == nil {
		return nil, errors.New("redifu: relation setter must not be nil")
	}

	var parent P
	parentType := reflect.TypeOf(&parent).Elem()
	if parentType.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("redifu: relation parent %s must be a pointer type — declare the collection as Base[*%s], not Base[%s]", parentType, parentType, parentType)
	}

	return &relation[P, R]{
		base:      base,
		getRandId: getRandId,
		setItem:   setItem,
	}, nil
}

// resolveRelations fills every registered relation for a page of items. All relations
// share a single pipeline, so a second relation costs no extra round-trip, and each
// related key is read once no matter how many items point at it.
func resolveRelations[T any](ctx context.Context, client redis.UniversalClient, relations []Relation[T], items []T) error {
	if len(relations) == 0 || len(items) == 0 {
		return nil
	}

	pipe := client.Pipeline()
	applies := make([]func() error, 0, len(relations))

	for _, relationFormat := range relations {
		apply, err := relationFormat.stage(ctx, pipe, items)
		if err != nil {
			return err
		}
		applies = append(applies, apply)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	for _, apply := range applies {
		if err := apply(); err != nil {
			return err
		}
	}

	return nil
}
