package redifu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/redis/go-redis/v9"
)

// DefaultRelationDepth is how many levels of nested relations a fetch resolves.
// Post -> Author -> Organisation is depth 2. The limit is what stops a relation
// cycle from looping forever.
const DefaultRelationDepth = 4

// Relation resolves an entity that is stored once in its own Base and referenced from
// many parent items by randId. Implementations come from Relate; the interface is closed
// so that a relation can only be built through it.
type Relation[P any] interface {
	// stage enqueues the reads for these items into the caller's pipeline and returns a
	// function that writes the fetched entities into them once the pipeline has run.
	// depth is how many further levels of nested relations the resolver may follow.
	stage(ctx context.Context, pipe redis.Pipeliner, items []P, depth int) (func() error, error)
}

type relation[P any, R Blueprint] struct {
	base      *Base[R]
	getRandId func(P) string
	setItem   func(P, R)
}

func (rl *relation[P, R]) stage(ctx context.Context, pipe redis.Pipeliner, items []P, depth int) (func() error, error) {
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

		// A related entity may carry relations of its own. Resolving them here is what
		// makes Post -> Author -> Organisation come back whole, and it still costs one
		// batch per level rather than one read per item.
		if depth > 1 && len(rl.base.relations) > 0 && len(fetchedItems) > 0 {
			nested := make([]R, 0, len(fetchedItems))
			for _, fetchedItem := range fetchedItems {
				nested = append(nested, fetchedItem)
			}
			if errNested := resolveRelations(ctx, rl.base.client, rl.base.relations, nested, depth-1); errNested != nil {
				return errNested
			}
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
// P must be a pointer type — the setter has to mutate the item that was fetched — and
// the field the setter writes into must be tagged json:"-". Relate verifies the tag and
// refuses the relation if it is missing, because an untagged field bakes a copy of the
// related entity into the parent's own key the first time the parent is written back,
// and that breaks the singleton permanently and silently.
func Relate[P any, R Blueprint](
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

	if err := verifyRelationIsTransient[P, R](setItem); err != nil {
		return nil, err
	}

	return &relation[P, R]{
		base:      base,
		getRandId: getRandId,
		setItem:   setItem,
	}, nil
}

// verifyRelationIsTransient checks invariant 3 — the relation field must be tagged
// json:"-" — without needing the field's name. It marshals a blank parent, runs the
// setter, and marshals it again: if the field is excluded from JSON the two renderings
// are identical, and if it is not, the related entity has just shown up in the parent's
// stored form, which is exactly the corruption this refuses to allow.
func verifyRelationIsTransient[P any, R Blueprint](setItem func(P, R)) error {
	var parent P
	parentType := reflect.TypeOf(&parent).Elem().Elem()
	if parentType.Kind() != reflect.Struct {
		return nil
	}

	var related R
	relatedType := reflect.TypeOf(&related).Elem()
	if relatedType.Kind() != reflect.Pointer || relatedType.Elem().Kind() != reflect.Struct {
		return nil
	}

	probeParentValue := reflect.New(parentType)
	allocateEmbedded(probeParentValue.Elem())
	probeParent, ok := probeParentValue.Interface().(P)
	if !ok {
		return nil
	}

	probeRelatedValue := reflect.New(relatedType.Elem())
	allocateEmbedded(probeRelatedValue.Elem())
	probeRelated, ok := probeRelatedValue.Interface().(R)
	if !ok {
		return nil
	}

	before, after, probed := probeRelationJSON(probeParent, probeRelated, setItem)
	if !probed {
		// The probe could not be rendered — an exotic item type or an accessor that
		// needs more than a blank struct. Nothing is proven either way, and refusing
		// a relation on that basis would be worse than not checking it.
		return nil
	}

	if !bytes.Equal(before, after) {
		return fmt.Errorf(
			"%w: setting the relation on %s changed its stored JSON from %s to %s — tag that field json:\"-\"",
			ErrRelationNotTransient, parentType, before, after,
		)
	}

	return nil
}

// probeRelationJSON renders the parent before and after the setter runs. It reports
// probed = false rather than an error if anything about the probe fails, since a probe
// that could not run proves nothing about the relation.
func probeRelationJSON[P any, R Blueprint](parent P, related R, setItem func(P, R)) (before []byte, after []byte, probed bool) {
	defer func() {
		if recover() != nil {
			probed = false
		}
	}()

	var err error
	if before, err = json.Marshal(parent); err != nil {
		return nil, nil, false
	}

	setItem(parent, related)

	if after, err = json.Marshal(parent); err != nil {
		return nil, nil, false
	}

	return before, after, true
}

// allocateEmbedded fills in nil embedded pointers, for an entity that embeds its
// identity as a pointer rather than embedding Record by value, so that a
// probe behaves like an item a consumer would hand to redifu. Named fields are left
// alone on purpose: the relation field is one of them, and it has to start out empty
// for the before/after comparison to mean anything.
func allocateEmbedded(value reflect.Value) {
	if value.Kind() != reflect.Struct {
		return
	}
	valueType := value.Type()
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if !valueType.Field(i).Anonymous {
			continue
		}
		if field.Kind() == reflect.Pointer && field.IsNil() && field.CanSet() {
			field.Set(reflect.New(field.Type().Elem()))
		}
	}
}

// resolveRelations fills every registered relation for a page of items. All relations
// at one level share a single pipeline, so a second relation costs no extra round-trip,
// and each related key is read once no matter how many items point at it.
func resolveRelations[T any](ctx context.Context, client redis.UniversalClient, relations []Relation[T], items []T, depth int) error {
	if len(relations) == 0 || len(items) == 0 {
		return nil
	}
	if depth <= 0 {
		return ErrRelationDepthExceeded
	}

	pipe := client.Pipeline()
	applies := make([]func() error, 0, len(relations))

	for _, relationFormat := range relations {
		apply, err := relationFormat.stage(ctx, pipe, items, depth)
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
