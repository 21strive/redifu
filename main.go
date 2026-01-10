package redifu

import (
	"context"
	"errors"
	"fmt"
	"github.com/21strive/item"
	"reflect"
	"time"
)

var (
	NoDatabaseProvided           = errors.New("No database provided!")
	DocumentOrReferencesNotFound = errors.New("Document or References not found!")
	QueryOrScannerNotConfigured  = errors.New("Required queries or scanner not configured")
	NilConfiguration             = errors.New("No configuration found!")
)

func getFieldValue(obj interface{}, fieldName string) interface{} {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return time.Time{}
	}

	field := val.FieldByName(fieldName)
	if !field.IsValid() {
		return time.Time{}
	}

	return field.Interface()
}

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

type Relation interface {
	GetByRandId(randId string) (interface{}, error)
	GetItemAttribute() string
	GetRandIdAttribute() string
	SetItem(item interface{}) error
}

type RelationFormat[T item.Blueprint] struct {
	base            *Base[T]
	itemAttribute   string
	randIdAttribute string
}

// Implement Relation interface
func (r *RelationFormat[T]) GetByRandId(ctx context.Context, randId string) (interface{}, error) {
	return r.base.Get(ctx, randId)
}

func (r *RelationFormat[T]) GetItemAttribute() string {
	return r.itemAttribute
}

func (r *RelationFormat[T]) GetRandIdAttribute() string {
	return r.randIdAttribute
}

func (r *RelationFormat[T]) SetItem(ctx context.Context, item interface{}) error {
	typedItem, ok := item.(T)
	if !ok {
		return fmt.Errorf("invalid item type: expected %T, got %T", *new(T), item)
	}

	return r.base.Upsert(ctx, typedItem)
}

func NewRelation[T item.Blueprint](base *Base[T], itemAttributeName string, randIdAttributeName string) *RelationFormat[T] {
	relation := &RelationFormat[T]{}
	relation.base = base
	relation.itemAttribute = itemAttributeName
	relation.randIdAttribute = randIdAttributeName
	return relation
}
