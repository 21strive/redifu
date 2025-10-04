package redifu

import (
	"github.com/21strive/item"
	"reflect"
)

type SQLItemBlueprint interface {
	item.Blueprint
	GetSelf() *SQLItem
}

type SQLItem struct {
	*item.Foundation `json:",inline" bson:",inline"`
}

func (si *SQLItem) GetSelf() *SQLItem { return si }

func InitSQLItem[T SQLItemBlueprint](sqlItem T) {
	value := reflect.ValueOf(sqlItem).Elem()

	// Iterate through the fields of the struct
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)

		// Check if the field is a pointer and is nil
		if field.Kind() == reflect.Ptr && field.IsNil() {
			// Allocate a new value for the pointer and set it
			field.Set(reflect.New(field.Type().Elem()))
		}
	}

	item.InitItem(sqlItem.GetSelf())
}
