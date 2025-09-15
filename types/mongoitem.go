package types

import (
	"github.com/21strive/item"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
)

type MongoItemBlueprint interface {
	item.Blueprint
	SetObjectID()
	GetObjectID() primitive.ObjectID
	GetSelf() *MongoItem
}

type MongoItem struct {
	*item.Foundation `json:",inline" bson:",inline"`
	ObjectID         primitive.ObjectID `json:"-" bson:"_id"` // MongoDB support
}

func (mi *MongoItem) SetObjectID() {
	mi.ObjectID = primitive.NewObjectID()
}

func (mi *MongoItem) GetObjectID() primitive.ObjectID {
	return mi.ObjectID
}

func (mi *MongoItem) GetSelf() *MongoItem {
	return mi
}

func InitMongoItem[T MongoItemBlueprint](mongoItem T) {
	value := reflect.ValueOf(mongoItem).Elem()

	// Iterate through the fields of the struct
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)

		// Check if the field is a pointer and is nil
		if field.Kind() == reflect.Ptr && field.IsNil() {
			// Allocate a new value for the pointer and set it
			field.Set(reflect.New(field.Type().Elem()))
		}
	}

	item.InitItem(mongoItem.GetSelf())
	mongoItem.SetObjectID()
}
