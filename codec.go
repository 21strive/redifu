package redifu

import (
	"encoding/json"
	"reflect"
)

// RawCodec is implemented by an item type that writes and reads its own Redis value. Base then
// stores what MarshalRedis returns as it is, and hands a stored value straight to UnmarshalRedis
// on a new item, instead of going through encoding/json — which validates and compacts what a
// MarshalJSON returns, and scans a value twice before an UnmarshalJSON sees it. For an item that
// already encodes itself (its own MarshalJSON / UnmarshalJSON) that is most of the cost of a read.
//
// The value must stay JSON (redifu compares an item's JSON before and after a relation is set),
// and UnmarshalRedis must read whatever MarshalRedis — or the item's earlier encoding — wrote.
// Implement both methods on the pointer type when the item type is a pointer, as Base[*Item].
type RawCodec interface {
	MarshalRedis() ([]byte, error)
	UnmarshalRedis(data []byte) error
}

var rawCodecType = reflect.TypeOf((*RawCodec)(nil)).Elem()

// encodeItem is the value Base stores for item.
func encodeItem[T any](item T) ([]byte, error) {
	if codec, ok := any(item).(RawCodec); ok {
		if value := reflect.ValueOf(item); value.Kind() != reflect.Pointer || !value.IsNil() {
			return codec.MarshalRedis()
		}
	}
	return json.Marshal(item)
}

// decodeItem reads a stored value into a new item.
func decodeItem[T any](data []byte) (T, error) {
	var item T
	itemType := reflect.TypeOf(&item).Elem()
	if itemType.Kind() == reflect.Pointer && itemType.Implements(rawCodecType) {
		item = reflect.New(itemType.Elem()).Interface().(T)
		return item, any(item).(RawCodec).UnmarshalRedis(data)
	}
	if codec, ok := any(&item).(RawCodec); ok {
		return item, codec.UnmarshalRedis(data)
	}
	err := json.Unmarshal(data, &item)
	return item, err
}
