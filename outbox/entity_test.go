package outbox

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// What an application gives its entities, and the outbox does not: a codec for the value Base
// stores (redifu.Base writes whatever an entity's MarshalJSON writes), a generation that
// fingerprints a row entity, and the TTL of its Base. The tests' stand-ins do what the tests
// rely on and nothing more.

// EntityTTL is the TTL of the tests' Bases: longer than ListTTL, so a member outlives its index.
const EntityTTL = 2 * time.Hour

// Encode writes the db columns of the struct v points at as one JSON object, embedded structs
// flattened the way sqlx fills them: a cached entity is what the query gave, so a column tagged
// json:"-" for the API survives the cache.
func Encode(v any) ([]byte, error) {
	columns := map[string]any{}
	eachColumn(reflect.ValueOf(v).Elem(), false, func(name string, field reflect.Value) {
		columns[name] = field.Interface()
	})
	return json.Marshal(columns)
}

// Decode reads what Encode wrote back into the struct v points at.
func Decode(data []byte, v any) error {
	var columns map[string]json.RawMessage
	if err := json.Unmarshal(data, &columns); err != nil {
		return err
	}
	var err error
	eachColumn(reflect.ValueOf(v).Elem(), true, func(name string, field reflect.Value) {
		if raw, ok := columns[name]; ok && err == nil {
			err = json.Unmarshal(raw, field.Addr().Interface())
		}
	})
	return err
}

// eachColumn calls fn with every db column of a struct value. An embedded struct is flattened; a
// nil embedded pointer is allocated when allocate is set (decoding) and skipped otherwise.
func eachColumn(v reflect.Value, allocate bool, fn func(name string, field reflect.Value)) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field, value := t.Field(i), v.Field(i)
		if field.Anonymous {
			if value.Kind() == reflect.Pointer {
				if value.IsNil() {
					if !allocate {
						continue
					}
					value.Set(reflect.New(field.Type.Elem()))
				}
				value = value.Elem()
			}
			if value.Kind() == reflect.Struct {
				eachColumn(value, allocate, fn)
				continue
			}
		}
		name, tagged := field.Tag.Lookup("db")
		if !tagged || name == "-" || !field.IsExported() {
			continue
		}
		fn(name, value)
	}
}

// Shape stands in for the generation an application gives a row entity's Base: a fingerprint of
// the sample's type and of the SQL that loads it, "s" and 16 hex digits.
func Shape(sample any, sql ...string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%T\x00%s", sample, strings.Join(sql, "\x00"))))
	return "s" + hex.EncodeToString(sum[:8])
}
