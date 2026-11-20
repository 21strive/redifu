package outbox

import (
	"testing"

	"github.com/lib/pq"
)

// textArray sends what lib/pq's StringArray sent before it, byte for byte: the queries of a build on
// this package bind the same text as those of a build that still uses lib/pq.
func TestTextArrayWritesWhatLibPQWrites(t *testing.T) {
	for _, values := range [][]string{
		nil,
		{},
		{"o1"},
		{"o1", "o2", "6f1c0a7e-4d5b-4c3a-9e2f-1a2b3c4d5e6f"},
		{"", " ", "NULL", "a,b", "{x}", `say "hi"`, `back\slash`, `\"`, "tab\there", "é ü 日本"},
	} {
		got, err := textArray(values).Value()
		if err != nil {
			t.Fatal(err)
		}
		want, err := pq.Array(values).Value()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%q: textArray %#v, lib/pq %#v", values, got, want)
		}
	}
}
