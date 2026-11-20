package outbox

import (
	"fmt"
	"testing"
)

// ChangeKeys gives each key once, in the order of its first change, with that first change: the
// one whose old row is what Redis holds.
func TestChangeKeysListsEachKeyOnceWithItsFirstChange(t *testing.T) {
	old := func(owner string) *string { row := fmt.Sprintf(`{"owner": %q}`, owner); return &row }
	changes := []Change{
		{ID: 1, Key: "a", Op: "INSERT"},
		{ID: 2, Key: "b", Op: "UPDATE", OldRow: old("o1")},
		{ID: 3, Key: "a", Op: "UPDATE", OldRow: old("o1")},
		{ID: 4, Key: "c", Op: "DELETE", OldRow: old("o2")},
		{ID: 5, Key: "b", Op: "UPDATE", OldRow: old("o3")},
	}
	keys, first := ChangeKeys(changes)
	if fmt.Sprint(keys) != "[a b c]" {
		t.Fatalf("keys = %v", keys)
	}
	if len(first) != 3 || first["a"].ID != 1 || first["b"].ID != 2 || first["c"].ID != 4 {
		t.Fatalf("first changes = %+v", first)
	}
	if first["a"].Old() != nil || first["b"].Old().Str("owner") != "o1" {
		t.Fatalf("old rows: a %v, b %v", first["a"].Old(), first["b"].Old())
	}

	keys, first = ChangeKeys(nil)
	if keys == nil || len(keys) != 0 || first == nil || len(first) != 0 {
		t.Fatalf("no changes: %v, %v", keys, first)
	}
}
