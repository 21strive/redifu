package outbox

import "database/sql/driver"

// textArray sends a []string to a text[] parameter as the array's text form, byte for byte what
// lib/pq's StringArray writes — {"a","b\"c"}, NULL for a nil slice — so that the package needs no
// driver of its own: database/sql hands the value to whichever driver the application uses.
type textArray []string

// Value renders the array literal: every element quoted, with a backslash before each " and \.
func (a textArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	if len(a) == 0 {
		return "{}", nil
	}
	b := make([]byte, 1, 1+3*len(a))
	b[0] = '{'
	for i, element := range a {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '"')
		for j := 0; j < len(element); j++ {
			if c := element[j]; c == '"' || c == '\\' {
				b = append(b, '\\')
			}
			b = append(b, element[j])
		}
		b = append(b, '"')
	}
	return string(append(b, '}')), nil
}
