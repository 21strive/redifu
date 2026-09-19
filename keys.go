package redifu

import (
	"errors"
	"fmt"
	"strings"
)

// maxKeyParamLength bounds a single key parameter. A runaway parameter would
// otherwise produce multi-megabyte Redis keys.
const maxKeyParamLength = 512

// keyBuilder turns a consumer-supplied key format such as "post:%s:timeline" into a
// concrete Redis key. It validates the format once at construction and the parameter
// count on every call, so a mismatch surfaces as an error instead of a key containing
// "%!s(MISSING)" that silently splits a collection in two.
type keyBuilder struct {
	format string
	arity  int
}

func newKeyBuilder(format string) (*keyBuilder, error) {
	if format == "" {
		return nil, errors.New("redifu: key format must not be empty")
	}

	arity := 0
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		if i+1 >= len(format) {
			return nil, fmt.Errorf("redifu: key format %q ends with a dangling %%", format)
		}
		switch format[i+1] {
		case '%':
			i++
		case 's':
			arity++
			i++
		default:
			return nil, fmt.Errorf("redifu: key format %q uses %%%c — only %%s is supported", format, format[i+1])
		}
	}

	if strings.ContainsAny(format, "{}") {
		return nil, fmt.Errorf("redifu: key format %q must not contain { or } — redifu adds its own cluster hash tag", format)
	}

	return &keyBuilder{format: format, arity: arity}, nil
}

// suffixed derives a new builder whose keys carry an extra literal suffix, e.g. the
// page index that hangs off a Page's key format.
func (k *keyBuilder) suffixed(suffix string) *keyBuilder {
	return &keyBuilder{format: k.format + suffix, arity: k.arity}
}

// extended derives a builder that takes one more parameter than this one, e.g. a
// Page's per-page sorted set.
func (k *keyBuilder) extended(suffix string) *keyBuilder {
	return &keyBuilder{format: k.format + suffix, arity: k.arity + 1}
}

func validateKeyParam(param string) error {
	if param == "" {
		return errors.New("redifu: key parameter must not be empty — an empty parameter makes two different collections share one key")
	}
	if len(param) > maxKeyParamLength {
		return fmt.Errorf("redifu: key parameter is %d bytes, limit is %d", len(param), maxKeyParamLength)
	}
	if strings.ContainsAny(param, "{}") {
		return fmt.Errorf("redifu: key parameter %q must not contain { or } — those characters control Redis Cluster slot placement", param)
	}
	for i := 0; i < len(param); i++ {
		if param[i] < 0x20 || param[i] == 0x7f {
			return fmt.Errorf("redifu: key parameter %q must not contain control characters", param)
		}
	}
	return nil
}

func (k *keyBuilder) build(params []string) (string, error) {
	if len(params) != k.arity {
		return "", fmt.Errorf("redifu: key format %q takes %d parameter(s), got %d", k.format, k.arity, len(params))
	}
	values := make([]interface{}, len(params))
	for i, param := range params {
		if err := validateKeyParam(param); err != nil {
			return "", err
		}
		values[i] = param
	}
	return fmt.Sprintf(k.format, values...), nil
}

// buildTagged wraps the key in a Redis Cluster hash tag so that the collection's
// sorted set and its state markers always land in the same slot. That is what makes
// the single-round-trip Lua ingest legal on a cluster: every key a script touches
// must belong to one slot.
func (k *keyBuilder) buildTagged(params []string) (string, error) {
	key, err := k.build(params)
	if err != nil {
		return "", err
	}
	return "{" + key + "}", nil
}

// appendParams returns a new slice. Spreading a variadic argument in Go hands the
// callee the caller's own slice, so appending to keyParams in place can write into
// the caller's backing array. Every derived key in redifu goes through here.
func appendParams(params []string, extra ...string) []string {
	joined := make([]string, 0, len(params)+len(extra))
	joined = append(joined, params...)
	joined = append(joined, extra...)
	return joined
}
