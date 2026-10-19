package redifu

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Generations let a consumer change what it stores without renaming its keys.
//
// A version segment in a key name ("post:v3:%s") works once and then sticks: every bump turns
// the whole cache cold at once, and during a rolling deploy the old process keeps writing the old
// names while the new one reads the new names, so whatever the old one changes never reaches the
// keys the new one serves. A generation keeps one name per piece of data and moves "which build
// wrote this" into the stored state, where every read and write compares it:
//
//   - Base.SetGeneration prefixes every value the Base writes with the generation. A value without
//     that prefix — written before generations, or by another generation — is a key that does not
//     exist: Get answers ErrNotFound, GetMany leaves it out, an index counts its member as
//     dangling, a relation leaves its field nil.
//   - SetGeneration on Sorted, Timeline, Page and TimeSeries stamps the index with a marker,
//     {K}:gen, next to its other markers. An index whose marker is absent or names another
//     generation does not exist for this one: the operation that finds it drops the sorted set and
//     all its markers first, atomically with its own work, so a read finds a cold index, AddItem
//     and RemoveItem report ErrNotIngested (the index is gone, the next read seeds it), and a seed
//     starts from an empty index and stamps it.
//
// Two builds of different generations can therefore share one Redis: each sees only its own values
// and indexes, and a write by either one removes the other's index instead of leaving it stale.
// Without SetGeneration nothing changes: values carry no prefix and no marker is read or written.

// markerGeneration is the suffix of an index's generation marker.
const markerGeneration = ":gen"

// generationSeparator ends the generation prefix of a stored value. A JSON document never holds
// it raw (JSON escapes every control character inside a string), so the first one in a value is
// always the end of the prefix, and a value written without a prefix never has one where the
// prefix ends.
const generationSeparator = "\x1f"

// maxGenerationLength bounds a generation. It is stored in every value and every index marker.
const maxGenerationLength = 128

// validateGeneration checks a generation before it is used. "" turns generations off.
func validateGeneration(generation string) error {
	if len(generation) > maxGenerationLength {
		return fmt.Errorf("redifu: generation is %d bytes, limit is %d", len(generation), maxGenerationLength)
	}
	for i := 0; i < len(generation); i++ {
		if generation[i] < 0x20 || generation[i] == 0x7f {
			return fmt.Errorf("redifu: generation %q must not contain control characters", generation)
		}
	}
	return nil
}

// generationPrefix is what a Base of this generation puts in front of every value ("" without
// a generation).
func generationPrefix(generation string) string {
	if generation == "" {
		return ""
	}
	return generation + generationSeparator
}

// ownValue returns the payload of a stored value when it carries prefix, and false for a value of
// another generation (or none).
func ownValue(value string, prefix string) (string, bool) {
	if prefix == "" {
		return value, true
	}
	if !strings.HasPrefix(value, prefix) {
		return "", false
	}
	return value[len(prefix):], true
}

// The generation scripts take the keys of one collection in a fixed order — the sorted set, its
// state markers, and its generation marker last — so that the guard can find the marker at
// KEYS[#KEYS] and drop everything with one DEL. The generation is always the last argument.
// Every key is under the collection's hash tag.

// generationGuard opens a script: when the marker does not hold this generation, the collection is
// another generation's (or nobody's), so it is dropped whole and the script returns onForeign.
func generationGuard(onForeign string) string {
	return `
if redis.call('GET', KEYS[#KEYS]) ~= ARGV[#ARGV] then
  redis.call('DEL', unpack(KEYS))
  return ` + onForeign + `
end
`
}

// generationCheckScript is the guard alone: 1 when the index is this generation's, 0 when it was
// another generation's and has just been dropped. Reads run it first in the same MULTI/EXEC, so
// what they read after it is either this generation's index or a cold one.
//
//	KEYS sorted set, markers..., generation marker   ARGV generation
var generationCheckScript = redis.NewScript(generationGuard("0") + `
return 1
`)

// generationStampScript is how a seed writes into an index of a generation: it drops the index
// when its marker names another generation (or none), stamps the marker with this generation and
// the index TTL, then performs one write. The first write of a seed therefore starts it from an
// empty index, and a seed run in one MULTI/EXEC can never mix its members with another
// generation's.
//
//	KEYS sorted set, markers..., generation marker
//	ARGV[1] TTL in ms (0: none)   ARGV[2] op   ARGV[3], ARGV[4] op arguments   ARGV[5] generation
//
// op is "zadd" (score, member), "mark" (KEYS index of a marker: set it to "1" with the TTL),
// "unmark" (KEYS index of a marker: delete it), "expire" (apply the TTL to the sorted set) or
// "stamp" (nothing more).
var generationStampScript = redis.NewScript(`
local ttl = tonumber(ARGV[1])
local generation = ARGV[#ARGV]
if redis.call('GET', KEYS[#KEYS]) ~= generation then
  redis.call('DEL', unpack(KEYS))
end
if ttl > 0 then
  redis.call('SET', KEYS[#KEYS], generation, 'PX', ttl)
else
  redis.call('SET', KEYS[#KEYS], generation)
end
local op = ARGV[2]
if op == 'zadd' then
  return redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
elseif op == 'mark' then
  local marker = KEYS[tonumber(ARGV[3])]
  if ttl > 0 then
    redis.call('SET', marker, '1', 'PX', ttl)
  else
    redis.call('SET', marker, '1')
  end
elseif op == 'unmark' then
  redis.call('DEL', KEYS[tonumber(ARGV[3])])
elseif op == 'expire' then
  if ttl > 0 then
    redis.call('PEXPIRE', KEYS[1], ttl)
  end
end
return 1
`)

// generationRemoveScript removes members from an index of this generation. An index of another
// generation is dropped instead: its members follow rules this build does not know, so leaving it
// would leave it stale.
//
//	KEYS sorted set, markers..., generation marker   ARGV members..., generation
//
// Returns 1 if the index was this generation's, 0 if it was dropped.
var generationRemoveScript = redis.NewScript(generationGuard("0") + `
if #ARGV > 1 then
  redis.call('ZREM', KEYS[1], unpack(ARGV, 1, #ARGV - 1))
end
return 1
`)

// generationTimelineRemoveScript is generationRemoveScript for a Timeline, which also drops its
// first-page and last-page markers: a removal invalidates both.
//
//	KEYS sorted set, :firstpage, :lastpage, :blankpage, generation marker   ARGV members..., generation
var generationTimelineRemoveScript = redis.NewScript(generationGuard("0") + `
if #ARGV > 1 then
  redis.call('ZREM', KEYS[1], unpack(ARGV, 1, #ARGV - 1))
end
redis.call('DEL', KEYS[2], KEYS[3])
return 1
`)

// generationSortedIngestScript is sortedIngestScript behind the generation guard.
//
//	KEYS sorted set, :blankpage, generation marker   ARGV score, member, generation
var generationSortedIngestScript = redis.NewScript(generationGuard("0") + sortedIngestBody)

// generationTimelineIngestScript is timelineIngestScript behind the generation guard.
//
//	KEYS sorted set, :firstpage, :lastpage, :blankpage, generation marker
//	ARGV score, member, itemPerPage, direction, generation
var generationTimelineIngestScript = redis.NewScript(generationGuard("0") + timelineIngestBody)

// ttlMillis renders a TTL for the generation scripts: whole milliseconds, 0 for none.
func ttlMillis(timeToLive time.Duration) int64 {
	if timeToLive <= 0 {
		return 0
	}
	if ms := timeToLive.Milliseconds(); ms > 0 {
		return ms
	}
	return 1
}

// isNilReply reports whether err is only the "no such key" reply of a read, which a pipeline
// reports as its error when any of its commands got one.
func isNilReply(err error) bool {
	return err == nil || errors.Is(err, redis.Nil)
}

// generationKeys are the keys a generation script of this collection takes: the sorted set, its
// state markers, and the generation marker last.
func (cr *SortedSet[T]) generationKeys(keyParams []string) ([]string, error) {
	key, err := cr.key(keyParams)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(cr.markers)+2)
	keys = append(keys, key)
	for _, marker := range cr.markers {
		keys = append(keys, key+marker)
	}
	return append(keys, key+markerGeneration), nil
}

// markerPosition is the 1-based position of a marker among generationKeys, as the stamp script
// addresses it.
func (cr *SortedSet[T]) markerPosition(suffix string) (int, error) {
	for i, marker := range cr.markers {
		if marker == suffix {
			return i + 2, nil
		}
	}
	return 0, fmt.Errorf("redifu: this collection has no %s marker", suffix)
}

// stamp enqueues one write of a seed into the caller's pipeline, behind the generation stamp (see
// generationStampScript). Only called with a generation set.
func (cr *SortedSet[T]) stamp(ctx context.Context, pipe redis.Pipeliner, timeToLive time.Duration, op string, first, second interface{}, keyParams []string) error {
	keys, err := cr.generationKeys(keyParams)
	if err != nil {
		return err
	}
	if first == nil {
		first = ""
	}
	if second == nil {
		second = ""
	}
	generationStampScript.Eval(ctx, pipe, keys, ttlMillis(timeToLive), op, first, second, cr.generation)
	return nil
}

// stampMarker enqueues setting (or, with set false, deleting) one state marker during a seed.
func (cr *SortedSet[T]) stampMarker(ctx context.Context, pipe redis.Pipeliner, timeToLive time.Duration, suffix string, set bool, keyParams []string) error {
	position, err := cr.markerPosition(suffix)
	if err != nil {
		return err
	}
	op := "unmark"
	if set {
		op = "mark"
	}
	return cr.stamp(ctx, pipe, timeToLive, op, position, nil, keyParams)
}

// guarded runs read's commands after the generation check, in one MULTI/EXEC, and reports whether
// the index was this generation's (false: it has just been dropped, and read saw a cold index).
// Only called with a generation set.
func (cr *SortedSet[T]) guarded(ctx context.Context, keyParams []string, read func(pipe redis.Pipeliner) error) (bool, error) {
	keys, err := cr.generationKeys(keyParams)
	if err != nil {
		return false, err
	}
	pipe := cr.client.TxPipeline()
	check := generationCheckScript.Eval(ctx, pipe, keys, cr.generation)
	if err := read(pipe); err != nil {
		return false, err
	}
	if _, err := pipe.Exec(ctx); !isNilReply(err) {
		return false, err
	}
	own, err := check.Int64()
	if err != nil {
		return false, err
	}
	return own == 1, nil
}

// removeMembers enqueues the removal of members from an index of this generation (see
// generationRemoveScript); withPageMarkers also drops a Timeline's first- and last-page markers.
// Only called with a generation set.
func (cr *SortedSet[T]) removeMembers(ctx context.Context, cmd redis.Scripter, withPageMarkers bool, members []interface{}, keyParams []string) (*redis.Cmd, error) {
	keys, err := cr.generationKeys(keyParams)
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, members...)
	args = append(args, cr.generation)
	if withPageMarkers {
		return generationTimelineRemoveScript.Eval(ctx, cmd, keys, args...), nil
	}
	return generationRemoveScript.Eval(ctx, cmd, keys, args...), nil
}

// guardedMarker reads one state marker behind the generation check: false when it is not set, or
// when the index was another generation's and has just been dropped.
func (cr *SortedSet[T]) guardedMarker(ctx context.Context, markerKey string, keyParams []string) (bool, error) {
	var marker *redis.StringCmd
	if _, err := cr.guarded(ctx, keyParams, func(pipe redis.Pipeliner) error {
		marker = pipe.Get(ctx, markerKey)
		return nil
	}); err != nil {
		return false, err
	}
	value, err := marker.Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "1", nil
}
