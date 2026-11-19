package outbox

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Purge deletes every key of the cache, and nothing else, in one SCAN of the keyspace. A key is the
// cache's when it has the shape of a registered format (Formats; each %s stands for one segment of
// the key, so coupon:%s takes coupon:<uuid> and never coupon:check:rejections:…), or the shape such
// a format had before generations (WithLegacyFormats). Whatever else shares the Redis — counters,
// rate limits, sessions, caches of other code — does not have such a shape.
//
// It is safe while the app runs: whatever it deletes is seeded again from SQL by the next read.
// Run it once a deploy has replaced every process, so that nothing an older build seeded is left,
// and before the cache is turned on again after it was off (Redis missed every write in between).

// purgeBatch is how many keys one SCAN step asks for and one UNLINK takes.
const purgeBatch = 500

// WithLegacyFormats makes Purge also delete the cache's keys from before generations, for the
// cutover from them: those of every registered format with a version segment right before its
// first %s (or after its last segment when it has none) — post:v3:%s for post:%s — where version is
// the segment's regular expression (`v[0-9]+`), and those of raw, formats in the same versioned
// shape that no registered format describes any more. Without a version Purge takes the registered
// formats alone, and raw is ignored.
func (s *Store) WithLegacyFormats(version string, raw ...string) *Store {
	s.legacyVersion = version
	s.legacyRaw = append([]string(nil), raw...)
	return s
}

// PurgeReport is what Purge found and did.
type PurgeReport struct {
	DryRun  bool
	Scanned int64
	// Deleted counts the keys deleted (or, on a dry run, to delete) per format; a format before
	// generations is listed as "legacy <format>".
	Deleted map[string]int64
	// Kept counts the keys left alone, by their first segment ("session", "stats", …).
	Kept map[string]int64
}

// Total is how many keys were deleted (or would be).
func (r PurgeReport) Total() int64 {
	var total int64
	for _, n := range r.Deleted {
		total += n
	}
	return total
}

// Lines renders the report, one count per line, sorted.
func (r PurgeReport) Lines() []string {
	verb := "deleted"
	if r.DryRun {
		verb = "would delete"
	}
	lines := []string{fmt.Sprintf("scanned %d keys, %s %d, kept %d", r.Scanned, verb, r.Total(), r.Scanned-r.Total())}
	for _, format := range sortedKeys(r.Deleted) {
		lines = append(lines, fmt.Sprintf("  %s %6d  %s", verb, r.Deleted[format], format))
	}
	for _, prefix := range sortedKeys(r.Kept) {
		lines = append(lines, fmt.Sprintf("  kept   %6d  %s:*", r.Kept[prefix], prefix))
	}
	return lines
}

func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// purgeMatcher recognises the keys of one format.
type purgeMatcher struct {
	label string
	re    *regexp.Regexp
}

// purgeMatchers builds the matchers of formats and, with a legacy version (WithLegacyFormats), of
// what they and the raw legacy formats looked like before generations.
func purgeMatchers(formats []KeyFormat, legacyVersion string, legacyRaw []string) []purgeMatcher {
	matchers := make([]purgeMatcher, 0, 2*len(formats)+len(legacyRaw))
	for _, format := range formats {
		matchers = append(matchers, purgeMatcher{label: format.Format, re: formatRegexp(format.Format, format.Index, "")})
	}
	if legacyVersion == "" {
		return matchers
	}
	for _, format := range formats {
		matchers = append(matchers, purgeMatcher{label: "legacy " + format.Format, re: formatRegexp(format.Format, format.Index, legacyVersion)})
	}
	for _, format := range legacyRaw {
		matchers = append(matchers, purgeMatcher{label: "legacy " + format, re: formatRegexp(format, false, legacyVersion)})
	}
	return matchers
}

// match names the format a key belongs to ("" for none).
func match(matchers []purgeMatcher, key string) string {
	for _, matcher := range matchers {
		if matcher.re.MatchString(key) {
			return matcher.label
		}
	}
	return ""
}

// formatRegexp is the regexp of the keys of a format: an item key and its missing-item marker
// (<key>:blank), or a collection under its cluster hash tag with its markers. A legacyVersion: the
// format as it was before generations, a version segment of that regexp right before its first %s
// (or last when it has none), where a parameter could still span segments.
func formatRegexp(format string, index bool, legacyVersion string) *regexp.Regexp {
	legacy := legacyVersion != ""
	if legacy {
		legacyVersion = `(?:` + legacyVersion + `)` // a version with an alternation stays one segment
	}
	param := `[^:{}]+`
	if legacy {
		param = `[^{}]+`
	}
	var body strings.Builder
	versioned := !legacy
	for i := 0; i < len(format); i++ {
		switch {
		case format[i] == '%' && i+1 < len(format) && format[i+1] == '%':
			body.WriteString(regexp.QuoteMeta("%"))
			i++
		case format[i] == '%' && i+1 < len(format) && format[i+1] == 's':
			if !versioned {
				body.WriteString(legacyVersion + ":")
				versioned = true
			}
			body.WriteString(param)
			i++
		default:
			body.WriteString(regexp.QuoteMeta(format[i : i+1]))
		}
	}
	if !versioned {
		body.WriteString(":" + legacyVersion)
	}
	if index {
		return regexp.MustCompile(`^\{` + body.String() + `\}(:(blankpage|firstpage|lastpage|gen))?$`)
	}
	return regexp.MustCompile(`^` + body.String() + `(:blank)?$`)
}

// Purge scans the keyspace of rdb (every master of a cluster) and deletes the cache's keys (see
// above); dryRun only counts them.
func (s *Store) Purge(ctx context.Context, rdb redis.UniversalClient, dryRun bool) (PurgeReport, error) {
	report := PurgeReport{DryRun: dryRun, Deleted: map[string]int64{}, Kept: map[string]int64{}}
	if s.legacyVersion != "" {
		if _, err := regexp.Compile(`(?:` + s.legacyVersion + `)`); err != nil {
			return report, fmt.Errorf("redifucache: legacy version %q: %w", s.legacyVersion, err)
		}
	}
	matchers := purgeMatchers(s.Formats(), s.legacyVersion, s.legacyRaw)
	var mu sync.Mutex // the masters of a cluster are scanned at once
	scan := func(ctx context.Context, node redis.Cmdable) error {
		var cursor uint64
		for {
			keys, next, err := node.Scan(ctx, cursor, "*", purgeBatch).Result()
			if err != nil {
				return err
			}
			var doomed []string
			mu.Lock()
			for _, key := range keys {
				report.Scanned++
				if label := match(matchers, key); label != "" {
					report.Deleted[label]++
					doomed = append(doomed, key)
					continue
				}
				prefix, _, _ := strings.Cut(key, ":")
				report.Kept[prefix]++
			}
			mu.Unlock()
			if len(doomed) > 0 && !dryRun {
				// One UNLINK per key keeps every command on one slot, as a cluster requires.
				pipe := node.Pipeline()
				for _, key := range doomed {
					pipe.Unlink(ctx, key)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					return err
				}
			}
			if next == 0 {
				return nil
			}
			cursor = next
		}
	}
	if cluster, ok := rdb.(*redis.ClusterClient); ok {
		err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error { return scan(ctx, node) })
		return report, err
	}
	return report, scan(ctx, rdb)
}
