package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// Buckets is a time-ranged view kept as one redifu Sorted per (owner, UTC month) — a calendar,
// say — rather than a TimeSeries: each month is a whole list, seeded and served like a List. The
// score is a Unix time in seconds; a range read walks the months it touches, seeding each cold
// one, and reads each with a score range. A month with nothing in it is marked empty, and the
// first write into it clears the marker so the next read seeds it again.
type Buckets[E Scored] struct {
	store  *Store
	base   *redifu.Base[E]
	sorted *redifu.Sorted[E]
	format string
	tables []string
	// load returns every member of owner whose score lies in [from, to) (seconds), complete and
	// scored.
	load func(ctx context.Context, tx *sqlx.Tx, owner string, from, to int64) ([]E, error)
}

// MaxBucketMonths caps the months one read walks; a longer range is read from SQL.
const MaxBucketMonths = 13

// NewBuckets builds the index. keyFormat takes two %s: the owner and the month (YYYY-MM).
func NewBuckets[E Scored](store *Store, base *redifu.Base[E], keyFormat string, tables []string,
	load func(ctx context.Context, tx *sqlx.Tx, owner string, from, to int64) ([]E, error),
) (*Buckets[E], error) {
	sorted, err := NewSorted[E](store, base, keyFormat, ListTTL)
	if err != nil {
		return nil, err
	}
	if err := sorted.SetSortingReference("Score"); err != nil {
		return nil, err
	}
	// A member whose entity is gone (evicted) must make the read rebuild its month, never quietly
	// leave the calendar: readMonth counts dangling members and purges.
	sorted.SetSelfHeal(false)
	return &Buckets[E]{store: store, base: base, sorted: sorted, format: keyFormat, tables: tables, load: load}, nil
}

// Month names the bucket of a time in seconds: its UTC calendar month.
func Month(seconds int64) string {
	return time.Unix(seconds, 0).UTC().Format("2006-01")
}

// monthBounds is [first second of the month, first second of the next) for a YYYY-MM.
func monthBounds(month string) (int64, int64, error) {
	start, err := time.Parse("2006-01", month)
	if err != nil {
		return 0, 0, err
	}
	return start.Unix(), start.AddDate(0, 1, 0).Unix(), nil
}

// MonthsBetween lists the months a [from, to] range of seconds touches.
func MonthsBetween(from, to int64) []string {
	if to < from {
		return nil
	}
	first := time.Unix(from, 0).UTC()
	cursor := time.Date(first.Year(), first.Month(), 1, 0, 0, 0, 0, time.UTC)
	last := time.Unix(to, 0).UTC()
	var months []string
	for !cursor.After(last) {
		months = append(months, cursor.Format("2006-01"))
		cursor = cursor.AddDate(0, 1, 0)
	}
	return months
}

// Read returns the members of owner with a score in [from, to] (seconds, both ends inclusive,
// as ZRANGEBYSCORE), in score order. ok is false when the cache cannot answer.
func (b *Buckets[E]) Read(ctx context.Context, owner string, from, to int64) ([]E, bool) {
	months := MonthsBetween(from, to)
	if len(months) == 0 {
		return []E{}, true
	}
	if len(months) > MaxBucketMonths {
		return nil, false
	}
	if !b.store.Serve(ctx, owner, b.tables...) {
		return nil, false
	}
	var out []E
	for _, month := range months {
		items, ok := b.readMonth(ctx, owner, month, from, to)
		if !ok {
			return nil, false
		}
		out = append(out, items...)
	}
	return out, true
}

func (b *Buckets[E]) readMonth(ctx context.Context, owner, month string, from, to int64) ([]E, bool) {
	start, end, err := monthBounds(month)
	if err != nil {
		return nil, false
	}
	lower, upper := from, to
	if lower < start {
		lower = start
	}
	if upper > end-1 {
		upper = end - 1
	}
	for attempt := 0; attempt < 2; attempt++ {
		needs, err := b.sorted.RequiresSeeding(ctx, owner, month)
		if err != nil {
			b.store.RedisFailed(err)
			return nil, false
		}
		if needs && !b.SeedMonth(ctx, owner, month) {
			return nil, false
		}
		items, dangling, err := b.sorted.Fetch(redifu.Ascending).WithParams(owner, month).WithRange(lower, upper).ExecReport(ctx)
		if errors.Is(err, redifu.ErrNotSeeded) {
			continue // the month stopped being this build's after the check: seed it again
		}
		if err != nil {
			b.store.RedisFailed(err)
			return nil, false
		}
		count, err := b.countRange(ctx, owner, month, lower, upper)
		if err != nil {
			b.store.RedisFailed(err)
			return nil, false
		}
		if dangling == 0 && int64(len(items)) == count {
			if count > 0 {
				return items, true
			}
			empty, errEmpty := b.sorted.IsEmpty(ctx, owner, month)
			total, errCount := b.sorted.Count(ctx, owner, month)
			if errEmpty == nil && errCount == nil && (empty || total > 0) {
				return items, true
			}
		}
		if err := b.sorted.Purge(ctx, owner, month); err != nil {
			b.store.RedisFailed(err)
			return nil, false
		}
	}
	return nil, false
}

// countRange counts the members with a score in [lower, upper] (ZCOUNT). It skips redifu's
// generation check: it runs right after the fetch, whose check has dropped an index of another
// generation, and a count that disagrees with the fetch only purges and seeds again.
func (b *Buckets[E]) countRange(ctx context.Context, owner, month string, lower, upper int64) (int64, error) {
	return b.store.rdb.ZCount(ctx, b.indexKey(owner, month), fmt.Sprint(lower), fmt.Sprint(upper)).Result()
}

// indexKey is the month's Sorted as redifu keys it: the key format inside a cluster hash tag.
func (b *Buckets[E]) indexKey(owner, month string) string {
	return fmt.Sprintf("{%s}", b.keyFor(owner, month))
}

// CountBetween is how many members of owner the months of [from, to] (seconds, inclusive) hold in
// that range, in one round trip and without reading a member: what a Read of the range would
// decode. A cold month counts nothing (reading it seeds it). ok false: Redis failed, or the range
// is longer than Read serves. It skips redifu's generation check (plain ZCOUNT): a month of another
// generation is counted as it stands, which only decides whether the caller reads the range from
// the cache or SQL — the read itself is checked.
func (b *Buckets[E]) CountBetween(ctx context.Context, owner string, from, to int64) (int64, bool) {
	months := MonthsBetween(from, to)
	if len(months) > MaxBucketMonths {
		return 0, false
	}
	if len(months) == 0 {
		return 0, true
	}
	pipe := b.store.rdb.Pipeline()
	counts := make([]*redis.IntCmd, 0, len(months))
	for _, month := range months {
		counts = append(counts, pipe.ZCount(ctx, b.indexKey(owner, month), fmt.Sprint(from), fmt.Sprint(to)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.store.RedisFailed(err)
		return 0, false
	}
	var total int64
	for _, count := range counts {
		total += count.Val()
	}
	return total, true
}

// keyFor renders the bucket key without the hash tag redifu wraps collection keys in.
func (b *Buckets[E]) keyFor(owner, month string) string {
	return fmt.Sprintf(b.format, owner, month)
}

// Sorted is the index (drain handlers add and remove through it).
func (b *Buckets[E]) Sorted() *redifu.Sorted[E] { return b.sorted }

// SeedMonth fills one month under the owner's lock.
func (b *Buckets[E]) SeedMonth(ctx context.Context, owner, month string) bool {
	start, end, err := monthBounds(month)
	if err != nil {
		return false
	}
	seeded, err := b.store.Seed(ctx, owner, func(tx *sqlx.Tx, pipe redis.Pipeliner) error {
		items, errLoad := b.load(ctx, tx, owner, start, end)
		if errLoad != nil {
			return errLoad
		}
		for _, item := range items {
			if errSet := b.base.WithPipeline(pipe).Set(ctx, item); errSet != nil {
				return errSet
			}
			if errIngest := b.sorted.IngestItem(ctx, pipe, item, true, owner, month); errIngest != nil {
				return errIngest
			}
		}
		if len(items) == 0 {
			if errMark := b.sorted.MarkEmpty(ctx, pipe, owner, month); errMark != nil {
				return errMark
			}
		} else if errData := b.sorted.HasData(ctx, pipe, owner, month); errData != nil {
			return errData
		}
		return b.sorted.SetExpiration(ctx, pipe, owner, month)
	})
	return err == nil && seeded
}

// Add enqueues item into its month (the caller sets its score, in seconds, first).
func (b *Buckets[E]) Add(ctx context.Context, pipe redis.Pipeliner, item E, owner string, seconds int64) error {
	return b.sorted.WithPipeline(pipe).AddItem(ctx, item, owner, Month(seconds))
}

// Remove enqueues item's removal from the month of seconds.
func (b *Buckets[E]) Remove(ctx context.Context, pipe redis.Pipeliner, item E, owner string, seconds int64) error {
	return b.sorted.WithPipeline(pipe).RemoveItem(ctx, item, owner, Month(seconds))
}
