package outbox

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

// Feed is a cursor-paged view kept in one redifu Timeline: the head is seeded on the
// first read, and the index is extended from its real end — never from a client's cursor — whenever
// a page runs past it, until the database says there is no more (lastpage). A cursor is a position
// (score, member), so a client keeps its place when the item it stopped at leaves the feed.
//
// The order is the Timeline's: Descending = score descending, then member (uuid) descending by bytes.
// load must return rows in exactly that order with the same keyset, e.g. for a feed scored by
// created_at in microseconds (exact in a float64 until 2255):
//
//	WHERE (floor(extract(epoch FROM created_at) * 1000000)::bigint, uuid COLLATE "C") < ($score, $member)
//	ORDER BY 1 DESC, uuid COLLATE "C" DESC LIMIT $limit
type Feed[E Scored] struct {
	store    *Store
	base     *redifu.Base[E]
	timeline *redifu.Timeline[E]
	tables   []string
	perPage  int64
	load     func(ctx context.Context, tx *sqlx.Tx, params []string, after redifu.Cursor, limit int64) ([]E, error)
	owner    func(params []string) string
}

// FeedPage is one page of a feed.
type FeedPage[E Scored] struct {
	Items   []E
	Next    redifu.Cursor
	HasMore bool
}

// feedChunk is how many rows one extension loads at least.
const feedChunk = 50

// feedMaxExtensions bounds the extensions one read may do to reach a deep cursor; past it the
// read goes to SQL.
const feedMaxExtensions = 20

// NewFeed builds a feed. perPage is the page size of every read; keyFormat names the index.
func NewFeed[E Scored](store *Store, base *redifu.Base[E], keyFormat string, perPage int64, direction string, tables []string,
	load func(ctx context.Context, tx *sqlx.Tx, params []string, after redifu.Cursor, limit int64) ([]E, error),
	owner func(params []string) string,
) (*Feed[E], error) {
	timeline, err := NewTimeline[E](store, base, keyFormat, perPage, direction, ListTTL)
	if err != nil {
		return nil, err
	}
	if err := timeline.SetSortingReference("Score"); err != nil {
		return nil, err
	}
	// A member whose entity is gone makes the read rebuild the feed (ReadServed purges on Dangling);
	// removing it quietly would shorten the feed instead.
	timeline.SetSelfHeal(false)
	return &Feed[E]{store: store, base: base, timeline: timeline, tables: tables, perPage: perPage, load: load, owner: owner}, nil
}

// Timeline is the index (drain handlers add and remove through it).
func (f *Feed[E]) Timeline() *redifu.Timeline[E] { return f.timeline }

// PerPage is the page size of the feed.
func (f *Feed[E]) PerPage() int64 { return f.perPage }

// Read returns the page after the position (the zero Cursor reads the first page). ok false: the
// caller reads SQL.
func (f *Feed[E]) Read(ctx context.Context, params []string, after redifu.Cursor) (*FeedPage[E], bool) {
	return f.ReadN(ctx, params, after, f.perPage)
}

// ReadN is Read with a page of n items instead of PerPage (n < 1 reads PerPage), so one index
// serves every page size a client asks for. Bound n before it gets here when a client chose it.
func (f *Feed[E]) ReadN(ctx context.Context, params []string, after redifu.Cursor, n int64) (*FeedPage[E], bool) {
	if !f.store.Serve(ctx, f.owner(params), f.tables...) {
		return nil, false
	}
	return f.ReadServed(ctx, params, after, n)
}

// ReadServed is ReadN for a caller that has just had the owner's queue applied (Store.Serve or
// ServeMany): one check of the queue before several reads of the same owner.
func (f *Feed[E]) ReadServed(ctx context.Context, params []string, after redifu.Cursor, n int64) (*FeedPage[E], bool) {
	if n < 1 {
		n = f.perPage
	}
	owner := f.owner(params)
	extensions := 0
	for attempt := 0; attempt < feedMaxExtensions+3; attempt++ {
		count, err := f.timeline.Count(ctx, params...)
		if err != nil {
			f.store.RedisFailed(err)
			return nil, false
		}
		if count == 0 {
			empty, err := f.timeline.IsEmpty(ctx, params...)
			if err != nil {
				f.store.RedisFailed(err)
				return nil, false
			}
			if !empty {
				if !f.seedAfter(ctx, owner, params, redifu.Cursor{}, n+1) {
					return nil, false
				}
				continue
			}
		}

		out := f.timeline.FetchFrom(after).WithParams(params...).WithLimit(n).Exec(ctx)
		if out.Error() != nil {
			f.store.RedisFailed(out.Error())
			return nil, false
		}
		if out.Dangling() > 0 {
			// Members whose entity is gone: rebuild the feed from the database.
			if err := f.timeline.Purge(ctx, params...); err != nil {
				f.store.RedisFailed(err)
				return nil, false
			}
			continue
		}
		if !out.HasMore() {
			end, err := f.timeline.ReachedEnd(ctx, params...)
			if err != nil {
				f.store.RedisFailed(err)
				return nil, false
			}
			if !end {
				if extensions >= feedMaxExtensions {
					return nil, false
				}
				extensions++
				tail, nonEmpty, err := f.timeline.Tail(ctx, params...)
				if err != nil {
					f.store.RedisFailed(err)
					return nil, false
				}
				if !nonEmpty {
					tail = redifu.Cursor{}
				}
				limit := n - int64(len(out.Items())) + 1
				if limit < feedChunk {
					limit = feedChunk
				}
				if !f.seedAfter(ctx, owner, params, tail, limit) {
					return nil, false
				}
				continue
			}
		}
		return &FeedPage[E]{Items: out.Items(), Next: out.NextCursor(), HasMore: out.HasMore()}, true
	}
	return nil, false
}

// seedAfter loads up to limit rows after anchor under the owner's lock and adds them to the
// index; fewer rows than asked marks the end of the feed.
//
// A head seed (zero anchor) starts the index's life: it drops a :lastpage marker left by the
// previous one (an evicted index can leave it), which would otherwise end the new feed after its
// first rows, and sets the index's TTL. A top-up is a redifu Extend: it applies only while the
// index still ends at anchor and is this build's, and renews the TTL with the marker it may set.
// Between Tail and the top-up the index can be dropped (a read of another generation), purged
// (dangling members) or seeded again from its head; written with IngestItem the rows after the old
// tail would then make an index without its head. A refused Extend writes nothing and the read
// loop reads again.
func (f *Feed[E]) seedAfter(ctx context.Context, owner string, params []string, anchor redifu.Cursor, limit int64) bool {
	seeded, err := f.store.Seed(ctx, owner, func(tx *sqlx.Tx, pipe redis.Pipeliner) error {
		items, err := f.load(ctx, tx, params, anchor, limit)
		if err != nil {
			return err
		}
		for _, item := range items {
			if err := f.base.WithPipeline(pipe).Set(ctx, item); err != nil {
				return err
			}
		}
		if !anchor.IsZero() {
			_, err := f.timeline.Extend(ctx, pipe, anchor, items, int64(len(items)) < limit, params...)
			return err
		}
		if err := f.timeline.UnmarkLastPage(ctx, pipe, params...); err != nil {
			return err
		}
		for _, item := range items {
			if err := f.timeline.IngestItem(ctx, pipe, item, true, params...); err != nil {
				return err
			}
		}
		switch {
		case len(items) == 0:
			if err := f.timeline.MarkEmpty(ctx, pipe, params...); err != nil {
				return err
			}
		case int64(len(items)) < limit:
			if err := f.timeline.MarkLastPage(ctx, pipe, params...); err != nil {
				return err
			}
		}
		if len(items) > 0 {
			if err := f.timeline.HasData(ctx, pipe, params...); err != nil {
				return err
			}
		}
		return f.timeline.SetExpiration(ctx, pipe, params...)
	})
	return err == nil && seeded
}

// Add enqueues item into the feed (the timeline decides whether it falls in the window it holds).
func (f *Feed[E]) Add(ctx context.Context, pipe redis.Pipeliner, item E, params ...string) error {
	return f.timeline.WithPipeline(pipe).AddItem(ctx, item, params...)
}

// Remove enqueues item's removal from the feed.
func (f *Feed[E]) Remove(ctx context.Context, pipe redis.Pipeliner, item E, params ...string) error {
	return f.timeline.WithPipeline(pipe).RemoveItem(ctx, item, params...)
}

// EncodeCursor renders a position for a client: opaque, URL safe, "" for the zero Cursor.
func EncodeCursor(cursor redifu.Cursor) string {
	if cursor.IsZero() {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(cursor.String()))
}

// ErrBadCursor is a cursor no feed handed out.
var ErrBadCursor = errors.New("redifucache: malformed cursor")

// DecodeCursor reads back what EncodeCursor wrote ("" is the zero Cursor).
func DecodeCursor(text string) (redifu.Cursor, error) {
	if text == "" {
		return redifu.Cursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil {
		return redifu.Cursor{}, ErrBadCursor
	}
	cursor, err := redifu.ParseCursor(string(raw))
	if err != nil {
		return redifu.Cursor{}, ErrBadCursor
	}
	return cursor, nil
}
