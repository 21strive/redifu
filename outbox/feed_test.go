package outbox

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// A feed of rows scored by created_at (ms), newest first, per owner and per (owner, status).

type feedFixture struct {
	*unitFixture
	feed      *Feed[*unitEntity]
	byStatus  *Feed[*unitEntity]
	loads     atomic.Int64
	createdAt time.Time
	serial    int
}

const feedColumns = `uuid, randid, created_at, updated_at, user_uuid, name, sort, hidden`

func (f *feedFixture) scoreSQL() string {
	return `(floor(extract(epoch FROM created_at) * 1000000))::bigint`
}

func (f *feedFixture) loadAfter(ctx context.Context, tx *sqlx.Tx, where string, args []any, after redifu.Cursor, limit int64) ([]*unitEntity, error) {
	f.loads.Add(1)
	query := `SELECT ` + feedColumns + `, ` + f.scoreSQL() + ` AS score FROM ` + f.table + ` WHERE ` + where
	if !after.IsZero() {
		query += fmt.Sprintf(` AND (%s, uuid COLLATE "C") < ($%d, $%d)`, f.scoreSQL(), len(args)+1, len(args)+2)
		args = append(args, int64(after.Score), after.Member)
	}
	query += fmt.Sprintf(` ORDER BY %s DESC, uuid COLLATE "C" DESC LIMIT %d`, f.scoreSQL(), limit)
	var rows []struct {
		unitItem
		ScoreValue int64 `db:"score"`
	}
	if err := tx.SelectContext(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]*unitEntity, 0, len(rows))
	for i := range rows {
		item := rows[i].unitItem
		out = append(out, &unitEntity{unitItem: &item, Score: rows[i].ScoreValue})
	}
	return out, nil
}

type feedHandler struct{ f *feedFixture }

func (h feedHandler) Tables() []string { return []string{h.f.table} }

func (h feedHandler) Apply(batch *Batch, table string, changes []Change) error {
	f := h.f
	keys := map[string]Change{}
	var order []string
	for _, change := range changes {
		if _, seen := keys[change.Key]; !seen {
			keys[change.Key] = change
			order = append(order, change.Key)
		}
	}
	var rows []struct {
		unitItem
		ScoreValue int64 `db:"score"`
	}
	if err := batch.Tx.SelectContext(batch.Ctx, &rows, `SELECT `+feedColumns+`, `+f.scoreSQL()+` AS score FROM `+f.table+` WHERE uuid = ANY($1)`, pq.Array(order)); err != nil {
		return err
	}
	byKey := map[string]*unitEntity{}
	for i := range rows {
		item := rows[i].unitItem
		byKey[item.UUID] = &unitEntity{unitItem: &item, Score: rows[i].ScoreValue}
	}
	for _, key := range order {
		old := keys[key].Old()
		now := byKey[key]
		stub := &unitEntity{unitItem: &unitItem{Record: &redifu.Record{UUID: key}}}
		if old != nil && (now == nil || old.Str("name") != now.Name.String) {
			if err := f.byStatus.Remove(batch.Ctx, batch.Pipe, stub, old.Str("user_uuid"), old.Str("name")); err != nil {
				return err
			}
		}
		if now == nil {
			if old != nil {
				if err := f.feed.Remove(batch.Ctx, batch.Pipe, stub, old.Str("user_uuid")); err != nil {
					return err
				}
			}
			if err := f.base.WithPipeline(batch.Pipe).Del(batch.Ctx, stub); err != nil {
				return err
			}
			continue
		}
		if err := f.base.WithPipeline(batch.Pipe).Set(batch.Ctx, now); err != nil {
			return err
		}
		if err := f.feed.Add(batch.Ctx, batch.Pipe, now, now.UserUUID); err != nil {
			return err
		}
		if err := f.byStatus.Add(batch.Ctx, batch.Pipe, now, now.UserUUID, now.Name.String); err != nil {
			return err
		}
	}
	return nil
}

func newFeedFixture(t *testing.T, perPage int64) *feedFixture {
	u := newUnitFixture(t)
	f := &feedFixture{unitFixture: u, createdAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var err error
	f.feed, err = NewFeed[*unitEntity](u.store, u.base, "unit:feed:%s", perPage, redifu.Descending, []string{u.table},
		func(ctx context.Context, tx *sqlx.Tx, params []string, after redifu.Cursor, limit int64) ([]*unitEntity, error) {
			return f.loadAfter(ctx, tx, "user_uuid = $1", []any{params[0]}, after, limit)
		}, func(params []string) string { return params[0] })
	if err != nil {
		t.Fatal(err)
	}
	f.byStatus, err = NewFeed[*unitEntity](u.store, u.base, "unit:feed:%s:%s", perPage, redifu.Descending, []string{u.table},
		func(ctx context.Context, tx *sqlx.Tx, params []string, after redifu.Cursor, limit int64) ([]*unitEntity, error) {
			return f.loadAfter(ctx, tx, "user_uuid = $1 AND name = $2", []any{params[0], params[1]}, after, limit)
		}, func(params []string) string { return params[0] })
	if err != nil {
		t.Fatal(err)
	}
	u.store.handlers = map[string][]Handler{}
	u.store.Register(feedHandler{f: f})
	return f
}

// insert adds n rows for owner, one second apart (two share each second when tie is set).
func (f *feedFixture) insert(t *testing.T, owner, status string, n int, tie bool) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		f.createdAt = f.createdAt.Add(time.Second)
		at := f.createdAt
		if tie && i%2 == 1 {
			at = f.createdAt.Add(-time.Second) // same second as the previous row
		}
		f.serial++
		id := fmt.Sprintf("%s-%04d-%s", owner, f.serial, strings.Repeat("x", i%3))
		f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort, created_at) VALUES ($1, $2, $3, 0, $4)`, id, owner, status, at)
		ids = append(ids, id)
	}
	return ids
}

func (f *feedFixture) walk(t *testing.T, feed *Feed[*unitEntity], params ...string) []string {
	t.Helper()
	var seen []string
	var cursor redifu.Cursor
	for page := 0; page < 200; page++ {
		got, ok := feed.Read(f.ctx, params, cursor)
		if !ok {
			t.Fatalf("page %d not served from the cache", page)
		}
		for _, item := range got.Items {
			seen = append(seen, item.UUID)
		}
		if !got.HasMore {
			return seen
		}
		cursor = got.Next
	}
	t.Fatal("the feed did not end")
	return nil
}

func (f *feedFixture) sqlOrder(t *testing.T, where string, args ...any) []string {
	t.Helper()
	var ids []string
	if err := f.db.Select(&ids, `SELECT uuid FROM `+f.table+` WHERE `+where+` ORDER BY `+f.scoreSQL()+` DESC, uuid COLLATE "C" DESC`, args...); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestFeedWalksEveryRowOnceThenFromRedis(t *testing.T) {
	for _, rows := range []int{0, 1, 7, 20, 21, 40, 50, 133} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			f := newFeedFixture(t, 20)
			f.insert(t, "o1", "INCOMING", rows, true)
			want := f.sqlOrder(t, "user_uuid = 'o1'")
			if got := f.walk(t, f.feed, "o1"); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("first walk:\n got %v\nwant %v", got, want)
			}
			loads := f.loads.Load()
			if got := f.walk(t, f.feed, "o1"); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("second walk differs")
			}
			if f.loads.Load() != loads {
				t.Fatalf("second walk read the database %d times", f.loads.Load()-loads)
			}
		})
	}
}

func TestFeedFollowsWritesMidScroll(t *testing.T) {
	f := newFeedFixture(t, 10)
	ids := f.insert(t, "o1", "INCOMING", 35, false)

	first, ok := f.feed.Read(f.ctx, []string{"o1"}, redifu.Cursor{})
	if !ok || len(first.Items) != 10 {
		t.Fatalf("first page: ok=%v n=%d", ok, len(first.Items))
	}
	// While the reader is on page one: a new row at the head, the row the cursor points at is
	// deleted, and a row further down is deleted.
	f.insert(t, "o1", "INCOMING", 1, false)
	f.exec(t, `DELETE FROM TBL WHERE uuid = $1`, first.Items[len(first.Items)-1].UUID)
	f.exec(t, `DELETE FROM TBL WHERE uuid = $1`, ids[5])

	seen := map[string]bool{}
	for _, item := range first.Items {
		seen[item.UUID] = true
	}
	cursor := first.Next
	for {
		page, ok := f.feed.Read(f.ctx, []string{"o1"}, cursor)
		if !ok {
			t.Fatal("not served")
		}
		for _, item := range page.Items {
			if seen[item.UUID] {
				t.Fatalf("%s seen twice", item.UUID)
			}
			seen[item.UUID] = true
		}
		if !page.HasMore {
			break
		}
		cursor = page.Next
	}
	if seen[ids[5]] {
		t.Fatal("a row deleted below the reader still showed")
	}
	if len(seen) != 34 { // 35 - ids[5] (the row the cursor pointed at was already on page one)
		t.Fatalf("saw %d rows", len(seen))
	}
	// A fresh walk equals the database.
	want := f.sqlOrder(t, "user_uuid = 'o1'")
	if got := f.walk(t, f.feed, "o1"); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("fresh walk:\n got %v\nwant %v", got, want)
	}
}

func TestFeedStatusTabsFollowStatusChanges(t *testing.T) {
	f := newFeedFixture(t, 5)
	ids := f.insert(t, "o1", "INCOMING", 12, false)
	f.walk(t, f.byStatus, "o1", "INCOMING")
	f.walk(t, f.byStatus, "o1", "COMPLETED") // empty, marked so

	// Complete the oldest and a middle one: they leave INCOMING and enter COMPLETED.
	f.exec(t, `UPDATE TBL SET name = 'COMPLETED' WHERE uuid = ANY($1)`, pq.Array([]string{ids[0], ids[6]}))
	for _, status := range []string{"INCOMING", "COMPLETED"} {
		want := f.sqlOrder(t, "user_uuid = 'o1' AND name = $1", status)
		if got := f.walk(t, f.byStatus, "o1", status); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s:\n got %v\nwant %v", status, got, want)
		}
	}
	// And back, after both tabs reached their end (lastpage): an old row re-enters a full index.
	f.exec(t, `UPDATE TBL SET name = 'INCOMING' WHERE uuid = $1`, ids[0])
	for _, status := range []string{"INCOMING", "COMPLETED"} {
		want := f.sqlOrder(t, "user_uuid = 'o1' AND name = $1", status)
		if got := f.walk(t, f.byStatus, "o1", status); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s after the way back:\n got %v\nwant %v", status, got, want)
		}
	}
	// Redis flushed in the middle of everything.
	f.mr.FlushAll()
	for _, status := range []string{"INCOMING", "COMPLETED"} {
		want := f.sqlOrder(t, "user_uuid = 'o1' AND name = $1", status)
		if got := f.walk(t, f.byStatus, "o1", status); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s after a flush:\n got %v\nwant %v", status, got, want)
		}
	}
}

func TestFeedCursorRoundTrip(t *testing.T) {
	for _, cursor := range []redifu.Cursor{{}, {Score: 1792168091754, Member: "9b6c-uuid"}, {Score: 0, Member: "a:b"}} {
		text := EncodeCursor(cursor)
		back, err := DecodeCursor(text)
		if err != nil || back != cursor {
			t.Fatalf("%v → %q → %v, %v", cursor, text, back, err)
		}
	}
	if _, err := DecodeCursor("not base64!"); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestFeedReadNServesEveryPageSizeFromOneIndex(t *testing.T) {
	f := newFeedFixture(t, 20)
	f.insert(t, "o1", "INCOMING", 37, true)
	want := f.sqlOrder(t, "user_uuid = 'o1'")
	walkN := func(n int64) []string {
		t.Helper()
		var seen []string
		var cursor redifu.Cursor
		for page := 0; page < 100; page++ {
			got, ok := f.feed.ReadN(f.ctx, []string{"o1"}, cursor, n)
			if !ok {
				t.Fatalf("n=%d page %d not served from the cache", n, page)
			}
			if int64(len(got.Items)) > n {
				t.Fatalf("n=%d: a page of %d", n, len(got.Items))
			}
			for _, item := range got.Items {
				seen = append(seen, item.UUID)
			}
			if !got.HasMore {
				return seen
			}
			cursor = got.Next
		}
		t.Fatalf("n=%d: the feed did not end", n)
		return nil
	}
	for _, n := range []int64{1, 5, 36, 37, 38, 50} {
		if got := walkN(n); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("n=%d:\n got %v\nwant %v", n, got, want)
		}
	}
	loads := f.loads.Load()
	for _, n := range []int64{2, 7, 50} {
		walkN(n)
	}
	if f.loads.Load() != loads {
		t.Fatalf("a full index read the database again (%d loads)", f.loads.Load()-loads)
	}
	// A first page larger than the head an empty index is seeded with.
	f.mr.FlushAll()
	first, ok := f.feed.ReadN(f.ctx, []string{"o1"}, redifu.Cursor{}, 30)
	if !ok || len(first.Items) != 30 || !first.HasMore {
		t.Fatalf("cold first page of 30: ok=%v n=%d more=%v", ok, len(first.Items), first != nil && first.HasMore)
	}
	for i, item := range first.Items {
		if item.UUID != want[i] {
			t.Fatalf("cold first page differs at %d", i)
		}
	}
}

// ReadN as a cursor-paged API uses it: every page that says more follow is full,
// walks by 3, 7, 50 and 100 equal the database, a warm walk by another size reads nothing from it,
// and a cold first page larger than the page size costs one head seed.
func TestFeedReadNPagesAreFullWhileMoreFollow(t *testing.T) {
	f := newFeedFixture(t, 20)
	f.insert(t, "o1", "INCOMING", 61, true)
	want := f.sqlOrder(t, "user_uuid = 'o1'")
	walkN := func(limit int64) []string {
		t.Helper()
		var seen []string
		var cursor redifu.Cursor
		for page := 0; page < 200; page++ {
			got, ok := f.feed.ReadN(f.ctx, []string{"o1"}, cursor, limit)
			if !ok {
				t.Fatalf("limit %d page %d not served", limit, page)
			}
			if int64(len(got.Items)) > limit {
				t.Fatalf("limit %d: page of %d", limit, len(got.Items))
			}
			if got.HasMore && int64(len(got.Items)) != limit {
				t.Fatalf("limit %d: a short page (%d) that says there is more", limit, len(got.Items))
			}
			for _, item := range got.Items {
				seen = append(seen, item.UUID)
			}
			if !got.HasMore {
				return seen
			}
			cursor = got.Next
		}
		t.Fatalf("limit %d: the feed did not end", limit)
		return nil
	}
	for _, limit := range []int64{3, 7, 50, 100} {
		if got := walkN(limit); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("limit %d:\n got %v\nwant %v", limit, got, want)
		}
	}
	loads := f.loads.Load()
	if got := walkN(11); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatal("walk by 11 differs")
	}
	if f.loads.Load() != loads {
		t.Fatalf("a warm walk by another page size read the database %d times", f.loads.Load()-loads)
	}
	// A first page larger than the page size on a cold index: one head seed answers it.
	f.mr.FlushAll()
	loads = f.loads.Load()
	page, ok := f.feed.ReadN(f.ctx, []string{"o1"}, redifu.Cursor{}, 40)
	if !ok || len(page.Items) != 40 || !page.HasMore {
		t.Fatalf("cold page of 40: ok=%v n=%d more=%v", ok, len(page.Items), page != nil && page.HasMore)
	}
	if f.loads.Load()-loads != 1 {
		t.Fatalf("cold page of 40 took %d loads, want one head seed", f.loads.Load()-loads)
	}
}
