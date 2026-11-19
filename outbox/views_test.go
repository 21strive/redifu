package outbox

// The reads the other tests leave alone: a page of a List by rank, a read for a caller that has
// applied the owner's queue itself, and the counts and months of Buckets. Same fixture as
// store_test.go (REDIFU_TEST_DSN; skips without it, except the month arithmetic).

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

func TestListReadPageAndReadServed(t *testing.T) {
	f := newUnitFixture(t)
	f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ('a','o1','A',1), ('b','o1','B',2), ('c','o1','C',3), ('d','o1','D',4), ('e','o1','E',5)`)
	page := func(start, stop int64) string {
		t.Helper()
		items, total, ok := f.list.ReadPage(f.ctx, start, stop, "o1")
		if !ok {
			t.Fatalf("page %d..%d not served", start, stop)
		}
		return fmt.Sprintf("%s/%d", names(items), total)
	}
	for _, c := range []struct {
		start, stop int64
		want        string
	}{
		{0, 1, "A,B/5"}, {1, 2, "B,C/5"}, {3, 9, "D,E/5"}, {9, 12, "/5"},
	} {
		if got := page(c.start, c.stop); got != c.want {
			t.Fatalf("page %d..%d = %q, want %q", c.start, c.stop, got, c.want)
		}
	}
	if _, _, ok := f.list.ReadPage(f.ctx, 2, 1, "o1"); ok {
		t.Fatal("a page that stops before it starts was served")
	}

	f.exec(t, `UPDATE TBL SET name = 'B2' WHERE uuid = 'b'`)
	if got := page(1, 1); got != "B2/5" {
		t.Fatalf("page after a rename = %q", got)
	}
	// ReadServed trusts the caller's Serve of the same call.
	f.exec(t, `DELETE FROM TBL WHERE uuid = 'e'`)
	if !f.store.Serve(f.ctx, "o1", f.table) {
		t.Fatal("not served")
	}
	if items, ok := f.list.ReadServed(f.ctx, "o1"); !ok || names(items) != "A,B2,C,D" {
		t.Fatalf("ReadServed = %q, %v", names(items), ok)
	}
}

func TestBucketsCountWhatAReadWouldDecode(t *testing.T) {
	f := newUnitFixture(t)
	march := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).Unix()
	april := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC).Unix()
	buckets, err := NewBuckets[*unitEntity](f.store, f.base, "unit:month:%s:%s", []string{f.table},
		func(ctx context.Context, tx *sqlx.Tx, owner string, from, to int64) ([]*unitEntity, error) {
			return loadUnitItems(ctx, tx, `SELECT `+unitColumns+` FROM `+f.table+
				` WHERE user_uuid = $1 AND sort >= $2 AND sort < $3 ORDER BY sort, uuid COLLATE "C"`, owner, from, to)
		})
	if err != nil {
		t.Fatal(err)
	}
	for i, at := range []int64{march + 86400, march + 2*86400, april - 1, april, april + 86400} {
		f.exec(t, `INSERT INTO TBL (uuid, user_uuid, name, sort) VALUES ($1, 'o1', $2, $3)`, fmt.Sprint("m", i), fmt.Sprint("M", i), at)
	}
	count := func(from, to int64) int64 {
		t.Helper()
		n, ok := buckets.CountBetween(f.ctx, "o1", from, to)
		if !ok {
			t.Fatalf("count %d..%d not served", from, to)
		}
		return n
	}
	if n := count(march, april+31*86400); n != 0 {
		t.Fatalf("cold months counted %d", n)
	}
	items, ok := buckets.Read(f.ctx, "o1", march, april+29*86400)
	if !ok || names(items) != "M0,M1,M2,M3,M4" {
		t.Fatalf("read of March and April = %q, %v", names(items), ok)
	}
	if n := count(march+2*86400, april); n != 3 {
		t.Fatalf("count across the month boundary = %d, want 3", n)
	}
	if items, ok := buckets.Read(f.ctx, "o1", april, april); !ok || names(items) != "M3" {
		t.Fatalf("read of one second = %q, %v", names(items), ok)
	}
	if _, ok := buckets.CountBetween(f.ctx, "o1", march, march+int64(MaxBucketMonths)*31*86400); ok {
		t.Fatal("a range longer than a read serves was counted")
	}
}

func TestMonthsBetween(t *testing.T) {
	at := func(year int, month time.Month, day int) int64 {
		return time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix()
	}
	for _, c := range []struct {
		from, to int64
		want     string
	}{
		{at(2026, 3, 5), at(2026, 3, 9), "[2026-03]"},
		{at(2025, 12, 31), at(2026, 2, 1), "[2025-12 2026-01 2026-02]"},
		{at(2026, 3, 9), at(2026, 3, 5), "[]"},
	} {
		if got := fmt.Sprint(MonthsBetween(c.from, c.to)); got != c.want {
			t.Errorf("MonthsBetween = %s, want %s", got, c.want)
		}
	}
	if Month(at(2026, 4, 1)-1) != "2026-03" {
		t.Error("the last second of March is not in March")
	}
}
