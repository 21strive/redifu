package redifu

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// processor arguments arrive as the caller passed them
// ---------------------------------------------------------------------------

func TestPageProcessorReceivesTheArgumentsItWasGiven(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	page := mustPage[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)

	post := newPost(t, "one", time.Now())
	pipe := client.Pipeline()
	if err := base.WithPipeline(pipe).Set(ctx, post); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := page.IngestItem(ctx, pipe, post, 1, "u1"); err != nil {
		t.Fatalf("IngestItem: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	var got []interface{}
	_, err := page.Fetch(1).
		WithParams("u1").
		WithProcessor(func(_ **Post, args []interface{}) { got = args }, "viewer", 42).
		Exec(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("processor received %d args (%#v), want the 2 that were passed", len(got), got)
	}
	if got[0] != "viewer" || got[1] != 42 {
		t.Fatalf("processor received %#v, want [viewer 42]", got)
	}
}

func TestTimeSeriesProcessorReceivesTheArgumentsItWasGiven(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	series := mustTimeSeries[*Post](t, client, base, feedKeyFormat, indexTTL)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	post := newPost(t, "one", from.Add(time.Minute))

	pipe := client.Pipeline()
	if err := base.WithPipeline(pipe).Set(ctx, post); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := series.IngestItem(ctx, pipe, post, "u1"); err != nil {
		t.Fatalf("IngestItem: %v", err)
	}
	if err := series.AddSegment(ctx, pipe, from, to, "u1"); err != nil {
		t.Fatalf("AddSegment: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	var got []interface{}
	_, _, err := series.Fetch(from, to).
		WithParams("u1").
		WithProcessor(func(_ **Post, args []interface{}) { got = args }, "viewer", 42).
		Exec(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if len(got) != 2 || got[0] != "viewer" || got[1] != 42 {
		t.Fatalf("processor received %#v, want [viewer 42]", got)
	}
}

// ---------------------------------------------------------------------------
// key parameters
// ---------------------------------------------------------------------------

func TestFetchDoesNotWriteIntoTheCallersSlice(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	page := mustPage[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)

	// Spreading a variadic argument hands the callee this exact slice, spare capacity
	// and all. Appending to it in place would overwrite "keep me".
	backing := make([]string, 2, 8)
	backing[0] = "u1"
	backing[1] = "keep me"

	if _, err := page.Fetch(1).WithParams(backing[:1]...).Exec(ctx); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if backing[1] != "keep me" {
		t.Fatalf("the caller's slice was overwritten with %q", backing[1])
	}
}

func TestReusingAFetchBuilderDoesNotAccumulateParameters(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	page := mustPage[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)

	builder := page.Fetch(1).WithParams("u1")
	if _, err := builder.Exec(ctx); err != nil {
		t.Fatalf("first Exec: %v", err)
	}
	if _, err := builder.Exec(ctx); err != nil {
		t.Fatalf("second Exec: %v — the page number was appended twice", err)
	}
}

func TestKeyFormatsAreValidatedAtConstruction(t *testing.T) {
	_, client := newTestRedis(t)

	cases := map[string]string{
		"a verb redifu cannot fill":       "post:%d",
		"more parameters than Base takes": "post:%s:%s",
		"no parameters at all":            "post",
		"a hash tag of its own":           "{post}:%s",
	}

	for name, format := range cases {
		if _, err := NewBase[*Post](client, format, baseTTL); err == nil {
			t.Fatalf("NewBase accepted %s (%q)", name, format)
		}
	}
}

func TestKeyParametersAreValidated(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)

	if _, err := sorted.Count(ctx, ""); err == nil {
		t.Fatal("an empty key parameter was accepted — two collections would share one key")
	}
	if _, err := sorted.Count(ctx, "u{1}"); err == nil {
		t.Fatal("a key parameter containing a hash tag was accepted")
	}
	if _, err := sorted.Count(ctx, "u1", "extra"); err == nil {
		t.Fatal("too many key parameters were accepted")
	}
}

func TestSetWithAnEmptyParameterSliceDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	post := newPost(t, "one", time.Now())

	empty := []string{}
	if err := base.Set(ctx, post, empty...); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := base.Del(ctx, post, empty...); err != nil {
		t.Fatalf("Del: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Base
// ---------------------------------------------------------------------------

func TestMissingItemReportsErrNotFound(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)

	_, err := base.Get(ctx, "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
}

func TestExistsReportsWhetherTheItemIsThere(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	post := newPost(t, "one", time.Now())

	present, err := base.Exists(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if present {
		t.Fatal("Exists reported an item that was never written")
	}

	if err := base.Set(ctx, post); err != nil {
		t.Fatalf("Set: %v", err)
	}

	present, err = base.Exists(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !present {
		t.Fatal("Exists did not report an item that is there")
	}
}

func TestMarkAsMissingSurvivesAndIsClearedByAWrite(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	post := newPost(t, "one", time.Now())

	if err := base.MarkAsMissing(ctx, post.GetRandId()); err != nil {
		t.Fatalf("MarkAsMissing: %v", err)
	}

	missing, err := base.IsMissing(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("IsMissing: %v", err)
	}
	if !missing {
		t.Fatal("IsMissing did not see the mark")
	}

	if err := base.Set(ctx, post); err != nil {
		t.Fatalf("Set: %v", err)
	}

	missing, err = base.IsMissing(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("IsMissing: %v", err)
	}
	if missing {
		t.Fatal("writing the item did not clear the missing mark")
	}
}

// ---------------------------------------------------------------------------
// sorting reference
// ---------------------------------------------------------------------------

func TestSortingReferenceIsValidatedWhenItIsSet(t *testing.T) {
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)

	if err := sorted.SetSortingReference("UpdatedAtt"); err == nil {
		t.Fatal("a misspelled sorting reference was accepted")
	} else if !strings.Contains(err.Error(), "UpdatedAtt") {
		t.Fatalf("the error should name the field, got: %v", err)
	}

	if err := sorted.SetSortingReference("Title"); err == nil {
		t.Fatal("a string field was accepted as a sorting reference")
	}

	if err := sorted.SetSortingReference("UpdatedAt"); err != nil {
		t.Fatalf("SetSortingReference(UpdatedAt): %v", err)
	}
}

func TestSortingByAnAlternativeField(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	sorted := mustSorted[*Post](t, client, base, feedKeyFormat, indexTTL)
	if err := sorted.SetSortingReference("UpdatedAt"); err != nil {
		t.Fatalf("SetSortingReference: %v", err)
	}

	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	older := newPost(t, "edited first", created)
	older.SetUpdatedAt(created.Add(time.Hour))
	newer := newPost(t, "edited last", created.Add(time.Minute))
	newer.SetUpdatedAt(created.Add(2 * time.Hour))

	seedSorted(t, client, base, sorted, "u1", older, newer)

	items, err := sorted.Fetch(Descending).WithParams("u1").Exec(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if items[0].Title != "edited last" {
		t.Fatalf("first item = %q, want the most recently updated", items[0].Title)
	}
}

func TestAnInt64ScoreBeyondExactPrecisionIsRefused(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Doc](t, client, "doc:%s", baseTTL)
	sorted, err := NewSorted[*Doc](client, base, "docs:%s", indexTTL)
	if err != nil {
		t.Fatalf("NewSorted: %v", err)
	}
	if errRef := sorted.SetSortingReference("Ranking"); errRef != nil {
		t.Fatalf("SetSortingReference: %v", errRef)
	}

	doc := newDoc(t, "snowflake", time.Now())
	doc.Ranking = 1 << 60 // a snowflake id; float64 cannot hold this exactly

	pipe := client.Pipeline()
	errIngest := sorted.IngestItem(ctx, pipe, doc, true, "u1")
	if !errors.Is(errIngest, ErrScoreOutOfRange) {
		t.Fatalf("IngestItem = %v, want ErrScoreOutOfRange rather than a silently reordered index", errIngest)
	}
}

// ---------------------------------------------------------------------------
// Page gained the write side it never had
// ---------------------------------------------------------------------------

func TestPageAddAndRemoveItem(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	page := mustPage[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)

	seeded := newPost(t, "seeded", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	pipe := client.Pipeline()
	if err := base.WithPipeline(pipe).Set(ctx, seeded); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := page.IngestItem(ctx, pipe, seeded, 1, "u1"); err != nil {
		t.Fatalf("IngestItem: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	fresh := newPost(t, "fresh", time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	if err := page.AddItem(ctx, fresh, 1, "u1"); err != nil {
		t.Fatalf("AddItem: %v", err)
	}

	count, err := page.Count(ctx, 1, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("page holds %d items, want 2", count)
	}

	if err := page.RemoveItem(ctx, fresh, 1, "u1"); err != nil {
		t.Fatalf("RemoveItem: %v", err)
	}

	count, err = page.Count(ctx, 1, "u1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("page holds %d items after a remove, want 1", count)
	}
}

func TestPageAddItemOnAnUnseededPageReportsItself(t *testing.T) {
	ctx := context.Background()
	_, client := newTestRedis(t)

	base := mustBase[*Post](t, client, postKeyFormat, baseTTL)
	page := mustPage[*Post](t, client, base, feedKeyFormat, 10, Descending, indexTTL)

	post := newPost(t, "fresh", time.Now())
	if err := page.AddItem(ctx, post, 1, "u1"); !errors.Is(err, ErrNotIngested) {
		t.Fatalf("AddItem = %v, want ErrNotIngested", err)
	}

	stored, err := base.Get(ctx, post.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Title != "fresh" {
		t.Fatal("the item was not stored even though it was not indexed")
	}
}
