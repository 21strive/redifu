package outbox

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// capturedLog is a slog handler that keeps every record.
type capturedLog struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capturedLog) Enabled(context.Context, slog.Level) bool { return true }
func (c *capturedLog) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *capturedLog) WithGroup(string) slog.Handler            { return c }
func (c *capturedLog) Handle(_ context.Context, record slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, record.Clone())
	return nil
}

func (c *capturedLog) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.records))
	for _, record := range c.records {
		out = append(out, record.Message)
	}
	return out
}

// attrs are the attributes of the first record with message, as text.
func (c *capturedLog) attrs(message string) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range c.records {
		if record.Message != message {
			continue
		}
		out := map[string]string{}
		record.Attrs(func(attr slog.Attr) bool {
			out[attr.Key] = attr.Value.String()
			return true
		})
		return out
	}
	return nil
}

func (c *capturedLog) waitFor(t *testing.T, message string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if c.attrs(message) != nil {
			return
		}
	}
	t.Fatalf("never logged %q; logged %q", message, c.messages())
}

// The store writes to the logger it is given, not to the default one.
func TestWithLoggerTakesTheStoresLines(t *testing.T) {
	logged := &capturedLog{}
	store := NewStore(nil, nil, true).WithLogger(slog.New(logged))
	store.RedisFailed(errors.New("LOADING"))
	attrs := logged.attrs("redifu: redis failed, reads go to SQL for a while")
	if attrs == nil || attrs["error"] != "LOADING" || attrs["pause"] != "5s" {
		t.Fatalf("logged %q, %v", logged.messages(), attrs)
	}
	if !store.redisDown() {
		t.Fatal("a Redis failure did not pause the reads")
	}
}

// A pass of the drain loop that panics — here a store without a database, whose first pass reaches
// for it — ends neither the loop nor the process: the default recovers and logs it with its stack,
// and WithRecover hands every pass to the application's own function.
func TestTheDrainLoopSurvivesAPanickingPass(t *testing.T) {
	run := func(store *Store) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { store.RunDrainLoop(ctx, time.Millisecond); close(done) }()
		t.Cleanup(func() { cancel(); <-done })
	}

	logged := &capturedLog{}
	run(NewStore(nil, nil, true).WithLogger(slog.New(logged)))
	logged.waitFor(t, "background pass panicked")
	attrs := logged.attrs("background pass panicked")
	if !strings.HasPrefix(attrs["error"], "redifu_drain: pass panicked: ") || attrs["loop"] != "redifu_drain" ||
		!strings.Contains(attrs["stack"], "RunDrainLoop") {
		t.Fatalf("the panic was logged as %v", attrs)
	}

	var mu sync.Mutex
	var loops []string
	var recovered []any
	wrapped := make(chan struct{}, 1)
	run(NewStore(nil, nil, true).WithLogger(slog.New(&capturedLog{})).WithRecover(func(loop string, pass func()) bool {
		defer func() {
			if r := recover(); r != nil {
				mu.Lock()
				loops, recovered = append(loops, loop), append(recovered, r)
				mu.Unlock()
				select {
				case wrapped <- struct{}{}:
				default:
				}
			}
		}()
		pass()
		return true
	}))
	select {
	case <-wrapped:
	case <-time.After(5 * time.Second):
		t.Fatal("the application's recover never saw the panicking pass")
	}
	mu.Lock()
	defer mu.Unlock()
	if loops[0] != "redifu_drain" || recovered[0] == nil {
		t.Fatalf("the application's recover ran %v and recovered %v", loops, recovered)
	}
}

// A store not given a build fingerprints the binary and orders it by its modification time, and
// says so once to the store's logger — the logger it has once the build is first needed, so the
// order of the chained calls does not matter. A store given a build logs nothing.
func TestWithBuildFallsBackToTheBinaryOnce(t *testing.T) {
	warnBuildRevision, warnBuildTime = sync.Once{}, sync.Once{}
	logged := &capturedLog{}
	store := NewStore(nil, nil, true).WithBuild("", -1).WithLogger(slog.New(logged))
	if got := store.BuildGeneration(); got != BuildGeneration() || !strings.HasPrefix(got, "bin-") {
		t.Fatalf("generation without a revision = %q", got)
	}
	if store.lease.order != BuildOrder() || store.lease.build != strconv.FormatInt(BuildOrder(), 10)+"|"+BuildGeneration() {
		t.Fatalf("lease of a build without a build time: %q (order %d)", store.lease.build, store.lease.order)
	}
	want := []string{
		"redifu: this binary has no build revision (GIT_SHA): its indexes and computed records use a fingerprint of the binary",
		"redifu: this binary has no usable build time (BUILD_TIME), the lease orders it by the binary's modification time",
	}
	if got := logged.messages(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("logged %q", got)
	}
	again := NewStore(nil, nil, true).WithLogger(slog.New(logged))
	again.BuildGeneration()
	if n := len(logged.messages()); n != 2 {
		t.Fatalf("%d lines after a second store without a build: the fallbacks log once", n)
	}

	quiet := &capturedLog{}
	given := NewStore(nil, nil, true).WithLogger(slog.New(quiet)).WithBuild("4b5e8d8", 1763500000)
	if given.BuildGeneration() != "4b5e8d8" || given.lease.build != "1763500000|4b5e8d8" || len(quiet.messages()) != 0 {
		t.Fatalf("a store given its build: %q, lease %q, logged %q", given.BuildGeneration(), given.lease.build, quiet.messages())
	}
	// A build given after the first one was needed replaces it at once, lease and all.
	given.WithBuild("61be11b", 1763600000)
	if given.BuildGeneration() != "61be11b" || given.lease.build != "1763600000|61be11b" {
		t.Fatalf("a second WithBuild: %q, lease %q", given.BuildGeneration(), given.lease.build)
	}
}

// The legacy version is one key segment however it is written, a raw format needs a version, and a
// version that is no regular expression fails the purge instead of deleting anything.
func TestWithLegacyFormats(t *testing.T) {
	formats := []KeyFormat{{Format: "post:%s", Generation: "s1"}}
	if n := len(purgeMatchers(formats, "", []string{"post_search_gen:%s"})); n != 1 {
		t.Fatalf("%d matchers without a legacy version, want the registered format alone", n)
	}
	matchers := purgeMatchers(formats, `v1|v2`, nil)
	for key, want := range map[string]string{
		"post:abc":    "post:%s",
		"post:v1:abc": "legacy post:%s",
		"post:v2:abc": "legacy post:%s",
		"post:v1":     "post:%s", // not a legacy key: no parameter after the version
		"v2:abc":      "",
		"post:v3:abc": "",
	} {
		if got := match(matchers, key); got != want {
			t.Errorf("%q matched %q, want %q", key, got, want)
		}
	}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	_ = mr.Set("post:v1:abc", "x")
	store := NewStore(nil, rdb, true).WithLegacyFormats(`v[0-9`)
	if _, err := store.Purge(context.Background(), rdb, false); err == nil || !strings.Contains(err.Error(), "legacy version") {
		t.Fatalf("purge with a broken legacy version: %v", err)
	}
	if !mr.Exists("post:v1:abc") {
		t.Fatal("a failed purge deleted a key")
	}
}
