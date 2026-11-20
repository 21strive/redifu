package outbox

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/21strive/redifu"
)

// Generations. A key names its data and nothing else — post:<randid>, {post:list:<owner>} — and
// redifu keeps which build wrote a value or seeded an index in the stored state, comparing it on
// every read and write (redifu SetGeneration). Two builds sharing one Redis, as during a rolling
// deploy, never read each other's state: what one wrote is a miss for the other, which reloads it
// from SQL, and a write of one into an index of the other drops that index.
//
//   - A row entity — one row of one SELECT, as its model and the loader's SQL make it — takes a
//     generation that fingerprints the Go type the value is encoded from and that SQL (the
//     application computes it). Builds that store the same thing share their values, so the cache
//     stays warm across a deploy that does not change them, and reloads them on the first one that
//     does.
//   - An index (List, Feed, Buckets, a view's own Sorted), a satellite (data computed in Go from
//     several reads), a handle and a computed record take the build (Store.BuildGeneration): what
//     they hold is decided by code — a WHERE, a score, a sum in Go — that no type shows.
//
// Every Base and index of the cache is built through NewBase, NewSorted and NewTimeline below:
// they set the generation and register the key format with the store, which refuses one format
// under two generations and lists them all for Purge (Formats).
//
// Every generation also carries the lease's epoch (lease.go, withEpoch): a takeover that starts a
// new epoch turns the whole cache cold at once, values and indexes alike.

// devGeneration is the build generation of a binary built without a revision whose own file cannot
// be read either: every such binary is the same build as far as the cache can tell.
const devGeneration = "dev"

var (
	buildGenerationOnce sync.Once
	buildGeneration     string
	warnBuildRevision   sync.Once
)

// BuildGeneration is the generation of a build that WithBuild gives none: a fingerprint of the
// running binary's own file, "bin-<hex>", computed once — so that two different builds never share
// indexes or computed records, as "dev" builds did. A build that stamps the commit it was built from
// (a GIT_SHA) passes that to WithBuild instead.
func BuildGeneration() string {
	buildGenerationOnce.Do(func() {
		buildGeneration = executableGeneration()
	})
	return buildGeneration
}

// generationOf is the build generation WithBuild gave, or BuildGeneration for none, logged once.
func (s *Store) generationOf(given string) string {
	if given != "" {
		return given
	}
	generation := BuildGeneration()
	warnBuildRevision.Do(func() {
		s.log().Warn("redifu: this binary has no build revision (GIT_SHA): its indexes and computed records use a fingerprint of the binary",
			"generation", generation)
	})
	return generation
}

// executableGeneration fingerprints the running binary, "dev" when it cannot be read.
func executableGeneration() string {
	path, err := os.Executable()
	if err != nil {
		return devGeneration
	}
	file, err := os.Open(path)
	if err != nil {
		return devGeneration
	}
	defer func() { _ = file.Close() }()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return devGeneration
	}
	return "bin-" + hex.EncodeToString(sum.Sum(nil)[:8])
}

// withEpoch is the generation a cache carries: its own generation and the lease's epoch.
func withEpoch(generation, epoch string) string {
	if epoch == "" {
		return generation
	}
	return generation + "@" + epoch
}

// generationHolder is a Base or index of the store, with the generation it was built under.
type generationHolder struct {
	generation string
	set        func(string) error
}

// Epoch is the lease's epoch the store's caches carry, "" before this process held the lease.
func (s *Store) Epoch() string {
	if epoch := s.epoch.Load(); epoch != nil {
		return *epoch
	}
	return ""
}

// setEpoch moves every cache of the store to epoch.
func (s *Store) setEpoch(epoch string) error {
	s.formatsMu.Lock()
	defer s.formatsMu.Unlock()
	for _, cache := range s.caches {
		if err := cache.set(withEpoch(cache.generation, epoch)); err != nil {
			return err
		}
	}
	s.epoch.Store(&epoch)
	return nil
}

// KeyFormat is one key format of the cache as NewBase, NewSorted or NewTimeline registered it.
type KeyFormat struct {
	Format string
	// Index: a redifu collection, stored under a cluster hash tag ({format}) with its markers.
	Index      bool
	Generation string
}

// register records a key format and the cache built on it, and gives the cache its generation with
// the store's epoch. One format under two generations would make the Bases sharing it read each
// other's values as misses forever: refused at startup.
func (s *Store) register(format string, index bool, generation string, set func(string) error) error {
	s.formatsMu.Lock()
	defer s.formatsMu.Unlock()
	if known, ok := s.formats[format]; ok && (known.Index != index || known.Generation != generation) {
		return fmt.Errorf("redifucache: key format %q is registered twice, with generation %q and %q", format, known.Generation, generation)
	}
	if err := set(withEpoch(generation, s.Epoch())); err != nil {
		return err
	}
	s.formats[format] = KeyFormat{Format: format, Index: index, Generation: generation}
	s.caches = append(s.caches, generationHolder{generation: generation, set: set})
	return nil
}

// Formats lists the registered key formats, sorted.
func (s *Store) Formats() []KeyFormat {
	s.formatsMu.Lock()
	defer s.formatsMu.Unlock()
	out := make([]KeyFormat, 0, len(s.formats))
	for _, format := range s.formats {
		out = append(out, format)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Format < out[j].Format })
	return out
}

// BuildGeneration is the generation of the store's indexes and computed records.
func (s *Store) BuildGeneration() string {
	s.settleBuild()
	return s.build
}

// WithBuild sets the store's build generation and build order, before any cache is built on it:
// what the build is (the commit it was built from) and when it was built (Unix seconds), so that of
// two builds sharing one Redis the later one takes the cache (lease.go). Tests use it to stand two
// builds side by side. A generation "" falls back to BuildGeneration (a fingerprint of the binary)
// and a negative order to BuildOrder (its modification time), each logged once; a store never given
// a build takes both.
func (s *Store) WithBuild(generation string, order int64) *Store {
	s.build = generation
	s.order = order
	if s.settled { // the build was needed already: this one replaces it at once, lease and all
		s.build = s.generationOf(generation)
		s.order = s.orderOf(order)
		s.lease = newBuildLease(s.build, s.order)
	}
	return s
}

// settleBuild settles the store's build the first time it is needed — the generation and order
// WithBuild gave, or their fallbacks — and with it the store's lease.
func (s *Store) settleBuild() {
	s.buildOnce.Do(func() {
		s.build = s.generationOf(s.build)
		s.order = s.orderOf(s.order)
		s.lease = newBuildLease(s.build, s.order)
		s.settled = true
	})
}

// NewBase builds the Base of an entity under generation (for a row entity a fingerprint of its
// type and its SQL, which the application computes; the store's BuildGeneration otherwise) and
// registers its key format.
func NewBase[E redifu.Blueprint](store *Store, keyFormat string, timeToLive time.Duration, generation string) (*redifu.Base[E], error) {
	if generation == "" {
		return nil, fmt.Errorf("redifucache: %s needs a generation", keyFormat)
	}
	base, err := redifu.NewBase[E](store.rdb, keyFormat, timeToLive)
	if err != nil {
		return nil, err
	}
	if err := store.register(keyFormat, false, generation, base.SetGeneration); err != nil {
		return nil, err
	}
	return base, nil
}

// NewSorted builds a Sorted index of the store's build generation and registers its key format.
func NewSorted[E redifu.Blueprint](store *Store, base *redifu.Base[E], keyFormat string, timeToLive time.Duration) (*redifu.Sorted[E], error) {
	sorted, err := redifu.NewSorted[E](store.rdb, base, keyFormat, timeToLive)
	if err != nil {
		return nil, err
	}
	if err := store.register(keyFormat, true, store.BuildGeneration(), sorted.SetGeneration); err != nil {
		return nil, err
	}
	return sorted, nil
}

// NewTimeline builds a Timeline index of the store's build generation and registers its key format.
func NewTimeline[E redifu.Blueprint](store *Store, base *redifu.Base[E], keyFormat string, perPage int64, direction string, timeToLive time.Duration) (*redifu.Timeline[E], error) {
	timeline, err := redifu.NewTimeline[E](store.rdb, base, keyFormat, perPage, direction, timeToLive)
	if err != nil {
		return nil, err
	}
	if err := store.register(keyFormat, true, store.BuildGeneration(), timeline.SetGeneration); err != nil {
		return nil, err
	}
	return timeline, nil
}
