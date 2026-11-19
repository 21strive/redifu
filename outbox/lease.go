package outbox

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// The cache's lease: one build at a time uses the cache — the newest build that has a process
// alive. Every other build answers every read from SQL and leaves the queue alone.
//
// Generations alone keep two builds from reading each other's values and indexes, but not from
// missing each other's changes. A build that drains a change applies its own rules to it: an index
// or satellite the newer build added, or one whose members or score the newer build decides
// differently, is not touched by the older build's drain, and the newer build would go on serving
// it as it was until its TTL. With the lease the older build stops draining the moment the newer
// one takes over, so every change from then on is applied by the newer build's rules; and the
// older build's indexes, which the newer build's drains do not keep, are not read again either.
//
// The lease is two keys under one hash tag. {redifu:lease}:build names the holder,
// "<build time>|<generation>|<epoch>"; {redifu:lease}:alive is renewed by that build's processes at
// every check and expires leaseTTL after the last one is gone. A process takes the lease when its
// build is newer (its build time, then its generation byte by byte) or when the holder is gone.
//
// The epoch is part of every generation the caches carry (withEpoch). A build that takes the lease
// from an older one keeps it: the older build's drains kept every value the two builds share, so
// those stay warm. Any other takeover — the newer build is gone and an older one takes over (a
// rollback, a crash-looping rollout), or nobody held it (first use, a flushed Redis) — starts a new
// epoch, and with it a cold cache: what any build wrote before was kept by rules nobody applied
// since, and a build coming back to the lease must not find its own earlier state and serve it.
//
// A drain writes Redis in a MULTI/EXEC that WATCHes {redifu:lease}:build (fence.go): a takeover
// between the drain's check and its write aborts the write and rolls the drain back, and the holder
// applies those changes. A seed is not fenced: a build that no longer holds the lease stops seeding
// within leaseCheck (its reads go to SQL), and what it seeded carries its own generations, which the
// holder either shares — kept by the holder's drains from then on — or does not read.

const (
	leaseBuildKey = "{redifu:lease}:build"
	leaseAliveKey = "{redifu:lease}:alive"
	// leaseTTL is how long the lease outlives the last process of its build.
	leaseTTL = 30 * time.Second
)

// leaseCheck is how long a process trusts its last check: a build that lost the lease reads its
// own state for at most this long before every read goes to SQL (its drains are fenced at once).
var leaseCheck = 250 * time.Millisecond

// leaseObserveOnly: this process never takes or renews the lease (ObserveLease).
var leaseObserveOnly atomic.Bool

// ObserveLease makes this process a bystander of the lease: it uses the cache only while its own
// build holds it, and never takes it over. main calls it for a one-off command, so that a command run
// from a newer build (a backfill started from a workstation before the deploy) cannot take the cache
// away from the processes that serve traffic.
func ObserveLease() { leaseObserveOnly.Store(true) }

// leaseHolderScript returns the lease's value when this build holds it and a process of it is
// alive, changing nothing; nil otherwise.
//
//	KEYS[1] {redifu:lease}:build   KEYS[2] {redifu:lease}:alive   ARGV[1] the build ("<order>|<generation>")
var leaseHolderScript = redis.NewScript(`
local held = redis.call('GET', KEYS[1])
if held and string.sub(held, 1, #ARGV[1] + 1) == ARGV[1] .. '|' and redis.call('EXISTS', KEYS[2]) == 1 then
  return held
end
return false
`)

// errLeaseLost: the drain's build no longer holds the lease (the holder applies the queue).
// errLeaseMissing: nobody does (the keys were flushed); the drain takes it again.
var (
	errLeaseLost    = errors.New("redifucache: another build holds the cache lease")
	errLeaseMissing = errors.New("redifucache: the cache lease is gone")
)

var warnBuildTime sync.Once

// BuildOrder is the build order of a build that WithBuild gives none: the modification time of the
// running binary in Unix seconds. Of two builds the one built later takes the lease, so a build that
// stamps when it was built (a BUILD_TIME) passes that to WithBuild instead.
func BuildOrder() int64 {
	return executableTime()
}

// orderOf is the build order WithBuild gave, or BuildOrder for a negative one, logged once.
func (s *Store) orderOf(given int64) int64 {
	if given >= 0 {
		return given
	}
	order := BuildOrder()
	warnBuildTime.Do(func() {
		s.log().Warn("redifu: this binary has no usable build time (BUILD_TIME), the lease orders it by the binary's modification time",
			"build_time", given, "order", order)
	})
	return order
}

// executableTime is the modification time of the running binary in Unix seconds, 0 when it cannot
// be read.
func executableTime() int64 {
	path, err := os.Executable()
	if err != nil {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.ModTime().Unix()
}

// buildLease is this process's view of the lease.
type buildLease struct {
	build      string // the lease's name for this build: "<order>|<generation>"
	order      int64
	generation string

	mu        sync.Mutex
	checkedAt atomic.Int64 // UnixNano of the last check
	held      atomic.Bool
	value     atomic.Pointer[string] // the lease's value when this build last held it: "<build>|<epoch>"
	logged    atomic.Int32           // 1 held, 2 lost: what was logged last
}

func newBuildLease(generation string, order int64) *buildLease {
	return &buildLease{build: strconv.FormatInt(order, 10) + "|" + generation, order: order, generation: generation}
}

// holder is the lease's value this process last held it under, "" before it ever did. A drain's
// fence compares the lease with it: a new epoch is a takeover too.
func (l *buildLease) holder() string {
	if value := l.value.Load(); value != nil {
		return *value
	}
	return ""
}

// leaseClaimScript renews the lease when this build holds it, takes it when this build is newer than
// the holder (keeping the holder's epoch) or the holder is gone (starting a new epoch), and returns
// the lease's value when this build holds it now, nil otherwise.
//
//	KEYS[1] {redifu:lease}:build   KEYS[2] {redifu:lease}:alive
//	ARGV[1] this build ("<order>|<generation>")   ARGV[2] TTL in ms   ARGV[3] order   ARGV[4] generation
//	ARGV[5] the epoch a takeover that starts one writes
var leaseClaimScript = redis.NewScript(`
local function less(a, b)
  local la, lb = #a, #b
  for i = 1, math.min(la, lb) do
    local x, y = string.byte(a, i), string.byte(b, i)
    if x ~= y then
      return x < y
    end
  end
  return la < lb
end

local mine = ARGV[1]
local held = redis.call('GET', KEYS[1])
if held and string.sub(held, 1, #mine + 1) == mine .. '|' then
  -- This build holds it. Its alive key may have lapsed (no process of it checked for a while):
  -- nobody else took it meanwhile, so nobody else drained, and the epoch stays.
  redis.call('SET', KEYS[2], mine, 'PX', ARGV[2])
  return held
end
local value = false
if not held then
  value = mine .. '|' .. ARGV[5]
else
  local sep = string.find(held, '|', 1, true)
  local order = sep and tonumber(string.sub(held, 1, sep - 1)) or -1
  local rest = sep and string.sub(held, sep + 1) or ''
  local last = string.find(rest, '|[^|]*$')
  local generation = last and string.sub(rest, 1, last - 1) or rest
  local epoch = last and string.sub(rest, last + 1) or ''
  local myOrder = tonumber(ARGV[3])
  if myOrder > order or (myOrder == order and less(generation, ARGV[4])) then
    -- An upgrade: the older build's drains kept everything the two share.
    if epoch == '' then
      epoch = ARGV[5]
    end
    value = mine .. '|' .. epoch
  elseif redis.call('EXISTS', KEYS[2]) == 0 then
    -- The newer build is gone and this older one takes over: nothing it wrote before is trusted.
    value = mine .. '|' .. ARGV[5]
  end
end
if not value then
  return false
end
redis.call('SET', KEYS[1], value)
redis.call('SET', KEYS[2], mine, 'PX', ARGV[2])
return value
`)

// newEpoch is the epoch a takeover that starts one writes: unique to it (nanoseconds, base 36).
func newEpoch() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

// holdsLease reports whether this build holds the lease, checking Redis at most every leaseCheck
// (the check renews the lease, or takes it). A Redis error answers no: the read goes to SQL.
func (s *Store) holdsLease(ctx context.Context) bool {
	s.settleBuild()
	l := s.lease
	if l == nil {
		return true // a store built for a test without the lease
	}
	if time.Now().UnixNano()-l.checkedAt.Load() < int64(leaseCheck) {
		return l.held.Load()
	}
	if !l.mu.TryLock() {
		return l.held.Load() // another goroutine is checking right now
	}
	defer l.mu.Unlock()
	if time.Now().UnixNano()-l.checkedAt.Load() < int64(leaseCheck) {
		return l.held.Load()
	}
	keys := []string{leaseBuildKey, leaseAliveKey}
	var value string
	var err error
	if leaseObserveOnly.Load() {
		value, err = leaseHolderScript.Run(ctx, s.rdb, keys, l.build).Text()
	} else {
		value, err = leaseClaimScript.Run(ctx, s.rdb, keys, l.build, leaseTTL.Milliseconds(), l.order, l.generation, newEpoch()).Text()
	}
	held := err == nil
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if err != nil {
		s.RedisFailed(err)
		l.held.Store(false)
		l.checkedAt.Store(time.Now().UnixNano())
		return false
	}
	if held && !s.adoptLease(value) {
		held = false
	}
	s.leaseIs(held)
	l.checkedAt.Store(time.Now().UnixNano())
	return held
}

// adoptLease records the lease's value this build holds it under and moves every cache of the
// store to its epoch, before any read is let through on it. False: the caches could not move, and
// this process must not use them.
func (s *Store) adoptLease(value string) bool {
	l := s.lease
	if l.holder() == value {
		return true
	}
	epoch := ""
	if len(value) > len(l.build)+1 {
		epoch = value[len(l.build)+1:]
	}
	if epoch != s.Epoch() {
		l.held.Store(false) // nothing reads the caches while they move
		if err := s.setEpoch(epoch); err != nil {
			s.log().Error("redifu: cannot move the caches to the lease's epoch", "epoch", epoch, "error", err.Error())
			return false
		}
		s.log().Info("redifu: cache epoch", "epoch", epoch, "build", l.build)
	}
	l.value.Store(&value)
	return true
}

// leaseIs records (and logs, on a change) whether this build holds the lease.
func (s *Store) leaseIs(held bool) {
	l := s.lease
	if l == nil {
		return
	}
	l.held.Store(held)
	state := int32(2)
	if held {
		state = 1
	}
	if l.logged.Swap(state) == state {
		return
	}
	if held {
		s.log().Info("redifu: this build holds the cache lease", "build", l.holder())
	} else {
		s.log().Warn("redifu: another build holds the cache lease, this process reads SQL", "build", l.build)
	}
}

// reclaimLease checks the lease now, whatever the last check said, and reports whether this build
// holds it.
func (s *Store) reclaimLease(ctx context.Context) bool {
	if s.lease == nil {
		return true
	}
	s.lease.checkedAt.Store(0)
	return s.holdsLease(ctx)
}

// leaseLost forgets a held lease at once (a fenced drain found another holder, or another epoch).
func (s *Store) leaseLost() {
	if s.lease == nil {
		return
	}
	s.lease.checkedAt.Store(time.Now().UnixNano())
	s.leaseIs(false)
}
