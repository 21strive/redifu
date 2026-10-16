package redifu

import "github.com/redis/go-redis/v9"

// The ingest scripts below replace a read-decide-write sequence that used to cost one
// round-trip per read. Two things follow from moving it into Redis:
//
//   - A fan-out write (one post into ten thousand follower timelines) went from five
//     round-trips per timeline to one enqueued command per timeline.
//   - The decision and the write are now atomic. Two writers can no longer both read
//     "this page holds itemPerPage items" and both add, pushing the page over its size.
//
// Every key a script touches must live in one Redis Cluster slot, which is why the
// collection key is wrapped in a hash tag and the markers hang off it.
//
// The scripts are sent with EVAL rather than EVALSHA. A pipelined EVALSHA cannot fall
// back to EVAL when the script is not cached — the NOSCRIPT error only surfaces at Exec,
// by which time the batch is gone — and correctness of a caller-owned pipeline is worth
// more than the few hundred bytes of script body.

// sortedIngestScript places an item into a Sorted index.
//
//	KEYS[1] sorted set   KEYS[2] :blankpage
//	ARGV[1] score        ARGV[2] member
//
// Returns 1 if the item entered the index, 0 if the collection is not seeded yet.
var sortedIngestScript = redis.NewScript(`
redis.call('DEL', KEYS[2])
if redis.call('ZCARD', KEYS[1]) > 0 then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
  return 1
end
return 0
`)

// timelineIngestScript places an item into a Timeline index, honouring the window the
// index currently holds.
//
//	KEYS[1] sorted set   KEYS[2] :firstpage   KEYS[3] :lastpage   KEYS[4] :blankpage
//	ARGV[1] score        ARGV[2] member       ARGV[3] itemPerPage ARGV[4] direction
//
// A Timeline index holds a prefix of the data in the timeline's order: the head, then
// whatever top-ups have loaded, ending at its tail. An item belongs to that window when
// it sorts at or before the tail — score first, then member, byte by byte, in the
// timeline's direction. An item past the tail is left out, because rows between the
// tail and the item may still be missing from the index: it arrives with the top-up
// that reaches it. Once :lastpage says the index holds the end of the data, there is
// nothing past the tail and every item belongs.
//
// :firstpage is the original first-page marker. It is still dropped when an item takes
// a full index past one page, as before; it does not widen or narrow the window.
//
// An empty index is not seeded (or was confirmed empty), so nothing is added to it; the
// :blankpage marker is dropped so the next read seeds it, this item included.
//
// Returns 1 if the item entered the index, 0 otherwise.
var timelineIngestScript = redis.NewScript(`
local score = tonumber(ARGV[1])
local member = ARGV[2]
local perPage = tonumber(ARGV[3])
local descending = ARGV[4] == 'desc'

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

redis.call('DEL', KEYS[4])

local count = redis.call('ZCARD', KEYS[1])
if count == 0 then
  return 0
end

local inside = redis.call('EXISTS', KEYS[3]) == 1
if not inside then
  local tail
  if descending then
    tail = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  else
    tail = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
  end
  local tailMember, tailScore = tail[1], tonumber(tail[2])
  if descending then
    inside = score > tailScore or (score == tailScore and not less(member, tailMember))
  else
    inside = score < tailScore or (score == tailScore and not less(tailMember, member))
  end
end

if not inside then
  return 0
end

if count == perPage and redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('DEL', KEYS[2])
end
redis.call('ZADD', KEYS[1], score, member)
return 1
`)

// timelineCursorScript reads one page of a Timeline positioned after a cursor member.
//
//	KEYS[1] sorted set
//	ARGV[1] cursor member   ARGV[2] limit   ARGV[3] direction
//
// The cursor's rank is looked up inside the script, at the moment of the read, so it
// cannot drift the way a rank handed to a client does: an item added at the head of a
// descending timeline between two requests shifts every rank by one, but not the
// member's own position. Members sharing the cursor's score keep their order, because
// the read continues from the cursor's rank rather than from its score.
//
// Returns false when the cursor is no longer in the index, otherwise the cursor's score
// followed by the page as member/score pairs:
//
//	{ cursorScore, member1, score1, member2, score2, ... }
var timelineCursorScript = redis.NewScript(`
local cursor = ARGV[1]
local limit = tonumber(ARGV[2])

local score = redis.call('ZSCORE', KEYS[1], cursor)
if not score then
  return false
end

local page
if ARGV[3] == 'desc' then
  local rank = redis.call('ZREVRANK', KEYS[1], cursor)
  page = redis.call('ZREVRANGE', KEYS[1], rank + 1, rank + limit, 'WITHSCORES')
else
  local rank = redis.call('ZRANK', KEYS[1], cursor)
  page = redis.call('ZRANGE', KEYS[1], rank + 1, rank + limit, 'WITHSCORES')
end

table.insert(page, 1, score)
return page
`)

// timelinePositionScript reads the page that follows a position — a score and a member —
// in a Timeline's order, whether or not that member is still in the index.
//
//	KEYS[1] sorted set
//	ARGV[1] score   ARGV[2] member   ARGV[3] limit   ARGV[4] direction
//
// Descending order is ZREVRANGE order: score descending, then member descending. The
// page starts at the first member with a lower score, or with the same score and a
// smaller member. Ascending order is ZRANGE order and the mirror image of it.
//
// The members sharing the cursor's score sit in one run of ranks, ordered by member.
// The script counts the members before that run, then binary-searches the run for the
// first member past the cursor, so a large tie group costs a few lookups rather than a
// read of the whole group. One round trip whatever the shape of the index.
//
// Members are compared byte by byte, the order Redis keeps equal-score members in.
// Lua's own '<' on strings goes through strcoll, which follows the server's LC_COLLATE
// and would place "B" after "a" under most locales other than C.
//
// Returns the page as member/score pairs: { member1, score1, member2, score2, ... }.
var timelinePositionScript = redis.NewScript(`
local key = KEYS[1]
local score = ARGV[1]
local member = ARGV[2]
local limit = tonumber(ARGV[3])
local descending = ARGV[4] == 'desc'

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

local first
if descending then
  first = redis.call('ZCOUNT', key, '(' .. score, '+inf')
else
  first = redis.call('ZCOUNT', key, '-inf', '(' .. score)
end
local tied = redis.call('ZCOUNT', key, score, score)

local lo, hi = first, first + tied
while lo < hi do
  local mid = math.floor((lo + hi) / 2)
  local past
  if descending then
    past = less(redis.call('ZREVRANGE', key, mid, mid)[1], member)
  else
    past = less(member, redis.call('ZRANGE', key, mid, mid)[1])
  end
  if past then
    hi = mid
  else
    lo = mid + 1
  end
end

if descending then
  return redis.call('ZREVRANGE', key, lo, lo + limit - 1, 'WITHSCORES')
end
return redis.call('ZRANGE', key, lo, lo + limit - 1, 'WITHSCORES')
`)
