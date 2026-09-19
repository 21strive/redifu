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
// index currently holds and the first/last page markers.
//
//	KEYS[1] sorted set   KEYS[2] :firstpage   KEYS[3] :lastpage   KEYS[4] :blankpage
//	ARGV[1] score        ARGV[2] member       ARGV[3] itemPerPage ARGV[4] direction
//
// Returns 1 if the item entered the index, 0 otherwise.
var timelineIngestScript = redis.NewScript(`
local score = tonumber(ARGV[1])
local member = ARGV[2]
local perPage = tonumber(ARGV[3])
local descending = ARGV[4] == 'desc'

redis.call('DEL', KEYS[4])

local count = redis.call('ZCARD', KEYS[1])
if count == 0 then
  return 0
end

local isFirstPage = redis.call('EXISTS', KEYS[2]) == 1
local isLastPage = redis.call('EXISTS', KEYS[3]) == 1

if descending then
  local edge = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  local lowest = tonumber(edge[2])
  if score >= lowest then
    if count == perPage and isFirstPage then
      redis.call('DEL', KEYS[2])
    end
    redis.call('ZADD', KEYS[1], score, member)
    return 1
  end
  return 0
end

local edge = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
local highest = tonumber(edge[2])
if score <= highest then
  if count == perPage and isFirstPage then
    redis.call('DEL', KEYS[2])
  end
  if isFirstPage or isLastPage then
    redis.call('ZADD', KEYS[1], score, member)
    return 1
  end
end
return 0
`)

// timelineCursorScript reads one page of a Timeline positioned after a cursor member.
//
//	KEYS[1] sorted set
//	ARGV[1] cursor member   ARGV[2] limit   ARGV[3] direction
//
// It anchors on the cursor's score rather than its rank. A rank cursor drifts: every
// item added at the head of a descending timeline shifts every rank by one, so the
// reader sees an item it has already seen. A score anchor is unaffected by inserts
// outside the window. Members sharing the cursor's exact score are handled by widening
// the window by the size of that tie group and skipping past the cursor inside it.
//
// Returns the page, or a nil reply when the cursor is no longer in the index.
var timelineCursorScript = redis.NewScript(`
local limit = tonumber(ARGV[2])
local descending = ARGV[3] == 'desc'
local cursor = ARGV[1]

local score = redis.call('ZSCORE', KEYS[1], cursor)
if not score then
  return false
end

local tied = redis.call('ZCOUNT', KEYS[1], score, score)
local window
if descending then
  window = redis.call('ZREVRANGEBYSCORE', KEYS[1], score, '-inf', 'LIMIT', 0, limit + tied)
else
  window = redis.call('ZRANGEBYSCORE', KEYS[1], score, '+inf', 'LIMIT', 0, limit + tied)
end

local page = {}
local passed = false
for i = 1, #window do
  if passed then
    page[#page + 1] = window[i]
    if #page >= limit then
      break
    end
  elseif window[i] == cursor then
    passed = true
  end
end
return page
`)
