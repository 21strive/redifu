# Redifu — CLAUDE.md

Redifu is a Go library for a Redis-backed data layer. Its primary purpose: provide collections that dynamically retain state across additions, deletions, and updates — with singleton behavior through the Relation mechanism.

## Architecture

Every data structure has two layers:

```
Base[T]          → key-value, single source of truth per item
SortedSet[T]     → sorted set, stores only randId as member
```

All fetch operations always follow: `sorted set → list of randIds → one batched Base read → resolve Relations in one batch per relation`.

### Struct hierarchy

```
SortedSet[T]          low-level: ZAdd, ZRange, ZRem, ZCard — not used directly by consumers
  ├── Sorted[T]       ordered collection, manages full collection without pagination state
  │     ├── Page[T]        wraps Sorted — numbered pagination (page 1, 2, 3)
  │     └── TimeSeries[T]  wraps Sorted — query by time range + gap detection
  └── Timeline[T]     ordered collection, manages cursor pagination (infinite scroll)
```

`Base[T]` stands alone, used by all structures above as the individual item store.

---

## Entity identity — `Record`

`Blueprint` is redifu's own interface: the methods redifu needs to index an entity
(`GetRandId`) and score it (`GetCreatedAt`). `Record` is the struct that implements it,
and every entity embeds it **as a pointer**:

```go
type Post struct {
    *redifu.Record
    Title string `json:"title"`
}

post := &Post{}          // Record is nil here
redifu.InitRecord(post)  // allocates it and mints the identity
```

**Every newly created entity goes through `InitRecord`.** That is the house rule, and
the pointer embed is what makes forgetting it visible: an entity with a nil `Record`
panics on its first method call instead of drifting on with a blank identity.

The one construction that does not mint is an entity about to be filled from a database
row — it already carries a randId, so it allocates without minting:

```go
func newPost() *Post { return &Post{Record: &redifu.Record{}} }
```

A fetch out of `Base` needs neither: unmarshalling allocates the embedded pointer.

redifu owns this contract; it does not depend on `github.com/21strive/item`. Because
`Blueprint` is an ordinary interface it is satisfied structurally, so an entity carrying
those methods some other way — an older one embedding `*item.Foundation`, or one
embedding its identity by value — still works. `record_test.go` pins that down; do not
turn `Blueprint` into a constraint that only `Record` can meet.

Two identifiers per entity, on purpose: `UUID` is private and `SecureUUID()` clears it
before an entity goes out over an API; `RandId` is the public handle, drawn from
`crypto/rand`, and it is the only thing a sorted set stores.

`InitRecord` allocates the embedded `Record` and mints identity for a **new** entity.
An entity being filled from a database row already has a randId — minting a fresh one
orphans every index pointing at the old one. It allocates the embedded pointers and, as
v1.0.0-rc.2 did, every other nil pointer field — except a relation field (a pointer to a
`Blueprint`), which is left nil because that is what a relation field must be until a
fetch resolves it.

---

## When to use which structure

| Use case | Structure |
|----------|-----------|
| Get/set a single item by ID | `Base[T]` |
| Fetch all sorted items, or query by score range | `Sorted[T]` |
| "Load more" feed / infinite scroll | `Timeline[T]` |
| Numbered page pagination | `Page[T]` |
| Query data by time range (charts, history) | `TimeSeries[T]` |

---

## `direction` and `sortingReference`

**`direction`** — configured at construction time. Controls fetch order (`ZRange` vs `ZRevRange`) and which items are eligible to enter the current page during `IngestItem`.

| Structure | direction |
|-----------|-----------|
| `Timeline` | configurable — `Ascending` or `Descending` |
| `Sorted` | configurable — `Ascending` or `Descending` |
| `Page` | configurable — `Ascending` or `Descending` |
| `TimeSeries` | always `Descending` (hardcoded) |

**`sortingReference`** — set after construction via `SetSortingReference(fieldName string) error`.
Controls which struct field is used as the sorted set score. Default is `createdAt` (resolved via `GetCreatedAt()`).
Supported field types: `time.Time`, `*time.Time`, `int64`. The field is resolved and
type-checked once, here — a typo fails at startup, not on the first write. An `int64`
beyond 2^53 is rejected with `ErrScoreOutOfRange` rather than silently losing precision.

```go
if err := timeline.SetSortingReference("UpdatedAt"); err != nil { ... }
```

If `sortingReference` is set, the same field must be the `ORDER BY` column in the SQL used to seed the collection, and the same field the cursor reference is read from.

`TimeSeries` also needs to know what an `int64` reference counts, because it converts in both
directions — a score into the instant whose segment must cover it, and the bounds of
`Fetch` into a score range. `SetScoreUnit(time.Second | time.Millisecond | time.Microsecond)`
sets it; the default is milliseconds. A `time.Time` reference is always scored in
milliseconds, so another unit is refused with a time reference, and reported on first use
of a series left on `createdAt`. Segments themselves are always recorded in milliseconds.

---

## Relation — Singleton Mechanism

Relation is how redifu avoids data duplication across entities.

**Entity shape (two fields per relation):**
```go
type Post struct {
    AccountRandId string   `json:"accountRandId"` // the pointer — this is what Redis stores
    Account       *Account `json:"-"`             // filled on fetch, never serialized
}
```

Field names are free — nothing is inferred from them. The `json:"-"` tag is required: it is
what keeps a fetched item safe to write back to `Base` without baking a copy of the related
entity into the item key.

**Item types must be pointers:** `Base[*Post]`, never `Base[Post]`. A Relation writes into
your struct, which is only possible through a pointer. `Relate` rejects value types.

**How it works during fetch:**
1. The page of items is read from `Base` in one round-trip
2. Per relation, the randIds are collected and deduped, then read in one batch
3. Each fetched entity is written into its items; the randId is **kept**, not cleared

**Effect:** Update `Account` once in `Base[Account]` → all `Post` records referencing it automatically reflect the change.

**Setting up a Relation:**
```go
relation, err := redifu.Relate(accountBase,
    func(p *Post) string    { return p.AccountRandId }, // where the randId lives
    func(p *Post, a *Account) { p.Account = a },        // where the entity goes
)
if err != nil {
    // wiring mistake — a value item type instead of a pointer, or a relation field
    // that is not tagged json:"-" (ErrRelationNotTransient)
}
postBase.AddRelation(relation)      // preferred: travels with the entity everywhere
postTimeline.AddRelation(relation)  // index-specific extras only
```

**Relations belong on `Base`.** A relation registered there is resolved by every read of
that entity — `Base.Get`, `Base.GetMany`, and every index built on top of it — and by
any other entity that relates to it. Registering on an index instead limits it to that
index, which is how a single-item read used to come back with nil relation fields.

**Relations nest.** `Doc → Owner → Org` resolves in full, one batched round-trip per
level. Depth is capped at `DefaultRelationDepth` (4) so a self-referential relation
terminates; change it with `base.SetRelationDepth(n)`.

**The `json:"-"` tag is enforced.** `Relate` renders a blank parent before and after
running the setter; if the related entity appears in the parent's stored JSON the
relation is refused with `ErrRelationNotTransient`.

Both accessors are ordinary functions, so the compiler checks them: renaming a field breaks
the build here instead of silently resolving to nothing at runtime. Register as many as the
entity needs — `AddRelation(authorRelation, categoryRelation)` — they share one pipeline.

If a related key has expired, that field comes back `nil` while the rest of the fetch
succeeds. Guard for it. Reads use `GETEX`, so anything that is being read stays alive;
`base.SetTouchOnRead(false)` on Redis older than 6.2.

Full guide: `docs/plan-2026-08-30/01-relation.md`. Upgrade notes: `MIGRATION.md`.

There is no one-to-many relation: a parent that owns a list keeps an index per parent
(`terms:of:%s`). Why, and what would come first if it is ever built: `docs/one-to-many-relations.md`.

---

## Pipeline Discipline (critical invariant)

Functions that receive `pipe redis.Pipeliner` as a parameter **must not** call `pipe.Exec(ctx)`. Only the caller may execute the pipeline.

```go
// CORRECT — internal function, receives pipe from caller
func (s *Sorted[T]) addItem(ctx context.Context, pipe redis.Pipeliner, ...) error {
    pipe.ZAdd(...)  // enqueue operations only, never Exec
    return nil
}

// CORRECT — public function, creates its own pipeline (selfPipe pattern)
func (s *Sorted[T]) AddItem(ctx context.Context, item T, ...) error {
    pipe := s.client.Pipeline()
    s.addItem(ctx, pipe, item, ...)
    _, err := pipe.Exec(ctx)
    return err
}
```

The `selfPipe` pattern is used in `Sorted.addItem`, `Timeline.addItem`, and `TimeSeries.addItem`.

---

## Key layout

Every collection key is wrapped in a Redis Cluster hash tag so that the sorted set and
its markers land in one slot — required for the atomic ingest scripts. A collection
declared as `feed:%s` is stored as `{feed:u1}`, its markers as `{feed:u1}:firstpage`.
Item keys are not tagged: `post:%s` stays `post:abc`.

Key formats are validated at construction (`newKeyBuilder`): only `%s` is accepted, the
parameter count is checked on every call, and parameters may not be empty or contain
`{`/`}`. Derived keys always copy the caller's slice — spreading a variadic argument
hands the callee the caller's own backing array.

A key names data, never a version of it: `post:%s`, not `post:v3:%s`. Which build wrote a
value or seeded an index is the generation's job (next section).

## Generations

A consumer changes what it stores — an entity gains a field, a list's membership rule or
score changes — and must then stop reading what the earlier build wrote. A version segment in
the key name did that once and stuck: every bump turned the whole cache cold, and during a
rolling deploy the old process kept writing the old names while the new one served the new
names, so the old one's changes never reached them. A generation keeps one name per piece of
data and puts the version into the stored state (`generation.go`):

| | `SetGeneration` on | Stored as | Another generation's (or none) |
|---|---|---|---|
| Item | `Base` | the value, prefixed `<generation>\x1f` | a missing key: `ErrNotFound`, absent from `GetMany`, dangling in an index, a nil relation; `SetIfAbsent` replaces it, `Exists` is false |
| Index | `Sorted`, `Timeline`, `Page`, `TimeSeries` | a marker `{K}:gen` next to the other markers, with the index TTL (a `TimeSeries` keeps its segment store under the same hash tag, `{K}:segments`, as one of the markers) | the index is not there: the operation that finds it drops the sorted set and **all** its markers first, in the same command, then does its own work on the cold index |

How each kind of operation applies it, with a generation set:

- **Reads** (`Fetch`/`FetchFrom`/`Exec`, `Count`, `IsEmpty`, `RequiresSeeding`, `ReachedEnd`,
  `Tail`, the `Is…Page` markers, every TimeSeries segment read) run `generationCheckScript` and
  then their own commands in one `MULTI/EXEC` (`SortedSet.guarded`). After the check the index
  is this generation's or gone, so a read never sees another generation's members or markers.
  A `Sorted`/`Page` member read of a dropped (or cold) index answers `ErrNotSeeded`, never an
  empty list; a Timeline read sees a cold index, which its flow seeds; a TimeSeries reads a gap.
- **Writes outside a seed** (`AddItem`, `RemoveItem`, a segment `Remove`) are scripts behind
  `generationGuard`: on a foreign index they drop it and report it (`ErrNotIngested` /
  0). Dropping is the only safe answer — another generation's members follow rules this
  build does not know, and leaving its index unchanged would leave it stale. A removal from an
  index that is not there at all is no error (`removalGuard` answers -1).
- **Self-heal** (`generationHealScript`) is the exception: it removes members only from an index
  that is still this generation's, a thousand per `ZREM`, and never drops one — the index may
  have become another generation's fresh seed since the read. Members whose item holds another
  generation's value are dangling for the report but not removed: the item exists.
- **Seed writes** (`IngestItem` with seed = true, `MarkEmpty`, `MarkLastPage`, `MarkFirstPage`,
  the `Unmark` methods, `HasData`, `SetExpiration`, `AddSegment`) go through
  `generationStampScript`: drop a foreign index, stamp the marker with the TTL, then write.
  The first write of a seed therefore starts from an empty index. A seed must run in one
  `MULTI/EXEC` for that to hold: interleaved with another generation's seed, a plain pipeline
  could end up holding a part of each.
- **A Timeline top-up** is `Extend` (`timelineExtendScript`), not a seed write: it applies only
  to this generation's index whose tail is still the cursor the rows follow, and writes nothing
  otherwise. A stamp would start a dropped index over with rows from the middle of the data.
- `Purge` drops the `:gen` marker with the rest, with or without a generation.
- The generation lives in a `generationCell` (an atomic pointer): `SetGeneration` may run while
  the collection is used, and an operation reads the generation once and passes it to every
  command it issues.

The generation scripts take the collection's keys in one order — the sorted set, its markers
(`SortedSet.markers`: `:blankpage` for a `Sorted`; `:blankpage`, `:segments` for a `TimeSeries`;
`:firstpage`, `:lastpage`, `:blankpage` for a `Timeline`), the generation marker last — and the generation as the last argument, so the
guard reads `KEYS[#KEYS]` / `ARGV[#ARGV]` and drops with `DEL unpack(KEYS)`. The ingest scripts
with a generation are the scripts without one behind that guard (`sortedIngestBody`,
`timelineIngestBody`), so the two can never drift apart.

Without `SetGeneration` nothing changes: no prefix, no marker, the original commands and
scripts. Every generation scenario is in `generation_test.go` and runs in `TestRealRedis`.

## Timeline state markers

Timeline stores pagination state via additional Redis keys:

| Key suffix   | Value | Meaning |
|--------------|-------|---------|
| `:blankpage` | `"1"` | Sorted set confirmed empty from DB |
| `:lastpage`  | `"1"` | The index reaches the end of the data: a seed got fewer rows than it asked for |
| `:firstpage` | `"1"` | Legacy: the original first-page seed returned less than a page. The documented flow no longer sets it |
| `:gen`       | the generation | Which generation seeded the index (only with `SetGeneration`; see Generations) |

These markers are set by seeders. Do not set them manually outside of a seeder.

A Timeline index is always a **prefix** of the data in the timeline's order: the head seed
loads it from the top and every later seed extends it from `Tail`. That is what the markers
and the ingest window rely on, and why a top-up must never be anchored on a client's cursor.
`ReachedEnd` reads `:blankpage` for an empty index and `:lastpage` for one that holds
members — a `:lastpage` left on an empty index outlived the index it described (the head seed
sets the index TTL, a later top-up the marker's) and is ignored; the documented head seed
also drops it.

## Ingest runs inside Redis

`IngestItem(seed=false)` and everything built on it is a single Lua script
(`scripts.go`): the state reads, the window check, the marker updates and the write are
one atomic operation enqueued into the caller's pipeline.

This is not an optimisation to undo. It is what removes the read-decide-write race
between concurrent writers, and it turns a fan-out write from five round-trips per
target into one enqueued command.

**The Timeline window.** The index is a prefix of the data ending at its tail, so an item
enters when it sorts at or before the tail — score, then member byte by byte, in the
timeline's direction — and anywhere once `:lastpage` says the index reaches the end of the
data. An item past the tail of a partial index is refused (`ErrNotIngested`): rows between
the tail and the item may not be loaded yet, so adding it would put a hole in the prefix;
the top-up that reaches it loads it. An empty index takes nothing (it is not seeded, or
confirmed empty — the `:blankpage` marker is dropped so the next read seeds it).
`:firstpage` does not move the window; it is still dropped when an item takes a full
index past one page. An item already in the index whose new score falls past the tail
keeps its old entry — scores of a Timeline are expected not to move backwards.

Scripts are sent with `EVAL`, not `EVALSHA`: a pipelined `EVALSHA` cannot fall back when
the script is not cached, because `NOSCRIPT` only surfaces at `Exec`. Every key a script
touches must share a slot — hence the hash tag above.

Two rules for every script:

- **Compare members byte by byte.** Redis orders members that share a score by their
  bytes, but Lua's `<` on strings inside Redis goes through `strcoll`, which follows the
  server's `LC_COLLATE` — under `en_US` it puts `"B"` after `"a"`. Use the `less` helper
  in `timelinePositionScript`, never `<`, on members.
- **Run the suite against a real server whenever a script changes.** miniredis runs
  scripts on gopher-lua, where `<` is byte-wise, so it cannot catch the rule above.
  Scenarios that exercise a script take a `clientFactory` and are listed in
  `TestRealRedis` (`realredis_test.go`), which runs them against `REDIFU_REDIS_ADDR`:

  ```bash
  LC_ALL=en_US.utf8 redis-server --port 6399 --save '' &
  REDIFU_REDIS_ADDR=127.0.0.1:6399 go test -run TestRealRedis -v ./...
  ```

## Timeline cursors

Two kinds, both answered by one script round trip:

| Cursor | Read with | When its member left the index |
|---|---|---|
| randId (`ValidLastId()`) | `Fetch(lastRandIds)` | `ErrResetPagination` — the client starts over |
| position (`NextCursor()`, a `Cursor`) | `FetchFrom(cursor)` | nothing: the page after a position exists without its member |

A position is `(score, member)` in the timeline's order: score then member, both
descending for `Descending` (ZREVRANGE order), both ascending for `Ascending` (ZRANGE
order). `timelinePositionScript` counts the members before the cursor's score, then
binary-searches the run of members sharing that score — so a tie group of thousands costs
a handful of lookups. Scores cross into Lua through `formatScore`, which renders any
float64 exactly; `Timeline.Score(item)` is the one way to compute the score of an item.

Every Timeline read takes `WithLimit(n)`: a page of `n` for that fetch instead of
`itemPerPage`, same `n+1` HasMore check (`FetchAll` ignores it). The page size is a property
of a read, not of the index, which is why a top-up is sized from the read it completes.

`Timeline.Tail` is the position of the index's last member in the same order (descending:
`ZRANGE 0 0`, the lowest score and the smallest member tied there; ascending:
`ZRANGE -1 -1`). A top-up seed continues from the tail, never from a client's cursor: the
cursor may lie beyond the tail after the index lost its end, and seeding from it would
leave a gap no later read fills.

---

## Seeding

Redifu does not read from SQL. Populating Redis from a database is the consumer's job — they
write plain SQL and a plain scan loop, then feed the results in through the primitives below.

Built-in seeders and the SQL `Builder` were removed: they could not express views, CTEs,
subqueries or `WHERE IN`, and the scanner indirection they required cost more than the SQL it
saved. Consumer-facing guidance and a worked example live in `CLAUDE.consumer.md`.

### Primitives a consumer seeder relies on

| Method | Purpose |
|--------|---------|
| `Base.WithPipeline(pipe).Set` | store the item itself |
| `Base.WithPipeline(pipe).SetIfAbsent` | store it only if absent, but always refresh its TTL |
| `Base.GetMany(ctx, randIds)` | read many items in one round-trip, relations resolved |
| `IngestItem(ctx, pipe, item, seed, keyParams...)` | add to the index — pass `seed = true` while seeding |
| `Timeline.Extend(ctx, pipe, after, items, reachedEnd, keyParams...)` | a top-up: append the rows after the tail, only if the index still ends there |
| `SetExpiration(ctx, pipe, keyParams...)` | apply the collection TTL |
| `RequiresSeeding(...)` | decide whether seeding is needed at all (`Sorted`, `Page`; deprecated on `Timeline`) |
| `MarkEmpty` / `MarkLastPage` / `UnmarkLastPage` | Timeline state markers (see above) |
| `Timeline.ReachedEnd` / `Timeline.Tail` | whether a page that ran past the index needs a top-up, and where it continues from |
| `TimeSeries.Covers(ctx, instant, ...)` | does a seeded segment cover this instant |
| `Page.AddPage` | register a page in the page index |
| `TimeSeries.AddSegment` / `FindGap` | record and locate seeded time ranges |

These are the only supported entry points for writing into a collection out of band. All of
them take a caller-owned pipeline and must not execute it — see Pipeline Discipline.

A Timeline is seeded by **top-up**: `FetchFrom(cursor)`; while the page has no `HasMore()`
and the index has not `ReachedEnd`, load `itemPerPage + 1 - len(items)` rows that follow
`Tail`, `Extend` the index with them and fetch again. The head seed of a cold index is the
same query with the zero `Cursor` as anchor, written with `IngestItem(seed = true)` and the
markers. The consumer's keyset must compare exactly what the index sorts by — the
score (`Timeline.Score`; milliseconds for a time field) and the member in byte order — and
`TestDocumentedTimelineFlow*` (`topup_flow_test.go`) drives the documented handler and
seeder against tables of 0 to 200 rows. It replaces the `subtraction` parameter of the
original flow, whose handler seeded the first page only.

---

## Invariants to preserve

1. **Collections never delete `Base` keys.** `RemoveItem` only removes a member from one index; `Purge` only drops the index and its markers. Deleting the entity itself is `Base.Del`, and it is deliberate: any other index still holding that randId will come back short until it is re-seeded or purged. Prefer a leak that expires on its own over a hole that does not.
2. **Functions that receive `pipe` must not call `pipe.Exec`.** See Pipeline Discipline above.
3. **A relation field must be tagged `json:"-"`** — without it, writing a fetched item back to `Base` bakes a copy of the related entity into the item key and breaks the singleton permanently.
4. **Scores in sorted sets are always numeric:** Unix timestamp in milliseconds or `int64`. No other types. A `TimeSeries` reads an `int64` score in the unit set with `SetScoreUnit` (milliseconds unless told otherwise).
5. **`Relate` returns an error** — always handle it at startup, never ignore it.
6. **`AddItem` places an item into an index; it does not update the item.** It writes the item only when `Base` does not hold it yet, but always refreshes its TTL — so an index can never outlive the items it points at. To change an item's contents, use `Base.Set`.
7. **An outcome is reported, never swallowed.** `AddItem` returns `ErrNotIngested` when it stored the item but did not index it. `Count` returns an error rather than folding a Redis failure into `0`. A `Sorted` or `Page` read reports the members it dropped because their item was gone (`ExecReport`), the way a Timeline read reports `Dangling()`. A randId cursor that has left the index returns `ErrResetPagination` rather than silently serving page one; a position cursor (`FetchFrom`) needs no such escape, because the page after a position exists whether or not its member does. Page position is read from the index, not from how many items survived hydration.
8. **Errors are redifu's own.** `ErrNotFound`, never `redis.Nil`, crosses the public API.
9. **A generation is checked by the command that acts on it.** With `SetGeneration`, every read of an index runs behind the check in the same `MULTI/EXEC`, every write behind the guard or the stamp in the same script, and a Base value is compared with its prefix on every read. A check in one round trip and the work in the next would let another generation slip in between.

---

## Adding new features

- New data structures must compose from `Sorted[T]` or `Base[T]`, not rebuild directly from `SortedSet[T]`.
- New methods that need a pipeline: follow the `publicMethod` (creates selfPipe) + `privateMethod` (receives pipe parameter) pattern.
- A function that receives a pipeline must not issue reads of its own either. If it needs to read state before it writes, that belongs in a Lua script enqueued into the same pipeline.
- Every new key goes through `keyBuilder`; every derived parameter list goes through `appendParams`.
- A new index operation handles the generation: a read goes through `SortedSet.guarded`, a seed write through `stamp`, any other write through a script behind `generationGuard`; a new marker joins `SortedSet.markers` so the guard drops it too. Add its scenario to `generation_test.go` and `TestRealRedis`.
- Do not reintroduce SQL into this package. Anything that needs a database belongs in the consumer, driven by the seeding primitives above.