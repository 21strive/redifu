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

## Timeline state markers

Timeline stores pagination state via additional Redis keys:

| Key suffix   | Value | Meaning |
|--------------|-------|---------|
| `:blankpage` | `"1"` | Sorted set confirmed empty from DB |
| `:firstpage` | `"1"` | No items exist before this page |
| `:lastpage`  | `"1"` | No items exist after this page |

These markers are set by seeders. Do not set them manually outside of a seeder.

## Ingest runs inside Redis

`IngestItem(seed=false)` and everything built on it is a single Lua script
(`scripts.go`): the state reads, the window check, the marker updates and the write are
one atomic operation enqueued into the caller's pipeline.

This is not an optimisation to undo. It is what removes the read-decide-write race
between concurrent writers, and it turns a fan-out write from five round-trips per
target into one enqueued command.

Scripts are sent with `EVAL`, not `EVALSHA`: a pipelined `EVALSHA` cannot fall back when
the script is not cached, because `NOSCRIPT` only surfaces at `Exec`. Every key a script
touches must share a slot — hence the hash tag above.

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
| `SetExpiration(ctx, pipe, keyParams...)` | apply the collection TTL |
| `RequiresSeeding(...)` | decide whether seeding is needed at all |
| `MarkEmpty` / `MarkFirstPage` / `MarkLastPage` | Timeline state markers (see above) |
| `TimeSeries.Covers(ctx, instant, ...)` | does a seeded segment cover this instant |
| `Page.AddPage` | register a page in the page index |
| `TimeSeries.AddSegment` / `FindGap` | record and locate seeded time ranges |

These are the only supported entry points for writing into a collection out of band. All of
them take a caller-owned pipeline and must not execute it — see Pipeline Discipline.

`subtraction` is a Timeline concept the consumer computes and applies themselves: the gap
between what Redis already holds and `itemPerPage`, subtracted from the SQL `LIMIT`.

---

## Invariants to preserve

1. **Collections never delete `Base` keys.** `RemoveItem` only removes a member from one index; `Purge` only drops the index and its markers. Deleting the entity itself is `Base.Del`, and it is deliberate: any other index still holding that randId will come back short until it is re-seeded or purged. Prefer a leak that expires on its own over a hole that does not.
2. **Functions that receive `pipe` must not call `pipe.Exec`.** See Pipeline Discipline above.
3. **A relation field must be tagged `json:"-"`** — without it, writing a fetched item back to `Base` bakes a copy of the related entity into the item key and breaks the singleton permanently.
4. **Scores in sorted sets are always numeric:** Unix timestamp in milliseconds or `int64`. No other types.
5. **`Relate` returns an error** — always handle it at startup, never ignore it.
6. **`AddItem` places an item into an index; it does not update the item.** It writes the item only when `Base` does not hold it yet, but always refreshes its TTL — so an index can never outlive the items it points at. To change an item's contents, use `Base.Set`.
7. **An outcome is reported, never swallowed.** `AddItem` returns `ErrNotIngested` when it stored the item but did not index it. `Count` returns an error rather than folding a Redis failure into `0`. A cursor that has left the index returns `ErrResetPagination` rather than silently serving page one. Page position is read from the index, not from how many items survived hydration.
8. **Errors are redifu's own.** `ErrNotFound`, never `redis.Nil`, crosses the public API.

---

## Adding new features

- New data structures must compose from `Sorted[T]` or `Base[T]`, not rebuild directly from `SortedSet[T]`.
- New methods that need a pipeline: follow the `publicMethod` (creates selfPipe) + `privateMethod` (receives pipe parameter) pattern.
- A function that receives a pipeline must not issue reads of its own either. If it needs to read state before it writes, that belongs in a Lua script enqueued into the same pipeline.
- Every new key goes through `keyBuilder`; every derived parameter list goes through `appendParams`.
- Do not reintroduce SQL into this package. Anything that needs a database belongs in the consumer, driven by the seeding primitives above.