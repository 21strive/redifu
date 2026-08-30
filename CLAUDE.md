# Redifu — CLAUDE.md

Redifu is a Go library for a Redis-backed data layer. Its primary purpose: provide collections that dynamically retain state across additions, deletions, and updates — with singleton behavior through the Relation mechanism.

## Architecture

Every data structure has two layers:

```
Base[T]          → key-value, single source of truth per item
SortedSet[T]     → sorted set, stores only randId as member
```

All fetch operations always follow: `sorted set → list of randIds → Base.Get per id → resolve Relation`.

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

**`sortingReference`** — set after construction via `SetSortingReference(fieldName string)`.
Controls which struct field is used as the sorted set score. Default is `createdAt` (resolved via `GetCreatedAt()`).
Supported field types: `time.Time`, `*time.Time`, `int64`.

```go
timeline.SetSortingReference("UpdatedAt") // sort by UpdatedAt instead of CreatedAt
```

If `sortingReference` is set, the same field must be the `ORDER BY` column in the SQL used to seed the collection, and the same field the cursor reference is read from.

---

## Relation — Singleton Mechanism

Relation is how redifu avoids data duplication across entities.

**Field naming convention (required):**
```go
type Post struct {
    Account       Account // related entity field
    AccountRandId string  // always: FieldName + "RandId"
}
```

**How it works during fetch:**
1. `Post` is fetched from `Base[Post]` — `Account` field is empty, `AccountRandId` holds the ID
2. Relation lookup: fetch `Account` from `Base[Account]` using `AccountRandId`
3. Set into the `Account` field, clear `AccountRandId`

**Effect:** Update `Account` once in `Base[Account]` → all `Post` records referencing it automatically reflect the change.

**Setting up a Relation:**
```go
relation, err := redifu.NewRelation[Account](&accountBase, redifu.TypeOf[Post]())
if err != nil {
    // Account field not found in Post — naming convention mismatch
}
postTimeline.AddRelation("account", relation)
```

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

## Timeline state markers

Timeline stores pagination state via additional Redis keys:

| Key suffix   | Value | Meaning |
|--------------|-------|---------|
| `:blankpage` | `"1"` | Sorted set confirmed empty from DB |
| `:firstpage` | `"1"` | No items exist before this page |
| `:lastpage`  | `"1"` | No items exist after this page |

These markers are set by seeders. Do not set them manually outside of a seeder.

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
| `IngestItem(ctx, pipe, item, seed, keyParams...)` | add to the index — pass `seed = true` while seeding |
| `SetExpiration(ctx, pipe, keyParams...)` | apply the collection TTL |
| `RequiresSeeding(...)` | decide whether seeding is needed at all |
| `MarkEmpty` / `MarkFirstPage` / `MarkLastPage` | Timeline state markers (see above) |
| `Page.AddPage` | register a page in the page index |
| `TimeSeries.AddSegment` / `FindGap` | record and locate seeded time ranges |

These are the only supported entry points for writing into a collection out of band. All of
them take a caller-owned pipeline and must not execute it — see Pipeline Discipline.

`subtraction` is a Timeline concept the consumer computes and applies themselves: the gap
between what Redis already holds and `itemPerPage`, subtracted from the SQL `LIMIT`.

---

## Invariants to preserve

1. **Never delete an item from Base without removing it from the index.** Always use `RemoveItem` (which handles both), not `Base.Del` alone.
2. **Functions that receive `pipe` must not call `pipe.Exec`.** See Pipeline Discipline above.
3. **The Relation naming convention is required:** `FieldName` + `FieldNameRandId`.
4. **Scores in sorted sets are always numeric:** Unix timestamp in milliseconds or `int64`. No other types.
5. **`NewRelation` returns an error** — always handle it, never ignore it.

---

## Adding new features

- New data structures must compose from `Sorted[T]` or `Base[T]`, not rebuild directly from `SortedSet[T]`.
- New methods that need a pipeline: follow the `publicMethod` (creates selfPipe) + `privateMethod` (receives pipe parameter) pattern.
- Do not reintroduce SQL into this package. Anything that needs a database belongs in the consumer, driven by the seeding primitives above.