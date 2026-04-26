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

`TimelineSeeder` reads `sortingReference` to determine which field to use when fetching the cursor reference from the DB (`getFieldValue` in `runSeed`). If `sortingReference` is set on the `Timeline`, the same field must be the `ORDER BY` column in the SQL queries passed to the seeder.

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

## Seeders

Seeders populate Redis from a SQL database. Each structure has a corresponding seeder:

| Seeder | For |
|--------|-----|
| `TimelineSeeder[T]` | Timeline — manages cursor (`lastRandId`) and `subtraction` |
| `SortedSeeder[T]` | Sorted — single Base query |
| `PageSeeder[T]` | Page — Base query + LIMIT/OFFSET |
| `TimeSeriesSeeder[T]` | TimeSeries — query by time range |

`TimelineSeeder` is the most complex: the first page uses `Base()`, subsequent pages use `WithCursor()`. The `subtraction` parameter represents the gap between items already in Redis and the target `itemPerPage`.

---

## Query Builder

`Builder` in `query_builder.go` is a PostgreSQL SQL query generator (`$1, $2, ...`).

- `Base()` — full query, used for seeding the first page
- `WithCursor()` — appends a cursor condition, used for seeding subsequent pages
- `Row(idCol)` — single-row query by ID, used by the seeder to fetch the cursor reference item

**Limitation:** PostgreSQL only, no support for `WHERE IN`, subqueries, or `HAVING`. The query builder remains available but is not the primary approach — **writing SQL directly is preferred**.

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
- New seeders: follow the `runSeed` + builder struct pattern with `WithQueryArgs`, `WithParams`, `Exec`.