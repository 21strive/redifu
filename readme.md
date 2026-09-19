<div align="center">
<pre style="white-space: pre-wrap; overflow-x: hidden; background: transparent;">
                █████████████           █████████                
                ████████████          ███████████                
                ██████████          █████████████                
                ███████           ███████████████                
                █████████████████████████████████                
                ████████████████         ████████                
                ██████████████           ████████                
                ███████████              ████████                
                █████████                ████████                

</pre>
</div>

# redifu

Go library for a Redis-backed data layer with **singleton behavior** — updating an entity once automatically reflects across every collection that references it, with no data duplication.

## Installation

```bash
go get github.com/21strive/redifu
```

## Mental Model

All data is stored in two layers:

```
Base[T]       → key-value, one item per key (source of truth)
SortedSet     → sorted set, stores only randId as member
```

On every fetch, redifu always: **fetches randIds from sorted set → fetches each item from Base → resolves Relations**.

This means: update an item once in `Base`, and every collection referencing it immediately shows the latest version.

## Data Structures

### Base[T] — individual item

Use for get/set of a single item by ID.

```go
postBase := redifu.NewBase[Post](redisClient, "post:%s", 7*24*time.Hour)

// Set
postBase.Set(ctx, post)

// Get
post, err := postBase.Get(ctx, randId)

// Mark as missing (avoid repeated DB hits for non-existent items)
postBase.MarkAsMissing(ctx, randId)
missing, _ := postBase.IsMissing(ctx, randId)
```

---

### Timeline[T] — cursor pagination (infinite scroll)

Use for "load more" feeds. Manages page state (first/last/empty) automatically.

```go
postTimeline := redifu.NewTimeline[Post](
    redisClient,
    postBase,
    "feed:user:%s:posts",   // key format — %s is filled by keyParams
    20,                      // items per page
    redifu.Descending,
    2*24*time.Hour,
)

// Add item
postTimeline.AddItem(ctx, post, userRandId)

// Fetch — lastRandIds is an array to tolerate items deleted from cache
output   := postTimeline.Fetch(lastRandIds).WithParams(userRandId).Exec(ctx)
items    := output.Items()
nextId   := output.ValidLastId()   // send to client, used as the next cursor
position := output.Position()      // FirstPage / MiddlePage / LastPage

// Remove item
postTimeline.RemoveItem(ctx, post, userRandId)
```

---

### Sorted[T] — full sorted collection

Use for collections that are always fetched in full, or queried by score range.

```go
commentSorted := redifu.NewSorted[Comment](
    redisClient,
    commentBase,
    "post:%s:comments",
    7*24*time.Hour,
)

// Add
commentSorted.AddItem(ctx, comment, postRandId)

// Fetch all
items, err := commentSorted.Fetch(redifu.Ascending).WithParams(postRandId).Exec(ctx)

// Fetch by score range (e.g. by timestamp)
items, err := commentSorted.Fetch(redifu.Ascending).
    WithParams(postRandId).
    WithRange(lowerUnixMilli, upperUnixMilli).
    Exec(ctx)
```

---

### Page[T] — numbered pagination

Use for numbered page pagination (page 1, 2, 3).

```go
productPage := redifu.NewPage[Product](
    redisClient,
    productBase,
    "category:%s:products",
    20,               // items per page
    redifu.Ascending,
    2*24*time.Hour,
)

// Fetch a specific page
items, err := productPage.Fetch(pageNumber).WithParams(categoryRandId).Exec(ctx)

// Check whether seeding is needed
needsSeed, _ := productPage.RequiresSeeding(ctx, pageNumber, categoryRandId)
```

---

### TimeSeries[T] — query by time range

Use for historical data, charts, and reports. Tracks seeded segments and detects gaps automatically.

```go
txTimeSeries := redifu.NewTimeSeries[Transaction](
    redisClient,
    txBase,
    "account:%s:transactions",
    7*24*time.Hour,
)

// Fetch — returns needsSeed=true if any gaps exist in the requested range
items, needsSeed, err := txTimeSeries.
    Fetch(startTime, endTime).
    WithParams(accountRandId).
    Exec(ctx)
```

---

## Relation — Singleton Behavior

Relations prevent data duplication across entities that reference each other.

**Entity shape (two fields per relation):**

```go
type Post struct {
    redifu.Record
    Title        string
    AuthorRandId string           `json:"authorRandId"` // the pointer — this is what Redis stores
    Author       *account.Account `json:"-"`            // filled on fetch, never serialized
}
```

`Author` is not stored inside `Post`. On fetch, redifu reads `AuthorRandId`, retrieves `Author` from `Base[*Account]`, and writes it into the `Author` field — keeping the randId, so the result is safe to write straight back to `Base`. Because `Base[*Account]` is the source of truth, any update to `Account` is immediately reflected in all `Post` records.

Item types are pointers: `Base[*Post]`, never `Base[Post]`.

**Setup:**

```go
authorRelation, err := redifu.Relate(
    account.AccountBase,
    func(p *Post) string              { return p.AuthorRandId }, // where the randId lives
    func(p *Post, a *account.Account) { p.Author = a },          // where the entity goes
)
if err != nil {
    log.Fatal(err)
}
postTimeline.AddRelation(authorRelation)
```

Both accessors are ordinary functions, so renaming a field breaks the build here rather than
resolving to nothing at runtime. Relations are resolved per relation for the whole page, so a
related key shared by twenty items is read once.

---

## Seeding

Redifu never touches your database. When a key has no data, you run your own SQL and feed the
rows in through redifu's public methods:

```go
func GetFeed(ctx context.Context, userRandId string, lastRandIds []string) ([]*Post, error) {
    needsSeed, err := postTimeline.RequiresSeeding(ctx, int64(len(lastRandIds)), userRandId)
    if err != nil {
        return nil, err
    }

    if needsSeed {
        if err := seedFeed(ctx, userRandId); err != nil {
            return nil, err
        }
    }

    output := postTimeline.Fetch(lastRandIds).WithParams(userRandId).Exec(ctx)
    return output.Items(), output.Error()
}

func seedFeed(ctx context.Context, userRandId string) error {
    rows, err := db.QueryContext(ctx, `
        SELECT p.randid, p.title, p.author_randid, p.created_at
        FROM posts p
        WHERE p.user_id = $1
        ORDER BY p.created_at DESC
        LIMIT 20`, userRandId)
    if err != nil {
        return err
    }
    defer rows.Close()

    pipe := redisClient.Pipeline()
    var count int64

    for rows.Next() {
        p := &Post{}
        if err := rows.Scan(&p.RandId, &p.Title, &p.AuthorRandId, &p.CreatedAt); err != nil {
            return err
        }
        postBase.WithPipeline(pipe).Set(ctx, p)
        postTimeline.IngestItem(ctx, pipe, p, true, userRandId)
        count++
    }
    if err := rows.Err(); err != nil {
        return err
    }

    if count == 0 {
        postTimeline.MarkEmpty(ctx, pipe, userRandId)
    } else if count < postTimeline.GetItemPerPage() {
        postTimeline.MarkFirstPage(ctx, pipe, userRandId)
    }
    postTimeline.SetExpiration(ctx, pipe, userRandId)

    _, err = pipe.Exec(ctx)
    return err
}
```

One pipeline, one round trip. `CLAUDE.consumer.md` carries the same pattern for `Sorted`,
`Page` and `TimeSeries`, plus the cursor handling a paged Timeline needs.

---

## Default TTL

| | TTL |
|--|-----|
| Individual item (`Base`) | 7 days |
| Sorted set / Timeline | 2 days |

TTL is configurable at initialization.

## Limitations

- Sorting only supports fields of type `time.Time`, `*time.Time` or `int64`.
- Seeding is yours to write — redifu has no SQL layer, no query builder and no database dependency.
- Relations resolve one level only: if a related entity has relations of its own, those stay empty.

---

## Breaking changes on `redifu-simplified`

Rationale and migration detail: [`docs/plan-2026-08-30/`](docs/plan-2026-08-30/).

| Change | Migration |
|--------|-----------|
| `NewRelation` and `TypeOf` removed | declare relations with `Relate(base, getRandId, setItem)` |
| `AddRelation(identifier, rel)` → `AddRelation(rels ...Relation[T])` | drop the identifier — it was never used when resolving |
| `GetRelation()` → `GetRelations()` | returns a slice, not a map |
| Item types must be pointers | `Base[*Post]`, not `Base[Post]`; tag relation fields `json:"-"` |
| `Sorted.Remove()` / `Timeline.Remove()` removed | use `Purge` — the two were identical once `Base` deletion was dropped |
| `Purge()` builder → `Purge(ctx, keyParams...)` | `tl.Purge().WithParams(u).Exec(ctx)` becomes `tl.Purge(ctx, u)` |

Two changes are **silent** — they compile unchanged but behave differently:

- **`RemoveItem` no longer deletes the item from `Base`.** It only removes the member from that
  one index. Deleting the entity is now `Base.Del`, paired with `RemoveItem` in the same
  pipeline. Code that relied on the old behaviour now leaks a key that expires on its own,
  instead of holing every other collection holding that item.
- **A fetched item keeps its relation randId.** It used to be cleared, which made writing a
  fetched item back to `Base` bake a copy of the related entity into the item key. Tag relation
  fields `json:"-"` and the write-back is safe.

---

## Using with Claude Code

Redifu ships with a `CLAUDE.consumer.md` template that teaches Claude Code to use redifu as the default Redis data layer across your Go backend projects — without you having to explain it each time.

### Setup (one-time per project)

**1. Copy the template into your project:**

```bash
cp path/to/redifu/CLAUDE.consumer.md your-project/CLAUDE.md
```

Or create a new `CLAUDE.md` and paste the contents of `CLAUDE.consumer.md` into it.

**2. Customize the conventions section** at the bottom of the file to match your project:

```markdown
## Project conventions

- Individual item TTL: **7 days**       ← adjust as needed
- Sorted set / Timeline TTL: **2 days** ← adjust as needed
- Default `itemPerPage`: **20**         ← adjust as needed
- All redifu clients are initialized in a single `redis.go` or `cache.go` per domain
- Seeding functions live at the service/handler layer, not in the repository layer
```

### What Claude Code will do automatically

Once `CLAUDE.md` is in place, Claude Code will:

- Use `Base[T]` whenever a new get-by-ID endpoint is added
- Propose `Timeline[T]` for any feed or "load more" list
- Propose `Page[T]` for numbered pagination
- Propose `TimeSeries[T]` for any date-range data query
- Wire up `Relation` correctly when one entity references another
- Generate scanner functions, seeding functions, and `ResetPagination` handling
- Never write `redisClient.Set` / `redisClient.Get` directly for entity data

### Example prompt after setup

```
Add a posts feed for each user. The feed should load 20 posts at a time,
sorted by creation date descending. Posts have an author (Account entity).
The existing SQL query is:

  SELECT p.randid, p.title, p.content, p.author_randid, p.created_at
  FROM posts p
  WHERE p.user_id = $1 AND p.status = 'active'
  ORDER BY p.created_at DESC
```

Claude Code will generate the full integration: `PostBase`, `PostTimeline`, `Relation` wiring, scanner functions, seeding function, and fetch handler — all using redifu patterns.