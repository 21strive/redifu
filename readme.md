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
postBase, err := redifu.NewBase[*Post](redisClient, "post:%s", 7*24*time.Hour)
if err != nil {
    log.Fatal(err) // the key format is validated here
}

// Set
postBase.Set(ctx, post)

// Get — relations registered on this Base are resolved, and the read extends the TTL
post, err := postBase.Get(ctx, randId)
if errors.Is(err, redifu.ErrNotFound) { ... }

// Exists
present, err := postBase.Exists(ctx, randId)

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

// Fetch the page after a position cursor — the zero Cursor reads page one
cursor, err := redifu.ParseCursor(r.URL.Query().Get("cursor")) // "" → zero Cursor
output   := postTimeline.FetchFrom(cursor).WithParams(userRandId).Exec(ctx)
items    := output.Items()
next     := output.NextCursor().String() // send to client, it comes back as ?cursor=
hasMore  := output.HasMore()             // read from the index — drive "load more" from this

// Remove item
postTimeline.RemoveItem(ctx, post, userRandId)
```

A `Cursor` is a position — the score and member of the last item the client saw — and the
page after it exists whether or not that item is still in the index. Deleting an item, or
moving it to another status, never sends a client back to page one.

The older randId cursor still works: `Fetch(lastRandIds)` reads the page after the newest of
those randIds still in the index, and returns `ErrResetPagination` when none of them is.
`Timeline.Score(item)` gives the score the index stores for an item, for building cursors
and database keysets that agree with it.

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

// A page of that range: ZRANGEBYSCORE ... LIMIT 20 20
items, err := commentSorted.Fetch(redifu.Ascending).
    WithParams(postRandId).
    WithRange(lowerUnixMilli, upperUnixMilli).
    WithLimit(20, 20).
    Exec(ctx)

// By rank: the ten newest
items, err := commentSorted.Fetch(redifu.Descending).
    WithParams(postRandId).
    WithRankRange(0, 9).
    Exec(ctx)

// ExecReport also counts the members whose item had expired out of Base — Exec drops
// them silently, so a list that must be whole reseeds when the count is not zero
items, dangling, err := commentSorted.Fetch(redifu.Ascending).WithParams(postRandId).ExecReport(ctx)
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

A series sorted by an `int64` column has to be told what the column counts —
`SetScoreUnit(time.Second)` for Unix seconds; milliseconds is the default.

---

## Relation — Singleton Behavior

Relations prevent data duplication across entities that reference each other.

**Entity shape (two fields per relation):**

```go
type Post struct {
    *item.Foundation
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
postBase.AddRelation(authorRelation)
```

Register the relation on the **`Base`**, not on one index: it then resolves on every read of a
`Post`, including `Base.Get` for a single item, and on any entity that relates to `Post` in turn.
`index.AddRelation` still exists for relations that only make sense for that one index.

Both accessors are ordinary functions, so renaming a field breaks the build here rather than
resolving to nothing at runtime. Relations are resolved per relation for the whole page, so a
related key shared by twenty items is read once, and they nest: `Post → Author → Org` comes back
whole, one batched round-trip per level.

`Relate` refuses a relation whose field is not tagged `json:"-"` (`ErrRelationNotTransient`).
Without the tag, writing a fetched item back to `Base` bakes a copy of the related entity into
the item's own key and the singleton is broken permanently — so it is checked at startup rather
than left to a comment.

---

## Seeding

Redifu never touches your database. When a collection has no data, you run your own SQL and
feed the rows in through redifu's public methods.

A Timeline holds a prefix of the table — the head, then whatever scrolling has asked for — and
is topped up from its tail whenever a page runs past it:

```go
type FeedPage struct {
    Posts   []*Post
    Next    redifu.Cursor // Next.String() goes to the client and comes back as its cursor
    HasMore bool
}

func GetFeed(ctx context.Context, userRandId string, cursor redifu.Cursor) (*FeedPage, error) {
    perPage := postTimeline.GetItemPerPage()

    output := postTimeline.FetchFrom(cursor).WithParams(userRandId).Exec(ctx)
    for topUps := 0; output.Error() == nil && !output.HasMore(); topUps++ {
        end, err := postTimeline.ReachedEnd(ctx, userRandId) // the index already holds the end
        if err != nil {
            return nil, err
        }
        if end {
            break
        }
        if topUps == 3 { // capped: the client asks again and the next request carries on
            return &FeedPage{output.Items(), output.NextCursor(), true}, nil
        }
        tail, _, err := postTimeline.Tail(ctx, userRandId) // the zero Cursor on a cold index
        if err != nil {
            return nil, err
        }
        if err := seedFeed(ctx, userRandId, tail, perPage+1-int64(len(output.Items()))); err != nil {
            return nil, err
        }
        output = postTimeline.FetchFrom(cursor).WithParams(userRandId).Exec(ctx)
    }
    if output.Error() != nil {
        return nil, output.Error()
    }
    return &FeedPage{output.Items(), output.NextCursor(), output.HasMore()}, nil
}

// seedFeed loads up to limit posts that follow `after` — the newest posts for the zero
// Cursor — keyed on exactly what the index sorts by: milliseconds, then randid byte order.
func seedFeed(ctx context.Context, userRandId string, after redifu.Cursor, limit int64) error {
    query, args := `
        SELECT p.randid, p.title, p.author_randid, p.created_at
        FROM posts p
        WHERE p.user_id = $1`, []interface{}{userRandId}
    if !after.IsZero() {
        query += ` AND (date_trunc('milliseconds', p.created_at), p.randid COLLATE "C") < ($2, $3)`
        args = append(args, time.UnixMilli(int64(after.Score)).UTC(), after.Member)
    }
    query += fmt.Sprintf(` ORDER BY date_trunc('milliseconds', p.created_at) DESC, p.randid COLLATE "C" DESC LIMIT %d`, limit)

    rows, err := db.QueryContext(ctx, query, args...)
    if err != nil {
        return err
    }
    defer rows.Close()

    pipe := redisClient.Pipeline()
    if after.IsZero() {
        postTimeline.UnmarkLastPage(ctx, pipe, userRandId) // a head seed starts the index over
    }
    var count int64
    for rows.Next() {
        p := &Post{Record: &redifu.Record{}}
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

    switch {
    case after.IsZero() && count == 0:
        postTimeline.MarkEmpty(ctx, pipe, userRandId)
    case count < limit:
        postTimeline.MarkLastPage(ctx, pipe, userRandId) // the index reaches the end of the table
    }
    if after.IsZero() {
        postTimeline.SetExpiration(ctx, pipe, userRandId)
    }

    _, err = pipe.Exec(ctx)
    return err
}
```

One pipeline, one round trip per seed; the first walk of a feed loads each row once and every
walk after it is served from Redis. `CLAUDE.consumer.md` explains each step, and carries the
pattern for `Sorted`, `Page` and `TimeSeries` and for clients that still send randId cursors.

---

## Default TTL

| | TTL |
|--|-----|
| Individual item (`Base`) | 7 days |
| Sorted set / Timeline | 2 days |

TTL is configurable at initialization.

## Limitations

- Sorting only supports fields of type `time.Time`, `*time.Time` or `int64`. An `int64` beyond
  2^53 is rejected rather than silently losing precision, so snowflake ids cannot be scores.
- Seeding is yours to write — redifu has no SQL layer, no query builder and no database dependency.
- Relations nest up to `DefaultRelationDepth` (4) levels; beyond that, resolution stops rather
  than failing, so a self-referential relation terminates.
- Reading an item extends its TTL with `GETEX`, which needs Redis 6.2. Call
  `base.SetTouchOnRead(false)` on anything older.
- Collection keys carry a Redis Cluster hash tag (`{feed:u1}`); item keys do not.
- A relation points at one entity. A parent that owns a list (a booking's payment terms) keeps
  an index per parent — see [`docs/one-to-many-relations.md`](docs/one-to-many-relations.md).

---

## Worked example

[`examples/feed/`](examples/feed/) is a complete consumer: three related entities, the wiring,
a seeder that reads them out of Postgres with one JOIN, and the read and write handlers on top.
It is part of `go build ./...` and has its own tests, so it cannot drift away from the API.

| File | |
|---|---|
| [`entity.go`](examples/feed/entity.go) | the entities and their `randId` / `json:"-"` field pairs |
| [`store.go`](examples/feed/store.go) | constructors, relations, sorting reference |
| [`seed.go`](examples/feed/seed.go) | the SQL, the row scan, and the seeding pipeline |
| [`handler.go`](examples/feed/handler.go) | feed fetch and single-item read |
| [`write.go`](examples/feed/write.go) | create, fan-out, update, delete |

## Upgrading

Current release: see [`MIGRATION.md`](MIGRATION.md) — constructors return errors, relations move
onto `Base` and nest, `json:"-"` is enforced, ingest is a single atomic script, and pagination no
longer mistakes an expired item for the end of a feed. A Timeline is read by position cursor
and grows by top-up from its tail (sections 15 and 16).

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