# CLAUDE.md — Go Backend Project

This project uses **redifu** (`github.com/21strive/redifu`) as the standard Redis data layer.

Whenever adding a new entity, endpoint, or caching layer — use redifu. Do not implement Redis manually (no direct SET/GET calls).

---

## Available redifu structures

| Struct | When to use |
|--------|-------------|
| `redifu.Base[T]` | Get/set a single item by randId |
| `redifu.Sorted[T]` | Full ordered collection, or query by score range |
| `redifu.Timeline[T]` | "Load more" feed / infinite scroll |
| `redifu.Page[T]` | Numbered page pagination |
| `redifu.TimeSeries[T]` | Query data by time range |

### How the structures relate

`Base[T]` is the single source of truth for every item. **Every collection structure
(`Timeline`, `Sorted`, `Page`, `TimeSeries`) is backed by a `Base[T]`** — the collection's
sorted set stores only `randId`s, and each fetch resolves those ids back through `Base[T]`.

```
Base[T]  ←── the item store, used by ALL of the below
  ├── Timeline[T]    cursor pagination (randId cursor)
  ├── Sorted[T]      full collection in one fetch
  ├── Page[T]        numbered pages
  └── TimeSeries[T]  ordered by time range
```

You always construct a `Base[T]` first, then pass it into whichever collection you need.

### What each structure is for

- **`Timeline[T]`** — pagination in *timeline form* (infinite scroll / "load more"). It paginates
  with a **`randId` cursor**, not page numbers: each page hands back a `validLastId` the client
  sends to fetch the next page. This is the only structure whose seeder takes a
  **`subtraction`** parameter (see below) — because it stitches partial pages together as the
  user scrolls.

- **`Sorted[T]`** — best for lists that are **near-finite and rarely change**, because a `Sorted`
  fetch returns the **entire collection in one go** (no pagination). Think leaderboards,
  category lists, a user's pinned items. It can also fetch by **score range** (`WithRange`).

- **`Page[T]`** — pagination by **page number** (page 1, 2, 3). Its one limitation: it does
  **not** support dynamically adding or removing items into an existing page. Pages are an
  immutable snapshot once seeded — to reflect a change you `Purge` and re-seed.

- **`TimeSeries[T]`** — displays data within a **time range**, in order. Fetching is driven by a
  `from`/`to` range, and it only seeds the time segments that are missing (gap detection).
  Use it for charts, history, and reports. Direction is always `Descending`.

### Choosing the right structure

Ask these questions before deciding:

1. Only need to get/set a single item? → `Base[T]`
2. User scrolls a feed without page numbers? → `Timeline[T]`
3. User selects "page 1, 2, 3"? → `Page[T]`
4. Data is queried by date range (charts, reports)? → `TimeSeries[T]`
5. Collection always fetched in full, or by score range? → `Sorted[T]`

### TTL rule: Base must outlive its collection

**Always give `Base[T]` a TTL longer than the sorted set TTL of any collection that uses it.**

A collection's sorted set stores only `randId`s; the actual item data lives in `Base`. If `Base`
expired first, the sorted set would still hold ids that no longer resolve to anything — a fetch
would return empty/broken items. Keeping `Base` alive longer guarantees that as long as a
`randId` is still indexed, its item is still retrievable. The project defaults below follow this
rule: **Base = 7 days, sorted set = 2 days.**

---

## Standard pattern: adding a new entity

### 1. Entity struct

Entities must implement `item.Blueprint` (from `github.com/21strive/item`).

**Entities are always used as pointer types** — `Base[*Post]`, never `Base[Post]`. A Relation
writes the related entity into your struct, and that is only possible through a pointer.

```go
type Post struct {
    redifu.Record                              // embed for SQLItemBlueprint
    Title        string
    Content      string

    AuthorRandId string                        // the pointer — this is what Redis stores
    Author       *account.Account `json:"-"`   // filled on fetch, never serialized
}
```

Two rules for a related entity:

- **keep its `RandId` field** — that is the only thing stored in Redis, and it is what makes
  the related entity a singleton;
- **tag the entity field `json:"-"`** — so an item returned by `Fetch` can be written straight
  back to `Base` without baking a flattened copy of the related entity into the key.

Field names are free. Nothing is inferred from them — you name both fields explicitly when you
declare the Relation.

### 2. Init redifu clients

```go
const (
    postKeyFormat     = "post:%s"
    postFeedKeyFormat = "feed:user:%s:posts"
    postTTL           = 7 * 24 * time.Hour
    postItemPerPage   = 20
)

var (
    PostBase     *redifu.Base[*Post]
    PostTimeline *redifu.Timeline[*Post]
)

func InitRedis(redisClient redis.UniversalClient) {
    PostBase = redifu.NewBase[*Post](redisClient, postKeyFormat, postTTL)

    PostTimeline = redifu.NewTimeline[*Post](
        redisClient,
        PostBase,
        postFeedKeyFormat,
        postItemPerPage,
        redifu.Descending, // direction: Ascending or Descending (Timeline, Sorted, Page only — TimeSeries is always Descending)
        2*24*time.Hour,
    )

    // sortingReference: the struct field name used as the sorted set score.
    // Default (if not set) is createdAt via GetCreatedAt().
    // Set this when sorting by a field other than createdAt.
    // Supported field types: time.Time, *time.Time, int64.
    // If set, the ORDER BY column in seeder SQL must match this field.
    // PostTimeline.SetSortingReference("UpdatedAt")

    // Add a Relation if Post references another entity.
    // You supply two functions: where the randId lives, and where the entity goes.
    authorRelation, err := redifu.Relate(account.AccountBase,
        func(p *Post) string                  { return p.AuthorRandId },
        func(p *Post, a *account.Account)     { p.Author = a },
    )
    if err != nil {
        log.Fatalf("redifu relation error: %v", err)
    }
    PostTimeline.AddRelation(authorRelation)
}
```

### 3. Scanner functions

Redifu does not define a scanner type — these are your own helpers, shared by every seeding
function for the entity. Two per entity is the usual shape:

```go
// single row — used to read a Timeline cursor reference
func scanPostRow(row *sql.Row) (*Post, error) {
    p := &Post{}
    err := row.Scan(&p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt)
    return p, err
}

// one row out of many — used in the seeding loop
func scanPostRows(rows *sql.Rows) (*Post, error) {
    p := &Post{}
    err := rows.Scan(&p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt)
    return p, err
}
```

Scan into `AuthorRandId`, never into `Author` — the Relation fills that at fetch time.

---

## Relation — one entity, stored once

> Full guide, worked seeding/fetch examples and the migration table:
> [`docs/plan-2026-08-30/01-relation.md`](docs/plan-2026-08-30/01-relation.md).

A Relation is how an entity that appears inside many other entities is stored **once** and
still shows up everywhere it is referenced.

Without it, an `Author` embedded in 500 posts is 500 copies: renaming the author means
rewriting 500 keys, and any copy you miss stays wrong until its TTL runs out. With it,
`author:a7` is one key. Update that key and all 500 posts reflect it on the next fetch —
no list is rebuilt, nothing is invalidated, no DB query is issued.

### Declaring one

```go
authorRelation, err := redifu.Relate(account.AccountBase,
    func(p *Post) string              { return p.AuthorRandId },  // where the randId lives
    func(p *Post, a *account.Account) { p.Author = a },            // where the entity goes
)
if err != nil {
    return err
}
PostTimeline.AddRelation(authorRelation)
```

Three arguments:

| Argument | Meaning |
|---|---|
| `account.AccountBase` | the `Base` the related entity is fetched from |
| `func(p *Post) string` | which field on `Post` holds the related randId |
| `func(p *Post, a *Account)` | which field on `Post` the fetched entity is written into |

These are ordinary Go functions, so the compiler checks them. Rename `AuthorRandId` and the
build fails at the Relation declaration — it cannot silently resolve to nothing at runtime.

`Relate` returns an error if the item type is not a pointer (`Base[Post]` instead of
`Base[*Post]`). Handle it at startup; it is a wiring mistake, not a runtime condition.

Register as many as the entity needs:

```go
PostTimeline.AddRelation(authorRelation, categoryRelation, brandRelation)
```

### What happens on fetch

Relations are resolved **per relation, in batch** — not per item. Fetching 5 posts that
share 2 authors:

```
ZREVRANGE feed:user:u1:posts 0 4
PIPELINE { post:p1, post:p2, post:p3, post:p4, post:p5 }
  → read AuthorRandId from all 5 → [a7, a7, a7, a9, a9] → dedupe → [a7, a9]
PIPELINE { author:a7, author:a9 }
  → write each author into its posts
```

Three round trips, and `author:a7` is fetched once no matter how many posts use it. Adding a
second Relation adds no round trip — all relations share one pipeline.

Every item comes back fully resolved:

```go
output := PostTimeline.Fetch(lastRandIds).WithParams(userRandId).Exec(ctx)
for _, p := range output.Items() {
    fmt.Println(p.Title, p.Author.Name)  // Author is populated
}
```

### The randId is never cleared

A fetched `Post` carries **both** `Author` (populated) and `AuthorRandId` (still set). That is
deliberate — it makes the result safe to write back:

```go
p := output.Items()[0]
p.Title = "new title"
PostBase.Set(ctx, p)     // safe: Author is json:"-", AuthorRandId survives
```

Because `Author` is tagged `json:"-"`, what lands in Redis is still just the pointer. Drop the
tag and this write bakes a full copy of the author into `post:p1`, permanently breaking the
singleton for that post.

### Updating a related entity

Write the entity's own `Base`. Nothing else.

```go
author.Name = "New Name"
err := account.AccountBase.Set(ctx, author)
```

Every timeline, page, sorted set and time series that references this author now reflects the
change. Do **not** purge or re-seed the lists — there is nothing stale in them.

### When an item points at a different entity

If a post moves to a different author, update the post itself:

```go
post.AuthorRandId = newAuthorRandId
err := PostBase.Set(ctx, post)
```

One key, one write. The randId inside the item is the only pointer that exists, so no list has
to be found or repaired.

The exception is a list **keyed by** that relation — `posts:by-author:a7`. There, the change is
a membership change, not just a pointer change, and you handle it explicitly:

```go
err = OldAuthorTimeline.RemoveItem(ctx, post, oldAuthorRandId)
err = NewAuthorTimeline.AddItem(ctx, post, newAuthorRandId)
```

### When the related entity is missing

If `author:a7` has expired or was evicted, that post comes back with `Author == nil` while the
rest of the fetch succeeds. Guard for it in handlers and processors:

```go
if p.Author != nil {
    resp.AuthorName = p.Author.Name
}
```

This is why `Base` TTL must outlive the sorted set TTL — see the TTL rule above. It applies to
the *related* entity's `Base` too: an `Account` that expires sooner than the posts referencing
it produces exactly this hole.

---

## Seeding — you write the SQL

**Redifu never touches your database.** There is no seeder type and no query builder — you
write plain SQL, scan the rows, and feed the results in through redifu's public methods.

Every seeding function has the same four steps:

1. build and run your query;
2. for each row: `Base.WithPipeline(pipe).Set` then `IngestItem(..., seed=true, ...)`;
3. set the state markers the collection needs;
4. `pipe.Exec(ctx)` — once, at the end.

Everything before step 4 only enqueues, so a whole page of seeding costs one round trip.

### Timeline

The most involved of the four, because it carries a cursor. Use it as the template:

```go
func seedPostFeed(ctx context.Context, userRandId string, subtraction int64, lastRandId string) error {
    var query string
    args := []interface{}{userRandId}

    if lastRandId == "" {
        // First page
        query = `
            SELECT p.randid, p.title, p.content, p.author_randid, p.created_at
            FROM post_view p
            WHERE p.user_id = $1
            ORDER BY p.created_at DESC`
    } else {
        // Find cursor reference to get its score
        var cursorTime time.Time
        row := db.QueryRowContext(ctx, `SELECT created_at FROM posts WHERE randid = $1`, lastRandId)
        if err := row.Scan(&cursorTime); err != nil {
            return err
        }
        query = `
            SELECT p.randid, p.title, p.content, p.author_randid, p.created_at
            FROM post_view p
            WHERE p.user_id = $1 AND p.created_at < $2
            ORDER BY p.created_at DESC`
        args = append(args, cursorTime)
    }

    limit := PostTimeline.GetItemPerPage() - subtraction
    query += fmt.Sprintf(" LIMIT %d", limit)

    rows, err := db.QueryContext(ctx, query, args...)
    if err != nil {
        return err
    }
    defer rows.Close()

    pipe := redisClient.Pipeline()
    var count int64

    for rows.Next() {
        item, err := scanPostRows(rows)
        if err != nil {
            return err
        }
        PostBase.WithPipeline(pipe).Set(ctx, item)
        PostTimeline.IngestItem(ctx, pipe, item, true, userRandId)
        count++
    }
    if err = rows.Err(); err != nil {
        return err
    }

    isFirstPage := lastRandId == ""

    if isFirstPage && count == 0 {
        PostTimeline.MarkEmpty(ctx, pipe, userRandId)
    } else if isFirstPage && count < PostTimeline.GetItemPerPage() {
        PostTimeline.MarkFirstPage(ctx, pipe, userRandId)
    } else if !isFirstPage && subtraction+count < PostTimeline.GetItemPerPage() {
        PostTimeline.MarkLastPage(ctx, pipe, userRandId)
    }

    if isFirstPage {
        PostTimeline.SetExpiration(ctx, pipe, userRandId)
    }

    _, err = pipe.Exec(ctx)
    return err
}
```

---

## Standard pattern: Timeline fetch handler

```go
func GetPostFeed(ctx context.Context, userRandId string, lastRandIds []string) ([]Post, string, string, error) {
    needsSeed, err := PostTimeline.RequiresSeeding(ctx, int64(len(lastRandIds)), userRandId)
    if err != nil {
        return nil, "", "", err
    }

    if needsSeed {
        if err := seedPostFeed(ctx, userRandId, 0, ""); err != nil {
            return nil, "", "", err
        }
    }

    output := PostTimeline.Fetch(lastRandIds).WithParams(userRandId).Exec(ctx)

    // ResetPagination: the client had a cursor (lastRandIds is non-empty) but the
    // sorted set expired mid-pagination. ZCard returned 0 on a set that previously
    // existed. Discard the cursor and re-seed from the first page.
    if errors.Is(output.Error(), redifu.ResetPagination) {
        if err := seedPostFeed(ctx, userRandId, 0, ""); err != nil {
            return nil, "", "", err
        }
        output = PostTimeline.Fetch(nil).WithParams(userRandId).Exec(ctx)
    }

    if output.Error() != nil {
        return nil, "", "", output.Error()
    }

    return output.Items(), output.ValidLastId(), output.Position(), nil
}
```

### The `subtraction` parameter

**`subtraction` is a Timeline-only concept** — `Sorted`, `Page` and `TimeSeries` never stitch a
partial page across a cursor, so they don't need it.

`subtraction` is the number of items already present in Redis for the page being seeded. You
subtract it from your SQL `LIMIT` so the total reaches exactly `itemPerPage`:

```
itemPerPage = 20, items already in Redis = 5 → subtraction = 5
LIMIT 15   (itemPerPage - subtraction)
```

Pass `subtraction = 0` on a first-page seed or when the sorted set is empty.

---

## Standard pattern: Sorted fetch handler

`Sorted` has no pagination — `RequiresSeeding` takes only key params, and `Fetch` returns the
**entire collection** in the direction you pass. Use it for near-finite lists (leaderboards,
category lists) that you always show in full.

```go
const sortedSetTTL = 2 * 24 * time.Hour

var (
    ScoreBase   *redifu.Base[*Score]
    ScoreSorted *redifu.Sorted[*Score]
)

func InitScore(redisClient redis.UniversalClient) {
    ScoreBase = redifu.NewBase[*Score](redisClient, "score:%s", 7*24*time.Hour)
    // Sorted takes no direction at construction — direction is passed at Fetch time.
    ScoreSorted = redifu.NewSorted[*Score](redisClient, ScoreBase, "leaderboard:%s", sortedSetTTL)
}

// Sorted seeder needs only a RowsScanner — there is no cursor reference to fetch.
func seedLeaderboard(ctx context.Context, seasonRandId string) error {
    rows, err := db.QueryContext(ctx, `
        SELECT s.randid, s.season_randid, s.player_randid, s.points, s.created_at
        FROM scores s
        WHERE s.season_randid = $1
        ORDER BY s.points DESC`, seasonRandId)
    if err != nil {
        return err
    }
    defer rows.Close()

    pipe := redisClient.Pipeline()
    var count int64

    for rows.Next() {
        item, err := scanScoreRows(rows)
        if err != nil {
            return err
        }
        ScoreBase.WithPipeline(pipe).Set(ctx, item)
        ScoreSorted.IngestItem(ctx, pipe, item, true, seasonRandId)
        count++
    }
    if err = rows.Err(); err != nil {
        return err
    }

    if count == 0 {
        ScoreSorted.MarkEmpty(ctx, pipe, seasonRandId)
    }
    ScoreSorted.SetExpiration(ctx, pipe, seasonRandId)

    _, err = pipe.Exec(ctx)
    return err
}

func GetLeaderboard(ctx context.Context, seasonRandId string) ([]*Score, error) {
    needsSeed, err := ScoreSorted.RequiresSeeding(ctx, seasonRandId)
    if err != nil {
        return nil, err
    }
    if needsSeed {
        if err := seedLeaderboard(ctx, seasonRandId); err != nil {
            return nil, err
        }
    }

    // Direction is supplied here, not at construction. Returns the full collection.
    return ScoreSorted.Fetch(redifu.Descending).
        WithParams(seasonRandId).
        Exec(ctx)
}
```

Fetch by **score range** instead of the full set with `WithRange(lower, upper)`:

```go
items, err := ScoreSorted.Fetch(redifu.Descending).
    WithParams(seasonRandId).
    WithRange(1000, 5000). // only items whose score is between 1000 and 5000
    Exec(ctx)
```

---

## Standard pattern: Page fetch handler

`Page` paginates by page number. `RequiresSeeding` and `Fetch` both take the page number. The
seeder derives `LIMIT`/`OFFSET` from the page and `itemPerPage` automatically.

> **Limitation:** `Page` does not support adding or removing items into an already-seeded page.
> A page is an immutable snapshot. To reflect inserts/deletes, `Purge` the index and re-seed.

```go
var (
    CommentBase *redifu.Base[*Comment]
    CommentPage *redifu.Page[*Comment]
)

func InitComment(redisClient redis.UniversalClient) {
    CommentBase = redifu.NewBase[*Comment](redisClient, "comment:%s", 7*24*time.Hour)
    CommentPage = redifu.NewPage[*Comment](
        redisClient,
        CommentBase,
        "post:%s:comments",
        20,               // itemPerPage
        redifu.Ascending, // direction
        2*24*time.Hour,
    )
}

func seedCommentPage(ctx context.Context, postRandId string, page int64) error {
    limit := CommentPage.GetItemPerPage()
    offset := (page - 1) * limit

    rows, err := db.QueryContext(ctx, `
        SELECT c.randid, c.post_randid, c.body, c.created_at
        FROM comments c
        WHERE c.post_randid = $1
        ORDER BY c.created_at ASC
        LIMIT $2 OFFSET $3`, postRandId, limit, offset)
    if err != nil {
        return err
    }
    defer rows.Close()

    pipe := redisClient.Pipeline()
    var count int64

    for rows.Next() {
        item, err := scanCommentRows(rows)
        if err != nil {
            return err
        }
        CommentBase.WithPipeline(pipe).Set(ctx, item)
        CommentPage.IngestItem(ctx, pipe, item, page, postRandId)
        count++
    }
    if err = rows.Err(); err != nil {
        return err
    }

    if count == 0 {
        CommentPage.MarkEmpty(ctx, pipe, page, postRandId)
    } else {
        CommentPage.AddPage(ctx, pipe, page, postRandId) // register in the page index
    }
    CommentPage.SetExpiration(ctx, pipe, page, postRandId)

    _, err = pipe.Exec(ctx)
    return err
}

func GetCommentPage(ctx context.Context, postRandId string, page int64) ([]*Comment, error) {
    needsSeed, err := CommentPage.RequiresSeeding(ctx, page, postRandId)
    if err != nil {
        return nil, err
    }
    if needsSeed {
        if err := seedCommentPage(ctx, postRandId, page); err != nil {
            return nil, err
        }
    }

    return CommentPage.Fetch(page).
        WithParams(postRandId).
        Exec(ctx)
}

// On insert/delete of a comment, the page index is stale — purge and let the next
// GetCommentPage re-seed.
func InvalidateComments(ctx context.Context, postRandId string) error {
    return CommentPage.Purge(ctx, postRandId)
}
```

`LIMIT` / `OFFSET` are yours to compute — `GetItemPerPage()` returns the page size the
collection was constructed with.

---

## Standard pattern: TimeSeries fetch handler

TimeSeries seeds only the segments missing from the requested range (gap detection).
The flow is always: **fetch → if gap exists → seed the gap → fetch again**.

`TimeSeries` is always `Descending` and takes no direction at construction. `FindGap` tells you
which sub-ranges are missing; you query each one and record it with `AddSegment`.

```go
var (
    TxBase       *redifu.Base[*Transaction]
    TxTimeSeries *redifu.TimeSeries[*Transaction]
)

func InitTransaction(redisClient redis.UniversalClient) {
    TxBase = redifu.NewBase[*Transaction](redisClient, "tx:%s", 7*24*time.Hour)
    TxTimeSeries = redifu.NewTimeSeries[*Transaction](redisClient, TxBase, "tx:account:%s", 2*24*time.Hour)
}

func seedTransactions(ctx context.Context, accountRandId string, from, to time.Time) error {
    gaps, err := TxTimeSeries.FindGap(ctx, from, to, accountRandId)
    if err != nil {
        return err
    }

    for _, gap := range gaps { // [][]int64 — unix millis pairs
        lower := time.UnixMilli(gap[0]).UTC()
        upper := time.UnixMilli(gap[1]).UTC()

        rows, err := db.QueryContext(ctx, `
            SELECT t.randid, t.account_randid, t.amount, t.created_at
            FROM transactions t
            WHERE t.account_randid = $1 AND t.created_at BETWEEN $2 AND $3
            ORDER BY t.created_at DESC`, accountRandId, lower, upper)
        if err != nil {
            return err
        }

        pipe := redisClient.Pipeline()
        alreadySeeded := TxTimeSeries.Count(ctx, accountRandId) > 0
        var count int64

        for rows.Next() {
            item, err := scanTxRows(rows)
            if err != nil {
                rows.Close()
                return err
            }
            TxBase.WithPipeline(pipe).Set(ctx, item)
            TxTimeSeries.IngestItem(ctx, pipe, item, accountRandId)
            count++
        }
        if err = rows.Err(); err != nil {
            rows.Close()
            return err
        }
        rows.Close()

        // TTL is applied once, when the first segment lands
        if count > 0 && !alreadySeeded {
            TxTimeSeries.SetExpiration(ctx, pipe, accountRandId)
        }

        // record the range as seeded even when it returned nothing,
        // otherwise it is re-queried on every request
        TxTimeSeries.AddSegment(ctx, pipe, lower, upper, accountRandId)

        if _, err := pipe.Exec(ctx); err != nil {
            return err
        }
    }
    return nil
}

func GetTransactions(ctx context.Context, accountRandId string, from, to time.Time) ([]*Transaction, error) {
    items, needsSeed, err := TxTimeSeries.
        Fetch(from, to).
        WithParams(accountRandId).
        Exec(ctx)
    if err != nil {
        return nil, err
    }

    if needsSeed {
        if err := seedTransactions(ctx, accountRandId, from, to); err != nil {
            return nil, err
        }

        // Re-fetch after seeding — the gap is now filled
        items, _, err = TxTimeSeries.
            Fetch(from, to).
            WithParams(accountRandId).
            Exec(ctx)
        if err != nil {
            return nil, err
        }
    }

    return items, nil
}
```

---

## Standard pattern: Add / Update / Remove

```go
// Add a new item to the timeline. AddItem places the item into the index: it writes the
// item only if Base does not hold it yet, but always refreshes its TTL.
err = PostTimeline.AddItem(ctx, newPost, userRandId)

// Update an item (update Base only — Relations reflect automatically).
// AddItem will not do this for you.
err = PostBase.Set(ctx, updatedPost)

// Remove an item from this timeline. The item stays in Base and in every other
// collection that holds it.
err = PostTimeline.RemoveItem(ctx, post, userRandId)

// Delete the entity itself, because it is gone from the database. Any other index still
// holding its randId comes back short until it is re-seeded or purged.
pipe := redisClient.Pipeline()
err = PostTimeline.WithPipeline(pipe).RemoveItem(ctx, post, userRandId)
err = PostBase.WithPipeline(pipe).Del(ctx, post)
_, err = pipe.Exec(ctx)

// Invalidate a whole collection so the next fetch seeds it again from the database.
// Item keys are left alone.
err = PostTimeline.Purge(ctx, userRandId)
```

---

## WithProcessor — post-fetch transformation

`WithProcessor` is called on each item after Relations are resolved, before the items are returned. Use it to transform field values, strip sensitive data, or compute derived fields.

Because Relations run first, `p.Author` is already populated here — but it may be `nil` if the related entity was missing, so guard before dereferencing it.

```go
func sanitizePost(p *Post, args []interface{}) {
    p.InternalToken = "" // strip before sending as response
}

output := PostTimeline.
    Fetch(lastRandIds).
    WithParams(userRandId).
    WithProcessor(sanitizePost).
    Exec(ctx)
```

With arguments:

```go
func applyUserContext(p *Post, args []interface{}) {
    viewerRandId := args[0].(string)
    p.IsLikedByViewer = checkLike(p.RandId, viewerRandId)
}

output := PostTimeline.
    Fetch(lastRandIds).
    WithParams(userRandId).
    WithProcessor(applyUserContext, viewerRandId).
    Exec(ctx)
```

---

## Seeding a related entity in the same pass

When your seeding query JOINs the related entity, warm its `Base` from the same row and the
same pipeline. Subsequent fetches then resolve the Relation from Redis without touching the DB.

```go
rows, err := db.QueryContext(ctx, `
    SELECT p.randid, p.title, p.content, p.author_randid, p.created_at,
           a.randid, a.name
    FROM posts p
    JOIN accounts a ON a.randid = p.author_randid
    WHERE p.user_id = $1
    ORDER BY p.created_at DESC`, userRandId)
if err != nil {
    return err
}
defer rows.Close()

pipe := redisClient.Pipeline()

for rows.Next() {
    p := &Post{}
    author := &account.Account{}
    if err := rows.Scan(
        &p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt,
        &author.RandId, &author.Name,
    ); err != nil {
        return err
    }

    PostBase.WithPipeline(pipe).Set(ctx, p)
    PostTimeline.IngestItem(ctx, pipe, p, true, userRandId)

    // warm the related entity's own Base in the same pipeline
    account.AccountBase.WithPipeline(pipe).Set(ctx, author)
}
```

Scan the JOINed columns into the related entity and write it to **its own** `Base`; scan only
`AuthorRandId` into the post. Never assign `p.Author` here — the Relation does that on fetch.

The same JOINed author appears on many rows, so this re-`SET`s `account:a7` repeatedly. That is
harmless — same key, same value — and it is one pipeline either way. Skip the duplicates with a
local `map[string]bool` if the payload is large.

---

## Project conventions

- Individual item (`Base`) TTL: **7 days**
- Sorted set TTL (Timeline / Sorted / Page / TimeSeries): **2 days**
- **`Base` TTL must always be longer than the sorted set TTL** — the sorted set holds only
  `randId`s and resolves them through `Base`; if `Base` expired first, the index would point
  at items that no longer exist.
- Default `itemPerPage`: **20**
- All redifu clients are initialized in a single `redis.go` or `cache.go` file per domain
- Seeding functions live at the service/handler layer, not in the repository layer

---

## What not to do

- Do not call `redisClient.Set(...)` / `redisClient.Get(...)` directly for entity data — use `Base[T]`
- Do not store a full related object inside a parent entity — keep its randId and declare a Relation
- Do not omit `json:"-"` on a relation field — without it, writing a fetched item back to `Base` bakes a flattened copy of the related entity into the key and breaks the singleton permanently
- Do not clear a relation's randId field after fetching — it is the only pointer that exists
- Do not assign a relation field in a scanner — scanners set the randId, Relations set the entity
- Do not purge or re-seed a list because a related entity changed — write that entity's `Base` and every list reflects it
- Do not use a value item type (`Base[Post]`) with Relations — `Relate` requires `Base[*Post]`
- Do not call `pipe.Exec()` inside a function that receives `pipe` as a parameter
- Do not expect `RemoveItem` or `Purge` to delete an item — they only touch the index; deleting the entity is `Base.Del`, paired with a `RemoveItem` in the same pipeline
- Do not use `AddItem` to update an item's contents — it only places the item into the index (and refreshes its TTL); use `Base.Set`
- Do not ignore `redifu.ResetPagination` — always handle it by discarding the cursor and re-seeding from page one
- Do not give a collection's sorted set a TTL longer than (or equal to) its `Base` TTL — `Base` must outlive the index
- Do not apply `subtraction` to anything but a Timeline seeding function — it is a Timeline-only concept
- Do not call `pipe.Exec` inside your seeding loop — enqueue everything, execute once at the end
- Do not seed a `TimeSeries` range without `AddSegment` — the range will be re-queried on every request
- Do not try to `AddItem`/`RemoveItem` into a `Page` — pages are snapshots; `Purge` and re-seed instead
