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

---

## Choosing the right structure

Ask these questions before deciding:

1. Only need to get/set a single item? → `Base[T]`
2. User scrolls a feed without page numbers? → `Timeline[T]`
3. User selects "page 1, 2, 3"? → `Page[T]`
4. Data is queried by date range (charts, reports)? → `TimeSeries[T]`
5. Collection always fetched in full, or by score range? → `Sorted[T]`

---

## Standard pattern: adding a new entity

### 1. Entity struct

Entities must implement `item.Blueprint` (from `github.com/21strive/item`).
If an entity references another, follow this naming convention:

```go
type Post struct {
    redifu.Record                  // embed for SQLItemBlueprint
    Title         string
    Content       string
    AuthorRandId  string           // reference to Author — always: FieldName + "RandId"
    Author        account.Account  // related entity field
}
```

### 2. Init redifu clients

```go
const (
    postKeyFormat     = "post:%s"
    postFeedKeyFormat = "feed:user:%s:posts"
    postTTL           = 7 * 24 * time.Hour
    postItemPerPage   = 20
)

var (
    PostBase     *redifu.Base[Post]
    PostTimeline *redifu.Timeline[Post]
)

func InitRedis(redisClient redis.UniversalClient) {
    PostBase = redifu.NewBase[Post](redisClient, postKeyFormat, postTTL)

    PostTimeline = redifu.NewTimeline[Post](
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

    // Add a Relation if Post references another entity
    authorRelation, err := redifu.NewRelation[account.Account](account.AccountBase, redifu.TypeOf[Post]())
    if err != nil {
        log.Fatalf("redifu relation error: %v", err)
    }
    PostTimeline.AddRelation("author", authorRelation)
}
```

### 3. Scanner functions

Scanners are required by seeders. Write two functions per entity:

```go
// RowScanner — single row (used by seeder to fetch cursor reference)
func scanPostRow(row *sql.Row) (Post, error) {
    var p Post
    err := row.Scan(&p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt)
    return p, err
}

// RowsScanner — multiple rows (used by seeder to populate Redis)
func scanPostRows(rows *sql.Rows) (Post, error) {
    var p Post
    err := rows.Scan(&p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt)
    return p, err
}
```

---

## Seeder — choosing built-in vs custom

### When to use the built-in seeder

Use `redifu.NewTimelineSeeder` when the query is straightforward: single table or simple joins, standard WHERE + ORDER BY, no views or CTEs.

```go
queryBuilder := redifu.NewQuery("posts", "p").
    Select("p.randid, p.title, p.content, p.author_randid, p.created_at").
    Where("p.user_id", redifu.Equal).
    Where("p.status", redifu.Equal).
    OrderBy("p.created_at", redifu.Descending)

func seedPostFeed(ctx context.Context, userRandId string, subtraction int64, lastRandId string) error {
    seeder := redifu.NewTimelineSeeder[Post](redisClient, db, PostBase, PostTimeline)
    return seeder.
        Seed(subtraction, lastRandId, queryBuilder).
        WithQueryArgs(userRandId, "active").
        WithParams(userRandId).
        Exec(ctx, scanPostRow, scanPostRows)
}
```

### When to use a custom seeder

Use a custom seeder when the query is complex: views, CTEs, subqueries, or anything the built-in `Builder` cannot express.

A custom seeder uses the same public redifu methods (`IngestItem`, `MarkFirstPage`, `MarkLastPage`, `MarkEmpty`, `SetExpiration`) assembled manually:

```go
func seedPostFeedCustom(ctx context.Context, userRandId string, subtraction int64, lastRandId string) error {
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

`subtraction` is the number of items already present in Redis for the page being seeded.
It tells the seeder to fetch fewer rows so the total reaches exactly `itemPerPage`.

```
itemPerPage = 20, items already in Redis = 5 → subtraction = 5
Seeder fetches LIMIT 15 (itemPerPage - subtraction).
```

Pass `subtraction = 0` on a first-page seed or when the sorted set is empty.

---

## Standard pattern: TimeSeries fetch handler

TimeSeries seeds only the segments missing from the requested range (gap detection).
The flow is always: **fetch → if gap exists → seed the gap → fetch again**.

```go
func GetTransactions(ctx context.Context, accountRandId string, from, to time.Time) ([]Transaction, error) {
    items, needsSeed, err := TxTimeSeries.
        Fetch(from, to).
        WithParams(accountRandId).
        Exec(ctx)
    if err != nil {
        return nil, err
    }

    if needsSeed {
        seeder := redifu.NewTimeSeriesSeeder[Transaction](redisClient, db, TxBase, TxTimeSeries)
        err = seeder.
            Seed(txQueryBuilder, from, to).
            WithQueryArgs(accountRandId).
            WithParams(accountRandId).
            Exec(ctx, scanTxRows)
        if err != nil {
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
// Add a new item to the timeline
err = PostTimeline.AddItem(ctx, newPost, userRandId)

// Update an item (update Base only — Relations reflect automatically)
err = PostBase.Set(ctx, updatedPost)

// Remove an item from the timeline and Base
err = PostTimeline.RemoveItem(ctx, post, userRandId)
```

---

## WithProcessor — post-fetch transformation

`WithProcessor` is called on each item after Relations are resolved, before the items are returned. Use it to transform field values, strip sensitive data, or compute derived fields.

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

## ExecWithRelation — seed and store related entities in one pass

Use `ExecWithRelation` instead of `Exec` when the seeder query uses a JOIN and the scanner can populate the related entity directly from the SQL result. The related entity is stored in its own `Base` during seeding, so subsequent fetches resolve it from Redis without hitting the DB.

```go
func scanPostWithAuthorRows(ctx context.Context, rows *sql.Rows, relation map[string]Relation) (Post, error) {
    var p Post
    var author account.Account
    err := rows.Scan(
        &p.RandId, &p.Title, &p.Content, &p.AuthorRandId, &p.CreatedAt,
        &author.RandId, &author.Name,
    )
    if err != nil {
        return p, err
    }

    // Store the related entity in its own Base during seed
    if rel, ok := relation["author"]; ok {
        rel.SetItem(ctx, author)
    }

    return p, nil
}

// Use ExecWithRelation instead of Exec
err = seeder.
    Seed(0, "", queryBuilder).
    WithQueryArgs(userRandId).
    WithParams(userRandId).
    ExecWithRelation(ctx, scanPostRow, scanPostWithAuthorRows)
```

---

## Project conventions

- Individual item TTL: **7 days**
- Sorted set / Timeline TTL: **2 days**
- Default `itemPerPage`: **20**
- All redifu clients are initialized in a single `redis.go` or `cache.go` file per domain
- Seeders are called at the service/handler layer, not in the repository layer

---

## What not to do

- Do not call `redisClient.Set(...)` / `redisClient.Get(...)` directly for entity data — use `Base[T]`
- Do not store a full related object inside a parent entity — use Relation (`XxxRandId` + `AddRelation`)
- Do not call `pipe.Exec()` inside a function that receives `pipe` as a parameter
- Do not delete an item with `Base.Del` alone — always use `RemoveItem` on its collection
- Do not ignore `redifu.ResetPagination` — always handle it by discarding the cursor and re-seeding from page one
