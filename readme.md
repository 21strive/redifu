# Redifu

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

**Field naming convention (required):**

```go
type Post struct {
    redifu.Record
    Title         string
    AuthorRandId  string          // reference: FieldName + "RandId"
    Author        account.Account // related entity field
}
```

`Author` is not stored inside `Post`. On fetch, redifu reads `AuthorRandId`, retrieves `Author` from `Base[Account]`, and injects it into the `Author` field. Because `Base[Account]` is the source of truth, any update to `Account` is immediately reflected in all `Post` records.

**Setup:**

```go
authorRelation, err := redifu.NewRelation[account.Account](
    account.AccountBase,
    redifu.TypeOf[Post](),
)
if err != nil {
    log.Fatal(err)
}
postTimeline.AddRelation("author", authorRelation)
```

---

## Seeder

Seeders populate Redis from a SQL database when a key has no data. Standard pattern:

```go
func GetFeed(ctx context.Context, userRandId string, lastRandIds []string) ([]Post, error) {
    needsSeed, err := postTimeline.RequiresSeeding(ctx, int64(len(lastRandIds)), userRandId)
    if err != nil {
        return nil, err
    }

    if needsSeed {
        seeder := redifu.NewTimelineSeeder[Post](redisClient, db, postBase, postTimeline)
        err = seeder.
            Seed(0, "", queryBuilder).
            WithQueryArgs(userRandId).
            WithParams(userRandId).
            Exec(ctx, scanPostRow, scanPostRows)
        if err != nil {
            return nil, err
        }
    }

    output := postTimeline.Fetch(lastRandIds).WithParams(userRandId).Exec(ctx)
    return output.Items(), output.Error()
}
```

Seeders are available for all structures: `NewTimelineSeeder`, `NewSortedSeeder`, `NewPageSeeder`, `NewTimeSeriesSeeder`.

---

## Default TTL

| | TTL |
|--|-----|
| Individual item (`Base`) | 7 days |
| Sorted set / Timeline | 2 days |

TTL is configurable at initialization.

## Limitations

- The built-in query builder only supports **PostgreSQL** (`$1, $2, ...`). For MySQL or complex queries, write SQL directly.
- Sorting only supports fields of type `time.Time` or `int64`.