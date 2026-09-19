# Upgrading to the hardened redifu

This release makes redifu actually guarantee the thing it was built for: an entity is
stored once, and every place that points at it sees the current value. Along the way it
fixes a set of bugs that produced wrong results rather than errors.

Everything here is a compile-time break or a documented behaviour change. Nothing
changes silently.

---

## Before you deploy

**Collection keys changed shape.** Every collection key is now wrapped in a Redis
Cluster hash tag, so `feed:u1` is stored as `{feed:u1}` and its markers as
`{feed:u1}:firstpage`. This is what makes the collection and its state markers land in
one slot, which is required for the atomic ingest below.

Item keys are unchanged: `post:abc` is still `post:abc`.

Nothing needs migrating. Redifu is a cache — the old collection keys are simply never
read again and expire on their own, and every collection reseeds from your database on
first access. Deploy during a period you are happy to serve from the database.

---

## 1. Constructors return an error

Every constructor validates its key format now — the number of `%s` placeholders, the
verbs used, the presence of characters redifu reserves.

```go
// before
postBase := redifu.NewBase[*Post](client, "post:%s", 7*24*time.Hour)

// after
postBase, err := redifu.NewBase[*Post](client, "post:%s", 7*24*time.Hour)
if err != nil {
    log.Fatal(err)
}
```

Same for `NewSorted`, `NewTimeline`, `NewPage`, `NewTimeSeries`, `NewSortedSet`.

`Base` takes exactly one `%s`, the item's randId. A format with two placeholders is
rejected: an item addressed by more than one parameter cannot be the target of a
relation, which only ever carries one id.

## 2. Relations live on `Base`, and they nest

Declare a relation once, on the entity, and it resolves everywhere that entity is read
— `Base.Get`, `Base.GetMany`, and every index built on top of it.

```go
postBase.AddRelation(authorRelation)   // preferred
timeline.AddRelation(pinnedRelation)   // still works, for index-specific extras
```

Two consequences:

- **`Base.Get` now resolves relations.** It previously returned an item with every
  relation field nil, so fetching one post by id gave you a post with no author. This
  was the largest hole in the singleton model.
- **Relations nest.** `Doc → Owner → Org` now comes back whole. Each level costs one
  batched round-trip, not one read per item. Depth is capped at
  `redifu.DefaultRelationDepth` (4) so a self-referential relation terminates; change it
  with `base.SetRelationDepth(n)`.

## 3. `Relate` refuses a field that is not tagged `json:"-"`

Invariant 3 was documented and unenforced, and breaking it corrupted data permanently
and silently: the related entity got baked into the parent's own key the first time the
parent was written back, and from then on that copy never updated again.

`Relate` now renders a blank parent before and after running your setter. If the related
entity shows up in the parent's stored JSON, the relation is refused:

```go
_, err := redifu.Relate(accountBase,
    func(p *Post) string      { return p.AuthorRandId },
    func(p *Post, a *Account) { p.Author = a },
)
// err is ErrRelationNotTransient if Post.Author is missing json:"-"
```

You handle this error at startup, which you were already told to do.

## 4. Reading an item extends its TTL

A shared entity is the most-read key you have and, without this, the first to expire out
from under everything pointing at it. `Base.Get`, `Base.GetMany` and relation resolution
now read with `GETEX`, refreshing the TTL.

Requires Redis 6.2. On anything older: `base.SetTouchOnRead(false)`.

## 5. Ingest is one atomic command

`AddItem` and `IngestItem(seed=false)` used to read the collection's state from Go and
then write — five round-trips per item for `Timeline`. That is now a single Lua script.

- **Fan-out got ~5000x cheaper in round-trips.** One post into ten thousand follower
  timelines was fifty thousand round-trips; it is now ten thousand pipelined commands.
- **The read-decide-write race is gone.** Two writers could both see "this page holds
  itemPerPage items" and both add, pushing the page over its size. The decision and the
  write are now one operation.

`IngestItem` still takes your pipeline and still never executes it.

## 6. `AddItem` tells you when it did not index the item

It returns `ErrNotIngested` when the item was stored in Base and its TTL refreshed, but
it was deliberately not placed into the index — the collection is not seeded, or the
item sorts outside the window the index holds. This was previously a silent `nil`.

```go
if err := timeline.AddItem(ctx, post, userId); err != nil && !errors.Is(err, redifu.ErrNotIngested) {
    return err
}
```

It is an outcome, not a failure: the item appears once the collection is seeded.

## 7. Pagination no longer mistakes expired items for the end of a feed

`Timeline` decided `LastPage` from how many items it managed to load. Items whose keys
had expired were skipped during loading, so three expired items in the middle of a feed
made the tenth page look like the last one and infinite scroll stopped early.

Page position is now read from the index, before any item is loaded:

```go
output := timeline.Fetch(cursor).WithParams(userId).Exec(ctx)
output.HasMore()      // read from the index — use this to drive "load more"
output.IsFirstPage()
output.Dangling()     // members that pointed at items which no longer exist
output.Position()     // unchanged, for existing callers
```

`Position()` still returns one of three values and still reports `FirstPage` for a page
that is both first and last; `HasMore()` and `IsFirstPage()` are there when you need
both facts.

Dangling members are also removed from the index as they are found, so a collection
converges instead of decaying. Turn it off with `SetSelfHeal(false)`.

## 8. A cursor that no longer exists returns `ErrResetPagination`

If none of the cursors you passed are still in the index, `Fetch` used to quietly serve
page one — the client received items it already had, with no way to know. It now returns
`ErrResetPagination`, which is what that error was for.

```go
output := timeline.Fetch(cursors).WithParams(userId).Exec(ctx)
if errors.Is(output.Error(), redifu.ErrResetPagination) {
    // tell the client to restart the feed
}
```

Resolving a cursor is also one round-trip now instead of four.

## 9. `Count` returns an error

```go
count, err := sorted.Count(ctx, userId)
```

It used to fold a Redis failure into `0`. A zero that means "Redis is down" reads as
"this collection is empty", which sent every request to the database at once and made
ingest drop items on the floor.

## 10. `SetSortingReference` returns an error

The field is resolved and type-checked once, when you set it, instead of by reflection
on every write.

```go
if err := timeline.SetSortingReference("UpdatedAt"); err != nil {
    log.Fatal(err)  // typo, unexported field, or a type that cannot be a score
}
```

An `int64` reference beyond 2^53 is now rejected with `ErrScoreOutOfRange` instead of
silently losing precision and reordering the index. Snowflake ids are affected; Unix
millisecond timestamps are nowhere near the limit.

`TimeSeries` has `SetSortingReference` for the first time — it read the field but gave
you no way to set it, so every series was pinned to `createdAt`.

## 11. `ErrNotFound` instead of `redis.Nil`

```go
post, err := postBase.Get(ctx, randId)
if errors.Is(err, redifu.ErrNotFound) { ... }
```

You no longer import go-redis to tell "missing" from "broken".

`ResetPagination` is now `ErrResetPagination`; the old name is a deprecated alias.

## 12. Bugs fixed with no API change

- **`Base.Exists` did the opposite of its name.** It called `UnmarkMissing`, deleting
  the not-found marker and returning nil. It now returns `(bool, error)`.
- **`TimeSeries` ignored your pipeline.** `WithPipeline(pipe).AddItem` discarded `pipe`
  and executed a second pipeline behind your back — the two branches were inverted.
- **Processor arguments arrived double-wrapped** through `Page.Fetch` and
  `TimeSeries.Fetch`, so your processor received `[][]interface{}` instead of what you
  passed. `Sorted` and `Timeline` were unaffected, so the same processor behaved
  differently depending on the structure.
- **`Base.Set`/`Del` panicked** on an empty, non-nil key-parameter slice.
- **Key parameters were appended to your own slice.** Spreading a variadic argument
  hands the callee your slice, spare capacity and all, so redifu could write a page
  number into a slot you still held. Every derived key now copies first. A fetch builder
  can also be executed twice without accumulating parameters.
- **`TimeSeries` rejected an item sitting exactly on a segment boundary**, because a
  point query was compared with the strict inequalities meant for ranges. There is a
  `Covers(ctx, instant, ...)` for this now.
- **`TimeSeries.Purge` did not exist**, so purging a series left the segment store
  claiming ranges that held no data. It does now, and it clears both.
- **`Page` had no `AddItem` or `RemoveItem`** — a new item could not enter a page
  without a full reseed. Both exist, taking the page number.
- **`Timeline.RemoveItem` made two round-trips to read markers it deleted either way.**
- **`Timeline` cursor resolution read each candidate from `Base`** only to recover a
  randId it already had.

## 13. Removed

`sqlitem.go` (`SQLItemBlueprint`) — a leftover from the SQL layer that was removed
earlier. Nothing replaces it: `redifu.Blueprint` is the only contract an entity has to
meet.

## 14. Identity is redifu's own again

`redifu.Record` and `redifu.InitRecord` are back, and this time they are not tied to the
SQL layer. redifu no longer depends on `github.com/21strive/item` at all — it defines
`Blueprint` itself, and `Record` is the identity entities embed.

```go
type Post struct {
    *redifu.Record         // was: *item.Foundation
    Title string `json:"title"`
}

post := &Post{}            // was: &Post{Foundation: &item.Foundation{}}
redifu.InitRecord(post)    // was: item.InitItem(post)
```

The shape is unchanged — a pointer embed, allocated by `InitRecord` — so a migration is
a rename and nothing else.

**Nothing forces you to migrate.** `Blueprint` is an ordinary interface, so it is
satisfied structurally: an entity that still embeds `*item.Foundation` keeps compiling
and keeps working, and the stored JSON is identical either way — `Record` inlines under
the same keys. Migrate an entity when you touch it, not before.

Two things do change if you adopt `Record`:

- **`InitRecord` does not allocate named pointer fields.** `item.InitItem` walked every
  field and allocated each nil pointer, which handed a relation field a non-nil empty
  entity before any fetch had run. `InitRecord` allocates embedded pointers only, so a
  relation field stays nil until a fetch resolves it — which is what the rest of redifu
  documents and what `Relate`'s transient probe assumes.
- **`RandId` draws from `crypto/rand`.** Same 16 characters, same alphabet, same wire
  format; a randId is a public handle that ends up in URLs, so it is no longer drawn
  from the `math/rand` global source.

---

## Checklist

1. Handle the error from every constructor.
2. Move `AddRelation` calls from your indexes onto the matching `Base`.
3. Run your app: `Relate` will fail at startup on any relation field missing `json:"-"`.
   Fix the tag — and then re-save those entities, because any already written back have
   a stale copy baked into them.
4. Handle `ErrNotIngested` from `AddItem`.
5. Replace `errors.Is(err, redis.Nil)` with `errors.Is(err, redifu.ErrNotFound)`.
6. Take the error from `Count` and `SetSortingReference`.
7. Switch "load more" from `Position() != LastPage` to `HasMore()`.
8. Handle `ErrResetPagination` on the fetch output.
9. If you run Redis below 6.2, call `SetTouchOnRead(false)` on every `Base`.
