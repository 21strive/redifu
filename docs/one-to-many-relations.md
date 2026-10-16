# One-to-many relations — design note

> Status: **considered, not built** (16 October 2026). Finding G of Aturjadwal's
> `docs/plans/redifu-formats.md` §8. No code in this branch.

## The ask

A `Relation` resolves one randId on a parent to one entity: a post's author. Some parents
own a list instead — a booking's payment terms, its staff, its resources — and a list page
of twenty bookings wants each booking's terms filled in the way an author is: batched, in a
fixed number of round trips, whatever the page size.

## What a consumer does today

An index per parent. Aturjadwal keeps `booking_payment:of:%s` as a `Sorted` keyed by the
booking's randId: a detail read is `Base.Get` for the booking plus one fetch of its index,
and every term is its own entity, stored once and updated through its own `Base`. It is
correct and uses nothing redifu lacks. Its cost is on list pages: a fetch builder executes
on its own, so twenty bookings are twenty index fetches, two or three round trips each.

## What it would take

```go
termsRelation, err := redifu.RelateMany(termIndex, // *Sorted[*Term], one key per booking
    func(b *Booking) string { return b.RandId },         // the child index's key parameter
    func(b *Booking, terms []*Term) { b.Terms = terms },  // json:"-", like any relation field
)
bookingBase.AddRelation(termsRelation)
```

Per level of resolution: one pipeline of `ZRANGE` over every parent's child index, one
pipeline of `GETEX` over the deduplicated children, then the children's own relations —
three round trips for a page, as with a single relation.

## Why not now

1. **A child list is an index, and an index has to be seeded.** A one-to-one relation follows
   a key that either exists or has expired. A child index can also be *unseeded* — and an
   empty index cannot tell "no terms" from "never loaded" without a marker per parent.
   redifu never reads the database, so the relation could only report "unknown" per parent
   and leave the consumer to seed and read again: the per-parent flow, with more machinery.
2. **Staleness would spread.** A relation key is always current, because there is one copy.
   A list relation reads membership, and membership can be stale (a missed `AddItem` or
   `RemoveItem`). Behind a relation, one stale index becomes wrong data in every read of
   every parent; as an explicit index it stays visible where it is maintained.
3. **Unbounded lists inside every read.** Terms and staff are short; a customer's bookings are
   not. A relation resolves on every read of the parent — `Base.Get` included — so it needs a
   size limit and an order, which are fetch options today, not relation options.
4. **The transient check would have to learn slices.** `Relate` proves the relation field is
   `json:"-"` by rendering a parent before and after the setter; a `[]*Term` field needs its
   own probe.
5. **Aturjadwal does not need it to ship.** Its per-parent indexes work (plan §4.1); the gap is
   round trips on list pages, measurable once RF5 lands.

## When to revisit, and the smaller step first

When a list page needs the children of every row and the per-parent fetches show up in its
latency. Start with a batched read — `Sorted.FetchMany(keyParamSets)`: every index's members in
one pipeline, all items hydrated in one more, results keyed by parent. That removes the round
trips without new semantics. Put `RelateMany` on top only once the seeding question has an
answer, for example a per-parent "seeded" marker the relation can check and report.
