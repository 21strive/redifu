package redifu

import "errors"

var (
	// ErrNotFound is returned when a key does not exist. It replaces the driver's
	// redis.Nil in redifu's public API so that consumers never have to import
	// go-redis just to tell "missing" apart from "broken".
	ErrNotFound = errors.New("redifu: not found")

	// ErrResetPagination tells a Timeline consumer that the cursor it supplied can no
	// longer be located and the client should restart from the first page.
	ErrResetPagination = errors.New("redifu: reset pagination")

	// ErrNotIngested reports that AddItem stored the item in Base and refreshed its
	// TTL, but deliberately did not place it into the index — the collection is not
	// seeded, or the item falls outside the window this index currently holds. It is
	// an outcome, not a failure: the item will appear once the collection is seeded.
	ErrNotIngested = errors.New("redifu: item was not ingested into the index")

	// ErrRelationNotTransient is returned by Relate when the field the setter writes
	// into is not tagged json:"-". Without that tag, writing a fetched item back to
	// Base bakes a copy of the related entity into the item key and the singleton is
	// broken permanently, so this is refused at construction time.
	ErrRelationNotTransient = errors.New("redifu: relation field must be tagged json:\"-\"")

	// ErrRelationDepthExceeded is returned when nested relations are deeper than the
	// configured limit, which usually means two entities relate to each other in a
	// cycle.
	ErrRelationDepthExceeded = errors.New("redifu: relation depth exceeded")

	// ErrScoreOutOfRange is returned when an int64 sorting reference cannot be held
	// exactly by a float64 sorted-set score, which would corrupt ordering silently.
	ErrScoreOutOfRange = errors.New("redifu: sorting reference exceeds the exact integer range of a sorted-set score")
)

// ResetPagination is the old name of ErrResetPagination.
//
// Deprecated: use ErrResetPagination.
var ResetPagination = ErrResetPagination
