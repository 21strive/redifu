package redifu

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Cursor is a position in a Timeline: the score and the member of the last item a client
// has seen. FetchFrom reads the page that follows it.
//
// A randId cursor (Fetch(lastRandIds)) has to find its member in the index, so it breaks
// as soon as that item leaves — deleted, or moved to another status — and the client is
// sent back to page one with ErrResetPagination. A Cursor is read by position instead: the
// page after (Score, Member) exists whether or not Member is still in the index.
//
// Build one from what the index returned (FetchOutput.NextCursor) or, for a row read from
// the database, from Timeline.Score and the row's randId. Never from a score you computed
// yourself: the cursor has to agree with the index to the last digit.
type Cursor struct {
	Score  float64
	Member string
}

// IsZero reports whether c is the zero Cursor, which carries no position: FetchFrom reads
// the first page for it.
func (c Cursor) IsZero() bool {
	return c == Cursor{}
}

// String renders the cursor for a client to hand back, as "<score>:<member>". The zero
// Cursor renders as "". ParseCursor reads either back exactly.
func (c Cursor) String() string {
	if c.IsZero() {
		return ""
	}
	return strconv.FormatFloat(c.Score, 'f', -1, 64) + ":" + c.Member
}

// ParseCursor reads a cursor rendered by Cursor.String. The empty string is the zero
// Cursor, so a request without a cursor reads the first page. A malformed cursor returns
// an error wrapping ErrInvalidCursor.
func ParseCursor(token string) (Cursor, error) {
	if token == "" {
		return Cursor{}, nil
	}

	// The score never contains a colon, so the first one ends it; the member may contain
	// any byte at all.
	scorePart, member, found := strings.Cut(token, ":")
	if !found {
		return Cursor{}, fmt.Errorf("%w: %q has no score separator", ErrInvalidCursor, token)
	}

	score, err := strconv.ParseFloat(scorePart, 64)
	if err != nil || math.IsNaN(score) {
		return Cursor{}, fmt.Errorf("%w: %q does not start with a score", ErrInvalidCursor, token)
	}

	return Cursor{Score: score, Member: member}, nil
}

// scoredMembers splits a flat [member, score, member, score, ...] reply, the shape both
// WITHSCORES and the Timeline scripts return.
func scoredMembers(flat []string) ([]string, []float64, error) {
	if len(flat)%2 != 0 {
		return nil, nil, fmt.Errorf("redifu: expected member/score pairs, got %d values", len(flat))
	}

	members := make([]string, 0, len(flat)/2)
	scores := make([]float64, 0, len(flat)/2)
	for i := 0; i < len(flat); i += 2 {
		score, err := strconv.ParseFloat(flat[i+1], 64)
		if err != nil {
			return nil, nil, fmt.Errorf("redifu: unreadable score %q for member %q: %w", flat[i+1], flat[i], err)
		}
		members = append(members, flat[i])
		scores = append(scores, score)
	}

	return members, scores, nil
}
