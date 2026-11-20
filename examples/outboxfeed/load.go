package outboxfeed

import (
	"context"
	"strconv"
	"strings"

	"github.com/21strive/redifu"
	"github.com/jmoiron/sqlx"
)

// The columns each entity is loaded with, everywhere: the seeds, the handler, the readers of the
// related entities and the reads that go to SQL. Change one and change that entity's generation
// (cache.go).
const (
	organisationColumns = `uuid, randid, name, plan, created_at, updated_at`
	accountColumns      = `uuid, randid, organisation_randid, name, handle, bio, created_at, updated_at`
	postColumns         = `p.uuid, p.randid, p.author_randid, p.title, p.body, p.published, p.created_at, p.updated_at`
)

// byRandIds selects rows by randid with the keys as one text parameter (a randid never holds a
// comma), so the query needs no driver-specific array type. With lib/pq, randid = ANY($1) and
// pq.Array(keys) does the same.
const byRandIds = `randid = ANY(string_to_array($1, ','))`

// feedScore is the feed's order in SQL: Published in microseconds, which is what Post.Score holds
// and the index stores (exact in a float64 until 2255). An application gives the keyset an index —
// (author_randid, the score, randid COLLATE "C"), the score kept in a column of its own.
const feedScore = `floor(extract(epoch FROM p.published) * 1000000)::bigint`

// loadOrganisations and loadAccounts read entities by randid through q: the primary, or the
// transaction of a seed or a drain.

func loadOrganisations(ctx context.Context, q sqlx.QueryerContext, randIds []string) ([]*Organisation, error) {
	var organisations []*Organisation
	err := sqlx.SelectContext(ctx, q, &organisations,
		`SELECT `+organisationColumns+` FROM organisations WHERE `+byRandIds, strings.Join(randIds, ","))
	return organisations, err
}

func loadAccounts(ctx context.Context, q sqlx.QueryerContext, randIds []string) ([]*Account, error) {
	var accounts []*Account
	err := sqlx.SelectContext(ctx, q, &accounts,
		`SELECT `+accountColumns+` FROM accounts WHERE `+byRandIds, strings.Join(randIds, ","))
	return accounts, err
}

// loadPosts runs one SELECT of posts through q and scores what it read.
func loadPosts(ctx context.Context, q sqlx.QueryerContext, where string, args ...any) ([]*Post, error) {
	var posts []*Post
	if err := sqlx.SelectContext(ctx, q, &posts, `SELECT `+postColumns+` FROM posts p `+where, args...); err != nil {
		return nil, err
	}
	for _, post := range posts {
		post.SetScore(post.Published.UnixMicro())
	}
	return posts, nil
}

// loadFeed reads up to limit of author's posts that follow after in the feed's order — the newest
// for the zero Cursor. The order is the index's (ZREVRANGE): the score descending, then the randId,
// the member the index stores, byte by byte descending. The keyset compares exactly that pair, so a
// top-up continuing from the index's tail neither skips nor repeats a row.
func loadFeed(ctx context.Context, q sqlx.QueryerContext, author string, after redifu.Cursor, limit int64) ([]*Post, error) {
	where := `WHERE p.author_randid = $1`
	args := []any{author}
	if !after.IsZero() {
		where += ` AND (` + feedScore + `, p.randid COLLATE "C") < ($2, $3)`
		args = append(args, int64(after.Score), after.Member)
	}
	where += ` ORDER BY ` + feedScore + ` DESC, p.randid COLLATE "C" DESC LIMIT ` + strconv.FormatInt(limit, 10)
	return loadPosts(ctx, q, where, args...)
}
