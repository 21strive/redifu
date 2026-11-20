package outboxfeed

import (
	"context"

	"github.com/21strive/redifu"
	"github.com/21strive/redifu/outbox"
)

// FeedPage is one page of an author's feed, as the transport layer gets it.
type FeedPage struct {
	Posts   []*Post // newest first, each with Author and Author.Organisation filled in
	Cursor  string  // where the page ends (outbox.EncodeCursor): the client sends it back
	HasMore bool
}

// AuthorFeed reads the page of author's posts that follows cursor ("" for the first page), perPage
// of them (below 1: postsPerPage). The cache answers when it can — with every change committed
// before the call already applied, and the relations resolved two levels down — and SQL when it
// cannot; both give the same posts and hand out the same cursor, so a client may get one page from
// each.
func (c *Cache) AuthorFeed(ctx context.Context, author, cursor string, perPage int64) (*FeedPage, error) {
	after, err := outbox.DecodeCursor(cursor)
	if err != nil {
		return nil, err // outbox.ErrBadCursor: a client error
	}
	if perPage < 1 {
		perPage = postsPerPage
	}
	if page, ok := c.cachedFeed(ctx, author, after, perPage); ok {
		return page, nil
	}
	return c.feedFromSQL(ctx, author, after, perPage)
}

// cachedFeed is the cache's answer, ok false when it cannot give one now: reads off, Redis down, a
// lock busy, a table not watched, another build holding the cache. Never read that as an empty feed.
func (c *Cache) cachedFeed(ctx context.Context, author string, after redifu.Cursor, perPage int64) (*FeedPage, bool) {
	// One read scope for the whole answer: three readers, one queue check per owner.
	ctx = outbox.NewReadScope(ctx)

	// The related entities first, for two reasons. A seed of the feed writes posts and nothing else,
	// so the author and the organisation are loaded here when Redis lacks them. And Serve applies one
	// owner's queue: the feed's read applies the author's, which holds the author's own row with the
	// posts, but an organisation is queued under the global owner. Reading it through its Detail
	// applies that queue too, so a rename committed before this call is in the answer.
	accounts, ok := c.Accounts.Get(ctx, author)
	if !ok {
		return nil, false
	}
	account, exists := accounts[author]
	if !exists {
		return &FeedPage{Cursor: outbox.EncodeCursor(after)}, true // no such author, so no posts
	}
	if _, ok := c.Organisations.Get(ctx, account.OrganisationRandId); !ok {
		return nil, false
	}

	page, ok := c.Feed.ReadN(ctx, []string{author}, after, perPage)
	if !ok {
		return nil, false
	}
	return &FeedPage{Posts: page.Items, Cursor: outbox.EncodeCursor(page.Next), HasMore: page.HasMore}, true
}

// feedFromSQL is the same answer from the database: the page by the feed's keyset, one row more
// than asked for HasMore, and the relations filled in by hand the way redifu fills them from Redis.
func (c *Cache) feedFromSQL(ctx context.Context, author string, after redifu.Cursor, perPage int64) (*FeedPage, error) {
	posts, err := loadFeed(ctx, c.db, author, after, perPage+1)
	if err != nil {
		return nil, err
	}
	page := &FeedPage{Cursor: outbox.EncodeCursor(after)} // an empty page ends where it started
	if int64(len(posts)) > perPage {
		posts, page.HasMore = posts[:perPage], true
	}
	if len(posts) == 0 {
		return page, nil
	}
	last := posts[len(posts)-1]
	page.Posts = posts
	page.Cursor = outbox.EncodeCursor(redifu.Cursor{Score: float64(last.Score), Member: last.RandId})

	accounts, err := loadAccounts(ctx, c.db, []string{author})
	if err != nil {
		return nil, err
	}
	for _, account := range accounts {
		organisations, err := loadOrganisations(ctx, c.db, []string{account.OrganisationRandId})
		if err != nil {
			return nil, err
		}
		for _, organisation := range organisations {
			account.Organisation = organisation
		}
		for _, post := range posts {
			post.Author = account
		}
	}
	return page, nil
}
