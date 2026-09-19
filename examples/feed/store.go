package feed

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/21strive/redifu"
	"github.com/redis/go-redis/v9"
)

const (
	postKeyFormat         = "post:%s"
	accountKeyFormat      = "account:%s"
	organisationKeyFormat = "organisation:%s"

	// One %s: the user whose feed this is.
	postFeedKeyFormat = "feed:user:%s:posts"

	itemTTL  = 7 * 24 * time.Hour
	indexTTL = 2 * 24 * time.Hour // must be shorter than itemTTL

	postsPerPage = 20
)

// Store holds everything wired at startup. A Base per entity, an index per view.
type Store struct {
	db    *sql.DB
	redis redis.UniversalClient

	PostBase         *redifu.Base[*Post]
	AccountBase      *redifu.Base[*Account]
	OrganisationBase *redifu.Base[*Organisation]

	PostFeed *redifu.Timeline[*Post]
}

// NewStore is the whole wiring. Every constructor validates its key format and returns
// an error, and Relate validates the relation, so a mistake here fails at startup
// rather than at the first request.
func NewStore(db *sql.DB, redisClient redis.UniversalClient) (*Store, error) {
	store := &Store{db: db, redis: redisClient}

	var err error

	if store.OrganisationBase, err = redifu.NewBase[*Organisation](redisClient, organisationKeyFormat, itemTTL); err != nil {
		return nil, fmt.Errorf("organisation base: %w", err)
	}
	if store.AccountBase, err = redifu.NewBase[*Account](redisClient, accountKeyFormat, itemTTL); err != nil {
		return nil, fmt.Errorf("account base: %w", err)
	}
	if store.PostBase, err = redifu.NewBase[*Post](redisClient, postKeyFormat, itemTTL); err != nil {
		return nil, fmt.Errorf("post base: %w", err)
	}

	// ----------------------------------------------------------------------
	// The relations. This is the only place the wiring lives.
	//
	// Each one goes on the Base of the entity that OWNS the randId field. An
	// account's organisation is a fact about the account, so it belongs to
	// AccountBase — not to whichever feed the account happens to be read through.
	//
	// Both accessors are plain Go functions, so renaming a field breaks the build
	// here instead of silently resolving to nothing at runtime.
	// ----------------------------------------------------------------------

	organisationRelation, err := redifu.Relate(store.OrganisationBase,
		func(a *Account) string { return a.OrganisationRandId },  // where the randId lives
		func(a *Account, o *Organisation) { a.Organisation = o }, // where the entity goes
	)
	if err != nil {
		return nil, fmt.Errorf("organisation relation: %w", err)
	}
	store.AccountBase.AddRelation(organisationRelation)

	authorRelation, err := redifu.Relate(store.AccountBase,
		func(p *Post) string { return p.AuthorRandId },
		func(p *Post, a *Account) { p.Author = a },
	)
	if err != nil {
		// Also fires with ErrRelationNotTransient if Post.Author loses its json:"-".
		return nil, fmt.Errorf("author relation: %w", err)
	}
	store.PostBase.AddRelation(authorRelation)

	// ----------------------------------------------------------------------
	// The index. It declares no relations of its own: everything it needs is
	// already on PostBase, so a feed fetch and a single PostBase.Get resolve
	// exactly the same chain.
	// ----------------------------------------------------------------------

	if store.PostFeed, err = redifu.NewTimeline[*Post](
		redisClient,
		store.PostBase,
		postFeedKeyFormat,
		postsPerPage,
		redifu.Descending,
		indexTTL,
	); err != nil {
		return nil, fmt.Errorf("post feed: %w", err)
	}

	// Sort by Published rather than createdAt. The field is resolved and type-checked
	// right here, so a typo fails now. Whatever you set must also be the ORDER BY
	// column in the seeder below, and the column the cursor is read from.
	if err := store.PostFeed.SetSortingReference("Published"); err != nil {
		return nil, fmt.Errorf("sorting reference: %w", err)
	}

	return store, nil
}
