// Package outboxfeed is examples/feed on the outbox: the same three entities — a post, its author
// and the author's organisation, each stored once and related through Base — and a cursor-paged
// feed, kept in step with PostgreSQL by the database itself. The write path is SQL and nothing
// else: a row trigger queues every change in the transaction of the write, and the store applies
// the queue to Redis before the next read of an affected feed, or in its drain loop.
//
// The files, in the order of an adoption:
//
//   - entity.go: the entities of examples/feed, plus the score the feed orders posts by;
//   - migrate.go: the tables, the outbox's schema, and a watch per table naming its owner;
//   - load.go: the SQL every seed, handler and fallback loads with;
//   - cache.go: the Bases and their relations, the feed, the readers of the related entities, and
//     the handler that applies the queue;
//   - read.go: an author's feed, from the cache when it can answer and from SQL when it cannot;
//   - write.go: the write path.
//
// One difference in shape: examples/feed keeps a feed per reader and fans every post out to it. On
// the outbox a change joins the queue of the row's owner, and a post has one — its author — so the
// feed here is an author's. A feed per reader would be a table of feed entries owned by the reader,
// watched and handled the way posts are here.
package outboxfeed

import (
	"time"

	"github.com/21strive/redifu"
)

// Organisation, Account and Post are the entities of examples/feed. Each relation is two fields: the
// randId, which is a column and is stored in Redis, and the entity pointer, tagged json:"-", which
// redifu fills on fetch. The db tags are the columns sqlx scans a row into; a relation pointer and
// the score are never columns.

type Organisation struct {
	*redifu.Record
	Name string `json:"name" db:"name"`
	Plan string `json:"plan" db:"plan"`
}

type Account struct {
	*redifu.Record
	Name   string `json:"name" db:"name"`
	Handle string `json:"handle" db:"handle"`
	Bio    string `json:"bio" db:"bio"`

	OrganisationRandId string        `json:"organisationRandId" db:"organisation_randid"`
	Organisation       *Organisation `json:"-" db:"-"`
}

type Post struct {
	*redifu.Record
	Title     string    `json:"title" db:"title"`
	Body      string    `json:"body" db:"body"`
	Published time.Time `json:"published" db:"published"`

	AuthorRandId string   `json:"authorRandId" db:"author_randid"`
	Author       *Account `json:"-" db:"-"`

	// Score orders the post in its author's feed: Published in microseconds. The loader sets it
	// before every index write (outbox.Scored); the index keeps it, the stored value does not.
	Score int64 `json:"-" db:"-"`
}

// SetScore sets what the next index write orders the post by.
func (p *Post) SetScore(score int64) { p.Score = score }

// NewOrganisation, NewAccount and NewPost build an entity created for the first time:
// redifu.InitRecord mints its uuid, its randId and its timestamps, and the insert writes them.

func NewOrganisation(name, plan string) *Organisation {
	organisation := &Organisation{}
	redifu.InitRecord(organisation)
	organisation.Name, organisation.Plan = name, plan
	return organisation
}

func NewAccount(organisationRandId, name, handle, bio string) *Account {
	account := &Account{}
	redifu.InitRecord(account)
	account.OrganisationRandId = organisationRandId
	account.Name, account.Handle, account.Bio = name, handle, bio
	return account
}

// NewPost is published the moment it is created; set Published before the insert to date it
// otherwise.
func NewPost(authorRandId, title, body string) *Post {
	post := &Post{}
	redifu.InitRecord(post)
	post.AuthorRandId = authorRandId
	post.Title, post.Body = title, body
	post.Published = post.GetCreatedAt()
	return post
}

// newOrganisation, newAccount and newPost allocate an entity that is named by its randId only — a
// handler deleting one — without minting an identity.

func newOrganisation(randId string) *Organisation {
	return &Organisation{Record: &redifu.Record{RandId: randId}}
}

func newAccount(randId string) *Account { return &Account{Record: &redifu.Record{RandId: randId}} }

func newPost(randId string) *Post { return &Post{Record: &redifu.Record{RandId: randId}} }
