// Package feed is a worked example of a redifu consumer: three related entities, a
// seeder that reads them out of Postgres with one JOIN, and the handlers on top.
//
// It compiles as part of `go build ./...`, so it cannot drift away from the API.
package feed

import (
	"time"

	"github.com/21strive/redifu"
)

// Three entities. Each one is stored once, in its own Base, and updated through that
// Base. The chain is Post -> Author -> Organisation.
//
// Every entity embeds *redifu.Record, which carries the uuid, the randId and the
// timestamps redifu indexes and scores by. It is a pointer, so it has to be allocated
// before any field on it is read or written — redifu.InitRecord does that for a new
// entity, and the constructors below do it for one about to be filled from a row.
//
// Each relation is two fields:
//
//   - the randId, which IS stored in Redis and is what the scanner fills in
//   - the entity pointer, tagged json:"-", which is NEVER stored and is filled in by
//     redifu on fetch
//
// The tag is not a style choice. Without it, writing a fetched Post back to Base bakes
// a copy of the account into the post's own key, and that copy stops tracking the
// account for good. redifu.Relate refuses a relation whose field is missing it.

type Organisation struct {
	*redifu.Record
	Name string `json:"name"`
	Plan string `json:"plan"`
}

type Account struct {
	*redifu.Record
	Name   string `json:"name"`
	Handle string `json:"handle"`
	Bio    string `json:"bio"`

	OrganisationRandId string        `json:"organisationRandId"`
	Organisation       *Organisation `json:"-"`
}

type Post struct {
	*redifu.Record
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Published time.Time `json:"published"`

	AuthorRandId string   `json:"authorRandId"`
	Author       *Account `json:"-"`
}

// newPost builds an entity that is about to be filled from a database row: the Record
// is allocated so the scanner has somewhere to write RandId, but no identity is minted.
// Do not call redifu.InitRecord here — that mints a fresh randId, and the row already
// carries one. Minting a second would orphan every index pointing at the first.
func newPost() *Post { return &Post{Record: &redifu.Record{}} }

func newAccount() *Account { return &Account{Record: &redifu.Record{}} }

func newOrganisation() *Organisation { return &Organisation{Record: &redifu.Record{}} }

// NewDraftPost is the other case: an entity being created for the first time, where
// redifu.InitRecord allocates the embedded Record and mints the uuid, the randId and
// the timestamps.
func NewDraftPost(title, body, authorRandId string) *Post {
	post := &Post{}
	redifu.InitRecord(post)
	post.Title = title
	post.Body = body
	post.AuthorRandId = authorRandId
	post.Published = post.GetCreatedAt()
	return post
}
