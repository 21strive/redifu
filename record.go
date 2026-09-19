package redifu

import (
	"crypto/rand"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// randIdLength is the number of characters in a randId. It is the public handle of an
// item — the member stored in every sorted set and the parameter of every item key —
// so it is generated from crypto/rand, not math/rand: these end up in URLs.
const randIdLength = 16

const randIdAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Blueprint is what redifu requires of an entity: a stable randId to index by and
// timestamps to score by. Embedding Record satisfies it.
//
// It is an ordinary interface, so it is satisfied structurally. An entity that already
// embeds another type carrying these methods keeps working without being rewritten.
type Blueprint interface {
	SetUUID()
	GetUUID() string
	SecureUUID()
	SetRandId()
	GetRandId() string
	SetCreatedAt(time time.Time)
	GetCreatedAt() time.Time
	SetUpdatedAt(time time.Time)
	GetUpdatedAt() time.Time
}

// Record is the identity every redifu entity embeds, as a pointer:
//
//	type Post struct {
//	    *redifu.Record
//	    Title string `json:"title"`
//	}
//
// The pointer has to be allocated before any field on it is read or written. There are
// exactly two ways an entity comes into being, and each one covers it:
//
//   - created fresh — InitRecord allocates the Record and mints the identity;
//   - filled from a database row — it already has a randId, so allocate without
//     minting: &Post{Record: &redifu.Record{}}. A fetch out of Base does this for
//     itself, since unmarshalling allocates the embedded pointer.
//
// An entity that reaches redifu with a nil Record panics on the first method call, and
// json.Marshal quietly drops the identity fields rather than failing — so neither path
// above is optional.
//
// Two identifiers, deliberately: UUID is the private one and never has to leave the
// server — SecureUUID clears it before an entity goes out over an API — while RandId
// is the public handle redifu indexes by.
type Record struct {
	UUID      string    `json:"uuid,omitempty" bson:"uuid" db:"uuid"`
	RandId    string    `json:"randid,omitempty" bson:"randid" db:"randid"`
	CreatedAt time.Time `json:"createdAt" bson:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updatedAt" bson:"updated_at" db:"updated_at"`
}

func (r *Record) SetUUID() { r.UUID = uuid.New().String() }

func (r *Record) GetUUID() string { return r.UUID }

// SecureUUID drops the private identifier, for an entity about to be serialised to a
// client. The randId is untouched: that one is meant to be seen.
func (r *Record) SecureUUID() { r.UUID = "" }

func (r *Record) SetRandId() { r.RandId = RandId() }

func (r *Record) GetRandId() string { return r.RandId }

func (r *Record) SetCreatedAt(time time.Time) { r.CreatedAt = time }

func (r *Record) GetCreatedAt() time.Time { return r.CreatedAt }

func (r *Record) SetUpdatedAt(time time.Time) { r.UpdatedAt = time }

func (r *Record) GetUpdatedAt() time.Time { return r.UpdatedAt }

// InitRecord mints the identity of an entity being created for the first time: a uuid,
// a randId, and both timestamps at the current UTC instant.
//
// Call it only on a genuinely new entity. An entity being filled from a database row
// already carries its randId, and minting a fresh one here would orphan every index
// that points at the old one.
//
// The embedded Record is allocated first, so &Post{} is all a caller has to write.
// Named fields are left alone — a relation field has to stay nil until a fetch
// resolves it, which is why this does not simply allocate every nil pointer it finds.
func InitRecord[T Blueprint](record T) {
	value := reflect.ValueOf(record)
	for value.Kind() == reflect.Pointer && !value.IsNil() {
		value = value.Elem()
	}
	allocateEmbedded(value)

	now := time.Now().In(time.UTC)
	record.SetUUID()
	record.SetRandId()
	record.SetCreatedAt(now)
	record.SetUpdatedAt(now)
}

// RandId returns a new public identifier. It is exported because a consumer seeding
// rows that predate redifu needs to mint one per row.
func RandId() string {
	alphabet := big.NewInt(int64(len(randIdAlphabet)))
	result := make([]byte, randIdLength)
	for i := range result {
		pick, err := rand.Int(rand.Reader, alphabet)
		if err != nil {
			// crypto/rand does not fail on any platform redifu runs on; if the system
			// entropy source is gone there is nothing sensible left to do here.
			panic("redifu: crypto/rand unavailable: " + err.Error())
		}
		result[i] = randIdAlphabet[pick.Int64()]
	}
	return string(result)
}
