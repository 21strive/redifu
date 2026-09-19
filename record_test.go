package redifu

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// valueFoundation stands in for an identity type a consumer already has, embedded by
// value rather than as a pointer. Blueprint is an ordinary interface, so such an entity
// satisfies it structurally and keeps working without being rewritten. If this stops
// compiling, the abstraction turned into a break.
type valueFoundation struct {
	UUID      string    `json:"uuid,omitempty"`
	RandId    string    `json:"randid,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func (v *valueFoundation) SetUUID()                  { v.UUID = "foreign-uuid" }
func (v *valueFoundation) GetUUID() string           { return v.UUID }
func (v *valueFoundation) SecureUUID()               { v.UUID = "" }
func (v *valueFoundation) SetRandId()                { v.RandId = RandId() }
func (v *valueFoundation) GetRandId() string         { return v.RandId }
func (v *valueFoundation) SetCreatedAt(at time.Time) { v.CreatedAt = at }
func (v *valueFoundation) GetCreatedAt() time.Time   { return v.CreatedAt }
func (v *valueFoundation) SetUpdatedAt(at time.Time) { v.UpdatedAt = at }
func (v *valueFoundation) GetUpdatedAt() time.Time   { return v.UpdatedAt }

type foreignEntity struct {
	valueFoundation
	Title string `json:"title"`
}

type recordEntity struct {
	*Record
	Title string `json:"title"`
}

// InitRecord is the single entry point for a new entity: it allocates the embedded
// Record and mints the identity, so a caller never writes &X{Record: &Record{}} for
// anything it is creating fresh.
func TestInitRecordAllocatesAndMints(t *testing.T) {
	entity := &recordEntity{}
	InitRecord(entity)

	if entity.Record == nil {
		t.Fatal("InitRecord left the embedded Record nil")
	}
	if entity.GetRandId() == "" || entity.GetUUID() == "" {
		t.Fatalf("InitRecord left the identity blank: %+v", entity.Record)
	}
	if entity.GetCreatedAt().IsZero() || entity.GetUpdatedAt().IsZero() {
		t.Fatal("InitRecord left the timestamps blank")
	}
	if entity.GetCreatedAt().Location() != time.UTC {
		t.Fatalf("timestamps must be UTC, got %s", entity.GetCreatedAt().Location())
	}
}

// An entity filled from a database row already carries its randId, so it allocates the
// Record without minting one. This is the only construction that spells it out.
func TestRecordAllocatedWithoutMintingRoundTrips(t *testing.T) {
	_, client := newTestRedis(t)
	ctx := context.Background()

	entity := &recordEntity{Record: &Record{}}
	entity.RandId = "from-the-row"
	entity.SetCreatedAt(time.Now().In(time.UTC))
	entity.Title = "scanned"

	base := mustBase[*recordEntity](t, client, "record:%s", time.Minute)
	if err := base.Set(ctx, entity); err != nil {
		t.Fatalf("Set: %v", err)
	}

	fetched, err := base.Get(ctx, "from-the-row")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fetched.GetRandId() != "from-the-row" || fetched.Title != "scanned" {
		t.Fatalf("round trip lost the entity: %+v", fetched)
	}
}

// Blueprint must stay structurally satisfiable. An entity carrying the methods some
// other way — here by value, the shape Record itself does not use — still works.
func TestForeignIdentityStillSatisfiesBlueprint(t *testing.T) {
	_, client := newTestRedis(t)
	ctx := context.Background()

	entity := &foreignEntity{}
	InitRecord(entity)
	entity.Title = "still works"

	base := mustBase[*foreignEntity](t, client, "foreign:%s", time.Minute)
	if err := base.Set(ctx, entity); err != nil {
		t.Fatalf("Set: %v", err)
	}
	fetched, err := base.Get(ctx, entity.GetRandId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fetched.Title != "still works" {
		t.Fatalf("round trip lost the entity: %+v", fetched)
	}
}

// Record is embedded as a pointer but must still inline into the entity's JSON, because
// entities written before it existed are already in Redis under these keys.
func TestRecordInlinesIntoEntityJSON(t *testing.T) {
	entity := &recordEntity{Title: "inline"}
	InitRecord(entity)

	encoded, err := json.Marshal(entity)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, key := range []string{"uuid", "randid", "createdAt", "updatedAt", "title"} {
		if _, present := decoded[key]; !present {
			t.Fatalf("key %q missing from %s", key, encoded)
		}
	}
	if _, nested := decoded["Record"]; nested {
		t.Fatalf("Record serialised as a nested object: %s", encoded)
	}
}

// Unmarshalling allocates the embedded Record on its own, so a fetch never hands back
// an entity whose identity would panic on the first method call.
func TestFetchAllocatesTheEmbeddedRecord(t *testing.T) {
	var entity recordEntity
	if err := json.Unmarshal([]byte(`{"randid":"abc","title":"t"}`), &entity); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if entity.Record == nil {
		t.Fatal("Unmarshal left the embedded Record nil")
	}
	if entity.GetRandId() != "abc" {
		t.Fatalf("randId did not survive: %q", entity.GetRandId())
	}
}

func TestRandIdIsDistinctAndWellFormed(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		randId := RandId()
		if len(randId) != randIdLength {
			t.Fatalf("randId %q is %d characters, want %d", randId, len(randId), randIdLength)
		}
		for _, character := range randId {
			if !strings.ContainsRune(randIdAlphabet, character) {
				t.Fatalf("randId %q contains %q, outside the alphabet", randId, character)
			}
		}
		if _, duplicate := seen[randId]; duplicate {
			t.Fatalf("randId %q generated twice in 1000 draws", randId)
		}
		seen[randId] = struct{}{}
	}
}
