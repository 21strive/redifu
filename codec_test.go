package redifu

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// rawItem encodes itself, and counts how often redifu asks it to.
type rawItem struct {
	*Record
	Name string
}

var rawMarshals, rawUnmarshals int

func (r *rawItem) MarshalRedis() ([]byte, error) {
	rawMarshals++
	return json.Marshal(map[string]any{"uuid": r.UUID, "randid": r.RandId, "name": "raw:" + r.Name})
}

func (r *rawItem) UnmarshalRedis(data []byte) error {
	rawUnmarshals++
	var fields map[string]string
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	r.Record = &Record{UUID: fields["uuid"], RandId: fields["randid"]}
	r.Name = strings.TrimPrefix(fields["name"], "raw:")
	return nil
}

func TestBaseUsesTheItemsRawCodec(t *testing.T) {
	server, client := newTestRedis(t)
	ctx := context.Background()
	base, err := NewBase[*rawItem](client, "raw:%s", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	item := &rawItem{Name: "first"}
	InitRecord(item)
	rawMarshals, rawUnmarshals = 0, 0
	if err := base.Set(ctx, item); err != nil {
		t.Fatal(err)
	}
	pipe := client.Pipeline()
	second := &rawItem{Name: "second"}
	InitRecord(second)
	if err := base.WithPipeline(pipe).SetIfAbsent(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if rawMarshals != 2 {
		t.Fatalf("MarshalRedis ran %d times, want 2", rawMarshals)
	}
	stored, _ := server.Get("raw:" + item.RandId)
	if !strings.Contains(stored, `"name":"raw:first"`) {
		t.Fatalf("stored %s, not what MarshalRedis wrote", stored)
	}
	got, err := base.Get(ctx, item.RandId)
	if err != nil || got.Name != "first" || got.UUID != item.UUID {
		t.Fatalf("Get: %+v, %v", got, err)
	}
	many, err := base.GetMany(ctx, []string{item.RandId, second.RandId, "missing"})
	if err != nil || len(many) != 2 || many[second.RandId].Name != "second" {
		t.Fatalf("GetMany: %+v, %v", many, err)
	}
	if rawUnmarshals != 3 {
		t.Fatalf("UnmarshalRedis ran %d times, want 3", rawUnmarshals)
	}
}

func TestBaseWithoutRawCodecStillUsesJSON(t *testing.T) {
	_, client := newTestRedis(t)
	ctx := context.Background()
	base, err := NewBase[*Account](client, "account:%s", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	account := newAccount(t, "json")
	if err := base.Set(ctx, account); err != nil {
		t.Fatal(err)
	}
	got, err := base.Get(ctx, account.RandId)
	if err != nil || got.Name != "json" {
		t.Fatalf("Get: %+v, %v", got, err)
	}
	if _, err := decodeItem[*Account]([]byte("{")); err == nil {
		t.Fatal("broken JSON decoded")
	}
	if data, err := encodeItem[*rawItem](nil); err != nil || string(data) != "null" {
		t.Fatalf("a nil raw item encodes as %s, %v", data, err)
	}
}
