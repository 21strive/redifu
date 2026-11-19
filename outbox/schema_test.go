package outbox

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// The schema is the outbox and nothing of an application: no transaction of its own, no setting,
// no watched table.
func TestSchemaHoldsTheOutboxAndNothingElse(t *testing.T) {
	for _, object := range []string{
		"CREATE TABLE IF NOT EXISTS redifu_outbox (",
		"CREATE INDEX IF NOT EXISTS redifu_outbox_owner_id ON redifu_outbox (owner_uuid, id);",
		"CREATE OR REPLACE FUNCTION redifu_owner_of(spec text, data jsonb) RETURNS text",
		"CREATE OR REPLACE FUNCTION redifu_capture() RETURNS trigger",
		"CREATE OR REPLACE FUNCTION redifu_watch(target regclass, key_column text, owner_spec text) RETURNS void",
		"CREATE OR REPLACE FUNCTION redifu_unwatch_all() RETURNS integer",
	} {
		if !strings.Contains(Schema, object) {
			t.Errorf("Schema lacks %q", object)
		}
	}
	if !strings.HasPrefix(Schema, "CREATE TABLE IF NOT EXISTS redifu_outbox (") || !strings.HasSuffix(Schema, "$$;\n") {
		t.Errorf("Schema does not start with the table and end with the last function")
	}
	for _, foreign := range []string{"BEGIN;", "COMMIT;", "SET LOCAL", "SELECT redifu_watch("} {
		if strings.Contains(Schema, foreign) {
			t.Errorf("Schema holds %q, which belongs to the application's migration", foreign)
		}
	}
}

// The schema installs into an empty database, and again over itself without an error: an
// application's migration tool may run it more than once. An empty namespace stands for the empty
// database, so the test needs no right to create one; search_path makes every unqualified name of
// the schema resolve there. What it installed then captures writes.
func TestSchemaInstallsTwiceIntoAnEmptyDatabase(t *testing.T) {
	dsn := os.Getenv("REDIFU_TEST_DSN")
	if dsn == "" {
		t.Skip("REDIFU_TEST_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	namespace := fmt.Sprintf("zz_schema_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE SCHEMA ` + namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP SCHEMA ` + namespace + ` CASCADE`) })

	inNamespace := func(fn func(tx *sql.Tx) error) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.Exec(`SET LOCAL search_path TO ` + namespace); err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit()
	}
	for install := 1; install <= 2; install++ {
		if err := inNamespace(func(tx *sql.Tx) error {
			_, err := tx.Exec(Schema)
			return err
		}); err != nil {
			t.Fatalf("install %d: %v", install, err)
		}
	}

	var table, index bool
	if err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL`,
		namespace+".redifu_outbox", namespace+".redifu_outbox_owner_id").Scan(&table, &index); err != nil {
		t.Fatal(err)
	}
	if !table || !index {
		t.Fatalf("after two installs: table %v, index %v", table, index)
	}
	var functions string
	if err := db.QueryRow(`SELECT string_agg(p.proname, ',' ORDER BY p.proname) FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1`, namespace).Scan(&functions); err != nil {
		t.Fatal(err)
	}
	if functions != "redifu_capture,redifu_owner_of,redifu_unwatch_all,redifu_watch" {
		t.Fatalf("functions after two installs: %s", functions)
	}

	// The installed functions at work: a row's own owner column, an owner read from the parent row,
	// an UPDATE that changes nothing, and one that moves the row to another owner.
	var queued string
	if err := inNamespace(func(tx *sql.Tx) error {
		for _, statement := range []string{
			`CREATE TABLE items (uuid varchar PRIMARY KEY, owner varchar NOT NULL, name text)`,
			`CREATE TABLE parents (uuid uuid PRIMARY KEY, owner varchar NOT NULL)`,
			`CREATE TABLE children (uuid varchar PRIMARY KEY, parent_uuid uuid NOT NULL)`,
			`SELECT redifu_watch('items', 'uuid', 'owner')`,
			`SELECT redifu_watch('items', 'uuid', 'owner')`, // attaching twice keeps one trigger
			`SELECT redifu_watch('children', 'uuid', 'parent:parents:parent_uuid:owner')`,
			`INSERT INTO parents VALUES ('6f1c0a7e-4d5b-4c3a-9e2f-1a2b3c4d5e6f', 'p1')`,
			`INSERT INTO items VALUES ('a', 'o1', 'A')`,
			`UPDATE items SET name = 'A' WHERE uuid = 'a'`,
			`UPDATE items SET owner = 'o2' WHERE uuid = 'a'`,
			`DELETE FROM items WHERE uuid = 'a'`,
			`INSERT INTO children VALUES ('c', '6f1c0a7e-4d5b-4c3a-9e2f-1a2b3c4d5e6f')`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return fmt.Errorf("%s: %w", statement, err)
			}
		}
		return tx.QueryRow(`SELECT string_agg(format('%s %s %s %s %s', table_name, row_key, op, owner_uuid,
			COALESCE(old_row ->> 'owner', '-')), '; ' ORDER BY id) FROM redifu_outbox`).Scan(&queued)
	}); err != nil {
		t.Fatal(err)
	}
	want := "items a INSERT o1 -; items a UPDATE o2 o1; items a UPDATE o1 o1; items a DELETE o2 o2; children c INSERT p1 -"
	if queued != want {
		t.Fatalf("queued:\n got %s\nwant %s", queued, want)
	}
	var triggers int
	if err := db.QueryRow(`SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND t.tgname = 'redifu_capture'`, namespace).Scan(&triggers); err != nil {
		t.Fatal(err)
	}
	if triggers != 2 {
		t.Fatalf("%d capture triggers, want one per watched table", triggers)
	}
}
