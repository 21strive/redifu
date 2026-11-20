package outbox

import _ "embed"

// Schema is the outbox's PostgreSQL schema: the redifu_outbox table and its index
// redifu_outbox_owner_id, and the functions redifu_owner_of, redifu_capture (the trigger
// function), redifu_watch and redifu_unwatch_all.
//
// Run it through the application's migration tool, as one script: it holds several statements and
// dollar-quoted function bodies. Every statement is CREATE ... IF NOT EXISTS or CREATE OR REPLACE,
// so running it again changes nothing. It watches no table: a migration of the application calls
// redifu_watch for each table the cache follows.
//
//go:embed schema.sql
var Schema string
