// Package outbox keeps redifu collections in Redis in line with a PostgreSQL database, whatever
// writes to it.
//
// The database records every change itself. Schema installs the outbox; redifu_watch attaches a
// row trigger to each cached table, which queues one redifu_outbox row in the transaction of the
// write. A worker, a webhook, a library or a backfill in psql reaches the cache as surely as a
// request does, and a rolled-back write never reaches it.
//
// A Store applies the queue to Redis per owner — the account whose views a row belongs to — under
// that owner's advisory lock. The background drain (RunDrainLoop) keeps the queue short; a read
// applies its owner's queue first (Serve) or answers from SQL, so a read that starts after a write
// committed sees it, whichever process wrote it. A seed of a cold index takes the same lock and
// reads one REPEATABLE READ snapshot, so it never interleaves with a drain of that owner. Redis is
// never the source of truth: any failure sends the read to SQL, and a flushed Redis seeds itself
// again.
//
// The application writes the Handlers: given the changes of one drain batch, a handler reloads the
// rows through batch.Tx and enqueues the Redis writes into batch.Pipe. The write paths write to the
// database only. Reads go through List (a whole list in one Sorted index), Buckets (a time range
// kept per UTC month), Feed (a cursor-paged Timeline) and Detail (entities by key); each answers
// ok == false when the cache cannot answer now, and the caller reads SQL.
//
// Two builds may share one Redis and one outbox during a rolling deploy. Every Base and index the
// store builds carries a generation (NewBase, NewSorted, NewTimeline), and a lease in Redis lets
// one build at a time — the newest with a process alive — use the cache, its drains fenced.
//
// The package works through sqlx with any database/sql driver, and imports none.
package outbox
