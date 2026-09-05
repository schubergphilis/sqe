# 403: Flight SQL CancelQuery cannot cancel anything, and streamed queries have no wall-clock timeout

- Scope: sqe-coordinator, read side (flight_sql.rs, streaming.rs, query_handler.rs, query_tracker.rs)
- Severity: high
- Category: reliability
- Effort: M (days)
- Status: open

## Problem

**1. The client never learns the query id.** `get_flight_info_statement` puts the raw SQL in the
ticket (`crates/sqe-coordinator/src/flight_sql.rs:1216-1220`, `handle: sql.clone()`), and the
`query_id` is minted later inside `execute_stream`
(`crates/sqe-coordinator/src/query_handler.rs:2929`). `do_action_cancel_query` then tries to
parse the ticket as a UUID and, when that fails, returns `result: 0` (UNSPECIFIED):

- `crates/sqe-coordinator/src/flight_sql.rs:2266-2300`
  ```rust
  // if it's a SQL string (legacy ticket format) we cannot map it to a tracked query yet.
  let cancelled = if let Ok(uuid) = uuid::Uuid::parse_str(&query_id) { ... } else { ... false };
  ```

Since every ticket this server issues is the SQL string, the Flight SQL cancel action is a
no-op for every client (ADBC, JDBC, dbt-sqe). The only working cancel is the admin web API.

**2. Cancellation is observed only on poll.** `TrackedRecordBatchStream::poll_next` checks
`token.is_cancelled()` at the top (`crates/sqe-coordinator/src/streaming.rs:786-794`) but never
registers a waker on the token. A stream parked in `Poll::Pending` on a slow worker or a
blocked gRPC send is not woken by `cancel()`; it notices at the next poll or when the idle
timer fires (up to `stream_idle_timeout_secs`, default 300).

**3. `query.timeout_secs` is not a deadline for streams.** The batch path races the whole
execution against `tokio::time::timeout`
(`crates/sqe-coordinator/src/query_handler.rs:2131-2157`). The streaming path instead passes
the value to `with_query_deadline` and the deadline is consulted only inside the idle branch:

- `crates/sqe-coordinator/src/streaming.rs:843-858`: `within_deadline` is evaluated only when
  `idle_fired && progressed`. A stream that yields a batch every few seconds resets the idle
  timer each time (`:797-799`) and never reaches that branch, so it runs indefinitely. The
  comment at `query_handler.rs:3040-3043` says this is "where `query.timeout_secs` gets enforced
  for streams", which is not what the code does.

**4. Tracked state can vanish while the query runs.** `QueryTracker::history` is a moka cache
with `time_to_live(ttl_secs)` (`crates/sqe-coordinator/src/query_tracker.rs:125-130`, default
1800 s from `sqe-core/src/config.rs:560-562`). TTL is reset on re-insert, but a query in
`Running` with no state change for 30 minutes (role timeout overrides allow this) is evicted
while its token stays in `active`. `owner_of` then returns `None`, so even the UUID cancel path
reports "not found"; `complete()`/`failed()` become no-ops and the query never appears in
history.

## Why it matters

- A BI tool's "stop query" button does nothing; the coordinator keeps the semaphore permit,
  the per-user memory reservation, and the worker fragments until the idle timer or the
  client disconnect. Under `max_concurrent_per_user = 20` a user can lock themselves out for
  five minutes per abandoned query.
- Operators believe `timeout_secs` bounds every query. For the dominant path (Flight SQL
  SELECT) it does not.
- Active-query loss from the tracker makes the admin dashboard and the audit trail wrong for
  exactly the long queries that matter.

## Proposed change

1. Mint the `query_id` in `get_flight_info_statement` / `do_action_create_prepared_statement`,
   store `(query_id -> sql, session_id)` in a short-TTL map (the existing `prepared_params`
   moka cache pattern at `flight_sql.rs:489-503` fits), and put the UUID in the ticket.
   `do_get` looks the SQL up by id and passes the id into `execute_stream` so the tracker and
   the ticket agree. Cancel then works as written.
2. In `TrackedRecordBatchStream::poll_next`, poll `cancel_token.cancelled()` as a future
   (store a `Pin<Box<WaitForCancellationFutureOwned>>`) so cancel wakes the task.
3. Add a real deadline: an absolute `tokio::time::Sleep` polled on every `Pending`, independent
   of the idle timer, that aborts with the existing "timed out after Ns" error.
4. Split `QueryTracker` into `active: DashMap<Uuid, Arc<QueryRecord>>` (no TTL) and
   `history: Cache` (TTL) so an in-flight record cannot be evicted; move it to `history` on
   terminal state.

## Acceptance criteria

- `tests/it/flight_sql_smoke.rs`: start a long `SELECT` via ADBC, call `CancelQuery` with the
  returned `FlightInfo`, assert result `CANCELLED` and that the tracker shows `Canceled` within
  one second.
- Unit test in `streaming.rs`: an inner stream that yields one batch every 100 ms with
  `with_query_deadline(500 ms)` errors at about 500 ms.
- Unit test: `cancel()` on a stream whose inner is `Pending` forever resolves the next
  `poll_next` without waiting for the idle timer.
- Unit test: a record in `Running` past `ttl_secs` is still returned by `owner_of`.

## Related

- 2026-07-25 review P0-4 (coordinator state SPOF) still holds; a UUID ticket is also the
  first step toward resumable result handles named there.
- 406 (prepared statement design) shares the ticket format.
