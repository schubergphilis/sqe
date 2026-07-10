# Async Trino statement protocol (issue #2)

Status: design, awaiting review. Branch `fix/trino-async-statement-protocol`.

## Problem

SQE's Trino-compat HTTP endpoint (`crates/sqe-trino-compat/src/server.rs`)
executes a statement synchronously inside the `POST /v1/statement` handler:
`submit_query` calls `Q::execute(session, sql)`, which runs the whole query and
materializes every result batch, and only then builds the response. One HTTP
call therefore spans the entire query.

Measured against the live demo: `POST /v1/statement` for a 167.9M-row CTAS
returns after 59.7s with `state=FINISHED`, no `nextUri`, 0 polls. The work is
fine (server-side `count(*)` = 0.6s, the CTAS = ~46s). But dbt-trino uses a
hardcoded 30s request timeout (`request_timeout`, not exposed in the profile)
and fails with "Read timed out" on `br_meter_readings`, so the EnergyCo
medallion cannot build at demo scale.

Real Trino returns the POST in ~1s with `state=QUEUED` + a `nextUri`, then the
client polls `nextUri` (each GET bounded by a small server-side `maxWait` ~1s)
while the query runs in the background, until results are ready. No single HTTP
call spans the whole query, so client request timeouts never fire.

This is independent of the #363 read_parquet cache fix: the failing statement
is a CTAS (a write) that never touches the directory-listing path, and the
fault is in the wire protocol, not the read path.

## Goal

No single HTTP call spans the whole query. `POST /v1/statement` and every
`nextUri` GET return within a bounded `maxWait`, reporting `QUEUED`/`RUNNING`
while the query runs in a background task; clients poll until results. Fixes
long queries for all Trino clients (dbt, Superset, JDBC) with request timeouts.

## Approach

Background execution + polling (chosen over a streaming-results rework and over
a non-viable "quick-return threshold"):

- Spawn `Q::execute` in a tokio task; the POST and each poll return within a
  bounded `maxWait` reporting `QUEUED`/`RUNNING`; once finished, serve pages
  from the **existing** result cache.
- Keep the current materialize-all semantics for `Q::execute` (a huge `SELECT`
  still buffers fully — pre-existing behavior, explicitly out of scope). This
  is the smallest change that meets the acceptance criteria.
- No new concurrency cap: the coordinator's DataFusion memory pool already
  bounds memory/spill, and blocking execution already loaded the system the
  same way. A max-concurrent-queries semaphore can be added later if load
  testing shows a need.

The only genuinely new machinery is a query-state registry and moving the
`execute` call onto a background task. Result pagination is reused unchanged:
the finished `PaginatedResult` is inserted into the existing result cache
(`build_result_cache` / `get_results` / `build_page_response`).

## Components

### 1. Query-state registry (new)

`MokaCache<String /* query_id */, Arc<QueryHandle>>` on `TrinoState`, TTL- and
idle-evicted like the existing result cache.

```
enum QueryStatus {
    Queued,
    Running,
    Finished,            // PaginatedResult is in the result cache under query_id
    Failed(TrinoError),  // preserves the mapped Trino error code/message
    Cancelled,
}

struct QueryHandle {
    status: Mutex<QueryStatus>,
    notify: tokio::sync::Notify,          // woken on every status transition
    abort: tokio::task::AbortHandle,      // for DELETE / eviction
    owner_username: String,               // cancel authorization (mirrors PaginatedResult)
    session_headers: TrinoSessionHeaders, // set-catalog/schema/session-props to echo
}
```

`session_headers`: the set/added session state a client must observe. SET / USE
/ PREPARE / DEALLOCATE / SHOW SESSION / DESCRIBE remain **synchronous
short-circuits** in `submit_query` (they are instant and already produce
immediate results), so the async path only carries the ordinary
statement-level session response headers, captured when the task finishes.

### 2. `submit_query` (POST /v1/statement)

Unchanged: authentication, and all current synchronous short-circuits.

For an ordinary statement:
1. Generate `query_id`.
2. Register `QueryHandle` with `status = Queued`.
3. `tokio::spawn` the execution task:
   - run `Q::execute(session, sql)`;
   - on `Ok(batches)`: build `PaginatedResult`, insert into the result cache
     under `query_id`, set `status = Finished`, `notify_waiters()`;
   - on `Err(e)`: set `status = Failed(TrinoError::from_sqe_error(e))`,
     `notify_waiters()`;
   - store the task's `AbortHandle` on the handle (set `status = Running` when
     the task starts).
4. Await completion up to `maxWait = min(client maxWait, server cap)`
   (`tokio::time::timeout` over `notify.notified()`; server cap default 1s,
   hard max ~10s).
   - **Finished within the wait** -> return the first page exactly as today
     (`state=FINISHED`, columns + data, `nextUri` to the results route if more
     pages). Preserves the fast-query inline UX.
   - **Otherwise** -> return a "started" response: no columns, no data,
     `state` reported via stats as `RUNNING`, `nextUri` ->
     `/v1/statement/queued/{query_id}/1`, plus the usual `infoUri`.

### 3. `get_queued_results` (new: GET /v1/statement/queued/{id}/{token})

1. Look up `QueryHandle`; missing -> Trino "query not found" error (mirrors the
   current `get_results` behavior for an unknown id).
2. `Queued`/`Running`: await up to `maxWait`.
   - still running -> `state=RUNNING`, no data, `nextUri` ->
     `.../queued/{id}/{token+1}` (token increments each poll; value is opaque).
   - finished during the wait -> fall through to the finished case.
3. `Finished`: return a status-only response (no data) whose `nextUri` points at
   the **results** route `/v1/statement/{id}/0`. The queued route stays purely a
   status/redirect endpoint; the results route stays purely data-paging. This
   keeps the two namespaces cleanly separated and is protocol-valid because
   clients only ever follow `nextUri` opaquely (standard Trino queued ->
   executing -> results handoff).
4. `Failed(e)`: Trino error JSON (`state=FAILED`, error object, `nextUri=None`).
5. `Cancelled`: Trino error ("query was canceled").

`nextUri` is opaque to clients, so the queued-route -> results-route handoff is
transparent and matches how real Trino hands out distinct queued/executing/
results URIs.

### 4. `get_results` (GET /v1/statement/{id}/{token})

Unchanged. Serves cached pages once the query is finished.

### 5. `cancel_query` (DELETE /v1/statement/{id})

Abort the background task via the `AbortHandle`, set `status = Cancelled`, evict
from the registry. Keep the existing owner-username authorization check.

## Error handling

- `Q::execute` error -> `Failed(TrinoError)`; the next poll returns the same
  Trino error JSON the synchronous path returns today
  (`TrinoError::from_sqe_error`), so error codes stay consistent with Flight SQL.
- Unknown / evicted `query_id` -> Trino "query not found" error.
- Task panic -> treated as `Failed` with an internal error.

## Lifecycle / cleanup

- Registry: moka idle + TTL eviction; on eviction of a still-`Running` query,
  abort its task (an abandoned client must not leak a running query forever).
- Finished results: the existing result cache already TTL-evicts them.
- `maxWait`: parse the `maxWait` query param Trino appends (e.g. `"1s"`); clamp
  to `[0, server cap]`.

## Testing

- Unit tests with a mock `Q` whose `execute` blocks on a released channel /
  barrier (deterministic, no real stack):
  - slow query: POST returns `QUEUED` + `nextUri` before completion;
  - poll returns `RUNNING`; after release, the next poll returns `FINISHED`
    with the data and correct paging;
  - fast query (execute returns immediately): POST returns the data inline;
  - failing `execute`: poll returns `FAILED` with the mapped error;
  - DELETE aborts a running query -> subsequent poll reports cancelled/gone.
- Assert the POST response for a still-running query carries a `nextUri` and no
  `data` field (the two properties dbt-trino needs to start polling).
- Manual/integration check: the reproduce CTAS
  (`CREATE TABLE ... AS SELECT * FROM read_parquet('s3://energyco-raw/raw/meter_readings/')`)
  returns promptly with a `nextUri` and polls to completion.

## Acceptance

- `POST /v1/statement` for the 167.9M CTAS returns promptly (within `maxWait`,
  ~1s) with `state` not FINISHED and a `nextUri`.
- Polls return within ~seconds while `RUNNING`, then deliver results.
- The full EnergyCo dbt medallion builds at demo scale (167.9M) with the default
  30s dbt-trino timeout: bronze(6) -> silver(7) -> gold(6), 19/19 PASS.
- Fast queries still return inline; existing Flight SQL path unaffected.

## Out of scope

- Streaming / incremental result delivery (would rework the `Q::execute`
  contract and the coordinator result path).
- Concurrency queueing / max-concurrent-queries semaphore.
- Per-query result-byte accounting beyond the existing memory pool + result
  cache weigher.
