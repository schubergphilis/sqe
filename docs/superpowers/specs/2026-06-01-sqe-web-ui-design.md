# SQE web UI: read-only ops dashboard

Date: 2026-06-01
Status: approved (user, 2026-06-01). Read-only scope, network-gated, embedded
in the coordinator. Ready for an implementation plan.

## Decisions (locked)

- **Scope:** read-only observability only. Queries (running + history), per-query
  tasks/fragments, worker/cluster health. No SQL console, no cancel, no admin or
  policy views in v1.
- **Auth:** none. Network-gated, ops-only. The UI binds to the existing internal
  health port and is protected at the network layer (like the Prometheus
  `/metrics` endpoint). Anyone who can reach it sees every user's queries; this
  is the accepted trade for an ops dashboard.
- **Placement:** embedded in the coordinator, on the existing health server
  (`metrics_port + 1`). No new crate, no new port, no separate deployable.
- **Frontend:** one server-rendered HTML page embedded via `include_str!`, plain
  JavaScript, no framework and no build step (no Node toolchain in a Rust repo).
- **Model to mirror:** Ballista 53's REST observability shapes
  (`JobResponse` / `QueryStageSummary` / `TaskSummary` / `ExecutorResponse`) and
  Trino's UI layout, mapped onto SQE's existing in-memory types. See
  `docs/ballista-evaluation-learnings.md` (borrowable ideas).

## Goal

Give operators a live view of what the engine is doing without scraping logs or
standing up Grafana: which queries are running or recently finished, how long
each phase took, how many rows/bytes they touched, which worker ran each
fragment, and whether the workers are healthy. The data already exists in the
coordinator (`QueryTracker`, `WorkerRegistry`); today it has no HTTP surface.

## Non-goals (YAGNI for v1)

- No login / OIDC / per-user filtering (network-gated instead).
- No interactive SQL console, no query cancel button.
- No GRANT/REVOKE or policy views, no catalog browser.
- No charting library, no websockets/SSE (polling is enough for an ops view).
- No persistence beyond what `QueryTracker` already keeps (its moka history).

These are clean phase-2 candidates (a query console + OIDC is the obvious next
step) but are explicitly out of scope here.

## Architecture

The coordinator already runs an axum health server (`start_health_server` in
`crates/sqe-coordinator/src/bin/sqe_server.rs`) on `metrics_port + 1`, serving
`/healthz`, `/readyz`, and a Ballista-style `/api/v1/status`. Its `HealthState`
already carries `Arc<WorkerRegistry>`. The UI is additive: more routes on that
same `Router`, and one more field on `HealthState`.

```
Browser (internal network)
  -> GET /                 (HTML dashboard, embedded static page)
  -> GET /api/v1/queries   (poll ~2s)  ─┐
  -> GET /api/v1/queries/:id            ├─ axum health server, metrics_port+1
  -> GET /api/v1/workers                │   HealthState { worker_registry,
  -> GET /api/v1/status   (unchanged)  ─┘                 query_tracker, load_tracker }
```

### Wiring change

`QueryTracker` is currently constructed *after* `start_health_server` is called.
Move its construction earlier and add `Arc<QueryTracker>` (and the
`WorkerLoadTracker`, if not already reachable) to `HealthState`, so the handlers
can read it. There are **two** `start_health_server` call sites in
`sqe_server.rs`; both must pass the populated state. This is the only change to
existing code paths; the legacy/bespoke execution path is untouched.

## Components

### 1. JSON API (new handlers, e.g. `crates/sqe-coordinator/src/web_ui.rs`)

Serde DTOs that are stable wire contracts, decoupled from the internal structs
so internal refactors do not break the UI. Field names mirror Ballista where
sensible.

- `GET /api/v1/queries?state=<running|finished|failed|all>&limit=<n>`
  Returns recent-first from `QueryTracker.records()`:
  ```
  [{ query_id, state, user, source, sql,           // sql truncated to ~512 chars
     created, started, ended,
     queued_ms, planning_ms, execution_ms,
     output_rows, rows_scanned, bytes_scanned,
     spill_bytes, peak_memory_bytes,
     error_type, error_code, error_message }]
  ```
  `limit` defaults to a sane cap (e.g. 200). `state` filters; `all` (default)
  returns everything in the history window.

- `GET /api/v1/queries/:id`
  The full record plus its fragments:
  ```
  { ...query fields..., tables_touched: [...],
    fragments: [{ task_id, worker_url, state, elapsed_ms,
                  input_rows, output_rows }] }
  ```
  404 if the id is unknown / aged out of history.

- `GET /api/v1/workers`
  From `WorkerRegistry` + `WorkerLoadTracker`:
  ```
  { workers: [{ url, healthy, in_flight }],
    total, healthy_count, active_queries }
  ```

- `GET /api/v1/status`: unchanged.

### 2. Frontend (`GET /`, single embedded HTML page + vanilla JS)

One page, three tabs, Trino/Ballista-style. Plain `fetch` + `setInterval`
(~2s). No framework, no bundler; CSS inline or a single embedded stylesheet.

- **Queries**: table of recent queries, rows colored by state (running /
  finished / failed). Columns: id (short), user, state, SQL (elided), elapsed,
  rows, bytes. Click a row -> detail.
- **Query detail**: the queue / planning / execution timing breakdown, the
  totals (rows, bytes, spill, peak memory), and the fragments table showing each
  task's worker, state, elapsed, and row counts.
- **Cluster**: workers table (url, healthy, in-flight load) and the active-query
  count. (Reuses `/api/v1/status` for the version/uptime header.)

### Data flow

Browser polls the three JSON endpoints on a timer and re-renders the active tab.
Stateless server side: each request reads the current `QueryTracker` /
`WorkerRegistry` snapshot. No server push.

## Error handling

- Handlers always return `200` with valid JSON. A missing tracker or registry
  (single-node, no workers configured) yields empty arrays and zero counts, not
  an error; the UI renders "no workers" / "no queries."
- `GET /api/v1/queries/:id` returns `404` for an unknown id (the only non-200).
- A failed poll in the browser shows a small banner ("lost connection,
  retrying") and keeps the last successfully rendered data rather than blanking.
- The SQL field is truncated server-side to bound payload size; full SQL is not
  needed for an ops glance and avoids shipping huge statements.

## Config

`[metrics] web_ui = true` (default on). The dashboard is already behind the
internal health port, so default-on is consistent with `/api/v1/status`; the
flag lets an operator turn the HTML + query endpoints off while keeping
`/healthz` / `/readyz` / `/api/v1/status` (which stay regardless of the flag).

## Testing

- Unit: DTO serialization (`QueryRecord` -> queries-list JSON; a record with
  fragments -> detail JSON), the worker mapping (`WorkerRegistry` snapshot ->
  workers JSON), and the `state`/`limit` query-param filtering.
- Handler: build a `HealthState` with a `QueryTracker` holding a few records and
  assert `/api/v1/queries` and `/api/v1/queries/:id` return them; assert `:id`
  404s for an unknown id.
- Smoke: `GET /` returns `200` with `content-type: text/html`.
- No integration/stack dependency; all of the above run as unit tests in
  `sqe-coordinator`.

## Build sequence

1. **API + wiring.** Add `Arc<QueryTracker>` to `HealthState`, reorder its
   construction, add the three JSON handlers + DTOs and their routes on both
   `start_health_server` call sites. Unit-test the DTOs and handlers. GREEN.
2. **Frontend.** Add the embedded HTML/JS page at `/`, wire the three tabs to the
   endpoints, poll. Smoke-test `/`. Manual check against a running coordinator
   with a live query load.
3. **Config + docs.** Add the `web_ui` flag, document the port + endpoints in the
   README/ops docs, update the roadmap.

Each step ends green and committed. The whole feature is additive and behind an
internal port, so it carries no risk to the query path.

## References

- Existing server: `crates/sqe-coordinator/src/bin/sqe_server.rs`
  (`start_health_server`, `HealthState`, `ClusterStatus`, `/api/v1/status`).
- Data sources: `crates/sqe-coordinator/src/query_tracker.rs`
  (`QueryRecord`, `FragmentInfo`, `records()`), `worker_registry.rs`
  (`WorkerRegistry`, `WorkerLoadTracker`).
- Shapes mirrored: `docs/ballista-evaluation-learnings.md` (Ballista REST API
  borrowable ideas).
