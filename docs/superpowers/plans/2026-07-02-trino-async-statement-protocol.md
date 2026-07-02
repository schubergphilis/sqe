# Async Trino Statement Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `POST /v1/statement` and every poll return within a bounded wait so no single HTTP call spans the whole query, fixing dbt-trino's 30s request timeout on large CTAS.

**Architecture:** Add a query-state registry (`query_id -> Arc<QueryHandle>`) to `TrinoState`. `submit_query` spawns `Q::execute` on a tokio task, waits up to a bounded `maxWait` for completion, and either returns the first page inline (fast queries, unchanged UX) or a "started" response whose `nextUri` points at a new `GET /v1/statement/queued/{id}/{token}` status route. Clients poll that route (each poll bounded by `maxWait`) until the query finishes, at which point the poll redirects to the existing results-paging route. Finished results reuse the existing result cache and `get_results` path unchanged.

**Tech Stack:** Rust, axum, tokio (`spawn`, `Notify`, `time::timeout`, `task::AbortHandle`), moka sync cache, serde_json.

## Global Constraints

- Branch: `fix/trino-async-statement-protocol` (already checked out).
- All work is inside crate `sqe-trino-compat` (`crates/sqe-trino-compat/src/server.rs` and `crates/sqe-trino-compat/src/protocol.rs`). No changes to `sqe-coordinator`, `sqe-core`, or the Flight SQL path.
- Existing behavior must not regress: fast queries still return their first page inline from the POST; the existing `get_results` / `cancel_query` / result-cache paths keep working; SET SESSION / RESET SESSION / PREPARE / DEALLOCATE stay synchronous short-circuits in `submit_query`.
- `Session` is `Clone + Send + Debug`; `A: TrinoAuthenticator` and `Q: TrinoQueryExecutor` are already bound `Send + Sync + 'static`. `TrinoError` derives `Clone`. `UpdatedSessionState` derives `Clone + Default`. `MokaCache` is cheap-`Clone` (Arc-backed).
- Clippy is strict (`-D warnings`). Run `cargo clippy -p sqe-trino-compat --all-targets -- -D warnings` before the final commit.
- Test command for this crate: `cargo test -p sqe-trino-compat`.

---

### Task 1: Query-state registry types + wiring into `TrinoState`

Adds `QueryStatus`, `QueryHandle`, a registry field on `TrinoState`, and a `build_query_registry()` constructor whose eviction listener aborts still-running tasks. Every `TrinoState { .. }` literal (one production site + ~15 test sites) gains the new field.

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs` (imports near line 1-14; new types after `PaginatedResult` ~line 112; `TrinoState` struct ~line 55; `build_result_cache` region ~line 38; `start_trino_server_with_options` state literal ~line 248; all `TrinoState { .. }` literals in `#[cfg(test)]`)
- Test: `crates/sqe-trino-compat/src/server.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Produces:
  - `pub enum QueryStatus { Queued, Running, Finished, Failed(protocol::TrinoError), Cancelled }` with `impl QueryStatus { pub fn is_terminal(&self) -> bool }` (true for `Finished`/`Failed`/`Cancelled`).
  - `pub struct QueryHandle { pub status: std::sync::Mutex<QueryStatus>, pub notify: tokio::sync::Notify, pub abort: std::sync::Mutex<Option<tokio::task::AbortHandle>>, pub owner_username: String, pub session_update: Option<protocol::UpdatedSessionState>, pub created_at: std::time::Instant }`
  - `fn build_query_registry() -> MokaCache<String, Arc<QueryHandle>>`
  - New `TrinoState` field: `pub queries: MokaCache<String, Arc<QueryHandle>>`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module in `server.rs`:

```rust
#[test]
fn query_registry_stores_and_reports_terminal_status() {
    let registry = build_query_registry();
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Running),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "alice".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    });
    registry.insert("q1".to_string(), handle.clone());

    let got = registry.get("q1").expect("handle present");
    assert!(!got.status.lock().unwrap().is_terminal());

    *got.status.lock().unwrap() = QueryStatus::Finished;
    assert!(got.status.lock().unwrap().is_terminal());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-trino-compat query_registry_stores_and_reports_terminal_status`
Expected: FAIL to compile — `QueryHandle`, `QueryStatus`, `build_query_registry` not defined.

- [ ] **Step 3: Add imports, types, registry constructor, and the state field**

Add near the other `const` declarations (after line 36):

```rust
/// A query-registry entry idle (un-polled) for this long is evicted. This is
/// `time_to_idle`, NOT `time_to_live`: every poll's `queries.get(&id)` counts
/// as an access and resets the timer, so a query that runs longer than this
/// but is actively polled is never reaped. Only a genuinely abandoned client
/// (no poll for this long) triggers eviction, whose listener aborts the
/// still-running background task. Using `time_to_live` here would abort any
/// query still executing at the 300s mark mid-flight — the opposite of the
/// feature's purpose.
const QUERY_REGISTRY_IDLE_SECS: u64 = 300;

/// Default bounded wait applied to the POST and to a poll with no explicit
/// `maxWait`: no single HTTP call blocks longer than this on query progress.
const DEFAULT_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Hard upper bound a client-supplied `maxWait` is clamped to.
const MAX_WAIT_CAP: std::time::Duration = std::time::Duration::from_secs(10);
```

Add after the `PaginatedResult` block (after line 112 / after `estimate_paginated_bytes` is fine — place it right before `// ── Trino /v1/info`):

```rust
/// Lifecycle state of a submitted statement executing on a background task.
#[derive(Debug)]
pub enum QueryStatus {
    /// Registered, background task not yet observed to start.
    Queued,
    /// Background task is running `Q::execute`.
    Running,
    /// Finished successfully; the `PaginatedResult` is in the result cache
    /// under the same `query_id`.
    Finished,
    /// Execution failed; carries the mapped Trino error to replay on poll.
    Failed(protocol::TrinoError),
    /// Cancelled via DELETE or evicted while still running.
    Cancelled,
}

impl QueryStatus {
    /// True once the query will never transition again.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryStatus::Finished | QueryStatus::Failed(_) | QueryStatus::Cancelled
        )
    }
}

/// Shared handle for a statement executing in the background. Stored in the
/// query registry under `query_id`; the background task and every poll share
/// the same `Arc<QueryHandle>`.
#[derive(Debug)]
pub struct QueryHandle {
    /// Current lifecycle state; mutated by the background task and by cancel.
    pub status: std::sync::Mutex<QueryStatus>,
    /// Woken on every status transition so waiting polls re-check promptly.
    pub notify: tokio::sync::Notify,
    /// Abort handle for the background task; set just after spawn.
    pub abort: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    /// Username that submitted the query; used for poll/cancel authorization.
    pub owner_username: String,
    /// Session-state mutation (USE / SET CATALOG) to echo as response headers
    /// when the query finishes. `None` for ordinary statements.
    pub session_update: Option<protocol::UpdatedSessionState>,
    /// Registration time; used for diagnostics.
    pub created_at: std::time::Instant,
}

/// Build the query-state registry. Idle-evicts abandoned entries and, on
/// eviction of a still-running query, aborts its background task.
fn build_query_registry() -> MokaCache<String, Arc<QueryHandle>> {
    MokaCache::builder()
        .time_to_idle(std::time::Duration::from_secs(QUERY_REGISTRY_IDLE_SECS))
        .eviction_listener(|_key, handle: Arc<QueryHandle>, _cause| {
            let mut status = handle.status.lock().unwrap();
            if !status.is_terminal() {
                if let Some(abort) = handle.abort.lock().unwrap().as_ref() {
                    abort.abort();
                }
                *status = QueryStatus::Cancelled;
            }
        })
        .build()
}
```

Add the field to the `TrinoState` struct (after `pub results: ...` at line 58):

```rust
    /// In-flight / recently-finished query lifecycle handles, keyed by
    /// `query_id`. Distinct from `results`, which holds finished page data.
    pub queries: MokaCache<String, Arc<QueryHandle>>,
```

Add `queries: build_query_registry(),` to the production state literal in `start_trino_server_with_options` (after `results: build_result_cache(),` at line 251).

- [ ] **Step 4: Add `queries: build_query_registry(),` to every test `TrinoState { .. }` literal**

Every `Arc::new(TrinoState::<...> { ... results: build_result_cache(), ... })` in the `#[cfg(test)]` module must gain `queries: build_query_registry(),`. There are ~15 (grep to confirm): lines ~1292, 1321, 1345, 1369, 1469 (`recording_state`), 1758, 2220, 2251, 2298, 2370, 2422, 2464, 2546, 2592, 2636, 2667.

Run to find them all:
```bash
grep -n "results: build_result_cache()," crates/sqe-trino-compat/src/server.rs
```
Add the line immediately after each match.

- [ ] **Step 5: Run test to verify it passes and the crate compiles**

Run: `cargo test -p sqe-trino-compat query_registry_stores_and_reports_terminal_status`
Expected: PASS. Then `cargo test -p sqe-trino-compat` compiles (all existing tests still pass — the new field is additive).

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "feat(#2): query-state registry types and TrinoState wiring"
```

---

### Task 2: `TrinoStats::queued()` + queued-route response builders

Adds a `QUEUED` stats constructor and the three server-side response builders the async path needs: a "started" response (POST couldn't finish in time), a still-running poll response, and a finished-redirect response that hands the client to the results route.

**Files:**
- Modify: `crates/sqe-trino-compat/src/protocol.rs` (`impl TrinoStats` ~line 561)
- Modify: `crates/sqe-trino-compat/src/server.rs` (pagination-helpers region, after `info_uri` ~line 695)
- Test: both files' `#[cfg(test)]` modules

**Interfaces:**
- Produces:
  - `TrinoStats::queued() -> TrinoStats` (state `"QUEUED"`, `queued=true`, `scheduled=false`).
  - `fn queued_uri(base_url: &str, query_id: &str, token: usize) -> String` -> `"{base_url}/v1/statement/queued/{query_id}/{token}"`.
  - `fn build_started_response(base_url: &str, query_id: &str) -> TrinoResponse` — `state=QUEUED`, `next_uri` -> `queued_uri(.., 1)`, no columns/data.
  - `fn build_running_response(base_url: &str, query_id: &str, next_token: usize) -> TrinoResponse` — `state=RUNNING`, `next_uri` -> `queued_uri(.., next_token)`, no columns/data.
  - `fn build_finished_redirect_response(base_url: &str, query_id: &str) -> TrinoResponse` — `state=RUNNING`, `next_uri` -> `"{base_url}/v1/statement/{query_id}/0"` (the results route, token 0), no columns/data.
- Consumes: `next_uri`/`info_uri` helpers and `TrinoResponse`/`TrinoStats` from earlier code.

- [ ] **Step 1: Write the failing tests**

In `protocol.rs` `#[cfg(test)]`:

```rust
#[test]
fn trino_stats_queued_state() {
    let s = TrinoStats::queued();
    assert_eq!(s.state, "QUEUED");
    assert!(s.queued);
    assert!(!s.scheduled);
}
```

In `server.rs` `#[cfg(test)]`:

```rust
#[test]
fn started_response_points_at_queued_route_without_data() {
    let resp = build_started_response("http://h:8080", "q1");
    assert_eq!(resp.stats.state, "QUEUED");
    assert_eq!(
        resp.next_uri.as_deref(),
        Some("http://h:8080/v1/statement/queued/q1/1")
    );
    assert!(resp.data.is_none());
    assert!(resp.columns.is_none());
}

#[test]
fn running_response_increments_queued_token() {
    let resp = build_running_response("http://h:8080", "q1", 4);
    assert_eq!(resp.stats.state, "RUNNING");
    assert_eq!(
        resp.next_uri.as_deref(),
        Some("http://h:8080/v1/statement/queued/q1/4")
    );
    assert!(resp.data.is_none());
}

#[test]
fn finished_redirect_response_points_at_results_route() {
    let resp = build_finished_redirect_response("http://h:8080", "q1");
    assert_eq!(resp.stats.state, "RUNNING");
    assert_eq!(
        resp.next_uri.as_deref(),
        Some("http://h:8080/v1/statement/q1/0")
    );
    assert!(resp.data.is_none());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-trino-compat trino_stats_queued_state started_response_points running_response_increments finished_redirect_response`
Expected: FAIL to compile — builders not defined.

- [ ] **Step 3: Implement `TrinoStats::queued()`**

Add inside `impl TrinoStats` (after `failed()` ~line 610) in `protocol.rs`:

```rust
    /// Stats for a query that has been accepted but not yet started/finished.
    pub fn queued() -> Self {
        Self {
            state: "QUEUED".to_string(),
            queued: true,
            scheduled: false,
            nodes: 0,
            total_splits: 0,
            queued_splits: 0,
            running_splits: 0,
            completed_splits: 0,
            cpu_time_millis: 0,
            wall_time_millis: 0,
            queued_time_millis: 0,
            elapsed_time_millis: 0,
            processed_rows: 0,
            processed_bytes: 0,
            physical_input_bytes: 0,
            peak_memory_bytes: 0,
            spilled_bytes: 0,
            root_stage: None,
        }
    }
```

- [ ] **Step 4: Implement the server response builders**

Add after `info_uri` (~line 695) in `server.rs`:

```rust
/// Build a `queued`-route URI (the status/poll namespace).
fn queued_uri(base_url: &str, query_id: &str, token: usize) -> String {
    format!("{base_url}/v1/statement/queued/{query_id}/{token}")
}

/// Response for a POST whose query did not finish within the bounded wait:
/// no data, `state=QUEUED`, `nextUri` -> the queued poll route (token 1).
fn build_started_response(base_url: &str, query_id: &str) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(queued_uri(base_url, query_id, 1)),
        stats: TrinoStats::queued(),
        ..Default::default()
    }
}

/// Response for a poll on a query still running: no data, `state=RUNNING`,
/// `nextUri` -> the next queued poll token.
fn build_running_response(base_url: &str, query_id: &str, next_token: usize) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(queued_uri(base_url, query_id, next_token)),
        stats: TrinoStats::running(0, 1),
        ..Default::default()
    }
}

/// Response for a poll on a query that just finished: no data, `state=RUNNING`,
/// `nextUri` -> the results-paging route at token 0. The queued route stays a
/// status/redirect endpoint; the results route stays pure data paging.
fn build_finished_redirect_response(base_url: &str, query_id: &str) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(format!("{base_url}/v1/statement/{query_id}/0")),
        stats: TrinoStats::running(0, 1),
        ..Default::default()
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p sqe-trino-compat trino_stats_queued_state started_response_points running_response_increments finished_redirect_response`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-trino-compat/src/protocol.rs crates/sqe-trino-compat/src/server.rs
git commit -m "feat(#2): QUEUED stats and queued-route response builders"
```

---

### Task 3: Extract `run_statement` + `build_paginated_result` helpers (pure refactor)

Pull the statement-dispatch (DESCRIBE prepared / SHOW SESSION / execute) and the batches-to-`PaginatedResult` post-processing out of `submit_query` into two free functions, so the background task in Task 5 can call them without duplicating logic. No behavior change: `submit_query` stays synchronous and its existing tests stay green.

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs` (`submit_query` lines ~997-1085; add helpers before `submit_query` ~line 758)
- Test: existing `submit_*` tests are the regression guard.

**Interfaces:**
- Produces:
  - ```rust
    async fn run_statement<Q: TrinoQueryExecutor>(
        handler: &Q,
        session: &Session,
        exec_sql: &str,
        prepared: &std::collections::HashMap<String, String>,
        show_session_props: &[(String, String)],
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError>
    ```
    (dispatches DESCRIBE prepared / SHOW SESSION / `handler.execute`, replacing the `exec_result` match at lines 997-1018).
  - ```rust
    fn build_paginated_result(
        batches: Vec<arrow_array::RecordBatch>,
        exec_sql: &str,
        session_catalog: Option<&str>,
        page_size: usize,
        owner_username: String,
    ) -> PaginatedResult
    ```
    (the info-schema/explain reshape + classify + paginate block at lines 1022-1075).
- Consumes: `prepared::parse_prepared_statements` output type is `HashMap<String, String>` (confirm with `prepared::` module); `incoming_session_properties(&headers) -> Vec<(String, String)>`.

- [ ] **Step 1: Confirm the prepared-map type**

Run:
```bash
grep -n "pub fn parse_prepared_statements\|pub fn get\|HashMap" crates/sqe-trino-compat/src/prepared.rs | head
```
Expected: `parse_prepared_statements` returns a `HashMap<String, String>` (or a newtype wrapping one). If it is a newtype, `run_statement`'s `prepared` parameter takes `&PreparedStatements` instead and calls `.get(&name)`; adjust the signature accordingly. Note the exact type before proceeding.

- [ ] **Step 2: Add `run_statement` and `build_paginated_result` helpers**

Insert before `submit_query` (before the `#[tracing::instrument...]` at line 760). Use the exact prepared-map type from Step 1 (shown here as `HashMap<String, String>`):

```rust
/// Dispatch a resolved statement to the right executor path: DESCRIBE
/// prepared, SHOW SESSION, or a normal `execute`. Mirrors the interception
/// order in `submit_query` so async execution behaves identically.
async fn run_statement<Q: TrinoQueryExecutor>(
    handler: &Q,
    session: &Session,
    exec_sql: &str,
    prepared: &std::collections::HashMap<String, String>,
    show_session_props: &[(String, String)],
) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
    if let Some((kind, name)) = protocol::parse_describe_prepared(exec_sql) {
        match prepared.get(&name) {
            Some(prepared_sql) => handler.describe_prepared(session, prepared_sql, kind).await,
            None => Err(sqe_core::SqeError::Execution(format!(
                "Prepared statement not found: {name}"
            ))),
        }
    } else if let Some(like) = protocol::parse_show_session(exec_sql) {
        Ok(build_show_session_batches(show_session_props, like.as_deref()))
    } else {
        handler.execute(session, exec_sql).await
    }
}

/// Turn a successful statement's record batches into a `PaginatedResult`:
/// apply info-schema/EXPLAIN Trino-compat reshaping, classify the update
/// type/count, and paginate. Pure post-processing shared by the sync and
/// async paths.
fn build_paginated_result(
    batches: Vec<arrow_array::RecordBatch>,
    exec_sql: &str,
    session_catalog: Option<&str>,
    page_size: usize,
    owner_username: String,
) -> PaginatedResult {
    let update_type = classify_update_type(exec_sql).map(str::to_string);
    let update_count = if update_type.is_some() {
        extract_update_count(&batches).or(Some(0))
    } else {
        None
    };
    let batches = if info_schema_compat::is_metadata_query(exec_sql) {
        let batches = info_schema_compat::apply_info_schema_compat(batches, session_catalog);
        if info_schema_compat::is_describe_or_show_columns(exec_sql) {
            info_schema_compat::reshape_describe_to_trino(batches)
        } else {
            batches
        }
    } else {
        batches
    };
    let batches = if explain_compat::is_explain(exec_sql) {
        explain_compat::reshape_explain_to_trino(batches)
    } else {
        batches
    };
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let (columns, data) = protocol::batches_to_trino(&batches);
    let pages = paginate_rows(data, page_size);
    let total_pages = pages.len();
    let estimated_bytes = estimate_paginated_bytes(&pages, &columns);
    PaginatedResult {
        columns,
        pages,
        total_pages,
        total_rows,
        created_at: std::time::Instant::now(),
        owner_username,
        update_type,
        update_count,
        estimated_bytes,
    }
}
```

- [ ] **Step 3: Rewrite the `submit_query` body to call the helpers**

Replace lines ~997-1085 (the `let exec_result = if ... else ...;` block through the `Ok(batches) => { ... resp }` arm) so the dispatch and post-processing go through the helpers. The `exec_result` computation becomes:

```rust
    let show_session_props = incoming_session_properties(&headers);
    let exec_result = run_statement(
        state.query_handler.as_ref(),
        &session,
        exec_sql,
        &prepared,
        &show_session_props,
    )
    .await;

    match exec_result {
        Ok(batches) => {
            let paginated = build_paginated_result(
                batches,
                exec_sql,
                session.default_catalog.as_deref(),
                state.page_size,
                session.user.username.clone(),
            );
            let response = build_page_response(&base_url, &query_id, &paginated, 0);
            state.results.insert(query_id, Arc::new(paginated));
            let mut resp = (StatusCode::OK, Json(response)).into_response();
            if let Some(ref update) = session_update {
                apply_session_headers(resp.headers_mut(), update);
            }
            resp
        }
        Err(sqe_err) => {
            // unchanged error arm (lines ~1087-1112)
        }
    }
```

Keep the `Err` arm exactly as it is today. Note `incoming_session_properties` moves above the dispatch (it was computed inline in the old SHOW SESSION branch); confirm no second call remains.

- [ ] **Step 4: Run the full crate test suite to verify no regression**

Run: `cargo test -p sqe-trino-compat`
Expected: PASS — all existing `submit_set_session_echoes...`, `submit_show_session_returns...`, `submit_resolves_prepared...`, `submit_metadata_query_translates...`, `submit_unresolved_execute...`, `test_submit_query_bearer_*` tests still green.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "refactor(#2): extract run_statement and build_paginated_result helpers"
```

---

### Task 4: Bounded-wait helper + `maxWait` parsing

Adds the wait primitive the POST and polls use, plus a `maxWait` query-param parser that clamps to the server cap.

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs` (helpers region; imports)
- Test: `#[cfg(test)]` module

**Interfaces:**
- Produces:
  - `async fn await_terminal_or_timeout(handle: &QueryHandle, max_wait: std::time::Duration)` — returns as soon as `handle.status` is terminal, or after `max_wait` elapses, whichever first.
  - `fn clamp_max_wait(raw: Option<&str>) -> std::time::Duration` — parses a Trino duration string (`"1s"`, `"500ms"`, `"2000ms"`, `"1m"`); clamps to `[Duration::ZERO, MAX_WAIT_CAP]`; falls back to `DEFAULT_MAX_WAIT` on absent/unparseable input.
- Consumes: `QueryHandle` (Task 1), `DEFAULT_MAX_WAIT` / `MAX_WAIT_CAP` (Task 1).

- [ ] **Step 1: Write the failing tests**

In the `#[cfg(test)]` module:

```rust
#[test]
fn clamp_max_wait_parses_and_clamps() {
    use std::time::Duration;
    assert_eq!(clamp_max_wait(Some("500ms")), Duration::from_millis(500));
    assert_eq!(clamp_max_wait(Some("2s")), Duration::from_secs(2));
    assert_eq!(clamp_max_wait(Some("1m")), MAX_WAIT_CAP); // 60s clamped to cap
    assert_eq!(clamp_max_wait(Some("garbage")), DEFAULT_MAX_WAIT);
    assert_eq!(clamp_max_wait(None), DEFAULT_MAX_WAIT);
}

#[tokio::test]
async fn await_terminal_returns_when_finished() {
    let handle = QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Running),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "u".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    };
    let handle = Arc::new(handle);
    let h2 = handle.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        *h2.status.lock().unwrap() = QueryStatus::Finished;
        h2.notify.notify_waiters();
    });
    // Long budget; must return well before it via the notify.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        await_terminal_or_timeout(&handle, std::time::Duration::from_secs(5)),
    )
    .await
    .expect("returned before outer timeout");
    assert!(handle.status.lock().unwrap().is_terminal());
}

#[tokio::test]
async fn await_terminal_returns_on_timeout_when_still_running() {
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Running),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "u".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    });
    await_terminal_or_timeout(&handle, std::time::Duration::from_millis(30)).await;
    assert!(!handle.status.lock().unwrap().is_terminal()); // timed out, still running
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-trino-compat clamp_max_wait await_terminal`
Expected: FAIL to compile — helpers not defined.

- [ ] **Step 3: Implement the helpers**

Add to the helpers region of `server.rs`:

```rust
/// Wait until `handle.status` is terminal or `max_wait` elapses. A missed
/// `notify_waiters` (tokio `Notify` does not store permits) only defers to the
/// next client poll — correctness holds, at most one extra round-trip.
async fn await_terminal_or_timeout(handle: &QueryHandle, max_wait: std::time::Duration) {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        {
            if handle.status.lock().unwrap().is_terminal() {
                return;
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        // Ignore the timeout result: on either arm we loop and re-check status,
        // and the deadline check above terminates the loop when time is up.
        let _ = tokio::time::timeout(remaining, handle.notify.notified()).await;
    }
}

/// Parse Trino's `maxWait` duration string and clamp to `[0, MAX_WAIT_CAP]`.
/// Absent or unparseable input falls back to `DEFAULT_MAX_WAIT`.
fn clamp_max_wait(raw: Option<&str>) -> std::time::Duration {
    let parsed = raw.and_then(parse_trino_duration);
    match parsed {
        Some(d) => d.min(MAX_WAIT_CAP),
        None => DEFAULT_MAX_WAIT,
    }
}

/// Parse a Trino duration literal: an integer followed by `ms`, `s`, or `m`.
fn parse_trino_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        return num.trim().parse::<u64>().ok().map(std::time::Duration::from_millis);
    }
    if let Some(num) = s.strip_suffix('s') {
        return num.trim().parse::<u64>().ok().map(std::time::Duration::from_secs);
    }
    if let Some(num) = s.strip_suffix('m') {
        return num
            .trim()
            .parse::<u64>()
            .ok()
            .map(|m| std::time::Duration::from_secs(m * 60));
    }
    None
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sqe-trino-compat clamp_max_wait await_terminal`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "feat(#2): bounded-wait helper and maxWait parsing"
```

---

### Task 5: Rewire `submit_query` to spawn + bounded wait

Turn the synchronous execution into a background task, register a `QueryHandle`, wait up to `DEFAULT_MAX_WAIT`, and return either the inline first page (finished in time) or the started response.

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs` (`submit_query` execution section, replacing the Task 3 synchronous block; imports for `HashMap`, `tokio::spawn`)
- Test: `#[cfg(test)]` module (add a controllable blocking mock executor)

**Interfaces:**
- Consumes: `QueryHandle`/`QueryStatus`/`build_query_registry` (Task 1), `run_statement`/`build_paginated_result` (Task 3), `await_terminal_or_timeout`/`DEFAULT_MAX_WAIT` (Task 4), `build_started_response` (Task 2).
- Produces: no new public symbols; `submit_query` now returns a started response for slow queries and inline data for fast ones.

- [ ] **Step 1: Write the failing tests**

Add a controllable mock and two tests to `#[cfg(test)]`. The mock blocks `execute` until a channel is fired, so the test deterministically forces the "did not finish in time" path.

```rust
struct GatedQuery {
    // Fired by the test to release execute(); execute returns an empty result.
    gate: Arc<tokio::sync::Notify>,
}

impl TrinoQueryExecutor for GatedQuery {
    async fn execute(
        &self,
        _session: &Session,
        _sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
        self.gate.notified().await;
        Ok(vec![])
    }
}

fn gated_state(
    gate: Arc<tokio::sync::Notify>,
) -> Arc<TrinoState<MockAuthOk, GatedQuery>> {
    Arc::new(TrinoState::<MockAuthOk, GatedQuery> {
        authenticator: Arc::new(MockAuthOk),
        query_handler: Arc::new(GatedQuery { gate }),
        results: build_result_cache(),
        queries: build_query_registry(),
        node: NodeContext {
            version: "test".to_string(),
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
        },
        page_size: DEFAULT_PAGE_SIZE,
        port: 8080,
        oauth2: None,
        security: SecurityConfig::default(),
        auth_rate_limiter: None,
        expose_version: false,
    })
}

#[tokio::test]
async fn submit_slow_query_returns_started_response_with_next_uri() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate.clone());
    let resp = submit_query(
        State(state.clone()),
        test_peer(),
        basic_auth_header("alice", "pw"),
        "SELECT 1".to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
    // Did not finish in the bounded wait: a nextUri to the queued route, no data.
    assert!(json.next_uri.as_deref().unwrap().contains("/v1/statement/queued/"));
    assert!(json.data.is_none());
    assert_ne!(json.stats.state, "FINISHED");
    // Release so the background task does not leak.
    gate.notify_waiters();
}

#[tokio::test]
async fn submit_fast_query_returns_inline_finished() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate.clone());
    // Release immediately so execute completes within the bounded wait.
    gate.notify_waiters();
    // Small settle so the pre-fired notify is observed; then submit.
    let resp = submit_query(
        State(state.clone()),
        test_peer(),
        basic_auth_header("alice", "pw"),
        "SELECT 1".to_string(),
    )
    .await;
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
    // A columnless empty result still reports FINISHED with no further nextUri.
    assert_eq!(json.stats.state, "FINISHED");
    assert!(json.next_uri.is_none());
}
```

Note: `Vec<RecordBatch>` = `vec![]` yields `total_pages == 1` (an empty page) and no columns, so `build_page_response` reports FINISHED with `data: None` and no `next_uri`. If `submit_fast_query_returns_inline_finished` proves flaky because the pre-fired `notify_waiters()` races the task's first poll, switch `GatedQuery` to a `tokio::sync::Mutex<Option<oneshot::Receiver>>` gate or an `AtomicBool` fast-path checked before `notified()`. Prefer the `AtomicBool` fast-path: add `open: Arc<AtomicBool>` to `GatedQuery`, and in `execute` return immediately if `open` is set, else await the notify.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-trino-compat submit_slow_query submit_fast_query`
Expected: FAIL — `submit_query` still runs synchronously, so the slow test blocks/`await`s forever on the gate (it will hang until the test harness timeout, or fail the assertion once you implement Step 3). Confirm compile failure first if `queries`/mock not yet present.

- [ ] **Step 3: Rewire `submit_query` execution**

Replace the Task 3 synchronous block (the `run_statement(...).await` + `match exec_result` section) with the spawn + wait version. `prepared` is `HashMap<String, String>` (Task 3 Step 1); clone the owned values the task captures:

```rust
    let show_session_props = incoming_session_properties(&headers);

    // Register the query handle before spawning so a fast poll always finds it.
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Queued),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: session.user.username.clone(),
        session_update: session_update.clone(),
        created_at: std::time::Instant::now(),
    });
    state.queries.insert(query_id.clone(), handle.clone());

    // Move owned state into the background task.
    let task_handler = state.query_handler.clone();
    let task_results = state.results.clone();
    let task_handle = handle.clone();
    let task_query_id = query_id.clone();
    let task_session = session.clone();
    let task_sql = effective_sql.clone();
    let task_prepared = prepared.clone();
    let task_props = show_session_props.clone();
    let task_catalog = session.default_catalog.clone();
    let page_size = state.page_size;
    let owner = session.user.username.clone();

    let task = tokio::spawn(async move {
        // Drop guard: if `run_statement` panics (or the task is otherwise torn
        // down) without a terminal status set, force `Failed` on unwind so the
        // next poll returns an error instead of the client polling a
        // never-terminal `Running` entry forever (idle eviction would never
        // fire because each poll resets the idle timer). Normal completion sets
        // a terminal status first, so this no-ops.
        struct TerminalGuard(Arc<QueryHandle>);
        impl Drop for TerminalGuard {
            fn drop(&mut self) {
                let mut s = self.0.status.lock().unwrap();
                if !s.is_terminal() {
                    *s = QueryStatus::Failed(protocol::TrinoError::user_error(
                        "query task terminated unexpectedly",
                        None,
                    ));
                    self.0.notify.notify_waiters();
                }
            }
        }
        let _guard = TerminalGuard(task_handle.clone());

        *task_handle.status.lock().unwrap() = QueryStatus::Running;
        task_handle.notify.notify_waiters();
        let result = run_statement(
            task_handler.as_ref(),
            &task_session,
            &task_sql,
            &task_prepared,
            &task_props,
        )
        .await;
        match result {
            Ok(batches) => {
                let paginated = build_paginated_result(
                    batches,
                    &task_sql,
                    task_catalog.as_deref(),
                    page_size,
                    owner,
                );
                task_results.insert(task_query_id, Arc::new(paginated));
                *task_handle.status.lock().unwrap() = QueryStatus::Finished;
            }
            Err(sqe_err) => {
                tracing::warn!(
                    error_code = %sqe_err.error_code(),
                    query_id = %task_query_id,
                    error = %sqe_err,
                    "Trino query execution failed"
                );
                let trino_error =
                    protocol::TrinoError::from_sqe_error(&sqe_err, Some(&task_query_id));
                *task_handle.status.lock().unwrap() = QueryStatus::Failed(trino_error);
            }
        }
        task_handle.notify.notify_waiters();
    });
    *handle.abort.lock().unwrap() = Some(task.abort_handle());

    // Bounded wait: return the first page inline if the query finished, else a
    // started response the client polls.
    await_terminal_or_timeout(&handle, DEFAULT_MAX_WAIT).await;

    let status_is_finished = matches!(*handle.status.lock().unwrap(), QueryStatus::Finished);
    if status_is_finished {
        if let Some(paginated) = state.results.get(&query_id) {
            let response = build_page_response(&base_url, &query_id, paginated.as_ref(), 0);
            let mut resp = (StatusCode::OK, Json(response)).into_response();
            if let Some(ref update) = session_update {
                apply_session_headers(resp.headers_mut(), update);
            }
            return resp;
        }
    }
    // Failed within the wait: replay the mapped Trino error immediately so fast
    // failures still surface on the POST rather than forcing a poll. Preserve
    // the `Retry-After` header the old synchronous error arm added for
    // ResourceExhausted (rate-limit/OOM), detected via the stable error name.
    if let QueryStatus::Failed(ref trino_error) = *handle.status.lock().unwrap() {
        let is_rate_limited = trino_error.error_name == "RESOURCE_EXHAUSTED";
        let response = TrinoResponse {
            id: query_id.clone(),
            info_uri: Some(info_uri(&base_url, &query_id)),
            stats: TrinoStats::failed(),
            error: Some(trino_error.clone()),
            ..Default::default()
        };
        let mut resp = (StatusCode::OK, Json(response)).into_response();
        if is_rate_limited {
            resp.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
        }
        return resp;
    }

    build_started_response(&base_url, &query_id)
        .pipe_response(&session_update)
```

Do NOT add a `pipe_response` extension — instead finish with the explicit form:

```rust
    let mut resp = (StatusCode::OK, Json(build_started_response(&base_url, &query_id)))
        .into_response();
    if let Some(ref update) = session_update {
        apply_session_headers(resp.headers_mut(), update);
    }
    resp
```

Add `use std::collections::HashMap;` only if not already imported (it is referenced via full path in Task 3; keep full paths to avoid an unused-import warning if the refactor changes). Remove the now-dead old `Ok`/`Err` match arms left from Task 3.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sqe-trino-compat submit_slow_query submit_fast_query`
Expected: PASS. Then run the full suite: `cargo test -p sqe-trino-compat` — existing `submit_*` tests still green (fast statements finish within the 1s wait and return inline exactly as before).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "feat(#2): submit_query spawns background task with bounded wait"
```

---

### Task 6: `get_queued_results` handler + route

Add the poll endpoint and register it. Handles running/finished/failed/cancelled/not-found and enforces the same owner-authorization as `get_results`.

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs` (new handler after `get_results` ~line 1208; route registration ~line 284)
- Test: `#[cfg(test)]` module

**Interfaces:**
- Produces:
  - ```rust
    async fn get_queued_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
        State(state): State<Arc<TrinoState<A, Q>>>,
        ConnectInfo(_peer): ConnectInfo<SocketAddr>,
        headers: HeaderMap,
        Path((id, token)): Path<(String, String)>,
        params: axum::extract::RawQuery,
    ) -> Response
    ```
  - Route: `.route("/v1/statement/queued/{id}/{token}", get(get_queued_results::<A, Q>))`, registered inside a new extracted `fn build_statement_router<A, Q>(state: Arc<TrinoState<A, Q>>) -> Router`.
- Consumes: `QueryHandle`/`QueryStatus` (Task 1), `await_terminal_or_timeout`/`clamp_max_wait` (Task 4), `build_running_response`/`build_finished_redirect_response` (Task 2).

- [ ] **Step 1: Write the failing tests**

Add to `#[cfg(test)]`. These call the handler directly. Use `axum::extract::RawQuery(None)` for no `maxWait`.

```rust
#[tokio::test]
async fn queued_poll_running_returns_running_with_incremented_token() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate.clone());
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Running),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "alice".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    });
    state.queries.insert("q1".to_string(), handle);
    let resp = get_queued_results(
        State(state),
        test_peer(),
        basic_auth_header("alice", "pw"),
        Path(("q1".to_string(), "3".to_string())),
        axum::extract::RawQuery(Some("maxWait=50ms".to_string())),
    )
    .await;
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(json.stats.state, "RUNNING");
    assert_eq!(
        json.next_uri.as_deref(),
        Some("http://localhost:8080/v1/statement/queued/q1/4")
    );
    assert!(json.data.is_none());
}

#[tokio::test]
async fn queued_poll_finished_redirects_to_results_route() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate.clone());
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Finished),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "alice".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    });
    state.queries.insert("q1".to_string(), handle);
    let resp = get_queued_results(
        State(state),
        test_peer(),
        basic_auth_header("alice", "pw"),
        Path(("q1".to_string(), "1".to_string())),
        axum::extract::RawQuery(None),
    )
    .await;
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json.next_uri.as_deref(),
        Some("http://localhost:8080/v1/statement/q1/0")
    );
    assert!(json.data.is_none());
}

#[tokio::test]
async fn queued_poll_failed_returns_error_json() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate.clone());
    let err = protocol::TrinoError::user_error("boom", Some("q1"));
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Failed(err)),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "alice".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    });
    state.queries.insert("q1".to_string(), handle);
    let resp = get_queued_results(
        State(state),
        test_peer(),
        basic_auth_header("alice", "pw"),
        Path(("q1".to_string(), "1".to_string())),
        axum::extract::RawQuery(None),
    )
    .await;
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: protocol::TrinoResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(json.stats.state, "FAILED");
    assert!(json.error.is_some());
    assert!(json.next_uri.is_none());
}

#[tokio::test]
async fn queued_poll_unknown_id_returns_not_found() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate);
    let resp = get_queued_results(
        State(state),
        test_peer(),
        basic_auth_header("alice", "pw"),
        Path(("missing".to_string(), "1".to_string())),
        axum::extract::RawQuery(None),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn queued_poll_rejects_non_owner() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate);
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Running),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "bob".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    });
    state.queries.insert("q1".to_string(), handle);
    let resp = get_queued_results(
        State(state),
        test_peer(),
        basic_auth_header("alice", "pw"), // MockAuthOk authenticates as "alice"
        Path(("q1".to_string(), "1".to_string())),
        axum::extract::RawQuery(Some("maxWait=10ms".to_string())),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-trino-compat queued_poll`
Expected: FAIL to compile — `get_queued_results` not defined.

- [ ] **Step 3: Implement the handler**

Add after `get_results` (after line 1208). Reuse the same auth block shape as `get_results` (bearer/basic/reject). Parse `maxWait` from the raw query string.

```rust
#[tracing::instrument(skip_all, name = "trino.get_queued_results")]
async fn get_queued_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    ConnectInfo(_peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, token)): Path<(String, String)>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let session = if let Some(bearer) = extract_bearer_token(&headers) {
        match state.authenticator.authenticate_bearer(&bearer).await {
            Ok(s) => s,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else if let Some((user, pass)) = extract_basic_auth(&headers) {
        match state.authenticator.authenticate(&user, &pass).await {
            Ok(s) => s,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    let base_url = extract_base_url(&headers, state.port);
    let next_token: usize = token.parse().unwrap_or(1).saturating_add(1);

    let handle = match state.queries.get(&id) {
        Some(h) => h,
        None => {
            // Unknown/evicted id -> Trino "query not found" (same shape as
            // get_results' None arm).
            let response = TrinoResponse {
                id: id.clone(),
                info_uri: Some(info_uri(&base_url, &id)),
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: "Query not found".to_string(),
                    error_code: 1,
                    error_name: "USER_ERROR".to_string(),
                    error_type: "USER_ERROR".to_string(),
                    query_id: None,
                    failure_info: None,
                    error_location: None,
                }),
                ..Default::default()
            };
            return (StatusCode::NOT_FOUND, Json(response)).into_response();
        }
    };

    if handle.owner_username != session.user.username {
        warn!(
            query_id = %id,
            caller = %session.user.username,
            owner = %handle.owner_username,
            "get_queued_results denied: caller does not own query"
        );
        return StatusCode::FORBIDDEN.into_response();
    }

    // Extract maxWait from the raw query string ("maxWait=1s").
    let max_wait_raw = raw_query.as_deref().and_then(|q| {
        q.split('&')
            .find_map(|kv| kv.strip_prefix("maxWait="))
    });
    let max_wait = clamp_max_wait(max_wait_raw);

    await_terminal_or_timeout(&handle, max_wait).await;

    let status = handle.status.lock().unwrap();
    match &*status {
        QueryStatus::Finished => {
            (StatusCode::OK, Json(build_finished_redirect_response(&base_url, &id)))
                .into_response()
        }
        QueryStatus::Failed(trino_error) => {
            let is_rate_limited = trino_error.error_name == "RESOURCE_EXHAUSTED";
            let response = TrinoResponse {
                id: id.clone(),
                info_uri: Some(info_uri(&base_url, &id)),
                stats: TrinoStats::failed(),
                error: Some(trino_error.clone()),
                ..Default::default()
            };
            let mut resp = (StatusCode::OK, Json(response)).into_response();
            if is_rate_limited {
                resp.headers_mut().insert(
                    axum::http::header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
            }
            resp
        }
        QueryStatus::Cancelled => {
            let response = TrinoResponse {
                id: id.clone(),
                info_uri: Some(info_uri(&base_url, &id)),
                stats: TrinoStats::failed(),
                error: Some(TrinoError::user_error("Query was canceled", Some(&id))),
                ..Default::default()
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        QueryStatus::Queued | QueryStatus::Running => {
            (StatusCode::OK, Json(build_running_response(&base_url, &id, next_token)))
                .into_response()
        }
    }
}
```

- [ ] **Step 4: Extract `build_statement_router` and register the route through it**

axum 0.8 uses `matchit`, which panics at `.route()` insert time if two patterns conflict at a segment. Asserting the 5-segment queued path "cannot collide" with the 4-segment results path is not enough — a bad registration panics at server startup, caught by nothing until deploy. Extract the router assembly into a function so a unit test can build it and catch a conflict panic at test time.

Add near `start_trino_server_with_options` (e.g. just before it):

```rust
/// Assemble the statement/info routes with state. Extracted so a unit test can
/// build the router and fail fast if a route pattern conflicts (matchit panics
/// at insert time).
fn build_statement_router<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    state: Arc<TrinoState<A, Q>>,
) -> Router {
    Router::new()
        .route("/v1/info", get(server_info::<A, Q>))
        .route("/v1/info/state", get(server_state::<A, Q>))
        .route("/v1/statement", post(submit_query::<A, Q>))
        .route("/v1/statement/{id}/{token}", get(get_results::<A, Q>))
        .route(
            "/v1/statement/queued/{id}/{token}",
            get(get_queued_results::<A, Q>),
        )
        .route("/v1/statement/{id}", delete(cancel_query::<A, Q>))
        .with_state(state)
}
```

Then in `start_trino_server_with_options`, replace the inline `let mut app = Router::new().route(...)....with_state(state);` block (lines ~280-287) with:

```rust
        let mut app = build_statement_router(state).layer(cors_layer);
```

(The `cors_layer` and the subsequent `oauth2` merge stay as they are; `.layer` and `.merge` apply to the `Router<()>` returned after `with_state`.)

- [ ] **Step 5: Add the router-build test**

In `#[cfg(test)]`:

```rust
#[tokio::test]
async fn statement_router_builds_without_route_conflict() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate);
    // Panics here if the queued route conflicts with the results route.
    let _router = build_statement_router(state);
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p sqe-trino-compat queued_poll statement_router_builds`
Expected: PASS (all six).

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "feat(#2): add get_queued_results poll handler and route"
```

---

### Task 7: `cancel_query` aborts the background task

Extend DELETE to abort a still-running task and mark the handle `Cancelled`, in addition to the existing result-cache invalidation.

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs` (`cancel_query` ~line 1210-1246)
- Test: `#[cfg(test)]` module

**Interfaces:**
- Consumes: `QueryHandle`/`QueryStatus` (Task 1). Keeps the existing owner check against `state.results` and now also checks `state.queries`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn cancel_aborts_running_query_and_marks_cancelled() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = gated_state(gate.clone());
    // Spawn a task that blocks on the gate; register its handle.
    let handle = Arc::new(QueryHandle {
        status: std::sync::Mutex::new(QueryStatus::Running),
        notify: tokio::sync::Notify::new(),
        abort: std::sync::Mutex::new(None),
        owner_username: "alice".to_string(),
        session_update: None,
        created_at: std::time::Instant::now(),
    });
    let gate2 = gate.clone();
    let task = tokio::spawn(async move {
        gate2.notified().await; // never released in this test
    });
    *handle.abort.lock().unwrap() = Some(task.abort_handle());
    state.queries.insert("q1".to_string(), handle.clone());

    let resp = cancel_query(
        State(state.clone()),
        test_peer(),
        basic_auth_header("alice", "pw"),
        Path("q1".to_string()),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(matches!(
        *handle.status.lock().unwrap(),
        QueryStatus::Cancelled
    ));
    assert!(task.is_finished()); // aborted
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-trino-compat cancel_aborts_running_query`
Expected: FAIL — `cancel_query` does not touch `state.queries`, so status stays `Running` and the task is not aborted.

- [ ] **Step 3: Extend `cancel_query`**

After the existing owner check against `state.results` (before `state.results.invalidate(&id);` at line 1244), add the registry abort. Also enforce ownership against the registry handle when the result entry is absent (the common case for an in-flight query — results are not inserted until the task finishes):

```rust
    // Abort the background task if the query is still in flight.
    if let Some(handle) = state.queries.get(&id) {
        if handle.owner_username != session.user.username {
            warn!(
                query_id = %id,
                caller = %session.user.username,
                owner = %handle.owner_username,
                "Cancel denied: caller does not own query"
            );
            return StatusCode::FORBIDDEN.into_response();
        }
        if let Some(abort) = handle.abort.lock().unwrap().as_ref() {
            abort.abort();
        }
        *handle.status.lock().unwrap() = QueryStatus::Cancelled;
        handle.notify.notify_waiters();
    }

    state.results.invalidate(&id);
    state.queries.invalidate(&id);
    StatusCode::NO_CONTENT.into_response()
```

Note: `state.queries.invalidate(&id)` fires the eviction listener, which is a no-op here because the status is already terminal (`Cancelled`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-trino-compat cancel_aborts_running_query`
Expected: PASS. Then run the existing `test_cancel_query_removes_result` to confirm the result-cache path still works: `cargo test -p sqe-trino-compat test_cancel_query_removes_result`.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "feat(#2): cancel_query aborts in-flight background task"
```

---

### Task 8: Full-suite + clippy verification and docs update

Run the whole crate's tests and clippy, then record status in project tracking files. No production-code changes unless clippy flags something.

**Files:**
- Modify: `nextsteps.md` (status line), `README.md` (roadmap checklist if a Trino-compat item exists)
- Test: whole `sqe-trino-compat` suite + clippy.

- [ ] **Step 1: Run the full crate test suite**

Run: `cargo test -p sqe-trino-compat`
Expected: PASS (all tests, old and new).

- [ ] **Step 2: Run clippy strict**

Run: `cargo clippy -p sqe-trino-compat --all-targets -- -D warnings`
Expected: no warnings. Fix any inline (common ones: unused imports left from the Task 3/5 refactor; a needless `clone()` — keep clones that move owned data into the spawned task).

- [ ] **Step 3: Build the whole workspace to confirm no downstream breakage**

Run: `cargo build -p sqe-coordinator`
Expected: builds — the coordinator constructs `TrinoState` only via `start_trino_server*`, so the added field is internal.

- [ ] **Step 4: Update tracking docs**

In `nextsteps.md`, add a status entry noting issue #2 (async Trino statement protocol) implemented on `fix/trino-async-statement-protocol`. In `README.md`, check off/add a Trino-compat async-statement roadmap item if the roadmap tracks it.

- [ ] **Step 5: Commit**

```bash
git add nextsteps.md README.md
git commit -m "docs(#2): record async Trino statement protocol status"
```

---

### Task 9 (manual, out-of-band): demo integration check

Not a code task; run once the branch is deployed to the demo stack. Reuses the #363 acceptance environment.

- [ ] **Step 1:** Deploy the branch to the demo coordinator.
- [ ] **Step 2:** From dbt-trino (default 30s `request_timeout`), run the EnergyCo medallion at demo scale (167.9M rows). Expected: `POST /v1/statement` for `br_meter_readings` returns within ~1s with a `nextUri` and no data; polls report `RUNNING`; the model completes; bronze(6) -> silver(7) -> gold(6) = 19/19 PASS.
- [ ] **Step 3:** Confirm a fast query (e.g. `SELECT 1`) still returns its data inline on the POST (no extra round-trip regression).

---

## Self-Review

**Spec coverage:**
- Query-state registry (spec §1) -> Task 1. `QueryStatus` enum + `QueryHandle` fields match the spec (`status`, `notify`, `abort`, `owner_username`, `session_headers` -> implemented as `session_update: Option<UpdatedSessionState>`, the concrete carrier of the "set session state to echo").
- `submit_query` spawn + bounded wait + inline-vs-started (spec §2) -> Tasks 3 (extract), 5 (rewire). Fast-fail replay on POST added (spec §2 "Finished within the wait" symmetry) — a failure within the wait returns the error inline rather than forcing a poll; consistent with the current synchronous UX.
- `get_queued_results` route + running/finished/failed/cancelled/not-found (spec §3) -> Task 6. Finished = status-only redirect to the results route (spec §3.3).
- `get_results` unchanged (spec §4) -> untouched; the redirect targets it at token 0.
- `cancel_query` aborts + marks cancelled (spec §5) -> Task 7.
- Error handling: `Q::execute` error -> `Failed(TrinoError::from_sqe_error)` (spec) -> Task 5 task body + Task 6 replay. Unknown id -> not found -> Task 6. Task panic -> the `TerminalGuard` Drop guard in the spawned task (Task 5) forces `Failed` on unwind, so the next poll returns an error rather than the client polling a never-terminal `Running` entry forever. Rate-limited (`RESOURCE_EXHAUSTED`) failures carry `Retry-After` on both the inline POST replay (Task 5) and the poll (Task 6), matching the old synchronous arm.
- Lifecycle/cleanup: registry `time_to_idle` (NOT `time_to_live`) + eviction-abort (Task 1 `build_query_registry`) — a polled query never gets reaped mid-flight; only an abandoned one (no poll for 300s) is evicted and its task aborted. Result TTL unchanged. `maxWait` clamp (spec) -> Task 4.
- Route safety: `build_statement_router` extraction + `statement_router_builds_without_route_conflict` test (Task 6) fail fast at test time if the queued route pattern conflicts with the results route (matchit panics at insert).
- Testing (spec): blocking mock (`GatedQuery`) -> Task 5; running/finished/failed/cancelled polls -> Task 6/7; started response carries `nextUri` and no `data` -> Task 5.
- Acceptance (spec): fast inline preserved (Task 5), started+poll+results handoff (Tasks 5/6), demo medallion (Task 9).
- Out of scope (streaming, concurrency queueing) -> not implemented, as specified.

**Placeholder scan:** No TBD/TODO. Every code step shows full code. The one deliberate note (Task 5 Step 1 flakiness fallback to an `AtomicBool` fast-path) is a concrete instruction, not a placeholder.

**Type consistency:** `QueryHandle` field names (`status`, `notify`, `abort`, `owner_username`, `session_update`, `created_at`) are identical across Tasks 1, 4, 5, 6, 7. `QueryStatus` variants (`Queued`, `Running`, `Finished`, `Failed(TrinoError)`, `Cancelled`) consistent throughout. `build_query_registry` / `build_started_response` / `build_running_response` / `build_finished_redirect_response` / `await_terminal_or_timeout` / `clamp_max_wait` / `run_statement` / `build_paginated_result` names are used verbatim where consumed. `prepared` map type is pinned in Task 3 Step 1 before use.
