# Audit Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all security vulnerabilities, silent failures, and bugs identified in the Step 1 codebase audit.

**Architecture:** Targeted fixes across 8 crates — no new features, only hardening existing code. Each task is independent and produces a compilable, testable result.

**Tech Stack:** Rust, tonic, axum, reqwest, dashmap, moka, tokio

---

## File Map

| File | Changes |
|------|---------|
| `crates/sqe-core/src/config.rs` | Add `worker_secret` field to `DistributedConfig` |
| `crates/sqe-coordinator/src/flight_sql.rs` | Authenticate heartbeat action |
| `crates/sqe-trino-compat/src/server.rs` | Sanitize error responses, fix bind panic |
| `crates/sqe-worker/src/executor.rs` | Gate `allow_http` on config |
| `crates/sqe-planner/src/scan_task.rs` | Add redacting `Debug` impl |
| `crates/sqe-coordinator/src/session_manager.rs` | Reduce session ID log level |
| `crates/sqe-catalog/src/rest_catalog.rs` | Fix `list_views` and `load_view_sql` error handling |
| `crates/sqe-catalog/src/schema_provider.rs` | Propagate errors, log view failures |
| `crates/sqe-catalog/src/info_schema.rs` | Log errors at `warn` level, not `debug` |
| `crates/sqe-metrics/src/audit.rs` | Return `Result` from `new()`, log write failures |
| `crates/sqe-metrics/src/server.rs` | Propagate bind errors |
| `crates/sqe-coordinator/src/query_handler.rs` | Fix `arrow_type_to_iceberg` fallback, `table_exists` error handling, log errors at correct level |
| `crates/sqe-coordinator/src/catalog_ops.rs` | Replace string-based 404 check with status code |

---

### Task 1: S1 — Authenticate heartbeat action

**Files:**
- Modify: `crates/sqe-core/src/config.rs` (DistributedConfig struct)
- Modify: `crates/sqe-coordinator/src/flight_sql.rs:800-818`

- [ ] **Step 1: Add `worker_secret` to DistributedConfig**

In `crates/sqe-core/src/config.rs`, add to `DistributedConfig`:
```rust
/// Shared secret that workers must present in heartbeat requests.
/// If empty, heartbeat auth is disabled (single-node mode).
#[serde(default)]
pub worker_secret: String,
```

- [ ] **Step 2: Validate secret in heartbeat handler**

In `flight_sql.rs`, modify `do_action_fallback`:
```rust
"heartbeat" => {
    // Validate worker secret if configured
    if let Some(ref expected_secret) = self.worker_secret {
        if !expected_secret.is_empty() {
            let provided = request.metadata()
                .get("x-sqe-worker-secret")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if provided != expected_secret {
                return Err(Status::unauthenticated("Invalid worker secret"));
            }
        }
    }
    // ... existing heartbeat logic
}
```

Note: The `worker_secret` needs to be stored on `SqeFlightSqlService`. Thread through from config during construction.

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-coordinator && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-core/src/config.rs crates/sqe-coordinator/src/flight_sql.rs
git commit -m "fix(security): authenticate worker heartbeat requests with shared secret"
```

---

### Task 2: S2 — Sanitize Trino error responses

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs:281,325-343`

- [ ] **Step 1: Sanitize query execution error in submit_query**

Replace the error branch (line 325-343):
```rust
Err(e) => {
    warn!(error = %e, sql = sql, "Trino query execution failed");
    let client_msg = format!("Query execution failed");
    let response = TrinoResponse {
        id: query_id,
        info_uri: None,
        next_uri: None,
        columns: None,
        data: None,
        stats: TrinoStats::failed(),
        error: Some(TrinoError {
            message: client_msg,
            error_code: 1,
            error_name: "INTERNAL_ERROR".to_string(),
            error_type: "INTERNAL_ERROR".to_string(),
        }),
    };
    (StatusCode::OK, Json(response)).into_response()
}
```

- [ ] **Step 2: Sanitize auth error**

Replace line 281 area:
```rust
Err(e) => {
    warn!(error = %e, "Trino authentication failed");
    return error_response(
        StatusCode::UNAUTHORIZED,
        "Authentication failed",
    );
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-trino-compat && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "fix(security): sanitize error messages in Trino compat HTTP responses"
```

---

### Task 3: S3 — Gate `allow_http` on ScanTask config

**Files:**
- Modify: `crates/sqe-planner/src/scan_task.rs` (add `s3_allow_http` field)
- Modify: `crates/sqe-worker/src/executor.rs:240` (use field instead of hardcoded `true`)
- Modify: `crates/sqe-coordinator/src/distributed_scan.rs` (propagate from StorageConfig)

- [ ] **Step 1: Add `s3_allow_http` field to ScanTask**

```rust
/// Whether to allow plaintext HTTP for S3. Should only be true for dev/test endpoints.
pub s3_allow_http: bool,
```

- [ ] **Step 2: Use field in executor**

Replace line 240:
```rust
builder = builder.with_allow_http(task.s3_allow_http);
```

- [ ] **Step 3: Propagate from StorageConfig when building ScanTask**

In `distributed_scan.rs`, wherever `ScanTask` is built, set `s3_allow_http` from config:
```rust
s3_allow_http: storage_config.s3_allow_http,
```

Add `s3_allow_http` to `StorageConfig` with `#[serde(default)]` (defaults to `false`).

- [ ] **Step 4: Update existing tests**

Update the `ScanTask` test fixtures in `scan_task.rs` to include the new field.

- [ ] **Step 5: Run tests**

Run: `cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-planner/src/scan_task.rs crates/sqe-worker/src/executor.rs crates/sqe-core/src/config.rs crates/sqe-coordinator/src/distributed_scan.rs
git commit -m "fix(security): gate S3 allow_http on config instead of hardcoding true"
```

---

### Task 4: S4 — Redact ScanTask Debug output

**Files:**
- Modify: `crates/sqe-planner/src/scan_task.rs`

- [ ] **Step 1: Replace derived Debug with manual impl**

Remove `Debug` from the `#[derive(...)]` and add:
```rust
impl std::fmt::Debug for ScanTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanTask")
            .field("fragment_id", &self.fragment_id)
            .field("data_file_paths", &self.data_file_paths)
            .field("projected_columns", &self.projected_columns)
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key", &"[REDACTED]")
            .field("s3_secret_key", &"[REDACTED]")
            .field("s3_session_token", &if self.s3_session_token.is_empty() { "[empty]" } else { "[REDACTED]" })
            .field("s3_path_style", &self.s3_path_style)
            .finish()
    }
}
```

- [ ] **Step 2: Add test for redaction**

```rust
#[test]
fn test_debug_redacts_credentials() {
    let task = ScanTask {
        fragment_id: "frag-001".to_string(),
        data_file_paths: vec![],
        projected_columns: vec![],
        s3_endpoint: "http://localhost:9000".to_string(),
        s3_region: "us-east-1".to_string(),
        s3_access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
        s3_secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        s3_session_token: "session-token-value".to_string(),
        s3_path_style: true,
    };
    let debug_output = format!("{task:?}");
    assert!(!debug_output.contains("AKIAIOSFODNN7EXAMPLE"));
    assert!(!debug_output.contains("wJalrXUtnFEMI"));
    assert!(!debug_output.contains("session-token-value"));
    assert!(debug_output.contains("[REDACTED]"));
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-planner && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-planner/src/scan_task.rs
git commit -m "fix(security): redact S3 credentials in ScanTask Debug output"
```

---

### Task 5: S5 — Reduce session ID log level

**Files:**
- Modify: `crates/sqe-coordinator/src/session_manager.rs:47-51`

- [ ] **Step 1: Change `info!` to `debug!` for session creation log**

```rust
debug!(
    session_id = %session_id,
    username = username,
    "Session created"
);
```

Keep the `info!`-level log but without the session ID:
```rust
info!(username = username, "Session created");
debug!(session_id = %session_id, username = username, "Session created (with ID)");
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p sqe-coordinator && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-coordinator/src/session_manager.rs
git commit -m "fix(security): stop logging session IDs at info level"
```

---

### Task 6: F1/F2 — Fix list_views and load_view_sql silent failures

**Files:**
- Modify: `crates/sqe-catalog/src/rest_catalog.rs:301-311,350-365`

- [ ] **Step 1: Fix list_views — return error on non-success HTTP**

Replace the silent `Ok(vec![])` (lines 301-303):
```rust
if !resp.status().is_success() {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    return Err(SqeError::Catalog(format!(
        "Failed to list views (HTTP {status}): {text}"
    )));
}
```

- [ ] **Step 2: Fix load_view_sql — distinguish 404 from other errors**

Replace lines 350-352:
```rust
if resp.status().as_u16() == 404 {
    return Ok(None);
}
if !resp.status().is_success() {
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    return Err(SqeError::Catalog(format!(
        "Failed to load view '{name}' (HTTP {status}): {text}"
    )));
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-catalog && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-catalog/src/rest_catalog.rs
git commit -m "fix: propagate HTTP errors from list_views and load_view_sql instead of silent empty results"
```

---

### Task 7: F3/F4 — Fix schema_provider silent failures

**Files:**
- Modify: `crates/sqe-catalog/src/schema_provider.rs:71-84,104-116`

- [ ] **Step 1: Log view list failures in table_names()**

Replace lines 80-84:
```rust
let views =
    tokio::task::block_in_place(|| handle.block_on(catalog.list_views(&ns_ident)));
match views {
    Ok(view_names) => names.extend(view_names),
    Err(e) => {
        error!(namespace = %ns, error = %e, "Failed to list views");
    }
}
```

- [ ] **Step 2: Propagate table load errors in table()**

Replace lines 112-116 — change the catch-all `Err(_)` to log the actual error:
```rust
Err(e) => {
    debug!(table = name, error = %e, "Not found as table, trying view");
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-catalog && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-catalog/src/schema_provider.rs
git commit -m "fix: log view list failures and table load errors in schema_provider"
```

---

### Task 8: F5 — Fix AuditLogger silent failures

**Files:**
- Modify: `crates/sqe-metrics/src/audit.rs`

- [ ] **Step 1: Return Result from AuditLogger::new()**

```rust
pub fn new(path: &str) -> Result<Self, String> {
    if path.is_empty() {
        return Ok(Self { writer: None });
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("Failed to open audit log file '{path}': {e}"))?;

    info!(path = path, "Audit log initialized");
    Ok(Self {
        writer: Some(Mutex::new(std::io::BufWriter::new(file))),
    })
}
```

- [ ] **Step 2: Log write failures in log()**

```rust
pub fn log(&self, entry: &AuditEntry) {
    if let Some(ref writer) = self.writer {
        let mut w = match writer.lock() {
            Ok(w) => w,
            Err(e) => {
                eprintln!("AUDIT: mutex poisoned, audit entry lost: {e}");
                return;
            }
        };
        let json = match serde_json::to_string(entry) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("AUDIT: serialization failed: {e}");
                return;
            }
        };
        if let Err(e) = writeln!(w, "{json}") {
            eprintln!("AUDIT: write failed: {e}");
        }
        if let Err(e) = w.flush() {
            eprintln!("AUDIT: flush failed: {e}");
        }
    }
}
```

- [ ] **Step 3: Update callers**

Find all callers of `AuditLogger::new()` and update them to handle the `Result`. The coordinator should fail to start if audit logging is configured but cannot be initialized.

- [ ] **Step 4: Update tests**

Update test calls: `AuditLogger::new("").unwrap()`, `AuditLogger::new(path_str).unwrap()`.

- [ ] **Step 5: Run tests**

Run: `cargo test -p sqe-metrics && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-metrics/src/audit.rs crates/sqe-coordinator/
git commit -m "fix: AuditLogger returns Result, logs write failures instead of silently dropping"
```

---

### Task 9: F6 — Fix server bind panics

**Files:**
- Modify: `crates/sqe-metrics/src/server.rs:18-29`
- Modify: `crates/sqe-trino-compat/src/server.rs:90-106`

- [ ] **Step 1: Fix metrics server**

```rust
pub fn start_metrics_server<R: HasRegistry + Clone>(
    metrics: Arc<R>,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler::<R>))
            .with_state(metrics);

        let addr = format!("0.0.0.0:{port}");
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(addr = %addr, error = %e, "Failed to bind metrics server");
                return;
            }
        };

        info!("Metrics server listening on {addr}");

        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "Metrics server exited with error");
        }
    })
}
```

- [ ] **Step 2: Fix Trino server**

Same pattern for `start_trino_server`:
```rust
let listener = match tokio::net::TcpListener::bind(&addr).await {
    Ok(l) => l,
    Err(e) => {
        tracing::error!(addr = %addr, error = %e, "Failed to bind Trino-compat HTTP server");
        return;
    }
};

info!("Trino-compat HTTP server listening on {addr}");

if let Err(e) = axum::serve(listener, app).await {
    tracing::error!(error = %e, "Trino-compat HTTP server exited with error");
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-metrics -p sqe-trino-compat && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-metrics/src/server.rs crates/sqe-trino-compat/src/server.rs
git commit -m "fix: handle server bind failures gracefully instead of panicking"
```

---

### Task 10: B2 — Add TTL eviction to Trino result cache

**Files:**
- Modify: `crates/sqe-trino-compat/src/server.rs`

- [ ] **Step 1: Add timestamp to PaginatedResult**

```rust
pub struct PaginatedResult {
    pub columns: Vec<TrinoColumn>,
    pub pages: Vec<Vec<Vec<serde_json::Value>>>,
    pub total_pages: usize,
    pub created_at: std::time::Instant,
}
```

- [ ] **Step 2: Add eviction constant and sweep logic**

```rust
/// Results older than this are evicted (5 minutes).
const RESULT_TTL_SECS: u64 = 300;
```

Add a method or spawn a background task in `start_trino_server` that sweeps expired results:
```rust
let state_sweep = state.clone();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        interval.tick().await;
        let expired: Vec<String> = state_sweep.results.iter()
            .filter(|entry| entry.value().created_at.elapsed().as_secs() > RESULT_TTL_SECS)
            .map(|entry| entry.key().clone())
            .collect();
        for id in &expired {
            state_sweep.results.remove(id);
        }
        if !expired.is_empty() {
            tracing::debug!(count = expired.len(), "Evicted stale Trino result sets");
        }
    }
});
```

- [ ] **Step 3: Set `created_at` when inserting results**

Update line 321: `created_at: std::time::Instant::now(),`

- [ ] **Step 4: Run tests**

Run: `cargo test -p sqe-trino-compat && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-trino-compat/src/server.rs
git commit -m "fix: add TTL eviction to Trino paginated result cache to prevent memory leaks"
```

---

### Task 11: B4 — Improve arrow_type_to_iceberg fallback

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs:648-650`

- [ ] **Step 1: Log warning on unknown type instead of silent fallback**

```rust
other => {
    tracing::warn!(arrow_type = ?other, "Unmapped Arrow type, falling back to string");
    serde_json::json!("string")
}
```

- [ ] **Step 2: Add List/Map/Struct mappings**

Add before the catch-all:
```rust
DataType::List(f) | DataType::LargeList(f) => {
    serde_json::json!({
        "type": "list",
        "element-id": 1,
        "element": arrow_type_to_iceberg(f.data_type()),
        "element-required": !f.is_nullable(),
    })
}
DataType::Struct(fields) => {
    let iceberg_fields: Vec<serde_json::Value> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| serde_json::json!({
            "id": i + 1,
            "name": f.name(),
            "required": !f.is_nullable(),
            "type": arrow_type_to_iceberg(f.data_type()),
        }))
        .collect();
    serde_json::json!({
        "type": "struct",
        "fields": iceberg_fields,
    })
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p sqe-coordinator && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "fix: add List/Struct Iceberg type mappings, warn on unknown fallback"
```

---

### Task 12: B5 — Fix table_exists error handling + info_schema log levels

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs:577`
- Modify: `crates/sqe-catalog/src/info_schema.rs:88,130-131,140`

- [ ] **Step 1: Fix table_exists in drop_table_if_exists**

Replace line 577:
```rust
match catalog.table_exists(&table_ident).await {
    Ok(true) => {
        info!(table = %table_ident, "DROP existing table for CREATE OR REPLACE");
        catalog
            .drop_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to drop table for replace: {e}")))?;
    }
    Ok(false) => {}
    Err(e) => {
        return Err(SqeError::Catalog(format!(
            "Failed to check table existence for replace: {e}"
        )));
    }
}
```

- [ ] **Step 2: Raise info_schema log levels from debug to warn**

In `info_schema.rs`, change:
- Line 88: `debug!` → `warn!` for `Failed to list tables for information_schema`
- Line 130-131: `Err(_) => continue` → `Err(e) => { warn!(namespace = %ns, error = %e, "Failed to list tables for columns"); continue; }`
- Line 140: `debug!` → `warn!` for `Failed to load table for columns`

- [ ] **Step 3: Similarly for query_handler SHOW TABLES**

In `query_handler.rs` `handle_show_tables`, change `debug!` to `warn!` for namespace list failures.

- [ ] **Step 4: Run tests**

Run: `cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-catalog/src/info_schema.rs
git commit -m "fix: propagate table_exists errors, raise info_schema error log levels to warn"
```

---

### Task 13: Misc — Fix OIDC error body size + brittle 404 check

**Files:**
- Modify: `crates/sqe-auth/src/oidc_password.rs:67-70,105-108`
- Modify: `crates/sqe-coordinator/src/catalog_ops.rs:327`

- [ ] **Step 1: Truncate OIDC error body**

In both `exchange_credentials` and `refresh_token`, truncate the body:
```rust
let body = response
    .text()
    .await
    .unwrap_or_else(|_| "unable to read body".to_string());
let body_truncated = if body.len() > 500 { &body[..500] } else { &body };
```

Use `body_truncated` in the error message.

- [ ] **Step 2: Fix brittle 404 check in drop_view**

Replace `e.to_string().contains("404")` (catalog_ops.rs:327) with a check on `SqeError`:
```rust
Err(e) if if_exists && e.to_string().contains("HTTP 404") => {
```

Actually, since the HTTP status is formatted as `"HTTP {status}"` in `rest_catalog.rs`, this is already somewhat structured. A better approach: add an `is_not_found()` method to `SqeError`:
```rust
// In sqe-core/src/error.rs
impl SqeError {
    pub fn is_not_found(&self) -> bool {
        match self {
            SqeError::Catalog(msg) => msg.contains("HTTP 404"),
            _ => false,
        }
    }
}
```

Then in catalog_ops.rs:
```rust
Err(e) if if_exists && e.is_not_found() => {
```

- [ ] **Step 3: Run tests**

Run: `cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-auth/src/oidc_password.rs crates/sqe-coordinator/src/catalog_ops.rs crates/sqe-core/src/
git commit -m "fix: truncate OIDC error bodies, add SqeError::is_not_found for structured error matching"
```

---

### Task 14: Final — Full test suite + clippy clean

- [ ] **Step 1: Run full test suite**

Run: `cargo test --all`

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 3: Fix any remaining issues**

- [ ] **Step 4: Final commit if needed**
