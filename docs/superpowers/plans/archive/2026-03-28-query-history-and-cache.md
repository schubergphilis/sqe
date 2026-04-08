# Query History & Result Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Trino-compatible query history (`system.runtime.queries`), additional system tables (`runtime.nodes`, `runtime.tasks`, `metadata.*`), and a write-invalidated query result cache.

**Architecture:** A `QueryTracker` replaces the existing `QueryRegistry`, tracking full query lifecycle (QUEUED→RUNNING→FINISHED/FAILED/CANCELED) in a moka cache exposed via `system.runtime.queries`. A `ResultCache` stores read query results keyed by `SHA256(user + normalized_sql)`, with automatic invalidation when writes touch involved tables. Both integrate into `QueryHandler::execute()`.

**Tech Stack:** Rust, moka 0.12, DashMap 6, uuid (v7), DataFusion 52, Arrow 57, iceberg-rust 0.9

**Spec:** `docs/superpowers/specs/2026-03-27-query-history-and-cache-design.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `Cargo.toml` (workspace) | Modify | Add `"v7"` to uuid features |
| `crates/sqe-core/src/config.rs` | Modify | Add `QueryCacheConfig` + `QueryHistoryConfig` |
| `crates/sqe-coordinator/src/query_tracker.rs` | Create | QueryTracker, QueryRecord, QueryState |
| `crates/sqe-coordinator/src/query_cache.rs` | Create | ResultCache, CachedResult, invalidation |
| `crates/sqe-coordinator/src/query_registry.rs` | Remove | Subsumed by QueryTracker |
| `crates/sqe-coordinator/src/lib.rs` | Modify | Replace `query_registry` with `query_tracker` + `query_cache` |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Wire tracker + cache into execute() |
| `crates/sqe-coordinator/src/flight_sql.rs` | Modify | UUID7 query_id, client_ip, use QueryTracker |
| `crates/sqe-catalog/src/system_runtime.rs` | Create | RuntimeSchemaProvider: queries, nodes, tasks |
| `crates/sqe-catalog/src/system_metadata.rs` | Create | MetadataSchemaProvider: catalogs, table_properties, schema_properties, table_comments |
| `crates/sqe-catalog/src/system_catalog.rs` | Modify | Add runtime + metadata schemas |
| `crates/sqe-catalog/src/lib.rs` | Modify | Export new modules |
| `crates/sqe-trino-compat/src/server.rs` | Modify | Pass client_ip |

---

### Task 1: Add uuid v7 feature + config structs

**Files:**
- Modify: `Cargo.toml:92` (workspace uuid line)
- Modify: `crates/sqe-core/src/config.rs`

- [ ] **Step 1: Add v7 feature to uuid in workspace Cargo.toml**

Change line 92 from:
```toml
uuid = { version = "1", features = ["v4"] }
```
to:
```toml
uuid = { version = "1", features = ["v4", "v7"] }
```

- [ ] **Step 2: Add config structs**

Add to `crates/sqe-core/src/config.rs`, after the existing `QueryConfig` block (around line 42):

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct QueryCacheConfig {
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cache_max_memory_mb")]
    pub max_memory_mb: u64,
    #[serde(default = "default_cache_max_entry_mb")]
    pub max_entry_mb: u64,
    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            max_memory_mb: default_cache_max_memory_mb(),
            max_entry_mb: default_cache_max_entry_mb(),
            ttl_secs: default_cache_ttl_secs(),
        }
    }
}

fn default_cache_enabled() -> bool { true }
fn default_cache_max_memory_mb() -> u64 { 256 }
fn default_cache_max_entry_mb() -> u64 { 5 }
fn default_cache_ttl_secs() -> u64 { 300 }

#[derive(Debug, Deserialize, Clone)]
pub struct QueryHistoryConfig {
    #[serde(default = "default_history_max_entries")]
    pub max_entries: u64,
    #[serde(default = "default_history_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for QueryHistoryConfig {
    fn default() -> Self {
        Self {
            max_entries: default_history_max_entries(),
            ttl_secs: default_history_ttl_secs(),
        }
    }
}

fn default_history_max_entries() -> u64 { 10000 }
fn default_history_ttl_secs() -> u64 { 1800 }
```

Add the fields to `SqeConfig`:

```rust
#[serde(default)]
pub query_cache: QueryCacheConfig,
#[serde(default)]
pub query_history: QueryHistoryConfig,
```

- [ ] **Step 3: Add config tests**

Add tests to the existing config test module:

```rust
#[test]
fn test_query_cache_defaults() {
    let config = QueryCacheConfig::default();
    assert!(config.enabled);
    assert_eq!(config.max_memory_mb, 256);
    assert_eq!(config.max_entry_mb, 5);
    assert_eq!(config.ttl_secs, 300);
}

#[test]
fn test_query_history_defaults() {
    let config = QueryHistoryConfig::default();
    assert_eq!(config.max_entries, 10000);
    assert_eq!(config.ttl_secs, 1800);
}
```

- [ ] **Step 4: Verify**

Run: `cargo test -p sqe-core && cargo clippy -p sqe-core -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/sqe-core/src/config.rs
git commit -m "feat: add query_cache and query_history config sections, uuid v7 feature"
```

---

### Task 2: Create QueryTracker (replace QueryRegistry)

**Files:**
- Create: `crates/sqe-coordinator/src/query_tracker.rs`
- Remove: `crates/sqe-coordinator/src/query_registry.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`

- [ ] **Step 1: Create query_tracker.rs with QueryRecord and QueryState**

Create `crates/sqe-coordinator/src/query_tracker.rs`:

```rust
use std::sync::Arc;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use moka::future::Cache;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use sqe_core::QueryHistoryConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryState {
    Queued,
    Running,
    Finished,
    Failed,
    Canceled,
}

impl std::fmt::Display for QueryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queued => write!(f, "QUEUED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Finished => write!(f, "FINISHED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Canceled => write!(f, "CANCELED"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryRecord {
    pub query_id: Uuid,
    pub state: QueryState,
    pub user: String,
    pub source: Option<String>,
    pub sql: String,
    pub session_id: String,
    pub client_ip: Option<String>,
    pub roles: Vec<String>,
    pub created: DateTime<Utc>,
    pub started: Option<DateTime<Utc>>,
    pub ended: Option<DateTime<Utc>>,
    pub queued_ms: u64,
    pub planning_ms: u64,
    pub execution_ms: u64,
    pub output_rows: usize,
    pub error_type: Option<String>,
    pub error_code: Option<String>,
    pub tables_touched: Vec<String>,
}

pub struct QueryTracker {
    history: Cache<Uuid, Arc<QueryRecord>>,
    active: DashMap<Uuid, CancellationToken>,
}

impl QueryTracker {
    pub fn new(config: &QueryHistoryConfig) -> Self {
        let history = Cache::builder()
            .max_capacity(config.max_entries)
            .time_to_live(std::time::Duration::from_secs(config.ttl_secs))
            .build();
        Self {
            history,
            active: DashMap::new(),
        }
    }

    pub fn start(
        &self,
        query_id: Uuid,
        user: &str,
        source: Option<&str>,
        sql: &str,
        session_id: &str,
        client_ip: Option<&str>,
        roles: Vec<String>,
    ) -> CancellationToken {
        let token = CancellationToken::new();
        let record = QueryRecord {
            query_id,
            state: QueryState::Queued,
            user: user.to_string(),
            source: source.map(|s| s.to_string()),
            sql: sql.to_string(),
            session_id: session_id.to_string(),
            client_ip: client_ip.map(|s| s.to_string()),
            roles,
            created: Utc::now(),
            started: None,
            ended: None,
            queued_ms: 0,
            planning_ms: 0,
            execution_ms: 0,
            output_rows: 0,
            error_type: None,
            error_code: None,
            tables_touched: Vec::new(),
        };
        self.history.insert(query_id, Arc::new(record));
        self.active.insert(query_id, token.clone());
        token
    }

    pub fn running(&self, query_id: &Uuid, planning_ms: u64) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            let now = Utc::now();
            record.state = QueryState::Running;
            record.started = Some(now);
            record.queued_ms = (now - record.created).num_milliseconds().max(0) as u64;
            record.planning_ms = planning_ms;
            self.history.insert(*query_id, Arc::new(record));
        }
    }

    pub fn complete(
        &self,
        query_id: &Uuid,
        rows: usize,
        execution_ms: u64,
        tables_touched: Vec<String>,
    ) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.state = QueryState::Finished;
            record.ended = Some(Utc::now());
            record.output_rows = rows;
            record.execution_ms = execution_ms;
            record.tables_touched = tables_touched;
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }

    pub fn failed(&self, query_id: &Uuid, error_type: &str, error_code: Option<&str>) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.state = QueryState::Failed;
            record.ended = Some(Utc::now());
            record.error_type = Some(error_type.to_string());
            record.error_code = error_code.map(|s| s.to_string());
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }

    pub fn canceled(&self, query_id: &Uuid) {
        if let Some(old) = self.history.get(query_id) {
            let mut record = (*old).clone();
            record.state = QueryState::Canceled;
            record.ended = Some(Utc::now());
            self.history.insert(*query_id, Arc::new(record));
        }
        self.active.remove(query_id);
    }

    pub fn cancel(&self, query_id: &Uuid) -> bool {
        if let Some((_, token)) = self.active.remove(query_id) {
            token.cancel();
            self.canceled(query_id);
            true
        } else {
            false
        }
    }

    pub fn records(&self) -> Vec<Arc<QueryRecord>> {
        let mut records: Vec<_> = Vec::new();
        for (_, v) in &self.history {
            records.push(v);
        }
        records
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}
```

- [ ] **Step 2: Add tests**

Add at the bottom of `query_tracker.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> QueryHistoryConfig {
        QueryHistoryConfig { max_entries: 100, ttl_secs: 60 }
    }

    #[tokio::test]
    async fn start_creates_queued_record() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let _token = tracker.start(id, "alice", Some("cli"), "SELECT 1", "s1", None, vec![]);
        let records = tracker.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, QueryState::Queued);
        assert_eq!(records[0].user, "alice");
    }

    #[tokio::test]
    async fn full_lifecycle_queued_running_finished() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "bob", None, "SELECT *", "s2", None, vec![]);
        tracker.running(&id, 10);
        tracker.complete(&id, 42, 150, vec!["ns.table1".to_string()]);
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Finished);
        assert_eq!(rec.output_rows, 42);
        assert_eq!(rec.execution_ms, 150);
        assert_eq!(rec.planning_ms, 10);
        assert!(rec.tables_touched.contains(&"ns.table1".to_string()));
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test]
    async fn failed_records_error() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        tracker.start(id, "carol", None, "BAD SQL", "s3", None, vec![]);
        tracker.running(&id, 0);
        tracker.failed(&id, "SyntaxError", Some("42000"));
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Failed);
        assert_eq!(rec.error_type.as_deref(), Some("SyntaxError"));
        assert_eq!(rec.error_code.as_deref(), Some("42000"));
    }

    #[tokio::test]
    async fn cancel_fires_token_and_records() {
        let tracker = QueryTracker::new(&test_config());
        let id = Uuid::now_v7();
        let token = tracker.start(id, "dave", None, "SELECT 1", "s4", None, vec![]);
        assert!(!token.is_cancelled());
        let cancelled = tracker.cancel(&id);
        assert!(cancelled);
        assert!(token.is_cancelled());
        let rec = tracker.history.get(&id).unwrap();
        assert_eq!(rec.state, QueryState::Canceled);
    }

    #[tokio::test]
    async fn cancel_unknown_returns_false() {
        let tracker = QueryTracker::new(&test_config());
        assert!(!tracker.cancel(&Uuid::now_v7()));
    }
}
```

- [ ] **Step 3: Update lib.rs — replace query_registry with query_tracker**

In `crates/sqe-coordinator/src/lib.rs`, replace `pub mod query_registry;` with `pub mod query_tracker;`.

- [ ] **Step 4: Verify**

Run: `cargo test -p sqe-coordinator -- query_tracker && cargo clippy -p sqe-coordinator -- -D warnings`

Note: This will break `flight_sql.rs` which uses `QueryRegistry`. Fix in Task 4.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/query_tracker.rs crates/sqe-coordinator/src/lib.rs
git rm crates/sqe-coordinator/src/query_registry.rs
git commit -m "feat: add QueryTracker with full lifecycle, replace QueryRegistry"
```

---

### Task 3: Create ResultCache

**Files:**
- Create: `crates/sqe-coordinator/src/query_cache.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs` (add module)

- [ ] **Step 1: Create query_cache.rs**

Create `crates/sqe-coordinator/src/query_cache.rs` with:
- `CachedResult` struct
- `ResultCache` with moka cache + DashMap invalidation index
- `extract_table_names()` from LogicalPlan
- User-scoped cache key via `cache_key(user, sql)`

Key implementation details:
- Cache key: reuse `sqe_metrics::audit::query_hash()` but prepend username: `SHA256(user + ":" + normalized_sql)`
- Weigher: `|_key, val: &Arc<CachedResult>| -> u32 { val.size_bytes as u32 }`
- max_capacity: `config.max_memory_mb * 1024 * 1024`
- Table extraction: walk `LogicalPlan` tree recursively, collect `TableScan` table names
- Non-deterministic detection: check for `now()`, `current_timestamp`, `random()`, `uuid()` in normalized SQL

- [ ] **Step 2: Add tests**

Test: cache hit/miss, user isolation, invalidation, size limits, non-deterministic bypass.

- [ ] **Step 3: Update lib.rs**

Add `pub mod query_cache;` to `crates/sqe-coordinator/src/lib.rs`.

- [ ] **Step 4: Verify**

Run: `cargo test -p sqe-coordinator -- query_cache && cargo clippy -p sqe-coordinator -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/query_cache.rs crates/sqe-coordinator/src/lib.rs
git commit -m "feat: add ResultCache with user-scoped keys and write invalidation"
```

---

### Task 4: Wire QueryTracker + ResultCache into QueryHandler and FlightSQL

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`
- Modify: `crates/sqe-coordinator/src/flight_sql.rs`
- Modify: `crates/sqe-coordinator/src/bin/sqe_server.rs`
- Modify: `crates/sqe-trino-compat/src/server.rs`

- [ ] **Step 1: Add QueryTracker + ResultCache to QueryHandler**

In `query_handler.rs`, add fields to the `QueryHandler` struct:
```rust
query_tracker: Arc<QueryTracker>,
query_cache: Option<Arc<ResultCache>>,
```

In `execute()`, wrap the existing execution flow:
1. Before execution: `tracker.start(query_id, ...)`, `tracker.running(query_id, planning_ms)`
2. For read queries: check `cache.lookup(user, sql)` before planning
3. After execution: `tracker.complete(query_id, rows, execution_ms, tables_touched)`
4. On error: `tracker.failed(query_id, error_type, error_code)`
5. For write queries: call `cache.invalidate(target_table)` after execution
6. Add `pub fn query_tracker(&self) -> &Arc<QueryTracker>` accessor

- [ ] **Step 2: Update flight_sql.rs — use QueryTracker instead of QueryRegistry**

Replace all `QueryRegistry` usage with `QueryTracker`:
- `self.query_registry.register(query_id)` → `self.query_tracker.start(query_id, ...)`
- `self.query_registry.cancel(query_id)` → `self.query_tracker.cancel(query_id)`
- `self.query_registry.complete(query_id)` → remove (QueryHandler now does this)
- Generate UUID7 via `Uuid::now_v7()` for query IDs
- Extract client_ip from request metadata

- [ ] **Step 3: Update sqe_server.rs — construct QueryTracker + ResultCache**

In the server binary, construct `QueryTracker` and `ResultCache` from config and pass to `QueryHandler`.

- [ ] **Step 4: Update Trino compat server — pass client_ip**

In `crates/sqe-trino-compat/src/server.rs`, extract `client_ip` from the HTTP request's `ConnectInfo` or `X-Forwarded-For` header and pass to the session.

- [ ] **Step 5: Verify full build + tests**

Run: `cargo build --all && cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/src/flight_sql.rs crates/sqe-coordinator/src/bin/sqe_server.rs crates/sqe-trino-compat/src/server.rs
git commit -m "feat: wire QueryTracker and ResultCache into query execution pipeline"
```

---

### Task 5: Create system.runtime.* virtual tables

**Files:**
- Create: `crates/sqe-catalog/src/system_runtime.rs`
- Modify: `crates/sqe-catalog/src/system_catalog.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`

- [x] **Step 1: Create RuntimeSchemaProvider**

Create `crates/sqe-catalog/src/system_runtime.rs` implementing `SchemaProvider` with three tables:

**`queries`** — 17-column schema built from `QueryTracker::records()`. Follow the `JdbcSchemaProvider` pattern: build `RecordBatch` from `StringBuilder`/`Int64Builder`/`TimestampMillisecondBuilder`, wrap in `MemTable`.

**`nodes`** — Built from `WorkerRegistry` (healthy workers) + a coordinator row. Columns: node_id, http_uri, node_version, coordinator, state, last_heartbeat.

**`tasks`** — In single-node mode, auto-generate one task per finished query from `QueryTracker::records()`. Columns: query_id, task_id, node_id, state, created, ended, elapsed_ms, input_rows, output_rows, input_bytes.

The `RuntimeSchemaProvider` needs `Arc<QueryTracker>` and optionally `Arc<WorkerRegistry>`.

- [x] **Step 2: Add tests**

Test: queries table schema has 17 columns, nodes table includes coordinator row, tasks table has one row per query.

- [x] **Step 3: Register in SystemCatalogProvider**

In `system_catalog.rs`, add a `runtime_schema` field, extend `schema_names()` to return `["jdbc", "runtime"]`, and route `schema("runtime")` to the new provider.

Update the constructor to accept `Arc<QueryTracker>` and `Option<Arc<WorkerRegistry>>`.

- [x] **Step 4: Update lib.rs**

Add `pub mod system_runtime;` to `crates/sqe-catalog/src/lib.rs`.

- [x] **Step 5: Verify**

Run: `cargo test -p sqe-catalog -- system_runtime && cargo clippy -p sqe-catalog -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-catalog/src/system_runtime.rs crates/sqe-catalog/src/system_catalog.rs crates/sqe-catalog/src/lib.rs
git commit -m "feat: add system.runtime.queries/nodes/tasks virtual tables"
```

---

### Task 6: Create system.metadata.* virtual tables

**Files:**
- Create: `crates/sqe-catalog/src/system_metadata.rs`
- Modify: `crates/sqe-catalog/src/system_catalog.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`

- [ ] **Step 1: Create MetadataSchemaProvider**

Create `crates/sqe-catalog/src/system_metadata.rs` implementing `SchemaProvider` with four tables:

**`catalogs`** — Single row: catalog_name (warehouse), connector_id ("iceberg").

**`table_properties`** — For each table in each namespace, load `table.metadata().properties()` and emit one row per property. Use `list_namespaces_safe()` pattern — skip tables that fail to load.

**`schema_properties`** — For each namespace, call catalog's `get_namespace()` to get properties. Emit one row per property.

**`table_comments`** — For each table, extract `properties()["comment"]` if present. One row per table.

The provider needs `Arc<SessionCatalog>` and `warehouse: String`.

- [ ] **Step 2: Add tests**

Test: catalogs table has 1 row, table_properties schema has 5 columns, table_comments schema has 4 columns.

- [ ] **Step 3: Register in SystemCatalogProvider**

Add `metadata_schema` field. Extend `schema_names()` to `["jdbc", "runtime", "metadata"]`. Route `schema("metadata")`.

- [ ] **Step 4: Update lib.rs**

Add `pub mod system_metadata;` to `crates/sqe-catalog/src/lib.rs`.

- [ ] **Step 5: Verify**

Run: `cargo test -p sqe-catalog -- system_metadata && cargo clippy -p sqe-catalog -- -D warnings`

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-catalog/src/system_metadata.rs crates/sqe-catalog/src/system_catalog.rs crates/sqe-catalog/src/lib.rs
git commit -m "feat: add system.metadata.catalogs/table_properties/schema_properties/table_comments"
```

---

### Task 7: Add cache metrics to Prometheus

**Files:**
- Modify: `crates/sqe-metrics/src/lib.rs`
- Modify: `crates/sqe-coordinator/src/query_cache.rs`

- [ ] **Step 1: Add cache metric definitions**

In `crates/sqe-metrics/src/lib.rs`, add to `MetricsRegistry`:

```rust
pub cache_hits: prometheus::Counter,
pub cache_misses: prometheus::Counter,
pub cache_evictions: prometheus::IntCounterVec,  // label: reason (ttl, lru, invalidation)
pub cache_size_bytes: prometheus::Gauge,
pub cache_entries: prometheus::Gauge,
```

Register them in the constructor.

- [ ] **Step 2: Instrument ResultCache**

In `query_cache.rs`, add a `metrics: Option<Arc<MetricsRegistry>>` field. Increment counters on lookup hit/miss, store, and invalidation.

- [ ] **Step 3: Verify**

Run: `cargo test --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-metrics/src/lib.rs crates/sqe-coordinator/src/query_cache.rs
git commit -m "feat: add Prometheus metrics for query result cache"
```

---

### Task 8: Update docs + final verification

**Files:**
- Modify: `README.md`
- Modify: `nextsteps.md`
- Modify: `sqe.toml.example`

- [ ] **Step 1: Add config sections to sqe.toml.example**

```toml
[query_cache]
enabled = true
max_memory_mb = 256
max_entry_mb = 5
ttl_secs = 300

[query_history]
max_entries = 10000
ttl_secs = 1800
```

- [ ] **Step 2: Update README roadmap**

Add checked items for query history and cache.

- [ ] **Step 3: Update nextsteps.md**

Update status line and mark completed.

- [ ] **Step 4: Full verification**

```bash
cargo build --all
cargo test --all
cargo clippy --all-targets --all-features -- -D warnings
cargo audit
```

- [ ] **Step 5: Commit**

```bash
git add README.md nextsteps.md sqe.toml.example
git commit -m "docs: update README, nextsteps, config example for query history and cache"
```

---

## Verification

After all tasks:
- [ ] `cargo build --all` — clean
- [ ] `cargo test --all` — all pass
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` — 0 errors
- [ ] `SELECT * FROM system.runtime.queries` — returns query history
- [ ] `SELECT * FROM system.runtime.nodes` — returns coordinator
- [ ] `SELECT * FROM system.metadata.catalogs` — returns warehouse
- [ ] Same SELECT twice — second hits cache (verify via metrics or timing)
- [ ] INSERT then same SELECT — cache miss (invalidated)
- [ ] Full benchmark suite still passes
