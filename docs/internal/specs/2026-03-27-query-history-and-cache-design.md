# Query History & Result Cache — Design Spec

## Summary

Add Trino-compatible query history (`system.runtime.queries`), runtime/metadata system tables, and a smart query result cache with write-invalidation. Two independent subsystems sharing a `QueryTracker` core.

## Motivation

- **Query history**: Operations teams need to see who ran what, when, and how long it took — via SQL, not log files. Trino's `system.runtime.queries` is the standard interface BI tools and operators expect.
- **Query result cache**: Repeated read queries (dashboards, BI refresh) re-execute the full Iceberg scan pipeline. Caching results by query hash with automatic invalidation on writes eliminates redundant work while guaranteeing freshness.

## Dependencies

- **uuid crate**: Add `"v7"` feature flag to workspace Cargo.toml: `uuid = { version = "1", features = ["v4", "v7"] }`
- **moka 0.12**: Already in workspace (used for credential vending)
- **dashmap 6**: Already in workspace (used for sessions)

## Architecture

```
Flight SQL / Trino HTTP request
    │
    ├─► Generate UUID7 query_id, extract client_ip
    │
    ▼
QueryTracker::start(query_id, user, source, sql)     → state: QUEUED
    │
    ▼
QueryHandler::execute()
    ├─► QueryTracker::running(query_id, planning_ms)  → state: RUNNING
    ├─► ResultCache::lookup(user, normalized_sql_hash)
    │       HIT  → return cached batches, skip execution
    │       MISS ↓
    ├─► Plan query (LogicalPlan)
    │       └─► Extract tables_touched from LogicalPlan
    ├─► Execute (DataFusion)
    ├─► ResultCache::store(user, hash, batches, tables_touched)
    ├─► QueryTracker::complete(query_id, rows, execution_ms, tables_touched)
    │                                                   → state: FINISHED
    └─► On error: QueryTracker::failed(query_id, error) → state: FAILED
        On cancel: QueryTracker::canceled(query_id)     → state: CANCELED

Write operations (INSERT/CTAS/DROP):
    └─► ResultCache::invalidate(target_table)
        └─► Evict all cached entries whose tables_touched includes target_table
```

## Component 1: QueryTracker

**File:** `crates/sqe-coordinator/src/query_tracker.rs`

Replaces the existing `QueryRegistry` (which only tracked cancellation tokens). The new `QueryTracker` subsumes cancellation and adds full lifecycle tracking.

### QueryRecord

```rust
pub struct QueryRecord {
    pub query_id: Uuid,                  // UUID7, time-sortable
    pub state: QueryState,               // Queued, Running, Finished, Failed, Canceled
    pub user: String,
    pub source: Option<String>,          // X-Trino-Source / client identifier
    pub sql: String,
    pub session_id: String,
    pub client_ip: Option<String>,
    pub roles: Vec<String>,              // user roles (maps to Trino resource_group_id)
    pub created: DateTime<Utc>,          // when request was received
    pub started: Option<DateTime<Utc>>,  // when execution began
    pub ended: Option<DateTime<Utc>>,    // when execution completed
    pub queued_ms: u64,                  // created → started
    pub planning_ms: u64,               // plan phase duration
    pub execution_ms: u64,              // execute phase duration
    pub output_rows: usize,
    pub error_type: Option<String>,      // SqeError variant name
    pub error_code: Option<String>,      // error code string
    pub tables_touched: Vec<String>,     // extracted from LogicalPlan
}

pub enum QueryState {
    Queued,
    Running,
    Finished,
    Failed,
    Canceled,
}
```

### Mutability Model

QueryRecord is **immutable after creation**. State transitions produce a new `Arc<QueryRecord>` via clone-and-replace:

```rust
fn transition(&self, query_id: &Uuid, f: impl FnOnce(&QueryRecord) -> QueryRecord) {
    if let Some(old) = self.history.get(query_id) {
        let mut new_record = (*old).clone();
        let updated = f(&new_record);
        self.history.insert(*query_id, Arc::new(updated));
    }
}
```

This avoids interior mutability (`Mutex`/`RwLock`) and is safe because moka's `insert` is atomic.

### Storage

- **History:** `moka::future::Cache<Uuid, Arc<QueryRecord>>` — configurable max entries (default 10,000) and TTL (default 30 min). Completed queries are kept in cache for history; evicted by LRU + TTL.
- **In-flight cancellation:** `DashMap<Uuid, CancellationToken>` — only holds tokens for Queued/Running queries. Removed on completion/failure/cancellation.

Note: The current `QueryRegistry` uses `String` keys. Migration: change all callers in `flight_sql.rs` and Trino compat server from `String` to `Uuid`.

### API

```rust
impl QueryTracker {
    /// Register a new query. Returns a CancellationToken for the caller.
    pub fn start(&self, query_id: Uuid, user: &str, source: Option<&str>,
                 sql: &str, session_id: &str, client_ip: Option<&str>,
                 roles: Vec<String>) -> CancellationToken;

    /// Transition to Running state. Caller provides planning_ms from the plan phase.
    pub fn running(&self, query_id: &Uuid, planning_ms: u64);

    /// Transition to Finished state with execution metrics.
    pub fn complete(&self, query_id: &Uuid, rows: usize, execution_ms: u64,
                    tables_touched: Vec<String>);

    /// Transition to Failed state.
    pub fn failed(&self, query_id: &Uuid, error_type: &str, error_code: Option<&str>);

    /// Transition to Canceled state.
    pub fn canceled(&self, query_id: &Uuid);

    /// Fire the cancellation token for an in-flight query. Returns false if not found.
    pub fn cancel(&self, query_id: &Uuid) -> bool;

    /// Snapshot of all records for system.runtime.queries table.
    pub fn records(&self) -> Vec<Arc<QueryRecord>>;

    /// Count of in-flight (Queued + Running) queries.
    pub fn active_count(&self) -> usize;
}
```

## Component 2: `system.runtime.queries` Virtual Table

**File:** `crates/sqe-catalog/src/system_runtime.rs`

Exposed via `RuntimeSchemaProvider` implementing DataFusion's `SchemaProvider`.

### Schema (17 columns, Trino-compatible + SQE extensions)

| Column | Arrow Type | Source |
|---|---|---|
| `query_id` | Utf8 | `record.query_id.to_string()` |
| `state` | Utf8 | QUEUED / RUNNING / FINISHED / FAILED / CANCELED |
| `user` | Utf8 | `record.user` |
| `source` | Utf8 (nullable) | `record.source` |
| `query` | Utf8 | `record.sql` |
| `resource_group_id` | Utf8 | `record.roles.join(",")` |
| `queued_time_ms` | Int64 | `record.queued_ms` |
| `analysis_time_ms` | Int64 | 0 (reserved for future instrumentation) |
| `planning_time_ms` | Int64 | `record.planning_ms` |
| `execution_time_ms` | Int64 | `record.execution_ms` |
| `created` | Timestamp(Millisecond, Some("UTC")) | `record.created` |
| `started` | Timestamp(Millisecond, Some("UTC")) | `record.started` |
| `last_heartbeat` | Timestamp(Millisecond, Some("UTC")) | `record.started` (single-node) |
| `end` | Timestamp(Millisecond, Some("UTC")) | `record.ended` |
| `output_rows` | Int64 | `record.output_rows` |
| `error_type` | Utf8 (nullable) | `record.error_type` |
| `error_code` | Utf8 (nullable) | `record.error_code` |

### Usage

```sql
-- Recent queries
SELECT query_id, state, "user", query, planning_time_ms
FROM system.runtime.queries
WHERE state = 'FINISHED'
ORDER BY created DESC LIMIT 20;

-- Slow queries
SELECT query_id, "user", execution_time_ms, output_rows, query
FROM system.runtime.queries
WHERE execution_time_ms > 10000
ORDER BY execution_time_ms DESC;

-- Failed queries
SELECT query_id, "user", error_type, error_code, query
FROM system.runtime.queries
WHERE state = 'FAILED';
```

## Component 3: Additional System Tables

### `system.runtime.nodes`

**Source:** `WorkerRegistry` (existing) + coordinator self-info.

| Column | Type | Source |
|---|---|---|
| `node_id` | Utf8 | Worker URL or "coordinator" |
| `http_uri` | Utf8 | Worker endpoint |
| `node_version` | Utf8 | SQE version |
| `coordinator` | Boolean | true for coordinator |
| `state` | Utf8 | "active" / "inactive" / "shutting_down" |
| `last_heartbeat` | Timestamp(Millisecond, Some("UTC")) | Last heartbeat time |

### `system.runtime.tasks`

Prepared for distributed execution. Each query can have multiple task fragments.

| Column | Type | Source |
|---|---|---|
| `query_id` | Utf8 | Parent query UUID7 |
| `task_id` | Utf8 | Fragment identifier |
| `node_id` | Utf8 | Worker that executed this task |
| `state` | Utf8 | PLANNED / RUNNING / FINISHED / FAILED |
| `created` | Timestamp(Millisecond, Some("UTC")) | |
| `ended` | Timestamp(Millisecond, Some("UTC")) | |
| `elapsed_ms` | Int64 | |
| `input_rows` | Int64 | Rows read by this task |
| `output_rows` | Int64 | Rows produced |
| `input_bytes` | Int64 | Bytes read |

**Single-node mode:** QueryTracker auto-creates one task record per query. `input_rows` and `output_rows` come from the DataFusion `ExecutionPlan` metrics (accessible via `plan.metrics()`). `input_bytes` estimated from RecordBatch memory size. When distributed execution ships, the scheduler populates multiple tasks per query with worker-reported metrics.

### `system.metadata.catalogs`

| Column | Type | Source |
|---|---|---|
| `catalog_name` | Utf8 | Config warehouse name |
| `connector_id` | Utf8 | "iceberg" |

### `system.metadata.table_properties`

| Column | Type | Source |
|---|---|---|
| `catalog_name` | Utf8 | Warehouse |
| `schema_name` | Utf8 | Namespace |
| `table_name` | Utf8 | Table |
| `property_name` | Utf8 | Key from `table.metadata().properties()` |
| `property_value` | Utf8 | Value |

**Performance:** Loading every table requires N REST calls. Follow the `list_namespaces_safe()` pattern from `system_jdbc.rs` — silently skip tables that fail to load (permission denied, deleted mid-scan). Log warnings for failures. No filtering pushdown (MemTable scanned after build). For catalogs with hundreds of tables, this table may be slow; acceptable for metadata browsing use cases.

### `system.metadata.schema_properties`

| Column | Type | Source |
|---|---|---|
| `catalog_name` | Utf8 | Warehouse |
| `schema_name` | Utf8 | Namespace |
| `property_name` | Utf8 | Key from `namespace.properties()` |
| `property_value` | Utf8 | Value |

### `system.metadata.table_comments`

| Column | Type | Source |
|---|---|---|
| `catalog_name` | Utf8 | Warehouse |
| `schema_name` | Utf8 | Namespace |
| `table_name` | Utf8 | Table |
| `comment` | Utf8 (nullable) | `table.metadata().properties()["comment"]` |

## Component 4: Query Result Cache

**File:** `crates/sqe-coordinator/src/query_cache.rs`

### CachedResult

```rust
pub struct CachedResult {
    pub query_id: Uuid,
    pub batches: Vec<RecordBatch>,
    pub tables_touched: Vec<String>,
    pub created: DateTime<Utc>,
    pub size_bytes: usize,
}
```

### Cache Key — User-Scoped

**Security-critical:** The cache key MUST include the user identity because SQE applies per-user policy enforcement (row filters, column masks). Without user scoping, User B could receive User A's cached results, bypassing policy enforcement.

Cache key: `SHA256(username + ":" + normalized_sql)`

Reuse the existing `sqe_metrics::audit::query_hash()` normalization (collapse whitespace, uppercase) for consistency with the audit log.

### Cache Store

`moka::future::Cache<String, Arc<CachedResult>>` where key is `SHA256(user + ":" + normalized_sql)`.

- **weigher**: returns `size_bytes` so moka enforces memory limit. Note: `max_capacity` in moka is the max total weight when a weigher is present — set to `max_memory_mb * 1024 * 1024`.
- **max_capacity**: configurable (default 256MB, passed as bytes to moka)
- **time_to_live**: configurable (default 5 min)
- **per-entry max**: configurable (default 5MB) — results exceeding this are not cached

### Invalidation

Secondary index: `DashMap<String, HashSet<String>>` mapping `table_name → {cache_keys}`.

On write (INSERT/CTAS/DROP/ALTER):
1. Extract target table name from the statement
2. Look up all cache keys whose `tables_touched` includes the target
3. Evict each from the moka cache
4. Remove entries from the secondary index

**Race condition note:** Between steps (2) and (4), a concurrent `store()` could add a new entry for the same table. This is acceptable — the new entry will be evicted by TTL or on the next write to the same table. The window is small (microseconds) and the consequence is a stale cache entry that expires within `ttl_secs`. Full consistency would require a write lock on the entire invalidation path, which is not worth the contention cost.

### Table Extraction from LogicalPlan

```rust
fn extract_table_names(plan: &LogicalPlan) -> Vec<String> {
    // Walk the plan tree recursively
    // Collect table names from TableScan nodes
    // DataFusion's LogicalPlan::TableScan has qualified table name
}
```

### What's NOT Cached

- DDL statements (CREATE, DROP, ALTER)
- DML statements (INSERT, DELETE, MERGE)
- EXPLAIN queries
- Results exceeding `max_entry_mb`
- Queries with non-deterministic functions (CURRENT_TIMESTAMP, RANDOM, UUID, NOW)
- System table queries (`system.*`, `information_schema.*`)

### Cache Metrics

Exposed via Prometheus:
- `sqe_cache_hits_total` — counter
- `sqe_cache_misses_total` — counter
- `sqe_cache_evictions_total` — counter (by reason: ttl, lru, invalidation)
- `sqe_cache_size_bytes` — gauge
- `sqe_cache_entries` — gauge

## Configuration

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

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `crates/sqe-coordinator/src/query_tracker.rs` | Create | QueryTracker, QueryRecord, QueryState, cancellation |
| `crates/sqe-coordinator/src/query_cache.rs` | Create | ResultCache, CachedResult, invalidation, table extraction |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Wire tracker lifecycle + cache lookup/store/invalidate |
| `crates/sqe-coordinator/src/flight_sql.rs` | Modify | Generate UUID7, pass client_ip, migrate String→Uuid keys |
| `crates/sqe-coordinator/src/query_registry.rs` | Remove | Subsumed by QueryTracker |
| `crates/sqe-catalog/src/system_catalog.rs` | Modify | Add "runtime" and "metadata" schemas |
| `crates/sqe-catalog/src/system_runtime.rs` | Create | RuntimeSchemaProvider: queries, nodes, tasks |
| `crates/sqe-catalog/src/system_metadata.rs` | Create | MetadataSchemaProvider: catalogs, table_properties, schema_properties, table_comments |
| `crates/sqe-core/src/config.rs` | Modify | Add [query_cache] and [query_history] sections |
| `crates/sqe-trino-compat/src/server.rs` | Modify | Pass client_ip from HTTP request |
| `Cargo.toml` (workspace) | Modify | Add "v7" feature to uuid dependency |

## Testing Strategy

### Unit Tests
- QueryTracker: lifecycle transitions (start→running→complete, start→running→failed, start→canceled), concurrent access, TTL eviction, cancellation token firing, clone-and-replace mutability
- ResultCache: hit/miss, user-scoped keys (same SQL different users = different keys), invalidation on write, memory limits, per-entry size limit, non-deterministic exclusion, TTL expiry
- Table extraction from LogicalPlan (mock plans with known table names)
- System table builders: correct schemas, correct row counts, correct column types

### Integration Tests
- `SELECT * FROM system.runtime.queries` returns recent queries including itself
- `SELECT * FROM system.runtime.nodes` returns coordinator
- `SELECT * FROM system.metadata.table_properties WHERE table_name = 'x'` returns Iceberg properties
- Cache hit: same SELECT twice, second is faster
- Cache invalidation: SELECT, INSERT into same table, same SELECT misses cache
- User isolation: User A's cached result is not returned for User B's identical query

## Success Criteria

1. `SELECT * FROM system.runtime.queries` shows query history with all 17 columns
2. `system.runtime.nodes` shows coordinator + workers
3. `system.runtime.tasks` shows per-query task fragments (1 per query in single-node)
4. `system.metadata.*` tables populated from Polaris
5. Identical SELECT queries by the same user hit cache on second execution
6. Different users running the same SQL get independent cache entries
7. INSERT/CTAS to table X evicts all cached results touching table X
8. Cache respects memory limit, TTL, and per-entry size limit
9. Non-deterministic queries bypass cache
10. Cache metrics visible in Prometheus
