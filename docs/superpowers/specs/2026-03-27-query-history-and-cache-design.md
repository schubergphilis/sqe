# Query History & Result Cache — Design Spec

## Summary

Add Trino-compatible query history (`system.runtime.queries`), runtime/metadata system tables, and a smart query result cache with write-invalidation. Two independent subsystems sharing a `QueryTracker` core.

## Motivation

- **Query history**: Operations teams need to see who ran what, when, and how long it took — via SQL, not log files. Trino's `system.runtime.queries` is the standard interface BI tools and operators expect.
- **Query result cache**: Repeated read queries (dashboards, BI refresh) re-execute the full Iceberg scan pipeline. Caching results by query hash with automatic invalidation on writes eliminates redundant work while guaranteeing freshness.

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
    ├─► QueryTracker::running(query_id)               → state: RUNNING
    ├─► ResultCache::lookup(normalized_sql_hash)
    │       HIT  → return cached batches, skip execution
    │       MISS ↓
    ├─► Plan query (LogicalPlan)
    │       └─► Extract tables_touched from LogicalPlan
    ├─► Execute (DataFusion)
    ├─► ResultCache::store(hash, batches, tables_touched)
    ├─► QueryTracker::complete(query_id, rows, duration, tables_touched)
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
    pub cancellation_token: Option<CancellationToken>,  // for in-flight cancellation
}

pub enum QueryState {
    Queued,
    Running,
    Finished,
    Failed,
    Canceled,
}
```

### Storage

`moka::future::Cache<Uuid, Arc<QueryRecord>>` — configurable max entries (default 10,000) and TTL (default 30 min). Completed queries are kept in cache for history; evicted by LRU + TTL.

In-flight queries (Queued/Running) are also tracked in a `DashMap<Uuid, CancellationToken>` for cancellation support (replacing the current `QueryRegistry`).

### API

```rust
impl QueryTracker {
    pub fn start(&self, query_id: Uuid, user: &str, source: Option<&str>, sql: &str, session_id: &str, client_ip: Option<&str>, roles: Vec<String>) -> CancellationToken;
    pub fn running(&self, query_id: &Uuid);
    pub fn complete(&self, query_id: &Uuid, rows: usize, tables_touched: Vec<String>);
    pub fn failed(&self, query_id: &Uuid, error_type: &str, error_code: Option<&str>);
    pub fn canceled(&self, query_id: &Uuid);
    pub fn cancel(&self, query_id: &Uuid) -> bool;  // fire cancellation token
    pub fn records(&self) -> Vec<Arc<QueryRecord>>;  // snapshot for system table
    pub fn active_count(&self) -> usize;
}
```

## Component 2: `system.runtime.queries` Virtual Table

**File:** `crates/sqe-catalog/src/system_runtime.rs`

Exposed via `RuntimeSchemaProvider` implementing DataFusion's `SchemaProvider`.

### Schema (15 columns, Trino-compatible)

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
| `created` | Timestamp(Millisecond, Some("UTC")) | `record.created` |
| `started` | Timestamp(Millisecond, Some("UTC")) | `record.started` |
| `last_heartbeat` | Timestamp(Millisecond, Some("UTC")) | `record.started` (single-node) |
| `end` | Timestamp(Millisecond, Some("UTC")) | `record.ended` |
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
SELECT query_id, "user", execution_ms, output_rows, query
FROM system.runtime.queries
WHERE execution_ms > 10000
ORDER BY execution_ms DESC;

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

Initially populated only for single-node (1 task per query). When distributed execution ships, the scheduler populates multiple tasks per query.

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

Populated by loading each table and iterating its properties map.

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

### Cache Store

`moka::future::Cache<String, Arc<CachedResult>>` where key is `SHA256(normalized_sql)`.

- **weigher**: returns `size_bytes` for memory-bounded eviction
- **max_capacity**: configurable (default 256MB)
- **time_to_live**: configurable (default 5 min)
- **per-entry max**: configurable (default 5MB) — results exceeding this are not cached

### Invalidation

Secondary index: `DashMap<String, HashSet<String>>` mapping `table_name → {cache_keys}`.

On write (INSERT/CTAS/DROP/ALTER):
1. Extract target table name from the statement
2. Look up all cache keys whose `tables_touched` includes the target
3. Evict each from the moka cache
4. Remove entries from the secondary index

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
| `crates/sqe-coordinator/src/flight_sql.rs` | Modify | Generate UUID7, pass client_ip |
| `crates/sqe-coordinator/src/query_registry.rs` | Remove | Subsumed by QueryTracker |
| `crates/sqe-catalog/src/system_catalog.rs` | Modify | Add "runtime" and "metadata" schemas |
| `crates/sqe-catalog/src/system_runtime.rs` | Create | RuntimeSchemaProvider: queries, nodes, tasks |
| `crates/sqe-catalog/src/system_metadata.rs` | Create | MetadataSchemaProvider: catalogs, table_properties, schema_properties, table_comments |
| `crates/sqe-core/src/config.rs` | Modify | Add [query_cache] and [query_history] sections |
| `crates/sqe-trino-compat/src/server.rs` | Modify | Pass client_ip from HTTP request |

## Testing Strategy

### Unit Tests
- QueryTracker: lifecycle transitions, concurrent access, TTL eviction, cancellation
- ResultCache: hit/miss, invalidation on write, memory limits, non-deterministic exclusion
- Table extraction from LogicalPlan (mock plans with known table names)
- System table builders: correct schemas, correct row counts

### Integration Tests
- `SELECT * FROM system.runtime.queries` returns recent queries
- `SELECT * FROM system.runtime.nodes` returns coordinator
- `SELECT * FROM system.metadata.table_properties WHERE table_name = 'x'` returns Iceberg properties
- Cache hit: same SELECT twice, second is faster
- Cache invalidation: SELECT, INSERT, same SELECT misses cache

## Success Criteria

1. `SELECT * FROM system.runtime.queries` shows query history with all 15 Trino-compatible columns
2. `system.runtime.nodes` shows coordinator + workers
3. `system.runtime.tasks` shows per-query task fragments
4. `system.metadata.*` tables populated from Polaris
5. Identical SELECT queries hit cache on second execution
6. INSERT/CTAS to table X evicts all cached results touching table X
7. Cache respects memory limit, TTL, and per-entry size limit
8. Non-deterministic queries bypass cache
9. Cache metrics visible in Prometheus
