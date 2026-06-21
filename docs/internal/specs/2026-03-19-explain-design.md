# EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL Design

## Goal

Add three query-plan inspection commands to SQE, all policy-aware (security enforcement applies before the plan is shown). Modelled loosely on Snowflake's EXPLAIN but grounded in what DataFusion and Iceberg actually expose.

## Architecture

The feature lives entirely within `sqe-coordinator`. Parsing is handled in `sqe-sql`. No new crate is needed.

```
Client SQL
    │
    ▼
sqe-sql: classifier.rs
    ├─ "EXPLAIN FULL ..."  → pre-scan, strip FULL, StatementKind::ExplainFull(inner_sql)
    ├─ "EXPLAIN ANALYZE …" → sqlparser Statement::Explain { analyze: true }  → StatementKind::Utility
    └─ "EXPLAIN …"         → sqlparser Statement::Explain { analyze: false } → StatementKind::Utility
    │
    ▼
sqe-coordinator: query_handler.rs  →  explain.rs
    ├─ ExplainFull             → ExplainHandler::full()
    ├─ Utility / analyze:false → ExplainHandler::plan()
    └─ Utility / analyze:true  → ExplainHandler::analyze()
```

## Statement Variants

| Syntax | Executes query? | Output schema |
|---|---|---|
| `EXPLAIN <query>` | No | `plan_type TEXT, plan TEXT` |
| `EXPLAIN ANALYZE <query>` | Yes | `step INT, operation TEXT, output_rows BIGINT, elapsed_ms DOUBLE` |
| `EXPLAIN FULL <query>` | No | `step INT, operation TEXT, estimated_rows BIGINT, estimated_bytes BIGINT, files_scanned INT, files_total INT` |

All three apply `PolicyEnforcer::evaluate()` before generating any output — the plan shown is the plan that actually executes.

## Components

### 1. `crates/sqe-sql/src/classifier.rs`

**Pre-scan for `EXPLAIN FULL`** (before sqlparser, case-insensitive):

```rust
let trimmed = sql.trim();
if trimmed.to_ascii_uppercase().starts_with("EXPLAIN FULL ") {
    let inner = trimmed["EXPLAIN FULL ".len()..].trim().to_string();
    return Ok(StatementKind::ExplainFull(inner));
}
```

This runs before the sqlparser call so it does not interfere with normal
`EXPLAIN` or `EXPLAIN ANALYZE` processing.

**Add variant to `StatementKind`:**

```rust
ExplainFull(String),   // inner SQL string
```

**Add arm to `StatementKind::name()`** (used by metrics/audit — must be exhaustive):

```rust
StatementKind::ExplainFull(_) => "explain_full",
```

**Fix existing `Utility` routing** in `query_handler.rs` to extract the `analyze` flag:

```rust
Statement::Explain { analyze, statement, .. } => {
    let inner = statement.to_string();
    if analyze {
        self.explain_handler.analyze(session, &inner, &ctx).await
    } else {
        self.explain_handler.plan(session, &inner, &ctx).await
    }
}
```

### 2. `crates/sqe-coordinator/src/explain.rs` (new file)

Owns all three explain handlers and their helpers. `QueryHandler` holds an `ExplainHandler` instance (zero-cost — it borrows the same `Arc<dyn PolicyEnforcer>`).

```rust
pub struct ExplainHandler {
    policy_enforcer: Arc<dyn PolicyEnforcer>,
}

impl ExplainHandler {
    pub async fn plan(&self, session: &Session, inner_sql: &str, ctx: &SessionContext)
        -> sqe_core::Result<Vec<RecordBatch>>;

    pub async fn analyze(&self, session: &Session, inner_sql: &str, ctx: &SessionContext)
        -> sqe_core::Result<Vec<RecordBatch>>;

    pub async fn full(&self, session: &Session, inner_sql: &str, ctx: &SessionContext)
        -> sqe_core::Result<Vec<RecordBatch>>;
}
```

#### `plan()` — logical + physical plan text

1. `ctx.sql(inner_sql)` → `DataFrame`
2. `policy_enforcer.evaluate(user, df.logical_plan().clone())` → `enforced_plan`
3. `ctx.state().create_physical_plan(&enforced_plan)` → `physical_plan`
4. Format:
   - logical: `format!("{}", enforced_plan.display_indent())`
   - physical: `format!("{}", datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true))`
5. Build 2-row `RecordBatch` with schema `(plan_type: Utf8, plan: Utf8)` — one row
   `("logical_plan", <logical text>)` and one row `("physical_plan", <physical text>)`.

#### `analyze()` — actual execution metrics

1. Apply policy enforcement → `enforced_plan`
2. `ctx.state().create_physical_plan(&enforced_plan)` → `Arc<dyn ExecutionPlan>`
3. Execute: `collect(physical_plan.clone(), ctx.task_ctx())` — DataFusion populates
   per-node metrics during execution.
4. Walk the physical plan tree recursively (post-order = leaf-to-root = execution order):
   - `node.metrics()` → `Option<MetricsSet>`
   - From `MetricsSet`: `elapsed_compute()` returns `Option<usize>` in **nanoseconds**;
     convert to milliseconds: `elapsed_ns as f64 / 1_000_000.0`
   - `output_rows()` returns `Option<usize>`; cast to `i64`.
   - Assign monotonically increasing step numbers starting at 0.
5. Build `RecordBatch` schema:
   `(step: Int32, operation: Utf8, output_rows: Int64, elapsed_ms: Float64)`
   Rows ordered leaf-to-root (natural execution order).

#### `full()` — plan + Iceberg statistics, no execution

1. Apply policy enforcement → `enforced_plan`
2. `ctx.state().create_physical_plan(&enforced_plan)` → physical plan. DataFusion
   resolves partition pruning during physical planning; the file list is embedded in
   each scan node at this point.
3. Walk the physical plan tree recursively (post-order):

   **For `IcebergScanExec` nodes** (the custom scan node in `sqe-catalog/src/iceberg_scan.rs`):
   - Downcast: `node.as_any().downcast_ref::<IcebergScanExec>()`
   - The node already holds the `Table` object via its `.table()` accessor — no
     separate catalog lookup is required.
   - `files_scanned`: count the data files selected by this plan node's file groups.
   - Iceberg snapshot stats: `table.metadata().current_snapshot()` returns
     `Option<&SnapshotRef>`. If `Some(snap)`, read from
     `snap.summary().additional_properties`:
     - `"total-data-files"` → `files_total: Int32`
     - `"total-records"` → `estimated_rows: Int64`
     - `"total-files-size"` → `estimated_bytes: Int64`
     All three parse as strings; use `.parse::<i64>()` with a fallback to NULL on
     parse error. Cache by table identifier to avoid duplicate lookups in self-joins.

   **For all other nodes** (Filter, HashAggregate, SortExec, etc.):
   - `estimated_rows`: use `node.partition_statistics(None)` (preferred over the
     deprecated `node.statistics()`). `IcebergScanExec` does not implement
     `partition_statistics()` so this returns `Statistics::new_unknown` for scan
     nodes — that is why scan rows must use the snapshot summary instead.
     For other nodes DataFusion may propagate row estimates from cardinality
     analysis; if `Precision::Absent`, emit NULL.
   - `estimated_bytes`, `files_scanned`, `files_total` → NULL.

4. Build `RecordBatch` schema:
   `(step: Int32, operation: Utf8, estimated_rows: Int64, estimated_bytes: Int64,
    files_scanned: Int32, files_total: Int32)`
   NULL values for columns that do not apply to a given operator.

### 3. `crates/sqe-coordinator/src/query_handler.rs`

- Add `explain_handler: ExplainHandler` field; constructed in `QueryHandler::new`.
- Call `create_session_context(session)` once per explain call, pass `&ctx` to handler.
- Route `StatementKind::ExplainFull(inner)` → `self.explain_handler.full(...)`.
- Update `Utility(stmt)` arm: extract `analyze` flag from `Statement::Explain { analyze, statement, .. }` and delegate to `plan()` or `analyze()` accordingly.
- All other `Utility` statements continue to return `SqeError::NotImplemented`.

## Error Handling

| Scenario | Behaviour |
|---|---|
| Inner SQL fails to parse | Return `SqeError::Execution` with the parse error message |
| Inner SQL references a non-existent table | Return `SqeError::Execution` — same as a normal SELECT on a missing table |
| Iceberg snapshot not found (`EXPLAIN FULL`) | `files_scanned`/`files_total`/`estimated_bytes` → NULL for that scan node; do not fail the whole explain |
| Snapshot summary key missing or unparseable | Treat as NULL for that field; log a `tracing::warn!` |
| Policy enforcement rewrites plan to empty | Return the empty/trivial plan — same as a normal query returning no rows |
| `EXPLAIN FULL <non-SELECT>` | Return `SqeError::NotImplemented` — only SELECT queries have meaningful plans |

## Testing

Four new integration tests in `crates/sqe-coordinator/tests/integration_test.rs`,
all using the `employees` fixture table from `setup_join_fixture()`:

| Test | Verifies |
|---|---|
| `test_explain_plan` | Returns exactly 2 rows (`logical_plan`, `physical_plan`); both plan strings are non-empty |
| `test_explain_analyze` | Returns ≥1 row; `output_rows` ≥ 0; `elapsed_ms` ≥ 0.0 |
| `test_explain_full` | Returns ≥1 row; the `IcebergScanExec` row has non-NULL `files_total` and `estimated_rows` |
| `test_explain_policy_aware` | With `PassthroughEnforcer` (current default): plan text contains the table name; structure test confirms policy path is exercised (enforcement is called even though passthrough does not modify the plan) |

## Out of Scope

- `EXPLAIN FULL` for non-Iceberg tables (e.g., `information_schema`) — returns NULL for file/row estimates, no error
- `EXPLAIN` for DDL statements (`CREATE TABLE`, `INSERT INTO`) — returns `SqeError::NotImplemented`
- Cost-based join reordering hints in EXPLAIN FULL output — future work
- Client-side formatted tree display — that is a CLI/JDBC concern, not engine concern
- Column-level statistics (min/max per column from Iceberg manifests) — future work
