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
    ├─ ExplainFull       → ExplainHandler::full()
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

**Pre-scan for `EXPLAIN FULL`** (before sqlparser):

```rust
let trimmed = sql.trim();
if trimmed.to_ascii_uppercase().starts_with("EXPLAIN FULL ") {
    let inner = trimmed["EXPLAIN FULL ".len()..].trim().to_string();
    return Ok(StatementKind::ExplainFull(inner));
}
```

**Add variant to `StatementKind`:**

```rust
ExplainFull(String),   // inner SQL string
```

**Fix existing `Utility` routing** in `query_handler.rs` to extract the `analyze` flag:

```rust
Statement::Explain { analyze, statement, .. } => {
    let inner = statement.to_string();
    if analyze {
        self.explain_handler.analyze(session, &inner).await
    } else {
        self.explain_handler.plan(session, &inner).await
    }
}
```

### 2. `crates/sqe-coordinator/src/explain.rs` (new file)

Owns all three explain handlers and their helpers. `QueryHandler` holds an `ExplainHandler` instance (zero-cost — it borrows the same `PolicyEnforcer` and `SessionContext` factory).

```rust
pub struct ExplainHandler {
    policy_enforcer: Arc<dyn PolicyEnforcer>,
    // config borrowed for create_session_context
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
5. Build 2-row `RecordBatch` with schema `(plan_type: Utf8, plan: Utf8)`

#### `analyze()` — actual execution metrics

1. Apply policy enforcement → `enforced_plan`
2. Create physical plan
3. `collect()` to execute (standard DataFusion collect populates metrics in-place)
4. Walk physical plan tree with a recursive visitor:
   - Each node: `node.metrics()` → extract `output_rows` (`MetricValue::Count`) and `elapsed_compute` (`MetricValue::Time`)
   - Assign step numbers leaf-to-root (execution order)
5. Build `RecordBatch` schema: `(step: Int32, operation: Utf8, output_rows: Int64, elapsed_ms: Float64)`
6. Rows ordered by step (leaf operators first — the natural reading order matching execution order)

#### `full()` — plan + Iceberg statistics

1. Apply policy enforcement → `enforced_plan`
2. Create physical plan (DataFusion resolves partition pruning during planning — file lists are embedded in `DataSourceExec` / `ParquetExec` nodes at this point)
3. Walk physical plan tree:
   - For **scan nodes** (`DataSourceExec`, `ParquetExec`): count `files_scanned` from the node's file list; query Iceberg snapshot summary for `total-data-files`, `total-records`, `total-files-size`
   - For **other nodes** (Filter, HashAggregate, SortExec, etc.): extract estimated output rows from `plan.statistics()` where available; `files_scanned` / `files_total` → NULL
4. Build `RecordBatch` schema:
   `(step: Int32, operation: Utf8, estimated_rows: Int64, estimated_bytes: Int64, files_scanned: Int32, files_total: Int32)`
   NULL values for columns that don't apply to a given operator.

**Iceberg snapshot summary lookup** (helper in `explain.rs`):
- The Iceberg catalog is already available through `SessionContext`'s registered `SqeCatalogProvider`
- For each table name in a scan node, call `catalog.load_table(ident)` → `Table` → `table.metadata().current_snapshot()` → `snapshot.summary()` map
- Extract: `total-data-files` (→ `files_total`), `total-records` (→ `estimated_rows`), `total-files-size` (→ `estimated_bytes`)
- Cache lookups within a single EXPLAIN FULL call (a table may appear multiple times in self-joins)

### 3. `crates/sqe-coordinator/src/query_handler.rs`

- Add `explain_handler: ExplainHandler` field (constructed in `QueryHandler::new`)
- Create `SessionContext` once per explain call (reuse existing `create_session_context`)
- Route `StatementKind::ExplainFull(inner)` → `self.explain_handler.full(session, &inner, &ctx)`
- Update `Utility(stmt)` arm to extract `analyze` flag and delegate accordingly

## Error Handling

| Scenario | Behaviour |
|---|---|
| Inner SQL fails to parse | Return `SqeError::Query` with the parse error message |
| Inner SQL references a non-existent table | Return `SqeError::Query` — same as a normal SELECT on a missing table |
| Iceberg snapshot not found (EXPLAIN FULL) | `files_scanned`/`files_total`/`estimated_bytes` → NULL for that scan node; do not fail the whole explain |
| Policy enforcement rewrites plan to empty | Return the empty plan — same as a normal query returning no rows |
| `EXPLAIN FULL <non-SELECT>` | Return `SqeError::NotImplemented` — only SELECT queries have meaningful plans |

## Testing

Three new integration tests in `crates/sqe-coordinator/tests/integration_test.rs`:

| Test | Verifies |
|---|---|
| `test_explain_plan` | `EXPLAIN SELECT * FROM test_ns.employees` returns 2 rows (`logical_plan`, `physical_plan`); plan text is non-empty |
| `test_explain_analyze` | `EXPLAIN ANALYZE SELECT * FROM test_ns.employees` returns ≥1 row; `output_rows` ≥ 0; `elapsed_ms` ≥ 0.0 |
| `test_explain_full` | `EXPLAIN FULL SELECT * FROM test_ns.employees` returns ≥1 row; the `TableScan` row has non-NULL `files_total` |

All three use the `employees` fixture table from `setup_join_fixture()`.

## Out of Scope

- `EXPLAIN FULL` for non-Iceberg tables (e.g., `information_schema`) — returns NULL for file/row estimates, no error
- `EXPLAIN` for DDL statements (`CREATE TABLE`, `INSERT INTO`) — returns `NotImplemented`
- Cost-based join reordering hints in EXPLAIN FULL output — future work
- Client-side formatted tree display — that is a CLI/JDBC concern, not engine concern
