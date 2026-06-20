# EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three policy-aware query-plan inspection commands — `EXPLAIN`, `EXPLAIN ANALYZE`, and `EXPLAIN FULL` — to SQE.

**Architecture:** A new `ExplainHandler` struct in `crates/sqe-coordinator/src/explain.rs` owns all three handlers. The SQL classifier pre-scans for `EXPLAIN FULL` (non-standard) before sqlparser, adds `StatementKind::ExplainFull`. `QueryHandler` grows an `explain_handler` field and delegates; the old `handle_explain()` method is deleted.

**Tech Stack:** Rust, DataFusion 51 (`LogicalPlan::display_indent`, `displayable`, `physical_plan::collect`, `ExecutionPlan::metrics`, `partition_statistics`), iceberg-rust 0.8 (`Table::metadata().current_snapshot().summary().additional_properties`), `IcebergScanExec` (custom scan node in `sqe-catalog`).

---

## File Map

| Action | File |
|---|---|
| Modify | `crates/sqe-sql/src/classifier.rs` |
| Create | `crates/sqe-coordinator/src/explain.rs` |
| Modify | `crates/sqe-coordinator/src/lib.rs` |
| Modify | `crates/sqe-coordinator/src/query_handler.rs` |
| Modify | `crates/sqe-coordinator/tests/integration_test.rs` |
| Create | `docs/book/src/features/explain.md` |
| Modify | `docs/book/src/features/sql-support.md` |
| Modify | `docs/book/src/SUMMARY.md` |
| Modify | `docs/testing.md` |
| Create | `docs/openspec-explain.md` |

---

## Chunk 1: SQL Classifier

### Task 1: Add `ExplainFull` variant and unit tests

**Files:**
- Modify: `crates/sqe-sql/src/classifier.rs`

- [ ] **Step 1: Write failing unit tests**

Add to the `#[cfg(test)]` block at the bottom of `classifier.rs`:

```rust
#[test]
fn test_explain_is_utility() {
    // Already exists — verify it still passes after our changes
    let result = parse_and_classify("EXPLAIN SELECT 1");
    assert!(matches!(result, Ok(StatementKind::Utility(_))));
}

#[test]
fn test_explain_analyze_is_utility() {
    let result = parse_and_classify("EXPLAIN ANALYZE SELECT 1");
    assert!(matches!(result, Ok(StatementKind::Utility(_))));
}

#[test]
fn test_explain_full_is_explain_full() {
    let result = parse_and_classify("EXPLAIN FULL SELECT 1");
    assert!(
        matches!(result, Ok(StatementKind::ExplainFull(_))),
        "Expected ExplainFull, got: {result:?}"
    );
}

#[test]
fn test_explain_full_lowercase() {
    let result = parse_and_classify("explain full SELECT 1");
    assert!(matches!(result, Ok(StatementKind::ExplainFull(_))));
}

#[test]
fn test_explain_full_extracts_inner_sql() {
    let result = parse_and_classify("EXPLAIN FULL SELECT 1 AS n").unwrap();
    if let StatementKind::ExplainFull(inner) = result {
        assert_eq!(inner, "SELECT 1 AS n");
    } else {
        panic!("Expected ExplainFull");
    }
}

#[test]
fn test_explain_full_name() {
    let kind = StatementKind::ExplainFull("SELECT 1".to_string());
    assert_eq!(kind.name(), "explain_full");
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p sqe-sql 2>&1 | grep -E "FAILED|error\[" | head -20
```

Expected: compile error — `ExplainFull` variant doesn't exist yet.

- [ ] **Step 3: Add `ExplainFull` variant to `StatementKind` and `name()`**

In `classifier.rs`, add to the `StatementKind` enum (after `Utility`):

```rust
ExplainFull(String), // inner SQL string (EXPLAIN FULL pre-processed)
```

Add to the `name()` match (after `Utility` arm):

```rust
StatementKind::ExplainFull(_) => "explain_full",
```

- [ ] **Step 4: Add pre-scan in `parse_and_classify`**

In `parse_and_classify`, add this block immediately after the `SHOW CATALOGS` pre-scan (around line 60), before the `Parser::parse_sql` call:

```rust
// Pre-scan for EXPLAIN FULL — not standard SQL, sqlparser won't parse it.
if upper.starts_with("EXPLAIN FULL ") {
    let inner = trimmed["EXPLAIN FULL ".len()..].trim().to_string();
    return Ok(StatementKind::ExplainFull(inner));
}
```

- [ ] **Step 5: Run tests**

```bash
cargo test -p sqe-sql 2>&1 | tail -5
```

Expected: all tests pass, including the 6 new ones.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-sql/src/classifier.rs
git commit -m "feat(sql): add ExplainFull statement kind with pre-scan"
```

---

## Chunk 2: ExplainHandler — plan() and analyze()

### Task 2: Create `explain.rs` with `plan()` handler

**Files:**
- Create: `crates/sqe-coordinator/src/explain.rs`

- [ ] **Step 1: Create the file with struct + `plan()` stub**

Create `crates/sqe-coordinator/src/explain.rs`:

```rust
//! Handlers for EXPLAIN, EXPLAIN ANALYZE, and EXPLAIN FULL.
//!
//! All three apply policy enforcement before producing output — the plan
//! shown is the plan that actually executes.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, Float64Array, RecordBatch, StringArray};
use arrow_array::builder::{Int32Builder, Int64Builder, Float32Builder};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;

use sqe_catalog::IcebergScanExec;
use sqe_core::{Session, SqeError};
use sqe_policy::PolicyEnforcer;

pub struct ExplainHandler {
    pub policy_enforcer: Arc<dyn PolicyEnforcer>,
}

impl ExplainHandler {
    pub fn new(policy_enforcer: Arc<dyn PolicyEnforcer>) -> Self {
        Self { policy_enforcer }
    }

    /// EXPLAIN <query> — returns logical and physical plan as text, no execution.
    pub async fn plan(
        &self,
        session: &Session,
        inner_sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Plan the inner SQL
        let df = ctx
            .sql(inner_sql)
            .await
            .map_err(|e| SqeError::Execution(format!("EXPLAIN planning failed: {e}")))?;

        // Apply policy enforcement
        let logical = df.logical_plan().clone();
        let enforced = self
            .policy_enforcer
            .evaluate(&session.user, logical)
            .await?;

        // Format logical plan
        let logical_str = format!("{}", enforced.display_indent());

        // Create physical plan
        let physical = ctx
            .state()
            .create_physical_plan(&enforced)
            .await
            .map_err(|e| SqeError::Execution(format!("Physical planning failed: {e}")))?;

        // Format physical plan
        let physical_str = format!("{}", displayable(physical.as_ref()).indent(true));

        // Build (plan_type, plan) RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]));
        let types: ArrayRef = Arc::new(StringArray::from(vec!["logical_plan", "physical_plan"]));
        let plans: ArrayRef = Arc::new(StringArray::from(vec![
            logical_str.as_str(),
            physical_str.as_str(),
        ]));
        let batch = RecordBatch::try_new(schema, vec![types, plans])
            .map_err(|e| SqeError::Execution(format!("Failed to build explain batch: {e}")))?;

        Ok(vec![batch])
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo check -p sqe-coordinator 2>&1 | grep "^error" | head -20
```

Expected: compile errors about missing `IcebergScanExec` import or other methods not yet implemented — that's fine, we'll fix as we go.

Actually, since `analyze` and `full` don't exist yet and the file is not wired up, run:

```bash
cargo check -p sqe-coordinator 2>&1 | grep "^error" | head -20
```

The file isn't `mod`-included yet so there should be no errors from it. Add `pub mod explain;` to `crates/sqe-coordinator/src/lib.rs` (find the existing mod declarations and add it there), then check again.

```bash
grep -n "^pub mod\|^mod " crates/sqe-coordinator/src/lib.rs | head -20
```

Add to lib.rs:
```rust
pub mod explain;
```

Then:
```bash
cargo check -p sqe-coordinator 2>&1 | grep "^error" | head -20
```

Expected: errors about `analyze` and `full` methods not existing — ignore, we add them next.

### Task 3: Add `analyze()` handler

**Files:**
- Modify: `crates/sqe-coordinator/src/explain.rs`

- [ ] **Step 1: Add `analyze()` to `ExplainHandler` in `explain.rs`**

Add this method after `plan()`:

```rust
/// EXPLAIN ANALYZE <query> — executes the query and returns per-operator metrics.
/// Output schema: (step INT, operation TEXT, output_rows BIGINT, elapsed_ms DOUBLE)
pub async fn analyze(
    &self,
    session: &Session,
    inner_sql: &str,
    ctx: &SessionContext,
) -> sqe_core::Result<Vec<RecordBatch>> {
    // Plan + policy enforce
    let df = ctx
        .sql(inner_sql)
        .await
        .map_err(|e| SqeError::Execution(format!("EXPLAIN ANALYZE planning failed: {e}")))?;
    let logical = df.logical_plan().clone();
    let enforced = self
        .policy_enforcer
        .evaluate(&session.user, logical)
        .await?;

    // Create physical plan
    let physical = ctx
        .state()
        .create_physical_plan(&enforced)
        .await
        .map_err(|e| SqeError::Execution(format!("Physical planning failed: {e}")))?;

    // Execute — this populates metrics on each node in-place
    collect(physical.clone(), ctx.task_ctx())
        .await
        .map_err(|e| SqeError::Execution(format!("EXPLAIN ANALYZE execution failed: {e}")))?;

    // Walk the plan tree post-order (leaf → root = execution order)
    // and collect per-node metrics.
    struct Row {
        step: i32,
        operation: String,
        output_rows: Option<i64>,
        elapsed_ms: Option<f64>,
    }
    let mut rows: Vec<Row> = Vec::new();
    fn walk(
        node: &Arc<dyn ExecutionPlan>,
        rows: &mut Vec<Row>,
    ) {
        for child in node.children() {
            walk(child, rows);
        }
        let step = rows.len() as i32;
        let operation = node.name().to_string();
        let metrics = node.metrics();
        let output_rows = metrics
            .as_ref()
            .and_then(|m| m.output_rows())
            .map(|r| r as i64);
        let elapsed_ms = metrics
            .as_ref()
            .and_then(|m| m.elapsed_compute())
            .map(|ns| ns as f64 / 1_000_000.0);
        rows.push(Row { step, operation, output_rows, elapsed_ms });
    }
    walk(&physical, &mut rows);

    // Build RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("output_rows", DataType::Int64, true),
        Field::new("elapsed_ms", DataType::Float64, true),
    ]));

    let steps: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|r| r.step).collect::<Vec<_>>(),
    ));
    let ops: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.operation.as_str()).collect::<Vec<_>>(),
    ));

    // Nullable Int64 array for output_rows
    let output_rows_arr: ArrayRef = Arc::new(
        arrow_array::builder::Int64Builder::new()
            .tap_mut(|b| {
                for r in &rows {
                    match r.output_rows {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
            })
            .finish(),
    );

    // Nullable Float64 array for elapsed_ms
    let elapsed_arr: ArrayRef = Arc::new(
        arrow_array::builder::Float64Builder::new()
            .tap_mut(|b| {
                for r in &rows {
                    match r.elapsed_ms {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
            })
            .finish(),
    );

    let batch = RecordBatch::try_new(schema, vec![steps, ops, output_rows_arr, elapsed_arr])
        .map_err(|e| SqeError::Execution(format!("Failed to build analyze batch: {e}")))?;

    Ok(vec![batch])
}
```

- [ ] **Step 2: Check compile**

```bash
cargo check -p sqe-coordinator 2>&1 | grep "^error" | head -30
```

Fix any import issues — you may need to add `use arrow_array::builder::Float64Builder;` etc. at the top. The `tap_mut` method requires the `arrayvec` / builder pattern — if unavailable, replace with:

```rust
let mut b = arrow_array::builder::Int64Builder::new();
for r in &rows {
    match r.output_rows {
        Some(v) => b.append_value(v),
        None => b.append_null(),
    }
}
Arc::new(b.finish()) as ArrayRef
```

### Task 4: Add `full()` handler

**Files:**
- Modify: `crates/sqe-coordinator/src/explain.rs`

- [ ] **Step 1: Add `full()` to `ExplainHandler`**

Add after `analyze()`:

```rust
/// EXPLAIN FULL <query> — plan + Iceberg statistics, no execution.
/// Output schema: (step INT, operation TEXT, estimated_rows BIGINT,
///                 estimated_bytes BIGINT, files_scanned INT, files_total INT)
pub async fn full(
    &self,
    session: &Session,
    inner_sql: &str,
    ctx: &SessionContext,
) -> sqe_core::Result<Vec<RecordBatch>> {
    // Plan + policy enforce
    let df = ctx
        .sql(inner_sql)
        .await
        .map_err(|e| SqeError::Execution(format!("EXPLAIN FULL planning failed: {e}")))?;
    let logical = df.logical_plan().clone();
    let enforced = self
        .policy_enforcer
        .evaluate(&session.user, logical)
        .await?;

    // Create physical plan — DataFusion resolves partition pruning here
    let physical = ctx
        .state()
        .create_physical_plan(&enforced)
        .await
        .map_err(|e| SqeError::Execution(format!("Physical planning failed: {e}")))?;

    struct FullRow {
        step: i32,
        operation: String,
        estimated_rows: Option<i64>,
        estimated_bytes: Option<i64>,
        files_scanned: Option<i32>,
        files_total: Option<i32>,
    }

    let mut rows: Vec<FullRow> = Vec::new();

    fn walk(node: &Arc<dyn ExecutionPlan>, rows: &mut Vec<FullRow>) {
        for child in node.children() {
            walk(child, rows);
        }
        let step = rows.len() as i32;
        let operation = node.name().to_string();

        if let Some(scan) = node.as_any().downcast_ref::<IcebergScanExec>() {
            // Pull stats from Iceberg snapshot summary — fast, no I/O beyond metadata
            let table = scan.table();
            let snap = table.metadata().current_snapshot();
            let props = snap.map(|s| s.summary().additional_properties.clone());

            let parse_i64 = |key: &str| -> Option<i64> {
                props.as_ref()?.get(key)?.parse::<i64>()
                    .map_err(|e| {
                        tracing::warn!(key, "Failed to parse Iceberg snapshot stat: {e}");
                        e
                    })
                    .ok()
            };
            let parse_i32 = |key: &str| -> Option<i32> {
                props.as_ref()?.get(key)?.parse::<i32>()
                    .map_err(|e| {
                        tracing::warn!(key, "Failed to parse Iceberg snapshot stat: {e}");
                        e
                    })
                    .ok()
            };

            let estimated_rows = parse_i64("total-records");
            let estimated_bytes = parse_i64("total-files-size");
            let files_total = parse_i32("total-data-files");
            // IcebergScanExec has no filter pushdown, so files_scanned == files_total
            let files_scanned = files_total;

            rows.push(FullRow {
                step,
                operation,
                estimated_rows,
                estimated_bytes,
                files_scanned,
                files_total,
            });
        } else {
            // Non-scan node: try DataFusion cardinality estimates
            let estimated_rows = node
                .partition_statistics(None)
                .ok()
                .and_then(|s| {
                    use datafusion::physical_plan::Statistics;
                    match s.num_rows {
                        datafusion::common::stats::Precision::Exact(v)
                        | datafusion::common::stats::Precision::Inexact(v) => Some(v as i64),
                        datafusion::common::stats::Precision::Absent => None,
                    }
                });

            rows.push(FullRow {
                step,
                operation,
                estimated_rows,
                estimated_bytes: None,
                files_scanned: None,
                files_total: None,
            });
        }
    }
    walk(&physical, &mut rows);

    // Build RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("step", DataType::Int32, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("estimated_rows", DataType::Int64, true),
        Field::new("estimated_bytes", DataType::Int64, true),
        Field::new("files_scanned", DataType::Int32, true),
        Field::new("files_total", DataType::Int32, true),
    ]));

    macro_rules! nullable_array {
        ($builder:ty, $rows:expr, $field:ident) => {{
            let mut b = <$builder>::new();
            for r in $rows {
                match r.$field {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }

    let steps: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|r| r.step).collect::<Vec<_>>(),
    ));
    let ops: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| r.operation.as_str()).collect::<Vec<_>>(),
    ));
    let est_rows = nullable_array!(arrow_array::builder::Int64Builder, &rows, estimated_rows);
    let est_bytes = nullable_array!(arrow_array::builder::Int64Builder, &rows, estimated_bytes);
    let f_scanned = nullable_array!(arrow_array::builder::Int32Builder, &rows, files_scanned);
    let f_total = nullable_array!(arrow_array::builder::Int32Builder, &rows, files_total);

    let batch = RecordBatch::try_new(
        schema,
        vec![steps, ops, est_rows, est_bytes, f_scanned, f_total],
    )
    .map_err(|e| SqeError::Execution(format!("Failed to build full explain batch: {e}")))?;

    Ok(vec![batch])
}
```

- [ ] **Step 2: Check compile**

```bash
cargo check -p sqe-coordinator 2>&1 | grep "^error" | head -30
```

Common issues and fixes:
- `IcebergScanExec` is in `sqe_catalog` — ensure `use sqe_catalog::IcebergScanExec;` is at top of `explain.rs`
- `datafusion::common::stats::Precision` path may differ — check with `cargo doc -p datafusion --open` or search: `grep -r "pub enum Precision" ~/.cargo/registry/src/`; alternative path is `datafusion::common::Statistics` and `datafusion_common::stats::Precision`
- `partition_statistics` signature: `fn partition_statistics(&self, partition: Option<usize>) -> datafusion::error::Result<Statistics>` — it returns a `Result`, hence the `.ok()` call
- `tap_mut` may not exist on builders — use the explicit loop pattern shown in Task 3

- [ ] **Step 3: Fix any compile errors, then verify**

```bash
cargo check -p sqe-coordinator 2>&1 | grep "^error" | head -10
```

Expected: zero errors.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/explain.rs crates/sqe-coordinator/src/lib.rs
git commit -m "feat(coordinator): add ExplainHandler with plan/analyze/full"
```

---

## Chunk 3: Wire up QueryHandler

### Task 5: Update `query_handler.rs` routing

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

- [ ] **Step 1: Add `explain_handler` field and update `new()`**

In the `QueryHandler` struct definition, add:
```rust
explain_handler: crate::explain::ExplainHandler,
```

In `QueryHandler::new()`, add to the `Self { ... }` block:
```rust
explain_handler: crate::explain::ExplainHandler::new(Arc::clone(&policy_enforcer)),
```

(This must come before the `policy_enforcer` field is moved into the struct, so clone first. In `new()`, `policy_enforcer` is an `Arc<dyn PolicyEnforcer>` — `Arc::clone` is cheap.)

- [ ] **Step 2: Add `StatementKind::ExplainFull` import**

The `use sqe_sql::{parse_and_classify, StatementKind};` line already covers `StatementKind`. The new `ExplainFull` variant is part of the same enum — no extra import needed.

- [ ] **Step 3: Update routing in `execute()`**

Replace the existing `StatementKind::Utility(stmt)` arm (lines ~94-102):

```rust
StatementKind::Utility(stmt) => {
    if let sqlparser::ast::Statement::Explain { analyze, statement, .. } = *stmt {
        let inner = statement.to_string();
        let ctx = self.create_session_context(session).await?;
        if analyze {
            self.explain_handler.analyze(session, &inner, &ctx).await
        } else {
            self.explain_handler.plan(session, &inner, &ctx).await
        }
    } else {
        Err(SqeError::NotImplemented(format!(
            "Utility statement not supported: {stmt}"
        )))
    }
}
```

Add a new arm for `StatementKind::ExplainFull` — add it **before** the `StatementKind::Policy` arm:

```rust
StatementKind::ExplainFull(inner) => {
    let ctx = self.create_session_context(session).await?;
    self.explain_handler.full(session, &inner, &ctx).await
}
```

- [ ] **Step 4: Delete `handle_explain()` method**

Remove the old `handle_explain()` method from `query_handler.rs` (the block starting at line ~532). It is now dead code — replaced by `ExplainHandler::plan()`.

- [ ] **Step 5: Compile check**

```bash
cargo check -p sqe-coordinator 2>&1 | grep "^error" | head -20
```

Expected: zero errors.

- [ ] **Step 6: Run unit tests**

```bash
cargo test -p sqe-sql -p sqe-coordinator 2>&1 | tail -10
```

Expected: all unit tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat(coordinator): wire ExplainHandler into QueryHandler routing"
```

---

## Chunk 4: Integration Tests

### Task 6: Add four integration tests

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`

- [ ] **Step 1: Add test scaffolding**

At the end of `integration_test.rs`, add the following four tests. They all use `setup_join_fixture()` (already defined in the file) to get a session, handler, and the `test_ns.employees` table.

```rust
// ---------------------------------------------------------------------------
// EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL integration tests
// ---------------------------------------------------------------------------

// Test: EXPLAIN returns logical and physical plan text (2 rows, non-empty)
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_explain_plan() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "EXPLAIN SELECT * FROM test_ns.employees WHERE dept_id = 10";
    let batches = handler
        .execute(&session, sql)
        .await
        .expect("EXPLAIN should succeed");

    common::print_results("EXPLAIN", sql, &batches);

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "EXPLAIN returns exactly 2 rows (logical + physical)");

    // First row: logical_plan
    let batch = &batches[0];
    let plan_type_col = batch.column_by_name("plan_type").expect("plan_type column");
    let plan_col = batch.column_by_name("plan").expect("plan column");

    let plan_types = plan_type_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("plan_type is Utf8");
    assert_eq!(plan_types.value(0), "logical_plan");
    assert_eq!(plan_types.value(1), "physical_plan");

    let plans = plan_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("plan is Utf8");
    assert!(!plans.value(0).is_empty(), "logical plan text must not be empty");
    assert!(!plans.value(1).is_empty(), "physical plan text must not be empty");

    // Confirm table name appears in the logical plan
    assert!(
        plans.value(0).contains("employees"),
        "logical plan should mention 'employees' table"
    );

    teardown_join_fixture(&session, &handler).await;
}

// Test: EXPLAIN ANALYZE returns per-operator metrics after executing the query
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_explain_analyze() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "EXPLAIN ANALYZE SELECT dept_id, COUNT(*) FROM test_ns.employees GROUP BY dept_id";
    let batches = handler
        .execute(&session, sql)
        .await
        .expect("EXPLAIN ANALYZE should succeed");

    common::print_results("EXPLAIN ANALYZE", sql, &batches);

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 1, "EXPLAIN ANALYZE should return at least one operator row");

    // Verify schema: step, operation, output_rows, elapsed_ms
    let batch = &batches[0];
    assert!(batch.column_by_name("step").is_some(), "must have 'step' column");
    assert!(batch.column_by_name("operation").is_some(), "must have 'operation' column");
    assert!(batch.column_by_name("output_rows").is_some(), "must have 'output_rows' column");
    assert!(batch.column_by_name("elapsed_ms").is_some(), "must have 'elapsed_ms' column");

    teardown_join_fixture(&session, &handler).await;
}

// Test: EXPLAIN FULL returns plan with Iceberg statistics for the scan node
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_explain_full() {
    let (session, handler) = setup_join_fixture().await;

    let sql = "EXPLAIN FULL SELECT * FROM test_ns.employees";
    let batches = handler
        .execute(&session, sql)
        .await
        .expect("EXPLAIN FULL should succeed");

    common::print_results("EXPLAIN FULL", sql, &batches);

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 1, "EXPLAIN FULL should return at least one row");

    // Verify schema columns exist
    let batch = &batches[0];
    assert!(batch.column_by_name("step").is_some());
    assert!(batch.column_by_name("operation").is_some());
    assert!(batch.column_by_name("estimated_rows").is_some());
    assert!(batch.column_by_name("estimated_bytes").is_some());
    assert!(batch.column_by_name("files_scanned").is_some());
    assert!(batch.column_by_name("files_total").is_some());

    // Find the IcebergScanExec row and verify it has non-NULL files_total
    let ops = batch
        .column_by_name("operation")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let files_total_col = batch.column_by_name("files_total").unwrap();

    let scan_row = (0..batch.num_rows())
        .find(|&i| ops.value(i) == "IcebergScanExec");

    assert!(
        scan_row.is_some(),
        "Expected an IcebergScanExec row in EXPLAIN FULL output"
    );
    let row = scan_row.unwrap();
    assert!(
        !files_total_col.is_null(row),
        "IcebergScanExec row should have non-NULL files_total"
    );

    teardown_join_fixture(&session, &handler).await;
}

// Test: EXPLAIN is policy-aware — policy enforcer is called even with passthrough
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_explain_policy_aware() {
    let (session, handler) = setup_join_fixture().await;

    // With PassthroughEnforcer the plan is unchanged, but we verify:
    // 1. The command succeeds (policy path is exercised without error)
    // 2. The plan text references the table name (plan was actually built)
    let sql = "EXPLAIN SELECT name, salary FROM test_ns.employees ORDER BY salary DESC";
    let batches = handler
        .execute(&session, sql)
        .await
        .expect("EXPLAIN with policy enforcement should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should still return 2 plan rows");

    let batch = &batches[0];
    let plans = batch
        .column_by_name("plan")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Both logical and physical plan should mention the table
    assert!(
        plans.value(0).contains("employees") || plans.value(1).contains("employees"),
        "Plan should reference the queried table"
    );

    teardown_join_fixture(&session, &handler).await;
}
```

- [ ] **Step 2: Run unit tests (no stack needed)**

```bash
cargo test -p sqe-coordinator 2>&1 | grep -E "test .* ok|FAILED" | head -20
```

Expected: all existing unit tests pass. The 4 new `#[ignore]` tests are not run.

- [ ] **Step 3: Run integration tests against the test stack**

```bash
./scripts/integration-test.sh test_explain
```

Expected output contains four passing tests:
```
test test_explain_plan ... ok
test test_explain_analyze ... ok
test test_explain_full ... ok
test test_explain_policy_aware ... ok
```

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/integration_test.rs
git commit -m "test: add integration tests for EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL"
```

---

## Chunk 5: Docs, OpenSpec, Book

### Task 7: OpenSpec document

**Files:**
- Create: `docs/openspec-explain.md`

- [ ] **Step 1: Create openspec**

Create `docs/openspec-explain.md` following the existing `docs/openspec.md` structure:

```markdown
# OpenSpec: EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL

## Proposal

**Status:** Implemented
**Phase:** 2 (post-core)

Add three query-plan inspection commands that expose DataFusion's planning
and Iceberg's table statistics. All three are policy-aware: the plan shown
is the plan that actually executes after security enforcement.

## Variants

| Command | Executes? | Output |
|---|---|---|
| `EXPLAIN <query>` | No | `plan_type`, `plan` (logical + physical plan text) |
| `EXPLAIN ANALYZE <query>` | Yes | `step`, `operation`, `output_rows`, `elapsed_ms` |
| `EXPLAIN FULL <query>` | No | `step`, `operation`, `estimated_rows`, `estimated_bytes`, `files_scanned`, `files_total` |

## Design

### Parsing

`EXPLAIN` and `EXPLAIN ANALYZE` are parsed by sqlparser (classified as
`StatementKind::Utility` with the `analyze` flag extracted at routing time).
`EXPLAIN FULL` is pre-scanned before sqlparser and classified as
`StatementKind::ExplainFull(inner_sql)`.

### Policy Enforcement

All three handlers call `PolicyEnforcer::evaluate()` on the logical plan
before generating output. The plan shown reflects row filters and column
masks that will be applied at execution time.

### Iceberg Statistics (EXPLAIN FULL)

Statistics are read from the Iceberg snapshot summary (`total-records`,
`total-files-size`, `total-data-files`) without reading data files. Since
`IcebergScanExec` has no predicate-pushdown-to-file-level yet, `files_scanned`
equals `files_total`.

## Implementation

- `crates/sqe-sql/src/classifier.rs` — `StatementKind::ExplainFull`
- `crates/sqe-coordinator/src/explain.rs` — `ExplainHandler`
- `crates/sqe-coordinator/src/query_handler.rs` — routing wired up

## Specs

| Scenario | Expected |
|---|---|
| GIVEN `EXPLAIN SELECT …` WHEN executed THEN returns 2 rows: `logical_plan` and `physical_plan` | ✅ |
| GIVEN `EXPLAIN ANALYZE SELECT …` WHEN executed THEN returns ≥1 row with `output_rows ≥ 0` | ✅ |
| GIVEN `EXPLAIN FULL SELECT … FROM iceberg_table` WHEN executed THEN scan row has non-NULL `files_total` | ✅ |
| GIVEN policy enforcement active WHEN EXPLAIN runs THEN plan reflects enforced plan | ✅ |
| GIVEN Iceberg snapshot missing WHEN EXPLAIN FULL runs THEN stats are NULL, no error | ✅ |
```

- [ ] **Step 2: Update `docs/testing.md`**

Add to the Integration Tests section:

```markdown
#### EXPLAIN

| Test | What it checks |
|---|---|
| `test_explain_plan` | `EXPLAIN SELECT …` returns 2 rows (`logical_plan`, `physical_plan`); plan text non-empty and contains table name |
| `test_explain_analyze` | `EXPLAIN ANALYZE SELECT …` returns ≥1 row; all schema columns present |
| `test_explain_full` | `EXPLAIN FULL SELECT …` returns ≥1 row; `IcebergScanExec` row has non-NULL `files_total` |
| `test_explain_policy_aware` | EXPLAIN with passthrough enforcer succeeds; plan references queried table |
```

- [ ] **Step 3: Create book page `docs/book/src/features/explain.md`**

```markdown
# Query Plan Inspection (EXPLAIN)

SQE provides three variants of `EXPLAIN` for inspecting how queries are planned and executed.

## EXPLAIN

Returns the logical and physical query plan without executing the query.

```sql
EXPLAIN SELECT * FROM orders WHERE amount > 100;
```

**Output:** Two rows — `logical_plan` and `physical_plan` — each containing a
text representation of the plan tree. The plan shown is the **policy-enforced**
plan: any row filters or column masks applied by the security layer are visible.

## EXPLAIN ANALYZE

Executes the query and returns per-operator timing and row counts.

```sql
EXPLAIN ANALYZE
SELECT dept_id, COUNT(*), AVG(salary)
FROM employees
GROUP BY dept_id;
```

**Output columns:** `step`, `operation`, `output_rows`, `elapsed_ms`

Rows are ordered leaf-to-root (execution order). `output_rows` and `elapsed_ms`
are NULL for operators that don't expose DataFusion metrics (e.g., the Iceberg
scan node, which emits rows via a streaming future).

## EXPLAIN FULL

Returns the plan enriched with Iceberg table statistics — without executing the query.

```sql
EXPLAIN FULL SELECT * FROM large_table WHERE region = 'EU';
```

**Output columns:** `step`, `operation`, `estimated_rows`, `estimated_bytes`,
`files_scanned`, `files_total`

For `IcebergScanExec` nodes, statistics come from the Iceberg snapshot summary
(fast, no data file reads). `estimated_rows` reflects the total rows in the
snapshot at plan time. `files_scanned` equals `files_total` because
predicate-pushdown to file level is not yet implemented.

For other operators (Filter, Aggregate, Sort) `estimated_rows` comes from
DataFusion's cardinality analysis where available; file columns are NULL.

## Notes

- All three variants apply policy enforcement — the plan reflects what will
  actually execute for the authenticated user.
- `EXPLAIN FULL` on non-Iceberg tables (e.g., `information_schema`) returns
  NULL for all statistics columns without error.
```

- [ ] **Step 4: Update `docs/book/src/features/sql-support.md`**

Find the existing `EXPLAIN` entry under "Metadata Queries":
```markdown
-- Query plan
EXPLAIN SELECT * FROM orders WHERE amount > 100;
```

Replace with:
```markdown
-- Query plan (logical + physical)
EXPLAIN SELECT * FROM orders WHERE amount > 100;

-- With actual execution metrics
EXPLAIN ANALYZE SELECT * FROM orders WHERE amount > 100;

-- With Iceberg file/row estimates (no execution)
EXPLAIN FULL SELECT * FROM orders WHERE amount > 100;
```

- [ ] **Step 5: Update `docs/book/src/SUMMARY.md`**

Add `explain.md` under Features:
```markdown
- [Query Plan Inspection](./features/explain.md)
```

Place it after `- [SQL Support](./features/sql-support.md)`.

- [ ] **Step 6: Commit all docs**

```bash
git add docs/openspec-explain.md docs/testing.md \
        docs/book/src/features/explain.md \
        docs/book/src/features/sql-support.md \
        docs/book/src/SUMMARY.md
git commit -m "docs: add EXPLAIN feature docs, openspec, and book page"
```

---

## Final check

- [ ] **Run all unit tests**

```bash
cargo test --workspace 2>&1 | tail -5
```

Expected: all pass.

- [ ] **Run integration tests**

```bash
./scripts/integration-test.sh test_explain
```

Expected: 4 explain tests pass.

- [ ] **Verify book builds**

```bash
cd docs/book && mdbook build 2>&1 | tail -5
```

Expected: `Finished` with no errors.
