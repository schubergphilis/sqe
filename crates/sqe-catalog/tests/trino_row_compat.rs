//! End-to-end verification that the Trino ROW-compat rewrites (#7) produce
//! SQL that DataFusion actually plans and executes.
//!
//! The string-shape of each rewrite is covered by unit tests in
//! `sqe-sql::trino_compat`. Those prove the rewriter emits `struct(...)` /
//! `named_struct(...)`. This test closes the other half: it feeds the Trino
//! probe SQL through `rewrite_trino_compat` and runs the rewritten string
//! against a bare `SessionContext`, so a regression where the rewrite target
//! is syntactically valid but not a registered DataFusion function (or casts
//! a type DataFusion cannot coerce) fails here rather than in production.

use datafusion::execution::context::SessionContext;
use sqe_sql::rewrite_trino_compat;

/// Rewrite `input` the way the coordinator's pre-parse stage does, then plan +
/// execute it against a default SessionContext and return the single scalar
/// result's debug rendering. Panics with a clear message if planning or
/// execution fails, which is exactly the regression this test guards.
async fn rewrite_and_run(input: &str) -> String {
    let rewritten = rewrite_trino_compat(input);
    let ctx = SessionContext::new();
    let df = ctx
        .sql(&rewritten)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{rewritten}` (from `{input}`): {e}"));
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("execution failed for `{rewritten}` (from `{input}`): {e}"));
    assert_eq!(batches.len(), 1, "expected one batch for `{rewritten}`");
    assert_eq!(batches[0].num_rows(), 1, "expected one row for `{rewritten}`");
    let col = batches[0].column(0);
    format!("{:?}", col.data_type())
}

/// Register a single-column i64 table `t`, rewrite + run `input`, and return
/// the values of column 0 across all batches (sorted) so order-independent
/// assertions are easy.
async fn run_int_query(values: Vec<i64>, input: &str) -> Vec<i64> {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();
    ctx.register_batch("t", batch).unwrap();

    let rewritten = rewrite_trino_compat(input);
    let df = ctx
        .sql(&rewritten)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{rewritten}` (from `{input}`): {e}"));
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("execution failed for `{rewritten}` (from `{input}`): {e}"));
    let mut out = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out.sort_unstable();
    out
}

#[tokio::test]
async fn fetch_first_with_ties_includes_tied_rows() {
    // Keys [10,20,20,30], FETCH FIRST 2 ROWS WITH TIES -> ranks 1,2,2,4;
    // rank <= 2 keeps [10,20,20]. The tie at the cutoff must be included --
    // this is the whole point of WITH TIES, and the no-ties case wouldn't
    // exercise it. Also proves DataFusion plans the RANK-in-subquery rewrite.
    let got = run_int_query(
        vec![10, 20, 20, 30],
        "SELECT k FROM t ORDER BY k FETCH FIRST 2 ROWS WITH TIES",
    )
    .await;
    assert_eq!(got, vec![10, 20, 20], "WITH TIES must include the tied row");
}

#[tokio::test]
async fn fetch_first_only_executes_as_limit() {
    // FETCH FIRST 2 ROWS ONLY -> LIMIT 2: exactly two rows, ties irrelevant.
    let got = run_int_query(
        vec![10, 20, 20, 30],
        "SELECT k FROM t ORDER BY k FETCH FIRST 2 ROWS ONLY",
    )
    .await;
    assert_eq!(got.len(), 2, "ONLY -> LIMIT 2 returns exactly two rows: {got:?}");
}

#[tokio::test]
async fn row_constructor_executes_as_struct() {
    // SELECT ROW(1, 'a', true) -> struct(1, 'a', true): a struct column.
    let dt = rewrite_and_run("SELECT ROW(1, 'a', true)").await;
    assert!(
        dt.starts_with("Struct"),
        "ROW(...) should yield a Struct column, got: {dt}"
    );
}

#[tokio::test]
async fn cast_row_to_named_row_executes_as_named_struct() {
    // SELECT CAST(ROW(1, 'a') AS ROW(x int, y varchar))
    //   -> named_struct('x', CAST(1 AS int), 'y', CAST('a' AS varchar))
    let dt = rewrite_and_run("SELECT CAST(ROW(1, 'a') AS ROW(x int, y varchar))").await;
    assert!(
        dt.starts_with("Struct"),
        "named-row cast should yield a Struct column, got: {dt}"
    );
    // The declared field names must survive into the struct schema.
    assert!(dt.contains("\"x\""), "field x missing from struct type: {dt}");
    assert!(dt.contains("\"y\""), "field y missing from struct type: {dt}");
}
