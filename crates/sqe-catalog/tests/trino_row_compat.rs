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
use sqe_sql::{rewrite_ctas_compat, rewrite_trino_compat};

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
    assert_eq!(
        batches[0].num_rows(),
        1,
        "expected one row for `{rewritten}`"
    );
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
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();
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
async fn fetch_with_ties_order_column_absent_from_select() {
    // #319 regression: the ORDER BY column is NOT in the SELECT list -- the
    // exact shape of testRollbackToSnapshot (`SELECT snapshot_id ... ORDER BY
    // committed_at FETCH FIRST 1 ROW WITH TIES`). The inner subquery projects
    // only the selected column + the synthetic rank, so the outer query must
    // order by the rank, not the (now-absent) order key. Before the fix this
    // failed to plan ("No field named ord"); now it must plan and execute.
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("sel", DataType::Int64, false),
        Field::new("ord", DataType::Int64, false),
    ]));
    // (sel, ord): ranks by ord are 1,2,2,4 -> rank<=2 keeps sel {100,200,300}.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
            Arc::new(Int64Array::from(vec![10, 20, 20, 30])),
        ],
    )
    .unwrap();
    ctx.register_batch("t2", batch).unwrap();

    let input = "SELECT sel FROM t2 ORDER BY ord FETCH FIRST 2 ROWS WITH TIES";
    let rewritten = rewrite_trino_compat(input);
    let batches = ctx
        .sql(&rewritten)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{rewritten}` (from `{input}`): {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("execution failed for `{rewritten}`: {e}"));
    let mut got = Vec::new();
    for b in &batches {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..col.len() {
            got.push(col.value(i));
        }
    }
    got.sort_unstable();
    assert_eq!(
        got,
        vec![100, 200, 300],
        "WITH TIES keeps the tied row by rank"
    );
}

#[tokio::test]
async fn fetch_first_only_executes_as_limit() {
    // FETCH FIRST 2 ROWS ONLY -> LIMIT 2: exactly two rows, ties irrelevant.
    let got = run_int_query(
        vec![10, 20, 20, 30],
        "SELECT k FROM t ORDER BY k FETCH FIRST 2 ROWS ONLY",
    )
    .await;
    assert_eq!(
        got.len(),
        2,
        "ONLY -> LIMIT 2 returns exactly two rows: {got:?}"
    );
}

#[tokio::test]
async fn ctas_as_table_qualified_source_copies_rows() {
    // #330: `CREATE TABLE dst AS TABLE <qualified.name>` -> `AS SELECT * FROM
    // <qualified.name>`. A dotted source is what sqlparser rejects, so use a
    // 3-part name DataFusion resolves (`datafusion.public.<table>`) to prove the
    // rewrite both parses and executes a real copy.
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
    ctx.register_batch("src", batch).unwrap();

    let ctas = "CREATE TABLE dst AS TABLE datafusion.public.src";
    let rewritten = rewrite_ctas_compat(ctas);
    assert!(
        rewritten.contains("AS SELECT * FROM datafusion.public.src"),
        "AS TABLE expanded: {rewritten}"
    );
    ctx.sql(&rewritten)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{rewritten}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("CTAS execution failed for `{rewritten}`: {e}"));

    let batches = ctx
        .sql("SELECT n FROM dst")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut got = Vec::new();
    for b in &batches {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..col.len() {
            got.push(col.value(i));
        }
    }
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3], "AS TABLE copied all source rows");
}

/// Run a CTAS through `rewrite_ctas_compat` against a fresh SessionContext,
/// then query `SELECT <select> FROM <table>` and return the column-0 i64 values
/// (sorted). Proves the rewritten DDL plans, executes, and produces the
/// expected schema/rows on DataFusion (which shares sqlparser 0.62 with SQE).
async fn run_ctas_then_query(ctas: &str, table: &str, select: &str) -> Vec<i64> {
    use datafusion::arrow::array::Int64Array;

    let ctx = SessionContext::new();
    let rewritten = rewrite_ctas_compat(ctas);
    ctx.sql(&rewritten)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{rewritten}` (from `{ctas}`): {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("CTAS execution failed for `{rewritten}`: {e}"));

    let q = format!("SELECT {select} FROM {table}");
    let batches = ctx
        .sql(&q)
        .await
        .unwrap_or_else(|e| panic!("query planning failed for `{q}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("query execution failed for `{q}`: {e}"));
    let mut out = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column 0");
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out.sort_unstable();
    out
}

#[tokio::test]
async fn ctas_column_alias_list_renames_output_columns() {
    // #328: `(x, y)` is a name-only alias list, not a typed coldef list. The
    // rewrite must rename the VALUES output columns positionally so the new
    // names resolve. Selecting by alias proves the rename took effect.
    let got = run_ctas_then_query(
        "CREATE TABLE aliased (x, y) AS VALUES (10, 20), (30, 40)",
        "aliased",
        "x",
    )
    .await;
    assert_eq!(
        got,
        vec![10, 30],
        "alias `x` must resolve to VALUES column 0"
    );
}

#[tokio::test]
async fn ctas_with_no_data_creates_empty_table() {
    // #322: WITH NO DATA creates the table structure with zero rows. The
    // column must still exist (schema preserved) but no rows materialize.
    let got = run_ctas_then_query(
        "CREATE TABLE empties AS SELECT 7 AS a WITH NO DATA",
        "empties",
        "a",
    )
    .await;
    assert!(
        got.is_empty(),
        "WITH NO DATA must yield zero rows, got {got:?}"
    );
}

#[tokio::test]
async fn ctas_with_data_materializes_rows() {
    // #322: WITH DATA is the default -- rows are materialized.
    let got = run_ctas_then_query(
        "CREATE TABLE filled AS SELECT 5 AS a WITH DATA",
        "filled",
        "a",
    )
    .await;
    assert_eq!(got, vec![5], "WITH DATA must materialize the row");
}

#[tokio::test]
async fn uuid_literal_executes_as_string() {
    // #326: `UUID '...'` is rejected by DataFusion ("Unsupported SQL type
    // UUID"). The compat rewrite turns it into a plain string literal (SQE
    // stores UUID as Utf8), so it plans and executes as a string column.
    let dt = rewrite_and_run("SELECT UUID 'bdeb4567-89ab-cdef-0123-456789abcdef'").await;
    assert!(
        dt.starts_with("Utf8"),
        "UUID literal should yield a Utf8 column, got: {dt}"
    );
}

#[tokio::test]
async fn cast_as_uuid_executes_as_string() {
    // #326: CAST(.. AS uuid) is likewise rejected by DataFusion; the compat
    // rewrite maps it to VARCHAR. Locks in that the read path works.
    let dt = rewrite_and_run("SELECT CAST('bdeb4567-89ab-cdef-0123-456789abcdef' AS uuid)").await;
    assert!(
        dt.starts_with("Utf8"),
        "CAST AS uuid should yield a Utf8 column, got: {dt}"
    );
}

#[tokio::test]
async fn ctas_with_uuid_value_materializes() {
    // #326: the issue's CTAS repro -- materialize a uuid value via CTAS. The
    // CAST(.. AS uuid) rewrite makes it a Utf8 column, which the (in-memory)
    // CTAS path writes without hitting "Unsupported SQL type UUID".
    use datafusion::arrow::array::StringViewArray;
    let ctx = SessionContext::new();
    let ctas = rewrite_trino_compat(
        "CREATE TABLE u AS SELECT CAST('bdeb4567-89ab-cdef-0123-456789abcdef' AS uuid) c",
    );
    ctx.sql(&ctas)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{ctas}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("CTAS execution failed for `{ctas}`: {e}"));
    let batches = ctx
        .sql("SELECT c FROM u")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // CAST(.. AS varchar) yields a Utf8View column; the uuid is stored as its
    // string representation.
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("uuid stored as a string-view column");
    assert_eq!(col.value(0), "bdeb4567-89ab-cdef-0123-456789abcdef");
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
    assert!(
        dt.contains("\"x\""),
        "field x missing from struct type: {dt}"
    );
    assert!(
        dt.contains("\"y\""),
        "field y missing from struct type: {dt}"
    );
}
