//! End-to-end verification that the Trino `_colN` aliasing (#8) produces SQL
//! whose executed output schema actually carries those column names.
//!
//! The string-shape of the rewrite is covered by unit tests in
//! `sqe-sql::trino_compat`. This closes the other half: feed SQL through
//! `alias_anonymous_select_columns` and run it against a bare `SessionContext`,
//! asserting the resulting Arrow schema names the anonymous columns `_col0`,
//! `_col1`, ... rather than DataFusion's expression-text names.

use datafusion::execution::context::SessionContext;
use sqe_sql::alias_anonymous_select_columns;

/// Rewrite `input`, execute it, and return the output schema's column names.
async fn rewrite_and_column_names(input: &str) -> Vec<String> {
    let rewritten = alias_anonymous_select_columns(input);
    let ctx = SessionContext::new();
    let df = ctx
        .sql(&rewritten)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{rewritten}` (from `{input}`): {e}"));
    df.schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

#[tokio::test]
async fn anonymous_columns_execute_with_col_names() {
    // SELECT 1 AS x, 2 + 2, 'h' -> x, _col1, _col2 (absolute select position).
    let names = rewrite_and_column_names("SELECT 1 AS x, 2 + 2, 'h'").await;
    assert_eq!(names, vec!["x", "_col1", "_col2"], "got: {names:?}");
}

#[tokio::test]
async fn aggregate_after_column_is_col1() {
    // The canonical Trino shape: SELECT count(*) lands at position 0 -> _col0.
    let names = rewrite_and_column_names("SELECT count(*) FROM (SELECT 1) t").await;
    assert_eq!(names, vec!["_col0"], "got: {names:?}");
}

#[tokio::test]
async fn plain_columns_keep_their_names() {
    // No anonymous expressions: the rewrite is a no-op and DataFusion keeps the
    // real column names.
    let names = rewrite_and_column_names("SELECT a, b FROM (SELECT 1 AS a, 2 AS b) t").await;
    assert_eq!(names, vec!["a", "b"], "got: {names:?}");
}
