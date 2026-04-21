//! Regression tests for the view-lifted IN-subquery rewriter.
//!
//! Covers the behaviour introduced by the `dml-subquery-streaming` OpenSpec
//! change (see `openspec/changes/dml-subquery-streaming/`). The rewriter takes
//! a WHERE clause containing one or more `IN (subquery)` nodes, executes each
//! subquery once against the session context, registers the deduplicated
//! keyset as a scratch `MemTable`, and returns:
//!
//! - the rewritten WHERE string (with every `IN` node replaced by
//!   `COALESCE("__sqN"."__matched", FALSE)`),
//! - a concatenated `LEFT JOIN` clause to splice into the outer SELECT's FROM,
//! - an RAII guard that deregisters every scratch table on drop.
//!
//! The tests here build a DataFusion `SessionContext`, register in-memory
//! tables for the outer relation and the keyset, invoke the rewriter, then
//! execute `SELECT ... FROM t <joins_sql> WHERE <rewritten_where>` and check
//! the row set. This matches the SQL shape the DML handlers construct for
//! CoW `filter_batch_match` / `filter_batch_negate`, CoW `apply_update`, and
//! MoR `filter_batch_match`, so the tests cover the rewriter's contract for
//! all three call sites without needing a live Iceberg + Polaris stack.
//!
//! The stack-overflow reproduction lives in
//! `tests/in_subquery_or_stack_overflow.rs`. That file exercises DataFusion
//! directly and stays as a regression gate against a future reintroduction of
//! literal-inlining (task 5.10).

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use sqe_coordinator::write_handler::lift_in_subqueries;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

/// Build a two-column `(k: Int64, label: Utf8)` MemTable and register it.
fn register_two_col(ctx: &SessionContext, name: &str, ks: &[i64], labels: &[&str]) {
    assert_eq!(ks.len(), labels.len(), "k and label lengths must match");
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ks.to_vec())),
            Arc::new(StringArray::from(labels.to_vec())),
        ],
    )
    .expect("build batch");
    let mem = MemTable::try_new(schema, vec![vec![batch]]).expect("build memtable");
    ctx.register_table(name, Arc::new(mem)).expect("register");
}

/// Build a three-column `(c1: Int64, c2: Utf8, v: Int64)` MemTable and register it.
fn register_three_col(ctx: &SessionContext, name: &str, c1: &[i64], c2: &[&str], v: &[i64]) {
    assert_eq!(c1.len(), c2.len());
    assert_eq!(c1.len(), v.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(c1.to_vec())),
            Arc::new(StringArray::from(c2.to_vec())),
            Arc::new(Int64Array::from(v.to_vec())),
        ],
    )
    .expect("build batch");
    let mem = MemTable::try_new(schema, vec![vec![batch]]).expect("build memtable");
    ctx.register_table(name, Arc::new(mem)).expect("register");
}

/// Build a single-column `k: Int64` MemTable. Used for large keysets and NULL
/// fixtures that need nullable data.
fn register_single_col(ctx: &SessionContext, name: &str, ks: Vec<Option<i64>>) {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ks))])
        .expect("build batch");
    let mem = MemTable::try_new(schema, vec![vec![batch]]).expect("build memtable");
    ctx.register_table(name, Arc::new(mem)).expect("register");
}

/// Execute `SELECT c1 FROM t <joins_sql> WHERE <where_sql>` and return the
/// resulting `c1` values in a sorted `Vec<i64>`. Sorting makes assertions
/// order-independent, which is how the DML handlers consume the rewriter's
/// output (they feed it to per-file CoW SELECTs and aggregate the results).
async fn select_c1_where(
    ctx: &SessionContext,
    table: &str,
    joins_sql: &str,
    where_sql: &str,
) -> Vec<i64> {
    let sql = format!("SELECT c1 FROM {table}{joins_sql} WHERE {where_sql}");
    let df = ctx.sql(&sql).await.expect("plan outer SELECT");
    let batches = df.collect().await.expect("collect outer SELECT");
    let mut out: Vec<i64> = Vec::new();
    for b in batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c1 is Int64");
        for i in 0..b.num_rows() {
            if col.is_valid(i) {
                out.push(col.value(i));
            }
        }
    }
    out.sort_unstable();
    out
}

// ---------------------------------------------------------------------------
// 5.2 Single-column IN with small keyset
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn single_column_in_small_keyset() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        &[10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    );
    register_two_col(
        &ctx,
        "keyset",
        &[2, 4, 6, 8, 10],
        &["x", "x", "x", "x", "x"],
    );

    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");

    assert!(
        where_sql.contains("__matched"),
        "WHERE should reference the matcher flag: {where_sql}"
    );
    assert!(
        joins_sql.contains("LEFT JOIN"),
        "joins_sql should include a LEFT JOIN: {joins_sql}"
    );

    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert_eq!(rows, vec![2, 4, 6, 8, 10]);
}

// ---------------------------------------------------------------------------
// 5.3 Multi-column tuple IN with small keyset
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn multi_column_tuple_in_small_keyset() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        &[10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    );
    // Keyset matches (2,'b'), (5,'e'), (10,'j'); tuples that don't match any
    // outer row are included to confirm the semi-join drops them.
    register_two_col(
        &ctx,
        "keyset",
        &[2, 5, 10, 99],
        &["b", "e", "j", "nope"],
    );

    let (where_sql, joins_sql, _guard) = lift_in_subqueries(
        "(c1, c2) IN (SELECT k, label FROM keyset)",
        &ctx,
    )
    .await
    .expect("lift");

    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert_eq!(rows, vec![2, 5, 10]);
}

// ---------------------------------------------------------------------------
// 5.4 CoW DELETE-style multi-column tuple IN
// ---------------------------------------------------------------------------
//
// `handle_delete` builds `SELECT ... WHERE NOT (<where>)` to *keep* the
// unmatched rows (the preserved rows that get written to the rewritten data
// file). This test checks that the rewriter's output is negatable via NOT
// without re-introducing the subquery AST, matching `filter_batch_negate`'s
// call shape.

#[tokio::test(flavor = "multi_thread")]
async fn delete_shape_multi_column_not_predicate() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        &[10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    );
    register_two_col(&ctx, "keyset", &[2, 5, 10], &["b", "e", "j"]);

    let (where_sql, joins_sql, _guard) = lift_in_subqueries(
        "(c1, c2) IN (SELECT k, label FROM keyset)",
        &ctx,
    )
    .await
    .expect("lift");

    // Wrap in NOT to match what `filter_batch_negate` splices into its SELECT.
    let negated = format!("NOT ({where_sql})");
    let rows = select_c1_where(&ctx, "t", &joins_sql, &negated).await;
    assert_eq!(rows, vec![1, 3, 4, 6, 7, 8, 9]);
}

// ---------------------------------------------------------------------------
// 5.5 MoR DELETE-style single-column IN
// ---------------------------------------------------------------------------
//
// `handle_delete_mor` uses `filter_batch_match` (not `_negate`), i.e. the
// positive predicate identifies the rows that get position-deleted. This
// matches the standard IN shape; the test asserts the rewriter produces the
// same result set a user would expect from `DELETE FROM t WHERE k IN (...)`.

#[tokio::test(flavor = "multi_thread")]
async fn mor_delete_shape_single_column() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        &[10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    );
    register_two_col(&ctx, "keyset", &[3, 6, 9], &["_", "_", "_"]);

    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");

    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert_eq!(rows, vec![3, 6, 9]);
}

// ---------------------------------------------------------------------------
// 5.6 NOT IN with small keyset
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn not_in_single_column() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3, 4, 5],
        &["a", "b", "c", "d", "e"],
        &[10, 20, 30, 40, 50],
    );
    register_two_col(&ctx, "keyset", &[2, 4], &["_", "_"]);

    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 NOT IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");

    // NOT IN is encoded as `NOT COALESCE("__sqN"."__matched", FALSE)` by the
    // rewriter, so bare `WHERE <rewritten>` already has the correct polarity.
    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert_eq!(rows, vec![1, 3, 5]);
}

// ---------------------------------------------------------------------------
// 5.7 Empty subquery result
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn in_empty_subquery_matches_nothing() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3],
        &["a", "b", "c"],
        &[10, 20, 30],
    );
    register_two_col(&ctx, "keyset", &[], &[]);

    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");

    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert!(rows.is_empty(), "IN (empty) must match nothing, got {rows:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn not_in_empty_subquery_matches_everything() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3],
        &["a", "b", "c"],
        &[10, 20, 30],
    );
    register_two_col(&ctx, "keyset", &[], &[]);

    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 NOT IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");

    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert_eq!(rows, vec![1, 2, 3]);
}

// ---------------------------------------------------------------------------
// 5.8 NULL handling
// ---------------------------------------------------------------------------
//
// Spec: rows from the subquery with NULL in any matcher column are dropped
// from the scratch keyset. Outer rows with NULL in matcher columns do not
// match. This matches the old rewriter's behaviour (which skipped NULL
// subquery rows at the Rust level) and is a documented deviation from strict
// SQL `IN`/`NOT IN` semantics.

#[tokio::test(flavor = "multi_thread")]
async fn null_rows_in_subquery_are_dropped_from_keyset() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3, 4],
        &["a", "b", "c", "d"],
        &[10, 20, 30, 40],
    );
    register_single_col(&ctx, "keyset", vec![Some(2), None, Some(4), None]);

    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");

    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert_eq!(rows, vec![2, 4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn not_in_with_null_subquery_returns_non_matches() {
    let ctx = SessionContext::new();
    register_three_col(
        &ctx,
        "t",
        &[1, 2, 3, 4],
        &["a", "b", "c", "d"],
        &[10, 20, 30, 40],
    );
    register_single_col(&ctx, "keyset", vec![Some(2), None, Some(4)]);

    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 NOT IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");

    // Under strict SQL, NOT IN with a NULL element returns UNKNOWN for every
    // row and therefore yields zero matches. The documented SQE deviation is
    // that NULL subquery rows are dropped, so `NOT IN` returns non-matching
    // non-NULL rows: here, 1 and 3.
    let rows = select_c1_where(&ctx, "t", &joins_sql, &where_sql).await;
    assert_eq!(rows, vec![1, 3]);
}

// ---------------------------------------------------------------------------
// 5.9 Stress test: 1M-row subquery
// ---------------------------------------------------------------------------
//
// Under the old rewriter, this produced ~45 MB of WHERE text and the DataFusion
// analyzer overflowed the 8 MiB thread stack. Under the view-lifted rewriter
// the WHERE string is O(1) in subquery cardinality; the scratch MemTable
// absorbs the keyset.
//
// Gated with `#[ignore]` because 1M-row MemTable construction is slow in
// debug builds. Run explicitly:
//
//   cargo test -p sqe-coordinator --release --test in_subquery_view_rewrite \
//     stress_one_million_row_keyset -- --ignored --nocapture

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn stress_one_million_row_keyset() {
    const N: usize = 1_000_000;
    let ctx = SessionContext::new();

    // Outer table: 100 rows. Only c1 values 0..=99 exist, all of which match.
    let outer_ks: Vec<i64> = (0..100).collect();
    let outer_labels: Vec<String> = (0..100).map(|i| format!("r{i}")).collect();
    let outer_vs: Vec<i64> = (0..100).map(|i| i * 10).collect();
    let t_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, true),
        Field::new("c2", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let t_batch = RecordBatch::try_new(
        t_schema.clone(),
        vec![
            Arc::new(Int64Array::from(outer_ks)),
            Arc::new(StringArray::from(
                outer_labels.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(outer_vs)),
        ],
    )
    .expect("outer batch");
    let t_mem = MemTable::try_new(t_schema, vec![vec![t_batch]]).expect("outer mt");
    ctx.register_table("t", Arc::new(t_mem)).expect("register t");

    // Keyset: 1M rows. Values 0..N; outer only matches on 0..100.
    let keyset_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let keyset_vals: Vec<i64> = (0..N as i64).collect();
    let keyset_batch = RecordBatch::try_new(
        keyset_schema.clone(),
        vec![Arc::new(Int64Array::from(keyset_vals))],
    )
    .expect("keyset batch");
    let keyset_mem =
        MemTable::try_new(keyset_schema, vec![vec![keyset_batch]]).expect("keyset mt");
    ctx.register_table("keyset", Arc::new(keyset_mem))
        .expect("register keyset");

    let start = Instant::now();
    let (where_sql, joins_sql, _guard) =
        lift_in_subqueries("c1 IN (SELECT k FROM keyset)", &ctx)
            .await
            .expect("lift");
    let sql = format!("SELECT COUNT(*) AS n FROM t{joins_sql} WHERE {where_sql}");
    let df = ctx.sql(&sql).await.expect("plan stress");
    let batches = df.collect().await.expect("collect stress");
    let elapsed = start.elapsed();

    let n = batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0))
        .unwrap_or(-1);
    assert_eq!(n, 100, "all 100 outer rows should match");

    // 30 s ceiling on release; generous to tolerate CI variance.
    assert!(
        elapsed.as_secs() < 30,
        "lift + execute must finish under 30s; took {elapsed:?}"
    );
    eprintln!("[stress] N={N} outer=100 elapsed={elapsed:?}");
}

// ---------------------------------------------------------------------------
// 5.10 Placeholder marker test
// ---------------------------------------------------------------------------
//
// The stack-overflow reproduction lives in its own test binary
// (`tests/in_subquery_or_stack_overflow.rs`). We can't invoke it from here
// because a stack overflow is a process-wide abort, not a recoverable panic,
// and running both in one binary would mask failures. This test asserts only
// that the file exists so a future refactor does not silently remove the
// regression gate.

#[test]
fn stack_overflow_regression_gate_file_exists() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("in_subquery_or_stack_overflow.rs");
    assert!(
        path.exists(),
        "stack-overflow regression gate is missing at {path:?}"
    );
}
