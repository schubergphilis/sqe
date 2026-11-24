//! `approx_distinct` over `double` and `real`, as Trino counts them: every
//! NaN is one value, `-0.0` and `0.0` are one value, NULL is not counted.

use std::sync::Arc;

use datafusion::arrow::array::{Float32Array, Float64Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

/// Rows: three NaNs (two bit patterns), `-0.0` and `0.0`, `1.5` twice, NULL.
/// Distinct values: NaN, 0, 1.5 -> 3. Group `a` holds NaN, -0.0, 1.5 (3);
/// group `b` holds NaN, 0.0, 1.5, NULL (3); group `c` holds only NaN (1).
fn batch() -> RecordBatch {
    let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
    let d = vec![
        Some(f64::NAN),
        Some(-0.0),
        Some(1.5),
        Some(other_nan),
        Some(0.0),
        Some(1.5),
        None,
        Some(-f64::NAN),
    ];
    let r: Vec<Option<f32>> = d.iter().map(|v| v.map(|x| x as f32)).collect();
    let k = vec!["a", "a", "a", "b", "b", "b", "b", "c"];
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("d", DataType::Float64, true),
        Field::new("r", DataType::Float32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(k)),
            Arc::new(Float64Array::from(d)),
            Arc::new(Float32Array::from(r)),
        ],
    )
    .expect("batch")
}

/// `t` from `parts`, under `config`.
fn ctx_with(config: SessionConfig, parts: Vec<Vec<RecordBatch>>) -> SessionContext {
    let ctx = SessionContext::new_with_config(config);
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);
    let table = MemTable::try_new(batch().schema(), parts).expect("memtable");
    ctx.register_table("t", Arc::new(table))
        .expect("register t");
    ctx
}

fn ctx() -> SessionContext {
    ctx_with(SessionConfig::new(), vec![vec![batch()]])
}

/// `t` split row by row over four partitions, so NaN payloads and both
/// zeros meet only in the final merge.
fn split_ctx() -> SessionContext {
    let b = batch();
    let parts = (0..4)
        .map(|p| {
            let rows = (p..b.num_rows()).step_by(4);
            rows.map(|r| b.slice(r, 1)).collect()
        })
        .collect();
    ctx_with(SessionConfig::new().with_target_partitions(4), parts)
}

async fn counts(sql: &str) -> Vec<u64> {
    counts_in(&ctx(), sql).await
}

async fn counts_in(ctx: &SessionContext, sql: &str) -> Vec<u64> {
    let batches = ctx.sql(sql).await.expect("plan").collect().await;
    let batches = batches.expect("execute");
    let mut out = Vec::new();
    for b in &batches {
        let c = b.column(b.num_columns() - 1);
        let c = c.as_any().downcast_ref::<UInt64Array>().expect("UInt64");
        out.extend(c.values().iter().copied());
    }
    out
}

#[tokio::test]
async fn approx_distinct_double_counts_nan_and_signed_zero_once() {
    assert_eq!(counts("SELECT approx_distinct(d) FROM t").await, vec![3]);
}

#[tokio::test]
async fn approx_distinct_real_counts_nan_and_signed_zero_once() {
    assert_eq!(counts("SELECT approx_distinct(r) FROM t").await, vec![3]);
}

#[tokio::test]
async fn approx_distinct_float_per_group() {
    for col in ["d", "r"] {
        let sql = format!("SELECT k, approx_distinct({col}) FROM t GROUP BY k ORDER BY k");
        assert_eq!(counts(&sql).await, vec![3, 3, 1], "{col}");
    }
}

#[tokio::test]
async fn approx_distinct_float_matches_count_distinct_on_ordinary_values() {
    let sql = "SELECT approx_distinct(x) FROM (VALUES (1.25), (2.5), (2.5), (-3.0)) v(x)";
    assert_eq!(counts(sql).await, vec![3]);
}

#[tokio::test]
async fn approx_distinct_other_types_unchanged() {
    let sql = "SELECT approx_distinct(k) FROM t";
    assert_eq!(counts(sql).await, vec![3]);
}

#[tokio::test]
async fn approx_distinct_float_merges_across_partitions() {
    let ctx = split_ctx();
    for col in ["d", "r"] {
        let all = format!("SELECT approx_distinct({col}) FROM t");
        assert_eq!(counts_in(&ctx, &all).await, vec![3], "{col}");
        let sql = format!("SELECT k, approx_distinct({col}) FROM t GROUP BY k ORDER BY k");
        assert_eq!(counts_in(&ctx, &sql).await, vec![3, 3, 1], "{col}");
    }
}

/// Partial aggregation decides to skip after its first batch, so the later
/// batches reach the final merge through `convert_to_state`: `-0.0` is
/// aggregated, `0.0` and a second NaN payload are converted.
#[tokio::test]
async fn approx_distinct_float_skipped_partial_aggregation() {
    let b = batch();
    // Rows 1 (-0.0, a), 4 (0.0, b), 3 (other NaN, b), 0 (NaN, a), 2 (1.5, a).
    let rows = [1, 4, 3, 0, 2];
    let k = StringArray::from(vec!["x"; 1]);
    let part: Vec<RecordBatch> = rows
        .iter()
        .map(|&r| {
            let one = b.slice(r, 1);
            let cols = vec![
                Arc::new(k.clone()) as _,
                one.column(1).clone(),
                one.column(2).clone(),
            ];
            RecordBatch::try_new(b.schema(), cols).expect("row")
        })
        .collect();
    let mut config = SessionConfig::new().with_target_partitions(4);
    let exec = &mut config.options_mut().execution;
    exec.skip_partial_aggregation_probe_rows_threshold = 1;
    exec.skip_partial_aggregation_probe_ratio_threshold = 0.0;
    let ctx = ctx_with(config, vec![part, vec![]]);
    for col in ["d", "r"] {
        let sql = format!("SELECT k, approx_distinct({col}) FROM t GROUP BY k");
        assert_eq!(counts_in(&ctx, &sql).await, vec![3], "{col}");
        let plan = ctx.sql(&format!("EXPLAIN ANALYZE {sql}")).await;
        let plan = plan.expect("plan").collect().await.expect("explain");
        let plan = pretty_format_batches(&plan).expect("format").to_string();
        assert!(plan.contains("skipped_aggregation_rows=4"), "{plan}");
    }
}
