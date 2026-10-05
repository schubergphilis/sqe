//! Shared DataFusion session flags used by the coordinator and sqe-cli
//! embedded mode so the two paths cannot drift.

use datafusion::prelude::SessionConfig;

/// Engine-wide DataFusion session flags (decimal literals, DuckDB dialect,
/// broadcast threshold, dynamic filter pushdown, Parquet predicate pushdown).
pub fn engine_session_config() -> SessionConfig {
    SessionConfig::new()
        // Parse numeric literals like 0.06 as DECIMAL instead of DOUBLE.
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
        // DuckDB dialect adds `x -> expr` lambda syntax without changing
        // identifier/quoting behaviour. See #354.
        .set_str("datafusion.sql_parser.dialect", "DuckDB")
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            64 * 1024 * 1024,
        )
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true)
}

/// SQE sends ROLLUP / CUBE / GROUPING SETS to DataFusion as written; these
/// pin the empty-input semantics that SQE used to patch with a rewrite
/// (apache/datafusion#21570). Run on the engine config with several input
/// partitions so the partial and final aggregate stages both see empty
/// input: the grand-total row must come out once, not once per partition.
#[cfg(test)]
mod grouping_sets_empty_input {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    async fn ctx() -> SessionContext {
        let ctx = SessionContext::new_with_config(super::engine_session_config().with_target_partitions(4));
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        // Four partitions, so the aggregate runs partial + repartition + final.
        let partitions = (0..4)
            .map(|p| {
                vec![RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![p, p + 10])),
                        Arc::new(Int64Array::from(vec![1, 2])),
                    ],
                )
                .unwrap()]
            })
            .collect();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, partitions).unwrap()))
            .unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn empty_input_rollup_emits_one_grand_total_row() {
        let ctx = ctx().await;
        let out = rows(
            &ctx,
            "SELECT a, SUM(b) AS s, COUNT(*) AS n, GROUPING(a) AS g \
             FROM t WHERE b > 100 GROUP BY ROLLUP(a)",
        )
        .await;
        let expected = "\
+---+---+---+---+
| a | s | n | g |
+---+---+---+---+
|   |   | 0 | 1 |
+---+---+---+---+";
        assert_eq!(out, expected, "\n{out}");
    }

    #[tokio::test]
    async fn empty_input_cube_and_grouping_sets_emit_their_empty_sets_only() {
        let ctx = ctx().await;
        let cube = rows(&ctx, "SELECT COUNT(*) AS n FROM t WHERE b > 100 GROUP BY CUBE(a, b)").await;
        assert_eq!(cube.matches("| 0 |").count(), 1, "CUBE: one grand total\n{cube}");
        // No empty set: no row at all, as in Trino.
        let no_total = rows(
            &ctx,
            "SELECT COUNT(*) AS n FROM t WHERE b > 100 GROUP BY GROUPING SETS ((a), (b))",
        )
        .await;
        assert!(!no_total.contains("| 0 |"), "no empty set, no row\n{no_total}");
    }

    /// TPC-DS q67 shape: ROLLUP in a subquery under RANK() and a filter.
    #[tokio::test]
    async fn nested_rollup_under_a_window_keeps_rows_and_grand_total() {
        let ctx = ctx().await;
        let sql = "SELECT a, s, rk FROM ( \
                     SELECT a, s, RANK() OVER (ORDER BY s DESC) AS rk FROM ( \
                       SELECT a, SUM(b) AS s FROM t GROUP BY ROLLUP(a)) dw1) dw2 \
                   WHERE rk <= 100 ORDER BY a NULLS LAST, s";
        let out = rows(&ctx, sql).await;
        // Eight groups plus the grand total; the total (a NULL, s 12) ranks first.
        assert_eq!(out.lines().filter(|l| l.starts_with("| ")).count() - 1, 9, "\n{out}");
        assert!(out.contains("|    | 12 | 1  |"), "grand total row\n{out}");
    }
}
