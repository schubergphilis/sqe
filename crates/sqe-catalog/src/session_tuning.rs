//! Shared DataFusion session flags used by the coordinator and sqe-cli
//! embedded mode so the two paths cannot drift.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::eliminate_cross_join::EliminateCrossJoin;
use crate::variant_read::VariantReadHints;

mod decimal_arithmetic;
mod decimal_rounding;
mod float_decimal_comparison;
mod sorted_row_number;
mod top_row_number;

pub use decimal_arithmetic::{DecimalArithUdf, DecimalArithmeticRule, TrinoSum, DECIMAL_ARITH};
pub use decimal_rounding::{
    register_decimal_rounding, register_rescale, DecimalRoundingRule, RescaleUdf, RoundedAvg,
    RESCALE,
};
pub use float_decimal_comparison::FloatOverDecimalRule;
pub use sorted_row_number::{SortedRowNumberExec, SortedRowNumberRule};
pub use top_row_number::TopRowNumberRule;

/// A context for user SQL: `config` and `runtime`, planned by
/// [`EngineQueryPlanner`], with SQE's [`EliminateCrossJoin`] in place of
/// DataFusion's and [`SortedRowNumberRule`] after DataFusion's physical rules.
/// Decimal arithmetic and `sum` take Trino's result types
/// ([`DecimalArithmeticRule`], [`TrinoSum`]); decimal division, `avg` and
/// decimal casts round half away from zero ([`DecimalRoundingRule`],
/// [`RoundedAvg`]).
pub fn engine_context(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_analyzer_rules(engine_analyzer_rules())
        .with_optimizer_rules(engine_optimizer_rules())
        .with_physical_optimizer_rule(Arc::new(SortedRowNumberRule))
        .with_query_planner(Arc::new(EngineQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    register_decimal_rounding(&ctx);
    ctx
}

/// DataFusion's analyzer rules with [`DecimalArithmeticRule`] before type
/// coercion, so coercion types every parent against Trino's decimal types,
/// then [`FloatOverDecimalRule`], so a float that meets a decimal
/// stays a float as in Trino, and [`DecimalRoundingRule`] after
/// coercion.
fn engine_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let mut rules = Analyzer::new().rules;
    let at = rules
        .iter()
        .position(|r| r.name() == "type_coercion")
        .unwrap_or(rules.len());
    rules.insert(at, Arc::new(FloatOverDecimalRule));
    rules.insert(at, Arc::new(DecimalArithmeticRule));
    rules.push(Arc::new(DecimalRoundingRule));
    rules
}

/// DataFusion's default logical rules, with `eliminate_cross_join` swapped
/// for SQE's copy that keeps unplaced `ON` keys (apache/datafusion#26058),
/// and [`TopRowNumberRule`] last, after projections are pruned, so the
/// aggregate it builds carries only the columns the query reads.
fn engine_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = Optimizer::new().rules;
    for rule in rules.iter_mut() {
        if rule.name() == "eliminate_cross_join" {
            *rule = Arc::new(EliminateCrossJoin::new());
        }
    }
    rules.push(Arc::new(TopRowNumberRule));
    rules
}

/// DataFusion's planner, except that a plan holding `WITH RECURSIVE` is
/// planned without the aggregate dynamic filter. `RecursiveQueryExec` resets
/// the recursive term between iterations, but `AggregateExec` keeps the
/// MIN/MAX bound the first iteration pushed into its scan, so later
/// iterations scan with a stale bound and return wrong rows
/// (apache/datafusion#26054). Drop this once the upstream fix lands.
#[derive(Debug)]
pub struct EngineQueryPlanner;

#[async_trait]
impl QueryPlanner for EngineQueryPlanner {
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        let Some(state) = session.as_any().downcast_ref::<SessionState>() else {
            return planner.create_physical_plan(plan, session).await;
        };
        let no_aggregate_filters = state
            .config_options()
            .optimizer
            .enable_aggregate_dynamic_filter_pushdown
            && has_recursive_query(plan);
        // The table scans read only the shredded variant fields the plan
        // reads (sqe#870); they find the hints in the session config.
        let variant_hints = VariantReadHints::of(plan);
        if !no_aggregate_filters && variant_hints.is_empty() {
            return planner.create_physical_plan(plan, session).await;
        }
        let mut state = state.clone();
        if no_aggregate_filters {
            state
                .config_mut()
                .options_mut()
                .optimizer
                .enable_aggregate_dynamic_filter_pushdown = false;
        }
        if !variant_hints.is_empty() {
            state.config_mut().set_extension(Arc::new(variant_hints));
        }
        planner.create_physical_plan(plan, &state).await
    }
}

fn has_recursive_query(plan: &LogicalPlan) -> bool {
    let mut found = false;
    // The closure never fails; a traversal error is treated as recursive.
    let walked = plan.apply_with_subqueries(|node| {
        found = matches!(node, LogicalPlan::RecursiveQuery(_));
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found || walked.is_err()
}

/// The session zone of every SQE session. Without one DataFusion types
/// `CAST(x AS TIMESTAMP WITH TIME ZONE)`, `now()` and `to_timestamp*` as naive
/// timestamps, so the zone a Trino query asked for was dropped. `+00:00` is
/// the zone iceberg-rust gives `timestamptz` columns, so these values compare
/// and union with scanned columns without a cast. `SET TIME ZONE` does not
/// change it (SQE has no per-session zone).
pub const ENGINE_TIME_ZONE: &str = "+00:00";

/// Engine-wide DataFusion session flags (decimal literals, DuckDB dialect,
/// session zone, broadcast threshold, dynamic filter pushdown, Parquet
/// predicate pushdown).
pub fn engine_session_config() -> SessionConfig {
    SessionConfig::new()
        // Parse numeric literals like 0.06 as DECIMAL instead of DOUBLE.
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
        .set_str("datafusion.execution.time_zone", ENGINE_TIME_ZONE)
        // DuckDB dialect adds `x -> expr` lambda syntax without changing
        // identifier/quoting behaviour. See #354. The front door refuses text
        // this dialect tokenizes differently from the classifier (#683).
        .set_str("datafusion.sql_parser.dialect", sqe_sql::EXECUTION_DIALECT)
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            64 * 1024 * 1024,
        )
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true)
}

/// The functions every SQE statement plans with on top of DataFusion's:
/// Trino's library (`year`, `if`, `date_parse`, ...), the JSON functions,
/// variant access and `ST_*`. The coordinator's session and every context
/// that plans a view body register this one set, so a view plans with the
/// names of the statement that reads it (ai-portal-data#359). Functions bound
/// to the session's user (`current_user()`) are not in it.
pub fn register_engine_functions(ctx: &mut SessionContext) -> Result<()> {
    sqe_trino_functions::register_trino_functions(ctx);
    sqe_trino_functions::register_extended_trino_functions(ctx);
    datafusion_functions_json::register_all(ctx)?;
    // A JSON column of a v3 table is a variant; the json_* functions read it
    // as its JSON text (#849).
    sqe_core::variant_json_args::accept_variant_in_json_functions(ctx);
    // Typed field access that skips the JSON text (#857).
    sqe_core::variant_get::register_variant_functions(ctx);
    // ST_* over WKB geometry values (#863).
    sqe_geo::register_geo_functions(ctx);
    Ok(())
}

/// The context a view body is planned in: the engine's flags, rules and
/// function set, with `catalog.schema` as the default for unqualified names.
/// The plan is only planned and optimized here; it runs in the reading
/// session, so the runtime is a throwaway.
pub fn view_planning_context(catalog: &str, schema: &str) -> Result<SessionContext> {
    let mut ctx = engine_context(
        engine_session_config()
            .with_information_schema(true)
            .with_default_catalog_and_schema(catalog, schema),
        Arc::new(RuntimeEnv::default()),
    );
    register_engine_functions(&mut ctx)?;
    Ok(ctx)
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

/// apache/datafusion#24421, backported in `vendor/datafusion-physical-plan`:
/// a filter that pins a grouped column (`k2 = 'TN'`) must not make that
/// column constant above a ROLLUP, which emits it as NULL in its subtotal
/// rows. TPC-DS q27 at SF10 returned duplicate `(i_item_id, s_state)` groups
/// and subtotals in the wrong order.
#[cfg(test)]
mod grouping_sets_constant_key {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    const ROLLUP: &str = "SELECT k1, k2, GROUPING(k2) AS g, SUM(v) AS s FROM t \
                          WHERE k2 = 'TN' GROUP BY ROLLUP (k1, k2)";

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new_with_config(
            super::engine_session_config()
                .with_target_partitions(4)
                .with_batch_size(8),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Utf8, true),
            Field::new("k2", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        // Every k1 appears in every partition, in both states, so each
        // group's partial states meet in the final aggregate.
        let partitions = (0..4)
            .map(|p| {
                let k1: Vec<String> = (0..200).map(|i| format!("item{:03}", i % 50)).collect();
                let k2: Vec<&str> = (0..200)
                    .map(|i| if i % 4 == 3 { "TX" } else { "TN" })
                    .collect();
                let v: Vec<i64> = (0..200).map(|i| i + p).collect();
                vec![
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(StringArray::from(k1)),
                            Arc::new(StringArray::from(k2)),
                            Arc::new(Int64Array::from(v)),
                        ],
                    )
                    .unwrap(),
                ]
            })
            .collect();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, partitions).unwrap()),
        )
        .unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn order_by_keeps_the_rolled_up_key() {
        let ctx = ctx();
        let plan = rows(&ctx, &format!("EXPLAIN {ROLLUP} ORDER BY k1, k2")).await;
        assert!(
            plan.contains("SortExec: expr=[k1@0 ASC NULLS LAST, k2@1 ASC NULLS LAST]"),
            "the sort must keep k2\n{plan}"
        );
        assert!(
            !plan.contains("ordering_mode"),
            "no sorted-input aggregate\n{plan}"
        );

        // Detail row before the subtotal for each k1, as Trino returns them.
        let out = rows(
            &ctx,
            &format!("SELECT k1, k2, g FROM ({ROLLUP}) ORDER BY k1, k2 LIMIT 4"),
        )
        .await;
        let expected = "\
+---------+----+---+
| k1      | k2 | g |
+---------+----+---+
| item000 | TN | 0 |
| item000 |    | 1 |
| item001 | TN | 0 |
| item001 |    | 1 |
+---------+----+---+";
        assert_eq!(out, expected, "\n{out}");
    }

    #[tokio::test]
    async fn every_group_comes_out_once() {
        let ctx = ctx();
        let out = rows(
            &ctx,
            &format!(
                "SELECT COUNT(*) AS n, COUNT(DISTINCT (k1, k2, g)) AS d, SUM(s) AS total \
                 FROM ({ROLLUP})"
            ),
        )
        .await;
        // 50 detail rows, 50 subtotals, 1 grand total. Each level sums the
        // same TN rows: 3 * the TN sum over all partitions.
        let tn_sum: i64 = (0..4)
            .flat_map(|p| (0..200).filter(|i| i % 4 != 3).map(move |i| i + p))
            .sum();
        assert!(
            out.contains(&format!("| 101 | 101 | {} |", 3 * tn_sum)),
            "\n{out}"
        );
    }
}

/// apache/datafusion#26054: a MIN/MAX in the recursive term of a recursive
/// CTE must give the same value on every iteration.
#[cfg(test)]
mod recursive_aggregate_dynamic_filter {
    use std::sync::Arc;

    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionContext;

    const QUERY: &str = "WITH RECURSIVE w(n, m) AS ( \
          SELECT 0, CAST(NULL AS DOUBLE) \
          UNION ALL \
          SELECT w.n + 1, x.m \
          FROM w CROSS JOIN (SELECT MAX(r) AS m FROM t WHERE r % 2 = 0) x \
          WHERE w.n < 3) \
        SELECT n, m FROM w WHERE m IS NOT NULL ORDER BY n";

    async fn non_null_iterations(ctx: &SessionContext, dir: &std::path::Path) -> usize {
        let path = dir.join("t.parquet");
        let path = path.to_str().unwrap();
        // A filtered MAX, so it is computed by scanning, not from statistics.
        ctx.sql(&format!(
            "COPY (SELECT CAST(value AS DOUBLE) AS r FROM generate_series(0, 128)) TO '{path}'"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        ctx.sql(&format!("CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '{path}'"))
            .await
            .unwrap();
        let batches = ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn every_iteration_sees_the_maximum() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = super::engine_context(super::engine_session_config(), Arc::new(RuntimeEnv::default()));
        assert_eq!(non_null_iterations(&ctx, dir.path()).await, 3);
    }

    /// Tripwire: when this fails, DataFusion has fixed #26054 and
    /// `EngineQueryPlanner` can go.
    #[tokio::test]
    async fn plain_datafusion_still_loses_later_iterations() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = SessionContext::new_with_config(super::engine_session_config());
        assert_eq!(non_null_iterations(&ctx, dir.path()).await, 1);
    }
}

/// The default null order, which a Databricks migration must not rely on
/// (#876). SQE keeps DataFusion's `nulls_max`, as Trino and PostgreSQL do:
/// nulls sort as the largest value, so they come last in `ASC` and first in
/// `DESC`. Spark and Databricks put them last in `DESC`; migrated SQL keeps
/// that order by writing `NULLS LAST`, which these tests also pin, together
/// with the window `ORDER BY` that `ROW_NUMBER` / `QUALIFY` top-N depend on.
#[cfg(test)]
mod null_ordering_default {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new_with_config(super::engine_session_config());
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    /// Column 0 of the result, top to bottom, with NULL as `None`.
    async fn column(sql: &str) -> Vec<Option<i64>> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            out.extend(col.iter());
        }
        out
    }

    #[tokio::test]
    async fn null_ordering_default_is_nulls_largest() {
        let asc = column("SELECT a FROM t ORDER BY a").await;
        assert_eq!(asc, vec![Some(1), Some(2), None]);
        let desc = column("SELECT a FROM t ORDER BY a DESC").await;
        assert_eq!(desc, vec![None, Some(2), Some(1)], "DESC puts nulls first");
    }

    #[tokio::test]
    async fn null_ordering_explicit_nulls_last_gives_spark_desc_order() {
        let desc = column("SELECT a FROM t ORDER BY a DESC NULLS LAST").await;
        assert_eq!(desc, vec![Some(2), Some(1), None]);
        let asc = column("SELECT a FROM t ORDER BY a ASC NULLS FIRST").await;
        assert_eq!(asc, vec![None, Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn null_ordering_window_top_n_follows_the_same_rule() {
        let top = "SELECT a FROM (SELECT a, ROW_NUMBER() OVER (ORDER BY a DESC) AS rn FROM t) \
                   WHERE rn = 1";
        // Bare DESC: the NULL row ranks first.
        assert_eq!(column(top).await, vec![None]);
        let spark = "SELECT a FROM (SELECT a, ROW_NUMBER() OVER (ORDER BY a DESC NULLS LAST) \
                     AS rn FROM t) WHERE rn = 1";
        assert_eq!(column(spark).await, vec![Some(2)]);
    }
}
