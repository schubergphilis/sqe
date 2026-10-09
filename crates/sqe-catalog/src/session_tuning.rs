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
use datafusion::optimizer::{Optimizer, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::eliminate_cross_join::EliminateCrossJoin;

/// A context for user SQL: `config` and `runtime`, planned by
/// [`EngineQueryPlanner`], with SQE's [`EliminateCrossJoin`] in place of
/// DataFusion's.
pub fn engine_context(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_optimizer_rules(engine_optimizer_rules())
        .with_query_planner(Arc::new(EngineQueryPlanner))
        .build();
    SessionContext::new_with_state(state)
}

/// DataFusion's default logical rules, with `eliminate_cross_join` swapped
/// for SQE's copy that keeps unplaced `ON` keys (apache/datafusion#26058).
fn engine_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = Optimizer::new().rules;
    for rule in rules.iter_mut() {
        if rule.name() == "eliminate_cross_join" {
            *rule = Arc::new(EliminateCrossJoin::new());
        }
    }
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
        let state = match session.as_any().downcast_ref::<SessionState>() {
            Some(state)
                if state.config_options().optimizer.enable_aggregate_dynamic_filter_pushdown
                    && has_recursive_query(plan) =>
            {
                state
            }
            _ => return planner.create_physical_plan(plan, session).await,
        };
        let mut state = state.clone();
        state
            .config_mut()
            .options_mut()
            .optimizer
            .enable_aggregate_dynamic_filter_pushdown = false;
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

/// Engine-wide DataFusion session flags (decimal literals, DuckDB dialect,
/// broadcast threshold, dynamic filter pushdown, Parquet predicate pushdown).
pub fn engine_session_config() -> SessionConfig {
    SessionConfig::new()
        // Parse numeric literals like 0.06 as DECIMAL instead of DOUBLE.
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
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
