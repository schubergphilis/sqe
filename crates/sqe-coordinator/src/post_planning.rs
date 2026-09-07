//! The single post-planning physical rule pipeline (review 404, issue #584).
//!
//! After `create_physical_plan()` the coordinator applies its own plan-shape
//! rules on top of DataFusion's physical optimizer output. Before this module
//! existed those rules were hand-rolled in three places (`execute_query`,
//! `open_stream`, `ExplainHandler`) that had drifted apart: the batch path
//! got no parallel-scan rule, EXPLAIN skipped JoinStrategy / SortMemory, and
//! most rules ran with `ConfigOptions::new()` instead of the session config.
//!
//! [`post_planning_rules`] is now the ONLY place that names the rule sequence.
//! [`apply_post_planning_rules`] runs it with the real session
//! `config_options()`. The one step that is not a `PhysicalOptimizerRule`,
//! `QueryHandler::try_distribute`, runs after this pipeline from
//! `QueryHandler::finish_physical_plan` (it is async: worker health, manifest
//! listing, ticket signing).
//!
//! # Order
//!
//! The order is the one the streaming path (`open_stream`) ran on 2026-09-06,
//! which is the perf-tuned path every benchmark has measured. Do not reorder
//! without re-running the SF1/SF10 sweeps (`benchmarks` skill).
//!
//! 1. `StarSchemaReorderRule` (gated by `query.star_schema_reorder`)
//! 2. `DimBuildSwapRule` (gated by `query.dim_build_swap`) -- before the
//!    parallel rules so build/probe roles are final.
//! 3. `ParallelProbeScanRule` (gated by `query.parallel_probe_scan`) -- before
//!    `ParallelScanRule`; takes the scans it can (issue #414).
//! 4. `ParallelScanRule` (gated by `query.parallel_scan`) -- covers what the
//!    probe rule does not reach.
//! 5. `JoinStrategyRule` -- spillable join fallback (unknown/large build ->
//!    SortMergeJoin).
//! 6. `AdaptiveSortRule` -- strips non-partition sorts under memory pressure
//!    per `query.sort_mode`.
//! 7. `SortMemoryRule` -- FAIL CLOSED when remaining `SortExec`s cannot
//!    reserve merge headroom (ExternalSorterMerge is non-spillable).
//! 8. `StripSelfJoinDynamicFiltersRule` -- q95-class inlist blowup; runs
//!    before distribution so the worker predicate and the coordinator join
//!    agree on which dynamic filters exist.
//! 9. (outside this module) `QueryHandler::try_distribute`.
//!
//! # Failure policy
//!
//! A rule that returns `Err` is either `FallBack` (keep the previous plan,
//! `warn!`, and bump `sqe_plan_rule_failures_total{rule}`) or `Fail`
//! (bump the metric and abort the query). `debug!` is never used here: the
//! `JoinStrategyRule` SMJ regression went unnoticed for five queries because
//! the `Err` arms were silent.
//!
//! # Why not `SessionStateBuilder::with_physical_optimizer_rule`
//!
//! The per-user `SessionContext` is shared with the write paths
//! (`write_handler` plans CTAS / INSERT ... SELECT / MERGE sources through
//! `df.execute_stream()` on the same state). Registering these rules on the
//! state would silently apply adaptive sort stripping and fail-closed sort
//! admission to sort-on-write CTAS, and SMJ conversion to every write source.
//! Keeping the list here makes that flip a one-line change later
//! (`for step in post_planning_rules(..) { builder.with_physical_optimizer_rule(step.rule) }`)
//! once the write path has been measured under it.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::JoinType;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use tracing::{debug, warn};

use sqe_core::{QueryConfig, SortMode, SqeError, SqeErrorCode};

use crate::adaptive_sort;
use crate::memory::MemoryPressure;

/// What to do when a rule returns `Err`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnError {
    /// Keep the plan from the previous step; `warn!` and count the failure.
    FallBack,
    /// Count the failure and abort the query with the given message prefix.
    Fail(&'static str),
}

/// One step of the pipeline: a rule plus its failure policy.
pub struct Step {
    pub rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    pub on_error: OnError,
}

impl Step {
    fn fallback(rule: impl PhysicalOptimizerRule + Send + Sync + 'static) -> Self {
        Self {
            rule: Arc::new(rule),
            on_error: OnError::FallBack,
        }
    }

    fn fail(
        rule: impl PhysicalOptimizerRule + Send + Sync + 'static,
        message: &'static str,
    ) -> Self {
        Self {
            rule: Arc::new(rule),
            on_error: OnError::Fail(message),
        }
    }
}

/// Merge headroom granted to every remaining `SortExec` (see `SortMemoryRule`).
fn sort_grant_bytes(query: &QueryConfig) -> usize {
    sqe_core::parse_memory_limit(&query.max_query_memory)
        .unwrap_or(256 * 1024 * 1024)
        .max(8 * 1024 * 1024)
}

/// Build the ordered rule list. This is the only place that names the
/// sequence; see the module docs for the rationale behind each position.
pub fn post_planning_rules(
    query: &QueryConfig,
    pressure: MemoryPressure,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
) -> Vec<Step> {
    let mut steps = Vec::with_capacity(8);
    if query.star_schema_reorder {
        steps.push(Step::fallback(sqe_planner::StarSchemaReorderRule::new(
            query.star_schema_min_ratio,
        )));
    }
    if query.dim_build_swap {
        steps.push(Step::fallback(
            crate::dim_build_swap::DimBuildSwapRule::new(),
        ));
    }
    if query.parallel_probe_scan {
        steps.push(Step::fallback(
            crate::parallel_probe_scan::ParallelProbeScanRule::new(),
        ));
    }
    if query.parallel_scan {
        let byte_threshold =
            sqe_core::parse_memory_limit(&query.distribution_threshold).unwrap_or(0);
        steps.push(Step::fallback(crate::parallel_scan::ParallelScanRule::new(
            byte_threshold,
        )));
    }
    steps.push(Step::fallback(sqe_planner::JoinStrategyRule::new(
        sqe_planner::DEFAULT_HASH_JOIN_THRESHOLD,
    )));
    steps.push(Step::fallback(AdaptiveSortRule {
        sort_mode: query.sort_mode,
        pressure,
        metrics: metrics.cloned(),
    }));
    steps.push(Step::fail(
        sqe_planner::SortMemoryRule::new(sort_grant_bytes(query)),
        "Sort memory admission failed",
    ));
    steps.push(Step::fallback(StripSelfJoinDynamicFiltersRule));
    steps
}

/// Run the post-planning pipeline on `plan` with the session's real
/// `config_options()`. Memory pressure is sampled once from the session's
/// runtime pool (the coordinator's shared `RuntimeEnv` in production).
pub fn apply_post_planning_rules(
    ctx: &SessionContext,
    query: &QueryConfig,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    plan: Arc<dyn ExecutionPlan>,
) -> sqe_core::Result<Arc<dyn ExecutionPlan>> {
    let pressure = crate::memory::check_pressure(&ctx.runtime_env().memory_pool);
    let steps = post_planning_rules(query, pressure, metrics);
    let state = ctx.state();
    apply_steps(&steps, state.config_options(), metrics, plan)
}

/// Fold `steps` over `plan`, applying the failure policy of each step.
pub fn apply_steps(
    steps: &[Step],
    config: &ConfigOptions,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    mut plan: Arc<dyn ExecutionPlan>,
) -> sqe_core::Result<Arc<dyn ExecutionPlan>> {
    for step in steps {
        let name = step.rule.name();
        match step.rule.optimize(Arc::clone(&plan), config) {
            Ok(optimized) => plan = optimized,
            Err(e) => {
                if let Some(m) = metrics {
                    m.plan_rule_failures_total.with_label_values(&[name]).inc();
                }
                match step.on_error {
                    OnError::FallBack => {
                        warn!(rule = name, error = %e, "post-planning rule failed; using previous plan");
                    }
                    OnError::Fail(prefix) => {
                        warn!(rule = name, error = %e, "post-planning rule failed; aborting query");
                        return Err(SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!("{prefix}: {e}"),
                        ));
                    }
                }
            }
        }
    }
    Ok(plan)
}

/// `adaptive_sort::apply_adaptive_sort` as a `PhysicalOptimizerRule`, so it
/// sits in the same ordered list as the other steps. Pressure is sampled by
/// the caller once per query (not per rule) so every step sees one reading.
struct AdaptiveSortRule {
    sort_mode: SortMode,
    pressure: MemoryPressure,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
}

impl std::fmt::Debug for AdaptiveSortRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdaptiveSortRule")
            .field("sort_mode", &self.sort_mode)
            .field("pressure", &self.pressure)
            .finish_non_exhaustive()
    }
}

impl PhysicalOptimizerRule for AdaptiveSortRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let (plan, decisions) = adaptive_sort::apply_adaptive_sort(
            plan,
            self.sort_mode,
            self.pressure,
            self.metrics.as_ref(),
        );
        if let Some(warning) = adaptive_sort::format_sort_warning(&decisions, self.sort_mode) {
            debug!(warning = %warning, "Adaptive sort stripping applied");
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "AdaptiveSortRule"
    }

    fn schema_check(&self) -> bool {
        // Stripping a SortExec keeps the schema; only ordering changes.
        true
    }
}

/// Strip the runtime dynamic filter from any `Inner` `HashJoinExec` whose two
/// inputs both scan the **same** Iceberg table -- a self-join (e.g. TPC-DS
/// q95's `ws_wh` CTE).
///
/// DataFusion only attaches a dynamic (runtime) filter to `Inner` joins, and
/// below `hash_join_inlist_pushdown_max_distinct_values` it materializes that
/// filter as an IN-list of the build side's distinct keys. For a self-join the
/// build side carries tens of thousands of fact-table keys and the IN-list path
/// collapses the join into a materialized cross product (q95: 0.2s -> 17s at the
/// 65536 threshold SQE uses for SSB dimension pushdown). We rebuild only the
/// offending self-join node without its dynamic filter, via
/// [`HashJoinExec::try_new`] (which starts with `dynamic_filter: None`). Every
/// other join -- including the dimension/fact joins inside the same query that
/// legitimately prune via the key-set push-down -- is left untouched.
///
/// Detection keys on table *identity*, not cardinality, so it is scale-invariant
/// (SF1/SF10/SF100) and never fires on dimension/fact joins (different tables).
/// Removing a runtime filter only changes pruning, never results.
///
/// Runs before `try_distribute` (review 404 item 3) so the ScanTask predicate
/// shipped to workers and the coordinator-side join agree on which dynamic
/// filters exist. A self-join has two `IcebergScanExec`s and is never
/// distributed anyway.
#[derive(Debug)]
struct StripSelfJoinDynamicFiltersRule;

impl PhysicalOptimizerRule for StripSelfJoinDynamicFiltersRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(hj) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            // DataFusion 55: dynamic_filter_expr is deprecated. This is only a
            // presence check, and dynamic_expressions_produced() is built from
            // the same Option, so is_empty() is exactly is_none().
            if *hj.join_type() != JoinType::Inner || hj.dynamic_expressions_produced().is_empty() {
                return Ok(Transformed::no(node));
            }
            let mut left = std::collections::HashSet::new();
            let mut right = std::collections::HashSet::new();
            crate::query_handler::collect_raw_iceberg_table_idents(hj.left(), &mut left);
            crate::query_handler::collect_raw_iceberg_table_idents(hj.right(), &mut right);
            if left.intersection(&right).next().is_none() {
                return Ok(Transformed::no(node));
            }
            let rebuilt = HashJoinExec::try_new(
                Arc::clone(hj.left()),
                Arc::clone(hj.right()),
                hj.on().to_vec(),
                hj.filter().cloned(),
                hj.join_type(),
                hj.projection.as_ref().map(|p| p.to_vec()),
                *hj.partition_mode(),
                hj.null_equality(),
                hj.null_aware,
            )?;
            debug!(
                on = ?hj.on(),
                "Stripped dynamic filter from Iceberg self-join HashJoinExec (q95-class)"
            );
            Ok(Transformed::yes(Arc::new(rebuilt) as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "StripSelfJoinDynamicFiltersRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;
    use std::sync::Mutex;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;

    /// A rule that always fails, to exercise the failure policy.
    #[derive(Debug)]
    struct FailingRule;

    impl PhysicalOptimizerRule for FailingRule {
        fn optimize(
            &self,
            _plan: Arc<dyn ExecutionPlan>,
            _config: &ConfigOptions,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Err(datafusion::common::DataFusionError::Plan(
                "synthetic rule failure".to_string(),
            ))
        }
        fn name(&self) -> &str {
            "FailingRule"
        }
        fn schema_check(&self) -> bool {
            true
        }
    }

    /// Shared buffer that a `tracing_subscriber` fmt layer writes into.
    #[derive(Clone, Default)]
    struct LogSink(Arc<Mutex<Vec<u8>>>);

    impl Write for LogSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
        type Writer = LogSink;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    fn ctx_with_tables() -> SessionContext {
        let ctx = SessionContext::new();
        let s = schema();
        let b = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "fact",
            Arc::new(MemTable::try_new(s.clone(), vec![vec![b.clone()], vec![b.clone()]]).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "dim",
            Arc::new(MemTable::try_new(s, vec![vec![b]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    async fn raw_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[test]
    fn rule_order_is_the_documented_streaming_order() {
        let query = QueryConfig::default();
        let names: Vec<String> = post_planning_rules(&query, MemoryPressure::Green, None)
            .iter()
            .map(|s| s.rule.name().to_string())
            .collect();
        // Config gates are on by default; if a default flips, this list is
        // the one place to update alongside the module docs.
        let mut expected: Vec<&str> = Vec::new();
        if query.star_schema_reorder {
            expected.push("StarSchemaReorderRule");
        }
        if query.dim_build_swap {
            expected.push("DimBuildSwapRule");
        }
        if query.parallel_probe_scan {
            expected.push("ParallelProbeScanRule");
        }
        if query.parallel_scan {
            expected.push("ParallelScanRule");
        }
        expected.extend([
            "JoinStrategyRule",
            "AdaptiveSortRule",
            "SortMemoryRule",
            "StripSelfJoinDynamicFiltersRule",
        ]);
        assert_eq!(names, expected, "post-planning rule order drifted");
        let sort_memory = post_planning_rules(&query, MemoryPressure::Green, None)
            .into_iter()
            .find(|s| s.rule.name() == "SortMemoryRule")
            .unwrap();
        assert!(
            matches!(sort_memory.on_error, OnError::Fail(_)),
            "SortMemoryRule must fail closed"
        );
    }

    #[test]
    fn rule_names_are_distinct_metric_labels() {
        let names: Vec<String> =
            post_planning_rules(&QueryConfig::default(), MemoryPressure::Red, None)
                .iter()
                .map(|s| s.rule.name().to_string())
                .collect();
        let mut dedup = names.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(names.len(), dedup.len(), "duplicate rule name: {names:?}");
    }

    #[tokio::test]
    async fn same_sql_yields_identical_plan_on_two_sessions() {
        // Both `execute_query` and `open_stream` now go through
        // `apply_post_planning_rules`; this pins that the pipeline itself is
        // deterministic for one SQL text across independently built
        // sessions, so the two entry points cannot diverge in plan shape.
        let sql = "SELECT fact.val FROM fact JOIN dim ON fact.id = dim.id ORDER BY fact.val";
        let query = QueryConfig::default();

        let ctx_a = ctx_with_tables();
        let plan_a =
            apply_post_planning_rules(&ctx_a, &query, None, raw_plan(&ctx_a, sql).await).unwrap();
        let ctx_b = ctx_with_tables();
        let plan_b =
            apply_post_planning_rules(&ctx_b, &query, None, raw_plan(&ctx_b, sql).await).unwrap();

        let a = format!("{}", displayable(plan_a.as_ref()).indent(true));
        let b = format!("{}", displayable(plan_b.as_ref()).indent(true));
        assert_eq!(a, b, "post-planning pipeline diverged between two sessions");
        assert!(
            a.contains("HashJoinExec") || a.contains("SortMergeJoin"),
            "{a}"
        );
    }

    #[tokio::test]
    async fn failing_fallback_rule_increments_metric_and_warns() {
        let ctx = ctx_with_tables();
        let plan = raw_plan(&ctx, "SELECT val FROM dim").await;
        let before = format!("{}", displayable(plan.as_ref()).indent(true));
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());

        let sink = LogSink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .finish();
        let steps = vec![Step::fallback(FailingRule)];
        let state = ctx.state();
        let out = tracing::subscriber::with_default(subscriber, || {
            apply_steps(
                &steps,
                state.config_options(),
                Some(&metrics),
                Arc::clone(&plan),
            )
        })
        .unwrap();

        // Fallback keeps the previous plan.
        assert_eq!(
            format!("{}", displayable(out.as_ref()).indent(true)),
            before
        );
        assert_eq!(
            metrics
                .plan_rule_failures_total
                .with_label_values(&["FailingRule"])
                .get(),
            1
        );
        let log = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
        assert!(
            log.contains("WARN"),
            "rule failure must be visible at warn: {log}"
        );
        assert!(log.contains("FailingRule"), "{log}");
        assert!(log.contains("synthetic rule failure"), "{log}");
    }

    #[tokio::test]
    async fn failing_fail_closed_rule_aborts_and_counts() {
        let ctx = ctx_with_tables();
        let plan = raw_plan(&ctx, "SELECT val FROM dim").await;
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        let steps = vec![Step::fail(FailingRule, "Sort memory admission failed")];
        let state = ctx.state();
        let err = apply_steps(&steps, state.config_options(), Some(&metrics), plan).unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::ExecutionFailed);
        assert!(
            err.to_string().contains("Sort memory admission failed"),
            "{err}"
        );
        assert_eq!(
            metrics
                .plan_rule_failures_total
                .with_label_values(&["FailingRule"])
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn healthy_pipeline_records_no_failures() {
        let ctx = ctx_with_tables();
        let plan = raw_plan(
            &ctx,
            "SELECT fact.val FROM fact JOIN dim ON fact.id = dim.id ORDER BY fact.val",
        )
        .await;
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        apply_post_planning_rules(&ctx, &QueryConfig::default(), Some(&metrics), plan).unwrap();
        for step in post_planning_rules(&QueryConfig::default(), MemoryPressure::Green, None) {
            assert_eq!(
                metrics
                    .plan_rule_failures_total
                    .with_label_values(&[step.rule.name()])
                    .get(),
                0,
                "{} failed on a plain MemTable join",
                step.rule.name()
            );
        }
    }
}
