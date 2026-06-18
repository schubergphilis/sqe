//! Two-phase distributed aggregation planning.
//!
//! For GROUP BY queries in a distributed setting, splitting aggregation into
//! two phases avoids shipping raw data to a single coordinator:
//!
//! **Phase 1 — Partial aggregation** (on each executor):
//! Each executor computes partial aggregates over its local data partition.
//! For example, `SUM(val)` produces `partial_sum` and `COUNT(*)` produces
//! `partial_count` per group key.
//!
//! **Phase 2 — Final aggregation** (on coordinator or via hash-partition shuffle):
//! Partial results are merged: `SUM(partial_sum)` and `SUM(partial_count)`.
//! For low-cardinality GROUP BY, a single coordinator can handle the merge.
//! For high-cardinality GROUP BY, a hash-partition shuffle distributes groups
//! across executors, each performing full aggregation on its hash partition.
//!
//! This module provides:
//! - [`PartialAggregateExec`] — wraps DataFusion's `AggregateExec` in `Partial` mode
//! - [`FinalAggregateExec`] — wraps DataFusion's `AggregateExec` in `Final` mode
//! - [`DistributedAggregateRule`] — `PhysicalOptimizerRule` that rewrites single-phase
//!   aggregation into two-phase when distributed mode is active
//! - [`AggregateStrategy`] — enum selecting between coordinator merge and shuffle merge

use std::fmt;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use tracing::{debug, trace};

// ────────────────────── Strategy selection ───────────────────────────────────

/// Strategy for merging partial aggregation results.
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateStrategy {
    /// Low-cardinality: collect all partial results on coordinator for final merge.
    /// Good when the number of distinct group keys is small relative to data volume.
    CoordinatorMerge,

    /// High-cardinality: hash-partition partial results on GROUP BY keys via
    /// DoExchange shuffle, then each executor does full aggregation on its
    /// hash partition. Avoids coordinator bottleneck.
    ShuffleMerge {
        /// Number of hash partitions (typically = number of executors).
        num_partitions: usize,
    },
}

impl fmt::Display for AggregateStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateStrategy::CoordinatorMerge => write!(f, "CoordinatorMerge"),
            AggregateStrategy::ShuffleMerge { num_partitions } => {
                write!(f, "ShuffleMerge(partitions={num_partitions})")
            }
        }
    }
}

/// Default threshold for estimated distinct group count above which
/// shuffle merge is preferred over coordinator merge.
///
/// When estimated distinct groups exceed this value, hash-partition
/// shuffle distributes the merge across executors.
pub const DEFAULT_HIGH_CARDINALITY_THRESHOLD: usize = 100_000;

/// Select the aggregation strategy based on estimated group cardinality.
///
/// # Arguments
/// - `estimated_groups`: Estimated number of distinct group keys (from Iceberg column stats).
/// - `num_executors`: Number of available executors.
/// - `high_cardinality_threshold`: Threshold above which shuffle merge is preferred.
///
/// # Returns
/// The recommended [`AggregateStrategy`].
pub fn select_aggregate_strategy(
    estimated_groups: Option<usize>,
    num_executors: usize,
    high_cardinality_threshold: usize,
) -> AggregateStrategy {
    match estimated_groups {
        Some(groups) if groups > high_cardinality_threshold && num_executors >= 2 => {
            AggregateStrategy::ShuffleMerge {
                num_partitions: num_executors,
            }
        }
        _ => AggregateStrategy::CoordinatorMerge,
    }
}

// ────────────────────── PartialAggregateExec ────────────────────────────────

/// Wrapper around DataFusion's `AggregateExec` in `AggregateMode::Partial` mode.
///
/// This plan node runs on each executor. It computes partial aggregates over
/// the local data partition and emits intermediate results (partial sums,
/// partial counts, etc.) that are later merged by [`FinalAggregateExec`].
///
/// The actual aggregation logic is delegated entirely to DataFusion's
/// `AggregateExec` — this wrapper exists to:
/// 1. Mark the plan node explicitly for the stage planner
/// 2. Carry the [`AggregateStrategy`] for downstream merge planning
/// 3. Provide a distinct `name()` for display/debugging
#[derive(Debug)]
pub struct PartialAggregateExec {
    /// The underlying DataFusion AggregateExec in Partial mode.
    inner: Arc<AggregateExec>,
    /// Strategy for the merge phase.
    strategy: AggregateStrategy,
    /// Cached plan properties.
    properties: Arc<PlanProperties>,
}

impl PartialAggregateExec {
    /// Create a new `PartialAggregateExec` from an existing `AggregateExec`.
    ///
    /// The caller must ensure the `AggregateExec` is in `AggregateMode::Partial`.
    pub fn new(inner: Arc<AggregateExec>, strategy: AggregateStrategy) -> Self {
        let schema = inner.schema();
        let input_partitions = inner.properties().partitioning.partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            inner,
            strategy,
            properties,
        }
    }

    /// Returns the aggregation merge strategy.
    pub fn strategy(&self) -> &AggregateStrategy {
        &self.strategy
    }

    /// Returns the underlying `AggregateExec`.
    pub fn inner(&self) -> &Arc<AggregateExec> {
        &self.inner
    }
}

impl DisplayAs for PartialAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PartialAggregateExec: strategy={}, groups=[{}], aggr=[{}]",
            self.strategy,
            self.inner
                .group_expr()
                .expr()
                .iter()
                .map(|(e, _)| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", "),
            self.inner
                .aggr_expr()
                .iter()
                .map(|e| e.name().to_string())
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

impl ExecutionPlan for PartialAggregateExec {
    fn name(&self) -> &str {
        "PartialAggregateExec"
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let new_inner = Arc::clone(&self.inner)
            .with_new_children(children)?
            .downcast_ref::<AggregateExec>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "with_new_children did not return AggregateExec".to_string(),
                )
            })?
            .clone();
        Ok(Arc::new(PartialAggregateExec::new(
            Arc::new(new_inner),
            self.strategy.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        debug!(
            partition = partition,
            strategy = %self.strategy,
            "Executing PartialAggregateExec"
        );
        // Delegate to the underlying AggregateExec
        self.inner.execute(partition, context)
    }
}

// ────────────────────── FinalAggregateExec ──────────────────────────────────

/// Wrapper around DataFusion's `AggregateExec` in `AggregateMode::Final` mode.
///
/// This plan node merges partial aggregation results from [`PartialAggregateExec`]
/// instances running on executors. Depending on the [`AggregateStrategy`]:
///
/// - **CoordinatorMerge**: Runs on the coordinator, receiving all partial results.
/// - **ShuffleMerge**: Runs on each executor after hash-partition shuffle of
///   partial results on the GROUP BY keys.
#[derive(Debug)]
pub struct FinalAggregateExec {
    /// The underlying DataFusion AggregateExec in Final mode.
    inner: Arc<AggregateExec>,
    /// Strategy that was used (for display/debugging).
    strategy: AggregateStrategy,
    /// Cached plan properties.
    properties: Arc<PlanProperties>,
}

impl FinalAggregateExec {
    /// Create a new `FinalAggregateExec` from an existing `AggregateExec`.
    ///
    /// The caller must ensure the `AggregateExec` is in `AggregateMode::Final`
    /// or `AggregateMode::FinalPartitioned`.
    pub fn new(inner: Arc<AggregateExec>, strategy: AggregateStrategy) -> Self {
        let schema = inner.schema();
        let input_partitions = inner.properties().partitioning.partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            inner,
            strategy,
            properties,
        }
    }

    /// Returns the aggregation merge strategy.
    pub fn strategy(&self) -> &AggregateStrategy {
        &self.strategy
    }

    /// Returns the underlying `AggregateExec`.
    pub fn inner(&self) -> &Arc<AggregateExec> {
        &self.inner
    }
}

impl DisplayAs for FinalAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FinalAggregateExec: strategy={}, groups=[{}], aggr=[{}]",
            self.strategy,
            self.inner
                .group_expr()
                .expr()
                .iter()
                .map(|(e, _)| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", "),
            self.inner
                .aggr_expr()
                .iter()
                .map(|e| e.name().to_string())
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

impl ExecutionPlan for FinalAggregateExec {
    fn name(&self) -> &str {
        "FinalAggregateExec"
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let new_inner = Arc::clone(&self.inner)
            .with_new_children(children)?
            .downcast_ref::<AggregateExec>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "with_new_children did not return AggregateExec".to_string(),
                )
            })?
            .clone();
        Ok(Arc::new(FinalAggregateExec::new(
            Arc::new(new_inner),
            self.strategy.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        debug!(
            partition = partition,
            strategy = %self.strategy,
            "Executing FinalAggregateExec"
        );
        // Delegate to the underlying AggregateExec
        self.inner.execute(partition, context)
    }
}

// ────────────────────── DistributedAggregateRule ─────────────────────────────

/// Default input data size threshold (bytes) above which distributed
/// aggregation is used instead of single-node aggregation.
pub const DEFAULT_DISTRIBUTED_AGGREGATE_THRESHOLD: usize = 128 * 1024 * 1024; // 128 MB

/// Minimum number of executors required for distributed aggregation.
pub const MIN_EXECUTORS_FOR_DISTRIBUTED_AGGREGATE: usize = 2;

/// Physical optimizer rule that rewrites a single `AggregateExec` into a
/// two-phase partial + final aggregation when distributed mode is active.
///
/// The rule detects `AggregateExec` nodes in `Single` or `Final` mode and,
/// when the input data exceeds the size threshold and enough executors are
/// available, rewrites them into:
///
/// 1. `PartialAggregateExec` (runs on each executor)
/// 2. `FinalAggregateExec` (runs on coordinator or via shuffle)
///
/// The actual creation of the partial/final `AggregateExec` instances uses
/// DataFusion's built-in `AggregateMode::Partial` and `AggregateMode::Final`.
#[derive(Debug)]
pub struct DistributedAggregateRule {
    /// Minimum data size (bytes) to trigger distributed aggregation.
    size_threshold: usize,
    /// Available executor endpoints.
    executors: Vec<String>,
    /// Threshold for high-cardinality GROUP BY detection.
    high_cardinality_threshold: usize,
}

impl DistributedAggregateRule {
    /// Create a new rule with the given configuration.
    pub fn new(
        size_threshold: usize,
        executors: Vec<String>,
        high_cardinality_threshold: usize,
    ) -> Self {
        Self {
            size_threshold,
            executors,
            high_cardinality_threshold,
        }
    }
}

impl PhysicalOptimizerRule for DistributedAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // No executors → single-node mode, keep local aggregation
        if self.executors.len() < MIN_EXECUTORS_FOR_DISTRIBUTED_AGGREGATE {
            return Ok(plan);
        }

        let num_executors = self.executors.len();
        let threshold = self.size_threshold;
        let hc_threshold = self.high_cardinality_threshold;

        let transformed = plan.transform_down(|node| {
            if let Some(agg_exec) = node.downcast_ref::<AggregateExec>() {
                // Only rewrite single-phase aggregations (not already split)
                let mode = agg_exec.mode();
                if *mode != AggregateMode::Single {
                    return Ok(Transformed::no(node));
                }

                let input = &agg_exec.children()[0];
                let input_size = estimate_data_size(input);

                trace!(
                    input_bytes = input_size,
                    threshold_bytes = threshold,
                    mode = ?mode,
                    num_executors = num_executors,
                    "DistributedAggregateRule: evaluating AggregateExec"
                );

                if input_size > threshold {
                    // Estimate group cardinality for strategy selection
                    let estimated_groups = estimate_group_cardinality(agg_exec);
                    let strategy = select_aggregate_strategy(
                        estimated_groups,
                        num_executors,
                        hc_threshold,
                    );

                    debug!(
                        input_bytes = input_size,
                        estimated_groups = ?estimated_groups,
                        strategy = %strategy,
                        "DistributedAggregateRule: rewriting AggregateExec → \
                         PartialAggregateExec + FinalAggregateExec"
                    );

                    // Create partial aggregate (same group/aggr expressions, Partial mode)
                    let partial_agg = match AggregateExec::try_new(
                        AggregateMode::Partial,
                        agg_exec.group_expr().clone(),
                        agg_exec.aggr_expr().to_vec(),
                        agg_exec.filter_expr().to_vec(),
                        Arc::clone(input),
                        agg_exec.input_schema(),
                    ) {
                        Ok(agg) => agg,
                        Err(e) => {
                            debug!(error = %e, "Failed to create partial AggregateExec");
                            return Ok(Transformed::no(node));
                        }
                    };

                    let partial_node: Arc<dyn ExecutionPlan> = Arc::new(
                        PartialAggregateExec::new(Arc::new(partial_agg), strategy.clone()),
                    );

                    // Create final aggregate consuming partial results
                    let final_agg = match AggregateExec::try_new(
                        AggregateMode::Final,
                        agg_exec.group_expr().clone(),
                        agg_exec.aggr_expr().to_vec(),
                        agg_exec.filter_expr().to_vec(),
                        partial_node,
                        agg_exec.input_schema(),
                    ) {
                        Ok(agg) => agg,
                        Err(e) => {
                            debug!(error = %e, "Failed to create final AggregateExec");
                            return Ok(Transformed::no(node));
                        }
                    };

                    let final_node: Arc<dyn ExecutionPlan> = Arc::new(
                        FinalAggregateExec::new(Arc::new(final_agg), strategy),
                    );

                    return Ok(Transformed::yes(final_node));
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "DistributedAggregateRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Estimate the total data size (bytes) from a plan's statistics.
fn estimate_data_size(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let stats = match plan.partition_statistics(None) {
        Ok(stats) => stats,
        Err(_) => return 0,
    };
    stats.total_byte_size.get_value().copied().unwrap_or(0)
}

/// Estimate the number of distinct group keys from the aggregate's input statistics.
///
/// Uses the `distinct_count` statistic of the FIRST GROUP BY column if available.
/// Returns `None` if statistics are unavailable or the first group-by expression
/// is not a plain column (conservative: assume low cardinality).
fn estimate_group_cardinality(agg: &AggregateExec) -> Option<usize> {
    let group_exprs = agg.group_expr().expr();
    // PLAN-03: map the FIRST group-by expression to its INPUT column index.
    // Previously this read `column_statistics.first()` (always column 0)
    // regardless of which column the group-by referenced, so a group-by on a
    // non-zero column got the wrong column's distinct-count, mis-picking the
    // aggregate strategy (e.g. CoordinatorMerge for a high-cardinality key).
    let Some((first_expr, _alias)) = group_exprs.first() else {
        // No GROUP BY → single group (global aggregation)
        return Some(1);
    };

    // Only plain column references map to an input column index; anything else
    // (expressions, function calls) has no single backing column statistic.
    let column = first_expr
        .downcast_ref::<datafusion::physical_expr::expressions::Column>()?;
    let col_index = column.index();

    let input = &agg.children()[0];
    let stats = input.partition_statistics(None).ok()?;

    // Index the input column statistics by the group-by column's index.
    let col_stats = stats.column_statistics.get(col_index)?;
    col_stats.distinct_count.get_value().copied()
}

// ─────────────────────────────── Tests ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── AggregateStrategy tests ─────────────────────────────────────

    #[test]
    fn test_strategy_display() {
        assert_eq!(
            format!("{}", AggregateStrategy::CoordinatorMerge),
            "CoordinatorMerge"
        );
        assert_eq!(
            format!(
                "{}",
                AggregateStrategy::ShuffleMerge {
                    num_partitions: 4
                }
            ),
            "ShuffleMerge(partitions=4)"
        );
    }

    #[test]
    fn test_select_strategy_low_cardinality() {
        let strategy = select_aggregate_strategy(Some(100), 4, DEFAULT_HIGH_CARDINALITY_THRESHOLD);
        assert_eq!(strategy, AggregateStrategy::CoordinatorMerge);
    }

    #[test]
    fn test_select_strategy_high_cardinality() {
        let strategy =
            select_aggregate_strategy(Some(200_000), 4, DEFAULT_HIGH_CARDINALITY_THRESHOLD);
        assert_eq!(
            strategy,
            AggregateStrategy::ShuffleMerge {
                num_partitions: 4
            }
        );
    }

    #[test]
    fn test_select_strategy_unknown_cardinality() {
        let strategy = select_aggregate_strategy(None, 4, DEFAULT_HIGH_CARDINALITY_THRESHOLD);
        assert_eq!(strategy, AggregateStrategy::CoordinatorMerge);
    }

    #[test]
    fn test_select_strategy_single_executor_always_coordinator() {
        // Even with high cardinality, single executor → coordinator merge
        let strategy =
            select_aggregate_strategy(Some(500_000), 1, DEFAULT_HIGH_CARDINALITY_THRESHOLD);
        assert_eq!(strategy, AggregateStrategy::CoordinatorMerge);
    }

    #[test]
    fn test_select_strategy_at_threshold() {
        // Exactly at threshold → coordinator merge (not above)
        let threshold = 100_000;
        let strategy = select_aggregate_strategy(Some(threshold), 4, threshold);
        assert_eq!(strategy, AggregateStrategy::CoordinatorMerge);
    }

    #[test]
    fn test_select_strategy_above_threshold() {
        let threshold = 100_000;
        let strategy = select_aggregate_strategy(Some(threshold + 1), 4, threshold);
        assert_eq!(
            strategy,
            AggregateStrategy::ShuffleMerge {
                num_partitions: 4
            }
        );
    }

    // ── DistributedAggregateRule tests ──────────────────────────────

    #[test]
    fn test_rule_no_executors_passthrough() {
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use datafusion::physical_plan::memory::LazyMemoryExec;

        let rule = DistributedAggregateRule::new(0, vec![], DEFAULT_HIGH_CARDINALITY_THRESHOLD);
        let config = ConfigOptions::new();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Float64, true),
        ]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap());

        // The rule should pass through without modification
        let result = rule.optimize(input, &config).unwrap();
        // Since there are no executors, the plan should be unchanged
        assert_eq!(result.name(), "LazyMemoryExec");
    }

    #[test]
    fn test_rule_single_executor_passthrough() {
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use datafusion::physical_plan::memory::LazyMemoryExec;

        let rule = DistributedAggregateRule::new(
            0,
            vec!["grpc://h1:50051".to_string()],
            DEFAULT_HIGH_CARDINALITY_THRESHOLD,
        );
        let config = ConfigOptions::new();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
        ]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap());

        let result = rule.optimize(input, &config).unwrap();
        assert_eq!(result.name(), "LazyMemoryExec");
    }

    #[test]
    fn test_rule_name() {
        let rule = DistributedAggregateRule::new(
            DEFAULT_DISTRIBUTED_AGGREGATE_THRESHOLD,
            vec![],
            DEFAULT_HIGH_CARDINALITY_THRESHOLD,
        );
        assert_eq!(rule.name(), "DistributedAggregateRule");
    }

    #[test]
    fn test_rule_schema_check() {
        let rule = DistributedAggregateRule::new(
            DEFAULT_DISTRIBUTED_AGGREGATE_THRESHOLD,
            vec![],
            DEFAULT_HIGH_CARDINALITY_THRESHOLD,
        );
        assert!(rule.schema_check());
    }

    // ── Strategy equality tests ─────────────────────────────────────

    #[test]
    fn test_strategy_equality() {
        assert_eq!(
            AggregateStrategy::CoordinatorMerge,
            AggregateStrategy::CoordinatorMerge
        );
        assert_ne!(
            AggregateStrategy::CoordinatorMerge,
            AggregateStrategy::ShuffleMerge {
                num_partitions: 4
            }
        );
        assert_eq!(
            AggregateStrategy::ShuffleMerge {
                num_partitions: 4
            },
            AggregateStrategy::ShuffleMerge {
                num_partitions: 4
            }
        );
        assert_ne!(
            AggregateStrategy::ShuffleMerge {
                num_partitions: 4
            },
            AggregateStrategy::ShuffleMerge {
                num_partitions: 8
            }
        );
    }

    // ── estimate_group_cardinality tests ────────────────────────────

    #[test]
    fn test_global_aggregation_cardinality() {
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use datafusion::physical_plan::memory::LazyMemoryExec;

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Float64, true),
        ]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(LazyMemoryExec::try_new(schema.clone(), vec![]).unwrap());

        // Create a global aggregation (no GROUP BY)
        let agg = AggregateExec::try_new(
            AggregateMode::Single,
            datafusion::physical_plan::aggregates::PhysicalGroupBy::new_single(vec![]),
            vec![],
            vec![],
            input,
            schema,
        )
        .unwrap();

        assert_eq!(estimate_group_cardinality(&agg), Some(1));
    }
}
