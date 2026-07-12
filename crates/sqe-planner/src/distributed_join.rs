//! Distributed join strategies for multi-node query execution.
//!
//! This module provides three distributed join strategies that leverage the
//! shuffle infrastructure ([`super::shuffle_exec`]) and stage decomposition
//! ([`super::stage_planner`]):
//!
//! 1. **Broadcast join** ([`BroadcastJoinRule`]): When one join side is small
//!    (< `broadcast_threshold`), the small side is collected on the coordinator
//!    and broadcast to all executors. The large side is scanned locally and
//!    joined with the broadcast hash table. No shuffle of the large side.
//!
//! 2. **Shuffle hash join** ([`ShuffleHashJoinPlan`]): When both sides are large,
//!    both are hash-partitioned on join keys via `ShuffleWriterExec`. Each
//!    executor builds a hash table for its partition of the build side and
//!    probes against the incoming probe side.
//!
//! 3. **Sort-merge join for pre-sorted tables** ([`PreSortedJoinRule`]): When
//!    both Iceberg tables have sort orders matching the join keys, use
//!    `SortMergeJoinExec` directly with no shuffle needed.

use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use tracing::{debug, trace};

use crate::shuffle_exec::{ShufflePartitioning, ShuffleWriterExec};

// ─────────────────────────── Constants ──────────────────────────────────────

/// Default broadcast threshold: 64 MB.
///
/// When the estimated size of one join side is below this threshold,
/// the broadcast join strategy is used: the small side is collected on the
/// coordinator and broadcast to all executors via Flight DoPut.
pub const DEFAULT_BROADCAST_THRESHOLD: usize = 64 * 1024 * 1024; // 64 MB

/// Default number of shuffle partitions for shuffle hash join.
/// In practice, this would be set to the number of executors.
pub const DEFAULT_SHUFFLE_PARTITIONS: usize = 8;

// ─────────────────────────── BroadcastJoinRule ──────────────────────────────

/// Physical optimizer rule that detects `HashJoinExec` where one side is
/// small enough to broadcast, and marks it for broadcast distribution.
///
/// The rule walks the physical plan tree, finds `HashJoinExec` nodes, and
/// estimates both sides' sizes from DataFusion statistics. If one side is
/// below the `broadcast_threshold`, the join is left as a local
/// `HashJoinExec` (the coordinator or scheduler will handle the actual
/// broadcast). A [`BroadcastJoinPlan`] wrapper is inserted to signal the
/// distributed execution strategy.
///
/// This rule should run **before** the existing `JoinStrategyRule` (which
/// rewrites large hash joins to sort-merge). Broadcast joins are preferred
/// for small/large joins because they avoid shuffling the large side entirely.
#[derive(Debug)]
pub struct BroadcastJoinRule {
    /// Maximum estimated size (bytes) for a join side to be broadcast.
    broadcast_threshold: usize,
}

impl BroadcastJoinRule {
    /// Create a new `BroadcastJoinRule` with the given threshold in bytes.
    ///
    /// - `broadcast_threshold = 0` disables the rule.
    /// - `broadcast_threshold = DEFAULT_BROADCAST_THRESHOLD` uses the 64 MB default.
    pub fn new(broadcast_threshold: usize) -> Self {
        Self {
            broadcast_threshold,
        }
    }
}

impl PhysicalOptimizerRule for BroadcastJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.broadcast_threshold == 0 {
            return Ok(plan);
        }

        let threshold = self.broadcast_threshold;
        let transformed = plan.transform_down(|node| {
            if let Some(hash_join) = node.downcast_ref::<HashJoinExec>() {
                let left_size = estimate_side_size(hash_join.left());
                let right_size = estimate_side_size(hash_join.right());

                trace!(
                    left_bytes = left_size,
                    right_bytes = right_size,
                    threshold_bytes = threshold,
                    join_type = ?hash_join.join_type(),
                    "BroadcastJoinRule: evaluating HashJoinExec"
                );

                // Determine if one side is small enough to broadcast.
                // Left side (build side) is preferred for broadcast since
                // HashJoinExec already builds a hash table on the left.
                let broadcast_side = if left_size > 0 && left_size < threshold {
                    Some(BroadcastSide::Left)
                } else if right_size > 0 && right_size < threshold {
                    Some(BroadcastSide::Right)
                } else {
                    None
                };

                if let Some(side) = broadcast_side {
                    let (small_size, large_size) = match side {
                        BroadcastSide::Left => (left_size, right_size),
                        BroadcastSide::Right => (right_size, left_size),
                    };
                    debug!(
                        broadcast_side = ?side,
                        small_bytes = small_size,
                        large_bytes = large_size,
                        threshold_bytes = threshold,
                        join_type = ?hash_join.join_type(),
                        "BroadcastJoinRule: marking join for broadcast distribution \
                         (small side {:.1} MB < threshold {:.1} MB)",
                        small_size as f64 / (1024.0 * 1024.0),
                        threshold as f64 / (1024.0 * 1024.0),
                    );

                    // PLAN-04: on a builder failure, keep the original join
                    // rather than panicking inside the optimizer rule.
                    match BroadcastJoinPlan::from_hash_join(hash_join, side) {
                        Ok(broadcast_plan) => {
                            let broadcast_plan = Arc::new(broadcast_plan);
                            return Ok(Transformed::yes(broadcast_plan as Arc<dyn ExecutionPlan>));
                        }
                        Err(e) => {
                            debug!(
                                error = %e,
                                "BroadcastJoinRule: failed to rebuild HashJoinExec, \
                                 keeping original join"
                            );
                            return Ok(Transformed::no(node));
                        }
                    }
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "BroadcastJoinRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ─────────────────────────── BroadcastSide ──────────────────────────────────

/// Which side of the join is broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastSide {
    /// Left (build) side is small and broadcast.
    Left,
    /// Right (probe) side is small and broadcast.
    Right,
}

// ─────────────────────────── BroadcastJoinPlan ──────────────────────────────

/// A wrapper `ExecutionPlan` that signals a broadcast join strategy.
///
/// The small side is collected on the coordinator and broadcast to all
/// executors via Flight `DoPut`. The large side is scanned on executors and
/// joined locally with the broadcast hash table.
///
/// At execution time, the coordinator:
/// 1. Collects the small side into memory (RecordBatches).
/// 2. Broadcasts the data to all executors via Flight DoPut.
/// 3. Each executor builds a local hash table from the broadcast data.
/// 4. Each executor scans its partition of the large side and probes
///    against the hash table.
///
/// This plan delegates actual execution to the inner `HashJoinExec` —
/// the broadcast distribution is handled by the scheduler/coordinator
/// when it sees this plan node type.
#[derive(Debug)]
pub struct BroadcastJoinPlan {
    /// The underlying hash join to execute locally on each executor.
    inner_join: Arc<HashJoinExec>,
    /// The inner join as a trait object (for children() lifetime).
    inner_as_plan: Arc<dyn ExecutionPlan>,
    /// Which side is broadcast (small side).
    broadcast_side: BroadcastSide,
    /// Estimated size of the broadcast side in bytes.
    broadcast_size_bytes: usize,
}

impl BroadcastJoinPlan {
    /// Create a `BroadcastJoinPlan` from an existing `HashJoinExec`.
    ///
    /// PLAN-04: returns `Result` instead of `.expect()`. If the upstream
    /// `HashJoinExec::builder().build()` ever returns `Err` (e.g. a join shape
    /// DataFusion's builder rejects after an upgrade), the rule should degrade
    /// gracefully (the caller falls back to the original plan) rather than
    /// panicking inside a `PhysicalOptimizerRule` and crashing the planning
    /// thread for the query.
    pub fn from_hash_join(hash_join: &HashJoinExec, broadcast_side: BroadcastSide) -> Result<Self> {
        let broadcast_size = match broadcast_side {
            BroadcastSide::Left => estimate_side_size(hash_join.left()),
            BroadcastSide::Right => estimate_side_size(hash_join.right()),
        };

        // Clone the HashJoinExec via its builder, which preserves all fields.
        let inner = hash_join.builder().build()?;

        let inner_join = Arc::new(inner);
        let inner_as_plan = Arc::clone(&inner_join) as Arc<dyn ExecutionPlan>;

        Ok(Self {
            inner_join,
            inner_as_plan,
            broadcast_side,
            broadcast_size_bytes: broadcast_size,
        })
    }

    /// Returns which side is broadcast.
    pub fn broadcast_side(&self) -> BroadcastSide {
        self.broadcast_side
    }

    /// Returns the estimated size of the broadcast side in bytes.
    pub fn broadcast_size_bytes(&self) -> usize {
        self.broadcast_size_bytes
    }

    /// Returns the inner `HashJoinExec`.
    pub fn inner_join(&self) -> &HashJoinExec {
        &self.inner_join
    }
}

impl std::fmt::Display for BroadcastJoinPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BroadcastJoinPlan: side={:?}, size={:.1}MB, join_type={:?}",
            self.broadcast_side,
            self.broadcast_size_bytes as f64 / (1024.0 * 1024.0),
            self.inner_join.join_type(),
        )
    }
}

impl datafusion::physical_plan::DisplayAs for BroadcastJoinPlan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl ExecutionPlan for BroadcastJoinPlan {
    fn name(&self) -> &str {
        "BroadcastJoinPlan"
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner_join.schema()
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.inner_join.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner_as_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "BroadcastJoinPlan expects exactly one child (the inner HashJoinExec)".to_string(),
            ));
        }
        // The child must be a HashJoinExec
        if let Some(hash_join) = children[0].downcast_ref::<HashJoinExec>() {
            Ok(Arc::new(BroadcastJoinPlan::from_hash_join(
                hash_join,
                self.broadcast_side,
            )?))
        } else {
            // If it was wrapped differently, just return self
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        // In the current implementation, delegate to the inner HashJoinExec.
        // The actual broadcast distribution is handled by the coordinator/scheduler
        // when it detects a BroadcastJoinPlan in the physical plan tree.
        debug!(
            broadcast_side = ?self.broadcast_side,
            broadcast_size = self.broadcast_size_bytes,
            partition = partition,
            "Executing BroadcastJoinPlan (delegating to inner HashJoinExec)"
        );
        self.inner_join.execute(partition, context)
    }
}

// ─────────────────────────── ShuffleHashJoinPlan ────────────────────────────

/// Distributed shuffle hash join plan.
///
/// When both join sides are large (neither below the broadcast threshold),
/// both sides are hash-partitioned on the join keys via `ShuffleWriterExec`.
/// Each executor builds a hash table for its partition of the build side
/// and probes against the incoming probe side.
///
/// Memory per executor: O(build_side / num_executors).
///
/// If the partitioned build side still exceeds the `JoinStrategyRule`
/// threshold, the existing fallback to `SortMergeJoinExec` will apply
/// (from the `JoinStrategyRule` optimizer that runs after this).
#[derive(Debug)]
pub struct ShuffleHashJoinPlan {
    /// The build side plan (left side of the join).
    build_side: Arc<dyn ExecutionPlan>,
    /// The probe side plan (right side of the join).
    probe_side: Arc<dyn ExecutionPlan>,
    /// Join key column names on the build side.
    build_key_columns: Vec<String>,
    /// Join key column names on the probe side.
    probe_key_columns: Vec<String>,
    /// The join type (Inner, Left, Right, etc.).
    join_type: JoinType,
    /// Number of shuffle partitions (typically = number of executors).
    num_partitions: usize,
    /// Cached plan properties from the underlying join.
    properties: Arc<datafusion::physical_plan::PlanProperties>,
}

impl ShuffleHashJoinPlan {
    /// Create a new `ShuffleHashJoinPlan` from two sides and join metadata.
    ///
    /// # Arguments
    /// - `build_side`: The build (left) side plan.
    /// - `probe_side`: The probe (right) side plan.
    /// - `build_key_columns`: Column names on the build side used as join keys.
    /// - `probe_key_columns`: Column names on the probe side used as join keys.
    /// - `join_type`: The SQL join type.
    /// - `num_partitions`: Number of hash partitions (executors).
    pub fn new(
        build_side: Arc<dyn ExecutionPlan>,
        probe_side: Arc<dyn ExecutionPlan>,
        build_key_columns: Vec<String>,
        probe_key_columns: Vec<String>,
        join_type: JoinType,
        num_partitions: usize,
    ) -> Self {
        // Combine schemas from both sides for the output
        let build_schema = build_side.schema();
        let probe_schema = probe_side.schema();
        let mut fields = build_schema.fields().to_vec();
        fields.extend(probe_schema.fields().iter().cloned());
        let output_schema = Arc::new(arrow_schema::Schema::new(fields));

        let properties = Arc::new(datafusion::physical_plan::PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(num_partitions),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));

        Self {
            build_side,
            probe_side,
            build_key_columns,
            probe_key_columns,
            join_type,
            num_partitions,
            properties,
        }
    }

    /// Create from a `HashJoinExec`, wrapping both sides in shuffle writers.
    pub fn from_hash_join(
        hash_join: &HashJoinExec,
        num_partitions: usize,
        target_endpoints: Vec<String>,
        query_id: String,
    ) -> Self {
        let build_keys: Vec<String> = hash_join
            .on()
            .iter()
            .map(|(left, _)| format!("{left}"))
            .collect();
        let probe_keys: Vec<String> = hash_join
            .on()
            .iter()
            .map(|(_, right)| format!("{right}"))
            .collect();

        // Wrap build side in ShuffleWriterExec
        let build_shuffle = Arc::new(ShuffleWriterExec::new(
            Arc::clone(hash_join.left()),
            ShufflePartitioning::Hash {
                key_columns: build_keys.clone(),
                num_partitions,
            },
            target_endpoints.clone(),
            query_id.clone(),
            "build".to_string(),
        ));

        // Wrap probe side in ShuffleWriterExec
        let probe_shuffle = Arc::new(ShuffleWriterExec::new(
            Arc::clone(hash_join.right()),
            ShufflePartitioning::Hash {
                key_columns: probe_keys.clone(),
                num_partitions,
            },
            target_endpoints,
            query_id,
            "probe".to_string(),
        ));

        Self::new(
            build_shuffle,
            probe_shuffle,
            build_keys,
            probe_keys,
            *hash_join.join_type(),
            num_partitions,
        )
    }

    /// Returns the build side plan.
    pub fn build_side(&self) -> &Arc<dyn ExecutionPlan> {
        &self.build_side
    }

    /// Returns the probe side plan.
    pub fn probe_side(&self) -> &Arc<dyn ExecutionPlan> {
        &self.probe_side
    }

    /// Returns the join type.
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// Returns the number of shuffle partitions.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Returns the build-side key columns.
    pub fn build_key_columns(&self) -> &[String] {
        &self.build_key_columns
    }

    /// Returns the probe-side key columns.
    pub fn probe_key_columns(&self) -> &[String] {
        &self.probe_key_columns
    }

    /// Estimate the memory per executor for the build side.
    pub fn estimated_memory_per_executor(&self) -> usize {
        let build_size = estimate_side_size(&self.build_side);
        if self.num_partitions > 0 {
            build_size / self.num_partitions
        } else {
            build_size
        }
    }
}

impl std::fmt::Display for ShuffleHashJoinPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShuffleHashJoinPlan: join_type={:?}, partitions={}, \
             build_keys=[{}], probe_keys=[{}]",
            self.join_type,
            self.num_partitions,
            self.build_key_columns.join(", "),
            self.probe_key_columns.join(", "),
        )
    }
}

impl datafusion::physical_plan::DisplayAs for ShuffleHashJoinPlan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl ExecutionPlan for ShuffleHashJoinPlan {
    fn name(&self) -> &str {
        "ShuffleHashJoinPlan"
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.properties.eq_properties.schema().clone()
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.build_side, &self.probe_side]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(datafusion::error::DataFusionError::Internal(
                "ShuffleHashJoinPlan expects exactly two children".to_string(),
            ));
        }
        Ok(Arc::new(ShuffleHashJoinPlan::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.build_key_columns.clone(),
            self.probe_key_columns.clone(),
            self.join_type,
            self.num_partitions,
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        // ShuffleHashJoinPlan is a planning-time node. Actual execution
        // is handled by the distributed scheduler which:
        // 1. Executes build_side ShuffleWriterExec on source executors
        // 2. Executes probe_side ShuffleWriterExec on source executors
        // 3. On each target executor, runs HashJoinExec on the shuffled data
        //
        // Direct execution returns an error to flag incorrect usage.
        Err(datafusion::error::DataFusionError::Internal(
            "ShuffleHashJoinPlan cannot be executed directly. \
             It must be decomposed into stages by the distributed scheduler."
                .to_string(),
        ))
    }
}

// ─────────────────────────── PreSortedJoinRule ──────────────────────────────

/// Physical optimizer rule that detects when both sides of a join are
/// already sorted on the join key columns and rewrites to
/// `SortMergeJoinExec` without adding `SortExec` wrappers.
///
/// This is a plan-time optimization: if both sides report sorted
/// `EquivalenceProperties` on the join key columns (e.g., from Iceberg
/// sort order detection in `sqe-catalog::sort_order`), the join can use
/// `SortMergeJoinExec` directly with O(batch_size) memory and zero shuffle.
///
/// This rule should run **before** both `BroadcastJoinRule` and
/// `JoinStrategyRule` since it produces the most efficient plan when
/// applicable.
#[derive(Debug)]
pub struct PreSortedJoinRule;

impl PreSortedJoinRule {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PreSortedJoinRule {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for PreSortedJoinRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let transformed = plan.transform_down(|node| {
            if let Some(hash_join) = node.downcast_ref::<HashJoinExec>() {
                let on = hash_join.on();
                if on.is_empty() {
                    return Ok(Transformed::no(node));
                }

                // Build sort expressions for each side from the join keys.
                let left_sort_exprs: Vec<PhysicalSortExpr> = on
                    .iter()
                    .map(|(left_col, _)| {
                        PhysicalSortExpr::new(Arc::clone(left_col), SortOptions::default())
                    })
                    .collect();

                let right_sort_exprs: Vec<PhysicalSortExpr> = on
                    .iter()
                    .map(|(_, right_col)| {
                        PhysicalSortExpr::new(Arc::clone(right_col), SortOptions::default())
                    })
                    .collect();

                let left_sorted = is_sorted_on(hash_join.left(), &left_sort_exprs);
                let right_sorted = is_sorted_on(hash_join.right(), &right_sort_exprs);

                trace!(
                    left_sorted = left_sorted,
                    right_sorted = right_sorted,
                    join_type = ?hash_join.join_type(),
                    "PreSortedJoinRule: checking sort compatibility"
                );

                if left_sorted && right_sorted {
                    debug!(
                        join_type = ?hash_join.join_type(),
                        "PreSortedJoinRule: both sides sorted on join keys, \
                         using SortMergeJoinExec without shuffle"
                    );

                    let sort_options: Vec<SortOptions> =
                        on.iter().map(|_| SortOptions::default()).collect();

                    match SortMergeJoinExec::try_new(
                        Arc::clone(hash_join.left()),
                        Arc::clone(hash_join.right()),
                        on.to_vec(),
                        hash_join.filter().cloned(),
                        *hash_join.join_type(),
                        sort_options,
                        hash_join.null_equality(),
                    ) {
                        Ok(smj) => {
                            return Ok(Transformed::yes(Arc::new(smj) as Arc<dyn ExecutionPlan>));
                        }
                        Err(e) => {
                            debug!(
                                error = %e,
                                "PreSortedJoinRule: failed to create SortMergeJoinExec, \
                                 keeping HashJoinExec"
                            );
                        }
                    }
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "PreSortedJoinRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ─────────────────────────── Helper functions ───────────────────────────────

/// Estimate the size of a plan side in bytes from DataFusion statistics.
///
/// Uses `total_byte_size` from the plan's statistics if available.
/// Returns `0` if statistics are unavailable.
fn estimate_side_size(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let stats = match plan.partition_statistics(None) {
        Ok(stats) => stats,
        Err(_) => return 0,
    };

    stats.total_byte_size.get_value().copied().unwrap_or(0)
}

/// Check whether the plan's output ordering satisfies the required sort
/// expressions. Verifies the output ordering has the same columns in the
/// same order (prefix match).
fn is_sorted_on(plan: &Arc<dyn ExecutionPlan>, required: &[PhysicalSortExpr]) -> bool {
    if required.is_empty() {
        return true;
    }

    let output_ordering = match plan.output_ordering() {
        Some(ordering) => ordering,
        None => return false,
    };

    if output_ordering.len() < required.len() {
        return false;
    }

    for (existing, required_expr) in output_ordering.iter().zip(required.iter()) {
        if format!("{existing}") != format!("{required_expr}") {
            return false;
        }
    }

    true
}

/// Select the appropriate distributed join strategy for a `HashJoinExec`.
///
/// Returns the strategy that should be used based on the estimated sizes
/// of both join sides and the broadcast threshold.
pub fn select_join_strategy(hash_join: &HashJoinExec, broadcast_threshold: usize) -> JoinStrategy {
    let left_size = estimate_side_size(hash_join.left());
    let right_size = estimate_side_size(hash_join.right());

    // Check for pre-sorted sides first (cheapest strategy)
    let on = hash_join.on();
    if !on.is_empty() {
        let left_sort_exprs: Vec<PhysicalSortExpr> = on
            .iter()
            .map(|(left_col, _)| {
                PhysicalSortExpr::new(Arc::clone(left_col), SortOptions::default())
            })
            .collect();
        let right_sort_exprs: Vec<PhysicalSortExpr> = on
            .iter()
            .map(|(_, right_col)| {
                PhysicalSortExpr::new(Arc::clone(right_col), SortOptions::default())
            })
            .collect();

        if is_sorted_on(hash_join.left(), &left_sort_exprs)
            && is_sorted_on(hash_join.right(), &right_sort_exprs)
        {
            return JoinStrategy::SortMerge;
        }
    }

    // Check for broadcast (one small side)
    if broadcast_threshold > 0 {
        if left_size > 0 && left_size < broadcast_threshold {
            return JoinStrategy::Broadcast(BroadcastSide::Left);
        }
        if right_size > 0 && right_size < broadcast_threshold {
            return JoinStrategy::Broadcast(BroadcastSide::Right);
        }
    }

    // Default: shuffle hash join
    JoinStrategy::ShuffleHash
}

/// The selected distributed join strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Both sides pre-sorted on join keys. Use SortMergeJoinExec directly.
    SortMerge,
    /// One side is small enough to broadcast. The specified side is broadcast.
    Broadcast(BroadcastSide),
    /// Both sides large. Hash-partition both on join keys.
    ShuffleHash,
}

impl std::fmt::Display for JoinStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinStrategy::SortMerge => write!(f, "SortMerge (pre-sorted)"),
            JoinStrategy::Broadcast(side) => write!(f, "Broadcast ({side:?})"),
            JoinStrategy::ShuffleHash => write!(f, "ShuffleHash"),
        }
    }
}

// ─────────────────────────────── Tests ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::NullEquality;
    use datafusion::physical_expr::LexOrdering;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::sorts::sort::SortExec;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_memory_plan(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    fn make_hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        schema: &Schema,
        join_type: JoinType,
    ) -> HashJoinExec {
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", schema).unwrap(),
        )];
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap()
    }

    fn make_hash_join_plan(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        schema: &Schema,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(make_hash_join(left, right, schema, join_type))
    }

    // ─── BroadcastJoinRule tests ───

    #[test]
    fn test_broadcast_rule_disabled_when_threshold_zero() {
        let rule = BroadcastJoinRule::new(0);
        let config = ConfigOptions::new();
        let schema = test_schema();
        let plan = make_hash_join_plan(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let result = rule.optimize(plan, &config).unwrap();
        assert!(
            result.downcast_ref::<HashJoinExec>().is_some(),
            "Expected HashJoinExec when threshold is 0"
        );
    }

    #[test]
    fn test_broadcast_rule_keeps_join_when_no_stats() {
        // LazyMemoryExec reports 0 bytes for both sides.
        // 0 is not > 0, so the rule won't trigger.
        let rule = BroadcastJoinRule::new(DEFAULT_BROADCAST_THRESHOLD);
        let config = ConfigOptions::new();
        let schema = test_schema();
        let plan = make_hash_join_plan(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let result = rule.optimize(plan, &config).unwrap();
        // With 0-byte stats, neither side triggers broadcast
        assert!(
            result.downcast_ref::<HashJoinExec>().is_some(),
            "Expected HashJoinExec when stats are unavailable"
        );
    }

    #[test]
    fn test_broadcast_rule_name() {
        let rule = BroadcastJoinRule::new(DEFAULT_BROADCAST_THRESHOLD);
        assert_eq!(rule.name(), "BroadcastJoinRule");
    }

    #[test]
    fn test_broadcast_rule_schema_check() {
        let rule = BroadcastJoinRule::new(DEFAULT_BROADCAST_THRESHOLD);
        assert!(rule.schema_check());
    }

    // ─── BroadcastJoinPlan tests ───

    #[test]
    fn test_broadcast_plan_from_hash_join() {
        let schema = test_schema();
        let hash_join = make_hash_join(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let plan = BroadcastJoinPlan::from_hash_join(&hash_join, BroadcastSide::Left).unwrap();
        assert_eq!(plan.broadcast_side(), BroadcastSide::Left);
        assert_eq!(plan.name(), "BroadcastJoinPlan");
    }

    #[test]
    fn test_broadcast_plan_display() {
        let schema = test_schema();
        let hash_join = make_hash_join(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let plan = BroadcastJoinPlan::from_hash_join(&hash_join, BroadcastSide::Left).unwrap();
        let display = format!("{plan}");
        assert!(display.contains("BroadcastJoinPlan"));
        assert!(display.contains("Left"));
    }

    #[test]
    fn test_broadcast_plan_schema_matches_inner() {
        let schema = test_schema();
        let hash_join = make_hash_join(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let plan = BroadcastJoinPlan::from_hash_join(&hash_join, BroadcastSide::Left).unwrap();
        assert_eq!(plan.schema(), hash_join.schema());
    }

    // ─── ShuffleHashJoinPlan tests ───

    #[test]
    fn test_shuffle_hash_join_plan_new() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());

        let plan = ShuffleHashJoinPlan::new(
            left,
            right,
            vec!["id".to_string()],
            vec!["id".to_string()],
            JoinType::Inner,
            4,
        );

        assert_eq!(plan.name(), "ShuffleHashJoinPlan");
        assert_eq!(plan.num_partitions(), 4);
        assert_eq!(*plan.join_type(), JoinType::Inner);
        assert_eq!(plan.build_key_columns(), &["id"]);
        assert_eq!(plan.probe_key_columns(), &["id"]);
    }

    #[test]
    fn test_shuffle_hash_join_plan_display() {
        let schema = test_schema();
        let plan = ShuffleHashJoinPlan::new(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            vec!["id".to_string()],
            vec!["id".to_string()],
            JoinType::Inner,
            4,
        );

        let display = format!("{plan}");
        assert!(display.contains("ShuffleHashJoinPlan"));
        assert!(display.contains("partitions=4"));
        assert!(display.contains("Inner"));
    }

    #[test]
    fn test_shuffle_hash_join_plan_children() {
        let schema = test_schema();
        let plan = ShuffleHashJoinPlan::new(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            vec!["id".to_string()],
            vec!["id".to_string()],
            JoinType::Inner,
            4,
        );

        assert_eq!(plan.children().len(), 2);
    }

    #[test]
    fn test_shuffle_hash_join_plan_execute_returns_error() {
        let schema = test_schema();
        let plan = ShuffleHashJoinPlan::new(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            vec!["id".to_string()],
            vec!["id".to_string()],
            JoinType::Inner,
            4,
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let result = plan.execute(0, ctx.task_ctx());
        assert!(
            result.is_err(),
            "ShuffleHashJoinPlan should not be directly executable"
        );
    }

    #[test]
    fn test_shuffle_hash_join_from_hash_join() {
        let schema = test_schema();
        let hash_join = make_hash_join(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let plan = ShuffleHashJoinPlan::from_hash_join(
            &hash_join,
            4,
            vec![
                "grpc://h1:50051".to_string(),
                "grpc://h2:50051".to_string(),
                "grpc://h3:50051".to_string(),
                "grpc://h4:50051".to_string(),
            ],
            "q1".to_string(),
        );

        assert_eq!(plan.num_partitions(), 4);
        // Both sides should be wrapped in ShuffleWriterExec
        assert!(
            plan.build_side()
                .downcast_ref::<ShuffleWriterExec>()
                .is_some(),
            "Build side should be wrapped in ShuffleWriterExec"
        );
        assert!(
            plan.probe_side()
                .downcast_ref::<ShuffleWriterExec>()
                .is_some(),
            "Probe side should be wrapped in ShuffleWriterExec"
        );
    }

    #[test]
    fn test_shuffle_hash_join_with_new_children() {
        let schema = test_schema();
        let plan = Arc::new(ShuffleHashJoinPlan::new(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            vec!["id".to_string()],
            vec!["id".to_string()],
            JoinType::Inner,
            4,
        ));

        let new_left = make_memory_plan(schema.clone());
        let new_right = make_memory_plan(schema.clone());
        let result = plan.with_new_children(vec![new_left, new_right]);
        assert!(result.is_ok());

        let new_plan = result.unwrap();
        assert_eq!(new_plan.name(), "ShuffleHashJoinPlan");
    }

    #[test]
    fn test_shuffle_hash_join_with_wrong_children_count() {
        let schema = test_schema();
        let plan = Arc::new(ShuffleHashJoinPlan::new(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            vec!["id".to_string()],
            vec!["id".to_string()],
            JoinType::Inner,
            4,
        ));

        let result = plan.with_new_children(vec![make_memory_plan(schema.clone())]);
        assert!(result.is_err());
    }

    // ─── PreSortedJoinRule tests ───

    #[test]
    fn test_presorted_rule_keeps_unsorted_joins() {
        let rule = PreSortedJoinRule::new();
        let config = ConfigOptions::new();
        let schema = test_schema();

        // LazyMemoryExec has no output ordering, so the rule should not fire
        let plan = make_hash_join_plan(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let result = rule.optimize(plan, &config).unwrap();
        assert!(
            result.downcast_ref::<HashJoinExec>().is_some(),
            "Expected HashJoinExec when inputs are not sorted"
        );
    }

    #[test]
    fn test_presorted_rule_rewrites_sorted_inputs() {
        let rule = PreSortedJoinRule::new();
        let config = ConfigOptions::new();
        let schema = test_schema();

        // Wrap both sides in SortExec on "id" to simulate pre-sorted inputs
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();

        let sorted_left: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            ordering.clone(),
            make_memory_plan(schema.clone()),
        ));
        let sorted_right: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, make_memory_plan(schema.clone())));

        let plan = make_hash_join_plan(sorted_left, sorted_right, &schema, JoinType::Inner);

        let result = rule.optimize(plan, &config).unwrap();
        assert!(
            result.downcast_ref::<SortMergeJoinExec>().is_some(),
            "Expected SortMergeJoinExec when both inputs are sorted on join keys"
        );
    }

    #[test]
    fn test_presorted_rule_name() {
        let rule = PreSortedJoinRule::new();
        assert_eq!(rule.name(), "PreSortedJoinRule");
    }

    #[test]
    fn test_presorted_rule_default() {
        let rule = PreSortedJoinRule;
        assert_eq!(rule.name(), "PreSortedJoinRule");
    }

    // ─── JoinStrategy selection tests ───

    #[test]
    fn test_select_strategy_defaults_to_shuffle_hash() {
        let schema = test_schema();
        let hash_join = make_hash_join(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        // Both sides have 0 stats, not sorted → ShuffleHash
        let strategy = select_join_strategy(&hash_join, DEFAULT_BROADCAST_THRESHOLD);
        assert_eq!(strategy, JoinStrategy::ShuffleHash);
    }

    #[test]
    fn test_select_strategy_presorted() {
        let schema = test_schema();

        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();

        let sorted_left: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            ordering.clone(),
            make_memory_plan(schema.clone()),
        ));
        let sorted_right: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, make_memory_plan(schema.clone())));

        let hash_join = make_hash_join(sorted_left, sorted_right, &schema, JoinType::Inner);
        let strategy = select_join_strategy(&hash_join, DEFAULT_BROADCAST_THRESHOLD);
        assert_eq!(strategy, JoinStrategy::SortMerge);
    }

    #[test]
    fn test_select_strategy_broadcast_disabled() {
        let schema = test_schema();
        let hash_join = make_hash_join(
            make_memory_plan(schema.clone()),
            make_memory_plan(schema.clone()),
            &schema,
            JoinType::Inner,
        );

        let strategy = select_join_strategy(&hash_join, 0);
        assert_eq!(strategy, JoinStrategy::ShuffleHash);
    }

    #[test]
    fn test_join_strategy_display() {
        assert_eq!(
            format!("{}", JoinStrategy::SortMerge),
            "SortMerge (pre-sorted)"
        );
        assert_eq!(
            format!("{}", JoinStrategy::Broadcast(BroadcastSide::Left)),
            "Broadcast (Left)"
        );
        assert_eq!(format!("{}", JoinStrategy::ShuffleHash), "ShuffleHash");
    }

    // ─── Helper function tests ───

    #[test]
    fn test_estimate_side_size_no_stats() {
        let schema = test_schema();
        let plan = make_memory_plan(schema);
        let size = estimate_side_size(&plan);
        assert_eq!(size, 0, "Empty LazyMemoryExec should report 0 bytes");
    }

    #[test]
    fn test_is_sorted_on_empty_required() {
        let schema = test_schema();
        let plan = make_memory_plan(schema);
        assert!(is_sorted_on(&plan, &[]));
    }

    #[test]
    fn test_is_sorted_on_unsorted_plan() {
        let schema = test_schema();
        let plan = make_memory_plan(schema.clone());
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            SortOptions::default(),
        );
        assert!(!is_sorted_on(&plan, &[sort_expr]));
    }

    #[test]
    fn test_is_sorted_on_sorted_plan() {
        let schema = test_schema();
        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr.clone()]).unwrap();
        let sorted_plan: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, make_memory_plan(schema)));
        assert!(is_sorted_on(&sorted_plan, &[sort_expr]));
    }
}
