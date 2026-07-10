//! Physical optimizer rule: put the dimension scan on the BUILD side of a
//! star-tail `CollectLeft` join, even when cascaded cardinality estimates
//! say otherwise.
//!
//! ## Why (SSB q4.1/q4.2 partkey gap)
//!
//! DataFusion's `JoinSelection` picks build vs probe by comparing statistics:
//! byte size first, row count as the fallback. Join OUTPUTS report their byte
//! size as `Absent` unconditionally (DF54 `joins/utils.rs`), so for a join
//! whose build candidate is itself a join pipeline the decision rides on the
//! cascaded row-count estimate — and those estimates compound multiplied
//! selectivities. Measured on SSB SF10 q4.1: the three-join lineorder stream
//! was estimated at 100,387 rows (actual 2,433,461, a 24x underestimate)
//! against part's filtered estimate of 160,000, so the stream stayed as the
//! build. The consequence is worse than a bigger build: hash-join dynamic
//! filters flow BUILD to PROBE, so the plan pushed a multi-million-key filter
//! from the fact stream into the 800K-row part scan, while part's selective
//! key set never reached the fact scan at all.
//!
//! ## The rule
//!
//! For an Inner `CollectLeft` join where the left (build) subtree contains
//! another hash join (a stream whose estimate is a cascaded guess) and the
//! right (probe) subtree is a join-free path over exactly one Iceberg scan
//! with a KNOWN byte size under the broadcast threshold, swap the sides.
//! The risk is asymmetric by construction: a wrong swap collects a
//! threshold-bounded dimension table (tens of MB at most); a wrong keep
//! collects an unbounded stream AND points the dynamic filter the useless
//! direction. Bounded downside, large upside.
//!
//! ## Rewiring
//!
//! This rule runs after planning, i.e. after the post-optimization
//! `FilterPushdown` pass has already wired dynamic filters for the OLD
//! orientation. After any swap the rule therefore: strips the pushed-down
//! dynamic filters from every Iceberg scan (`handle_child_pushdown_result`
//! appends, so re-running without stripping would double-wire), rebuilds
//! every hash join so stale `dynamic_filter_expr` state is dropped, inserts
//! a `CoalescePartitionsExec` under any swapped build side that still has
//! more than one partition (`CollectLeft` requires a single build
//! partition; `EnforceDistribution` ran before us), and finally re-runs
//! `FilterPushdown::new_post_optimization()` so every join re-wires its
//! dynamic filter for the new orientation.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::JoinType;
use sqe_catalog::iceberg_scan::IcebergScanExec;
use tracing::debug;

/// See module docs. Swaps dim scans onto the build side of star-tail
/// `CollectLeft` joins and rewires dynamic filters for the new orientation.
#[derive(Debug, Default)]
pub struct DimBuildSwapRule;

impl DimBuildSwapRule {
    pub fn new() -> Self {
        Self
    }
}

/// Count hash joins in a subtree.
fn count_hash_joins(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    let _ = plan.apply(|node| {
        if node.downcast_ref::<HashJoinExec>().is_some() {
            count += 1;
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    count
}

/// Collect the Iceberg scans in a subtree.
fn collect_iceberg_scans(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut scans = Vec::new();
    let _ = plan.apply(|node| {
        if node.downcast_ref::<IcebergScanExec>().is_some() {
            scans.push(Arc::clone(node));
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    scans
}

/// Table identifiers of every Iceberg scan in a subtree (self-join guard).
fn iceberg_table_idents(plan: &Arc<dyn ExecutionPlan>) -> HashSet<String> {
    collect_iceberg_scans(plan)
        .iter()
        .filter_map(|s| {
            s.downcast_ref::<IcebergScanExec>()
                .map(|scan| scan.table().identifier().to_string())
        })
        .collect()
}

/// Pure eligibility decision, separated from plan walking for unit tests.
///
/// `left_bytes`/`right_bytes` are the `total_byte_size` statistics of the
/// respective subtrees (`None` when `Precision::Absent`).
fn should_swap(
    left_join_count: usize,
    right_join_count: usize,
    right_scan_count: usize,
    left_bytes: Option<usize>,
    right_bytes: Option<usize>,
    broadcast_threshold: usize,
    tables_overlap: bool,
) -> bool {
    // Left must be a join pipeline: its stats are cascaded estimates.
    if left_join_count == 0 {
        return false;
    }
    // Right must be a join-free path over exactly one scan: its stats are
    // manifest-backed facts.
    if right_join_count != 0 || right_scan_count != 1 {
        return false;
    }
    // Self-join tails are handled by the dedicated q95-class strip logic;
    // do not restructure them here.
    if tables_overlap {
        return false;
    }
    // Only act where JoinSelection was blind: the join-output side reports
    // no byte size, while the scan side has a known, bounded one.
    if left_bytes.is_some() {
        return false;
    }
    match right_bytes {
        Some(bytes) => bytes < broadcast_threshold,
        None => false,
    }
}

impl PhysicalOptimizerRule for DimBuildSwapRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let threshold = config.optimizer.hash_join_single_partition_threshold;
        let mut swapped_any = false;

        let transformed = Arc::clone(&plan).transform_up(|node| {
            let Some(hj) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if *hj.partition_mode() != PartitionMode::CollectLeft
                || *hj.join_type() != JoinType::Inner
                || hj.null_aware
            {
                return Ok(Transformed::no(node));
            }

            let left = hj.left();
            let right = hj.right();
            let left_stats = left.partition_statistics(None)?;
            let right_stats = right.partition_statistics(None)?;
            let left_tables = iceberg_table_idents(left);
            let right_tables = iceberg_table_idents(right);

            let eligible = should_swap(
                count_hash_joins(left),
                count_hash_joins(right),
                collect_iceberg_scans(right).len(),
                left_stats.total_byte_size.get_value().copied(),
                right_stats.total_byte_size.get_value().copied(),
                threshold,
                left_tables.intersection(&right_tables).next().is_some(),
            );
            if !eligible {
                return Ok(Transformed::no(node));
            }

            debug!(
                on = ?hj.on(),
                right_bytes = ?right_stats.total_byte_size.get_value(),
                "DimBuildSwap: moving dim scan to build side of star-tail join"
            );
            let swapped = hj.swap_inputs(PartitionMode::CollectLeft)?;
            swapped_any = true;
            Ok(Transformed::yes(swapped))
        })?;

        if !swapped_any {
            return Ok(transformed.data);
        }

        // Rewire for the new orientation (see module docs):
        // 1. Strip stale pushed-down dynamic filters from every scan.
        // 2. Rebuild every hash join so stale dynamic_filter_expr state and
        //    stale single-partition build requirements are re-derived.
        // 3. Coalesce any CollectLeft build side left with >1 partition.
        let stripped = transformed.data.transform_up(|node| {
            if let Some(scan) = node.downcast_ref::<IcebergScanExec>() {
                if !scan.pushed_down_filters().is_empty() {
                    let new_scan: Arc<dyn ExecutionPlan> =
                        Arc::new(scan.clone_without_pushed_filters());
                    return Ok(Transformed::yes(new_scan));
                }
                return Ok(Transformed::no(node));
            }
            let Some(hj) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            let left = if *hj.partition_mode() == PartitionMode::CollectLeft
                && hj.left().output_partitioning().partition_count() > 1
            {
                Arc::new(CoalescePartitionsExec::new(Arc::clone(hj.left())))
                    as Arc<dyn ExecutionPlan>
            } else {
                Arc::clone(hj.left())
            };
            let rebuilt = HashJoinExec::try_new(
                left,
                Arc::clone(hj.right()),
                hj.on().to_vec(),
                hj.filter().cloned(),
                hj.join_type(),
                hj.projection.as_ref().map(|p| p.to_vec()),
                *hj.partition_mode(),
                hj.null_equality(),
                hj.null_aware,
            )?;
            Ok(Transformed::yes(Arc::new(rebuilt) as Arc<dyn ExecutionPlan>))
        })?;

        // 4. Re-run the post-optimization filter pushdown so every join
        //    re-wires its dynamic filter toward its (possibly new) probe side.
        FilterPushdown::new_post_optimization().optimize(stripped.data, config)
    }

    fn name(&self) -> &str {
        "DimBuildSwapRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::should_swap;

    const MB64: usize = 64 * 1024 * 1024;

    /// The SSB q4.1 shape: left is a 3-join stream (bytes Absent), right is
    /// one part scan with 1.5MB known bytes. Must swap.
    #[test]
    fn swaps_star_tail_with_known_small_dim() {
        assert!(should_swap(3, 0, 1, None, Some(1_550_584), MB64, false));
    }

    /// Left is a plain scan (no joins): JoinSelection had real stats for
    /// both sides and made an informed call. Never override it.
    #[test]
    fn keeps_scan_vs_scan_joins() {
        assert!(!should_swap(0, 0, 1, None, Some(1_000), MB64, false));
    }

    /// Right subtree contains a join: its stats are cascaded estimates too,
    /// no side is trustworthy. Keep.
    #[test]
    fn keeps_join_vs_join() {
        assert!(!should_swap(2, 1, 2, None, Some(1_000), MB64, false));
    }

    /// Dim above the broadcast threshold must not become a collected build.
    #[test]
    fn keeps_large_dim() {
        assert!(!should_swap(3, 0, 1, None, Some(MB64), MB64, false));
    }

    /// Unknown dim size: no byte evidence, no swap.
    #[test]
    fn keeps_unknown_dim_size() {
        assert!(!should_swap(3, 0, 1, None, None, MB64, false));
    }

    /// If the join-output side somehow has known bytes, JoinSelection could
    /// compare properly; stay out of its way.
    #[test]
    fn keeps_when_left_bytes_known() {
        assert!(!should_swap(3, 0, 1, Some(500), Some(1_000), MB64, false));
    }

    /// Self-join tails belong to the q95-class strip logic.
    #[test]
    fn keeps_self_joins() {
        assert!(!should_swap(3, 0, 1, None, Some(1_000), MB64, true));
    }
}
