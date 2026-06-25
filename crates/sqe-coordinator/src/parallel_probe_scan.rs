//! Physical optimizer rule: parallelize the PROBE-side Iceberg scan under a
//! `CollectLeft` (broadcast) hash join, without touching the build side.
//!
//! ## Why (issue #235, follow-up to #131)
//!
//! `IcebergScanExec` defaults to one output partition because auto-wiring
//! `target_partitions` at the table-provider level bumped EVERY scan -- including
//! the BUILD (left) side of `CollectLeft` joins. A build-side scan advertising
//! `UnknownPartitioning(N)` forces `EnforceDistribution` to insert a
//! `CoalescePartitionsExec` and build the hash table from many tiny round-robin
//! batches; tpcds q72 regressed 5-6x. See `table_provider.rs` and #131.
//!
//! A `CollectLeft` join, however, already supports a PARALLEL PROBE: the left
//! build is collected into one partition, but the right probe may have N
//! partitions (shared build hash table, N probe threads). Star-schema fact
//! tables (SSB `lineorder`, TPC-H `lineitem`) are always the probe side. So the
//! safe, regression-free win is: parallelize ONLY scans on the probe (right)
//! side of `CollectLeft` joins, never a build-side scan.
//!
//! The load-bearing invariant -- "never parallelize a build-side scan" -- is the
//! q72 regression guard, and it is unit-tested directly via
//! [`collect_probe_side_leaves`].

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use sqe_catalog::iceberg_scan::IcebergScanExec;
use tracing::debug;

/// Physical optimizer rule that parallelizes the probe-side Iceberg scan of
/// `CollectLeft` hash joins. Build-side scans are never touched (q72 guard).
///
/// Runs AFTER `StarSchemaReorderRule` and DataFusion's own `EnforceDistribution`
/// (single-node path in `query_handler`). After bumping probe scans to N output
/// partitions it re-runs `EnforceDistribution` so the upward operator chain
/// (e.g. the final aggregate) gets the exchanges it needs and results stay
/// correct.
#[derive(Debug, Default)]
pub struct ParallelProbeScanRule;

impl ParallelProbeScanRule {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for ParallelProbeScanRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let n = config.execution.target_partitions;
        if n <= 1 {
            return Ok(plan);
        }
        // Single source of truth for the q72 guard: the unit-tested
        // probe-side discrimination decides which leaves may be bumped.
        let probe_leaves = collect_probe_side_leaves(&plan);
        let bumpable: Vec<Arc<dyn ExecutionPlan>> = probe_leaves
            .into_iter()
            .filter(|leaf| {
                leaf.downcast_ref::<IcebergScanExec>().is_some()
                    && leaf.output_partitioning().partition_count() == 1
            })
            .collect();
        if bumpable.is_empty() {
            return Ok(plan);
        }
        let rewritten = bump_scans(&plan, &bumpable, n)?;
        debug!(
            target_partitions = n,
            "ParallelProbeScanRule bumped probe-side scan(s); re-running EnforceDistribution"
        );
        // Re-run EnforceDistribution so the partition count propagates upward
        // with correct exchanges (the final aggregate must still see a coalesced
        // / hash-partitioned input). Only probe-side scans changed, so a
        // CollectLeft build is never coalesced -> no q72-class regression.
        EnforceDistribution::new().optimize(rewritten, config)
    }

    fn name(&self) -> &str {
        "ParallelProbeScanRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Rebuild the plan, replacing each scan in `targets` (matched by pointer
/// identity) with a copy bumped to `n` output partitions. `targets` comes from
/// [`collect_probe_side_leaves`] filtered to single-partition `IcebergScanExec`,
/// so a build-side scan can never be a target.
fn bump_scans(
    plan: &Arc<dyn ExecutionPlan>,
    targets: &[Arc<dyn ExecutionPlan>],
    n: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let transformed = Arc::clone(plan).transform_up(|node| {
        if targets.iter().any(|t| Arc::ptr_eq(t, &node)) {
            // Identified above as a single-partition probe-side IcebergScanExec.
            let scan = node
                .downcast_ref::<IcebergScanExec>()
                .expect("target is an IcebergScanExec");
            let bumped: Arc<dyn ExecutionPlan> =
                Arc::new(scan.clone().with_target_partitions(n));
            Ok(Transformed::yes(bumped))
        } else {
            Ok(Transformed::no(node))
        }
    })?;
    Ok(transformed.data)
}

/// Collect the leaf execution plans (no children) that sit on a pure PROBE
/// path: a leaf is included iff, on its path from the root, it is never reached
/// by descending into the build (left) input of a `CollectLeft` hash join (nor
/// into either input of any other join type, which we conservatively skip).
///
/// This is the q72 regression guard expressed as a pure function: build-side
/// leaves are NEVER returned, so the rule can never bump a build-side scan.
pub(crate) fn collect_probe_side_leaves(
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut out = Vec::new();
    walk(plan, false, &mut out);
    out
}

fn walk(
    node: &Arc<dyn ExecutionPlan>,
    build_tainted: bool,
    out: &mut Vec<Arc<dyn ExecutionPlan>>,
) {
    let children = node.children();
    if children.is_empty() {
        if !build_tainted {
            out.push(Arc::clone(node));
        }
        return;
    }

    if let Some(hj) = node.downcast_ref::<HashJoinExec>() {
        if *hj.partition_mode() == PartitionMode::CollectLeft {
            // Left = build side: taint it. Right = probe side: inherit.
            walk(hj.left(), true, out);
            walk(hj.right(), build_tainted, out);
            return;
        }
        // Non-CollectLeft join (e.g. Partitioned): out of scope, taint both
        // sides so we never parallelize a scan under it.
        for child in &children {
            walk(child, true, out);
        }
        return;
    }

    for child in &children {
        walk(child, build_tainted, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    fn leaf() -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema(), vec![]).unwrap())
    }

    fn collect_left_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let ls = left.schema();
        let rs = right.schema();
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", &ls).unwrap(),
            datafusion::physical_expr::expressions::col("id", &rs).unwrap(),
        )];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn partitioned_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let ls = left.schema();
        let rs = right.schema();
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", &ls).unwrap(),
            datafusion::physical_expr::expressions::col("id", &rs).unwrap(),
        )];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    #[test]
    fn collect_left_join_returns_only_the_probe_leaf_not_the_build() {
        let build = leaf();
        let probe = leaf();
        let join = collect_left_join(Arc::clone(&build), Arc::clone(&probe));

        let leaves = collect_probe_side_leaves(&join);

        assert_eq!(leaves.len(), 1, "exactly one probe-side leaf expected");
        assert!(
            Arc::ptr_eq(&leaves[0], &probe),
            "the returned leaf must be the probe (right) input, never the build (left)"
        );
    }

    #[test]
    fn nested_collect_left_joins_collect_only_the_deepest_fact_probe() {
        // SSB shape: dimA ⋈ (dimB ⋈ fact), all CollectLeft. The fact table is
        // the deepest right child; every dimension is a build (left) input and
        // MUST be excluded (the q72 regression guard).
        let dim_a = leaf();
        let dim_b = leaf();
        let fact = leaf();
        let inner = collect_left_join(Arc::clone(&dim_b), Arc::clone(&fact));
        let outer = collect_left_join(Arc::clone(&dim_a), inner);

        let leaves = collect_probe_side_leaves(&outer);

        assert_eq!(leaves.len(), 1, "only the fact probe should be collected");
        assert!(Arc::ptr_eq(&leaves[0], &fact), "must be the fact table");
    }

    #[test]
    fn partitioned_join_yields_no_probe_leaves() {
        // Non-CollectLeft joins are out of scope: never parallelize a scan
        // under them (conservative).
        let l = leaf();
        let r = leaf();
        let join = partitioned_join(l, r);

        assert!(
            collect_probe_side_leaves(&join).is_empty(),
            "Partitioned-mode joins must yield no parallelizable leaves"
        );
    }
}
