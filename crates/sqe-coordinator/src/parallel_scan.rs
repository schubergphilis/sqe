//! Physical optimizer rule: parallelize single-node Iceberg scans by giving
//! each qualifying scan N output partitions, then letting `EnforceDistribution`
//! place the exchanges the new partition counts require.
//!
//! ## Why (issue #131 follow-up)
//!
//! `IcebergScanExec` defaults to one output partition. Auto-wiring
//! `target_partitions` at the table-provider level made the scan advertise
//! `Partitioning::UnknownPartitioning(N)`, which `EnforceDistribution` could not
//! use to satisfy a downstream `HashJoinExec`: it fell back to `CollectLeft` and
//! inserted a `CoalescePartitionsExec` immediately above the scan, gathering the
//! N streams back into one and fragmenting the hash build. tpcds q72 regressed
//! 5-6x. See `table_provider.rs` and issue #131.
//!
//! ## Approach
//!
//! This rule runs AFTER `create_physical_plan` (so after DataFusion's own
//! `EnforceDistribution`). Because an `IcebergScanExec` is always a single
//! partition when `EnforceDistribution` first runs, a `Partitioned` hash join
//! ends up directly over its scan inputs with no repartition between them (an
//! empirical fact, verified in the tests). So placing a `RepartitionExec(Hash)`
//! per scan by hand cannot keep both join inputs at the same partition count:
//! bump only the fact side and the join sees N vs 1, which is invalid.
//!
//! Instead the rule bumps every qualifying scan to `RoundRobinBatch(N)` and
//! re-runs `EnforceDistribution` once. Given a `Partitioned` join with one input
//! now at N and the other at 1, `EnforceDistribution` inserts
//! `RepartitionExec(Hash(key), N)` above BOTH inputs, so partition counts match,
//! the join stays `Partitioned` (re-running distribution does not re-pick join
//! modes, that is `JoinSelection`), and no `CoalescePartitionsExec` lands above
//! a scan. Aggregates get their `Hash` / coalesce before the final merge, and a
//! multi-partition root is coalesced by `execute_stream`. This is the mechanism
//! the sibling `ParallelProbeScanRule` (#235) already ships and that q72 was
//! validated against.
//!
//! ## The one load-bearing guard
//!
//! A scan on the build (left) side of a `CollectLeft` join is NEVER bumped.
//! `EnforceDistribution` requires that side to be a single partition, so bumping
//! it re-creates the exact `CoalescePartitionsExec`-above-scan shape that q72
//! regressed on. The guard is expressed generically through
//! [`ExecutionPlan::required_input_distribution`]: a child reached via a
//! `SinglePartition` requirement (a `CollectLeft` build side, a global sort, a
//! global limit, a final aggregate over a collapsed single-partition input) or a
//! required input ordering is tainted and left serial. `Partitioned`-join sides
//! are deliberately NOT tainted: those are the scans this rule exists to cover
//! (this is where it goes beyond #235, which only parallelizes `CollectLeft`
//! probes).

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::Distribution;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use sqe_catalog::iceberg_scan::IcebergScanExec;
use tracing::debug;

/// Physical optimizer rule that parallelizes single-node Iceberg scans. Gated
/// by `query.parallel_scan`; `byte_threshold` reuses `distribution_threshold`.
#[derive(Debug)]
pub struct ParallelScanRule {
    byte_threshold: usize,
}

impl ParallelScanRule {
    pub fn new(byte_threshold: usize) -> Self {
        Self { byte_threshold }
    }
}

impl PhysicalOptimizerRule for ParallelScanRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let n = config.execution.target_partitions;
        if n <= 1 {
            return Ok(plan);
        }
        // Single source of truth for the q72 guard: the unit-tested taint walk
        // decides which leaves may be bumped (build-side scans never appear).
        let leaves = collect_non_build_leaves(&plan);
        let bumpable: Vec<Arc<dyn ExecutionPlan>> = leaves
            .into_iter()
            .filter(|leaf| self.is_bumpable_scan(leaf))
            .collect();
        if bumpable.is_empty() {
            return Ok(plan);
        }
        let rewritten = bump_scans(&plan, &bumpable, n)?;
        debug!(
            target_partitions = n,
            count = bumpable.len(),
            "ParallelScanRule bumped scan(s); re-running EnforceDistribution"
        );
        // Re-run EnforceDistribution so the bumped partition counts get the
        // exchanges they need (Hash repartition on both sides of a Partitioned
        // join, coalesce before a final aggregate). Only non-build scans changed,
        // so no CollectLeft build side is coalesced: no q72-class regression.
        EnforceDistribution::new().optimize(rewritten, config)
    }

    fn name(&self) -> &str {
        "ParallelScanRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl ParallelScanRule {
    /// A leaf is bumpable when it is a single-partition `IcebergScanExec` whose
    /// cached manifest byte size reaches the threshold. Unknown size counts as
    /// below threshold (conservative): the pass never parallelizes a scan it
    /// cannot size. A zero threshold disables the size gate.
    fn is_bumpable_scan(&self, leaf: &Arc<dyn ExecutionPlan>) -> bool {
        let Some(scan) = leaf.downcast_ref::<IcebergScanExec>() else {
            return false;
        };
        if scan.properties().output_partitioning().partition_count() != 1 {
            return false;
        }
        if self.byte_threshold == 0 {
            return true;
        }
        match scan.partition_statistics(None) {
            Ok(stats) => stats
                .total_byte_size
                .get_value()
                .is_some_and(|b| *b >= self.byte_threshold),
            Err(_) => false,
        }
    }
}

/// Rebuild the plan, replacing each scan in `targets` (matched by pointer
/// identity) with a copy bumped to `n` output partitions. `targets` come from
/// [`collect_non_build_leaves`] filtered to single-partition `IcebergScanExec`,
/// so a build-side scan can never be a target.
fn bump_scans(
    plan: &Arc<dyn ExecutionPlan>,
    targets: &[Arc<dyn ExecutionPlan>],
    n: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let transformed = Arc::clone(plan).transform_up(|node| {
        if targets.iter().any(|t| Arc::ptr_eq(t, &node)) {
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

/// Collect the leaf plans that are safe to parallelize: a leaf is included
/// unless, on its path from the root, it is reached through an operator that
/// requires that child to be a single partition or to arrive in a specific
/// order. Build sides of `CollectLeft` joins, global sorts, global limits, and
/// final aggregates over collapsed single-partition inputs all taint their
/// subtree; `Partitioned`-join and pipeline (filter/projection) branches do not.
///
/// This is the q72 regression guard as a pure function over the plan tree, so it
/// is unit-tested directly with stand-in leaves (the sibling `parallel_probe_scan`
/// rule tests its guard the same way).
pub(crate) fn collect_non_build_leaves(
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut out = Vec::new();
    walk(plan, false, &mut out);
    out
}

fn walk(node: &Arc<dyn ExecutionPlan>, tainted: bool, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    let children = node.children();
    if children.is_empty() {
        if !tainted {
            out.push(Arc::clone(node));
        }
        return;
    }
    let reqs = node.required_input_distribution();
    for (idx, child) in children.iter().enumerate() {
        let child_tainted = tainted
            || matches!(reqs.get(idx), Some(Distribution::SinglePartition))
            || requires_ordering(node, idx);
        walk(child, child_tainted, out);
    }
}

/// Whether `parent` requires its child `idx` to arrive in a specific order.
/// Bumping a scan under such an operator (e.g. `SortPreservingMergeExec`) would
/// scramble the per-partition ordering it assumes, so those scans stay serial.
fn requires_ordering(parent: &Arc<dyn ExecutionPlan>, idx: usize) -> bool {
    parent
        .required_input_ordering()
        .into_iter()
        .nth(idx)
        .flatten()
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, true),
        ]))
    }

    // A childless stand-in for a scan leaf. The taint walk is pure over the plan
    // tree, so a `LazyMemoryExec` exercises the q72 guard without a live Iceberg
    // `Table` (mirrors the sibling `parallel_probe_scan` tests). The rule's
    // `is_bumpable_scan` filter (IcebergScanExec + byte threshold) is applied
    // separately in `optimize`.
    fn leaf() -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema(), vec![]).unwrap())
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        mode: PartitionMode,
    ) -> Arc<dyn ExecutionPlan> {
        let ls = left.schema();
        let rs = right.schema();
        let on = vec![(col("id", &ls).unwrap(), col("id", &rs).unwrap())];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                mode,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn filter(child: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let predicate = datafusion::physical_plan::expressions::IsNotNullExpr::new(
            col("val", &child.schema()).unwrap(),
        );
        Arc::new(FilterExec::try_new(Arc::new(predicate), child).unwrap())
    }

    fn ptr_in(leaves: &[Arc<dyn ExecutionPlan>], target: &Arc<dyn ExecutionPlan>) -> bool {
        leaves.iter().any(|l| Arc::ptr_eq(l, target))
    }

    // Tasks 3.1 / 3.2: both inputs of a `Partitioned` join are collected, so the
    // fact scan is bumped. Re-running `EnforceDistribution` then repartitions
    // both sides to `Hash(N)` (see `enforce_distribution_reconciles_partitioned_join`),
    // which keeps the join `Partitioned` and inserts no `CoalescePartitionsExec`
    // above the scan.
    #[test]
    fn partitioned_join_both_sides_collected() {
        let l = leaf();
        let r = leaf();
        let j = join(Arc::clone(&l), Arc::clone(&r), PartitionMode::Partitioned);
        let leaves = collect_non_build_leaves(&j);
        assert!(ptr_in(&leaves, &l), "left Partitioned-join input is bumpable");
        assert!(ptr_in(&leaves, &r), "right Partitioned-join input is bumpable");
    }

    // Task 3.4 / q72 guard: the build (left) side of a `CollectLeft` join is
    // excluded; the probe (right) side is included.
    #[test]
    fn collect_left_excludes_build_includes_probe() {
        let build = leaf();
        let probe = leaf();
        let j = join(Arc::clone(&build), Arc::clone(&probe), PartitionMode::CollectLeft);
        let leaves = collect_non_build_leaves(&j);
        assert!(!ptr_in(&leaves, &build), "CollectLeft build side must never be bumped");
        assert!(ptr_in(&leaves, &probe), "CollectLeft probe side is bumpable");
    }

    // Nested SSB shape: dimA join (dimB join fact), all CollectLeft. Only the
    // fact probe is collectable; every dimension build side is excluded.
    #[test]
    fn nested_collect_left_collects_only_the_fact_probe() {
        let dim_a = leaf();
        let dim_b = leaf();
        let fact = leaf();
        let inner = join(Arc::clone(&dim_b), Arc::clone(&fact), PartitionMode::CollectLeft);
        let outer = join(Arc::clone(&dim_a), inner, PartitionMode::CollectLeft);
        let leaves = collect_non_build_leaves(&outer);
        assert!(ptr_in(&leaves, &fact), "the fact probe is collected");
        assert!(!ptr_in(&leaves, &dim_a), "dimA build excluded");
        assert!(!ptr_in(&leaves, &dim_b), "dimB build excluded");
    }

    // Task 3.3: a filter passes its scan through as bumpable (no distribution
    // requirement).
    #[test]
    fn filter_passes_scan_through() {
        let l = leaf();
        let f = filter(Arc::clone(&l));
        assert!(ptr_in(&collect_non_build_leaves(&f), &l));
    }

    // Task 2.4: a global sort requires a single partition, so its scan is left
    // serial (excluded).
    #[test]
    fn global_sort_excludes_scan() {
        let l = leaf();
        let sort_expr = PhysicalSortExpr::new_default(col("id", &l.schema()).unwrap());
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        // preserve_partitioning defaults to false: a global sort.
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, Arc::clone(&l)));
        assert!(!ptr_in(&collect_non_build_leaves(&sort), &l));
    }

    // An operator that requires an input ordering (SortPreservingMerge) excludes
    // its scan: bumping would scramble the assumed per-partition order.
    #[test]
    fn ordering_requiring_parent_excludes_scan() {
        let l = leaf();
        let sort_expr = PhysicalSortExpr::new_default(col("id", &l.schema()).unwrap());
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let spm: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, Arc::clone(&l)));
        assert!(!ptr_in(&collect_non_build_leaves(&spm), &l));
    }

    // The load-bearing DataFusion behaviour the rule relies on: bump one input of
    // a Partitioned join (fact at 4 partitions) and leave the other at 1, then
    // let EnforceDistribution (inside `create_physical_plan`) reconcile. It must
    // insert `RepartitionExec(Hash)` above BOTH inputs, keep the join
    // `Partitioned`, and add no `CoalescePartitionsExec` above the 1-partition
    // side. This is what makes bump + re-run safe for the fact-dim case (tasks
    // 3.1 / 3.2).
    #[tokio::test]
    async fn enforce_distribution_reconciles_partitioned_join() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::displayable;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let cfg = SessionConfig::new()
            .with_target_partitions(4)
            // Force Partitioned (not CollectLeft) so we exercise the fact-dim
            // hash-partitioned shape.
            .set_usize("datafusion.optimizer.hash_join_single_partition_threshold", 0);
        let ctx = SessionContext::new_with_config(cfg);
        let s = schema();
        let b = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        // fact: 4 partitions (a bumped scan); dim: 1 partition (unbumped).
        ctx.register_table(
            "fact",
            Arc::new(
                MemTable::try_new(
                    s.clone(),
                    vec![vec![b.clone()], vec![b.clone()], vec![b.clone()], vec![b.clone()]],
                )
                .unwrap(),
            ),
        )
        .unwrap();
        ctx.register_table("dim", Arc::new(MemTable::try_new(s.clone(), vec![vec![b]]).unwrap()))
            .unwrap();

        let plan = ctx
            .sql("SELECT fact.val FROM fact JOIN dim ON fact.id = dim.id")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = format!("{}", displayable(plan.as_ref()).indent(true));

        assert!(
            rendered.contains("mode=Partitioned"),
            "join must stay Partitioned, not fall back to CollectLeft:\n{rendered}"
        );
        assert!(
            !rendered.contains("CoalescePartitionsExec"),
            "no CoalescePartitionsExec may be inserted above a scan:\n{rendered}"
        );
        assert_eq!(
            rendered.matches("RepartitionExec: partitioning=Hash").count(),
            2,
            "EnforceDistribution must hash-repartition BOTH join inputs to match \
             partition counts:\n{rendered}"
        );
    }
}
