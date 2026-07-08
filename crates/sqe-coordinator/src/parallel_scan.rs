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
//! re-runs `EnforceDistribution` then `EnforceSorting` (the same order
//! DataFusion's stock pipeline runs them). Given a `Partitioned` join with one
//! input now at N and the other at 1, `EnforceDistribution` inserts
//! `RepartitionExec(Hash(key), N)` above BOTH inputs, so partition counts match,
//! the join stays `Partitioned` (re-running distribution does not re-pick join
//! modes, that is `JoinSelection`), and no `CoalescePartitionsExec` lands above
//! a scan. `EnforceSorting` then removes any sort stranded below a newly
//! inserted repartition (its ordering is destroyed by the exchange and redone
//! above it) so wide rows are not re-sorted for nothing. Aggregates get their
//! `Hash` / coalesce before the final merge; a root that legitimately stays
//! multi-partition is coalesced back to a single partition by
//! [`restore_single_partition_root`], since SQE's result collection concatenates
//! root partitions rather than merging them. This is the mechanism the sibling
//! `ParallelProbeScanRule` (#235) already ships and that q72 was validated
//! against.
//!
//! ## The one load-bearing guard
//!
//! A scan on the build (left) side of a `CollectLeft` hash join is NEVER bumped.
//! `EnforceDistribution` requires that side to be a single partition, so bumping
//! it re-creates the exact `CoalescePartitionsExec`-above-scan shape that q72
//! regressed on. This is the ONLY single-partition requirement the rule cannot
//! repair by re-running the stock pipeline: join-mode selection (`JoinSelection`)
//! is not re-run here, so a `CollectLeft` join stays `CollectLeft` and keeps
//! demanding a one-partition build.
//!
//! Everything else the re-run repairs. `EnforceDistribution` then
//! `EnforceSorting` re-place every exchange and re-erect every ordering the new
//! partition counts require, so a scan under a global sort, a
//! `SortPreservingMergeExec`, a window, or a final aggregate is safe to bump: the
//! merge / sort / coalesce is simply rebuilt ABOVE the inserted exchange. The
//! taint walk therefore keys ONLY on joins, not on
//! [`ExecutionPlan::required_input_distribution`] /
//! [`ExecutionPlan::required_input_ordering`]. Keying on those (as an earlier
//! version did) tainted the child of nearly every plan's root
//! `SortPreservingMergeExec` / global `SortExec` and left `bumpable` empty on
//! every real benchmark query, so the rule never fired. The walk's rules:
//!
//! - `CollectLeft` hash join: taint the build (left) side; inherit on the probe.
//! - `Partitioned` hash join: inherit taint on BOTH sides. These are the scans
//!   this rule exists to unlock (tpcds q72 `inventory`, tpch q9 `lineitem`);
//!   `EnforceDistribution` hash-repartitions both inputs to N and the join stays
//!   `Partitioned`. This is where it goes beyond #235, which only parallelizes
//!   `CollectLeft` probes.
//! - `Auto` hash join: taint BOTH sides. `Auto` advertises
//!   `UnknownPartitioning(N)` (the #131 shape), so a bumped child scan is exactly
//!   what `EnforceDistribution` coalesces above; treat it as unsafe.
//! - Any other join (sort-merge, nested-loop, cross, symmetric-hash,
//!   piecewise-merge): taint both sides (conservative; their build sides carry
//!   single-partition or strict-order requirements this walk does not model).
//! - `UnionExec` and pipeline operators (filter, projection, aggregate, sort,
//!   limit, merge, coalesce): inherit the incoming taint unchanged.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
    SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::ExecutionPlan;
use sqe_catalog::iceberg_scan::IcebergScanExec;
use tracing::debug;

use crate::parallel_probe_scan::{restore_single_partition_root, restore_stranded_global_fetch};

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
        let redistributed = EnforceDistribution::new().optimize(rewritten, config)?;
        // Then EnforceSorting, matching DataFusion's stock pipeline order, so a
        // sort stranded below a newly inserted repartition (its ordering redone
        // above the exchange) is removed instead of re-sorting wide rows and
        // risking OOM. Finally restore a single-partition root: a plan that stays
        // multi-partition to the top (e.g. a partitioned window ending in a
        // per-partition TopK) would otherwise over-return `N * limit` rows,
        // because SQE's result collection concatenates root partitions.
        let resorted = EnforceSorting::new().optimize(redistributed, config)?;
        let single = restore_single_partition_root(resorted);
        Ok(restore_stranded_global_fetch(single))
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
/// unless, on its path from the root, it is reached by descending into the build
/// (left) side of a `CollectLeft` hash join, into either side of an `Auto` hash
/// join, or into either side of any non-hash join. Hash-`Partitioned` join sides,
/// `UnionExec` branches, and pipeline operators (filter, projection, aggregate,
/// sort, limit, merge) pass taint through unchanged, so their scans stay
/// bumpable.
///
/// This is the q72 regression guard as a pure function over the plan tree, so it
/// is unit-tested directly with stand-in leaves (the sibling `parallel_probe_scan`
/// rule tests its guard the same way — but note the two walks treat a
/// `Partitioned` join oppositely, so they are deliberately NOT shared).
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

    if let Some(hj) = node.downcast_ref::<HashJoinExec>() {
        match hj.partition_mode() {
            // Build (left) side is collected to one partition by
            // EnforceDistribution; bumping it re-creates the q72 regression.
            // Probe (right) side inherits the incoming taint.
            PartitionMode::CollectLeft => {
                walk(hj.left(), true, out);
                walk(hj.right(), tainted, out);
            }
            // Both sides are hash-repartitioned to N and the join stays
            // Partitioned: exactly the scans this rule unlocks.
            PartitionMode::Partitioned => {
                walk(hj.left(), tainted, out);
                walk(hj.right(), tainted, out);
            }
            // Auto advertises UnknownPartitioning(N) (the #131 shape); a bumped
            // child is what EnforceDistribution coalesces above. Taint both.
            PartitionMode::Auto => {
                walk(hj.left(), true, out);
                walk(hj.right(), true, out);
            }
        }
        return;
    }

    // Any non-hash join has a single-partition build side (or a strict-order
    // requirement) this walk does not model: taint both inputs conservatively.
    if is_non_hash_join(node) {
        for child in &children {
            walk(child, true, out);
        }
        return;
    }

    // Pipeline operators and UnionExec: row plumbing that EnforceDistribution +
    // EnforceSorting re-establish above the inserted exchange. Inherit taint.
    for child in &children {
        walk(child, tainted, out);
    }
}

/// Whether `node` is a join other than `HashJoinExec` (handled by the caller).
/// Enumerates the join operators DataFusion 54 exports; a scan under any of them
/// is left serial because their build sides carry single-partition or
/// strict-order requirements the taint walk does not model.
fn is_non_hash_join(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.downcast_ref::<CrossJoinExec>().is_some()
        || node.downcast_ref::<NestedLoopJoinExec>().is_some()
        || node.downcast_ref::<PiecewiseMergeJoinExec>().is_some()
        || node.downcast_ref::<SortMergeJoinExec>().is_some()
        || node.downcast_ref::<SymmetricHashJoinExec>().is_some()
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

    // A global (preserve_partitioning=false) SortExec no longer excludes its
    // scan: EnforceSorting rebuilds the sort above the inserted exchange after
    // the bump, so the scan is safe to parallelize. (Inverted from the old
    // ordering-taint behaviour, which kept the rule from ever firing.)
    #[test]
    fn global_sort_no_longer_excludes_scan() {
        let l = leaf();
        let sort_expr = PhysicalSortExpr::new_default(col("id", &l.schema()).unwrap());
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        // preserve_partitioning defaults to false: a global sort.
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, Arc::clone(&l)));
        assert!(
            ptr_in(&collect_non_build_leaves(&sort), &l),
            "a scan under a global sort is now bumpable (ordering re-erected above the exchange)"
        );
    }

    // Test (3): a `SortPreservingMergeExec` root no longer excludes its scan.
    // Nearly every real query roots in an SPM; the old ordering taint made this
    // subtree serial and left `bumpable` empty on every benchmark query.
    #[test]
    fn spm_root_no_longer_excludes_scan() {
        let l = leaf();
        let sort_expr = PhysicalSortExpr::new_default(col("id", &l.schema()).unwrap());
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let spm: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, Arc::clone(&l)));
        assert!(
            ptr_in(&collect_non_build_leaves(&spm), &l),
            "a scan under an SPM root is now bumpable (this is what unlocks the rule)"
        );
    }

    // Test (2), the actual unlock: SPM -> global SortExec -> Partitioned hash
    // join -> two leaves. BOTH leaves must be collected. Before the rewrite the
    // SPM/sort ordering taint propagated all the way down and returned neither
    // (the tpcds q72 `inventory` / `catalog_sales` shape).
    #[test]
    fn spm_over_sort_over_partitioned_join_collects_both_leaves() {
        let l = leaf();
        let r = leaf();
        let j = join(Arc::clone(&l), Arc::clone(&r), PartitionMode::Partitioned);
        let sort_expr = PhysicalSortExpr::new_default(col("id", &j.schema()).unwrap());
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering.clone(), j)); // global sort (preserve=false)
        let spm: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(ordering, sort));

        let leaves = collect_non_build_leaves(&spm);
        assert!(
            ptr_in(&leaves, &l) && ptr_in(&leaves, &r),
            "both Partitioned-join inputs must be bumpable through an SPM+sort cap"
        );
    }

    // Test (4): a non-hash join (CrossJoinExec here) taints both sides, so
    // neither leaf is collected. Same for SortMergeJoin / NestedLoop / etc.
    #[test]
    fn non_hash_join_taints_both_sides() {
        let l = leaf();
        let r = leaf();
        let cj: Arc<dyn ExecutionPlan> = Arc::new(CrossJoinExec::new(Arc::clone(&l), Arc::clone(&r)));
        let leaves = collect_non_build_leaves(&cj);
        assert!(
            !ptr_in(&leaves, &l) && !ptr_in(&leaves, &r),
            "a non-hash join must leave both its scans serial"
        );
    }

    // Test (5): UnionExec is multi-child but NOT a join; it inherits taint, so an
    // untainted union collects both branches.
    #[test]
    fn union_inherits_taint_and_collects_both_branches() {
        use datafusion::physical_plan::union::UnionExec;
        let a = leaf();
        let b = leaf();
        let u = UnionExec::try_new(vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();
        let leaves = collect_non_build_leaves(&u);
        assert!(
            ptr_in(&leaves, &a) && ptr_in(&leaves, &b),
            "UnionExec must inherit taint (untainted -> both branches bumpable)"
        );
    }

    // The rule now shares `parallel_probe_scan`'s single-partition-root restore.
    // An ordered multi-partition root (a `preserve_partitioning` TopK, e.g. a
    // partitioned window ending in a per-partition `SortExec`) is merged back to
    // one partition via `SortPreservingMergeExec` carrying its fetch, so the
    // rewritten plan cannot over-return `N * limit` rows through SQE's
    // partition-concatenating result collection. The rule's own `optimize` tail
    // needs a live `IcebergScanExec` to trigger a bump, so this shared behaviour
    // is exercised here (and in the `parallel_probe_scan` tests) rather than
    // through `optimize` with stand-in leaves.
    #[test]
    fn shared_root_restore_merges_ordered_multi_partition_root_with_fetch() {
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning};

        let repartitioned: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(leaf(), Partitioning::RoundRobinBatch(8)).unwrap());
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("id", &schema()).unwrap())])
                .unwrap();
        let root: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering, repartitioned)
                .with_preserve_partitioning(true)
                .with_fetch(Some(100)),
        );
        assert!(root.output_partitioning().partition_count() > 1);

        let restored = restore_single_partition_root(root);

        assert_eq!(restored.output_partitioning().partition_count(), 1);
        assert!(
            restored.downcast_ref::<SortPreservingMergeExec>().is_some(),
            "ordered multi-partition root must be merged with SortPreservingMergeExec"
        );
        assert_eq!(
            restored.fetch(),
            Some(100),
            "the global ORDER BY ... LIMIT fetch must be carried onto the merge"
        );
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
