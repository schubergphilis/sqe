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
//! safe, regression-free win is: parallelize any scan that is NOT on the build
//! (left) side of a `CollectLeft` join -- the probe side of such joins, and
//! join-free scans (which have no build side to coalesce). Build-side scans are
//! never touched.
//!
//! The load-bearing invariant -- "never parallelize a build-side scan" -- is the
//! q72 regression guard, and it is unit-tested directly via
//! [`collect_probe_side_leaves`].

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use sqe_catalog::iceberg_scan::IcebergScanExec;
use tracing::debug;

/// Physical optimizer rule that parallelizes the probe-side Iceberg scan of
/// `CollectLeft` hash joins. Build-side scans are never touched (q72 guard).
///
/// Runs AFTER `StarSchemaReorderRule` and DataFusion's own `EnforceDistribution`
/// (single-node path in `query_handler`). After bumping probe scans to N output
/// partitions it re-runs `EnforceDistribution` THEN `EnforceSorting` (the same
/// order DataFusion's stock pipeline runs them). Re-running only
/// `EnforceDistribution` would leave a stale pre-bump sort below a newly
/// inserted repartition, which re-sorts wide rows for nothing and can OOM;
/// `EnforceSorting` removes it. Finally, if the resulting root is still
/// multi-partition (e.g. a partitioned window function ending in a per-partition
/// TopK), the root is coalesced back to a single partition so result collection
/// does not concatenate N partitions and over-return `N * limit` rows. Last, a
/// global `LIMIT` that `LimitPushdown` stranded as a per-partition `fetch` below
/// the (single-partition) root is re-applied at the root; see
/// [`restore_stranded_global_fetch`]. A fetch the re-run ERASED from the tree
/// entirely (sortless `GROUP BY ... LIMIT`, issue #364) is re-applied from the
/// pre-bump plan; see [`reapply_erased_root_fetch`].
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
        // The pre-bump plan is correct by construction: remember its global row
        // cap so a fetch the re-optimization ERASES (not merely strands) can be
        // re-applied at the root; see [`reapply_erased_root_fetch`].
        let pre_fetch = effective_root_fetch(&plan);
        let rewritten = bump_scans(&plan, &bumpable, n)?;
        debug!(
            target_partitions = n,
            "ParallelProbeScanRule bumped probe-side scan(s); re-running EnforceDistribution"
        );
        // Re-run EnforceDistribution so the partition count propagates upward
        // with correct exchanges (the final aggregate must still see a coalesced
        // / hash-partitioned input). Only probe-side scans changed, so a
        // CollectLeft build is never coalesced -> no q72-class regression.
        let redistributed = EnforceDistribution::new().optimize(rewritten, config)?;
        // Then EnforceSorting, matching DataFusion's stock pipeline order. This
        // drops any sort that EnforceDistribution stranded below a repartition
        // (its ordering is destroyed by the exchange and re-established above it)
        // -- the redundant wide-row sort that OOMs on rollup inputs.
        let resorted = EnforceSorting::new().optimize(redistributed, config)?;
        let single = restore_single_partition_root(resorted);
        let restored = restore_stranded_global_fetch(single);
        Ok(reapply_erased_root_fetch(restored, pre_fetch))
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
            let bumped: Arc<dyn ExecutionPlan> = Arc::new(scan.clone().with_target_partitions(n));
            Ok(Transformed::yes(bumped))
        } else {
            Ok(Transformed::no(node))
        }
    })?;
    Ok(transformed.data)
}

/// Restore a single-partition root so the rest of SQE (result collection
/// concatenates root partitions) does not over-return rows. If the root already
/// has a single output partition it is returned untouched. A root that keeps an
/// output ordering (the common case: a `preserve_partitioning` TopK sort) is
/// merged with `SortPreservingMergeExec`, carrying the root's fetch so a global
/// `ORDER BY ... LIMIT` still caps at `limit` rows; an unordered root is merged
/// with `CoalescePartitionsExec`, also carrying any fetch so a bare `LIMIT`
/// cannot over-return `N * limit` rows.
///
/// Shared with [`crate::parallel_scan::ParallelScanRule`], which has the same
/// post-bump obligation to hand the rest of SQE a single-partition root.
pub(crate) fn restore_single_partition_root(
    plan: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    if plan.output_partitioning().partition_count() <= 1 {
        return plan;
    }
    let ordering = plan.output_ordering().cloned();
    let fetch = plan.fetch();
    match ordering {
        Some(ordering) => Arc::new(SortPreservingMergeExec::new(ordering, plan).with_fetch(fetch)),
        None => Arc::new(CoalescePartitionsExec::new(plan).with_fetch(fetch)),
    }
}

/// Restore a global `LIMIT` that DataFusion's `LimitPushdown` stranded as a
/// per-partition cap during the re-optimization this rule performs.
///
/// `LimitPushdown` runs on the ORIGINAL single-partition plan and pushes a
/// global `ORDER BY ... LIMIT n` into a `fetch` on a mid-plan operator (a
/// `SortExec`, a `LocalLimitExec`, or a `FilterExec`). After the scans are
/// bumped and `EnforceDistribution` + `EnforceSorting` re-run, that operator's
/// output becomes multi-partition, so its `fetch` now caps EACH of `N`
/// partitions at `n` rows (`N * n` total), and the `SortPreservingMergeExec`
/// that `EnforceSorting` inserts above it carries NO fetch.
/// [`restore_single_partition_root`] does not catch this: the root is already a
/// single partition (the fetchless merge), so it is returned untouched.
///
/// Walk down from the root through single-child operators that preserve row
/// COUNT and either preserve row ORDER (`ProjectionExec`, fetchless
/// `SortPreservingMergeExec`) or emit an unordered stream (fetchless
/// `CoalescePartitionsExec`), stopping at the first node that carries a `fetch`.
/// If that node's output is multi-partition and the root itself has no fetch,
/// re-apply the cap at the root. Re-capping is safe precisely because no
/// intervening operator drops rows: for an ordered root the stream reaching the
/// root is in final output order and every global-top-`n` row survived its
/// partition's own top-`n` cap, so truncating the merged stream at `n`
/// reconstructs exactly the rows the original `ORDER BY ... LIMIT` selected; for
/// an unordered root (a `CoalescePartitionsExec`, reached only for a bare
/// `LIMIT`) any `n` rows satisfy the limit. The invariant breaks the moment a
/// `FilterExec`, sort, aggregate, or join sits between the root and the fetch
/// node, which is why the walk stops at any other operator.
///
/// Shared with [`crate::parallel_scan::ParallelScanRule`], which re-optimizes
/// the same way and inherits the same stranding.
pub(crate) fn restore_stranded_global_fetch(
    plan: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    // The root already carries the global cap: nothing was stranded.
    if plan.fetch().is_some() {
        return plan;
    }
    let Some(fetch) = find_stranded_fetch(&plan) else {
        return plan;
    };
    apply_fetch_at_root(plan, fetch)
}

/// Re-apply a global `LIMIT` that the re-optimization ERASED from the tree
/// entirely, so [`restore_stranded_global_fetch`] has nothing to find.
///
/// For `GROUP BY ... LIMIT` with no `ORDER BY`, `LimitPushdown` (on the
/// original single-partition plan) deletes the `GlobalLimitExec` and parks the
/// fetch on the root `CoalescePartitionsExec` (an `AggregateExec` has no
/// `fetch()` to push into). When the rules re-run `EnforceDistribution`, its
/// `remove_dist_changing_operators` strips that coalesce -- discarding the
/// fetch -- and `add_merge_on_top` re-inserts a fetchless one. The cap is gone
/// from the tree, and the plan over-returns every group (clickbench q17
/// returned ~1M rows for `LIMIT 10`; issue #364).
///
/// `pre_fetch` is the pre-bump plan's global cap, captured with
/// [`effective_root_fetch`] BEFORE re-optimizing (the pre-bump plan is correct
/// by construction). If the re-optimized plan's root spine carries any fetch --
/// the cap survived, or a stranded one was already restored -- nothing is done.
/// Otherwise the cap is re-applied at the root: safe for an ordered root (the
/// merged stream is in final output order and no per-partition cap dropped rows
/// below, so truncating at `n` reproduces the original selection) and for an
/// unordered root (a bare `LIMIT`, where any `n` rows satisfy the query).
///
/// Shared with [`crate::parallel_scan::ParallelScanRule`].
pub(crate) fn reapply_erased_root_fetch(
    plan: Arc<dyn ExecutionPlan>,
    pre_fetch: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    let Some(fetch) = pre_fetch else {
        return plan;
    };
    if root_spine_fetch(&plan).is_some() {
        return plan;
    }
    apply_fetch_at_root(plan, fetch)
}

/// The global row cap of a plan whose root spine is trusted (the pre-bump
/// plan): the first fetch on the root spine, provided it caps a
/// single-partition stream (a multi-partition fetch is a per-partition cap,
/// not a global one).
pub(crate) fn effective_root_fetch(root: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    root_spine_fetch(root).and_then(|(fetch, partitions)| (partitions <= 1).then_some(fetch))
}

/// Cap `plan` at `fetch` rows in final output order. A merge root folds the
/// fetch in directly (both merge nodes return `Some` from `with_fetch`); any
/// other root (a bare `LIMIT` with no `ORDER BY`, whose root is unordered) is
/// wrapped in a `GlobalLimitExec`.
fn apply_fetch_at_root(plan: Arc<dyn ExecutionPlan>, fetch: usize) -> Arc<dyn ExecutionPlan> {
    let is_mergeish = plan.downcast_ref::<SortPreservingMergeExec>().is_some()
        || plan.downcast_ref::<CoalescePartitionsExec>().is_some();
    if is_mergeish {
        if let Some(capped) = plan.with_fetch(Some(fetch)) {
            return capped;
        }
    }
    Arc::new(GlobalLimitExec::new(plan, 0, Some(fetch)))
}

/// Walk down from `root` through single-child, row-order- and row-count-
/// preserving operators, returning the `fetch` of the first fetch-bearing node
/// IF that node's output is multi-partition (a stranded per-partition cap). The
/// walk descends only through `ProjectionExec` and fetchless
/// `SortPreservingMergeExec` / `CoalescePartitionsExec`; any other operator
/// (including a fetchless `FilterExec`) ends the walk with no result.
fn find_stranded_fetch(root: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    root_spine_fetch(root).and_then(|(fetch, partitions)| (partitions > 1).then_some(fetch))
}

/// Walk down from `root` through single-child, row-count-preserving operators
/// (see [`is_row_preserving_passthrough`]), returning the first fetch-bearing
/// node's `fetch` together with its output partition count. Any other operator
/// (including a fetchless `FilterExec`) ends the walk with no result.
fn root_spine_fetch(root: &Arc<dyn ExecutionPlan>) -> Option<(usize, usize)> {
    let mut node = Arc::clone(root);
    loop {
        if let Some(fetch) = node.fetch() {
            return Some((fetch, node.output_partitioning().partition_count()));
        }
        if !is_row_preserving_passthrough(&node) {
            return None;
        }
        let children = node.children();
        let next = match children.as_slice() {
            [child] => Arc::clone(child),
            // ScalarSubqueryExec lists its subquery plans as extra children;
            // the pass-through main input is always child 0 and is the only
            // stream that reaches the root.
            [input, ..] if node.downcast_ref::<ScalarSubqueryExec>().is_some() => Arc::clone(input),
            _ => return None,
        };
        node = next;
    }
}

/// Whether `node` passes its input through without dropping rows, so a global
/// cap above it is equivalent to the same cap below it. `ProjectionExec` and
/// `SortPreservingMergeExec` also preserve order; `CoalescePartitionsExec` does
/// not, but it is only walked past when fetchless (its `fetch` would stop the
/// walk first) and an unordered root only ever carries a bare `LIMIT`.
/// `ScalarSubqueryExec` resolves its uncorrelated subqueries once and then
/// streams its main input through 1:1 in order (`CardinalityEffect::Equal`);
/// tpcds q14 strands its TopK fetch below one. Only these node types are safe
/// to walk past when hunting a stranded fetch — notably NOT window operators,
/// which preserve row count but compute values over whatever row set reaches
/// them, so a cap below one is not equivalent to a cap above it.
fn is_row_preserving_passthrough(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.downcast_ref::<ProjectionExec>().is_some()
        || node.downcast_ref::<SortPreservingMergeExec>().is_some()
        || node.downcast_ref::<CoalescePartitionsExec>().is_some()
        || node.downcast_ref::<ScalarSubqueryExec>().is_some()
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

fn walk(node: &Arc<dyn ExecutionPlan>, build_tainted: bool, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
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
    fn join_free_scan_is_parallelizable() {
        // A bare scan with no join above it has no build side to coalesce, so
        // it is safe (and beneficial) to parallelize. Documented as intentional:
        // the rule's guard excludes BUILD-side scans, not join-free ones.
        let scan = leaf();
        let leaves = collect_probe_side_leaves(&scan);
        assert_eq!(leaves.len(), 1);
        assert!(Arc::ptr_eq(&leaves[0], &scan));
    }

    fn repartitioned(n: usize) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::Partitioning;
        Arc::new(RepartitionExec::try_new(leaf(), Partitioning::RoundRobinBatch(n)).unwrap())
    }

    #[test]
    fn ordered_multi_partition_root_becomes_sort_preserving_merge_with_fetch() {
        // q67 shape: a preserve_partitioning TopK sort left as the multi-partition
        // root. restore_single_partition_root must merge it back to one partition
        // via SortPreservingMergeExec while carrying the global LIMIT (fetch).
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::sorts::sort::SortExec;

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            datafusion::physical_expr::expressions::col("id", &schema()).unwrap(),
        )])
        .unwrap();
        let root: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering, repartitioned(8))
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

    #[test]
    fn unordered_multi_partition_root_becomes_coalesce_partitions() {
        let root = repartitioned(8);
        assert!(root.output_partitioning().partition_count() > 1);
        assert!(root.output_ordering().is_none());

        let restored = restore_single_partition_root(root);

        assert_eq!(restored.output_partitioning().partition_count(), 1);
        assert!(
            restored.downcast_ref::<CoalescePartitionsExec>().is_some(),
            "unordered multi-partition root must be coalesced"
        );
    }

    #[test]
    fn single_partition_root_is_not_rewrapped() {
        let root: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(repartitioned(8)));
        assert_eq!(root.output_partitioning().partition_count(), 1);

        let restored = restore_single_partition_root(Arc::clone(&root));

        assert!(
            Arc::ptr_eq(&root, &restored),
            "an already single-partition root must be returned unchanged (no double-wrap)"
        );
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

    // ---- restore_stranded_global_fetch (q10/q14/q51 wrong-results fix) ----

    /// An 8-partition round-robin input, standing in for a bumped probe scan.
    fn repartitioned8() -> Arc<dyn ExecutionPlan> {
        repartitioned(8)
    }

    /// A `FilterExec` over `input`, optionally carrying a per-partition fetch.
    /// The predicate (`id IS NOT NULL`) is irrelevant; the fetch and the
    /// partition count are what the walk inspects.
    fn filter(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_plan::expressions::IsNotNullExpr;
        use datafusion::physical_plan::filter::FilterExec;
        let predicate = Arc::new(IsNotNullExpr::new(col("id", &input.schema()).unwrap()));
        let base: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, input).unwrap());
        // FilterExec exposes fetch only through the trait `with_fetch` (the
        // inherent builder-style one lives on FilterExecBuilder); it always
        // returns Some.
        base.with_fetch(fetch).unwrap()
    }

    /// An identity `ProjectionExec` (fetchless, order- and count-preserving).
    fn identity_projection(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_expr::expressions::col;
        let s = input.schema();
        let exprs = vec![
            (col("id", &s).unwrap(), "id".to_string()),
            (col("val", &s).unwrap(), "val".to_string()),
        ];
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    /// A `SortPreservingMergeExec` on `id`, optionally carrying a fetch. Its
    /// output is always a single partition.
    fn spm(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("id", &input.schema()).unwrap(),
        )])
        .unwrap();
        Arc::new(SortPreservingMergeExec::new(ordering, input).with_fetch(fetch))
    }

    /// A per-partition `SortExec` (preserve_partitioning) carrying a fetch, over
    /// an 8-partition input. Stands in for a stranded fetch on a `SortExec`.
    fn per_partition_sort_with_fetch(fetch: usize) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        use datafusion::physical_plan::sorts::sort::SortExec;
        let input = repartitioned8();
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("id", &input.schema()).unwrap(),
        )])
        .unwrap();
        Arc::new(
            SortExec::new(ordering, input)
                .with_preserve_partitioning(true)
                .with_fetch(Some(fetch)),
        )
    }

    #[test]
    fn stranded_per_partition_fetch_below_spm_projection_is_restored_at_root() {
        // The q51 spine, verbatim from the executed plan:
        //   SortPreservingMergeExec [ordering]      (no fetch, 1 partition)
        //     ProjectionExec                        (8 partitions)
        //       FilterExec: ..., fetch=100          (8 x 100 = 800 rows escape)
        // LimitPushdown pushed the global `ORDER BY ... LIMIT 100` onto the
        // filter while the plan was single-partition; after the scans were bumped
        // the filter caps each of 8 partitions, and the merge above it carries no
        // fetch. The pass must re-apply the cap at the root.
        let spine = spm(
            identity_projection(filter(repartitioned8(), Some(100))),
            None,
        );
        assert_eq!(spine.output_partitioning().partition_count(), 1);
        assert_eq!(spine.fetch(), None, "the fetchless merge root is the bug");

        let restored = restore_stranded_global_fetch(spine);

        assert_eq!(restored.output_partitioning().partition_count(), 1);
        assert_eq!(
            restored.fetch(),
            Some(100),
            "the stranded per-partition LIMIT must be re-applied globally at the root"
        );
    }

    #[test]
    fn stranded_fetch_on_a_sort_exec_is_also_restored() {
        // The bug note lists SortExec as another operator LimitPushdown can strand
        // the fetch on. SortExec surfaces its cap through the trait `fetch()`, so
        // the generic walk catches it too (covers the q10/q14 sort-strand shape).
        let spine = spm(
            identity_projection(per_partition_sort_with_fetch(100)),
            None,
        );
        assert_eq!(spine.fetch(), None);

        let restored = restore_stranded_global_fetch(spine);

        assert_eq!(restored.fetch(), Some(100));
    }

    #[test]
    fn stranded_fetch_below_scalar_subquery_exec_is_restored() {
        // The q14 spine, verbatim from the executed plan:
        //   SortPreservingMergeExec [ordering]      (no fetch, 1 partition)
        //     ScalarSubqueryExec: subqueries=1      (main input streamed 1:1)
        //       SortExec: TopK(fetch=100), preserve_partitioning (8 partitions)
        // ScalarSubqueryExec resolves its subqueries once and passes the main
        // input through unchanged, but it lists the subquery plans as extra
        // children — the walk must descend into child 0, not stop at the
        // multi-child node (q14 returned 749 rows on the rig before this).
        use datafusion::logical_expr::execution_props::ScalarSubqueryResults;
        let scalar_wrap: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
            per_partition_sort_with_fetch(100),
            vec![],
            ScalarSubqueryResults::new(0),
        ));
        let ordering = scalar_wrap
            .output_ordering()
            .expect("preserve_partitioning sort propagates its ordering")
            .clone();
        let spine: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, scalar_wrap));
        assert_eq!(spine.output_partitioning().partition_count(), 1);
        assert_eq!(spine.fetch(), None, "the fetchless merge root is the bug");

        let restored = restore_stranded_global_fetch(spine);

        assert_eq!(
            restored.fetch(),
            Some(100),
            "the fetch stranded below ScalarSubqueryExec must be re-applied at the root"
        );
    }

    #[test]
    fn fetchless_spine_with_no_fetch_below_is_unchanged() {
        // SPM -> Projection -> Repartition, no fetch anywhere: nothing stranded.
        let root = spm(identity_projection(repartitioned8()), None);
        let restored = restore_stranded_global_fetch(Arc::clone(&root));
        assert!(
            Arc::ptr_eq(&root, &restored),
            "with no fetch below, the plan must be returned untouched"
        );
    }

    #[test]
    fn fetch_already_on_root_is_unchanged() {
        // The root already carries the global cap (single-partition merge with
        // fetch): the LIMIT is intact, so the pass is a no-op.
        let root = spm(repartitioned8(), Some(100));
        assert_eq!(root.fetch(), Some(100));
        let restored = restore_stranded_global_fetch(Arc::clone(&root));
        assert!(
            Arc::ptr_eq(&root, &restored),
            "a root that already caps globally must be returned untouched"
        );
    }

    #[test]
    fn fetch_below_a_fetchless_filter_is_not_hoisted() {
        // SPM -> FilterExec(no fetch) -> SortExec(fetch=100). The filter changes
        // row count, so the walk stops at it and the deeper fetch is NOT hoisted
        // (hoisting a limit above a filter would be wrong).
        let root = spm(filter(per_partition_sort_with_fetch(100), None), None);
        let restored = restore_stranded_global_fetch(Arc::clone(&root));
        assert!(
            Arc::ptr_eq(&root, &restored),
            "the walk must stop at a fetchless FilterExec and hoist nothing"
        );
    }

    // ---- reapply_erased_root_fetch (#364: sortless GROUP BY ... LIMIT) ----

    /// A GROUP-BY-only `AggregateExec` (no aggregate expressions) over `input`.
    /// Stands in for the final aggregate of `GROUP BY ... LIMIT` with no ORDER
    /// BY; it has no `fetch()` and is not a row-preserving pass-through, so the
    /// stranded-fetch walk stops at it.
    fn group_by_agg(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };
        let s = input.schema();
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("id", &s).unwrap(), "id".to_string())]);
        Arc::new(
            AggregateExec::try_new(AggregateMode::Single, group_by, vec![], vec![], input, s)
                .unwrap(),
        )
    }

    /// A fetchless `CoalescePartitionsExec` root, as `add_merge_on_top`
    /// re-inserts it after `remove_dist_changing_operators` discarded the
    /// fetch-bearing one.
    fn coalesce(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalescePartitionsExec::new(input).with_fetch(fetch))
    }

    #[test]
    fn effective_root_fetch_reads_the_pre_bump_coalesce_cap() {
        // The pre-bump clickbench q17 spine: CoalescePartitionsExec(fetch=10)
        // over the final aggregate. LimitPushdown deleted the GlobalLimitExec
        // and parked the global cap here; the capture must read it.
        let pre = coalesce(group_by_agg(leaf()), Some(10));
        assert_eq!(effective_root_fetch(&pre), Some(10));
    }

    #[test]
    fn effective_root_fetch_ignores_a_per_partition_cap() {
        // A multi-partition fetch caps each partition, not the query: it must
        // NOT be captured as a global cap (re-applying it at the root of a
        // different plan could truncate a result that was never globally capped).
        let per_partition = per_partition_sort_with_fetch(100);
        assert!(per_partition.output_partitioning().partition_count() > 1);
        assert_eq!(effective_root_fetch(&per_partition), None);
    }

    #[test]
    fn erased_group_by_limit_fetch_is_reapplied_from_the_pre_bump_plan() {
        // The #364 shape after the re-run: a FETCHLESS coalesce root (rebuilt by
        // add_merge_on_top) over the multi-partition final aggregate. The fetch
        // exists NOWHERE in the tree, so restore_stranded_global_fetch finds
        // nothing (its walk also stops at AggregateExec) -- the pre-bump cap
        // must be re-applied at the root.
        let post = coalesce(group_by_agg(repartitioned8()), None);
        assert_eq!(post.output_partitioning().partition_count(), 1);

        let after_stranded = restore_stranded_global_fetch(Arc::clone(&post));
        assert!(
            Arc::ptr_eq(&post, &after_stranded),
            "precondition: the stranded-fetch pass alone cannot see an erased fetch"
        );

        let restored = reapply_erased_root_fetch(after_stranded, Some(10));
        assert_eq!(
            restored.fetch(),
            Some(10),
            "the erased GROUP BY ... LIMIT cap must be re-applied at the root"
        );
        assert_eq!(restored.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn surviving_root_fetch_is_not_double_applied() {
        // The cap survived the re-run (or restore_stranded_global_fetch already
        // re-applied one): the pass must leave the plan untouched, even when the
        // pre-bump fetch differs (the re-optimized tree is the source of truth).
        let root = coalesce(group_by_agg(repartitioned8()), Some(10));
        let restored = reapply_erased_root_fetch(Arc::clone(&root), Some(7));
        assert!(
            Arc::ptr_eq(&root, &restored),
            "a root spine that already carries a fetch must be returned untouched"
        );
    }

    #[test]
    fn no_pre_bump_fetch_means_no_reapply() {
        let root = coalesce(group_by_agg(repartitioned8()), None);
        let restored = reapply_erased_root_fetch(Arc::clone(&root), None);
        assert!(
            Arc::ptr_eq(&root, &restored),
            "with no pre-bump cap there is nothing to re-apply"
        );
    }
}
