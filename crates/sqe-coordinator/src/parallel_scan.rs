//! Physical optimizer rule: parallelize a single-node Iceberg scan by giving
//! it N output partitions, choosing partitioning the consuming operator can use
//! so `EnforceDistribution` never inserts a redundant gather.
//!
//! ## Why (issue #131 follow-up)
//!
//! `IcebergScanExec` defaults to one output partition. Auto-wiring
//! `target_partitions` at the table-provider level made the scan advertise
//! `Partitioning::UnknownPartitioning(N)`, which `EnforceDistribution` cannot
//! use to satisfy a downstream `HashJoinExec`: it falls back to `CollectLeft`
//! and inserts a `CoalescePartitionsExec` immediately above the scan, gathering
//! the N streams back into one. tpcds q72 regressed 5-6x. See
//! `table_provider.rs` and issue #131.
//!
//! The lesson is not "do not parallelize" but "do not announce parallelism the
//! optimizer cannot use, and do not parallelize a scan whose consumer requires
//! a single partition". This rule runs AFTER `create_physical_plan` (so after
//! DataFusion's own `EnforceDistribution`) and decides per scan using the one
//! signal DataFusion itself uses: the parent's
//! [`ExecutionPlan::required_input_distribution`].
//!
//! ## What it does
//!
//! For each single-partition [`IcebergScanExec`] whose cached manifest byte
//! size reaches the threshold, it looks at how the parent consumes that input:
//!
//! - Parent is an absorbing exchange (`RepartitionExec` / `CoalescePartitionsExec`):
//!   bump the scan to `RoundRobinBatch(N)`. The exchange already re-partitions
//!   or gathers, so nothing else changes. This is the production win: a
//!   `Partitioned` hash join's inputs already carry a `RepartitionExec(Hash)`
//!   inserted by `EnforceDistribution`, and bumping the scan below it only
//!   changes that exchange's input partition count. The join stays
//!   `Partitioned`; no `CoalescePartitionsExec` lands above the scan.
//! - Parent requires `HashPartitioned(keys)` on the scan directly (a
//!   `Partitioned` join with the scan as a direct child, no intervening
//!   exchange): bump the scan to `RoundRobinBatch(N)` and insert an explicit
//!   `RepartitionExec(Hash(keys), N)` between scan and parent. The keys come
//!   straight from the parent's distribution requirement, so no key recovery
//!   guesswork.
//! - Parent requires `SinglePartition` (the build side of a `CollectLeft`
//!   join, a global sort, a global limit, a final/single aggregate) or requires
//!   an input ordering: leave the scan serial. The `CollectLeft` build-side
//!   case is the q72 regression guard, and here it is the same generic signal
//!   the optimizer uses, not a special case.
//! - Parent requires `UnspecifiedDistribution` (filter, projection): bump to
//!   `RoundRobinBatch(N)` with no inserted exchange, but only when the
//!   parallelism will be absorbed above (see `Ctx` below). No absorbing
//!   boundary means the extra partitions would reach the single-partition
//!   output boundary unmerged, so the scan is left serial.
//!
//! ## Ctx: does the parallelism get absorbed?
//!
//! Bumping a scan raises its parent's output partition count, which propagates
//! up until an operator merges or re-partitions it. `Ctx::Free` means some
//! ancestor already absorbs multiple partitions (an exchange was crossed, or
//! the plan root, which `execute_stream` wraps in a `CoalescePartitionsExec`);
//! `Ctx::Blocked` means a `SinglePartition` / ordering requirement lies above
//! with no absorbing exchange between, so raising the count would violate it.
//! A pipeline (`UnspecifiedDistribution`) scan is bumped only under `Ctx::Free`.
//!
//! ## Not handled (deferred)
//!
//! A scan directly under a partial aggregate, or a scan-bound query whose only
//! consumer is a single-partition output with no exchange (a bare
//! `SELECT count(*) ... WHERE`), is left serial: bumping it would need an
//! absorbing boundary this pass does not insert. Those speedups belong to the
//! benchmark-gate phase.

use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::{Distribution, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use sqe_catalog::iceberg_scan::IcebergScanExec;
use tracing::debug;

/// Whether the parallelism produced below `node` is absorbed before the
/// single-partition output boundary. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ctx {
    /// An exchange (or the plan root) will merge / re-partition extra partitions.
    Free,
    /// A `SinglePartition` or ordering requirement lies above with no absorbing
    /// exchange between; extra partitions here would violate it.
    Blocked,
}

/// What to do with a single-partition scan given how its parent consumes it.
#[derive(Debug, Clone)]
enum LeafAction {
    /// Leave the scan at one partition.
    Serial,
    /// Bump the scan to `RoundRobinBatch(N)` with no inserted exchange.
    BumpRoundRobin,
    /// Bump the scan and wrap it in `RepartitionExec(Hash(keys), N)`.
    BumpHash(Vec<Arc<dyn PhysicalExpr>>),
}

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

        // A bare scan as the whole plan: `execute_stream` coalesces a
        // multi-partition root, so parallelism is absorbed (Ctx::Free).
        if plan.children().is_empty() {
            if let Some(bumped) = self.bump_if_qualifying(&plan, n) {
                debug!(target_partitions = n, "ParallelScanRule parallelized a root scan");
                return Ok(bumped);
            }
            return Ok(plan);
        }

        self.rewrite(&plan, Ctx::Free, n)
    }

    fn name(&self) -> &str {
        "ParallelScanRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl ParallelScanRule {
    /// Rebuild `node`, parallelizing any qualifying leaf scan among its direct
    /// children according to how `node` consumes that child, and recursing into
    /// non-leaf children with the propagated [`Ctx`].
    fn rewrite(
        &self,
        node: &Arc<dyn ExecutionPlan>,
        ctx: Ctx,
        n: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let children = node.children();
        if children.is_empty() {
            return Ok(Arc::clone(node));
        }
        let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());
        let mut changed = false;
        for (idx, child) in children.iter().enumerate() {
            if child.children().is_empty() {
                match self.rewrite_leaf(node, idx, ctx, child, n) {
                    Some(replacement) => {
                        new_children.push(replacement);
                        changed = true;
                    }
                    None => new_children.push(Arc::clone(child)),
                }
            } else {
                let child_ctx = child_ctx(node, idx, ctx);
                let rewritten = self.rewrite(child, child_ctx, n)?;
                if !Arc::ptr_eq(&rewritten, child) {
                    changed = true;
                }
                new_children.push(rewritten);
            }
        }
        if changed {
            Arc::clone(node).with_new_children(new_children)
        } else {
            Ok(Arc::clone(node))
        }
    }

    /// Decide and apply the action for a leaf `child` of `parent` at `idx`.
    /// Returns the replacement node, or `None` to leave the child unchanged.
    fn rewrite_leaf(
        &self,
        parent: &Arc<dyn ExecutionPlan>,
        idx: usize,
        parent_ctx: Ctx,
        child: &Arc<dyn ExecutionPlan>,
        n: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let scan = child.downcast_ref::<IcebergScanExec>()?;
        if scan.properties().output_partitioning().partition_count() != 1 || !self.qualifies(scan) {
            return None;
        }
        match classify_child(parent, idx, parent_ctx) {
            LeafAction::Serial => None,
            LeafAction::BumpRoundRobin => {
                Some(Arc::new(scan.clone().with_target_partitions(n)))
            }
            LeafAction::BumpHash(exprs) => {
                let bumped: Arc<dyn ExecutionPlan> =
                    Arc::new(scan.clone().with_target_partitions(n));
                let repart =
                    RepartitionExec::try_new(bumped, Partitioning::Hash(exprs, n)).ok()?;
                Some(Arc::new(repart))
            }
        }
    }

    /// Bump a root-level scan when it qualifies. Root parallelism is absorbed by
    /// the `CoalescePartitionsExec` `execute_stream` adds over a multi-partition
    /// plan, so a bare scan root is always safe to parallelize.
    fn bump_if_qualifying(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        n: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let scan = plan.downcast_ref::<IcebergScanExec>()?;
        if scan.properties().output_partitioning().partition_count() != 1 || !self.qualifies(scan) {
            return None;
        }
        Some(Arc::new(scan.clone().with_target_partitions(n)))
    }

    /// A scan qualifies when its cached manifest byte size reaches the
    /// threshold. Unknown size is treated as below threshold (conservative):
    /// the pass never parallelizes a scan it cannot size. A zero threshold
    /// disables the gate.
    fn qualifies(&self, scan: &IcebergScanExec) -> bool {
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

/// True if `node` merges or re-partitions its input, absorbing whatever
/// partition count arrives from below.
fn is_absorbing_exchange(node: &Arc<dyn ExecutionPlan>) -> bool {
    node.downcast_ref::<RepartitionExec>().is_some()
        || node.downcast_ref::<CoalescePartitionsExec>().is_some()
}

/// The [`Ctx`] to propagate into child `idx` of `parent`.
fn child_ctx(parent: &Arc<dyn ExecutionPlan>, idx: usize, parent_ctx: Ctx) -> Ctx {
    if is_absorbing_exchange(parent) {
        return Ctx::Free;
    }
    if requires_ordering(parent, idx) {
        return Ctx::Blocked;
    }
    match parent.required_input_distribution().into_iter().nth(idx) {
        Some(Distribution::SinglePartition) => Ctx::Blocked,
        // A Hash requirement is satisfied by an exchange within the child
        // subtree (`EnforceDistribution` placed one) or, for a direct leaf, by
        // the explicit repartition this pass inserts; either way parallelism
        // below is absorbed.
        Some(Distribution::HashPartitioned(_)) => Ctx::Free,
        // Unspecified (or missing): the parent passes its input partition count
        // straight up, so the child inherits the parent's absorption state.
        _ => parent_ctx,
    }
}

/// Decide the action for a leaf child `idx` of `parent`, given the parent's
/// absorption state. Pure over the plan tree (no `IcebergScanExec` dependency)
/// so the q72-guard decisions are unit-tested directly.
fn classify_child(parent: &Arc<dyn ExecutionPlan>, idx: usize, parent_ctx: Ctx) -> LeafAction {
    if is_absorbing_exchange(parent) {
        return LeafAction::BumpRoundRobin;
    }
    if requires_ordering(parent, idx) {
        return LeafAction::Serial;
    }
    match parent.required_input_distribution().into_iter().nth(idx) {
        Some(Distribution::SinglePartition) => LeafAction::Serial,
        Some(Distribution::HashPartitioned(exprs)) => {
            if parent_ctx == Ctx::Free {
                LeafAction::BumpHash(exprs)
            } else {
                LeafAction::Serial
            }
        }
        _ => {
            if parent_ctx == Ctx::Free {
                LeafAction::BumpRoundRobin
            } else {
                LeafAction::Serial
            }
        }
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
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_expr::{expressions::Column, LexOrdering, PhysicalSortExpr};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, true),
        ]))
    }

    // A childless stand-in for a scan leaf. The classification functions are
    // pure over the plan tree, so a `LazyMemoryExec` exercises the q72-guard
    // decisions without needing a live Iceberg `Table` (mirrors the sibling
    // `parallel_probe_scan` tests).
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
        // val IS NOT NULL: any always-typed boolean predicate works; we only
        // inspect the plan shape, never execute.
        let predicate = datafusion::physical_plan::expressions::IsNotNullExpr::new(
            col("val", &child.schema()).unwrap(),
        );
        Arc::new(FilterExec::try_new(Arc::new(predicate), child).unwrap())
    }

    // Task 3.2 (and 3.1): the probe input of a `Partitioned` hash join is
    // classified `BumpHash` on the join key. `BumpHash` inserts a
    // `RepartitionExec(Hash)` between scan and join and never rebuilds the join,
    // so no `CoalescePartitionsExec` appears above the scan (3.1) and the join
    // stays `Partitioned`, not `CollectLeft` (3.2).
    #[test]
    fn partitioned_join_probe_is_bump_hash_on_the_join_key() {
        let j = join(leaf(), leaf(), PartitionMode::Partitioned);
        match classify_child(&j, 1, Ctx::Free) {
            LeafAction::BumpHash(exprs) => {
                assert_eq!(exprs.len(), 1, "one join key");
                let c = exprs[0].downcast_ref::<Column>().expect("column key");
                assert_eq!(c.name(), "id", "hash repartition must use the join key");
            }
            other => panic!("expected BumpHash, got {other:?}"),
        }
        // The build (left) side of the same join is also hash-partitioned.
        assert!(matches!(
            classify_child(&j, 0, Ctx::Free),
            LeafAction::BumpHash(_)
        ));
    }

    // Task 3.3: a filter-only parent parallelizes with round-robin and inserts
    // no exchange.
    #[test]
    fn filter_parent_is_bump_round_robin_no_exchange() {
        let f = filter(leaf());
        assert!(matches!(
            classify_child(&f, 0, Ctx::Free),
            LeafAction::BumpRoundRobin
        ));
    }

    // Task 3.4 / q72 guard: the build side of a `CollectLeft` join requires a
    // single partition, so it is left serial and can never be parallelized.
    #[test]
    fn collect_left_build_side_is_serial() {
        let j = join(leaf(), leaf(), PartitionMode::CollectLeft);
        assert!(matches!(
            classify_child(&j, 0, Ctx::Free),
            LeafAction::Serial
        ));
    }

    // The probe side of a `CollectLeft` join has no distribution requirement, so
    // it is parallelizable when the parallelism is absorbed above (Ctx::Free)
    // and left serial when it is not (Ctx::Blocked).
    #[test]
    fn collect_left_probe_side_depends_on_absorption() {
        let j = join(leaf(), leaf(), PartitionMode::CollectLeft);
        assert!(matches!(
            classify_child(&j, 1, Ctx::Free),
            LeafAction::BumpRoundRobin
        ));
        assert!(matches!(
            classify_child(&j, 1, Ctx::Blocked),
            LeafAction::Serial
        ));
    }

    // A pipeline scan whose extra partitions would reach a single-partition
    // boundary unmerged (Ctx::Blocked) is left serial.
    #[test]
    fn unspecified_parent_serial_when_blocked() {
        let f = filter(leaf());
        assert!(matches!(
            classify_child(&f, 0, Ctx::Blocked),
            LeafAction::Serial
        ));
    }

    // An operator that requires an input ordering (e.g. SortPreservingMerge)
    // leaves its child serial: bumping the scan would scramble the assumed
    // per-partition order.
    #[test]
    fn ordering_requiring_parent_is_serial() {
        let child = leaf();
        let sort_expr = PhysicalSortExpr::new_default(col("id", &child.schema()).unwrap());
        let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
        let spm = Arc::new(SortPreservingMergeExec::new(ordering, child));
        let spm: Arc<dyn ExecutionPlan> = spm;
        assert!(matches!(
            classify_child(&spm, 0, Ctx::Free),
            LeafAction::Serial
        ));
    }

    // An absorbing exchange parent bumps its child to round-robin regardless of
    // the incoming ctx: the exchange re-partitions or gathers below it.
    #[test]
    fn exchange_parent_bumps_round_robin() {
        let child = leaf();
        let repart = Arc::new(
            RepartitionExec::try_new(child, Partitioning::RoundRobinBatch(4)).unwrap(),
        );
        let repart: Arc<dyn ExecutionPlan> = repart;
        assert!(matches!(
            classify_child(&repart, 0, Ctx::Blocked),
            LeafAction::BumpRoundRobin
        ));
    }
}
