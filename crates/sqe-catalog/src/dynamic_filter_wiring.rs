//! Keep the dynamic filters an `IcebergScanExec` holds connected to the
//! operators that update them (#787).
//!
//! DataFusion's post-optimization `FilterPushdown` hands every scan the
//! `DynamicFilterPhysicalExpr` of the operator above it: a hash join's
//! build-side bound, a TopK `SortExec`'s threshold, an aggregate's bound. The
//! scan and the producer share the filter's state, so the producer's
//! `update()` reaches the scan.
//!
//! Rules that run after that pass rebuild producers. `EnsureRequirements` in
//! the parallel-scan rules splits a TopK sort into per-partition sorts, and
//! a new `SortExec` mints a new filter. `StarSchemaReorderRule` rebuilds hash
//! joins with `HashJoinExec::try_new`, which starts without one. The scan
//! keeps the old filter, which nothing updates any more. It stays at the
//! `lit(true)` placeholder, so it prunes nothing. Worse, the scan waits the full
//! `runtime_filter_wait_ms` at stream open for it to seal: on a 150M-row
//! `ORDER BY price DESC LIMIT 10` that was 819 ms of waiting and zero rows
//! filtered.
//!
//! [`settle_dynamic_filters`] fixes the wiring after the last rule that can
//! rebuild a producer:
//!
//! 1. Collect the `expression_id` of every filter the plan still produces.
//! 2. Drop each scan filter whose id is not among them (an orphan).
//! 3. If any orphan was dropped, re-run the post-optimization
//!    `FilterPushdown`, so the new producers push their filters. That pass
//!    re-pushes filters a scan already holds (a `SortExec` pushes its TopK
//!    filter on every run); the scan skips those by id. A hash join that
//!    still has its filter does not push again.
//! 4. Mark the filters made by a single-input producer (TopK, aggregate) as
//!    progressive. Those tighten as this scan's own rows arrive, so the scan
//!    neither waits for them nor freezes their first snapshot.
//!
//! Not covered: `DistributedScanExec`, which ships its filters to workers.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::iceberg_scan::IcebergScanExec;

/// Ids of the dynamic filters the plan produces, and the subset made by a
/// single-input producer.
#[derive(Debug, Default)]
struct Producers {
    produced: HashSet<u64>,
    progressive: HashSet<u64>,
}

fn producers(plan: &Arc<dyn ExecutionPlan>) -> Result<Producers> {
    let mut out = Producers::default();
    plan.apply(|node| {
        // A hash join's filter seals once its build side (the other child)
        // is read. A single-input producer's filter tracks its only input,
        // which is the scan the filter is pushed into.
        let single_input = node.children().len() == 1;
        for expr in node.dynamic_expressions_produced() {
            if let Some(id) = expr.expression_id() {
                out.produced.insert(id);
                if single_input {
                    out.progressive.insert(id);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(out)
}

/// Drop orphaned scan filters and mark progressive ones. Returns the plan
/// and whether any orphan was dropped.
fn settle_scans(
    plan: Arc<dyn ExecutionPlan>,
    producers: &Producers,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    let mut dropped = false;
    let keep = |f: &Arc<dyn PhysicalExpr>| {
        f.expression_id()
            .is_none_or(|id| producers.produced.contains(&id))
    };
    let settled = plan.transform_up(|node| {
        let Some(scan) = node.downcast_ref::<IcebergScanExec>() else {
            return Ok(Transformed::no(node));
        };
        let filters = scan.pushed_down_filters();
        let kept = filters.iter().filter(|f| keep(f)).count();
        let progressive: Vec<u64> = filters
            .iter()
            .filter(|f| keep(f))
            .filter_map(|f| f.expression_id())
            .filter(|id| producers.progressive.contains(id))
            .collect();
        if kept == filters.len() && progressive == scan.progressive_filter_ids() {
            return Ok(Transformed::no(node));
        }
        dropped |= kept < filters.len();
        let settled: Arc<dyn ExecutionPlan> =
            Arc::new(scan.clone_with_settled_filters(keep, progressive));
        Ok(Transformed::yes(settled))
    })?;
    Ok((settled.data, dropped))
}

/// Re-attach every `IcebergScanExec` dynamic filter to a live producer; see
/// the module docs.
///
/// `config` enables step 3. Pass `None` from a rule that removes a
/// producer's filter on purpose (`StripSelfJoinDynamicFiltersRule`):
/// re-pushing would hand the join a fresh filter and undo it.
pub fn settle_dynamic_filters(
    plan: Arc<dyn ExecutionPlan>,
    config: Option<&ConfigOptions>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (plan, dropped) = settle_scans(Arc::clone(&plan), &producers(&plan)?)?;
    let Some(config) = config.filter(|_| dropped) else {
        return Ok(plan);
    };
    let repushed = FilterPushdown::new_post_optimization().optimize(plan, config)?;
    // The re-push added filters, unmarked: mark them. Nothing is orphaned now.
    Ok(settle_scans(Arc::clone(&repushed), &producers(&repushed)?)?.0)
}

/// [`settle_dynamic_filters`] as a physical optimizer rule. Runs after every
/// rule that can rebuild a hash join or a TopK sort.
#[derive(Debug, Default)]
pub struct SettleDynamicFiltersRule;

impl SettleDynamicFiltersRule {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for SettleDynamicFiltersRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        settle_dynamic_filters(plan, Some(config))
    }

    fn name(&self) -> &str {
        "SettleDynamicFiltersRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::{col, lit, DynamicFilterPhysicalExpr};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::sorts::sort::SortExec;

    use crate::parallel_probe_scan::ParallelProbeScanRule;
    use crate::parallel_scan::ParallelScanRule;

    fn config() -> ConfigOptions {
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 4;
        config
    }

    async fn iceberg_scan(dir: &std::path::Path) -> Arc<dyn ExecutionPlan> {
        let table = crate::test_fixtures::fake_iceberg_table(dir, false).await;
        let schema =
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).unwrap();
        Arc::new(IcebergScanExec::new(table, Arc::new(schema), None, None))
    }

    fn scans(plan: &Arc<dyn ExecutionPlan>) -> Vec<IcebergScanExec> {
        let mut out = Vec::new();
        plan.apply(|node| {
            if let Some(scan) = node.downcast_ref::<IcebergScanExec>() {
                out.push(scan.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        out
    }

    fn produced(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut out = Vec::new();
        plan.apply(|node| {
            out.extend(node.dynamic_expressions_produced());
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        out
    }

    fn ids(filters: &[Arc<dyn PhysicalExpr>]) -> Vec<u64> {
        filters.iter().filter_map(|f| f.expression_id()).collect()
    }

    /// `ORDER BY id DESC LIMIT 10` over an Iceberg scan, with the TopK filter
    /// already pushed into the scan the way DataFusion's own pass leaves it.
    async fn topk_over_scan(dir: &std::path::Path) -> Arc<dyn ExecutionPlan> {
        let scan = iceberg_scan(dir).await;
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("id", &scan.schema()).unwrap(),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .unwrap();
        let sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, scan).with_fetch(Some(10)));
        FilterPushdown::new_post_optimization()
            .optimize(sort, &config())
            .unwrap()
    }

    /// #787: the parallel-scan rule re-runs `EnsureRequirements`, which
    /// splits the TopK into per-partition sorts with a NEW filter. The scan
    /// kept the old one, which nothing updates. Settling hands the scan the
    /// live filter, once, marked progressive.
    #[tokio::test]
    async fn topk_dynamic_filter_follows_the_rebuilt_sort() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let plan = topk_over_scan(dir.path()).await;
        assert_eq!(scans(&plan)[0].pushed_down_filters().len(), 1);

        let bumped = ParallelScanRule::new(0).optimize(plan, &config).unwrap();
        let live = ids(&produced(&bumped));
        assert_eq!(live.len(), 1, "one TopK filter after the bump");
        let held = ids(scans(&bumped)[0].pushed_down_filters());
        assert!(
            held.iter().all(|id| !live.contains(id)),
            "precondition: the bump orphans the scan's TopK filter"
        );

        let settled = SettleDynamicFiltersRule::new()
            .optimize(bumped, &config)
            .unwrap();
        let producers = produced(&settled);
        let scan = &scans(&settled)[0];
        assert_eq!(ids(scan.pushed_down_filters()), ids(&producers));
        assert_eq!(scan.progressive_filter_ids(), ids(&producers).as_slice());

        // The sort's updates now reach the scan.
        let sort_filter = producers[0]
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap();
        sort_filter.update(lit(false)).unwrap();
        assert!(scan.pushed_down_filters()[0].snapshot_generation() > 1);
    }

    /// Re-running the post pass re-pushes the TopK filter the scan holds;
    /// the scan keeps one copy.
    #[tokio::test]
    async fn repeated_pushdown_does_not_duplicate_a_scan_filter() {
        let dir = tempfile::tempdir().unwrap();
        let plan = topk_over_scan(dir.path()).await;
        let again = FilterPushdown::new_post_optimization()
            .optimize(plan, &config())
            .unwrap();
        assert_eq!(scans(&again)[0].pushed_down_filters().len(), 1);
    }

    fn collect_left_join(
        build: Arc<dyn ExecutionPlan>,
        probe: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(
            col("id", &build.schema()).unwrap(),
            col("id", &probe.schema()).unwrap(),
        )];
        Arc::new(
            HashJoinExec::try_new(
                build,
                probe,
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

    fn build_side() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    /// A join filter that survives the probe bump keeps its id, is not
    /// marked progressive (it seals once, so the scan still waits for it),
    /// and is not duplicated by the settle's re-push.
    #[tokio::test]
    async fn join_dynamic_filter_survives_probe_bump_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let join = collect_left_join(build_side(), iceberg_scan(dir.path()).await);
        let plan = FilterPushdown::new_post_optimization()
            .optimize(join, &config)
            .unwrap();
        let before = ids(scans(&plan)[0].pushed_down_filters());
        assert_eq!(before.len(), 1);

        let bumped = ParallelProbeScanRule::new()
            .optimize(plan, &config)
            .unwrap();
        let settled = SettleDynamicFiltersRule::new()
            .optimize(bumped, &config)
            .unwrap();
        let scan = &scans(&settled)[0];
        assert_eq!(ids(scan.pushed_down_filters()), before);
        assert!(scan.progressive_filter_ids().is_empty());
    }

    /// A join rebuilt without its filter (`HashJoinExec::try_new`, as
    /// `StarSchemaReorderRule` and the self-join strip do) orphans the scan's
    /// copy. Without a config the settle only drops it; with one it re-pushes,
    /// so the rebuilt join gets a live filter.
    #[tokio::test]
    async fn rebuilt_join_orphan_is_dropped_or_rewired() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let join = collect_left_join(build_side(), iceberg_scan(dir.path()).await);
        let plan = FilterPushdown::new_post_optimization()
            .optimize(join, &config)
            .unwrap();
        let hj = plan.downcast_ref::<HashJoinExec>().unwrap();
        let rebuilt = collect_left_join(Arc::clone(hj.left()), Arc::clone(hj.right()));
        assert!(produced(&rebuilt).is_empty());

        let dropped = settle_dynamic_filters(Arc::clone(&rebuilt), None).unwrap();
        assert!(scans(&dropped)[0].pushed_down_filters().is_empty());

        let rewired = settle_dynamic_filters(rebuilt, Some(&config)).unwrap();
        let live = ids(&produced(&rewired));
        assert_eq!(live.len(), 1, "the re-push gave the join a filter");
        assert_eq!(ids(scans(&rewired)[0].pushed_down_filters()), live);
    }
}
