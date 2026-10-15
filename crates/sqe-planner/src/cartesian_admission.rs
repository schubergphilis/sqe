//! Physical optimizer rule that refuses cartesian joins whose estimated work
//! exceeds `query.max_cartesian_rows` (#755).
//!
//! Workers only scan, so every join runs on the coordinator, which is shared
//! by every tenant. A `CrossJoinExec`, or a `NestedLoopJoinExec` (a join with
//! no equi-key, e.g. `ON a.x < b.y`), evaluates every pair of input rows.
//! Memory is already bounded: the build side goes through per-query operator
//! admission (!1236). CPU is not bounded: a 10^6 x 10^6 cross join with a
//! tiny build side holds a core until `query.timeout_secs`. This rule refuses
//! such a plan before it runs.
//!
//! The work estimate is the product of the two inputs' `num_rows` statistics.
//! For a nested-loop join that product is what the join filter is evaluated
//! over, whatever the output size. Exact and inexact estimates both count:
//! an Iceberg scan reports exact counts from the snapshot summary, and a
//! filter above it makes them inexact. A side with absent statistics admits
//! the join (warn-logged): refusing would break every query over a source
//! that cannot count.
//!
//! The estimates come from snapshot totals, which `SHOW STATS` already
//! reports to anyone who can load the table, so refusing on them reveals
//! nothing new. The error still carries only the configured cap, never the
//! per-side estimates: those go to the warn log.
//!
//! Not gated here: equi-joins with skewed keys, large aggregations, and the
//! write path. Those are bounded by memory admission and the query timeout.

use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{CrossJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion::physical_plan::ExecutionPlan;
use tracing::{debug, warn};

/// Refuse `CrossJoinExec` / `NestedLoopJoinExec` nodes whose input row
/// product exceeds `max_rows`. `0` disables the gate.
#[derive(Debug, Clone)]
pub struct CartesianAdmissionRule {
    max_rows: u64,
}

impl CartesianAdmissionRule {
    pub fn new(max_rows: u64) -> Self {
        Self { max_rows }
    }
}

/// Row-count estimate of one join input. `None` when the statistics are
/// absent or cannot be computed.
fn input_rows(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let stats = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .ok()?;
    match stats.num_rows {
        Precision::Exact(n) | Precision::Inexact(n) => Some(n),
        Precision::Absent => None,
    }
}

impl PhysicalOptimizerRule for CartesianAdmissionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.max_rows == 0 {
            return Ok(plan);
        }
        let mut refused = false;
        plan.apply(|node| {
            let (kind, left, right) = if let Some(j) = node.downcast_ref::<CrossJoinExec>() {
                ("CrossJoinExec", j.left(), j.right())
            } else if let Some(j) = node.downcast_ref::<NestedLoopJoinExec>() {
                ("NestedLoopJoinExec", j.left(), j.right())
            } else {
                return Ok(TreeNodeRecursion::Continue);
            };
            match (input_rows(left), input_rows(right)) {
                (Some(l), Some(r)) => {
                    let product = (l as u64).saturating_mul(r as u64);
                    if product > self.max_rows {
                        warn!(
                            join = kind,
                            left_rows = l,
                            right_rows = r,
                            max_cartesian_rows = self.max_rows,
                            "CartesianAdmissionRule: refusing cartesian join"
                        );
                        refused = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    debug!(
                        join = kind,
                        left_rows = l,
                        right_rows = r,
                        product,
                        "CartesianAdmissionRule: admitted"
                    );
                }
                (l, r) => {
                    warn!(
                        join = kind,
                        left_rows = ?l,
                        right_rows = ?r,
                        "CartesianAdmissionRule: input statistics absent, admitting"
                    );
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if refused {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "cartesian join admission failed: the estimated row pairs exceed \
                 query.max_cartesian_rows ({}). Add a join condition on equal keys, \
                 filter the inputs, or raise query.max_cartesian_rows.",
                self.max_rows
            )));
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "CartesianAdmissionRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::memory::LazyMemoryExec;

    fn rows(name: &str, n: i64) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..n))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn no_stats(name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    fn cross(l: Arc<dyn ExecutionPlan>, r: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CrossJoinExec::new(l, r))
    }

    fn nested_loop(l: Arc<dyn ExecutionPlan>, r: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(NestedLoopJoinExec::try_new(l, r, None, &JoinType::Inner, None).unwrap())
    }

    fn run(cap: u64, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        CartesianAdmissionRule::new(cap).optimize(plan, &ConfigOptions::new())
    }

    #[test]
    fn cross_join_over_the_cap_is_refused_and_names_only_the_cap() {
        let err = run(399, cross(rows("a", 20), rows("b", 20))).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "{msg}"
        );
        assert!(msg.contains("query.max_cartesian_rows (399)"), "{msg}");
        assert!(
            !msg.contains("400") && !msg.contains("20"),
            "estimates leaked: {msg}"
        );
    }

    #[test]
    fn cross_join_at_the_cap_is_admitted() {
        assert!(run(400, cross(rows("a", 20), rows("b", 20))).is_ok());
    }

    #[test]
    fn nested_loop_join_over_the_cap_is_refused() {
        assert!(run(24, nested_loop(rows("a", 5), rows("b", 5))).is_err());
        assert!(run(25, nested_loop(rows("a", 5), rows("b", 5))).is_ok());
    }

    #[test]
    fn a_join_below_another_node_is_still_found() {
        let join = cross(rows("a", 20), rows("b", 20));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(join));
        assert!(run(100, plan).is_err());
    }

    #[test]
    fn absent_statistics_admit() {
        assert_eq!(input_rows(&no_stats("a")), None);
        assert!(run(1, cross(no_stats("a"), rows("b", 20))).is_ok());
    }

    #[test]
    fn zero_disables_the_gate() {
        assert!(run(0, cross(rows("a", 1000), rows("b", 1000))).is_ok());
    }
}
