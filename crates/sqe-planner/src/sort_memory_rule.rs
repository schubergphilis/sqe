//! Physical optimizer rule that gates `SortExec` against merge headroom.
//!
//! DataFusion's external sort creates an `ExternalSorterMerge` consumer with
//! `can_spill=false`. Unbounded fan-in under a tight pool OOMs the process.
//! This rule fails the plan with a typed error when the sort grant cannot
//! reserve `max_fan_in` merge buffers, so operators get a clear
//! `ResourcesExhausted` instead of a hard crash.
//!
//! Run after adaptive sort stripping so already-stripped sorts are not gated.

use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DataFusionError, Result};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use tracing::debug;

use crate::sort_memory::{SortAdmissionError, SortMemoryPolicy};
use sqe_spill::MemoryGrant;

/// Merge headroom granted to every remaining `SortExec`, derived from
/// `query.max_query_memory`. One formula for the read path
/// (`post_planning`) and the write path (`WriteHandler`), so a sort-on-write
/// is admitted or refused by the same rule as a sorted SELECT (#412).
/// Unparseable or missing: 256 MiB; never below 8 MiB.
pub fn sort_grant_bytes(max_query_memory: &str) -> usize {
    sqe_core::parse_memory_limit(max_query_memory)
        .unwrap_or(256 * 1024 * 1024)
        .max(8 * 1024 * 1024)
}

/// Gate SortExec nodes against a fixed sort memory grant.
#[derive(Debug, Clone)]
pub struct SortMemoryRule {
    /// Total bytes available for one sort operator (run + merge).
    sort_grant_bytes: usize,
    /// Merge fan-in to budget for. `None` (the default) reads the session's
    /// `datafusion.execution.target_partitions` at `optimize` time: a
    /// partitioned `SortExec` merges one sorted run per input partition, so
    /// that is the number of merge buffers the headroom must hold. The old
    /// fixed default of [`crate::sort_memory::DEFAULT_SORT_MERGE_FANIN`] made the gate
    /// unreachable in practice (#412): with the 8 MiB grant floor a quarter
    /// of the grant holds 32 buffers, and 8 never exceeded it.
    max_fan_in: Option<usize>,
    /// When true (default), insufficient headroom fails the plan. When false,
    /// only logs a warning (escape hatch for tests).
    fail_closed: bool,
}

impl SortMemoryRule {
    pub fn new(sort_grant_bytes: usize) -> Self {
        Self {
            sort_grant_bytes: sort_grant_bytes.max(64 * 1024),
            max_fan_in: None,
            fail_closed: true,
        }
    }

    /// Pin the merge fan-in instead of reading `target_partitions`.
    #[must_use]
    pub fn with_max_fan_in(mut self, fan_in: usize) -> Self {
        self.max_fan_in = Some(fan_in.max(2));
        self
    }

    #[must_use]
    pub fn with_fail_closed(mut self, fail_closed: bool) -> Self {
        self.fail_closed = fail_closed;
        self
    }

    fn fan_in(&self, config: &ConfigOptions) -> usize {
        self.max_fan_in
            .unwrap_or(config.execution.target_partitions)
            .max(2)
    }

    fn policy(&self, fan_in: usize) -> SortMemoryPolicy {
        let mut p =
            SortMemoryPolicy::from_grant(MemoryGrant::new("sort-gate", self.sort_grant_bytes));
        p.max_fan_in = fan_in;
        p
    }
}

impl PhysicalOptimizerRule for SortMemoryRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let fan_in = self.fan_in(config);
        let policy = self.policy(fan_in);
        // Plan-level admission: if even one unbounded SortExec exists and the
        // grant cannot hold merge buffers, fail closed before execution.
        let mut sort_count = 0usize;
        let mut has_unbounded = false;
        plan.apply(|node| {
            if let Some(sort) = node.downcast_ref::<SortExec>() {
                sort_count += 1;
                if sort.fetch().is_none() {
                    has_unbounded = true;
                }
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;

        if sort_count == 0 {
            return Ok(plan);
        }

        // TopK-only plans always fit (fetch limits merge state).
        if !has_unbounded {
            debug!(sort_count, "SortMemoryRule: only TopK sorts, skip gate");
            return Ok(plan);
        }

        match policy.can_admit(0) {
            Ok(()) => {
                debug!(
                    sort_grant = self.sort_grant_bytes,
                    merge_headroom = policy.merge_headroom_bytes(),
                    max_fan_in = fan_in,
                    sort_count,
                    "SortMemoryRule: sort grant admits merge headroom"
                );
                Ok(plan)
            }
            Err(e @ SortAdmissionError::InsufficientMergeHeadroom { .. }) => {
                if self.fail_closed {
                    Err(DataFusionError::ResourcesExhausted(format!(
                        "sort merge admission failed: {e}. \
                         Raise worker.memory.operator_budget / memory_limit, \
                         lower datafusion.execution.target_partitions, or use \
                         adaptive/partition_only sort_mode under pressure."
                    )))
                } else {
                    debug!(error = %e, "SortMemoryRule: headroom insufficient (warn-only)");
                    Ok(plan)
                }
            }
        }
    }

    fn name(&self) -> &str {
        "SortMemoryRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::empty::EmptyExec;

    fn sort_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let input = Arc::new(EmptyExec::new(schema.clone()));
        let expr = PhysicalSortExpr::new(
            col("id", &schema).unwrap(),
            datafusion::arrow::compute::SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![expr]).unwrap();
        Arc::new(SortExec::new(ordering, input))
    }

    fn config_with_partitions(target_partitions: usize) -> ConfigOptions {
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = target_partitions;
        config
    }

    #[test]
    fn admits_reasonable_grant() {
        let rule = SortMemoryRule::new(8 * 1024 * 1024);
        let plan = sort_plan();
        // Pinned: `ConfigOptions::new()` takes target_partitions from the
        // machine's core count, and the gate now reads it.
        let out = rule
            .optimize(plan, &config_with_partitions(8))
            .expect("should admit");
        assert_eq!(out.name(), "SortExec");
    }

    /// #412: without an explicit fan-in the gate budgets one merge buffer per
    /// input partition. 8 MiB of grant is 2 MiB of headroom: 32 buffers of
    /// 64 KiB, so 32 partitions fit and 64 do not.
    #[test]
    fn fan_in_defaults_to_target_partitions() {
        let rule = SortMemoryRule::new(8 * 1024 * 1024);
        assert!(rule
            .optimize(sort_plan(), &config_with_partitions(32))
            .is_ok());
        let err = rule
            .optimize(sort_plan(), &config_with_partitions(64))
            .unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "unexpected: {err}"
        );
        // An explicit fan-in wins over the session setting.
        assert!(rule
            .clone()
            .with_max_fan_in(8)
            .optimize(sort_plan(), &config_with_partitions(64))
            .is_ok());
    }

    #[test]
    fn rejects_tiny_grant_fail_closed() {
        let rule = SortMemoryRule::new(1024).with_max_fan_in(8);
        let plan = sort_plan();
        let err = rule.optimize(plan, &ConfigOptions::new()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("sort merge")
                || msg.contains("ResourcesExhausted")
                || msg.contains("headroom"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn warn_only_allows_tiny_grant() {
        let rule = SortMemoryRule::new(1024)
            .with_max_fan_in(8)
            .with_fail_closed(false);
        let plan = sort_plan();
        assert!(rule.optimize(plan, &ConfigOptions::new()).is_ok());
    }
}
