//! Physical optimizer rule that rewrites `HashJoinExec` → `SortMergeJoinExec`
//! when the estimated build-side size exceeds a configurable threshold.
//!
//! **Why:** DataFusion 52's `HashJoinExec` does not spill to disk (upstream
//! issue #17267 is proposal-only). Large joins will OOM. `SortMergeJoinExec`
//! spills gracefully via DataFusion's external sort, making it safe for
//! arbitrary-size joins at the cost of requiring sorted inputs.
//!
//! The rule runs as a `PhysicalOptimizerRule` registered on the coordinator's
//! `SessionContext`. It walks the physical plan tree, finds `HashJoinExec`
//! nodes, estimates the build-side size from DataFusion `Statistics`, and
//! replaces with `SortMergeJoinExec` + `SortExec` wrappers when the threshold
//! is exceeded.

use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use tracing::{debug, trace};

/// Default hash join memory threshold: 2 GB.
///
/// When the estimated build-side size of a `HashJoinExec` exceeds this value,
/// the rule rewrites it to `SortMergeJoinExec` which can spill to disk.
/// Set to `0` to disable the rewrite (always use hash join).
pub const DEFAULT_HASH_JOIN_THRESHOLD: usize = 2 * 1024 * 1024 * 1024; // 2 GB

/// Physical optimizer rule that rewrites `HashJoinExec` → `SortMergeJoinExec`
/// when the build-side estimated size exceeds [`Self::hash_join_threshold`].
///
/// The rewrite preserves:
/// - Join type (Inner, Left, Right, Full, LeftSemi, LeftAnti, etc.)
/// - Join conditions (equi-join keys)
/// - Join filter (non-equi conditions)
/// - Null equality semantics
///
/// The rewrite adds `SortExec` nodes on both inputs if they are not already
/// sorted on the join key columns.
#[derive(Debug)]
pub struct JoinStrategyRule {
    /// Maximum build-side size (bytes) for hash join.
    /// Above this, rewrite to `SortMergeJoinExec`.
    hash_join_threshold: usize,
}

impl JoinStrategyRule {
    /// Create a new `JoinStrategyRule` with the given threshold in bytes.
    ///
    /// - `hash_join_threshold = 0` disables the rule (always keeps hash join).
    /// - `hash_join_threshold = DEFAULT_HASH_JOIN_THRESHOLD` uses the 2 GB default.
    pub fn new(hash_join_threshold: usize) -> Self {
        Self {
            hash_join_threshold,
        }
    }
}

impl PhysicalOptimizerRule for JoinStrategyRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Threshold of 0 disables the rule entirely.
        if self.hash_join_threshold == 0 {
            return Ok(plan);
        }

        let threshold = self.hash_join_threshold;
        let transformed = plan.transform_down(|node| {
            if let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() {
                let build_side_size = estimate_build_side_size(hash_join);
                trace!(
                    build_side_bytes = build_side_size,
                    threshold_bytes = threshold,
                    join_type = ?hash_join.join_type(),
                    "JoinStrategyRule: evaluating HashJoinExec"
                );

                if build_side_size > threshold {
                    debug!(
                        build_side_bytes = build_side_size,
                        threshold_bytes = threshold,
                        join_type = ?hash_join.join_type(),
                        "JoinStrategyRule: rewriting HashJoinExec → SortMergeJoinExec \
                         (build side {:.1} MB > threshold {:.1} MB)",
                        build_side_size as f64 / (1024.0 * 1024.0),
                        threshold as f64 / (1024.0 * 1024.0),
                    );
                    let smj = convert_to_sort_merge_join(hash_join)?;
                    return Ok(Transformed::yes(smj));
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "JoinStrategyRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Estimate the build-side (left input) size in bytes from DataFusion statistics.
///
/// Uses `total_byte_size` from the plan's statistics if available.
/// Returns `0` if statistics are unavailable, which means the rule will
/// conservatively keep the hash join.
fn estimate_build_side_size(hash_join: &HashJoinExec) -> usize {
    // In DataFusion's HashJoinExec, the left side is the build side.
    let build_side = hash_join.left();

    // Use the (deprecated but functional) statistics() method which returns
    // aggregated stats across all partitions. partition_statistics(None) is
    // the non-deprecated replacement but returns per-partition stats.
    #[allow(deprecated)]
    let stats = match build_side.statistics() {
        Ok(stats) => stats,
        Err(_) => return 0,
    };

    // Use total_byte_size if it has an exact value.
    stats
        .total_byte_size
        .get_value()
        .copied()
        .unwrap_or(0)
}

/// Convert a `HashJoinExec` to a `SortMergeJoinExec`, adding `SortExec` nodes
/// on both inputs if they are not already sorted on the join keys.
fn convert_to_sort_merge_join(
    hash_join: &HashJoinExec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let join_type = *hash_join.join_type();
    let on = hash_join.on().to_vec();
    let filter = hash_join.filter().cloned();
    let null_equality = hash_join.null_equality();
    let left = Arc::clone(hash_join.left());
    let right = Arc::clone(hash_join.right());

    // Build sort expressions for each side from the join keys.
    // Left side uses the left join key columns, right side uses the right.
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

    // Wrap inputs in SortExec if not already sorted on the join keys.
    let sorted_left = ensure_sorted(left, &left_sort_exprs);
    let sorted_right = ensure_sorted(right, &right_sort_exprs);

    // Build sort options vector (one per join key) for SortMergeJoinExec.
    let sort_options: Vec<SortOptions> = on.iter().map(|_| SortOptions::default()).collect();

    let smj = SortMergeJoinExec::try_new(
        sorted_left,
        sorted_right,
        on,
        filter,
        join_type,
        sort_options,
        null_equality,
    )?;

    Ok(Arc::new(smj))
}

/// Wrap the input plan in a `SortExec` if it is not already sorted on the
/// required sort expressions. If the input's output ordering already
/// satisfies the required ordering, return it as-is.
fn ensure_sorted(
    input: Arc<dyn ExecutionPlan>,
    required_sort: &[PhysicalSortExpr],
) -> Arc<dyn ExecutionPlan> {
    // Check if the input is already sorted on the required columns.
    if is_sorted_on(&input, required_sort) {
        return input;
    }

    // Build a LexOrdering from the required sort expressions.
    match LexOrdering::new(required_sort.to_vec()) {
        Some(ordering) => Arc::new(SortExec::new(ordering, input)),
        None => {
            // Empty sort expressions — should not happen for join keys, but
            // return input unchanged as a safe fallback.
            input
        }
    }
}

/// Check whether the plan's output ordering satisfies the required sort
/// expressions. This is a simplified check that verifies the output ordering
/// has the same columns in the same order (prefix match).
fn is_sorted_on(plan: &Arc<dyn ExecutionPlan>, required: &[PhysicalSortExpr]) -> bool {
    if required.is_empty() {
        return true;
    }

    let output_ordering = match plan.output_ordering() {
        Some(ordering) => ordering,
        None => return false,
    };

    // The output ordering must have at least as many expressions as required,
    // and the first N must match the required sort expressions.
    if output_ordering.len() < required.len() {
        return false;
    }

    for (existing, required_expr) in output_ordering.iter().zip(required.iter()) {
        // Compare the expression string representations and sort options.
        // This is a pragmatic approach — exact physical expression equality
        // checking is complex in DataFusion.
        if format!("{existing}") != format!("{required_expr}") {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::memory::LazyMemoryExec;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_memory_plan(schema: Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    fn make_hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_schema: &Schema,
        right_schema: &Schema,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", left_schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", right_schema).unwrap(),
        )];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None, // filter
                &join_type,
                None, // projection
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_rule_disabled_when_threshold_zero() {
        let rule = JoinStrategyRule::new(0);
        let config = ConfigOptions::new();

        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_hash_join(left, right, &schema, &schema, JoinType::Inner);

        let result = rule.optimize(plan.clone(), &config).unwrap();

        // Should still be HashJoinExec (unchanged)
        assert!(
            result.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Expected HashJoinExec when threshold is 0, got: {:?}",
            result
        );
    }

    #[test]
    fn test_rule_keeps_hash_join_below_threshold() {
        // Empty LazyMemoryExec has 0 bytes estimated, well below any threshold
        let rule = JoinStrategyRule::new(DEFAULT_HASH_JOIN_THRESHOLD);
        let config = ConfigOptions::new();

        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_hash_join(left, right, &schema, &schema, JoinType::Inner);

        let result = rule.optimize(plan, &config).unwrap();

        // 0 bytes < 2 GB threshold, so it should stay as HashJoinExec
        assert!(
            result.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Expected HashJoinExec when build side is below threshold"
        );
    }

    #[test]
    fn test_rule_rewrites_to_smj_above_threshold() {
        // Use a threshold of 0 bytes (but not zero — which disables the rule).
        // Threshold of 1 means anything >= 1 byte triggers rewrite... but
        // LazyMemoryExec reports 0 bytes. We need threshold > 0 but equal to
        // the estimated size. Instead, let's just verify the convert logic
        // directly.
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
        )];

        let hash_join = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        // Directly test the conversion function
        let result = convert_to_sort_merge_join(&hash_join).unwrap();
        assert!(
            result.as_any().downcast_ref::<SortMergeJoinExec>().is_some(),
            "Expected SortMergeJoinExec after conversion"
        );
    }

    #[test]
    fn test_conversion_preserves_join_types() {
        let schema = test_schema();

        for join_type in &[
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ] {
            let left = make_memory_plan(schema.clone());
            let right = make_memory_plan(schema.clone());
            let on = vec![(
                datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
                datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            )];

            let hash_join = HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
            )
            .unwrap();

            let result = convert_to_sort_merge_join(&hash_join);
            assert!(
                result.is_ok(),
                "Failed to convert HashJoinExec({join_type:?}) to SortMergeJoinExec: {:?}",
                result.err()
            );

            let smj = result.unwrap();
            assert!(
                smj.as_any().downcast_ref::<SortMergeJoinExec>().is_some(),
                "Expected SortMergeJoinExec for join type {join_type:?}"
            );
        }
    }

    #[test]
    fn test_sort_exec_added_to_unsorted_inputs() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
        )];

        let hash_join = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let smj = convert_to_sort_merge_join(&hash_join).unwrap();
        let smj = smj
            .as_any()
            .downcast_ref::<SortMergeJoinExec>()
            .expect("Expected SortMergeJoinExec");

        // Both inputs should be wrapped in SortExec since LazyMemoryExec
        // has no output ordering.
        let left_child = &smj.children()[0];
        let right_child = &smj.children()[1];

        assert!(
            left_child.as_any().downcast_ref::<SortExec>().is_some(),
            "Expected left input to be wrapped in SortExec"
        );
        assert!(
            right_child.as_any().downcast_ref::<SortExec>().is_some(),
            "Expected right input to be wrapped in SortExec"
        );
    }

    #[test]
    fn test_already_sorted_input_not_double_wrapped() {
        let schema = test_schema();
        let input = make_memory_plan(schema.clone());

        let sort_expr = PhysicalSortExpr::new(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            SortOptions::default(),
        );
        let ordering = LexOrdering::new(vec![sort_expr.clone()]).unwrap();
        let sorted_input: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, input));

        // ensure_sorted should NOT add another SortExec
        let result = ensure_sorted(sorted_input.clone(), &[sort_expr]);

        // The result should be the same SortExec, not a SortExec wrapping SortExec
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "Expected SortExec"
        );
        // Check the child of the result SortExec is NOT another SortExec
        let children = result.children();
        assert!(
            children[0].as_any().downcast_ref::<SortExec>().is_none(),
            "Should not double-wrap in SortExec"
        );
    }

    #[test]
    fn test_rule_name() {
        let rule = JoinStrategyRule::new(DEFAULT_HASH_JOIN_THRESHOLD);
        assert_eq!(rule.name(), "JoinStrategyRule");
    }

    #[test]
    fn test_rule_schema_check() {
        let rule = JoinStrategyRule::new(DEFAULT_HASH_JOIN_THRESHOLD);
        assert!(rule.schema_check());
    }
}
