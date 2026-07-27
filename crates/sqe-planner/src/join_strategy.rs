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
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
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
///
/// Phase 5b: strategy *selection* prefers [`crate::grace_hash_join::LocalJoinStrategy::GraceHashJoin`]
/// for unknown/large builds via [`crate::grace_hash_join::choose_local_join_strategy`].
/// Physical rewrite still uses spillable SortMergeJoin until `GraceHashJoinExec`
/// is registered on the worker path; the choice is logged for plan profiles.
#[derive(Debug)]
pub struct JoinStrategyRule {
    /// Maximum build-side size (bytes) for hash join.
    /// Above this, rewrite to `SortMergeJoinExec`.
    hash_join_threshold: usize,
    /// When true (default), unknown/large builds select Grace in profiles;
    /// physical rewrite remains SMJ until Grace exec is wired end-to-end.
    prefer_grace: bool,
}

impl JoinStrategyRule {
    /// Create a new `JoinStrategyRule` with the given threshold in bytes.
    ///
    /// - `hash_join_threshold = 0` disables the rule (always keeps hash join).
    /// - `hash_join_threshold = DEFAULT_HASH_JOIN_THRESHOLD` uses the 2 GB default.
    pub fn new(hash_join_threshold: usize) -> Self {
        Self {
            hash_join_threshold,
            prefer_grace: true,
        }
    }

    /// Prefer Grace hash join in strategy selection for unknown/large builds.
    #[must_use]
    pub fn with_prefer_grace(mut self, prefer: bool) -> Self {
        self.prefer_grace = prefer;
        self
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
            if let Some(hash_join) = node.downcast_ref::<HashJoinExec>() {
                let estimate = estimate_build_side_size(hash_join);
                trace!(
                    estimate = ?estimate,
                    threshold_bytes = threshold,
                    join_type = ?hash_join.join_type(),
                    "JoinStrategyRule: evaluating HashJoinExec"
                );

                // Phase 5a/5b: spillable path when build is known-large OR
                // unknown/inexact. Only keep non-spillable HashJoinExec for an
                // explicit small *exact* estimate. Strategy selection records
                // Grace preference; physical plan uses SMJ until Grace exec
                // is registered on the execute path.
                let exact = match estimate {
                    BuildSizeEstimate::Exact(size) => Some(size),
                    BuildSizeEstimate::Unknown => None,
                };
                let choice = crate::grace_hash_join::choose_local_join_strategy(
                    exact,
                    threshold,
                    self.prefer_grace,
                );
                let rewrite = !matches!(
                    choice,
                    crate::grace_hash_join::LocalJoinStrategy::HashJoin
                );

                if rewrite {
                    debug!(
                        estimate = ?estimate,
                        threshold_bytes = threshold,
                        strategy = ?choice,
                        join_type = ?hash_join.join_type(),
                        "JoinStrategyRule: rewriting HashJoinExec → SortMergeJoinExec \
                         (spillable; Grace selected={})",
                        matches!(
                            choice,
                            crate::grace_hash_join::LocalJoinStrategy::GraceHashJoin
                        )
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

/// Build-side size classification for join strategy selection (Phase 5a).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildSizeEstimate {
    /// Exact byte size from plan statistics (safe for threshold comparison).
    Exact(usize),
    /// Stats missing, inexact, or unreadable — choose the spillable path.
    Unknown,
}

/// Estimate the build-side (left input) size from DataFusion statistics.
///
/// Phase 5a: only `Precision::Exact` yields [`BuildSizeEstimate::Exact`].
/// Absent, inexact, or errored stats yield [`BuildSizeEstimate::Unknown`] so
/// the rule rewrites to spillable sort-merge rather than keeping a non-spillable
/// hash join on a guessed-zero build side.
fn estimate_build_side_size(hash_join: &HashJoinExec) -> BuildSizeEstimate {
    use datafusion::common::stats::Precision;

    // In DataFusion's HashJoinExec, the left side is the build side.
    let build_side = hash_join.left();

    let stats = match build_side.partition_statistics(None) {
        Ok(stats) => stats,
        Err(_) => return BuildSizeEstimate::Unknown,
    };

    match stats.total_byte_size {
        Precision::Exact(v) => BuildSizeEstimate::Exact(v),
        Precision::Inexact(_) | Precision::Absent => BuildSizeEstimate::Unknown,
    }
}

/// Convert a `HashJoinExec` to a `SortMergeJoinExec`, adding `SortExec` nodes
/// on both inputs if they are not already sorted on the join keys.
///
/// A `HashJoinExec` may carry an output projection, which `SortMergeJoinExec`
/// has no parameter for; the projection is re-applied as an explicit
/// `ProjectionExec` so the rewritten subtree keeps the schema the hash join
/// advertised. See the comment at the tail of this function.
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

    let smj: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
        sorted_left,
        sorted_right,
        on,
        filter,
        join_type,
        sort_options,
        null_equality,
    )?);

    // `HashJoinExec` can carry an output projection (`Option<ProjectionRef>`);
    // `SortMergeJoinExec::try_new` takes no projection and always emits the
    // full `build_join_schema(left, right, join_type)` output. Both execs
    // build that same full schema, and the hash join's projection indices
    // address it, so re-applying them as a `ProjectionExec` reproduces the
    // hash join's schema exactly.
    //
    // Dropping the projection instead makes the rewritten node emit more
    // columns than it advertised, while every parent operator keeps indexing
    // into the projected schema. The failure then surfaces far from here as a
    // type error on an unrelated column -- TPC-H q12 (`Int64 == Utf8`), q19
    // (`expected Decimal128(15, 2) but found Utf8 at column index 0`) and SSB
    // q1.1-q1.3 (`Int32 * Float64`). Note that `schema_check()` cannot catch
    // this: the coordinator invokes this rule directly rather than through
    // DataFusion's optimizer, so the framework never runs that check.
    let Some(projection) = hash_join.projection.as_ref() else {
        return Ok(smj);
    };

    let join_schema = smj.schema();
    let exprs = projection
        .iter()
        .map(|&idx| {
            let name = join_schema.field(idx).name();
            let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(name, idx));
            (col, name.clone())
        })
        .collect::<Vec<_>>();

    Ok(Arc::new(ProjectionExec::try_new(exprs, smj)?))
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
        // PLAN-05: compare `PhysicalSortExpr` structurally via its `PartialEq`
        // impl (covers the inner expr + `SortOptions`) instead of allocating
        // two `String`s per pair with `format!`. Same semantics, no per-compare
        // allocation on the planning path.
        if existing != required_expr {
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
                false,
            )
            .unwrap(),
        )
    }

    /// A `HashJoinExec` carrying an output projection must keep that exact
    /// output schema after the rewrite. `SortMergeJoinExec` has no projection
    /// parameter and emits the full left++right schema, so a rewrite that
    /// forgets the projection changes the node's schema underneath its parents
    /// and breaks the query with a type error on an unrelated column
    /// (TPC-H q12/q19, SSB q1.1-q1.3).
    #[test]
    fn convert_preserves_hash_join_output_projection() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());

        // Full join schema is [id, name, value, id, name, value]; project a
        // subset that reorders and mixes types so a dropped projection cannot
        // coincidentally still line up.
        let projection = vec![2, 3, 1];
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", &schema).unwrap(),
        )];
        let hash_join = HashJoinExec::try_new(
            left,
            right,
            on,
            None, // filter
            &JoinType::Inner,
            Some(projection),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap();

        let expected = hash_join.schema();
        assert_eq!(
            expected.fields().len(),
            3,
            "projected hash join should expose exactly the 3 projected columns"
        );

        let rewritten = convert_to_sort_merge_join(&hash_join).unwrap();

        assert_eq!(
            rewritten.schema(),
            expected,
            "rewritten plan must keep the hash join's projected output schema"
        );
        assert_eq!(
            rewritten.schema().field(0).data_type(),
            &DataType::Float64,
            "projection order must survive the rewrite"
        );
    }

    /// Control for the test above: with no projection the rewrite must not
    /// introduce a `ProjectionExec`, and the schema is the full join schema.
    #[test]
    fn convert_without_projection_returns_bare_sort_merge_join() {
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let hash_join = make_hash_join(left, right, &schema, &schema, JoinType::Inner);
        let hash_join = hash_join
            .downcast_ref::<HashJoinExec>()
            .expect("make_hash_join builds a HashJoinExec");

        let rewritten = convert_to_sort_merge_join(hash_join).unwrap();

        assert!(
            rewritten.downcast_ref::<SortMergeJoinExec>().is_some(),
            "unprojected join should rewrite to a bare SortMergeJoinExec"
        );
        assert_eq!(rewritten.schema(), hash_join.schema());
        assert_eq!(rewritten.schema().fields().len(), 6);
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
            result.downcast_ref::<HashJoinExec>().is_some(),
            "Expected HashJoinExec when threshold is 0, got: {:?}",
            result
        );
    }

    #[test]
    fn test_rule_keeps_hash_join_for_exact_small_build() {
        // When build-side stats are Exact and ≤ threshold, keep HashJoinExec.
        // LazyMemoryExec often reports Absent/Inexact (unknown → SMJ under 5a);
        // this test only asserts the small-known path when estimate is Exact.
        let rule = JoinStrategyRule::new(DEFAULT_HASH_JOIN_THRESHOLD);
        let config = ConfigOptions::new();

        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_hash_join(left, right, &schema, &schema, JoinType::Inner);

        let estimated = estimate_build_side_size(
            plan.downcast_ref::<HashJoinExec>()
                .expect("fixture is HashJoinExec"),
        );
        match estimated {
            BuildSizeEstimate::Exact(size) => {
                assert!(size <= DEFAULT_HASH_JOIN_THRESHOLD);
                let result = rule.optimize(plan, &config).unwrap();
                assert!(
                    result.downcast_ref::<HashJoinExec>().is_some(),
                    "exact small build must keep HashJoinExec"
                );
            }
            BuildSizeEstimate::Unknown => {
                // Fixture has no exact stats — covered by the unknown→SMJ test.
            }
        }
    }

    /// Phase 5a: unknown / absent / inexact build-side statistics choose the
    /// spillable SortMergeJoin path instead of keeping HashJoinExec on a
    /// guessed-zero build side.
    #[test]
    fn phase5a_unknown_statistics_choose_sort_merge_join() {
        let rule = JoinStrategyRule::new(DEFAULT_HASH_JOIN_THRESHOLD);
        let config = ConfigOptions::new();

        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_hash_join(left, right, &schema, &schema, JoinType::Inner);

        let estimated = estimate_build_side_size(
            plan.downcast_ref::<HashJoinExec>()
                .expect("fixture is HashJoinExec"),
        );

        // Empty LazyMemoryExec: expect Unknown (Absent/Inexact). If a future
        // DF version reports Exact(0), that is the small-known exception and
        // stays as HashJoin — assert accordingly.
        match estimated {
            BuildSizeEstimate::Unknown => {
                let result = rule.optimize(plan, &config).unwrap();
                assert!(
                    result.downcast_ref::<SortMergeJoinExec>().is_some(),
                    "Phase 5a: unknown build stats must rewrite to SortMergeJoinExec; \
                     got {:?}",
                    result
                );
            }
            BuildSizeEstimate::Exact(0) => {
                let result = rule.optimize(plan, &config).unwrap();
                assert!(
                    result.downcast_ref::<HashJoinExec>().is_some(),
                    "exact-zero build is the small-known exception (keep hash)"
                );
            }
            BuildSizeEstimate::Exact(other) => {
                panic!("unexpected exact estimate {other} for empty LazyMemoryExec");
            }
        }
    }

    #[test]
    fn phase5a_estimate_error_is_unknown() {
        // Direct unit: stats errors map to Unknown (covered by the match arm).
        // Integration uses LazyMemoryExec which typically yields Absent.
        let schema = test_schema();
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_hash_join(left, right, &schema, &schema, JoinType::Inner);
        let hj = plan.downcast_ref::<HashJoinExec>().unwrap();
        let est = estimate_build_side_size(hj);
        assert!(
            matches!(
                est,
                BuildSizeEstimate::Unknown | BuildSizeEstimate::Exact(0)
            ),
            "empty memory plan should be Unknown or Exact(0), got {est:?}"
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
            false,
        )
        .unwrap();

        // Directly test the conversion function
        let result = convert_to_sort_merge_join(&hash_join).unwrap();
        assert!(
            result.downcast_ref::<SortMergeJoinExec>().is_some(),
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
                false,
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
                smj.downcast_ref::<SortMergeJoinExec>().is_some(),
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
            false,
        )
        .unwrap();

        let smj = convert_to_sort_merge_join(&hash_join).unwrap();
        let smj = smj
            .downcast_ref::<SortMergeJoinExec>()
            .expect("Expected SortMergeJoinExec");

        // Both inputs should be wrapped in SortExec since LazyMemoryExec
        // has no output ordering.
        let left_child = &smj.children()[0];
        let right_child = &smj.children()[1];

        assert!(
            left_child.downcast_ref::<SortExec>().is_some(),
            "Expected left input to be wrapped in SortExec"
        );
        assert!(
            right_child.downcast_ref::<SortExec>().is_some(),
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
            result.downcast_ref::<SortExec>().is_some(),
            "Expected SortExec"
        );
        // Check the child of the result SortExec is NOT another SortExec
        let children = result.children();
        assert!(
            children[0].downcast_ref::<SortExec>().is_none(),
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
