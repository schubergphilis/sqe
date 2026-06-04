//! Physical optimizer rule that reorders inner equi-joins in star-schema
//! patterns so that small dimension tables are joined first (building small
//! hash tables) and the large fact table is probed last.
//!
//! **Why:** In data-warehouse star schemas, a large fact table (e.g., `orders`)
//! is joined with multiple smaller dimension tables (e.g., `customers`,
//! `products`, `dates`). When DataFusion plans the join order based on SQL
//! syntax (left-to-right), the fact table often ends up as the build side of
//! the first join, creating a massive hash table.
//!
//! This rule detects chains of `HashJoinExec` (inner joins only) and reorders
//! them so the smallest inputs are joined first, producing progressively
//! larger intermediate results. The fact table, being the largest, is always
//! the final probe side.
//!
//! **Activation conditions:**
//! - All joins in the chain are inner equi-joins
//! - All input tables have row count statistics (`Exact` or `Inexact`)
//! - The ratio between the largest and smallest table >= `min_ratio` (default 10)
//!
//! The rule runs as a `PhysicalOptimizerRule` registered on the coordinator's
//! `SessionContext`.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;
use tracing::{debug, info, trace};

/// Default minimum ratio between the largest and smallest table row counts
/// required to trigger star-schema reordering.
pub const DEFAULT_MIN_RATIO: usize = 10;

/// Physical optimizer rule that reorders inner equi-joins in star-schema
/// patterns.
///
/// Detects when a large fact table is joined with multiple small dimension
/// tables and reorders so dimensions are joined first (building small hash
/// tables) and the fact table is probed last.
///
/// Only activates when:
/// - All joins are inner equi-joins
/// - Statistics (row counts) are available for all inputs
/// - The ratio between largest and smallest table exceeds the threshold
///
/// # Example
///
/// Given a query joining fact table `orders` (10M rows) with dimension tables
/// `customers` (10K), `products` (5K), and `dates` (365):
///
/// Before (SQL order): `orders JOIN customers JOIN products JOIN dates`
///
/// After (reordered): `dates JOIN products JOIN customers JOIN orders`
///
/// The smallest tables build the smallest hash tables, and the fact table
/// is only ever probed, never used as a build side.
#[derive(Debug)]
pub struct StarSchemaReorderRule {
    /// Minimum ratio between fact table rows and dimension table rows
    /// to trigger reordering. Default: 10.
    min_ratio: usize,
}

impl StarSchemaReorderRule {
    /// Create a new `StarSchemaReorderRule` with the given minimum ratio.
    ///
    /// - `min_ratio = 0` disables the rule.
    /// - `min_ratio = 10` (default) requires the fact table to be at least
    ///   10x larger than the smallest dimension table.
    pub fn new(min_ratio: usize) -> Self {
        Self { min_ratio }
    }
}

impl Default for StarSchemaReorderRule {
    fn default() -> Self {
        Self::new(DEFAULT_MIN_RATIO)
    }
}

/// A pair of physical expressions representing (left_key, right_key) in an equi-join.
type JoinKeyPair = (
    Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    Arc<dyn datafusion::physical_expr::PhysicalExpr>,
);

/// Resolved join condition: equi-join key pairs, optional filter, null equality, partition mode.
type ResolvedCondition = (
    Vec<JoinKeyPair>,
    Option<JoinFilter>,
    datafusion::common::NullEquality,
    PartitionMode,
);

/// A join input extracted from a chain of `HashJoinExec` nodes.
///
/// Each entry represents a leaf plan (scan node or subquery) that participates
/// in the star-schema join pattern.
#[derive(Debug)]
struct JoinInput {
    /// The execution plan for this input.
    plan: Arc<dyn ExecutionPlan>,
    /// Estimated row count from statistics.
    row_count: usize,
    /// Original index in the flattened join chain (for stable sorting).
    original_index: usize,
}

/// Metadata about a join condition between two inputs in the chain.
///
/// After we flatten a chain of HashJoinExec nodes, each join condition
/// references columns from a left subtree and a right leaf. We track
/// these as indices into our flattened input list so we can reconnect
/// conditions after reordering.
#[derive(Debug, Clone)]
struct JoinCondition {
    /// Column expressions from the left side of the original join.
    left_cols: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Column expressions from the right side of the original join.
    right_cols: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// The join filter (non-equi conditions), if any.
    filter: Option<JoinFilter>,
    /// Index of the right input in the flattened list.
    right_input_index: usize,
    /// Null equality setting from the original join.
    null_equality: datafusion::common::NullEquality,
    /// Partition mode from the original join.
    partition_mode: PartitionMode,
}

impl PhysicalOptimizerRule for StarSchemaReorderRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Ratio of 0 disables the rule entirely.
        if self.min_ratio == 0 {
            return Ok(plan);
        }

        let min_ratio = self.min_ratio;
        let transformed = plan.transform_down(|node| {
            // Only process INNER HashJoinExec nodes. Skip non-HashJoin nodes and
            // non-INNER joins (LEFT, RIGHT, FULL) — transform_down will recurse
            // into their children, where we'll find the INNER chain.
            let is_inner_hash = node
                .as_any()
                .downcast_ref::<HashJoinExec>()
                .is_some_and(|hj| *hj.join_type() == JoinType::Inner);
            if !is_inner_hash {
                return Ok(Transformed::no(node));
            }

            // Flatten the chain of inner HashJoinExec nodes.
            let mut inputs: Vec<JoinInput> = Vec::new();
            let mut conditions: Vec<JoinCondition> = Vec::new();

            if !flatten_join_chain(&node, &mut inputs, &mut conditions) {
                debug!(
                    "StarSchemaReorderRule: join chain not eligible (missing stats or cross join)"
                );
                return Ok(Transformed::no(node));
            }

            // Need at least 3 inputs (2 joins) for star-schema reordering to matter.
            if inputs.len() < 3 {
                debug!(
                    inputs = inputs.len(),
                    "StarSchemaReorderRule: too few inputs for star-schema pattern"
                );
                return Ok(Transformed::no(node));
            }

            // Log what we found
            let row_counts: Vec<usize> = inputs.iter().map(|i| i.row_count).collect();
            info!(
                inputs = inputs.len(),
                conditions = conditions.len(),
                row_counts = ?row_counts,
                "StarSchemaReorderRule: evaluating join chain"
            );

            // Check that all inputs have row count stats.
            if inputs.iter().any(|i| i.row_count == 0) {
                info!(
                    "StarSchemaReorderRule: some inputs have no row count statistics, skipping"
                );
                return Ok(Transformed::no(node));
            }

            // Check the ratio between largest and smallest.
            let max_rows = inputs.iter().map(|i| i.row_count).max().unwrap_or(0);
            let min_rows = inputs.iter().map(|i| i.row_count).min().unwrap_or(0);

            if min_rows == 0 || max_rows / min_rows < min_ratio {
                debug!(
                    max_rows,
                    min_rows,
                    min_ratio,
                    "StarSchemaReorderRule: ratio below threshold, skipping"
                );
                return Ok(Transformed::no(node));
            }

            // Check if already optimally ordered (sorted ascending by row count).
            let already_optimal = inputs.windows(2).all(|w| w[0].row_count <= w[1].row_count);
            if already_optimal {
                debug!(
                    "StarSchemaReorderRule: join order is already optimal"
                );
                return Ok(Transformed::no(node));
            }

            // Log the reorder decision.
            let table_sizes: Vec<String> = inputs
                .iter()
                .map(|i| format!("{}={}", i.original_index, i.row_count))
                .collect();
            info!(
                input_count = inputs.len(),
                max_rows,
                min_rows,
                ratio = max_rows / min_rows,
                tables = %table_sizes.join(", "),
                "StarSchemaReorderRule: reordering joins — smallest dimensions first, fact table last"
            );

            // Rebuild the join chain with inputs sorted by row count (ascending).
            match rebuild_join_chain(&mut inputs, &conditions) {
                Ok(new_plan) => {
                    info!(
                        "StarSchemaReorderRule: successfully reordered {} joins",
                        inputs.len() - 1
                    );
                    Ok(Transformed::yes(new_plan))
                }
                Err(e) => {
                    debug!(
                        error = %e,
                        "StarSchemaReorderRule: failed to rebuild join chain, keeping original order"
                    );
                    Ok(Transformed::no(node))
                }
            }
        })?;

        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "StarSchemaReorderRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Flatten a chain of inner `HashJoinExec` nodes into a list of leaf inputs
/// and join conditions.
///
/// The chain is walked from the root (outermost join) down through the left
/// child. At each level:
/// - The right child is collected as a leaf input.
/// - The join condition (equi-join keys) is recorded.
/// - The left child is recursively examined.
///
/// When the left child is no longer a `HashJoinExec` (or is not an inner
/// join), it becomes the final leaf input.
///
/// Returns `false` if any join in the chain is not INNER, indicating the
/// chain is not eligible for reordering.
fn flatten_join_chain(
    plan: &Arc<dyn ExecutionPlan>,
    inputs: &mut Vec<JoinInput>,
    conditions: &mut Vec<JoinCondition>,
) -> bool {
    let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        // Not a HashJoinExec — this is a leaf input.
        let row_count = estimate_row_count(plan);
        let idx = inputs.len();
        inputs.push(JoinInput {
            plan: Arc::clone(plan),
            row_count,
            original_index: idx,
        });
        return true;
    };

    // Non-INNER joins (LEFT, RIGHT, FULL, CROSS): treat as a boundary.
    // We don't reorder across them, but we DO reorder the INNER chain
    // below them. This handles q72-style plans where a LEFT JOIN promotion
    // sits on top of a chain of INNER joins.
    if *hash_join.join_type() != JoinType::Inner {
        trace!(
            join_type = ?hash_join.join_type(),
            "StarSchemaReorderRule: non-inner join, treating as leaf (not reordering across)"
        );
        // Treat this entire subtree as a single opaque input.
        // The reorder rule will be applied recursively to children
        // via the top-level optimize() walk.
        let row_count = estimate_row_count(plan);
        let idx = inputs.len();
        inputs.push(JoinInput {
            plan: Arc::clone(plan),
            row_count,
            original_index: idx,
        });
        return true;
    }

    // Only handle equi-joins without complex filter conditions that reference
    // multiple tables (simple filters are ok).
    if hash_join.on().is_empty() {
        trace!("StarSchemaReorderRule: cross join detected, aborting");
        return false;
    }

    // Recurse into the left child (which may be another HashJoinExec).
    let left = hash_join.left();
    if !flatten_join_chain(left, inputs, conditions) {
        return false;
    }

    // The right child is always a leaf input in our flattening.
    let right = hash_join.right();
    let right_row_count = estimate_row_count(right);
    let right_idx = inputs.len();
    inputs.push(JoinInput {
        plan: Arc::clone(right),
        row_count: right_row_count,
        original_index: right_idx,
    });

    // Record the join condition. The left side of the join condition
    // references columns from the accumulated left subtree (inputs[0..right_idx]),
    // the right side references columns from inputs[right_idx].
    let on = hash_join.on();
    let left_cols: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
        on.iter().map(|(l, _)| Arc::clone(l)).collect();
    let right_cols: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
        on.iter().map(|(_, r)| Arc::clone(r)).collect();

    conditions.push(JoinCondition {
        left_cols,
        right_cols,
        filter: hash_join.filter().cloned(),
        right_input_index: right_idx,
        null_equality: hash_join.null_equality(),
        partition_mode: *hash_join.partition_mode(),
    });

    true
}

/// Estimate the row count for a plan node from DataFusion statistics.
///
/// Returns 0 if statistics are unavailable.
fn estimate_row_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let stats = match plan.partition_statistics(None) {
        Ok(stats) => stats,
        Err(_) => return 0,
    };

    stats.num_rows.get_value().copied().unwrap_or(0)
}

/// Rebuild the join chain with inputs sorted by row count ascending.
///
/// Strategy: We use a greedy approach. Sort inputs by row count. Then build
/// the join tree bottom-up: start with the two smallest inputs, join them,
/// then join the result with the next smallest, and so on.
///
/// For each pair of inputs being joined, we need to find the appropriate
/// join condition. We look through the original conditions to find one
/// that connects columns between the already-joined set and the new input.
///
/// When an exact condition match is not found (because the original condition
/// referenced a different pairing), we use column name matching to construct
/// the correct equi-join condition for the new plan structure.
fn rebuild_join_chain(
    inputs: &mut [JoinInput],
    conditions: &[JoinCondition],
) -> Result<Arc<dyn ExecutionPlan>> {
    // Sort inputs by row count ascending (stable sort preserves original order
    // for equal row counts).
    inputs.sort_by_key(|i| i.row_count);

    // Track which original input indices are in the "left accumulated" set.
    let mut accumulated_indices: Vec<usize> = Vec::new();

    // Start with the smallest input as the initial left side.
    let mut current_plan = Arc::clone(&inputs[0].plan);
    accumulated_indices.push(inputs[0].original_index);

    // Join each subsequent input onto the accumulated plan.
    for new_input in inputs.iter().skip(1) {
        let new_idx = new_input.original_index;

        // Find a join condition that connects the accumulated set with
        // the new input.
        let condition = find_condition_for_pair(
            &accumulated_indices,
            new_idx,
            conditions,
            &current_plan,
            &new_input.plan,
        );

        match condition {
            Some((on, filter, null_eq, partition_mode)) => {
                let new_join = HashJoinExec::try_new(
                    current_plan,
                    Arc::clone(&new_input.plan),
                    on,
                    filter,
                    &JoinType::Inner,
                    None, // projection
                    partition_mode,
                    null_eq,
                    false, // is_null_safe
                )?;
                current_plan = Arc::new(new_join);
            }
            None => {
                // Could not find a valid join condition linking these inputs.
                // This can happen with complex join graphs. Fall back gracefully.
                return Err(datafusion::error::DataFusionError::Internal(
                    format!(
                        "StarSchemaReorderRule: cannot find join condition connecting \
                         input {} to the accumulated set {:?}",
                        new_idx, accumulated_indices,
                    ),
                ));
            }
        }

        accumulated_indices.push(new_idx);
    }

    Ok(current_plan)
}

/// Find a join condition that connects the accumulated left set with a new
/// right input.
///
/// Searches through the original conditions for one where:
/// - The right_input_index matches `new_idx`, AND
/// - The left columns can be resolved in the accumulated plan's schema.
///
/// If found directly, returns the condition adapted to the new accumulated
/// plan's schema. If the condition originally referenced a different left
/// arrangement, uses column name matching to rewrite the left column
/// references.
fn find_condition_for_pair(
    accumulated_indices: &[usize],
    new_idx: usize,
    conditions: &[JoinCondition],
    accumulated_plan: &Arc<dyn ExecutionPlan>,
    new_input: &Arc<dyn ExecutionPlan>,
) -> Option<ResolvedCondition> {
    let accumulated_schema = accumulated_plan.schema();
    let new_schema = new_input.schema();

    // First, try to find a condition where the new input is the right side.
    for cond in conditions {
        if cond.right_input_index == new_idx {
            // Try to resolve left columns in the accumulated schema by name.
            let on = resolve_join_keys(
                &cond.left_cols,
                &cond.right_cols,
                &accumulated_schema,
                &new_schema,
            );
            if let Some(on) = on {
                return Some((on, cond.filter.clone(), cond.null_equality, cond.partition_mode));
            }
        }
    }

    // Second pass: the new input might have originally been on the left side
    // of a condition (if it was part of the accumulated left subtree in the
    // original plan). Look for conditions where the left columns can be
    // resolved in the new input's schema and right columns in the accumulated
    // schema.
    for cond in conditions {
        // Check if any of the accumulated inputs were the right side of this
        // condition (meaning the new_input was in the left subtree).
        if accumulated_indices.contains(&cond.right_input_index) {
            // Try swapping: left cols from the condition map to new_input,
            // right cols map to accumulated.
            let on = resolve_join_keys(
                &cond.right_cols,
                &cond.left_cols,
                &accumulated_schema,
                &new_schema,
            );
            if let Some(on) = on {
                return Some((on, cond.filter.clone(), cond.null_equality, cond.partition_mode));
            }
        }
    }

    // Third pass: try name-based matching across all conditions.
    // Some conditions might reference columns that exist in both the
    // accumulated plan and the new input but were paired differently
    // in the original plan.
    for cond in conditions {
        let on = resolve_join_keys_by_name(
            &cond.left_cols,
            &cond.right_cols,
            &accumulated_schema,
            &new_schema,
        );
        if let Some(on) = on {
            return Some((on, cond.filter.clone(), cond.null_equality, cond.partition_mode));
        }
    }

    None
}

/// Try to resolve join key columns from original expressions into the new
/// schemas by matching column names.
///
/// Returns `None` if any column cannot be resolved.
fn resolve_join_keys(
    left_cols: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
    right_cols: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
    accumulated_schema: &arrow_schema::SchemaRef,
    new_schema: &arrow_schema::SchemaRef,
) -> Option<Vec<JoinKeyPair>> {
    let mut on = Vec::with_capacity(left_cols.len());

    for (left_expr, right_expr) in left_cols.iter().zip(right_cols.iter()) {
        // Extract column name from the left expression.
        let left_col_name = extract_column_name(left_expr)?;
        // Extract column name from the right expression.
        let right_col_name = extract_column_name(right_expr)?;

        // PLAN-01: Find the column in the accumulated schema by name, but bail
        // if the name is ambiguous. After joins concatenate columns the
        // accumulated (left) schema commonly carries duplicate column names (a
        // shared surrogate key like `id`/`sk` across dimensions is the
        // star-schema norm). `Schema::index_of` returns the FIRST field of that
        // name, which would silently rebind the join key to the wrong table's
        // column -- a valid join with the same output shape but WRONG rows.
        // Returning `None` makes `find_condition_for_pair` (and thus
        // `rebuild_join_chain`) bail, keeping the original, correct plan.
        let left_idx = index_of_unique(accumulated_schema, &left_col_name)?;
        let new_left: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new(&left_col_name, left_idx));

        // The new input is a single fresh leaf; its names are not subject to
        // post-join duplication, but guard it the same way for safety.
        let right_idx = index_of_unique(new_schema, &right_col_name)?;
        let new_right: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new(&right_col_name, right_idx));

        on.push((new_left, new_right));
    }

    Some(on)
}

/// Try name-based matching: for each (left_col, right_col) pair from the
/// original condition, check if either name exists in the accumulated schema
/// and the other in the new schema.
fn resolve_join_keys_by_name(
    left_cols: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
    right_cols: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
    accumulated_schema: &arrow_schema::SchemaRef,
    new_schema: &arrow_schema::SchemaRef,
) -> Option<Vec<JoinKeyPair>> {
    let mut on = Vec::with_capacity(left_cols.len());

    for (left_expr, right_expr) in left_cols.iter().zip(right_cols.iter()) {
        let left_name = extract_column_name(left_expr)?;
        let right_name = extract_column_name(right_expr)?;

        // PLAN-01: use unique-name resolution on both schemas. An ambiguous
        // accumulated-side name (duplicate after join concatenation) would
        // otherwise bind to the wrong column via `index_of`'s first-match.
        // Try: left_name in accumulated, right_name in new.
        if let (Some(acc_idx), Some(new_idx)) = (
            index_of_unique(accumulated_schema, &left_name),
            index_of_unique(new_schema, &right_name),
        ) {
            let acc_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                Arc::new(Column::new(&left_name, acc_idx));
            let new_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                Arc::new(Column::new(&right_name, new_idx));
            on.push((acc_col, new_col));
            continue;
        }

        // Try: right_name in accumulated, left_name in new.
        if let (Some(acc_idx), Some(new_idx)) = (
            index_of_unique(accumulated_schema, &right_name),
            index_of_unique(new_schema, &left_name),
        ) {
            let acc_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                Arc::new(Column::new(&right_name, acc_idx));
            let new_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                Arc::new(Column::new(&left_name, new_idx));
            on.push((acc_col, new_col));
            continue;
        }

        // Could not resolve this pair unambiguously.
        return None;
    }

    if on.is_empty() {
        return None;
    }

    Some(on)
}

/// Extract the column name from a physical expression.
///
/// Only supports `Column` expressions — returns `None` for complex
/// expressions (casts, functions, etc.).
fn extract_column_name(expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>) -> Option<String> {
    expr.as_any()
        .downcast_ref::<Column>()
        .map(|c| c.name().to_string())
}

/// PLAN-01: resolve a column name to its index ONLY when the name is unique in
/// the schema. Returns `None` when the name is missing OR appears more than
/// once. `Schema::index_of` returns the first match, which silently binds a
/// rebuilt join key to the wrong table's column when the accumulated left
/// schema carries duplicate names (the shared-surrogate-key star-schema case).
/// Bailing on ambiguity makes the reorder keep the original, correct plan.
fn index_of_unique(schema: &arrow_schema::Schema, name: &str) -> Option<usize> {
    let mut found: Option<usize> = None;
    for (i, field) in schema.fields().iter().enumerate() {
        if field.name() == name {
            if found.is_some() {
                // Ambiguous: more than one field with this name.
                return None;
            }
            found = Some(i);
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use std::sync::Arc;

    /// Create a schema with the given fields.
    fn make_schema(fields: &[(&str, DataType)]) -> Arc<Schema> {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt)| Field::new(*name, dt.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    /// Create a LazyMemoryExec with no data (zero row count in statistics).
    fn make_memory_plan(schema: Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        Arc::new(LazyMemoryExec::try_new(schema, vec![]).unwrap())
    }

    /// Create a HashJoinExec between two plans on the given column names.
    fn make_inner_hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_col: &str,
        right_col: &str,
    ) -> Arc<dyn ExecutionPlan> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let on = vec![(
            datafusion::physical_expr::expressions::col(left_col, &left_schema).unwrap(),
            datafusion::physical_expr::expressions::col(right_col, &right_schema).unwrap(),
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

    #[test]
    fn test_rule_disabled_when_ratio_zero() {
        let rule = StarSchemaReorderRule::new(0);
        let config = ConfigOptions::new();

        let schema = make_schema(&[("id", DataType::Int64), ("val", DataType::Utf8)]);
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_inner_hash_join(left, right, "id", "id");

        let result = rule.optimize(plan.clone(), &config).unwrap();
        // Should remain unchanged.
        assert!(
            result.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Expected unchanged HashJoinExec when rule is disabled"
        );
    }

    #[test]
    fn test_rule_skips_two_input_joins() {
        // Star-schema reordering needs at least 3 inputs (2 joins).
        let rule = StarSchemaReorderRule::new(10);
        let config = ConfigOptions::new();

        let schema = make_schema(&[("id", DataType::Int64), ("val", DataType::Utf8)]);
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_inner_hash_join(left, right, "id", "id");

        let result = rule.optimize(plan.clone(), &config).unwrap();
        // Two-input join should not be reordered.
        assert!(
            result.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Expected unchanged two-input HashJoinExec"
        );
    }

    #[test]
    fn test_rule_skips_non_inner_joins() {
        let rule = StarSchemaReorderRule::new(10);
        let config = ConfigOptions::new();

        let schema = make_schema(&[("id", DataType::Int64), ("val", DataType::Utf8)]);
        let a = make_memory_plan(schema.clone());
        let b = make_memory_plan(schema.clone());
        let c = make_memory_plan(schema.clone());

        // Create a LEFT join instead of INNER.
        let left_schema = a.schema();
        let right_schema = b.schema();
        let on = vec![(
            datafusion::physical_expr::expressions::col("id", &left_schema).unwrap(),
            datafusion::physical_expr::expressions::col("id", &right_schema).unwrap(),
        )];
        let left_join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                a,
                b,
                on,
                None,
                &JoinType::Left,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        let plan = make_inner_hash_join(left_join, c, "id", "id");

        let result = rule.optimize(plan.clone(), &config).unwrap();
        // The LEFT join is treated as an opaque leaf. The top INNER join
        // has only 2 inputs (LEFT subtree + C), so it doesn't reorder
        // (needs 3+ inputs). Plan remains unchanged.
        assert!(
            result.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Expected unchanged plan: LEFT join treated as leaf, only 2 inputs at INNER level"
        );
    }

    #[test]
    fn test_rule_name_and_schema_check() {
        let rule = StarSchemaReorderRule::new(DEFAULT_MIN_RATIO);
        assert_eq!(rule.name(), "StarSchemaReorderRule");
        assert!(rule.schema_check());
    }

    #[test]
    fn test_default_constructor() {
        let rule = StarSchemaReorderRule::default();
        assert_eq!(rule.min_ratio, DEFAULT_MIN_RATIO);
    }

    #[test]
    fn test_estimate_row_count_no_stats() {
        let schema = make_schema(&[("id", DataType::Int64)]);
        let plan = make_memory_plan(schema);
        // LazyMemoryExec with empty batches should give 0 rows.
        let count = estimate_row_count(&plan);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_extract_column_name() {
        let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("my_col", 0));
        assert_eq!(extract_column_name(&col), Some("my_col".to_string()));
    }

    #[test]
    fn test_flatten_single_inner_join() {
        let schema = make_schema(&[("id", DataType::Int64), ("val", DataType::Utf8)]);
        let left = make_memory_plan(schema.clone());
        let right = make_memory_plan(schema.clone());
        let plan = make_inner_hash_join(left, right, "id", "id");

        let mut inputs = Vec::new();
        let mut conditions = Vec::new();
        let eligible = flatten_join_chain(&plan, &mut inputs, &mut conditions);

        assert!(eligible);
        assert_eq!(inputs.len(), 2);
        assert_eq!(conditions.len(), 1);
    }

    #[test]
    fn test_flatten_chain_of_three() {
        let schema_a = make_schema(&[("a_id", DataType::Int64), ("a_val", DataType::Utf8)]);
        let schema_b = make_schema(&[("a_id", DataType::Int64), ("b_val", DataType::Utf8)]);
        let schema_c = make_schema(&[("c_id", DataType::Int64), ("c_val", DataType::Utf8)]);

        let a = make_memory_plan(schema_a.clone());
        let b = make_memory_plan(schema_b.clone());
        let c = make_memory_plan(schema_c.clone());

        // A JOIN B on a_id = a_id
        let ab = make_inner_hash_join(a, b, "a_id", "a_id");
        // (A JOIN B) JOIN C — we need a column that exists in the AB result.
        // AB schema has: a_id, a_val, a_id (from B), b_val
        // We join on a_id (from left) = c_id (from right).
        let ab_schema = ab.schema();
        let c_schema = c.schema();
        let on = vec![(
            datafusion::physical_expr::expressions::col("a_id", &ab_schema).unwrap(),
            datafusion::physical_expr::expressions::col("c_id", &c_schema).unwrap(),
        )];
        let abc: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                ab,
                c,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );

        let mut inputs = Vec::new();
        let mut conditions = Vec::new();
        let eligible = flatten_join_chain(&abc, &mut inputs, &mut conditions);

        assert!(eligible);
        assert_eq!(inputs.len(), 3, "Expected 3 leaf inputs in the chain");
        assert_eq!(conditions.len(), 2, "Expected 2 join conditions");
    }

    #[test]
    fn test_resolve_join_keys_simple() {
        let schema_left = make_schema(&[("id", DataType::Int64), ("name", DataType::Utf8)]);
        let schema_right = make_schema(&[("id", DataType::Int64), ("value", DataType::Float64)]);

        let left_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("id", 0));
        let right_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("id", 0));

        let result = resolve_join_keys(&[left_col], &[right_col], &schema_left, &schema_right);
        assert!(result.is_some());
        let on = result.unwrap();
        assert_eq!(on.len(), 1);
    }

    #[test]
    fn test_resolve_join_keys_missing_column() {
        let schema_left = make_schema(&[("id", DataType::Int64)]);
        let schema_right = make_schema(&[("other", DataType::Int64)]);

        let left_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("id", 0));
        let right_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("missing", 0));

        let result = resolve_join_keys(&[left_col], &[right_col], &schema_left, &schema_right);
        assert!(result.is_none());
    }

    // ── PLAN-01: wrong-column rebind on ambiguous join keys ──────────────

    /// Anchor test (correct-by-construction, trigger-independent): the
    /// unique-name resolver returns the index only when the name is unique,
    /// and bails on a duplicate. This is the core of the fix. Against the old
    /// `Schema::index_of` the duplicate case returned `Some(0)` (the bug).
    #[test]
    fn plan01_index_of_unique_bails_on_duplicate_name() {
        // Two `id` columns (the post-join accumulated-schema case) + one unique.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),   // 0
            Field::new("a_val", DataType::Utf8, true), // 1
            Field::new("id", DataType::Int64, true),   // 2  <- duplicate name
            Field::new("c_id", DataType::Int64, true), // 3  <- unique
        ]);
        assert_eq!(
            index_of_unique(&schema, "id"),
            None,
            "ambiguous name must NOT resolve (would silently bind wrong column)"
        );
        assert_eq!(
            index_of_unique(&schema, "c_id"),
            Some(3),
            "unique name resolves to its index"
        );
        assert_eq!(index_of_unique(&schema, "absent"), None, "missing name -> None");
    }

    /// `resolve_join_keys` must bail when the accumulated schema has a duplicate
    /// key name (the wrong-rebind hazard), but still succeed when unique.
    #[test]
    fn plan01_resolve_join_keys_bails_on_ambiguous_accumulated_key() {
        // Accumulated schema carries TWO `id` columns (post-join concatenation).
        let accumulated = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("a_val", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true), // duplicate
        ]));
        let new_schema = make_schema(&[("id", DataType::Int64), ("c_val", DataType::Utf8)]);

        let left_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("id", 0));
        let right_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("id", 0));

        let result = resolve_join_keys(&[left_col], &[right_col], &accumulated, &new_schema);
        assert!(
            result.is_none(),
            "ambiguous accumulated key must bail (keeps original plan), \
             not silently bind to the first `id`"
        );
    }

    /// Control vs subject at the `rule.optimize()` level, compared by plan
    /// STRUCTURE (stat-independent), defeating both the inert-trigger trap and
    /// the absent-join-statistics trap.
    ///
    /// Flatten collects left-first, so `((fact JOIN dimA) JOIN dimB)` yields
    /// inputs `[fact=100, dimA=2, dimB=2]`: not ascending (100 <= 2 is false),
    /// so `already_optimal` is false and ratio 50 >= 10 passes -- the rule
    /// reaches the rebuild path for BOTH cases. The difference is only at the
    /// join-key rebind:
    ///   - CONTROL (distinct FK names) resolves unambiguously and reorders, so
    ///     the plan string changes. This proves the gate actually fired.
    ///   - SUBJECT (shared `id`) hits a duplicate name in the accumulated
    ///     schema after the first join, so the fix bails (`Transformed::no`,
    ///     literally the same node) and the plan string is unchanged. Pre-fix,
    ///     `index_of` would bind to the wrong `id` and emit a (wrong) reorder,
    ///     changing the string -- so this assertion fails before the fix.
    #[test]
    fn plan01_control_reorders_subject_bails() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_plan::displayable;
        use arrow_array::{Int64Array, RecordBatch, StringArray};

        // Build an executable, statistics-bearing leaf from real rows.
        fn leaf(
            fields: &[(&str, DataType)],
            id_col: Vec<i64>,
            val_col: Vec<&str>,
        ) -> Arc<dyn ExecutionPlan> {
            let schema = make_schema(fields);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(id_col)),
                    Arc::new(StringArray::from(val_col)),
                ],
            )
            .unwrap();
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
        }

        let plan_str = |p: &Arc<dyn ExecutionPlan>| {
            displayable(p.as_ref()).indent(true).to_string()
        };

        let rule = StarSchemaReorderRule::new(10);
        let config = ConfigOptions::new();

        // --- CONTROL: distinct FK/PK names, big ratio -> must reorder. ---
        let fact: Arc<dyn ExecutionPlan> = leaf(
            &[("fact_d1", DataType::Int64), ("fact_val", DataType::Utf8)],
            (0..100).map(|i| i % 2).collect(),
            vec!["x"; 100],
        );
        let dim1: Arc<dyn ExecutionPlan> = leaf(
            &[("d1_id", DataType::Int64), ("d1_val", DataType::Utf8)],
            vec![0, 1],
            vec!["a", "b"],
        );
        let dim2: Arc<dyn ExecutionPlan> = leaf(
            &[("d2_id", DataType::Int64), ("d2_val", DataType::Utf8)],
            vec![0, 1],
            vec!["c", "d"],
        );
        let fd1 = make_inner_hash_join(fact, dim1, "fact_d1", "d1_id");
        // Chain dim2 onto dim1 via distinct names (d1_id = d2_id), NOT a pure
        // star off `fact`. A pure star (both joins keying off fact) can't be
        // linearly reordered: the greedy joins the two smallest (dim1, dim2)
        // first, but they share no key, so the rebuild bails -> control would
        // not reorder and the assertion would wrongly fail. With the chain,
        // dim1 ⋈ dim2 connects on d1_id=d2_id and ⋈ fact on d1_id=fact_d1, all
        // distinct names, so the reorder fires both pre- and post-fix.
        let control = make_inner_hash_join(fd1, dim2, "d1_id", "d2_id");

        let control_before = plan_str(&control);
        let control_out = rule.optimize(Arc::clone(&control), &config).unwrap();
        let control_after = plan_str(&control_out);
        assert_ne!(
            control_before, control_after,
            "CONTROL (distinct keys) must reorder -- if equal, the row-count / \
             ratio gate never fired and this whole test is inert"
        );

        // --- SUBJECT: shared `id` key across all three -> must bail. ---
        let fact2: Arc<dyn ExecutionPlan> = leaf(
            &[("id", DataType::Int64), ("fact_val", DataType::Utf8)],
            (0..100).map(|i| i % 2).collect(),
            vec!["x"; 100],
        );
        let dim_a: Arc<dyn ExecutionPlan> = leaf(
            &[("id", DataType::Int64), ("a_val", DataType::Utf8)],
            vec![0, 1],
            vec!["a", "b"],
        );
        let dim_b: Arc<dyn ExecutionPlan> = leaf(
            &[("id", DataType::Int64), ("b_val", DataType::Utf8)],
            vec![0, 1],
            vec!["c", "d"],
        );
        let fa = make_inner_hash_join(fact2, dim_a, "id", "id");
        let subject = make_inner_hash_join(fa, dim_b, "id", "id");

        let subject_before = plan_str(&subject);
        let subject_out = rule.optimize(Arc::clone(&subject), &config).unwrap();
        let subject_after = plan_str(&subject_out);
        assert_eq!(
            subject_before, subject_after,
            "SUBJECT (shared `id`) must bail and keep the original plan. A \
             reorder here would rebind the shared key to the wrong table's \
             column (same shape, WRONG rows). Pre-fix this assertion fails \
             because the buggy `index_of` emitted a wrong reorder."
        );
    }
}
