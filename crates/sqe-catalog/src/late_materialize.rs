//! Late materialization for Parquet scans via arrow-rs RowFilter.
//!
//! Two-phase scan strategy:
//! 1. **Phase 1**: Read only predicate columns, evaluate filter, produce a RowSelection
//! 2. **Phase 2**: Read remaining projection columns only for surviving rows
//!
//! This reduces I/O by 10-50x for selective queries on wide tables.
//!
//! Key types:
//! - [`ColumnClassification`] classifies columns as predicate or projection-only
//! - [`build_row_filter`] converts a DataFusion `PhysicalExpr` into a parquet `RowFilter`
//! - [`is_late_materialization_beneficial`] decides whether to enable the optimization
//! - [`PredicateOrderHint`] classifies individual predicates by evaluation cost
//! - [`order_predicates`] reorders predicates for optimal RowFilter evaluation

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{cast::AsArray, RecordBatch};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use tracing::debug;

/// Classification of projected columns into predicate and projection-only groups.
///
/// - `predicate_columns`: referenced in the WHERE clause filter expression.
///   These are read in Phase 1 of the two-phase scan.
/// - `projection_only_columns`: in SELECT but not in WHERE.
///   These are read in Phase 2, only for rows surviving the filter.
#[derive(Debug, Clone)]
pub struct ColumnClassification {
    /// Columns needed for predicate evaluation (Phase 1).
    pub predicate_columns: Vec<String>,
    /// Columns needed only for output (Phase 2, read only for surviving rows).
    pub projection_only_columns: Vec<String>,
}

impl ColumnClassification {
    /// Returns true if late materialization would be beneficial.
    ///
    /// Late materialization helps when there are projection-only columns
    /// that can be skipped during Phase 1. If all projected columns are
    /// also predicate columns, there is no benefit.
    pub fn is_beneficial(&self) -> bool {
        !self.projection_only_columns.is_empty() && !self.predicate_columns.is_empty()
    }
}

/// Walk a `PhysicalExpr` tree and collect all column name references.
///
/// Uses recursive descent through `PhysicalExpr::children()`, checking
/// each node for `Column` via `as_any().downcast_ref()`.
fn collect_column_refs(expr: &dyn PhysicalExpr) -> HashSet<String> {
    let mut columns = HashSet::new();
    collect_column_refs_inner(expr, &mut columns);
    columns
}

fn collect_column_refs_inner(expr: &dyn PhysicalExpr, columns: &mut HashSet<String>) {
    // Check if this node is a Column expression
    if let Some(col) = expr.downcast_ref::<Column>() {
        columns.insert(col.name().to_string());
    }

    // Recurse into children
    for child in expr.children() {
        collect_column_refs_inner(child.as_ref(), columns);
    }
}

/// Classify projected columns as predicate or projection-only based on
/// which columns the filter expression references.
///
/// # Arguments
/// - `predicate`: The filter expression (WHERE clause) as a `PhysicalExpr`
/// - `projection`: The list of column names in the SELECT projection
///
/// # Returns
/// A [`ColumnClassification`] with predicate columns and projection-only columns.
/// The predicate columns list preserves only columns that are also in the projection
/// schema, plus any additional predicate columns not in the projection (which the
/// reader needs for filter evaluation but won't emit in output).
pub fn classify_columns(
    predicate: &dyn PhysicalExpr,
    projection: &[String],
) -> ColumnClassification {
    let predicate_cols = collect_column_refs(predicate);

    let projection_only: Vec<String> = projection
        .iter()
        .filter(|col| !predicate_cols.contains(col.as_str()))
        .cloned()
        .collect();

    // Predicate columns: those referenced in the filter.
    // We return them in a stable order (sorted) for deterministic behavior.
    let mut predicate_columns: Vec<String> = predicate_cols.into_iter().collect();
    predicate_columns.sort();

    ColumnClassification {
        predicate_columns,
        projection_only_columns: projection_only,
    }
}

/// Determines whether late materialization is worthwhile for a given scan.
///
/// Returns `false` when:
/// - There is no predicate (nothing to filter on)
/// - All projected columns are predicate columns (no columns to defer)
/// - The number of projection-only columns is below `min_projection_cols`
///
/// The `min_projection_cols` parameter controls the minimum number of
/// deferrable (projection-only) columns required. Pass `1` to apply late
/// materialization whenever there is at least one deferrable column (default).
/// Pass `0` to disable late materialization entirely.
pub fn is_late_materialization_beneficial(
    predicate: Option<&dyn PhysicalExpr>,
    projection: &[String],
) -> bool {
    is_late_materialization_beneficial_with_threshold(predicate, projection, 1)
}

/// Like [`is_late_materialization_beneficial`] but with a configurable minimum
/// projection-only column threshold.
pub fn is_late_materialization_beneficial_with_threshold(
    predicate: Option<&dyn PhysicalExpr>,
    projection: &[String],
    min_projection_cols: usize,
) -> bool {
    let Some(pred) = predicate else {
        debug!("Late materialization skipped: no predicate");
        return false;
    };

    if min_projection_cols == 0 {
        debug!("Late materialization disabled via config (min_projection_cols=0)");
        return false;
    }

    let classification = classify_columns(pred, projection);

    if classification.predicate_columns.is_empty() {
        debug!("Late materialization skipped: no predicate columns identified");
        return false;
    }

    if classification.projection_only_columns.len() < min_projection_cols {
        debug!(
            predicate_cols = classification.predicate_columns.len(),
            projection_only_cols = classification.projection_only_columns.len(),
            min_required = min_projection_cols,
            "Late materialization skipped: too few projection-only columns"
        );
        return false;
    }

    debug!(
        predicate_cols = classification.predicate_columns.len(),
        projection_only_cols = classification.projection_only_columns.len(),
        "Late materialization applied: two-phase scan enabled"
    );
    true
}

/// Build a parquet `RowFilter` from a DataFusion `PhysicalExpr` predicate.
///
/// The RowFilter instructs the Parquet reader to:
/// 1. Decode only the predicate columns for each row group
/// 2. Evaluate the predicate closure, producing a `BooleanArray`
/// 3. Skip decoding remaining columns for rows where the predicate is false
///
/// # Arguments
/// - `predicate`: The filter expression to evaluate
/// - `predicate_schema`: Arrow schema containing only the predicate columns
/// - `parquet_schema`: The full Parquet file schema descriptor (for ProjectionMask)
///
/// # Returns
/// A `RowFilter` ready to pass to `ParquetRecordBatchStreamBuilder::with_row_filter()`.
pub fn build_row_filter(
    predicate: Arc<dyn PhysicalExpr>,
    predicate_schema: &SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> RowFilter {
    // Build a ProjectionMask that selects only the predicate columns
    // from the full Parquet schema.
    let predicate_col_indices: Vec<usize> = predicate_schema
        .fields()
        .iter()
        .filter_map(|field| {
            // Find the root column index in the Parquet schema
            parquet_schema
                .columns()
                .iter()
                .position(|col| col.name() == field.name().as_str())
        })
        .collect();

    let projection_mask = ProjectionMask::roots(parquet_schema, predicate_col_indices);

    let arrow_predicate = ArrowPredicateFn::new(projection_mask, move |batch: RecordBatch| {
        // The batch contains only predicate columns (per the ProjectionMask).
        // The PhysicalExpr Column references use indices from the
        // predicate_schema, which must match the batch column order.
        let result = predicate
            .evaluate(&batch)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        // Convert ColumnarValue to a BooleanArray
        match result {
            ColumnarValue::Array(array) => {
                let bool_array = array.as_boolean().clone();
                Ok(bool_array)
            }
            ColumnarValue::Scalar(scalar) => {
                // Scalar true/false -- expand to array
                let bool_val = scalar
                    .to_array_of_size(batch.num_rows())
                    .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                let bool_array = bool_val.as_boolean().clone();
                Ok(bool_array)
            }
        }
    });

    RowFilter::new(vec![Box::new(arrow_predicate)])
}

/// Build a predicate schema containing only the columns referenced by the filter.
///
/// This schema is used both for the `ProjectionMask` in the RowFilter and for
/// remapping the predicate expression's column indices.
pub fn build_predicate_schema(
    classification: &ColumnClassification,
    full_schema: &SchemaRef,
) -> SchemaRef {
    // Field order MUST follow the full (file) schema, not the alphabetical
    // order classify_columns uses for its column set: the parquet reader's
    // phase-1 batch delivers predicate columns in file order regardless of
    // the index order handed to ProjectionMask::roots, and the remapped
    // Column indices are resolved against THIS schema. Alphabetical order
    // misaligned every index whenever it differed from file order: typed
    // mismatches failed the scan ("Invalid comparison operation: Utf8 ==
    // Int32" on TPC-C order_status), and same-typed mismatches silently
    // evaluated the wrong comparison and dropped rows.
    let mut indexed: Vec<(usize, Arc<Field>)> = classification
        .predicate_columns
        .iter()
        .filter_map(|name| {
            full_schema
                .index_of(name)
                .ok()
                .map(|i| (i, Arc::new(full_schema.field(i).clone())))
        })
        .collect();
    indexed.sort_by_key(|(i, _)| *i);

    Arc::new(Schema::new(
        indexed.into_iter().map(|(_, f)| f).collect::<Vec<_>>(),
    ))
}

/// Remap a `PhysicalExpr` tree so that `Column` indices reference the predicate
/// schema instead of the full table schema.
///
/// When we evaluate the predicate against a batch containing only predicate
/// columns, the column indices must correspond to positions in that smaller
/// schema, not the full table schema.
pub fn remap_predicate_columns(
    expr: &Arc<dyn PhysicalExpr>,
    predicate_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, datafusion::error::DataFusionError> {
    remap_expr(expr, predicate_schema)
}

fn remap_expr(
    expr: &Arc<dyn PhysicalExpr>,
    target_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, datafusion::error::DataFusionError> {
    if let Some(col) = expr.downcast_ref::<Column>() {
        // Find this column's index in the target schema
        let new_index = target_schema.index_of(col.name()).map_err(|_| {
            datafusion::error::DataFusionError::Internal(format!(
                "Column '{}' not found in predicate schema",
                col.name()
            ))
        })?;
        return Ok(Arc::new(Column::new(col.name(), new_index)));
    }

    // Recursively remap children
    let children = expr.children();
    if children.is_empty() {
        return Ok(Arc::clone(expr));
    }

    let new_children: Vec<Arc<dyn PhysicalExpr>> = children
        .iter()
        .map(|child| remap_expr(child, target_schema))
        .collect::<Result<Vec<_>, _>>()?;

    expr.clone().with_new_children(new_children)
}

// ────────────────────────────────────────────────────────────────────
// Task 6: CachedArrayReader / shared column verification
// ────────────────────────────────────────────────────────────────────
//
// In parquet 57, the `ArrowReaderBuilder` exposes `with_max_predicate_cache_size()`
// which controls a built-in cache for decoded predicate column arrays. When a column
// appears in both the RowFilter predicate AND the output projection, the reader
// caches the decoded array from Phase 1 and reuses it in Phase 2 output, avoiding
// a redundant decode. This is the "CachedArrayReader" behavior.
//
// Verification:
// - `ArrowReaderBuilder::with_max_predicate_cache_size(usize)` is available in
//   parquet 57.3.0 (confirmed via generated docs).
// - The default cache size is non-zero, meaning caching is enabled by default.
// - The `ArrowPredicate::projection()` method tells the reader which columns the
//   predicate needs; the reader internally caches those decoded arrays.
// - No manual caching implementation is required.
//
// See `test_row_filter_construction` below for compile-time API verification.

// ────────────────────────────────────────────────────────────────────
// Task 23: Predicate ordering optimization
// ────────────────────────────────────────────────────────────────────

/// Classification of a predicate's evaluation cost tier.
///
/// When multiple predicates exist in a query's WHERE clause, evaluating the
/// cheapest and most selective predicates first maximizes early row elimination
/// and minimizes I/O. This enum defines the priority tiers from cheapest to
/// most expensive.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PredicateCostTier {
    /// Tier 0: Predicate on a partition column.
    /// These are effectively free — they are evaluated at the manifest level
    /// during file pruning, not at the row level. Including them in the
    /// RowFilter is redundant but harmless and provides defense-in-depth.
    PartitionColumn = 0,

    /// Tier 1: Predicate on a column with bloom filter support.
    /// Bloom filters allow skipping row groups without decoding column data.
    /// Very cheap and often highly selective (e.g., point lookups on ID columns).
    BloomFilter = 1,

    /// Tier 2: Predicate on a sort-order column.
    /// Sort-order columns have the best zone map (min/max) statistics because
    /// data is clustered. Row group pruning via min/max is very effective.
    SortOrderColumn = 2,

    /// Tier 3: Predicate on a regular column with available statistics.
    /// Uses generic min/max statistics for pruning. Less effective than
    /// sort-order columns but still provides some benefit.
    RegularWithStats = 3,

    /// Tier 4: Predicate on a column with no special properties.
    /// Must be evaluated by decoding column data — the most expensive tier.
    Regular = 4,
}

/// A predicate annotated with its evaluation cost and estimated selectivity.
#[derive(Debug, Clone)]
pub struct OrderedPredicate {
    /// The predicate expression.
    pub expr: Arc<dyn PhysicalExpr>,
    /// Column names referenced by this predicate.
    pub columns: Vec<String>,
    /// Cost tier for this predicate.
    pub cost_tier: PredicateCostTier,
    /// Estimated selectivity (0.0 = filters everything, 1.0 = keeps everything).
    /// Lower is better (more selective). None if unknown.
    pub estimated_selectivity: Option<f64>,
}

/// Metadata about the table's columns used for predicate ordering.
///
/// This struct carries information from Iceberg table metadata that helps
/// classify predicates by their evaluation cost.
#[derive(Debug, Clone, Default)]
pub struct PredicateOrderingContext {
    /// Names of partition columns (predicates on these are free).
    pub partition_columns: HashSet<String>,
    /// Names of columns with bloom filter support.
    pub bloom_filter_columns: HashSet<String>,
    /// Names of columns in the table's sort order (identity transform only).
    pub sort_order_columns: Vec<String>,
    /// Names of columns with available statistics (min/max/null_count).
    pub columns_with_stats: HashSet<String>,
    /// Optional estimated selectivity per column name (from column stats).
    /// Values between 0.0 (filters everything) and 1.0 (keeps everything).
    pub column_selectivity: Vec<(String, f64)>,
}

/// Decompose a conjunction (AND-connected) predicate into individual terms.
///
/// Walks the expression tree and splits at AND boundaries. Non-AND expressions
/// are returned as a single-element vector.
pub fn decompose_conjunction(expr: &Arc<dyn PhysicalExpr>) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut terms = Vec::new();
    decompose_conjunction_inner(expr, &mut terms);
    if terms.is_empty() {
        // Not a conjunction — return the expression itself
        terms.push(Arc::clone(expr));
    }
    terms
}

fn decompose_conjunction_inner(
    expr: &Arc<dyn PhysicalExpr>,
    terms: &mut Vec<Arc<dyn PhysicalExpr>>,
) {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_plan::expressions::BinaryExpr;

    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        if *binary.op() == Operator::And {
            decompose_conjunction_inner(binary.left(), terms);
            decompose_conjunction_inner(binary.right(), terms);
            return;
        }
    }
    // Not an AND — this is a leaf predicate term
    terms.push(Arc::clone(expr));
}

/// Classify a predicate's cost tier based on which columns it references
/// and the table's metadata context.
fn classify_predicate_cost(
    columns: &[String],
    ctx: &PredicateOrderingContext,
) -> PredicateCostTier {
    // A predicate's tier is determined by the "best" column it references.
    // If any referenced column is a partition column, it's tier 0, etc.

    let mut best_tier = PredicateCostTier::Regular;

    for col in columns {
        let tier = if ctx.partition_columns.contains(col) {
            PredicateCostTier::PartitionColumn
        } else if ctx.bloom_filter_columns.contains(col) {
            PredicateCostTier::BloomFilter
        } else if ctx.sort_order_columns.contains(col) {
            PredicateCostTier::SortOrderColumn
        } else if ctx.columns_with_stats.contains(col) {
            PredicateCostTier::RegularWithStats
        } else {
            PredicateCostTier::Regular
        };

        if tier < best_tier {
            best_tier = tier.clone();
        }
    }

    best_tier
}

/// Estimate selectivity for a predicate based on its referenced columns.
///
/// Uses the minimum selectivity of any referenced column (most selective wins).
fn estimate_predicate_selectivity(
    columns: &[String],
    ctx: &PredicateOrderingContext,
) -> Option<f64> {
    let mut min_selectivity: Option<f64> = None;

    for col in columns {
        for (name, sel) in &ctx.column_selectivity {
            if name == col {
                min_selectivity = Some(match min_selectivity {
                    Some(current) => current.min(*sel),
                    None => *sel,
                });
            }
        }
    }

    min_selectivity
}

/// Order predicates by evaluation cost and estimated selectivity.
///
/// When multiple predicates exist in a WHERE clause (connected by AND),
/// this function determines the optimal evaluation order:
///
/// 1. **Partition column predicates** (free at manifest level)
/// 2. **Predicates with bloom filter support** (very cheap)
/// 3. **Predicates on sort-order columns** (best zone map pruning)
/// 4. **Remaining predicates by estimated selectivity** (most selective first)
///
/// This ordering maximizes the filtering effect of early predicates in the
/// RowFilter, reducing the number of rows that need to be evaluated by
/// later (more expensive) predicates.
///
/// # Arguments
/// - `predicate`: The combined filter expression (may be a conjunction of ANDs).
/// - `ctx`: Metadata context about the table's columns.
///
/// # Returns
/// A vector of [`OrderedPredicate`]s sorted from cheapest/most selective to
/// most expensive/least selective.
pub fn order_predicates(
    predicate: &Arc<dyn PhysicalExpr>,
    ctx: &PredicateOrderingContext,
) -> Vec<OrderedPredicate> {
    let terms = decompose_conjunction(predicate);

    let mut ordered: Vec<OrderedPredicate> = terms
        .into_iter()
        .map(|expr| {
            let columns: Vec<String> = collect_column_refs(expr.as_ref()).into_iter().collect();
            let cost_tier = classify_predicate_cost(&columns, ctx);
            let estimated_selectivity = estimate_predicate_selectivity(&columns, ctx);

            OrderedPredicate {
                expr,
                columns,
                cost_tier,
                estimated_selectivity,
            }
        })
        .collect();

    // Sort by: (1) cost tier ascending, (2) selectivity ascending (most selective first)
    ordered.sort_by(|a, b| {
        a.cost_tier.cmp(&b.cost_tier).then_with(|| {
            match (a.estimated_selectivity, b.estimated_selectivity) {
                (Some(sa), Some(sb)) => sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal),
                (Some(_), None) => std::cmp::Ordering::Less, // known selectivity first
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        })
    });

    ordered
}

/// Build a multi-stage `RowFilter` from ordered predicates.
///
/// Each predicate becomes a separate stage in the RowFilter, evaluated in
/// order. The Parquet reader evaluates them sequentially: rows eliminated
/// by stage N are not evaluated by stage N+1. This means putting the
/// cheapest/most selective predicate first minimizes total I/O.
///
/// # Arguments
/// - `ordered_predicates`: Predicates sorted by [`order_predicates`].
/// - `predicate_schema`: Arrow schema containing only the predicate columns.
/// - `parquet_schema`: The full Parquet file schema descriptor.
///
/// # Returns
/// A `RowFilter` with one stage per predicate, in optimal evaluation order.
pub fn build_ordered_row_filter(
    ordered_predicates: &[OrderedPredicate],
    _predicate_schema: &SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> RowFilter {
    let stages: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = ordered_predicates
        .iter()
        .map(|op| {
            let pred = Arc::clone(&op.expr);

            // Build a ProjectionMask for this specific predicate's columns
            let col_indices: Vec<usize> = op
                .columns
                .iter()
                .filter_map(|col_name| {
                    parquet_schema
                        .columns()
                        .iter()
                        .position(|c| c.name() == col_name.as_str())
                })
                .collect();

            let projection_mask = ProjectionMask::roots(parquet_schema, col_indices);

            Box::new(ArrowPredicateFn::new(
                projection_mask,
                move |batch: RecordBatch| {
                    let result = pred
                        .evaluate(&batch)
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                    match result {
                        ColumnarValue::Array(array) => {
                            let bool_array = array.as_boolean().clone();
                            Ok(bool_array)
                        }
                        ColumnarValue::Scalar(scalar) => {
                            let bool_val = scalar
                                .to_array_of_size(batch.num_rows())
                                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                            let bool_array = bool_val.as_boolean().clone();
                            Ok(bool_array)
                        }
                    }
                },
            )) as Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>
        })
        .collect();

    RowFilter::new(stages)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::expressions::{self, Column, Literal};
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::scalar::ScalarValue;

    /// Helper to build a schema with named columns.
    fn test_schema(names: &[&str]) -> SchemaRef {
        let fields: Vec<Field> = names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Helper to build a Column physical expr referencing a column by name and index.
    fn col_expr(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    /// Helper to build a literal i64 value.
    fn lit_i64(val: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int64(Some(val))))
    }

    // ── Task 4 tests: column classification ─────────────────────────

    #[test]
    fn test_single_predicate_column() {
        // WHERE a > 10, projection [a, b, c]
        let schema = test_schema(&["a", "b", "c"]);
        let a = col_expr("a", 0);
        let ten = lit_i64(10);
        let predicate =
            expressions::binary(a, datafusion::logical_expr::Operator::Gt, ten, &schema)
                .expect("build binary expr");

        let projection = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let result = classify_columns(predicate.as_ref(), &projection);

        assert_eq!(result.predicate_columns, vec!["a"]);
        assert_eq!(result.projection_only_columns, vec!["b", "c"]);
        assert!(result.is_beneficial());
    }

    #[test]
    fn test_two_predicate_columns() {
        // WHERE a > 10 AND b = 42, projection [a, b, c]
        let schema = test_schema(&["a", "b", "c"]);
        let a_gt_10 = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build a > 10");

        let b_eq_42 = expressions::binary(
            col_expr("b", 1),
            datafusion::logical_expr::Operator::Eq,
            lit_i64(42),
            &schema,
        )
        .expect("build b = 42");

        let predicate = expressions::binary(
            a_gt_10,
            datafusion::logical_expr::Operator::And,
            b_eq_42,
            &schema,
        )
        .expect("build AND");

        let projection = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let result = classify_columns(predicate.as_ref(), &projection);

        assert!(result.predicate_columns.contains(&"a".to_string()));
        assert!(result.predicate_columns.contains(&"b".to_string()));
        assert_eq!(result.predicate_columns.len(), 2);
        assert_eq!(result.projection_only_columns, vec!["c"]);
        assert!(result.is_beneficial());
    }

    #[test]
    fn test_all_columns_are_predicate() {
        // WHERE a > 10, projection [a]
        let schema = test_schema(&["a"]);
        let predicate = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build binary expr");

        let projection = vec!["a".to_string()];
        let result = classify_columns(predicate.as_ref(), &projection);

        assert_eq!(result.predicate_columns, vec!["a"]);
        assert!(result.projection_only_columns.is_empty());
        assert!(!result.is_beneficial());
    }

    #[test]
    fn test_no_predicate_benefit_check() {
        // No predicate -- late materialization is not beneficial
        let projection = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert!(!is_late_materialization_beneficial(None, &projection));
    }

    #[test]
    fn test_beneficial_with_predicate() {
        // WHERE a > 10, projection [a, b, c] -- beneficial
        let schema = test_schema(&["a", "b", "c"]);
        let predicate = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build binary expr");

        let projection = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert!(is_late_materialization_beneficial(
            Some(predicate.as_ref()),
            &projection
        ));
    }

    #[test]
    fn test_not_beneficial_all_predicate_cols() {
        // WHERE a > 10, projection [a] -- not beneficial
        let schema = test_schema(&["a"]);
        let predicate = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build binary expr");

        let projection = vec!["a".to_string()];
        assert!(!is_late_materialization_beneficial(
            Some(predicate.as_ref()),
            &projection
        ));
    }

    // ── Column remapping tests ──────────────────────────────────────

    #[test]
    fn test_remap_predicate_columns() {
        // Full schema: [a(0), b(1), c(2)]
        // Predicate schema: [b(0)] (only 'b' is in predicate)
        // Column 'b' at index 1 in full schema -> index 0 in predicate schema
        let predicate_schema = test_schema(&["b"]);
        let expr: Arc<dyn PhysicalExpr> = col_expr("b", 1);

        let remapped =
            remap_predicate_columns(&expr, &predicate_schema).expect("remap should succeed");

        let col = remapped.downcast_ref::<Column>().expect("should be Column");
        assert_eq!(col.name(), "b");
        assert_eq!(col.index(), 0);
    }

    #[test]
    fn test_remap_compound_predicate() {
        // Full schema: [a(0), b(1), c(2), d(3)]
        // Predicate: a > 10 AND c = 42
        // Predicate schema: [a(0), c(1)]
        let full_schema = test_schema(&["a", "b", "c", "d"]);
        let predicate_schema = test_schema(&["a", "c"]);

        let a_gt_10 = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &full_schema,
        )
        .expect("build a > 10");

        let c_eq_42 = expressions::binary(
            col_expr("c", 2), // index 2 in full schema
            datafusion::logical_expr::Operator::Eq,
            lit_i64(42),
            &full_schema,
        )
        .expect("build c = 42");

        let predicate = expressions::binary(
            a_gt_10,
            datafusion::logical_expr::Operator::And,
            c_eq_42,
            &full_schema,
        )
        .expect("build AND");

        let remapped =
            remap_predicate_columns(&predicate, &predicate_schema).expect("remap should succeed");

        // Collect column refs from the remapped expression
        let cols = collect_column_refs(remapped.as_ref());
        assert!(cols.contains("a"));
        assert!(cols.contains("c"));

        // Verify we can evaluate against a batch with predicate schema
        let a_array = arrow_array::Int64Array::from(vec![15, 5, 20]);
        let c_array = arrow_array::Int64Array::from(vec![42, 42, 10]);
        let batch = RecordBatch::try_new(
            predicate_schema.clone(),
            vec![Arc::new(a_array), Arc::new(c_array)],
        )
        .expect("build batch");

        let result = remapped.evaluate(&batch).expect("evaluate");
        let bool_arr = match result {
            ColumnarValue::Array(a) => a.as_boolean().clone(),
            _ => panic!("expected array"),
        };
        // Row 0: a=15>10 AND c=42=42 -> true
        // Row 1: a=5>10 -> false
        // Row 2: a=20>10 AND c=10=42 -> false
        assert!(bool_arr.value(0));
        assert!(!bool_arr.value(1));
        assert!(!bool_arr.value(2));
    }

    // ── Build predicate schema tests ────────────────────────────────

    #[test]
    fn test_build_predicate_schema() {
        let full_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
            Field::new("d", DataType::Boolean, true),
        ]));

        let classification = ColumnClassification {
            predicate_columns: vec!["a".to_string(), "c".to_string()],
            projection_only_columns: vec!["b".to_string(), "d".to_string()],
        };

        let pred_schema = build_predicate_schema(&classification, &full_schema);
        assert_eq!(pred_schema.fields().len(), 2);
        assert_eq!(pred_schema.field(0).name(), "a");
        assert_eq!(pred_schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(pred_schema.field(1).name(), "c");
        assert_eq!(pred_schema.field(1).data_type(), &DataType::Float64);
    }

    // ── Task 6 tests: shared column caching verification ────────────

    /// Verify that parquet's RowFilter + predicate cache prevents double-read
    /// of columns that appear in both the predicate and the projection.
    ///
    /// In parquet 57, `ArrowReaderBuilder::with_max_predicate_cache_size()`
    /// controls the built-in cache that stores decoded arrays from predicate
    /// evaluation. When a column is needed for both filtering and projection,
    /// the reader caches the predicate column's decoded array and reuses it
    /// for the final output batch, avoiding a second decode pass.
    ///
    /// This test constructs a RowFilter for a predicate on column 'a' and
    /// verifies the API chain compiles correctly. Full end-to-end verification
    /// that the cache prevents double I/O requires reading from an actual
    /// Parquet file (covered by integration tests).
    #[test]
    fn test_row_filter_construction() {
        // Verify that build_row_filter produces a valid RowFilter.
        // This is a compile-time + API correctness test.
        let predicate_schema = test_schema(&["a"]);
        let predicate: Arc<dyn PhysicalExpr> = col_expr("a", 0);

        // Build a minimal Parquet schema descriptor for the mask
        use parquet::schema::types::Type;
        let parquet_fields = vec![Arc::new(
            Type::primitive_type_builder("a", parquet::basic::Type::INT64)
                .build()
                .expect("build parquet type"),
        )];
        let parquet_schema = SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(parquet_fields)
                .build()
                .expect("build group type"),
        ));

        let _row_filter = build_row_filter(predicate, &predicate_schema, &parquet_schema);
        // If we get here, the RowFilter was constructed successfully.
    }

    /// Verify that with_max_predicate_cache_size API is available in parquet 57.
    /// This confirms the CachedArrayReader mechanism exists without needing
    /// manual implementation.
    ///
    /// The key insight for Task 6: parquet 57's ArrowReaderBuilder has
    /// `with_max_predicate_cache_size()` which controls how many decoded
    /// predicate column arrays are cached. The default is non-zero,
    /// meaning shared columns (appearing in both WHERE and SELECT) are
    /// automatically cached and not decoded twice. No manual CachedArrayReader
    /// implementation is needed.
    #[test]
    fn test_predicate_cache_api_available() {
        // Build a minimal Parquet file in memory and verify we can call
        // with_max_predicate_cache_size on the builder.
        use arrow_array::Int64Array;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let schema = test_schema(&["a"]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("build batch");

        // Write a Parquet file to memory
        let mut buf = Vec::new();
        {
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None)
                .expect("create writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        // Build reader and verify with_max_predicate_cache_size is callable
        let reader = bytes::Bytes::from(buf);
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(reader).expect("create reader builder");
        let _builder = builder.with_max_predicate_cache_size(1024);
        // If we get here, the predicate cache API is available and functional.
    }

    // ── Task 23 tests: predicate ordering optimization ─────────────

    #[test]
    fn test_decompose_conjunction_single_predicate() {
        let schema = test_schema(&["a"]);
        let pred = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build a > 10");

        let terms = decompose_conjunction(&pred);
        assert_eq!(terms.len(), 1, "Single predicate should produce 1 term");
    }

    #[test]
    fn test_decompose_conjunction_two_and() {
        let schema = test_schema(&["a", "b"]);
        let a_gt_10 = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build a > 10");

        let b_eq_42 = expressions::binary(
            col_expr("b", 1),
            datafusion::logical_expr::Operator::Eq,
            lit_i64(42),
            &schema,
        )
        .expect("build b = 42");

        let combined = expressions::binary(
            a_gt_10,
            datafusion::logical_expr::Operator::And,
            b_eq_42,
            &schema,
        )
        .expect("build AND");

        let terms = decompose_conjunction(&combined);
        assert_eq!(
            terms.len(),
            2,
            "AND of two predicates should produce 2 terms"
        );
    }

    #[test]
    fn test_decompose_conjunction_nested_and() {
        let schema = test_schema(&["a", "b", "c"]);
        let a_pred = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build a > 10");

        let b_pred = expressions::binary(
            col_expr("b", 1),
            datafusion::logical_expr::Operator::Eq,
            lit_i64(42),
            &schema,
        )
        .expect("build b = 42");

        let c_pred = expressions::binary(
            col_expr("c", 2),
            datafusion::logical_expr::Operator::Lt,
            lit_i64(100),
            &schema,
        )
        .expect("build c < 100");

        // (a > 10 AND b = 42) AND c < 100
        let ab = expressions::binary(
            a_pred,
            datafusion::logical_expr::Operator::And,
            b_pred,
            &schema,
        )
        .expect("build a AND b");

        let abc = expressions::binary(ab, datafusion::logical_expr::Operator::And, c_pred, &schema)
            .expect("build (a AND b) AND c");

        let terms = decompose_conjunction(&abc);
        assert_eq!(
            terms.len(),
            3,
            "Nested AND of 3 predicates should produce 3 terms"
        );
    }

    #[test]
    fn test_decompose_conjunction_or_not_split() {
        let schema = test_schema(&["a", "b"]);
        let a_pred = expressions::binary(
            col_expr("a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build a > 10");

        let b_pred = expressions::binary(
            col_expr("b", 1),
            datafusion::logical_expr::Operator::Eq,
            lit_i64(42),
            &schema,
        )
        .expect("build b = 42");

        // a > 10 OR b = 42 -- OR should NOT be split
        let or_pred = expressions::binary(
            a_pred,
            datafusion::logical_expr::Operator::Or,
            b_pred,
            &schema,
        )
        .expect("build OR");

        let terms = decompose_conjunction(&or_pred);
        assert_eq!(terms.len(), 1, "OR predicate should not be decomposed");
    }

    #[test]
    fn test_predicate_cost_tier_ordering() {
        // Verify the Ord implementation matches our priority
        assert!(PredicateCostTier::PartitionColumn < PredicateCostTier::BloomFilter);
        assert!(PredicateCostTier::BloomFilter < PredicateCostTier::SortOrderColumn);
        assert!(PredicateCostTier::SortOrderColumn < PredicateCostTier::RegularWithStats);
        assert!(PredicateCostTier::RegularWithStats < PredicateCostTier::Regular);
    }

    #[test]
    fn test_order_predicates_partition_first() {
        let schema = test_schema(&["part_col", "regular_col"]);

        // Two predicates: one on partition column, one on regular column
        let part_pred = expressions::binary(
            col_expr("part_col", 0),
            datafusion::logical_expr::Operator::Eq,
            lit_i64(1),
            &schema,
        )
        .expect("build part_col = 1");

        let regular_pred = expressions::binary(
            col_expr("regular_col", 1),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(100),
            &schema,
        )
        .expect("build regular_col > 100");

        // Combine with regular first (wrong order)
        let combined = expressions::binary(
            regular_pred,
            datafusion::logical_expr::Operator::And,
            part_pred,
            &schema,
        )
        .expect("build AND");

        let ctx = PredicateOrderingContext {
            partition_columns: HashSet::from(["part_col".to_string()]),
            ..Default::default()
        };

        let ordered = order_predicates(&combined, &ctx);
        assert_eq!(ordered.len(), 2);
        // Partition column predicate should come first
        assert_eq!(ordered[0].cost_tier, PredicateCostTier::PartitionColumn);
        assert!(ordered[0].columns.contains(&"part_col".to_string()));
        assert_eq!(ordered[1].cost_tier, PredicateCostTier::Regular);
    }

    #[test]
    fn test_order_predicates_sort_order_before_regular() {
        let schema = test_schema(&["sorted_col", "regular_col"]);

        let sorted_pred = expressions::binary(
            col_expr("sorted_col", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(50),
            &schema,
        )
        .expect("build sorted_col > 50");

        let regular_pred = expressions::binary(
            col_expr("regular_col", 1),
            datafusion::logical_expr::Operator::Eq,
            lit_i64(42),
            &schema,
        )
        .expect("build regular_col = 42");

        // Put regular first
        let combined = expressions::binary(
            regular_pred,
            datafusion::logical_expr::Operator::And,
            sorted_pred,
            &schema,
        )
        .expect("build AND");

        let ctx = PredicateOrderingContext {
            sort_order_columns: vec!["sorted_col".to_string()],
            ..Default::default()
        };

        let ordered = order_predicates(&combined, &ctx);
        assert_eq!(ordered.len(), 2);
        // Sort-order column should come first
        assert_eq!(ordered[0].cost_tier, PredicateCostTier::SortOrderColumn);
        assert_eq!(ordered[1].cost_tier, PredicateCostTier::Regular);
    }

    #[test]
    fn test_order_predicates_full_priority_chain() {
        let schema = test_schema(&[
            "part_col",
            "bloom_col",
            "sort_col",
            "stats_col",
            "plain_col",
        ]);

        let make_pred = |name: &str, idx: usize| {
            expressions::binary(
                col_expr(name, idx),
                datafusion::logical_expr::Operator::Eq,
                lit_i64(1),
                &schema,
            )
            .expect("build pred")
        };

        // Build: plain AND stats AND sort AND bloom AND part (reverse order)
        let p4 = make_pred("plain_col", 4);
        let p3 = make_pred("stats_col", 3);
        let p2 = make_pred("sort_col", 2);
        let p1 = make_pred("bloom_col", 1);
        let p0 = make_pred("part_col", 0);

        let and1 =
            expressions::binary(p4, datafusion::logical_expr::Operator::And, p3, &schema).unwrap();
        let and2 = expressions::binary(and1, datafusion::logical_expr::Operator::And, p2, &schema)
            .unwrap();
        let and3 = expressions::binary(and2, datafusion::logical_expr::Operator::And, p1, &schema)
            .unwrap();
        let combined =
            expressions::binary(and3, datafusion::logical_expr::Operator::And, p0, &schema)
                .unwrap();

        let ctx = PredicateOrderingContext {
            partition_columns: HashSet::from(["part_col".to_string()]),
            bloom_filter_columns: HashSet::from(["bloom_col".to_string()]),
            sort_order_columns: vec!["sort_col".to_string()],
            columns_with_stats: HashSet::from(["stats_col".to_string()]),
            column_selectivity: vec![],
        };

        let ordered = order_predicates(&combined, &ctx);
        assert_eq!(ordered.len(), 5);

        assert_eq!(ordered[0].cost_tier, PredicateCostTier::PartitionColumn);
        assert_eq!(ordered[1].cost_tier, PredicateCostTier::BloomFilter);
        assert_eq!(ordered[2].cost_tier, PredicateCostTier::SortOrderColumn);
        assert_eq!(ordered[3].cost_tier, PredicateCostTier::RegularWithStats);
        assert_eq!(ordered[4].cost_tier, PredicateCostTier::Regular);
    }

    #[test]
    fn test_order_predicates_selectivity_breaks_ties() {
        let schema = test_schema(&["col_a", "col_b"]);

        let a_pred = expressions::binary(
            col_expr("col_a", 0),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build col_a > 10");

        let b_pred = expressions::binary(
            col_expr("col_b", 1),
            datafusion::logical_expr::Operator::Gt,
            lit_i64(10),
            &schema,
        )
        .expect("build col_b > 10");

        // Both are regular columns (same tier), but col_b is more selective
        let combined = expressions::binary(
            a_pred,
            datafusion::logical_expr::Operator::And,
            b_pred,
            &schema,
        )
        .expect("build AND");

        let ctx = PredicateOrderingContext {
            column_selectivity: vec![
                ("col_a".to_string(), 0.8), // 80% pass (not very selective)
                ("col_b".to_string(), 0.1), // 10% pass (very selective)
            ],
            ..Default::default()
        };

        let ordered = order_predicates(&combined, &ctx);
        assert_eq!(ordered.len(), 2);
        // col_b should come first (lower selectivity = more selective)
        assert!(ordered[0].columns.contains(&"col_b".to_string()));
        assert!(ordered[1].columns.contains(&"col_a".to_string()));
    }

    #[test]
    fn test_classify_predicate_cost_empty_context() {
        let ctx = PredicateOrderingContext::default();
        let tier = classify_predicate_cost(&["any_col".to_string()], &ctx);
        assert_eq!(tier, PredicateCostTier::Regular);
    }

    #[test]
    fn test_predicate_ordering_context_default() {
        let ctx = PredicateOrderingContext::default();
        assert!(ctx.partition_columns.is_empty());
        assert!(ctx.bloom_filter_columns.is_empty());
        assert!(ctx.sort_order_columns.is_empty());
        assert!(ctx.columns_with_stats.is_empty());
        assert!(ctx.column_selectivity.is_empty());
    }

    /// Regression: predicate-schema column order must follow FILE order, not
    /// alphabetical order. The parquet reader's phase-1 batch delivers
    /// predicate columns in file-schema order regardless of the order of the
    /// indices passed to ProjectionMask::roots. classify_columns sorts names
    /// alphabetically; building the predicate schema in that order misaligns
    /// every remapped Column index whenever alphabetical order differs from
    /// file order. Observed on TPC-C order_status (customer predicate on
    /// c_w_id, c_d_id, c_last): the remapped `c_last = 'BARBARBAR'` pointed at
    /// the Int32 c_w_id column ("Invalid comparison operation: Utf8 == Int32").
    /// When the swapped columns share a type the filter evaluates the WRONG
    /// comparison silently and discards rows the coordinator never sees.
    #[test]
    fn predicate_schema_follows_file_order_not_alphabetical() {
        use arrow_array::{Int32Array, RecordBatch, StringArray};

        // File order deliberately differs from alphabetical order of the
        // predicate columns: file = [c_d_id, c_w_id, c_last], predicate cols
        // sorted alphabetically = [c_d_id, c_last, c_w_id].
        let full_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c_d_id", DataType::Int32, true),
            Field::new("c_w_id", DataType::Int32, true),
            Field::new("c_last", DataType::Utf8, true),
        ]));

        // Predicate: c_w_id = 1 AND c_last = 'BARBARBAR' (indices vs full schema)
        let pred_w = expressions::binary(
            col_expr("c_w_id", 1),
            datafusion::logical_expr::Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            &full_schema,
        )
        .expect("build c_w_id = 1");
        let pred_last = expressions::binary(
            col_expr("c_last", 2),
            datafusion::logical_expr::Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("BARBARBAR".into())))),
            &full_schema,
        )
        .expect("build c_last = literal");
        let predicate = expressions::binary(
            pred_w,
            datafusion::logical_expr::Operator::And,
            pred_last,
            &full_schema,
        )
        .expect("build AND");

        let projection = vec![
            "c_d_id".to_string(),
            "c_w_id".to_string(),
            "c_last".to_string(),
        ];
        let classification = classify_columns(predicate.as_ref(), &projection);
        let predicate_schema = build_predicate_schema(&classification, &full_schema);

        // The schema the filter evaluates against must be in FILE order.
        let names: Vec<&str> = predicate_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            vec!["c_w_id", "c_last"],
            "predicate schema must follow file order, got {names:?}"
        );

        let remapped =
            remap_predicate_columns(&predicate, &predicate_schema).expect("remap predicate");

        // Phase-1 batch as the parquet reader delivers it: predicate columns
        // in FILE order. Row 0 matches, row 1 fails c_w_id, row 2 fails c_last.
        let batch = RecordBatch::try_new(
            predicate_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1])),
                Arc::new(StringArray::from(vec!["BARBARBAR", "BARBARBAR", "OTHER"])),
            ],
        )
        .expect("build phase-1 batch");

        let value = remapped
            .evaluate(&batch)
            .expect("evaluate remapped predicate");
        let array = value
            .into_array(batch.num_rows())
            .expect("materialize boolean result");
        let bools = array.as_boolean();
        let kept: Vec<bool> = (0..bools.len()).map(|i| bools.value(i)).collect();
        assert_eq!(kept, vec![true, false, false]);
    }
}
