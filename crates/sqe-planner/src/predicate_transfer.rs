//! Predicate transfer for join optimization.
//!
//! After scanning the build side of a join, extract the set of distinct
//! join-key values and push them as an IN-list predicate to the probe side's
//! Iceberg scan. This enables:
//!
//! - **File-level pruning**: Iceberg manifest min/max statistics can skip
//!   entire data files whose key ranges don't overlap with the IN-list values.
//!
//! - **Bloom filter pruning**: If the probe-side Parquet files have bloom
//!   filters on the join key column, the IN-list values can be checked against
//!   them to skip row groups.
//!
//! - **Row-group pruning**: Parquet row-group level min/max statistics can
//!   further prune within files.
//!
//! Predicate transfer is only applied when the distinct key set is small
//! enough (< [`MAX_PREDICATE_TRANSFER_VALUES`]). Large sets would bloat the
//! IN-list predicate and slow evaluation at the probe side.

use std::collections::HashSet;

use arrow_array::{Array, RecordBatch};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{col, lit, Expr};
use tracing::debug;

// ─────────────────────────── Constants ──────────────────────────────────────

/// Maximum number of distinct join-key values for predicate transfer.
///
/// When the build side produces more than this many distinct values,
/// predicate transfer is skipped because the IN-list predicate would
/// be too large to evaluate efficiently at the probe side.
pub const MAX_PREDICATE_TRANSFER_VALUES: usize = 10_000;

// ─────────────────────────── PredicateTransfer ──────────────────────────────

/// Holds distinct join-key values extracted from the build side of a join,
/// ready to be pushed as an IN-list predicate to the probe side.
///
/// # Example
///
/// ```ignore
/// // After scanning the build side, extract distinct "customer_id" values
/// let transfer = PredicateTransfer::new(
///     key_values,          // HashSet<ScalarValue> from build side scan
///     "customer_id",       // probe-side column name
/// );
///
/// // Convert to a DataFusion predicate for the probe side
/// if let Some(predicate) = transfer.to_predicate() {
///     // Push predicate to probe-side IcebergScanExec for file pruning
/// }
/// ```
#[derive(Debug, Clone)]
pub struct PredicateTransfer {
    /// Distinct join-key values extracted from the build side.
    key_values: HashSet<ScalarValue>,
    /// Column name on the probe side to match against.
    probe_column: String,
}

impl PredicateTransfer {
    /// Create a new `PredicateTransfer` with the given key values and probe column.
    ///
    /// # Arguments
    /// - `key_values`: Distinct values from the build side's join key column.
    /// - `probe_column`: The column name on the probe side to filter.
    pub fn new(key_values: HashSet<ScalarValue>, probe_column: impl Into<String>) -> Self {
        Self {
            key_values,
            probe_column: probe_column.into(),
        }
    }

    /// Returns the number of distinct key values.
    pub fn num_values(&self) -> usize {
        self.key_values.len()
    }

    /// Returns the probe column name.
    pub fn probe_column(&self) -> &str {
        &self.probe_column
    }

    /// Returns a reference to the key values set.
    pub fn key_values(&self) -> &HashSet<ScalarValue> {
        &self.key_values
    }

    /// Returns `true` if predicate transfer should be applied.
    ///
    /// Transfer is skipped when:
    /// - The key set is empty (no build-side rows matched).
    /// - The key set exceeds [`MAX_PREDICATE_TRANSFER_VALUES`] (too large).
    pub fn should_apply(&self) -> bool {
        !self.key_values.is_empty() && self.key_values.len() <= MAX_PREDICATE_TRANSFER_VALUES
    }

    /// Convert to a DataFusion `Expr::InList` predicate for the probe side.
    ///
    /// Returns `None` if the key set is empty or exceeds the size limit.
    ///
    /// The returned expression is:
    /// ```sql
    /// probe_column IN (value1, value2, ..., valueN)
    /// ```
    ///
    /// This predicate can be pushed down to the probe side's Iceberg scan
    /// for file-level pruning via min/max statistics and bloom filters.
    pub fn to_predicate(&self) -> Option<Expr> {
        if !self.should_apply() {
            debug!(
                num_values = self.key_values.len(),
                max_values = MAX_PREDICATE_TRANSFER_VALUES,
                probe_column = %self.probe_column,
                "Predicate transfer skipped: {}",
                if self.key_values.is_empty() {
                    "empty key set"
                } else {
                    "key set too large"
                }
            );
            return None;
        }

        let list: Vec<Expr> = self
            .key_values
            .iter()
            .cloned()
            .map(lit)
            .collect();

        debug!(
            num_values = list.len(),
            probe_column = %self.probe_column,
            "Generated predicate transfer IN-list"
        );

        Some(col(&self.probe_column).in_list(list, false))
    }
}

// ─────────────────────────── Extraction helpers ─────────────────────────────

/// Extract distinct values from a single column of a `RecordBatch`.
///
/// Converts each non-null value to a `ScalarValue` and inserts it into
/// the result set. Null values are skipped (they won't match in an IN-list).
///
/// Returns `None` if the column is not found in the batch.
pub fn extract_distinct_values(
    batch: &RecordBatch,
    column_name: &str,
) -> Option<HashSet<ScalarValue>> {
    let col_idx = batch.schema().index_of(column_name).ok()?;
    let array = batch.column(col_idx);
    let mut values = HashSet::new();

    for row_idx in 0..array.len() {
        if array.is_null(row_idx) {
            continue;
        }
        if let Ok(scalar) = ScalarValue::try_from_array(array, row_idx) {
            values.insert(scalar);
        }
    }

    Some(values)
}

/// Extract distinct values from multiple `RecordBatch`es.
///
/// Accumulates distinct values across all batches for the specified column.
/// Stops early if the accumulated set exceeds `max_values` (returns `None`
/// to signal that predicate transfer should not be applied).
pub fn extract_distinct_from_batches(
    batches: &[RecordBatch],
    column_name: &str,
    max_values: usize,
) -> Option<HashSet<ScalarValue>> {
    let mut values = HashSet::new();

    for batch in batches {
        if let Some(batch_values) = extract_distinct_values(batch, column_name) {
            values.extend(batch_values);
            if values.len() > max_values {
                debug!(
                    accumulated = values.len(),
                    max = max_values,
                    column = column_name,
                    "Predicate transfer: distinct values exceeded limit, aborting"
                );
                return None;
            }
        }
    }

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

/// Build a `PredicateTransfer` from collected build-side batches.
///
/// This is the main entry point for predicate transfer during join
/// execution. Call after collecting build-side batches:
///
/// 1. Extract distinct values from the build-side join key column.
/// 2. If within limits, create a `PredicateTransfer`.
/// 3. Convert to an `Expr::InList` and push to probe-side scan.
///
/// Returns `None` if the distinct value count exceeds the limit or
/// the column is not found.
pub fn build_predicate_transfer(
    batches: &[RecordBatch],
    build_column: &str,
    probe_column: &str,
) -> Option<PredicateTransfer> {
    let values = extract_distinct_from_batches(
        batches,
        build_column,
        MAX_PREDICATE_TRANSFER_VALUES,
    )?;

    let transfer = PredicateTransfer::new(values, probe_column);

    if transfer.should_apply() {
        debug!(
            num_values = transfer.num_values(),
            build_column = build_column,
            probe_column = probe_column,
            "Built predicate transfer from build-side batches"
        );
        Some(transfer)
    } else {
        None
    }
}

// ─────────────────────────────── Tests ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn make_int_batch(values: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_array = Int64Array::from(values.clone());
        let name_array = StringArray::from(
            values
                .iter()
                .map(|v| v.map(|n| format!("name_{n}")))
                .collect::<Vec<_>>(),
        );
        RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    // ─── PredicateTransfer tests ───

    #[test]
    fn test_predicate_transfer_new() {
        let values: HashSet<ScalarValue> = vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
        ]
        .into_iter()
        .collect();

        let transfer = PredicateTransfer::new(values.clone(), "customer_id");
        assert_eq!(transfer.num_values(), 3);
        assert_eq!(transfer.probe_column(), "customer_id");
        assert_eq!(transfer.key_values(), &values);
    }

    #[test]
    fn test_predicate_transfer_should_apply_normal() {
        let values: HashSet<ScalarValue> = (1..=100)
            .map(|i| ScalarValue::Int64(Some(i)))
            .collect();

        let transfer = PredicateTransfer::new(values, "id");
        assert!(transfer.should_apply());
    }

    #[test]
    fn test_predicate_transfer_should_not_apply_empty() {
        let transfer = PredicateTransfer::new(HashSet::new(), "id");
        assert!(!transfer.should_apply());
    }

    #[test]
    fn test_predicate_transfer_should_not_apply_too_large() {
        let values: HashSet<ScalarValue> = (1..=(MAX_PREDICATE_TRANSFER_VALUES + 1) as i64)
            .map(|i| ScalarValue::Int64(Some(i)))
            .collect();

        let transfer = PredicateTransfer::new(values, "id");
        assert!(!transfer.should_apply());
    }

    #[test]
    fn test_to_predicate_returns_some() {
        let values: HashSet<ScalarValue> = vec![
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(20)),
            ScalarValue::Int64(Some(30)),
        ]
        .into_iter()
        .collect();

        let transfer = PredicateTransfer::new(values, "order_id");
        let predicate = transfer.to_predicate();
        assert!(predicate.is_some());

        let expr = predicate.unwrap();
        let display = format!("{expr}");
        assert!(
            display.contains("order_id"),
            "Predicate should reference probe column, got: {display}"
        );
        assert!(
            display.contains("IN"),
            "Predicate should be an IN-list, got: {display}"
        );
    }

    #[test]
    fn test_to_predicate_returns_none_when_empty() {
        let transfer = PredicateTransfer::new(HashSet::new(), "id");
        assert!(transfer.to_predicate().is_none());
    }

    #[test]
    fn test_to_predicate_returns_none_when_too_large() {
        let values: HashSet<ScalarValue> = (1..=(MAX_PREDICATE_TRANSFER_VALUES + 1) as i64)
            .map(|i| ScalarValue::Int64(Some(i)))
            .collect();

        let transfer = PredicateTransfer::new(values, "id");
        assert!(transfer.to_predicate().is_none());
    }

    #[test]
    fn test_predicate_transfer_with_string_values() {
        let values: HashSet<ScalarValue> = vec![
            ScalarValue::Utf8(Some("us-east".to_string())),
            ScalarValue::Utf8(Some("eu-west".to_string())),
        ]
        .into_iter()
        .collect();

        let transfer = PredicateTransfer::new(values, "region");
        assert!(transfer.should_apply());

        let predicate = transfer.to_predicate().unwrap();
        let display = format!("{predicate}");
        assert!(display.contains("region"));
        assert!(display.contains("IN"));
    }

    #[test]
    fn test_predicate_transfer_at_exact_limit() {
        let values: HashSet<ScalarValue> = (1..=MAX_PREDICATE_TRANSFER_VALUES as i64)
            .map(|i| ScalarValue::Int64(Some(i)))
            .collect();

        let transfer = PredicateTransfer::new(values, "id");
        assert!(transfer.should_apply(), "Exact limit should still apply");
        assert!(transfer.to_predicate().is_some());
    }

    // ─── extract_distinct_values tests ───

    #[test]
    fn test_extract_distinct_values() {
        let batch = make_int_batch(vec![Some(1), Some(2), Some(3), Some(2), Some(1)]);
        let values = extract_distinct_values(&batch, "id").unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.contains(&ScalarValue::Int64(Some(1))));
        assert!(values.contains(&ScalarValue::Int64(Some(2))));
        assert!(values.contains(&ScalarValue::Int64(Some(3))));
    }

    #[test]
    fn test_extract_distinct_values_skips_nulls() {
        let batch = make_int_batch(vec![Some(1), None, Some(3), None, Some(1)]);
        let values = extract_distinct_values(&batch, "id").unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&ScalarValue::Int64(Some(1))));
        assert!(values.contains(&ScalarValue::Int64(Some(3))));
    }

    #[test]
    fn test_extract_distinct_values_all_nulls() {
        let batch = make_int_batch(vec![None, None, None]);
        let values = extract_distinct_values(&batch, "id").unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn test_extract_distinct_values_missing_column() {
        let batch = make_int_batch(vec![Some(1)]);
        let values = extract_distinct_values(&batch, "nonexistent");
        assert!(values.is_none());
    }

    #[test]
    fn test_extract_distinct_from_batches_single() {
        let batches = vec![make_int_batch(vec![Some(1), Some(2), Some(3)])];
        let values =
            extract_distinct_from_batches(&batches, "id", MAX_PREDICATE_TRANSFER_VALUES).unwrap();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_extract_distinct_from_batches_multiple() {
        let batches = vec![
            make_int_batch(vec![Some(1), Some(2)]),
            make_int_batch(vec![Some(2), Some(3)]),
            make_int_batch(vec![Some(3), Some(4)]),
        ];
        let values =
            extract_distinct_from_batches(&batches, "id", MAX_PREDICATE_TRANSFER_VALUES).unwrap();
        assert_eq!(values.len(), 4);
    }

    #[test]
    fn test_extract_distinct_from_batches_exceeds_limit() {
        let batch = make_int_batch((1..=20).map(Some).collect());
        let values = extract_distinct_from_batches(&[batch], "id", 5);
        assert!(
            values.is_none(),
            "Should return None when distinct count exceeds limit"
        );
    }

    #[test]
    fn test_extract_distinct_from_batches_empty() {
        let values =
            extract_distinct_from_batches(&[], "id", MAX_PREDICATE_TRANSFER_VALUES);
        assert!(values.is_none());
    }

    // ─── build_predicate_transfer tests ───

    #[test]
    fn test_build_predicate_transfer() {
        let batches = vec![
            make_int_batch(vec![Some(10), Some(20), Some(30)]),
            make_int_batch(vec![Some(20), Some(40)]),
        ];

        let transfer = build_predicate_transfer(&batches, "id", "order_id");
        assert!(transfer.is_some());

        let transfer = transfer.unwrap();
        assert_eq!(transfer.num_values(), 4); // 10, 20, 30, 40
        assert_eq!(transfer.probe_column(), "order_id");
    }

    #[test]
    fn test_build_predicate_transfer_empty_batches() {
        let transfer = build_predicate_transfer(&[], "id", "order_id");
        assert!(transfer.is_none());
    }

    #[test]
    fn test_build_predicate_transfer_missing_column() {
        let batches = vec![make_int_batch(vec![Some(1)])];
        let transfer = build_predicate_transfer(&batches, "nonexistent", "order_id");
        assert!(transfer.is_none());
    }

    #[test]
    fn test_build_predicate_transfer_all_nulls() {
        let batches = vec![make_int_batch(vec![None, None])];
        let transfer = build_predicate_transfer(&batches, "id", "order_id");
        assert!(transfer.is_none());
    }

    #[test]
    fn test_predicate_transfer_clone() {
        let values: HashSet<ScalarValue> = vec![ScalarValue::Int64(Some(1))]
            .into_iter()
            .collect();
        let transfer = PredicateTransfer::new(values, "id");
        let cloned = transfer.clone();
        assert_eq!(cloned.num_values(), transfer.num_values());
        assert_eq!(cloned.probe_column(), transfer.probe_column());
    }

    #[test]
    fn test_predicate_transfer_debug() {
        let values: HashSet<ScalarValue> = vec![ScalarValue::Int64(Some(42))]
            .into_iter()
            .collect();
        let transfer = PredicateTransfer::new(values, "id");
        let debug_str = format!("{transfer:?}");
        assert!(debug_str.contains("PredicateTransfer"));
        assert!(debug_str.contains("id"));
    }

    // ─── Integration-style test: full pipeline ───

    #[test]
    fn test_full_predicate_transfer_pipeline() {
        // Simulate a join scenario:
        // Build side: small dimension table with customer IDs [100, 200, 300]
        // Probe side: large fact table with customer_id column
        // Predicate transfer pushes: WHERE customer_id IN (100, 200, 300)

        let build_batches = vec![
            make_int_batch(vec![Some(100), Some(200), Some(300)]),
        ];

        // Step 1: Build the transfer from build-side batches
        let transfer = build_predicate_transfer(
            &build_batches,
            "id",
            "customer_id",
        )
        .expect("Should create predicate transfer");

        assert_eq!(transfer.num_values(), 3);
        assert_eq!(transfer.probe_column(), "customer_id");

        // Step 2: Convert to a predicate
        let predicate = transfer.to_predicate().expect("Should create predicate");

        // Step 3: Verify the predicate structure
        let display = format!("{predicate}");
        assert!(display.contains("customer_id"));
        assert!(display.contains("IN"));

        // The predicate can now be pushed to the probe-side IcebergScanExec
        // for file-level min/max pruning and bloom filter pruning.
    }
}
