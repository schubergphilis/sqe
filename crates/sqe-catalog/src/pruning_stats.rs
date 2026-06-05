//! Iceberg manifest statistics as DataFusion PruningStatistics.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Column, ScalarValue, Statistics};
use iceberg::spec::{DataFile, Datum, PrimitiveLiteral};

pub struct IcebergManifestStatistics {
    data_files: Vec<DataFile>,
    schema: SchemaRef,
    name_to_field_id: Vec<(String, i32)>,
}

impl IcebergManifestStatistics {
    pub fn new(data_files: Vec<DataFile>, schema: SchemaRef, iceberg_schema: &iceberg::spec::Schema) -> Self {
        let name_to_field_id: Vec<(String, i32)> = iceberg_schema.as_struct().fields().iter().map(|f| (f.name.clone(), f.id)).collect();
        Self { data_files, schema, name_to_field_id }
    }

    fn field_id(&self, column_name: &str) -> Option<i32> {
        self.name_to_field_id.iter().find(|(name, _)| name == column_name).map(|(_, id)| *id)
    }

    pub fn count_pruned(results: &[bool]) -> usize {
        results.iter().filter(|&&keep| !keep).count()
    }
}

fn datum_to_scalar(datum: &Datum, data_type: &DataType) -> Option<ScalarValue> {
    match datum.literal() {
        PrimitiveLiteral::Boolean(v) => Some(ScalarValue::Boolean(Some(*v))),
        PrimitiveLiteral::Int(v) => match data_type {
            DataType::Date32 => Some(ScalarValue::Date32(Some(*v))),
            _ => Some(ScalarValue::Int32(Some(*v))),
        },
        PrimitiveLiteral::Long(v) => match data_type {
            DataType::Timestamp(TimeUnit::Microsecond, None) => Some(ScalarValue::TimestampMicrosecond(Some(*v), None)),
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => Some(ScalarValue::TimestampMicrosecond(Some(*v), Some(tz.clone()))),
            _ => Some(ScalarValue::Int64(Some(*v))),
        },
        PrimitiveLiteral::Float(v) => Some(ScalarValue::Float32(Some(v.0))),
        PrimitiveLiteral::Double(v) => Some(ScalarValue::Float64(Some(v.0))),
        PrimitiveLiteral::String(v) => Some(ScalarValue::Utf8(Some(v.clone()))),
        _ => None,
    }
}

fn build_bounds_array(data_files: &[DataFile], field_id: i32, data_type: &DataType, use_upper: bool) -> Option<ArrayRef> {
    let scalars: Vec<Option<ScalarValue>> = data_files.iter().map(|df| {
        let bounds = if use_upper { df.upper_bounds() } else { df.lower_bounds() };
        bounds.get(&field_id).and_then(|datum| datum_to_scalar(datum, data_type))
    }).collect();
    if scalars.iter().all(|s| s.is_none()) { return None; }
    match data_type {
        DataType::Boolean => { let a: BooleanArray = scalars.iter().map(|s| match s { Some(ScalarValue::Boolean(v)) => *v, _ => None }).collect(); Some(Arc::new(a) as ArrayRef) }
        DataType::Int32 => { let a: Int32Array = scalars.iter().map(|s| match s { Some(ScalarValue::Int32(v)) => *v, _ => None }).collect(); Some(Arc::new(a) as ArrayRef) }
        DataType::Int64 => { let a: Int64Array = scalars.iter().map(|s| match s { Some(ScalarValue::Int64(v)) => *v, _ => None }).collect(); Some(Arc::new(a) as ArrayRef) }
        DataType::Float32 => { let a: Float32Array = scalars.iter().map(|s| match s { Some(ScalarValue::Float32(v)) => *v, _ => None }).collect(); Some(Arc::new(a) as ArrayRef) }
        DataType::Float64 => { let a: Float64Array = scalars.iter().map(|s| match s { Some(ScalarValue::Float64(v)) => *v, _ => None }).collect(); Some(Arc::new(a) as ArrayRef) }
        DataType::Utf8 => { let a: StringArray = scalars.iter().map(|s| match s { Some(ScalarValue::Utf8(v)) => v.as_deref(), _ => None }).collect(); Some(Arc::new(a) as ArrayRef) }
        DataType::Date32 => { let a: Date32Array = scalars.iter().map(|s| match s { Some(ScalarValue::Date32(v)) => *v, _ => None }).collect(); Some(Arc::new(a) as ArrayRef) }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let a: TimestampMicrosecondArray = scalars.iter().map(|s| match s { Some(ScalarValue::TimestampMicrosecond(v, _)) => *v, _ => None }).collect();
            let a = if let Some(tz) = tz { a.with_timezone(tz.clone()) } else { a };
            Some(Arc::new(a) as ArrayRef)
        }
        _ => None,
    }
}

/// Aggregate per-column statistics from Iceberg manifest entries into the form
/// DataFusion's cost-based optimizer expects.
///
/// For each field in `arrow_schema` we sum `null_value_counts`, take the min of
/// `lower_bounds` and the max of `upper_bounds` across all `data_files`. The
/// result is one `ColumnStatistics` entry per Arrow field, in the same order.
///
/// Fields where the manifest carries no bounds (or the field doesn't map to an
/// Iceberg field id) yield `Precision::Absent` rather than failing — partial
/// stats are better than no stats for join order selection.
pub fn aggregate_column_statistics(
    data_files: &[DataFile],
    arrow_schema: &Schema,
    iceberg_schema: &iceberg::spec::Schema,
) -> Vec<ColumnStatistics> {
    let id_lookup: Vec<(String, i32)> = iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.name.clone(), f.id))
        .collect();

    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let field_id = id_lookup
                .iter()
                .find(|(name, _)| name == field.name())
                .map(|(_, id)| *id);
            let Some(fid) = field_id else {
                return ColumnStatistics::new_unknown();
            };
            let null_count = aggregate_null_count(data_files, fid);
            let min_value = aggregate_bound(data_files, fid, field.data_type(), false);
            let max_value = aggregate_bound(data_files, fid, field.data_type(), true);
            ColumnStatistics {
                null_count: null_count
                    .map(Precision::Inexact)
                    .unwrap_or(Precision::Absent),
                max_value: max_value
                    .map(Precision::Inexact)
                    .unwrap_or(Precision::Absent),
                min_value: min_value
                    .map(Precision::Inexact)
                    .unwrap_or(Precision::Absent),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }
        })
        .collect()
}

/// Build a `Statistics` for an entire Iceberg snapshot from its data files.
///
/// Combines table-level row count and byte size with per-column min/max/null
/// counts aggregated across all files. The `arrow_schema` should match the
/// projection the scan node will return — typically `IcebergScanExec`'s
/// `projected_schema`.
pub fn aggregate_table_statistics(
    data_files: &[DataFile],
    arrow_schema: &Schema,
    iceberg_schema: &iceberg::spec::Schema,
) -> Statistics {
    let num_rows: usize = data_files
        .iter()
        .map(|df| df.record_count() as usize)
        .sum();
    let total_byte_size: usize = data_files
        .iter()
        .map(|df| df.file_size_in_bytes() as usize)
        .sum();
    let column_statistics = aggregate_column_statistics(data_files, arrow_schema, iceberg_schema);
    Statistics {
        num_rows: Precision::Inexact(num_rows),
        total_byte_size: Precision::Inexact(total_byte_size),
        column_statistics,
    }
}

fn aggregate_null_count(data_files: &[DataFile], field_id: i32) -> Option<usize> {
    let mut total: u64 = 0;
    let mut any = false;
    for df in data_files {
        if let Some(c) = df.null_value_counts().get(&field_id).copied() {
            total = total.saturating_add(c);
            any = true;
        }
    }
    any.then_some(total as usize)
}

fn aggregate_bound(
    data_files: &[DataFile],
    field_id: i32,
    data_type: &DataType,
    use_max: bool,
) -> Option<ScalarValue> {
    let mut acc: Option<ScalarValue> = None;
    for df in data_files {
        let bounds = if use_max {
            df.upper_bounds()
        } else {
            df.lower_bounds()
        };
        let Some(datum) = bounds.get(&field_id) else {
            continue;
        };
        let Some(scalar) = datum_to_scalar(datum, data_type) else {
            continue;
        };
        acc = Some(match acc {
            None => scalar,
            Some(prev) => match prev.partial_cmp(&scalar) {
                // Skip if the two ScalarValues are not comparable. Mixed
                // types here would imply schema corruption; keep what we
                // already accepted rather than silently swapping.
                None => prev,
                Some(ord) => {
                    let take_new = if use_max {
                        ord == std::cmp::Ordering::Less
                    } else {
                        ord == std::cmp::Ordering::Greater
                    };
                    if take_new { scalar } else { prev }
                }
            },
        });
    }
    acc
}

impl PruningStatistics for IcebergManifestStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let fid = self.field_id(&column.name)?;
        let field = self.schema.field_with_name(&column.name).ok()?;
        build_bounds_array(&self.data_files, fid, field.data_type(), false)
    }
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let fid = self.field_id(&column.name)?;
        let field = self.schema.field_with_name(&column.name).ok()?;
        build_bounds_array(&self.data_files, fid, field.data_type(), true)
    }
    fn num_containers(&self) -> usize { self.data_files.len() }
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let fid = self.field_id(&column.name)?;
        let counts: Vec<Option<u64>> = self.data_files.iter().map(|df| df.null_value_counts().get(&fid).copied()).collect();
        if counts.iter().all(|c| c.is_none()) { return None; }
        Some(Arc::new(UInt64Array::from(counts)) as ArrayRef)
    }
    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let counts: Vec<Option<u64>> = self.data_files.iter().map(|df| Some(df.record_count())).collect();
        Some(Arc::new(UInt64Array::from(counts)) as ArrayRef)
    }
    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> { None }
}

// ── Tier-1 clustering gate (issue #132) ──────────────────────────────────────
//
// MR #220's two-tier dynamic-filter pushdown wins big when a fact table's data
// files are clustered on the filter column (tight per-file min/max -> Tier-1
// manifest/row-group bounds pruning skips most files). It is pure overhead when
// the data is uniformly distributed (e.g. SSB `lineorder` by `lo_orderdate`):
// the bounds-only filter prunes nothing, yet iceberg-rust still pays the per-
// file bind + RowFilter eval, which Tier-2 then repeats. This gate inspects the
// already-loaded manifest bounds and lets the scan skip Tier-1 registration
// when every filter column is effectively uniform.

/// Read element `i` of a manifest-bounds array as `f64`, for the numeric and
/// temporal types `build_bounds_array` produces. Non-numeric bounds (bool,
/// string) and nulls return `None` — the column is then undecidable and the
/// gate conservatively keeps Tier-1.
fn numeric_bound_at(arr: &ArrayRef, i: usize) -> Option<f64> {
    if !arr.is_valid(i) {
        return None;
    }
    let any = arr.as_any();
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        return Some(a.value(i) as f64);
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(a.value(i) as f64);
    }
    if let Some(a) = any.downcast_ref::<Float32Array>() {
        return Some(a.value(i) as f64);
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Some(a.value(i));
    }
    if let Some(a) = any.downcast_ref::<Date32Array>() {
        return Some(a.value(i) as f64);
    }
    if let Some(a) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return Some(a.value(i) as f64);
    }
    None
}

/// Median per-file spread relative to the column's overall range. `per_file` is
/// each file's `(lower, upper)`. A spread near 1.0 means files each span most of
/// the range (uniform); near 0.0 means tight, well-separated files (clustered).
/// Returns `None` when undecidable: no files, or a degenerate (zero / non-finite)
/// overall range.
fn median_relative_spread(per_file: &[(f64, f64)]) -> Option<f64> {
    if per_file.is_empty() {
        return None;
    }
    let smin = per_file.iter().map(|(lo, _)| *lo).fold(f64::INFINITY, f64::min);
    let smax = per_file.iter().map(|(_, hi)| *hi).fold(f64::NEG_INFINITY, f64::max);
    let range = smax - smin;
    if !range.is_finite() || range <= 0.0 {
        return None;
    }
    let mut spreads: Vec<f64> = per_file
        .iter()
        .map(|(lo, hi)| ((hi - lo) / range).clamp(0.0, 1.0))
        .collect();
    spreads.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = spreads.len() / 2;
    Some(if spreads.len() % 2 == 0 {
        (spreads[mid - 1] + spreads[mid]) / 2.0
    } else {
        spreads[mid]
    })
}

impl IcebergManifestStatistics {
    /// Median per-file bounds spread for one column, or `None` if the column is
    /// undecidable (unknown field, non-numeric bounds, no bounds, degenerate
    /// range).
    fn column_median_spread(&self, column: &str) -> Option<f64> {
        let field_id = self.field_id(column)?;
        let data_type = self.schema.field_with_name(column).ok()?.data_type();
        let lo = build_bounds_array(&self.data_files, field_id, data_type, false)?;
        let hi = build_bounds_array(&self.data_files, field_id, data_type, true)?;
        let n = lo.len().min(hi.len());
        let mut per_file = Vec::with_capacity(n);
        for i in 0..n {
            if let (Some(l), Some(h)) = (numeric_bound_at(&lo, i), numeric_bound_at(&hi, i)) {
                per_file.push((l, h));
            }
        }
        median_relative_spread(&per_file)
    }

    /// Tier-1 clustering gate (issue #132). Returns `true` when Tier-1 dynamic-
    /// predicate registration should proceed — i.e. the planned files are
    /// clustered tightly enough on at least one filter column that bounds
    /// pruning is likely to help.
    ///
    /// Returns `false` (skip Tier-1; Tier-2 still applies) only when at least
    /// one filter column is decidable **and every** decidable filter column is
    /// effectively uniform (median spread >= `uniform_threshold`). Undecidable
    /// cases (too few files, unknown/non-numeric columns) conservatively keep
    /// Tier-1 so the gate can never regress a workload it cannot reason about.
    pub fn clustered_on_filters(&self, filter_columns: &[String], uniform_threshold: f64) -> bool {
        // With 0 or 1 file there is nothing to prune across; keep current behavior.
        if self.data_files.len() < 2 {
            return true;
        }
        let mut any_decidable = false;
        for col in filter_columns {
            if let Some(median) = self.column_median_spread(col) {
                any_decidable = true;
                if median < uniform_threshold {
                    return true; // clustered on this column -> Tier-1 helps
                }
            }
        }
        // Decidable columns existed but all were uniform -> skip Tier-1.
        // Nothing decidable -> keep Tier-1 (conservative).
        !any_decidable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};

    // ── Tier-1 clustering gate (issue #132) ─────────────────────────────
    #[test]
    fn spread_none_when_empty() {
        assert_eq!(median_relative_spread(&[]), None);
    }

    #[test]
    fn spread_none_when_range_degenerate() {
        // Every file has the same single value -> zero overall range -> undecidable.
        assert_eq!(median_relative_spread(&[(5.0, 5.0), (5.0, 5.0)]), None);
    }

    #[test]
    fn spread_high_when_uniform() {
        // SSB lineorder shape: every file spans (almost) the whole range.
        let per_file = [(0.0, 100.0), (0.0, 100.0), (1.0, 99.0)];
        let median = median_relative_spread(&per_file).unwrap();
        assert!(median >= 0.8, "uniform data should report a high spread, got {median}");
    }

    #[test]
    fn spread_low_when_clustered() {
        // TPC-DS date-sorted shape: each file covers a narrow, disjoint slice.
        let per_file = [(0.0, 10.0), (20.0, 30.0), (45.0, 55.0), (90.0, 100.0)];
        let median = median_relative_spread(&per_file).unwrap();
        assert!(median < 0.5, "clustered data should report a low spread, got {median}");
    }

    #[test]
    fn spread_median_is_robust_to_one_wide_file() {
        // Three tight files + one file spanning the range. Median stays low, so
        // a single straggler file does not flip a clustered table to "uniform".
        let per_file = [(0.0, 5.0), (10.0, 15.0), (20.0, 25.0), (0.0, 100.0)];
        let median = median_relative_spread(&per_file).unwrap();
        assert!(median < 0.5, "median should resist one wide file, got {median}");
    }

    #[test]
    fn numeric_bound_reads_int_and_date_and_null() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
        assert_eq!(numeric_bound_at(&ints, 0), Some(7.0));
        assert_eq!(numeric_bound_at(&ints, 1), None);
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![Some(19_000)]));
        assert_eq!(numeric_bound_at(&dates, 0), Some(19_000.0));
        // Non-numeric bounds (strings) are undecidable.
        let strs: ArrayRef = Arc::new(StringArray::from(vec![Some("x")]));
        assert_eq!(numeric_bound_at(&strs, 0), None);
    }

    /// Test helper: datum_to_scalar conversion for various types.
    #[test]
    fn test_datum_to_scalar_int() {
        let datum = Datum::int(42);
        let sv = datum_to_scalar(&datum, &DataType::Int32).unwrap();
        assert_eq!(sv, ScalarValue::Int32(Some(42)));
    }

    #[test]
    fn test_datum_to_scalar_long() {
        let datum = Datum::long(1_000_000i64);
        let sv = datum_to_scalar(&datum, &DataType::Int64).unwrap();
        assert_eq!(sv, ScalarValue::Int64(Some(1_000_000)));
    }

    #[test]
    fn test_datum_to_scalar_string() {
        let datum = Datum::string("hello");
        let sv = datum_to_scalar(&datum, &DataType::Utf8).unwrap();
        assert_eq!(sv, ScalarValue::Utf8(Some("hello".to_string())));
    }

    #[test]
    fn test_datum_to_scalar_float() {
        let datum = Datum::float(1.5f32);
        let sv = datum_to_scalar(&datum, &DataType::Float32).unwrap();
        assert_eq!(sv, ScalarValue::Float32(Some(1.5)));
    }

    #[test]
    fn test_datum_to_scalar_date() {
        let datum = Datum::int(19000); // days since epoch
        let sv = datum_to_scalar(&datum, &DataType::Date32).unwrap();
        assert_eq!(sv, ScalarValue::Date32(Some(19000)));
    }

    #[test]
    fn test_datum_to_scalar_timestamp_micros() {
        let datum = Datum::long(1_700_000_000_000_000i64);
        let sv = datum_to_scalar(&datum, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        assert_eq!(sv, ScalarValue::TimestampMicrosecond(Some(1_700_000_000_000_000), None));
    }

    #[test]
    fn test_count_pruned() {
        assert_eq!(IcebergManifestStatistics::count_pruned(&[true, false, true, false]), 2);
        assert_eq!(IcebergManifestStatistics::count_pruned(&[true, true, true]), 0);
        assert_eq!(IcebergManifestStatistics::count_pruned(&[false, false]), 2);
        assert_eq!(IcebergManifestStatistics::count_pruned(&[]), 0);
    }

    /// Test that field_id lookup works correctly.
    #[test]
    fn test_field_id_lookup() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int))),
                Arc::new(NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))),
            ])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let stats = IcebergManifestStatistics::new(vec![], arrow_schema, &iceberg_schema);
        assert_eq!(stats.field_id("id"), Some(1));
        assert_eq!(stats.field_id("name"), Some(2));
        assert_eq!(stats.field_id("nonexistent"), None);
    }

    /// Empty containers should return 0.
    #[test]
    fn test_empty_containers() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)))])
            .build()
            .unwrap();
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stats = IcebergManifestStatistics::new(vec![], arrow_schema, &iceberg_schema);
        assert_eq!(stats.num_containers(), 0);
        assert!(stats.min_values(&Column::from_name("id")).is_none());
        assert!(stats.max_values(&Column::from_name("id")).is_none());
    }

    /// Two-file aggregation: min/max should span both files; null counts sum.
    #[test]
    fn test_aggregate_column_statistics_two_files() {
        use iceberg::spec::{
            DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Struct,
            Type,
        };
        use std::collections::HashMap;

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let file_a = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path("a.parquet".into())
            .file_size_in_bytes(1024)
            .record_count(100)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .lower_bounds(HashMap::from([
                (1, Datum::int(10)),
                (2, Datum::string("alpha")),
            ]))
            .upper_bounds(HashMap::from([
                (1, Datum::int(50)),
                (2, Datum::string("middle")),
            ]))
            .null_value_counts(HashMap::from([(1, 0u64), (2, 3u64)]))
            .build()
            .unwrap();
        let file_b = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path("b.parquet".into())
            .file_size_in_bytes(2048)
            .record_count(200)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .lower_bounds(HashMap::from([
                (1, Datum::int(40)),
                (2, Datum::string("middle")),
            ]))
            .upper_bounds(HashMap::from([
                (1, Datum::int(99)),
                (2, Datum::string("zulu")),
            ]))
            .null_value_counts(HashMap::from([(1, 1u64), (2, 7u64)]))
            .build()
            .unwrap();

        let stats =
            aggregate_column_statistics(&[file_a, file_b], &arrow_schema, &iceberg_schema);
        assert_eq!(stats.len(), 2);

        // id: min=10, max=99, nulls=0+1=1
        assert_eq!(stats[0].min_value, Precision::Inexact(ScalarValue::Int32(Some(10))));
        assert_eq!(stats[0].max_value, Precision::Inexact(ScalarValue::Int32(Some(99))));
        assert_eq!(stats[0].null_count, Precision::Inexact(1));
        assert_eq!(stats[0].distinct_count, Precision::Absent);

        // name: min="alpha", max="zulu", nulls=10
        assert_eq!(
            stats[1].min_value,
            Precision::Inexact(ScalarValue::Utf8(Some("alpha".into())))
        );
        assert_eq!(
            stats[1].max_value,
            Precision::Inexact(ScalarValue::Utf8(Some("zulu".into())))
        );
        assert_eq!(stats[1].null_count, Precision::Inexact(10));
    }

    /// Field present in arrow_schema but not in iceberg_schema should yield
    /// all-Absent statistics rather than panicking.
    #[test]
    fn test_aggregate_column_statistics_unknown_field() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("not_in_iceberg", DataType::Utf8, true),
        ]);
        let stats = aggregate_column_statistics(&[], &arrow_schema, &iceberg_schema);
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[1].null_count, Precision::Absent);
        assert_eq!(stats[1].min_value, Precision::Absent);
        assert_eq!(stats[1].max_value, Precision::Absent);
    }

    /// Table-level aggregation should sum row counts and bytes across files.
    #[test]
    fn test_aggregate_table_statistics_totals() {
        use iceberg::spec::{
            DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Struct,
            Type,
        };

        let iceberg_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let arrow_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let make = |size: u64, rows: u64| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_format(DataFileFormat::Parquet)
                .file_path(format!("f-{rows}.parquet"))
                .file_size_in_bytes(size)
                .record_count(rows)
                .partition_spec_id(0)
                .partition(Struct::empty())
                .build()
                .unwrap()
        };
        let stats = aggregate_table_statistics(
            &[make(1_000, 50), make(2_000, 75), make(500, 25)],
            &arrow_schema,
            &iceberg_schema,
        );
        assert_eq!(stats.num_rows, Precision::Inexact(150));
        assert_eq!(stats.total_byte_size, Precision::Inexact(3_500));
    }
}
