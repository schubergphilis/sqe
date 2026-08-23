//! Size-only statistics for a Hive external table.
//!
//! `serde_map::listing_options` sets `with_collect_stat(false)`: reading a
//! real per-column statistic (a Parquet footer, say) at plan time is the
//! exact per-query latency this design avoids, and CSV/JSON have no footer
//! to read regardless. Without any statistics `ListingTable::statistics()`
//! returns `None`, which is `Statistics::new_unknown` at the optimizer.
//! `JoinStrategyRule` and `star_schema_reorder` both plan from statistics,
//! and Unknown row counts are what caused the five-query SMJ regression in
//! MR !724 (`docs/internal/...`, see that MR's postmortem): a join reorder
//! or strategy choice made blind is not merely imprecise, it can be actively
//! wrong. A byte count from a single object-store LIST call is nearly free
//! next to that, so this always derives at least a row estimate from it
//! rather than leaving the table Unknown.
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use object_store::ObjectMeta;

/// Used for a variable-width column (string/binary) when estimating the
/// average row width: there is no fixed size to sum, and this is a plainer
/// guess than reading any actual data to measure one.
const VARIABLE_WIDTH_HINT_BYTES: usize = 16;

/// A byte-width estimate for one row of `schema`, summing each field's fixed
/// Arrow width or [`VARIABLE_WIDTH_HINT_BYTES`] for a variable-width one.
///
/// This is intentionally crude: it exists only to turn a total byte count
/// into a row-count *estimate* (`Precision::Inexact`), not to size memory or
/// pick a physical encoding.
pub fn row_width_hint(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|f| fixed_width(f.data_type()).unwrap_or(VARIABLE_WIDTH_HINT_BYTES))
        .sum::<usize>()
        .max(1)
}

/// The fixed on-wire width of an Arrow primitive type, or `None` for a type
/// whose length varies per value (strings, binary, lists, structs, ...).
fn fixed_width(dt: &DataType) -> Option<usize> {
    use DataType::*;
    match dt {
        Boolean => Some(1),
        Int8 | UInt8 => Some(1),
        Int16 | UInt16 => Some(2),
        Int32 | UInt32 | Float32 | Date32 => Some(4),
        Int64 | UInt64 | Float64 | Date64 | Timestamp(_, _) => Some(8),
        Decimal128(_, _) => Some(16),
        Decimal256(_, _) => Some(32),
        _ => None,
    }
}

/// Size-derived statistics for a table backed by `files`: `total_byte_size`
/// is the sum of each object's size (`Exact`, since it comes straight from
/// the object store's own metadata), and `num_rows` divides that by
/// `row_width_hint` (`Inexact`, since it is only ever an estimate). Column
/// statistics are left at their unknown default: nothing here reads a byte
/// of the files themselves.
/// Size-derived statistics from index metadata (file counts/sizes captured
/// at ADD/REPAIR time) so planning does not LIST the table prefix.
pub fn size_only_from_index(
    total_bytes: u64,
    row_width_hint: usize,
    schema: &Schema,
) -> Statistics {
    let total_bytes = total_bytes as usize;
    let row_width_hint = row_width_hint.max(1);
    Statistics {
        num_rows: Precision::Inexact(total_bytes / row_width_hint),
        total_byte_size: Precision::Exact(total_bytes),
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    }
}

/// Planning statistics from a prior `ANALYZE TABLE`: exact row count, size
/// still from the index byte total when known.
pub fn analyzed_statistics(
    row_count: u64,
    total_bytes: Option<u64>,
    schema: &Schema,
) -> Statistics {
    Statistics {
        num_rows: Precision::Exact(row_count as usize),
        total_byte_size: match total_bytes {
            Some(b) => Precision::Exact(b as usize),
            None => Precision::Absent,
        },
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    }
}

pub fn size_only_statistics(
    files: &[ObjectMeta],
    row_width_hint: usize,
    schema: &Schema,
) -> Statistics {
    let total_bytes: u64 = files.iter().map(|f| f.size).sum();
    let total_bytes = total_bytes as usize;
    let row_width_hint = row_width_hint.max(1);

    Statistics {
        num_rows: Precision::Inexact(total_bytes / row_width_hint),
        total_byte_size: Precision::Exact(total_bytes),
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use datafusion::arrow::datatypes::Field;
    use object_store::path::Path;

    fn meta(size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from("f"),
            last_modified: Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn row_width_sums_fixed_columns_and_uses_the_hint_for_variable_width() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        // 8 (Int64) + 16 (variable-width hint for Utf8)
        assert_eq!(row_width_hint(&schema), 24);
    }

    #[test]
    fn total_bytes_is_exact_and_row_count_divides_by_the_width_hint() {
        let files = vec![meta(1000), meta(1000)];
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let stats = size_only_statistics(&files, row_width_hint(&schema), &schema);
        assert_eq!(stats.total_byte_size, Precision::Exact(2000));
        assert_eq!(stats.num_rows, Precision::Inexact(2000 / 8));
    }

    #[test]
    fn zero_width_hint_is_clamped_so_division_never_panics() {
        let files = vec![meta(100)];
        let schema = Schema::empty();
        let stats = size_only_statistics(&files, 0, &schema);
        assert_eq!(stats.num_rows, Precision::Inexact(100));
    }

    #[test]
    fn no_files_is_zero_bytes_and_zero_rows_not_absent() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let stats = size_only_statistics(&[], row_width_hint(&schema), &schema);
        assert_eq!(stats.total_byte_size, Precision::Exact(0));
        assert_eq!(stats.num_rows, Precision::Inexact(0));
    }
}
