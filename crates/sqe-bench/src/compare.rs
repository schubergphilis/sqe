use arrow_array::{
    cast::AsArray, Array, Float32Array, Float64Array, RecordBatch,
};
use arrow_schema::DataType;

#[derive(Debug)]
pub enum CompareStatus {
    Pass,
    /// Minor mismatch (e.g. decimal precision within epsilon).
    Diff(String),
    /// Wrong results — row count differs or values outside tolerance.
    Fail(String),
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Compare actual Arrow results against expected CSV content.
///
/// Returns `Pass` when every row matches within `epsilon` tolerance for
/// floating-point columns.  If no expected content is supplied (empty string),
/// the function returns `Pass` — callers use `Option<String>` and only call
/// this when content is present.
pub fn compare_results(
    actual: &[RecordBatch],
    expected_csv: &str,
    epsilon: f64,
) -> anyhow::Result<CompareStatus> {
    // 1. Parse expected CSV
    let (headers, expected_rows) = parse_csv(expected_csv)?;

    // 2. Convert actual batches to string rows
    let actual_rows = batches_to_string_rows(actual)?;

    // 3. Check row counts
    if actual_rows.len() != expected_rows.len() {
        return Ok(CompareStatus::Fail(format!(
            "row count mismatch: got {}, expected {}",
            actual_rows.len(),
            expected_rows.len()
        )));
    }

    if actual_rows.is_empty() {
        return Ok(CompareStatus::Pass);
    }

    // 4. Sort both lexicographically so order-independent comparison works
    let mut actual_sorted = actual_rows;
    actual_sorted.sort();
    let mut expected_sorted = expected_rows;
    expected_sorted.sort();

    // Determine which columns are numeric (by looking at the schema of the
    // first non-empty batch, aligned to CSV headers).
    let float_columns = detect_float_columns(actual, &headers);

    // 5. Compare row by row
    let mut any_diff = false;

    for (row_idx, (actual_row, expected_row)) in
        actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
    {
        if actual_row.len() != expected_row.len() {
            return Ok(CompareStatus::Fail(format!(
                "column count mismatch at row {}: got {}, expected {}",
                row_idx + 1,
                actual_row.len(),
                expected_row.len()
            )));
        }

        for (col_idx, (a, e)) in actual_row.iter().zip(expected_row.iter()).enumerate() {
            // Exact match — fast path
            if a == e {
                continue;
            }

            // Numeric tolerance for float columns
            if float_columns.get(col_idx).copied().unwrap_or(false) {
                match (a.parse::<f64>(), e.parse::<f64>()) {
                    (Ok(av), Ok(ev)) => {
                        let diff = (av - ev).abs();
                        let tolerance = epsilon.max(epsilon * ev.abs());
                        if diff <= tolerance {
                            any_diff = true; // within tolerance but not exact
                            continue;
                        }
                        return Ok(CompareStatus::Fail(format!(
                            "value mismatch at row {}, col {}: got '{}', expected '{}' (diff {diff:.6} > tol {tolerance:.6})",
                            row_idx + 1,
                            col_idx + 1,
                            a,
                            e
                        )));
                    }
                    _ => {
                        return Ok(CompareStatus::Fail(format!(
                            "value mismatch at row {}, col {}: got '{}', expected '{}'",
                            row_idx + 1,
                            col_idx + 1,
                            a,
                            e
                        )));
                    }
                }
            } else {
                return Ok(CompareStatus::Fail(format!(
                    "value mismatch at row {}, col {}: got '{}', expected '{}'",
                    row_idx + 1,
                    col_idx + 1,
                    a,
                    e
                )));
            }
        }
    }

    if any_diff {
        Ok(CompareStatus::Diff(
            "numeric values differ within epsilon tolerance".to_string(),
        ))
    } else {
        Ok(CompareStatus::Pass)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse CSV text into (header_names, data_rows).
///
/// Each data row is a `Vec<String>` aligned to the headers.
fn parse_csv(csv: &str) -> anyhow::Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(csv.as_bytes());

    let headers: Vec<String> = reader
        .headers()?
        .iter()
        .map(str::to_string)
        .collect();

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        let row: Vec<String> = record.iter().map(str::to_string).collect();
        rows.push(row);
    }

    Ok((headers, rows))
}

/// Convert Arrow RecordBatches to a flat list of string rows.
fn batches_to_string_rows(batches: &[RecordBatch]) -> anyhow::Result<Vec<Vec<String>>> {
    let mut rows: Vec<Vec<String>> = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);
                row.push(cell_to_string(col, row_idx));
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

/// Format a single Arrow array cell as a string.
fn cell_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Float32 => {
            let v = array.as_any().downcast_ref::<Float32Array>().unwrap().value(row);
            format!("{v}")
        }
        DataType::Float64 => {
            let v = array.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            format!("{v}")
        }
        DataType::Utf8 => array.as_string::<i32>().value(row).to_string(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_string(),
        DataType::Int8 => format!("{}", array.as_primitive::<arrow_array::types::Int8Type>().value(row)),
        DataType::Int16 => format!("{}", array.as_primitive::<arrow_array::types::Int16Type>().value(row)),
        DataType::Int32 => format!("{}", array.as_primitive::<arrow_array::types::Int32Type>().value(row)),
        DataType::Int64 => format!("{}", array.as_primitive::<arrow_array::types::Int64Type>().value(row)),
        DataType::UInt8 => format!("{}", array.as_primitive::<arrow_array::types::UInt8Type>().value(row)),
        DataType::UInt16 => format!("{}", array.as_primitive::<arrow_array::types::UInt16Type>().value(row)),
        DataType::UInt32 => format!("{}", array.as_primitive::<arrow_array::types::UInt32Type>().value(row)),
        DataType::UInt64 => format!("{}", array.as_primitive::<arrow_array::types::UInt64Type>().value(row)),
        DataType::Boolean => format!("{}", array.as_boolean().value(row)),
        DataType::Date32 | DataType::Date64 => {
            arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
        }
        DataType::Decimal128(_, scale) => {
            let raw = array
                .as_primitive::<arrow_array::types::Decimal128Type>()
                .value(row);
            let scale = *scale as u32;
            if scale == 0 {
                format!("{raw}")
            } else {
                let divisor = 10i128.pow(scale);
                let integer = raw / divisor;
                let frac = (raw % divisor).unsigned_abs();
                format!("{integer}.{frac:0>width$}", width = scale as usize)
            }
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<arrow_array::StringViewArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Timestamp(_, _) | DataType::Time32(_) | DataType::Time64(_) => {
            arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
        }
        DataType::Decimal256(_, _) => {
            arrow::util::display::array_value_to_string(array, row).unwrap_or_default()
        }
        other => {
            // Fallback: use Debug representation of the array type
            format!("<{other}>")
        }
    }
}

/// Return a boolean mask of which columns contain float values, aligned to
/// the CSV header order (which we assume matches the schema column order).
fn detect_float_columns(batches: &[RecordBatch], headers: &[String]) -> Vec<bool> {
    if let Some(batch) = batches.first() {
        let schema = batch.schema();
        // Use schema fields; if column count matches headers we map 1:1
        let fields = schema.fields();
        let _header_count = headers.len();
        fields
            .iter()
            .map(|f| {
                matches!(
                    f.data_type(),
                    DataType::Float32
                        | DataType::Float64
                        | DataType::Decimal128(_, _)
                        | DataType::Decimal256(_, _)
                )
            })
            .collect()
    } else {
        vec![false; headers.len()]
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray, *};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn make_batch(
        cols: Vec<(&str, Arc<dyn Array>)>,
    ) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = cols.into_iter().map(|(_, a)| a).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn identical_results_pass() {
        let batch = make_batch(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![1i64, 2, 3])) as Arc<dyn Array>,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"]))
                    as Arc<dyn Array>,
            ),
        ]);
        let csv = "id,name\n1,alpha\n2,beta\n3,gamma\n";
        let status = compare_results(&[batch], csv, 1e-4).unwrap();
        assert!(matches!(status, CompareStatus::Pass), "{status:?}");
    }

    #[test]
    fn row_count_mismatch_fails() {
        let batch = make_batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1i64, 2])) as Arc<dyn Array>,
        )]);
        let csv = "id\n1\n2\n3\n";
        let status = compare_results(&[batch], csv, 1e-4).unwrap();
        assert!(matches!(status, CompareStatus::Fail(_)), "{status:?}");
    }

    #[test]
    fn float_within_epsilon_gives_diff() {
        let batch = make_batch(vec![(
            "val",
            Arc::new(Float64Array::from(vec![1.000_001f64])) as Arc<dyn Array>,
        )]);
        let csv = "val\n1.0\n";
        let status = compare_results(&[batch], csv, 1e-4).unwrap();
        // 0.000001 < 1e-4 tolerance, so this should be a Diff not Fail
        assert!(
            matches!(status, CompareStatus::Pass | CompareStatus::Diff(_)),
            "{status:?}"
        );
    }

    #[test]
    fn float_outside_epsilon_fails() {
        let batch = make_batch(vec![(
            "val",
            Arc::new(Float64Array::from(vec![2.0f64])) as Arc<dyn Array>,
        )]);
        let csv = "val\n1.0\n";
        let status = compare_results(&[batch], csv, 1e-4).unwrap();
        assert!(matches!(status, CompareStatus::Fail(_)), "{status:?}");
    }

    #[test]
    fn string_mismatch_fails() {
        let batch = make_batch(vec![(
            "name",
            Arc::new(StringArray::from(vec!["wrong"])) as Arc<dyn Array>,
        )]);
        let csv = "name\ncorrect\n";
        let status = compare_results(&[batch], csv, 1e-4).unwrap();
        assert!(matches!(status, CompareStatus::Fail(_)), "{status:?}");
    }

    #[test]
    fn order_independent_matching() {
        let batch = make_batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![3i64, 1, 2])) as Arc<dyn Array>,
        )]);
        // CSV in different order — should still pass after sorting
        let csv = "id\n1\n2\n3\n";
        let status = compare_results(&[batch], csv, 1e-4).unwrap();
        assert!(matches!(status, CompareStatus::Pass), "{status:?}");
    }

    #[test]
    fn empty_batches_pass() {
        let batch = make_batch(vec![(
            "id",
            Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn Array>,
        )]);
        let csv = "id\n";
        let status = compare_results(&[batch], csv, 1e-4).unwrap();
        assert!(matches!(status, CompareStatus::Pass), "{status:?}");
    }

    #[test]
    fn test_cell_to_string_utf8view() {
        let arr = StringViewArray::from(vec!["hello"]);
        assert_eq!(cell_to_string(&arr, 0), "hello");
    }

    #[test]
    fn test_cell_to_string_timestamp() {
        let arr = TimestampMicrosecondArray::from(vec![1_710_000_000_000_000i64]);
        let s = cell_to_string(&arr, 0);
        assert!(s.contains("2024"), "timestamp should contain year: {s}");
    }

    #[test]
    fn test_cell_to_string_date32_readable() {
        let arr = Date32Array::from(vec![19800]);
        let s = cell_to_string(&arr, 0);
        assert!(s.contains("2024"), "date should be human-readable: {s}");
    }
}
