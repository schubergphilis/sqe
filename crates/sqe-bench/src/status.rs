//! Expected-vs-actual comparison and the per-query outcome taxonomy.
//!
//! This module owns two related things:
//!   1. The SQE-vs-expected-CSV comparator (`CompareStatus`, `compare_results`,
//!      `load_expected`) -- moved here verbatim from the crate's former
//!      `compare.rs`/`test.rs` split.
//!   2. The richer `QueryOutcome` taxonomy layered on top of it, plus the
//!      `legacy_bucket`/`is_real_failure` mappings that keep the stable
//!      `BENCH_SUMMARY` line and JSON schema byte-compatible while letting
//!      the run loop (`test.rs`) and exit code (`run.rs`) treat
//!      timeouts/vacuous results distinctly.

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

            // Normalize: trim trailing zeros from decimal-like strings
            // "123.4500" == "123.45", "100.00" == "100"
            let a_norm = a.trim_end_matches('0').trim_end_matches('.');
            let e_norm = e.trim_end_matches('0').trim_end_matches('.');
            if a_norm == e_norm {
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

/// Try to load the expected results CSV for a query.
///
/// Returns `Ok(None)` when the file does not exist (query runs without
/// validation), `Ok(Some(content))` when found, and `Err` for I/O errors.
pub fn load_expected(benchmark: &str, scale: f64, query_id: &str) -> anyhow::Result<Option<String>> {
    let path = format!("benchmarks/expected/{benchmark}/sf{scale}/{query_id}.csv");
    match std::fs::read_to_string(&path) {
        Ok(content) => Ok(Some(content)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

// ---------------------------------------------------------------------------
// QueryOutcome taxonomy
// ---------------------------------------------------------------------------

/// Per-query outcome, richer than the legacy 5 report buckets so the human
/// summary and the run's exit code can treat timeouts and vacuous results
/// distinctly. Mapped back to the legacy buckets by `legacy_bucket` for the
/// stable `BENCH_SUMMARY` line and JSON schema.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutcome {
    Pass,
    WrongRows(String),
    Error(String),
    Timeout(u64),
    /// Not yet produced by `classify_vs_expected` -- vacuous-result detection
    /// (both sides zero rows) lands in a later task. Kept in the taxonomy
    /// now so `legacy_bucket`/`print_summary`/JSON reporting are ready for it.
    #[allow(dead_code)]
    Vacuous,
    Diff(String),
    Skip(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyBucket { Pass, Fail, Diff, Skip, Error }

/// The run fails only on genuine correctness/execution failures. Timeouts and
/// vacuous results are surfaced but do not fail the run (see SP1 design).
#[allow(dead_code)]
pub fn is_real_failure(o: &QueryOutcome) -> bool {
    matches!(o, QueryOutcome::Error(_) | QueryOutcome::WrongRows(_))
}

/// Collapse a `QueryOutcome` onto the five legacy report buckets so the
/// `BENCH_SUMMARY` line and JSON `summary` stay byte-compatible. Timeout folds
/// into Error exactly as the pre-refactor code did (it built
/// `TestStatus::Error("Timed out ...")`); Vacuous folds into Pass.
pub fn legacy_bucket(o: &QueryOutcome) -> LegacyBucket {
    match o {
        QueryOutcome::Pass | QueryOutcome::Vacuous => LegacyBucket::Pass,
        QueryOutcome::WrongRows(_) => LegacyBucket::Fail,
        QueryOutcome::Diff(_) => LegacyBucket::Diff,
        QueryOutcome::Skip(_) => LegacyBucket::Skip,
        QueryOutcome::Error(_) | QueryOutcome::Timeout(_) => LegacyBucket::Error,
    }
}

/// Classify an executed SQE result against the expected-rows manifest.
/// Mirrors the pre-refactor logic in `run_benchmark_test`.
pub fn classify_vs_expected(
    benchmark: &str,
    scale: f64,
    id: &str,
    batches: &[RecordBatch],
) -> QueryOutcome {
    match load_expected(benchmark, scale, id) {
        Ok(Some(expected)) => match compare_results(batches, &expected, 1e-4) {
            Ok(CompareStatus::Pass) => QueryOutcome::Pass,
            Ok(CompareStatus::Diff(m)) => QueryOutcome::Diff(m),
            Ok(CompareStatus::Fail(m)) => QueryOutcome::WrongRows(m),
            Err(e) => QueryOutcome::Error(format!("compare error: {e}")),
        },
        Ok(None) => QueryOutcome::Pass,
        Err(e) => QueryOutcome::Error(format!("failed to load expected: {e}")),
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

    #[test]
    fn test_compare_decimal_trailing_zeros() {
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Decimal128(10, 4), false),
        ]));
        // 1234500 with scale 4 = 123.4500
        let arr = Decimal128Array::from(vec![1_234_500i128])
            .with_precision_and_scale(10, 4)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let csv = "amount\n123.45\n";
        let result = compare_results(&[batch], csv, 1e-4).unwrap();
        assert!(matches!(result, CompareStatus::Pass), "trailing zeros should match: {result:?}");
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;

    #[test]
    fn is_real_failure_only_error_and_wrongrows() {
        assert!(is_real_failure(&QueryOutcome::Error("x".into())));
        assert!(is_real_failure(&QueryOutcome::WrongRows("x".into())));
        for o in [
            QueryOutcome::Pass,
            QueryOutcome::Timeout(60),
            QueryOutcome::Vacuous,
            QueryOutcome::Diff("x".into()),
            QueryOutcome::Skip("x".into()),
        ] {
            assert!(!is_real_failure(&o), "{o:?} must not be a real failure");
        }
    }

    #[test]
    fn legacy_bucket_maps_new_variants_onto_five() {
        assert_eq!(legacy_bucket(&QueryOutcome::Pass), LegacyBucket::Pass);
        assert_eq!(legacy_bucket(&QueryOutcome::Vacuous), LegacyBucket::Pass);
        assert_eq!(legacy_bucket(&QueryOutcome::WrongRows("x".into())), LegacyBucket::Fail);
        assert_eq!(legacy_bucket(&QueryOutcome::Diff("x".into())), LegacyBucket::Diff);
        assert_eq!(legacy_bucket(&QueryOutcome::Skip("x".into())), LegacyBucket::Skip);
        assert_eq!(legacy_bucket(&QueryOutcome::Error("x".into())), LegacyBucket::Error);
        assert_eq!(legacy_bucket(&QueryOutcome::Timeout(60)), LegacyBucket::Error);
    }
}
