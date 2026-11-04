//! Value-level comparison of the two engines' result sets.
//!
//! A row-count match says nothing about the values: a wrong sum, a wrong join
//! key or a lost decimal place returns as many rows as the right answer. This
//! module compares the rows themselves, order-independently, with numeric
//! rules that absorb the engines' type differences but not wrong numbers:
//!
//! - Exact numbers (integers, decimals) compare exactly after rounding both
//!   sides half-up to the smaller of the two scales. DataFusion's
//!   `avg(decimal(7,2))` is `decimal(11,6)` where Trino's is `decimal(7,2)`;
//!   12.345000 and 12.35 are the same answer, 12.34 and 12.35 are not.
//! - Floating point (on either side) compares with a relative tolerance:
//!   aggregation order changes the last bits.
//! - Everything else compares by its display string; NULL only equals NULL.

use crate::report::CompareStatusReport;
use arrow::util::display::array_value_to_string;
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use std::cmp::Ordering;

/// Relative tolerance for cells where either engine returns floating point.
const FLOAT_REL_TOL: f64 = 1e-6;
/// Floor of the float tolerance, for values at or near zero.
const FLOAT_ABS_TOL: f64 = 1e-9;
/// Significant digits of a float cell's sort key.
const FLOAT_KEY_DIGITS: usize = 6;

/// How one result column is compared, from both engines' types.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Rule {
    /// Exact numbers, rounded to this scale.
    Exact(i8),
    /// Floating point on at least one side.
    Float,
    /// Display string.
    Text,
}

#[derive(Clone, Copy)]
enum Kind {
    Exact(i8),
    Float,
    Other,
}

fn kind(dt: &DataType) -> Kind {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Kind::Exact(0),
        DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s) => Kind::Exact(*s),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Kind::Float,
        _ => Kind::Other,
    }
}

fn rule(sqe: &DataType, trino: &DataType) -> Rule {
    match (kind(sqe), kind(trino)) {
        (Kind::Exact(a), Kind::Exact(b)) => Rule::Exact(a.min(b)),
        (Kind::Other, _) | (_, Kind::Other) => Rule::Text,
        _ => Rule::Float,
    }
}

/// One cell, normalised under its column's rule.
#[derive(Clone, Debug, PartialEq)]
enum Cell {
    Null,
    /// Unscaled value at the column's rule scale.
    Exact(i128),
    Float(f64),
    Text(String),
}

impl Cell {
    /// Sort key: equal cells must get equal keys on both engines.
    fn key(&self) -> String {
        match self {
            Cell::Null => String::new(),
            Cell::Exact(v) => format!("e{v:040}"),
            Cell::Float(v) => {
                let v = if *v == 0.0 { 0.0 } else { *v };
                format!("f{v:.*e}", FLOAT_KEY_DIGITS - 1)
            }
            Cell::Text(s) => format!("t{s}"),
        }
    }

    fn display(&self, scale: i8) -> String {
        match self {
            Cell::Null => "NULL".to_string(),
            Cell::Exact(v) => format_unscaled(*v, scale),
            Cell::Float(v) => v.to_string(),
            Cell::Text(s) => s.clone(),
        }
    }

    fn matches(&self, other: &Cell) -> bool {
        match (self, other) {
            (Cell::Float(a), Cell::Float(b)) => {
                (a - b).abs() <= FLOAT_ABS_TOL.max(FLOAT_REL_TOL * a.abs().max(b.abs()))
            }
            _ => self == other,
        }
    }
}

/// Split a decimal display string ("-12.3450") into (unscaled, scale).
fn parse_exact(s: &str) -> Option<(i128, i8)> {
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    let scale = i8::try_from(frac.len()).ok()?;
    let digits = format!("{int}{frac}");
    Some((digits.parse::<i128>().ok()?, scale))
}

/// Round `v` at scale `from` half-up (away from zero) to scale `to`.
fn rescale(v: i128, from: i8, to: i8) -> Option<i128> {
    match from.cmp(&to) {
        Ordering::Equal => Some(v),
        Ordering::Less => v.checked_mul(10i128.checked_pow(u32::from((to - from) as u8))?),
        Ordering::Greater => {
            let d = 10i128.checked_pow(u32::from((from - to) as u8))?;
            let (q, r) = (v / d, v % d);
            Some(if r.abs() * 2 >= d { q + v.signum() } else { q })
        }
    }
}

fn format_unscaled(v: i128, scale: i8) -> String {
    if scale <= 0 {
        return v.to_string();
    }
    let s = v.unsigned_abs().to_string();
    let scale = scale as usize;
    let padded = format!("{s:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    let sign = if v < 0 { "-" } else { "" };
    format!("{sign}{int}.{frac}")
}

fn cell(array: &dyn Array, row: usize, rule: Rule) -> Cell {
    if array.is_null(row) {
        return Cell::Null;
    }
    let text = array_value_to_string(array, row).unwrap_or_default();
    match rule {
        Rule::Text => Cell::Text(text),
        Rule::Float => text
            .parse::<f64>()
            .map(Cell::Float)
            .unwrap_or(Cell::Text(text)),
        Rule::Exact(to) => parse_exact(&text)
            .and_then(|(v, from)| rescale(v, from, to))
            .map(Cell::Exact)
            .unwrap_or(Cell::Text(text)),
    }
}

fn rows(batches: &[RecordBatch], rules: &[Rule]) -> Vec<Vec<Cell>> {
    let mut out: Vec<Vec<Cell>> = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            out.push(
                rules
                    .iter()
                    .enumerate()
                    .map(|(col, rule)| cell(batch.column(col).as_ref(), row, *rule))
                    .collect(),
            );
        }
    }
    out.sort_by_cached_key(|r| r.iter().map(Cell::key).collect::<Vec<_>>());
    out
}

fn column_types(batches: &[RecordBatch]) -> Option<Vec<(String, DataType)>> {
    let schema = batches.first()?.schema();
    Some(
        schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect(),
    )
}

/// Compare two result sets row by row, ignoring row order. Returns `None`
/// when every row matches, else a description of the first difference and
/// how many rows differ. Callers check row counts first; a count mismatch
/// here is reported too.
pub fn value_diff(sqe: &[RecordBatch], trino: &[RecordBatch]) -> Option<String> {
    let (sqe_cols, trino_cols) = match (column_types(sqe), column_types(trino)) {
        (Some(s), Some(t)) => (s, t),
        // No batch on at least one side: nothing to compare values of.
        _ => return None,
    };
    if sqe_cols.len() != trino_cols.len() {
        return Some(format!(
            "column count: SQE {}, Trino {}",
            sqe_cols.len(),
            trino_cols.len()
        ));
    }
    let rules: Vec<Rule> = sqe_cols
        .iter()
        .zip(&trino_cols)
        .map(|((_, s), (_, t))| rule(s, t))
        .collect();
    let (a, b) = (rows(sqe, &rules), rows(trino, &rules));
    if a.len() != b.len() {
        return Some(format!("row count: SQE {}, Trino {}", a.len(), b.len()));
    }
    let mut first = None;
    let mut differing = 0usize;
    for (i, (ra, rb)) in a.iter().zip(&b).enumerate() {
        let bad = ra.iter().zip(rb).position(|(x, y)| !x.matches(y));
        if let Some(col) = bad {
            differing += 1;
            if first.is_none() {
                let scale = match rules[col] {
                    Rule::Exact(s) => s,
                    _ => 0,
                };
                first = Some(format!(
                    "sorted row {}, column {} `{}`: SQE {} vs Trino {}",
                    i + 1,
                    col + 1,
                    sqe_cols[col].0,
                    ra[col].display(scale),
                    rb[col].display(scale)
                ));
            }
        }
    }
    first.map(|f| format!("{f} ({differing} of {} rows differ)", a.len()))
}

/// Refine a row-count verdict with the values: a `Match` whose rows differ
/// becomes `ValueDiff`, with the difference. Every other status stands.
pub fn refine_status(
    status: CompareStatusReport,
    sqe: Option<&[RecordBatch]>,
    trino: Option<&[RecordBatch]>,
) -> (CompareStatusReport, Option<String>) {
    let (CompareStatusReport::Match, Some(sqe), Some(trino)) = (&status, sqe, trino) else {
        return (status, None);
    };
    match value_diff(sqe, trino) {
        Some(diff) => (CompareStatusReport::ValueDiff, Some(diff)),
        None => (status, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Decimal128Array, Float64Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch(cols: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            cols.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap()
    }

    fn dec(values: Vec<Option<i128>>, p: u8, s: i8) -> Arc<dyn Array> {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        )
    }

    fn strs(values: Vec<Option<&str>>) -> Arc<dyn Array> {
        Arc::new(StringArray::from(values))
    }

    #[test]
    fn equal_rows_in_another_order_match() {
        let sqe = batch(vec![
            ("k", strs(vec![Some("a"), Some("b"), None])),
            ("n", Arc::new(Int64Array::from(vec![1, 2, 3])) as _),
        ]);
        let trino = batch(vec![
            ("k", strs(vec![None, Some("b"), Some("a")])),
            ("n", Arc::new(Int64Array::from(vec![3, 2, 1])) as _),
        ]);
        assert_eq!(value_diff(&[sqe], &[trino]), None);
    }

    #[test]
    fn decimal_scales_round_to_the_smaller_one() {
        // DataFusion avg(decimal(7,2)) -> decimal(11,6); Trino keeps (7,2).
        let sqe = batch(vec![(
            "avg",
            dec(vec![Some(12_345_000), Some(-1_004_999), None], 11, 6),
        )]);
        let trino = batch(vec![("avg", dec(vec![Some(1235), Some(-100), None], 7, 2))]);
        assert_eq!(value_diff(&[sqe], &[trino]), None);
    }

    #[test]
    fn a_wrong_cent_is_a_value_diff() {
        let sqe = batch(vec![("s", dec(vec![Some(1234)], 7, 2))]);
        let trino = batch(vec![("s", dec(vec![Some(1235)], 7, 2))]);
        let d = value_diff(&[sqe], &[trino]).expect("12.34 vs 12.35 must differ");
        assert!(d.contains("SQE 12.34 vs Trino 12.35"), "{d}");
        assert!(d.contains("1 of 1 rows"), "{d}");
    }

    fn int(v: i64) -> Arc<dyn Array> {
        Arc::new(Int64Array::from(vec![v]))
    }

    fn float(v: f64) -> Arc<dyn Array> {
        Arc::new(Float64Array::from(vec![v]))
    }

    #[test]
    fn integer_and_decimal_compare_by_value() {
        let sqe = batch(vec![("c", int(100))]);
        let trino = batch(vec![("c", dec(vec![Some(10_000)], 10, 2))]);
        assert_eq!(value_diff(std::slice::from_ref(&sqe), &[trino]), None);
        // 100 must never equal 1.00.
        let trino = batch(vec![("c", dec(vec![Some(100)], 10, 2))]);
        assert!(value_diff(&[sqe], &[trino]).is_some());
    }

    #[test]
    fn floats_compare_with_relative_tolerance() {
        let sqe = batch(vec![("f", float(1234.5678901))]);
        let near = batch(vec![("f", float(1234.5678902))]);
        assert_eq!(value_diff(std::slice::from_ref(&sqe), &[near]), None);
        let far = batch(vec![("f", float(1234.6))]);
        assert!(value_diff(std::slice::from_ref(&sqe), &[far]).is_some());
        // A float on one side and a decimal on the other is a float column.
        let as_dec = batch(vec![("f", dec(vec![Some(123_456_789_010)], 18, 8))]);
        assert_eq!(value_diff(&[sqe], &[as_dec]), None);
    }

    #[test]
    fn null_only_equals_null() {
        let sqe = batch(vec![("k", strs(vec![None]))]);
        let trino = batch(vec![("k", strs(vec![Some("NULL")]))]);
        assert!(value_diff(&[sqe], &[trino]).is_some());
    }

    #[test]
    fn column_count_and_empty_inputs() {
        let one = batch(vec![("a", strs(vec![Some("x")]))]);
        let two = batch(vec![
            ("a", strs(vec![Some("x")])),
            ("b", strs(vec![Some("y")])),
        ]);
        let diff = value_diff(std::slice::from_ref(&one), &[two]).unwrap();
        assert!(diff.starts_with("column count"), "{diff}");
        assert_eq!(value_diff(&[], &[one]), None);
    }

    #[test]
    fn rescale_rounds_half_away_from_zero() {
        assert_eq!(rescale(12_345, 3, 2), Some(1235));
        assert_eq!(rescale(-12_345, 3, 2), Some(-1235));
        assert_eq!(rescale(12_344, 3, 2), Some(1234));
        assert_eq!(rescale(12, 0, 2), Some(1200));
        assert_eq!(format_unscaled(-5, 2), "-0.05");
        assert_eq!(parse_exact("-0.05"), Some((-5, 2)));
    }
}
