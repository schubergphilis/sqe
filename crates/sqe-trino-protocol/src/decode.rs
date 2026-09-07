//! Trino JSON pages -> Arrow `RecordBatch`, with faithful types.
//!
//! The compare-with-Trino harness is the engine's correctness oracle, so the
//! oracle's decoder must not lose information: `decimal(p,s)` becomes
//! `Decimal128(p,s)` (not `Float64`), `tinyint`/`smallint`/`integer`/`bigint`
//! keep their widths, `real` stays `Float32`, dates and timestamps become
//! Arrow temporal types. Anything Trino serializes as a string that Arrow
//! has no scalar for (`json`, `uuid`, intervals, `array`/`map`/`row`) is kept
//! as `Utf8` verbatim.

use std::str::FromStr;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use base64::Engine;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use serde_json::Value;

use crate::types::{parse_trino_type, TrinoColumn, TrinoType};

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("column {column:?} row {row}: cannot decode {value} as {expected}")]
    Value {
        column: String,
        row: usize,
        value: String,
        expected: String,
    },
    #[error("column {column:?}: decimal({precision},{scale}) exceeds Arrow Decimal128 (precision <= 38)")]
    DecimalPrecision {
        column: String,
        precision: u8,
        scale: i8,
    },
}

/// Arrow type for a parsed Trino type.
pub fn arrow_type_for(t: &TrinoType) -> DataType {
    match t {
        TrinoType::Boolean => DataType::Boolean,
        TrinoType::TinyInt => DataType::Int8,
        TrinoType::SmallInt => DataType::Int16,
        TrinoType::Integer => DataType::Int32,
        TrinoType::BigInt => DataType::Int64,
        TrinoType::Real => DataType::Float32,
        TrinoType::Double => DataType::Float64,
        TrinoType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        TrinoType::Varchar
        | TrinoType::Char
        | TrinoType::Json
        | TrinoType::Uuid
        | TrinoType::IntervalYearToMonth
        | TrinoType::IntervalDayToSecond
        | TrinoType::Other(_) => DataType::Utf8,
        TrinoType::Varbinary => DataType::Binary,
        TrinoType::Date => DataType::Date32,
        // Arrow has no time-with-zone scalar; keep Trino's text.
        TrinoType::Time {
            with_time_zone: true,
            ..
        } => DataType::Utf8,
        TrinoType::Time { .. } => DataType::Time64(TimeUnit::Microsecond),
        TrinoType::Timestamp {
            with_time_zone: false,
            ..
        } => DataType::Timestamp(TimeUnit::Microsecond, None),
        TrinoType::Timestamp {
            with_time_zone: true,
            ..
        } => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
    }
}

/// Arrow schema for a Trino column list. Every column is nullable: the wire
/// carries no nullability.
pub fn schema_for(columns: &[TrinoColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, arrow_type_for(&parse_trino_type(&c.r#type)), true))
            .collect::<Vec<Field>>(),
    ))
}

/// Decode `rows` (row-major JSON, as Trino sends them) for `columns` into one
/// `RecordBatch`. Zero rows produce an empty batch with the right schema.
pub fn rows_to_record_batch(
    columns: &[TrinoColumn],
    rows: &[Vec<Value>],
) -> Result<RecordBatch, DecodeError> {
    let schema = schema_for(columns);
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(idx, col)| build_column(idx, col, rows))
        .collect::<Result<Vec<ArrayRef>, DecodeError>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn value_err(col: &TrinoColumn, row: usize, v: &Value, expected: &str) -> DecodeError {
    DecodeError::Value {
        column: col.name.clone(),
        row,
        value: v.to_string(),
        expected: expected.to_string(),
    }
}

fn cell(rows: &[Vec<Value>], row: usize, idx: usize) -> &Value {
    rows[row].get(idx).unwrap_or(&Value::Null)
}

fn build_column(
    idx: usize,
    col: &TrinoColumn,
    rows: &[Vec<Value>],
) -> Result<ArrayRef, DecodeError> {
    let ty = parse_trino_type(&col.r#type);
    let n = rows.len();
    macro_rules! ints {
        ($builder:ty, $cast:ty, $name:literal) => {{
            let mut b = <$builder>::with_capacity(n);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    v => {
                        let i = v
                            .as_i64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                            .and_then(|i| <$cast>::try_from(i).ok())
                            .ok_or_else(|| value_err(col, row, v, $name))?;
                        b.append_value(i);
                    }
                }
            }
            Ok(Arc::new(b.finish()) as ArrayRef)
        }};
    }
    match ty {
        TrinoType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(n);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    Value::Bool(x) => b.append_value(*x),
                    v => return Err(value_err(col, row, v, "boolean")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::TinyInt => ints!(Int8Builder, i8, "tinyint"),
        TrinoType::SmallInt => ints!(Int16Builder, i16, "smallint"),
        TrinoType::Integer => ints!(Int32Builder, i32, "integer"),
        TrinoType::BigInt => ints!(Int64Builder, i64, "bigint"),
        TrinoType::Real => {
            let mut b = Float32Builder::with_capacity(n);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    v => b.append_value(
                        parse_float(v).ok_or_else(|| value_err(col, row, v, "real"))? as f32,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::Double => {
            let mut b = Float64Builder::with_capacity(n);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    v => b.append_value(
                        parse_float(v).ok_or_else(|| value_err(col, row, v, "double"))?,
                    ),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::Decimal { precision, scale } => {
            if precision > 38 || precision == 0 || scale < 0 {
                return Err(DecodeError::DecimalPrecision {
                    column: col.name.clone(),
                    precision,
                    scale,
                });
            }
            let mut b = Decimal128Builder::with_capacity(n)
                .with_data_type(DataType::Decimal128(precision, scale));
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    v => {
                        let text = match v {
                            Value::String(s) => s.clone(),
                            Value::Number(num) => num.to_string(),
                            other => return Err(value_err(col, row, other, "decimal")),
                        };
                        let scaled = parse_decimal(&text, scale as u32)
                            .ok_or_else(|| value_err(col, row, v, "decimal"))?;
                        b.append_value(scaled);
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::Varbinary => {
            let mut b = BinaryBuilder::with_capacity(n, 0);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    Value::String(s) => {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(s)
                            .map_err(|_| {
                                value_err(col, row, &Value::String(s.clone()), "base64 varbinary")
                            })?;
                        b.append_value(bytes);
                    }
                    v => return Err(value_err(col, row, v, "varbinary")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::Date => {
            let mut b = Date32Builder::with_capacity(n);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    Value::String(s) => {
                        let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .map_err(|_| value_err(col, row, &Value::String(s.clone()), "date"))?;
                        let days = d.signed_duration_since(epoch).num_days();
                        b.append_value(i32::try_from(days).map_err(|_| {
                            value_err(col, row, &Value::String(s.clone()), "date32 range")
                        })?);
                    }
                    v => return Err(value_err(col, row, v, "date")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::Time {
            with_time_zone: false,
            ..
        } => {
            let mut b = Time64MicrosecondBuilder::with_capacity(n);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    Value::String(s) => {
                        let t = NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                            .map_err(|_| value_err(col, row, &Value::String(s.clone()), "time"))?;
                        let micros = t.signed_duration_since(NaiveTime::MIN).num_seconds()
                            * 1_000_000
                            + i64::from(t.nanosecond() / 1000);
                        b.append_value(micros);
                    }
                    v => return Err(value_err(col, row, v, "time")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::Timestamp {
            with_time_zone: false,
            ..
        } => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    Value::String(s) => {
                        let dt = parse_naive_timestamp(s).ok_or_else(|| {
                            value_err(col, row, &Value::String(s.clone()), "timestamp")
                        })?;
                        b.append_value(dt.and_utc().timestamp_micros());
                    }
                    v => return Err(value_err(col, row, v, "timestamp")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        TrinoType::Timestamp {
            with_time_zone: true,
            ..
        } => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n).with_data_type(
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            );
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    Value::String(s) => {
                        let micros = parse_zoned_timestamp_micros(s).ok_or_else(|| {
                            value_err(
                                col,
                                row,
                                &Value::String(s.clone()),
                                "timestamp with time zone",
                            )
                        })?;
                        b.append_value(micros);
                    }
                    v => return Err(value_err(col, row, v, "timestamp with time zone")),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Utf8 catch-all: varchar/char/json/uuid/intervals/time-with-zone and
        // nested types. Strings verbatim, other JSON as its text.
        _ => {
            let mut b = StringBuilder::with_capacity(n, 0);
            for row in 0..n {
                match cell(rows, row, idx) {
                    Value::Null => b.append_null(),
                    Value::String(s) => b.append_value(s),
                    other => b.append_value(other.to_string()),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

use chrono::Timelike;

fn parse_float(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        // Trino serializes non-finite doubles as strings.
        Value::String(s) => match s.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" | "+Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            other => other.parse::<f64>().ok(),
        },
        _ => None,
    }
}

/// Parse a decimal literal (`-123.4500`, `7`, `.5`) into an i128 scaled by
/// `scale`. Extra fractional digits beyond `scale` are rejected rather than
/// rounded: Trino emits exactly `scale` digits, so more means a type mismatch.
fn parse_decimal(text: &str, scale: u32) -> Option<i128> {
    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let scale = scale as usize;
    if frac_part.len() > scale {
        // Only trailing zeros may exceed the declared scale.
        if !frac_part[scale..].chars().all(|c| c == '0') {
            return None;
        }
    }
    let mut unscaled: i128 = 0;
    for c in int_part.chars() {
        unscaled = unscaled
            .checked_mul(10)?
            .checked_add(i128::from(c.to_digit(10)?))?;
    }
    let frac_digits: Vec<u32> = frac_part
        .chars()
        .take(scale)
        .map(|c| c.to_digit(10))
        .collect::<Option<_>>()?;
    for d in frac_digits
        .iter()
        .chain(std::iter::repeat_n(&0, scale - frac_digits.len()))
    {
        unscaled = unscaled.checked_mul(10)?.checked_add(i128::from(*d))?;
    }
    Some(if negative { -unscaled } else { unscaled })
}

/// `YYYY-MM-DD HH:MM:SS[.fffffffff]`; also accepts a `T` separator.
fn parse_naive_timestamp(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
}

/// Trino renders `timestamp with time zone` as `YYYY-MM-DD HH:MM:SS.fff ZONE`
/// where ZONE is `UTC`, a fixed offset (`+02:00`) or an IANA name
/// (`Europe/Amsterdam`). Returns UTC microseconds.
fn parse_zoned_timestamp_micros(s: &str) -> Option<i64> {
    let (local, zone) = s.trim().rsplit_once(' ')?;
    let naive = parse_naive_timestamp(local)?;
    if zone.eq_ignore_ascii_case("UTC") || zone == "Z" {
        return Some(naive.and_utc().timestamp_micros());
    }
    if zone.starts_with('+') || zone.starts_with('-') {
        let offset = chrono::FixedOffset::from_str(zone).ok()?;
        return offset
            .from_local_datetime(&naive)
            .single()
            .map(|dt| dt.timestamp_micros());
    }
    let tz: chrono_tz::Tz = zone.parse().ok()?;
    tz.from_local_datetime(&naive)
        .earliest()
        .map(|dt| dt.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::type_signature_for;
    use arrow_array::{Array, Decimal128Array, Int16Array, TimestampMicrosecondArray};
    use serde_json::json;

    fn col(name: &str, ty: &str) -> TrinoColumn {
        TrinoColumn {
            name: name.to_string(),
            r#type: ty.to_string(),
            type_signature: type_signature_for(ty),
        }
    }

    #[test]
    fn decimal_18_2_round_trips_as_decimal128_18_2() {
        let cols = [col("amount", "decimal(18,2)")];
        let rows = vec![
            vec![json!("1234.56")],
            vec![json!("-0.01")],
            vec![Value::Null],
        ];
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(18, 2)
        );
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 123456);
        assert_eq!(arr.value(1), -1);
        assert!(arr.is_null(2));
        assert_eq!(arr.value_as_string(0), "1234.56");
    }

    #[test]
    fn integer_widths_are_preserved() {
        let cols = [
            col("a", "tinyint"),
            col("b", "smallint"),
            col("c", "integer"),
            col("d", "bigint"),
            col("e", "real"),
        ];
        let rows = vec![vec![
            json!(-1),
            json!(300),
            json!(70000),
            json!(1i64 << 40),
            json!(1.5),
        ]];
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        let schema = batch.schema();
        let types: Vec<&DataType> = schema.fields().iter().map(|f| f.data_type()).collect();
        assert_eq!(
            types,
            vec![
                &DataType::Int8,
                &DataType::Int16,
                &DataType::Int32,
                &DataType::Int64,
                &DataType::Float32
            ]
        );
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(b.value(0), 300);
    }

    #[test]
    fn out_of_range_integer_is_an_error_not_a_wrap() {
        let cols = [col("a", "tinyint")];
        let err = rows_to_record_batch(&cols, &[vec![json!(300)]])
            .err()
            .unwrap();
        assert!(matches!(err, DecodeError::Value { .. }), "{err}");
    }

    #[test]
    fn timestamps_and_dates_decode_to_arrow_temporal_types() {
        let cols = [
            col("d", "date"),
            col("ts", "timestamp(3)"),
            col("tstz", "timestamp(3) with time zone"),
            col("t", "time(3)"),
        ];
        let rows = vec![vec![
            json!("1970-01-02"),
            json!("1970-01-01 00:00:01.500"),
            json!("1970-01-01 02:00:00.000 +02:00"),
            json!("00:00:01.250"),
        ]];
        let batch = rows_to_record_batch(&cols, &rows).unwrap();
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Date32);
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            batch.schema().field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
        assert_eq!(
            batch.schema().field(3).data_type(),
            &DataType::Time64(TimeUnit::Microsecond)
        );
        let ts = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_500_000);
        let tstz = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(tstz.value(0), 0, "+02:00 local 02:00 is the epoch in UTC");
    }

    #[test]
    fn named_zone_is_resolved() {
        // 1970-01-01 01:00 Amsterdam (CET, +01:00) is the epoch.
        assert_eq!(
            parse_zoned_timestamp_micros("1970-01-01 01:00:00.000 Europe/Amsterdam"),
            Some(0)
        );
        assert_eq!(
            parse_zoned_timestamp_micros("1970-01-01 00:00:00.000 UTC"),
            Some(0)
        );
    }

    #[test]
    fn parse_decimal_handles_signs_padding_and_rejects_overflow_of_scale() {
        assert_eq!(parse_decimal("1234.56", 2), Some(123456));
        assert_eq!(parse_decimal("-7", 2), Some(-700));
        assert_eq!(parse_decimal(".5", 1), Some(5));
        assert_eq!(parse_decimal("1.500", 2), Some(150));
        assert_eq!(parse_decimal("1.501", 2), None);
        assert_eq!(parse_decimal("abc", 2), None);
        assert_eq!(parse_decimal("", 2), None);
    }

    #[test]
    fn empty_rows_yield_empty_batch_with_schema() {
        let cols = [col("x", "varchar"), col("y", "array(bigint)")];
        let batch = rows_to_record_batch(&cols, &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn nested_values_are_kept_as_json_text() {
        let cols = [col("y", "array(bigint)")];
        let batch = rows_to_record_batch(&cols, &[vec![json!([1, 2, 3])]]).unwrap();
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "[1,2,3]");
    }
}
