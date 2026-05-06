//! Extended Trino-compatible functions for DataFusion.
//!
//! This module contains additional Trino SQL functions beyond the core set
//! in `trino_functions.rs`. Split into a separate file for maintainability.

use std::any::Any;
use std::sync::{Arc, LazyLock};

use serde_json;

use arrow::array::{Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::scalar::ScalarValue;

/// Register all extended Trino-compatible functions.
pub fn register_extended_trino_functions(ctx: &datafusion::prelude::SessionContext) {
    // Trivial aliases
    ctx.register_udf(ScalarUDF::from(Every));
    ctx.register_udf(ScalarUDF::from(Millisecond));
    ctx.register_udf(ScalarUDF::from(Infinity));
    ctx.register_udf(ScalarUDF::from(Nan));
    ctx.register_udf(ScalarUDF::from(IsJsonScalar));
    ctx.register_udf(ScalarUDF::from(JsonArrayContains));

    // Simple UDFs
    ctx.register_udf(ScalarUDF::from(Soundex));
    ctx.register_udf(ScalarUDF::from(HammingDistance));
    ctx.register_udf(ScalarUDF::from(FromBase));
    ctx.register_udf(ScalarUDF::from(ToBase));
    ctx.register_udf(ScalarUDF::from(FromIso8601Date));
    ctx.register_udf(ScalarUDF::from(FromIso8601Timestamp));
    ctx.register_udf(ScalarUDF::from(ToIso8601));
    ctx.register_udf(ScalarUDF::from(CurrentTimezone));
    ctx.register_udf(ScalarUDF::from(HumanReadableSeconds));
    ctx.register_udf(ScalarUDF::from(LastDayOfMonth));

    // Medium UDFs
    ctx.register_udf(ScalarUDF::from(RegexpExtract));
    ctx.register_udf(ScalarUDF::from(RegexpExtractAll));
    ctx.register_udf(ScalarUDF::from(RegexpSplit));
    ctx.register_udf(ScalarUDF::from(Normalize));
    ctx.register_udf(ScalarUDF::from(WithTimezone));
    ctx.register_udf(ScalarUDF::from(AtTimezone));

    // Hard UDFs
    ctx.register_udf(ScalarUDF::from(FormatDatetime));
    ctx.register_udf(ScalarUDF::from(ParseDatetime));
    // word_stem(s) and word_stem(s, lang) live under the same name with a
    // OneOf signature; word_stem_lang is registered as an alias for
    // backward compatibility with callers that adopted the old separate
    // name. Both `word_stem` and `word_stem_lang` resolve to the same
    // implementation.
    ctx.register_udf(ScalarUDF::from(WordStem));

    // Aggregate-like scalar aliases
    ctx.register_udf(ScalarUDF::from(Arbitrary));
    ctx.register_udf(ScalarUDF::from(MaxBy));
    ctx.register_udf(ScalarUDF::from(MinBy));
    ctx.register_udf(ScalarUDF::from(Checksum));
    ctx.register_udf(ScalarUDF::from(TimezoneHour));
    ctx.register_udf(ScalarUDF::from(TimezoneMinute));
    ctx.register_udf(ScalarUDF::from(JsonSize));
    ctx.register_udf(ScalarUDF::from(JsonArrayGet));

    // TRY(expr) — error-suppressing wrapper
    ctx.register_udf(ScalarUDF::from(Try));

    // Format / JSON
    ctx.register_udf(ScalarUDF::from(Format));
    ctx.register_udf(ScalarUDF::from(ToJson));
}

// ═══════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════

/// Apply a scalar string→string transform to a ColumnarValue.
fn str_transform(
    arg: &ColumnarValue,
    f: impl Fn(&str) -> Option<String>,
) -> DFResult<ColumnarValue> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(f(s))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        ColumnarValue::Array(arr) => {
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
            let result: StringArray = str_arr.iter().map(|opt| opt.and_then(&f)).collect();
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
    }
}

/// Apply a transform over two string ColumnarValues (scalar/scalar or array/scalar).
fn str_transform_2(
    args: &[ColumnarValue],
    f: impl Fn(&str, &str) -> Option<String>,
) -> DFResult<ColumnarValue> {
    match (&args[0], &args[1]) {
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s1))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s2))),
        ) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(f(s1, s2)))),
        (ColumnarValue::Array(arr), ColumnarValue::Scalar(ScalarValue::Utf8(Some(s2)))) => {
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.and_then(|s| f(s, s2)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
    }
}

// ═══════════════════════════════════════════════════════════════════
// TRIVIAL ALIASES
// ═══════════════════════════════════════════════════════════════════

/// every(x) — scalar passthrough (aggregate form is bool_and(), already built-in)
#[derive(Debug, PartialEq, Eq, Hash)]
struct Every;

impl ScalarUDFImpl for Every {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "every"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

/// millisecond(ts) — extract millisecond component (0–999)
#[derive(Debug, PartialEq, Eq, Hash)]
struct Millisecond;

impl ScalarUDFImpl for Millisecond {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "millisecond"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow::array::TimestampMicrosecondArray;
        match &args.args[0] {
            ColumnarValue::Array(arr) => {
                if let Some(ts_arr) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    let result: Int64Array =
                        ts_arr.iter().map(|opt| opt.map(|us| (us / 1000) % 1000)).collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)))
                }
            }
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(us), _)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some((us / 1000) % 1000))))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
        }
    }
}

/// infinity() → f64::INFINITY
#[derive(Debug, PartialEq, Eq, Hash)]
struct Infinity;

impl ScalarUDFImpl for Infinity {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "infinity"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> = LazyLock::new(|| {
            Signature::new(TypeSignature::Exact(vec![]), Volatility::Immutable)
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
            f64::INFINITY,
        ))))
    }
}

/// nan() → f64::NAN
#[derive(Debug, PartialEq, Eq, Hash)]
struct Nan;

impl ScalarUDFImpl for Nan {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "nan"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> = LazyLock::new(|| {
            Signature::new(TypeSignature::Exact(vec![]), Volatility::Immutable)
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN))))
    }
}

/// is_json_scalar(json) → BOOLEAN — true if the JSON value is a scalar (not object/array)
#[derive(Debug, PartialEq, Eq, Hash)]
struct IsJsonScalar;

impl ScalarUDFImpl for IsJsonScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "is_json_scalar"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let is_scalar = serde_json::from_str::<serde_json::Value>(s)
                    .map(|v| !v.is_object() && !v.is_array())
                    .unwrap_or(false);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(is_scalar))))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
                let result: BooleanArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .map(|v| !v.is_object() && !v.is_array())
                                .unwrap_or(false)
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))),
        }
    }
}

/// json_array_contains(json, value) → BOOLEAN
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayContains;

impl ScalarUDFImpl for JsonArrayContains {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_array_contains"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (ColumnarValue::Scalar(ScalarValue::Utf8(Some(json))), val) => {
                let val_str = scalar_to_json_comparable(val);
                let contains = check_json_array_contains(json, &val_str);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(contains))))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))),
        }
    }
}

fn scalar_to_json_comparable(val: &ColumnarValue) -> String {
    match val {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => format!("\"{}\"", s),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => n.to_string(),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(f))) => f.to_string(),
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => b.to_string(),
        _ => "null".to_string(),
    }
}

fn check_json_array_contains(json: &str, search_val: &str) -> bool {
    if let Ok(serde_json::Value::Array(arr)) = serde_json::from_str::<serde_json::Value>(json) {
        let search: serde_json::Value =
            serde_json::from_str(search_val).unwrap_or(serde_json::Value::Null);
        arr.contains(&search)
    } else {
        false
    }
}

// ═══════════════════════════════════════════════════════════════════
// SIMPLE UDFs
// ═══════════════════════════════════════════════════════════════════

/// soundex(s) → 4-character Soundex phonetic code
#[derive(Debug, PartialEq, Eq, Hash)]
struct Soundex;

impl ScalarUDFImpl for Soundex {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "soundex"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        str_transform(&args.args[0], |s| Some(compute_soundex(s)))
    }
}

fn compute_soundex(s: &str) -> String {
    let s = s.to_uppercase();
    let mut chars = s.chars().filter(|c| c.is_ascii_alphabetic());
    let first = match chars.next() {
        Some(c) => c,
        None => return "0000".to_string(),
    };
    let code = |c: char| -> Option<char> {
        match c {
            'B' | 'F' | 'P' | 'V' => Some('1'),
            'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
            'D' | 'T' => Some('3'),
            'L' => Some('4'),
            'M' | 'N' => Some('5'),
            'R' => Some('6'),
            _ => None,
        }
    };
    let mut result = String::with_capacity(4);
    result.push(first);
    let mut last_code = code(first);
    for c in chars {
        let c_code = code(c);
        if let Some(cc) = c_code {
            if c_code != last_code {
                result.push(cc);
            }
            if result.len() == 4 {
                break;
            }
        }
        last_code = c_code;
    }
    while result.len() < 4 {
        result.push('0');
    }
    result
}

/// hamming_distance(s1, s2) → number of positions where characters differ
#[derive(Debug, PartialEq, Eq, Hash)]
struct HammingDistance;

impl ScalarUDFImpl for HammingDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "hamming_distance"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s1))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s2))),
            ) => {
                if s1.len() != s2.len() {
                    return Err(DataFusionError::Execution(
                        "Strings must have equal length for hamming_distance".into(),
                    ));
                }
                let dist = s1.bytes().zip(s2.bytes()).filter(|(a, b)| a != b).count() as i64;
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(dist))))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
        }
    }
}

/// from_base(s, radix) → BIGINT parsed from base-`radix` string
#[derive(Debug, PartialEq, Eq, Hash)]
struct FromBase;

impl ScalarUDFImpl for FromBase {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "from_base"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))),
                ColumnarValue::Scalar(v),
            ) => {
                let radix = match v {
                    ScalarValue::Int64(Some(r)) => *r as u32,
                    ScalarValue::Int32(Some(r)) => *r as u32,
                    _ => return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
                };
                let result = i64::from_str_radix(s, radix).ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
        }
    }
}

/// to_base(n, radix) → VARCHAR representation of n in base `radix`
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToBase;

impl ScalarUDFImpl for ToBase {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_base"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Int64(Some(n))),
                ColumnarValue::Scalar(v),
            ) => {
                let radix = match v {
                    ScalarValue::Int64(Some(r)) => *r as u32,
                    ScalarValue::Int32(Some(r)) => *r as u32,
                    _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };
                let result = match radix {
                    2 => Some(format!("{:b}", n)),
                    8 => Some(format!("{:o}", n)),
                    10 => Some(n.to_string()),
                    16 => Some(format!("{:x}", n)),
                    _ => {
                        let mut num = if *n < 0 { (-*n) as u64 } else { *n as u64 };
                        if num == 0 {
                            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                                "0".to_string(),
                            ))));
                        }
                        let digits = "0123456789abcdefghijklmnopqrstuvwxyz";
                        let mut s = String::new();
                        while num > 0 {
                            let d = (num % radix as u64) as usize;
                            s.push(digits.as_bytes()[d] as char);
                            num /= radix as u64;
                        }
                        if *n < 0 {
                            s.push('-');
                        }
                        Some(s.chars().rev().collect())
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

/// from_iso8601_date(s) → DATE
#[derive(Debug, PartialEq, Eq, Hash)]
struct FromIso8601Date;

impl ScalarUDFImpl for FromIso8601Date {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "from_iso8601_date"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let date = parse_iso_date(s);
                let days = date.map(|d| d.signed_duration_since(epoch).num_days() as i32);
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(days)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
                let result: Date32Array = str_arr
                    .iter()
                    .map(|opt| {
                        opt.and_then(|s| {
                            parse_iso_date(s)
                                .map(|d| d.signed_duration_since(epoch).num_days() as i32)
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Date32(None))),
        }
    }
}

fn parse_iso_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))
        .ok()
}

/// from_iso8601_timestamp(s) → TIMESTAMP WITH MICROSECOND precision
#[derive(Debug, PartialEq, Eq, Hash)]
struct FromIso8601Timestamp;

impl ScalarUDFImpl for FromIso8601Timestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "from_iso8601_timestamp"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let ts = parse_iso_timestamp(s);
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    ts, None,
                )))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            ))),
        }
    }
}

fn parse_iso_timestamp(s: &str) -> Option<i64> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .ok()
        .map(|dt| dt.and_utc().timestamp_micros())
}

/// to_iso8601(ts_or_date) → VARCHAR ISO 8601 string
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToIso8601;

impl ScalarUDFImpl for ToIso8601 {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_iso8601"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(us), _)) => {
                let dt = chrono::DateTime::from_timestamp_micros(*us)
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(dt)))
            }
            ColumnarValue::Scalar(ScalarValue::Date32(Some(days))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(*days as i64);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    date.format("%Y-%m-%d").to_string(),
                ))))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

/// current_timezone() → VARCHAR — returns 'UTC' (SQE runs in UTC)
#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentTimezone;

impl ScalarUDFImpl for CurrentTimezone {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "current_timezone"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> = LazyLock::new(|| {
            Signature::new(TypeSignature::Exact(vec![]), Volatility::Stable)
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "UTC".to_string(),
        ))))
    }
}

/// human_readable_seconds(n) → VARCHAR human-friendly duration string
#[derive(Debug, PartialEq, Eq, Hash)]
struct HumanReadableSeconds;

impl ScalarUDFImpl for HumanReadableSeconds {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "human_readable_seconds"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let secs = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(f))) => Some(*f),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => Some(*n as f64),
            ColumnarValue::Array(arr) => {
                // For arrays: process element-wise
                if let Some(f_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                    let result: StringArray = f_arr
                        .iter()
                        .map(|opt| opt.map(format_seconds))
                        .collect();
                    return Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef));
                } else if let Some(i_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                    let result: StringArray = i_arr
                        .iter()
                        .map(|opt| opt.map(|n| format_seconds(n as f64)))
                        .collect();
                    return Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef));
                }
                None
            }
            _ => None,
        };
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            secs.map(format_seconds),
        )))
    }
}

fn format_seconds(total: f64) -> String {
    let total = total.abs();
    let hours = (total / 3600.0) as u64;
    let minutes = ((total % 3600.0) / 60.0) as u64;
    let seconds = total % 60.0;
    if hours > 0 {
        format!("{} hours, {} minutes, {:.2} seconds", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{} minutes, {:.2} seconds", minutes, seconds)
    } else {
        format!("{:.2} seconds", seconds)
    }
}

/// last_day_of_month(date) → DATE — the last calendar day of the month containing `date`
#[derive(Debug, PartialEq, Eq, Hash)]
struct LastDayOfMonth;

impl ScalarUDFImpl for LastDayOfMonth {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "last_day_of_month"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(days))) => {
                let date = epoch + chrono::Duration::days(*days as i64);
                let last = last_day_of_month(date);
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(
                    last.signed_duration_since(epoch).num_days() as i32,
                ))))
            }
            ColumnarValue::Array(arr) => {
                let date_arr = arr
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| DataFusionError::Internal("Expected Date32Array".into()))?;
                let result: Date32Array = date_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = epoch + chrono::Duration::days(days as i64);
                            let last = last_day_of_month(date);
                            last.signed_duration_since(epoch).num_days() as i32
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Date32(None))),
        }
    }
}

fn last_day_of_month(date: NaiveDate) -> NaiveDate {
    let (y, m) = if date.month() == 12 {
        (date.year() + 1, 1)
    } else {
        (date.year(), date.month() + 1)
    };
    NaiveDate::from_ymd_opt(y, m, 1)
        .unwrap()
        .pred_opt()
        .unwrap()
}

// ═══════════════════════════════════════════════════════════════════
// MEDIUM UDFs
// ═══════════════════════════════════════════════════════════════════

/// regexp_extract(s, pattern) → VARCHAR first match (or first capture group)
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtract;

impl ScalarUDFImpl for RegexpExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_extract"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        str_transform_2(&args.args, |s, pattern| {
            let re = regex::Regex::new(pattern).ok()?;
            let caps = re.captures(s)?;
            if caps.len() > 1 {
                caps.get(1).map(|m| m.as_str().to_string())
            } else {
                caps.get(0).map(|m| m.as_str().to_string())
            }
        })
    }
}

/// regexp_extract_all(s, pattern) → ARRAY(VARCHAR) of all matches.
///
/// Trino returns ARRAY(VARCHAR). Earlier SQE versions returned a JSON-array
/// string for compatibility with callers that did not have ARRAY support;
/// now that DataFusion's ARRAY plumbing is solid, this returns the proper
/// `List<Utf8>` so downstream operators (UNNEST, cardinality, element_at)
/// work without parsing the result.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtractAll;

impl ScalarUDFImpl for RegexpExtractAll {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_extract_all"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let strings = column_strings(&args.args, 0, "regexp_extract_all", args.number_rows)?;
        let pattern_arr =
            column_strings(&args.args, 1, "regexp_extract_all", args.number_rows)?;
        build_regex_list_array("regexp_extract_all", &strings, &pattern_arr, |s, re| {
            re.find_iter(s).map(|m| m.as_str().to_string()).collect()
        })
    }
}

/// regexp_split(s, pattern) → ARRAY(VARCHAR) of parts split by the pattern.
///
/// Same return-type story as regexp_extract_all: real `List<Utf8>` instead
/// of a JSON-array string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpSplit;

impl ScalarUDFImpl for RegexpSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_split"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let strings = column_strings(&args.args, 0, "regexp_split", args.number_rows)?;
        let pattern_arr = column_strings(&args.args, 1, "regexp_split", args.number_rows)?;
        build_regex_list_array("regexp_split", &strings, &pattern_arr, |s, re| {
            re.split(s).map(|p| p.to_string()).collect()
        })
    }
}

/// Coerce a string-typed column or scalar argument into an owned StringArray
/// of length `n_rows` so the regex evaluators can iterate row-by-row without
/// caring whether the input is a scalar broadcast or a real array column.
fn column_strings(
    args: &[ColumnarValue],
    idx: usize,
    fn_name: &str,
    n_rows: usize,
) -> DFResult<StringArray> {
    let arg = args.get(idx).ok_or_else(|| {
        DataFusionError::Plan(format!("{fn_name}: missing argument at index {idx}"))
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt)) => {
            let v = opt.clone();
            Ok(StringArray::from_iter(std::iter::repeat_n(
                v.as_deref(),
                n_rows.max(1),
            )))
        }
        ColumnarValue::Array(arr) => {
            if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
                Ok(s.clone())
            } else if let Some(s) = arr.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
                let conv = arrow::compute::cast(s, &DataType::Utf8).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "{fn_name}: cannot convert LargeUtf8 to Utf8: {e}"
                    ))
                })?;
                Ok(conv
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "{fn_name}: cast to StringArray failed silently"
                        ))
                    })?
                    .clone())
            } else {
                Err(DataFusionError::Plan(format!(
                    "{fn_name}: argument {idx} must be Utf8 / VARCHAR, got {:?}",
                    arr.data_type()
                )))
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "{fn_name}: argument {idx} must be a string scalar or array, got {other:?}"
        ))),
    }
}

/// Build a `ListArray` of Utf8 by applying `f(input_string, compiled_regex)`
/// row-by-row. Used by both regexp_extract_all and regexp_split. Invalid
/// patterns surface as a Plan error rather than a silent NULL row, mirroring
/// Trino's behaviour.
fn build_regex_list_array(
    fn_name: &str,
    strings: &StringArray,
    patterns: &StringArray,
    f: impl Fn(&str, &regex::Regex) -> Vec<String>,
) -> DFResult<ColumnarValue> {
    use arrow::array::{ListBuilder, StringBuilder};
    let n = strings.len();
    let mut builder = ListBuilder::new(StringBuilder::new());
    for i in 0..n {
        if strings.is_null(i) || patterns.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = strings.value(i);
        let p = patterns.value(i);
        let re = regex::Regex::new(p).map_err(|e| {
            DataFusionError::Plan(format!("{fn_name}: invalid regex pattern '{p}': {e}"))
        })?;
        let parts = f(s, &re);
        for part in parts {
            builder.values().append_value(part);
        }
        builder.append(true);
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
}

/// normalize(s, form) → Unicode-normalized string (NFC/NFD/NFKC/NFKD)
#[derive(Debug, PartialEq, Eq, Hash)]
struct Normalize;

impl ScalarUDFImpl for Normalize {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "normalize"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use unicode_normalization::UnicodeNormalization;
        str_transform_2(&args.args, |s, form| {
            let result = match form.to_uppercase().as_str() {
                "NFC" => s.nfc().collect::<String>(),
                "NFD" => s.nfd().collect::<String>(),
                "NFKC" => s.nfkc().collect::<String>(),
                "NFKD" => s.nfkd().collect::<String>(),
                _ => s.nfc().collect::<String>(),
            };
            Some(result)
        })
    }
}

/// with_timezone(ts, tz) → TIMESTAMP WITH TIME ZONE — attaches the given timezone to the timestamp
#[derive(Debug, PartialEq, Eq, Hash)]
struct WithTimezone;

impl ScalarUDFImpl for WithTimezone {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "with_timezone"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(us), _)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz_str))),
            ) => {
                use chrono_tz::Tz;
                let _tz: Tz = tz_str.parse().map_err(|_| {
                    DataFusionError::Execution(format!("Invalid timezone: {}", tz_str))
                })?;
                // Return same micros, but tagged with the timezone name
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(*us),
                    Some(tz_str.clone().into()),
                )))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            ))),
        }
    }
}

/// at_timezone(ts, tz) → TIMESTAMP — converts UTC timestamp to the given timezone offset
#[derive(Debug, PartialEq, Eq, Hash)]
struct AtTimezone;

impl ScalarUDFImpl for AtTimezone {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "at_timezone"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(us), _)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz_str))),
            ) => {
                use chrono::TimeZone;
                use chrono_tz::Tz;
                let tz: Tz = tz_str.parse().map_err(|_| {
                    DataFusionError::Execution(format!("Invalid timezone: {}", tz_str))
                })?;
                let utc_dt = chrono::DateTime::from_timestamp_micros(*us)
                    .ok_or_else(|| DataFusionError::Execution("Invalid timestamp".into()))?;
                // Convert UTC → local timezone
                let local_dt = utc_dt.with_timezone(&tz);
                // Represent the local wall-clock time as UTC micros (for display purposes)
                let local_naive = local_dt.naive_local();
                let as_utc = chrono::Utc.from_utc_datetime(&local_naive);
                let us_out = as_utc.timestamp_micros();
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(us_out),
                    Some(tz_str.clone().into()),
                )))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            ))),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// HARD UDFs
// ═══════════════════════════════════════════════════════════════════

/// format_datetime(ts, joda_format) → VARCHAR — format using Joda-style patterns
#[derive(Debug, PartialEq, Eq, Hash)]
struct FormatDatetime;

impl ScalarUDFImpl for FormatDatetime {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "format_datetime"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(us), _)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(joda_fmt))),
            ) => {
                let chrono_fmt = joda_to_chrono(joda_fmt);
                let dt = chrono::DateTime::from_timestamp_micros(*us)
                    .map(|dt| dt.format(&chrono_fmt).to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(dt)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

/// parse_datetime(s, joda_format) → TIMESTAMP — parse using Joda-style patterns
#[derive(Debug, PartialEq, Eq, Hash)]
struct ParseDatetime;

impl ScalarUDFImpl for ParseDatetime {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "parse_datetime"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(joda_fmt))),
            ) => {
                let chrono_fmt = joda_to_chrono(joda_fmt);
                let ts = NaiveDateTime::parse_from_str(s, &chrono_fmt)
                    .ok()
                    .map(|dt| dt.and_utc().timestamp_micros());
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    ts, None,
                )))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                None, None,
            ))),
        }
    }
}

/// Translate Joda datetime format patterns to chrono format patterns.
/// Covers the most common patterns used in Trino queries.
fn joda_to_chrono(joda: &str) -> String {
    // Order matters — longer patterns must be replaced before shorter ones
    joda.replace("yyyy", "%Y")
        .replace("yy", "%y")
        .replace("MMMM", "%B")  // full month name
        .replace("MMM", "%b")   // abbreviated month
        .replace("MM", "%m")    // zero-padded month
        .replace("M", "%m")
        .replace("dd", "%d")    // zero-padded day
        .replace("d", "%d")
        .replace("HH", "%H")    // 24-hour
        .replace("hh", "%I")    // 12-hour
        .replace("H", "%H")
        .replace("h", "%I")
        .replace("mm", "%M")    // minute
        .replace("ss", "%S")    // second
        .replace("SSS", "%.3f") // milliseconds
        .replace("SS", "%.2f")
        .replace("EEEE", "%A")  // full day name (must be before EEE)
        .replace("EEE", "%a")   // abbreviated day name
        .replace("ZZ", "%:z")   // timezone offset with colon
        .replace("Z", "%z")     // timezone offset
        .replace("a", "%p")     // AM/PM
}

/// word_stem(s) or word_stem(s, lang) → stemmed word in the specified language.
///
/// Trino's `word_stem` accepts both arities under the same name. DataFusion's
/// signature system supports the `OneOf` variant for that. The 1-arg form
/// defaults to English; the 2-arg form picks the algorithm by ISO code or
/// English name.
///
/// `word_stem_lang` is also kept as an alias for backward compatibility with
/// existing SQE callers that adopted the 2-arg name.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WordStem;

impl ScalarUDFImpl for WordStem {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "word_stem"
    }
    fn signature(&self) -> &Signature {
        // Either 1 arg (string) or 2 args (string, lang).
        static SIG: LazyLock<Signature> = LazyLock::new(|| Signature::one_of(
            vec![
                TypeSignature::Any(1),
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ));
        &SIG
    }
    fn aliases(&self) -> &[String] {
        // Some SQE deployments and tests still call `word_stem_lang(s, lang)`
        // explicitly. Keep that name working as a registered alias.
        static ALIASES: LazyLock<Vec<String>> =
            LazyLock::new(|| vec!["word_stem_lang".to_string()]);
        &ALIASES
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use rust_stemmers::{Algorithm, Stemmer};
        match args.args.len() {
            1 => {
                let stemmer = Stemmer::create(Algorithm::English);
                str_transform(&args.args[0], |s| Some(stemmer.stem(s).into_owned()))
            }
            2 => str_transform_2(&args.args, |s, lang| {
                let algo = match lang.to_lowercase().as_str() {
                    "en" | "english" => Algorithm::English,
                    "fr" | "french" => Algorithm::French,
                    "de" | "german" => Algorithm::German,
                    "es" | "spanish" => Algorithm::Spanish,
                    "it" | "italian" => Algorithm::Italian,
                    "pt" | "portuguese" => Algorithm::Portuguese,
                    "nl" | "dutch" => Algorithm::Dutch,
                    "sv" | "swedish" => Algorithm::Swedish,
                    "no" | "norwegian" => Algorithm::Norwegian,
                    "da" | "danish" => Algorithm::Danish,
                    "fi" | "finnish" => Algorithm::Finnish,
                    "ro" | "romanian" => Algorithm::Romanian,
                    "hu" | "hungarian" => Algorithm::Hungarian,
                    "tr" | "turkish" => Algorithm::Turkish,
                    "ru" | "russian" => Algorithm::Russian,
                    "ar" | "arabic" => Algorithm::Arabic,
                    _ => Algorithm::English,
                };
                let stemmer = Stemmer::create(algo);
                Some(stemmer.stem(s).into_owned())
            }),
            n => Err(DataFusionError::Plan(format!(
                "word_stem: expected 1 or 2 arguments, got {n}"
            ))),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// AGGREGATE-LIKE SCALAR ALIASES
// ═══════════════════════════════════════════════════════════════════

/// arbitrary(x) — Returns the first non-null value (scalar passthrough; aggregate form is first_value).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Arbitrary;

impl ScalarUDFImpl for Arbitrary {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "arbitrary"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(1), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // For scalar: pass through. For array: return first non-null.
        match &args.args[0] {
            ColumnarValue::Array(arr) => {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(arr, i)?));
                    }
                }
                Ok(ColumnarValue::Scalar(ScalarValue::try_from(arr.data_type())?))
            }
            other => Ok(other.clone()),
        }
    }
}

/// max_by(x, y) — Returns x for the row where y is maximum (scalar stub; returns first arg).
///
/// NOTE: Full aggregate semantics require an aggregate UDF with ORDER BY. This scalar stub
/// handles single-row contexts and prevents parse errors in Trino-compat mode.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MaxBy;

impl ScalarUDFImpl for MaxBy {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "max_by"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(2), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Scalar stub: return first arg (for single-row contexts)
        Ok(args.args[0].clone())
    }
}

/// min_by(x, y) — Returns x for the row where y is minimum (scalar stub; returns first arg).
///
/// NOTE: Full aggregate semantics require an aggregate UDF with ORDER BY. This scalar stub
/// handles single-row contexts and prevents parse errors in Trino-compat mode.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MinBy;

impl ScalarUDFImpl for MinBy {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "min_by"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(2), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

/// checksum(x) — Order-insensitive hash aggregate (XOR of hashes). Scalar impl for single values.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Checksum;

impl ScalarUDFImpl for Checksum {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "checksum"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(1), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use sha2::{Digest, Sha256};
        match &args.args[0] {
            ColumnarValue::Scalar(v) => {
                let mut hasher = Sha256::new();
                hasher.update(format!("{:?}", v).as_bytes());
                let result = hasher.finalize();
                let hex = format!("{:x}", result).chars().take(16).collect::<String>();
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(hex))))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arrow::compute::cast(arr, &DataType::Utf8)?;
                let str_arr = str_arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| {
                            let mut hasher = Sha256::new();
                            hasher.update(format!("{:?}", s).as_bytes());
                            let digest = hasher.finalize();
                            format!("{:x}", digest).chars().take(16).collect::<String>()
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// DATE/TIME — TIMEZONE HELPERS
// ═══════════════════════════════════════════════════════════════════

/// timezone_hour(ts) — Returns the hour component of the timezone offset.
///
/// SQE operates in UTC, so the offset is always 0.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TimezoneHour;

impl ScalarUDFImpl for TimezoneHour {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "timezone_hour"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(1), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // SQE operates in UTC, so timezone offset is always 0
        match &args.args[0] {
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(0)))),
            ColumnarValue::Array(arr) => {
                let result: Int64Array = (0..arr.len()).map(|_| Some(0i64)).collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

/// timezone_minute(ts) — Returns the minute component of the timezone offset.
///
/// SQE operates in UTC, so the offset is always 0.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TimezoneMinute;

impl ScalarUDFImpl for TimezoneMinute {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "timezone_minute"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(1), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(0)))),
            ColumnarValue::Array(arr) => {
                let result: Int64Array = (0..arr.len()).map(|_| Some(0i64)).collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// JSON HELPERS
// ═══════════════════════════════════════════════════════════════════

/// json_size(json, path) — Returns the size of a JSON object or array at the given path.
///
/// For objects: number of keys. For arrays: number of elements. For strings: string length.
/// For scalars (numbers, booleans): 0.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonSize;

impl ScalarUDFImpl for JsonSize {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_size"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(2), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(json))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(path))),
            ) => {
                let size = json_size_at_path(json, path);
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(size)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
        }
    }
}

fn json_size_at_path(json: &str, path: &str) -> Option<i64> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let key = path.trim_start_matches("$.");
    let target = if key.is_empty() || key == "$" {
        &v
    } else {
        crate::trino_functions::navigate_json(&v, key)?
    };
    match target {
        serde_json::Value::Object(m) => Some(m.len() as i64),
        serde_json::Value::Array(a) => Some(a.len() as i64),
        serde_json::Value::String(s) => Some(s.len() as i64),
        _ => Some(0),
    }
}

/// json_array_get(json, index) — Returns the element at the given index in a JSON array.
///
/// Supports negative indices (counting from the end). Returns NULL if out of range.
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayGet;

impl ScalarUDFImpl for JsonArrayGet {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_array_get"
    }
    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(2), Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match (&args.args[0], &args.args[1]) {
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(json))),
                ColumnarValue::Scalar(idx_val),
            ) => {
                let idx = match idx_val {
                    ScalarValue::Int64(Some(i)) => *i,
                    ScalarValue::Int32(Some(i)) => *i as i64,
                    _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };
                let result = json_array_get_impl(json, idx);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        }
    }
}

fn json_array_get_impl(json: &str, idx: i64) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let arr = v.as_array()?;
    let actual_idx = if idx < 0 {
        let len = arr.len() as i64;
        let pos = len + idx;
        if pos < 0 {
            return None;
        }
        pos as usize
    } else {
        idx as usize
    };
    arr.get(actual_idx).map(|v| v.to_string())
}

// ═══════════════════════════════════════════════════════════════════
// TRY(expr) — Trino error-suppressing wrapper
// ═══════════════════════════════════════════════════════════════════

/// try(expr) — Trino error-suppressing wrapper.
///
/// In SQE this is a passthrough UDF: DataFusion evaluates arguments before
/// calling UDFs, so by the time try() runs, the argument already succeeded.
/// This implementation ensures queries using TRY() are recognised rather than
/// failing with "unknown function". For type-conversion errors, use TRY_CAST.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Try;

impl ScalarUDFImpl for Try {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try"
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Volatile));
        &SIG
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // TRY wraps any expression — return the same type as the argument.
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Passthrough: the argument was already successfully evaluated.
        Ok(args.args[0].clone())
    }
}

// ═══════════════════════════════════════════════════════════════════
// format() — printf-style string formatting
// ═══════════════════════════════════════════════════════════════════

/// format(fmt, ...) — Trino printf-style string formatting.
///
/// Supports: `%s` (string), `%d` (integer), `%f` (float), `%03d` (zero-padded
/// integer), `%.2f` (float with precision), `%-7s` (left-aligned string), `%%`
/// (literal percent). Covers the vast majority of real-world `format()` usage.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Format;

impl ScalarUDFImpl for Format {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "format"
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> = LazyLock::new(|| {
            Signature::new(TypeSignature::VariadicAny, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.is_empty() {
            return Err(DataFusionError::Execution(
                "format() requires at least 1 argument".into(),
            ));
        }

        let fmt_str = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            _ => {
                return Err(DataFusionError::Execution(
                    "format() first argument must be a string literal".into(),
                ))
            }
        };

        // Collect remaining args as strings for substitution
        let arg_strs: Vec<String> = args.args[1..]
            .iter()
            .map(|a| match a {
                ColumnarValue::Scalar(v) => format_scalar_value(v),
                ColumnarValue::Array(_) => "?".to_string(),
            })
            .collect();

        let result = apply_format(&fmt_str, &arg_strs)?;
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
    }
}

/// Convert a `ScalarValue` to its string representation for format() substitution.
fn format_scalar_value(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
        ScalarValue::Int8(Some(n)) => n.to_string(),
        ScalarValue::Int16(Some(n)) => n.to_string(),
        ScalarValue::Int32(Some(n)) => n.to_string(),
        ScalarValue::Int64(Some(n)) => n.to_string(),
        ScalarValue::UInt8(Some(n)) => n.to_string(),
        ScalarValue::UInt16(Some(n)) => n.to_string(),
        ScalarValue::UInt32(Some(n)) => n.to_string(),
        ScalarValue::UInt64(Some(n)) => n.to_string(),
        ScalarValue::Float32(Some(f)) => f.to_string(),
        ScalarValue::Float64(Some(f)) => f.to_string(),
        ScalarValue::Boolean(Some(b)) => b.to_string(),
        ScalarValue::Null => "NULL".to_string(),
        other => format!("{other}"),
    }
}

/// Apply printf-style format string substitution.
///
/// Supported specifiers: `%s`, `%d`, `%f`, `%%`,
/// `%<width>d`, `%0<width>d`, `%.<prec>f`, `%-<width>s`.
fn apply_format(fmt: &str, args: &[String]) -> DFResult<String> {
    let mut result = String::new();
    let mut chars = fmt.chars().peekable();
    let mut arg_idx = 0;

    while let Some(c) = chars.next() {
        if c != '%' {
            result.push(c);
            continue;
        }

        match chars.peek() {
            None => {
                result.push('%');
            }
            Some('%') => {
                chars.next();
                result.push('%');
            }
            Some('s') => {
                chars.next();
                if arg_idx < args.len() {
                    result.push_str(&args[arg_idx]);
                    arg_idx += 1;
                }
            }
            Some('d') => {
                chars.next();
                if arg_idx < args.len() {
                    let val = args[arg_idx].parse::<i64>().unwrap_or(0);
                    result.push_str(&val.to_string());
                    arg_idx += 1;
                }
            }
            Some('f') => {
                chars.next();
                if arg_idx < args.len() {
                    let val = args[arg_idx].parse::<f64>().unwrap_or(0.0);
                    result.push_str(&format!("{val:.6}"));
                    arg_idx += 1;
                }
            }
            Some(&ch) if ch.is_ascii_digit() || ch == '.' || ch == '-' => {
                // Width/precision specifier: %03d, %.2f, %-7s, %7d, etc.
                let mut spec = String::new();
                while let Some(&next_ch) = chars.peek() {
                    if next_ch.is_ascii_digit() || next_ch == '.' || next_ch == '-' {
                        spec.push(next_ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let type_char = chars.next().unwrap_or('s');
                if arg_idx < args.len() {
                    match type_char {
                        'd' => {
                            let val = args[arg_idx].parse::<i64>().unwrap_or(0);
                            if let Some(rest) = spec.strip_prefix('0') {
                                // Zero-padding: %03d → 008
                                let width: usize = rest.parse().unwrap_or(0);
                                result.push_str(&format!("{val:0>width$}"));
                            } else if let Some(rest) = spec.strip_prefix('-') {
                                let width: usize = rest.parse().unwrap_or(0);
                                result.push_str(&format!("{val:<width$}"));
                            } else {
                                let width: usize = spec.parse().unwrap_or(0);
                                result.push_str(&format!("{val:>width$}"));
                            }
                        }
                        'f' => {
                            let val = args[arg_idx].parse::<f64>().unwrap_or(0.0);
                            if let Some(rest) = spec.strip_prefix('.') {
                                let prec: usize = rest.parse().unwrap_or(6);
                                result.push_str(&format!("{val:.prec$}"));
                            } else {
                                result.push_str(&format!("{val}"));
                            }
                        }
                        's' => {
                            if let Some(rest) = spec.strip_prefix('-') {
                                let width: usize = rest.parse().unwrap_or(0);
                                result.push_str(&format!("{:<width$}", args[arg_idx]));
                            } else {
                                let width: usize = spec.parse().unwrap_or(0);
                                result.push_str(&format!("{:>width$}", args[arg_idx]));
                            }
                        }
                        _ => {
                            result.push_str(&args[arg_idx]);
                        }
                    }
                    arg_idx += 1;
                }
            }
            _ => {
                result.push('%');
            }
        }
    }

    Ok(result)
}

// ═══════════════════════════════════════════════════════════════════
// to_json() — convert scalar value to JSON string
// ═══════════════════════════════════════════════════════════════════

/// to_json(x) — converts a scalar value to its JSON string representation.
///
/// Strings are JSON-encoded (i.e. wrapped in double quotes and escaped).
/// Numbers and booleans are rendered as JSON primitives.
/// NULL → `"null"`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToJson;

impl ScalarUDFImpl for ToJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(1), Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Scalar(v) => {
                let json = scalar_to_json_string(v);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json))))
            }
            ColumnarValue::Array(arr) => {
                let result: StringArray = (0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            Some("null".to_string())
                        } else {
                            ScalarValue::try_from_array(arr.as_ref(), i)
                                .ok()
                                .map(|sv| scalar_to_json_string(&sv))
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

/// Convert a `ScalarValue` to its JSON string representation.
fn scalar_to_json_string(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            serde_json::to_string(s.as_str()).unwrap_or_else(|_| format!("\"{s}\""))
        }
        ScalarValue::Int8(Some(n)) => n.to_string(),
        ScalarValue::Int16(Some(n)) => n.to_string(),
        ScalarValue::Int32(Some(n)) => n.to_string(),
        ScalarValue::Int64(Some(n)) => n.to_string(),
        ScalarValue::UInt8(Some(n)) => n.to_string(),
        ScalarValue::UInt16(Some(n)) => n.to_string(),
        ScalarValue::UInt32(Some(n)) => n.to_string(),
        ScalarValue::UInt64(Some(n)) => n.to_string(),
        ScalarValue::Float32(Some(f)) => f.to_string(),
        ScalarValue::Float64(Some(f)) => f.to_string(),
        ScalarValue::Boolean(Some(b)) => b.to_string(),
        ScalarValue::Null => "null".to_string(),
        other => format!("\"{other}\""),
    }
}
