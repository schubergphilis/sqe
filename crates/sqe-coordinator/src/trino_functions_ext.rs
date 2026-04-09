//! Extended Trino-compatible functions for DataFusion.
//!
//! This module contains additional Trino SQL functions beyond the core set
//! in `trino_functions.rs`. Split into a separate file for maintainability.

use std::any::Any;
use std::sync::{Arc, LazyLock};

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
    ctx.register_udf(ScalarUDF::from(WordStem));
    ctx.register_udf(ScalarUDF::from(WordStemLang));
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
        if c_code.is_some() && c_code != last_code {
            result.push(c_code.unwrap());
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

/// regexp_extract_all(s, pattern) → all matches as a JSON array string
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
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        str_transform_2(&args.args, |s, pattern| {
            let re = regex::Regex::new(pattern).ok()?;
            let matches: Vec<&str> = re.find_iter(s).map(|m| m.as_str()).collect();
            Some(serde_json::to_string(&matches).unwrap_or_default())
        })
    }
}

/// regexp_split(s, pattern) → parts as a JSON array string
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
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        str_transform_2(&args.args, |s, pattern| {
            let re = regex::Regex::new(pattern).ok()?;
            let parts: Vec<&str> = re.split(s).collect();
            Some(serde_json::to_string(&parts).unwrap_or_default())
        })
    }
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

/// word_stem(s) → stemmed word (English by default)
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
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use rust_stemmers::{Algorithm, Stemmer};
        let stemmer = Stemmer::create(Algorithm::English);
        str_transform(&args.args[0], |s| Some(stemmer.stem(s).into_owned()))
    }
}

/// word_stem_lang(s, lang) → stemmed word in specified language
///
/// NOTE: Trino's `word_stem(s, lang)` overload is registered here as `word_stem_lang`
/// because DataFusion doesn't support multiple UDF arities for the same name. Users
/// should call `word_stem_lang(word, 'en')` from SQE when needing language selection.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WordStemLang;

impl ScalarUDFImpl for WordStemLang {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "word_stem_lang"
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
        use rust_stemmers::{Algorithm, Stemmer};
        str_transform_2(&args.args, |s, lang| {
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
        })
    }
}
