//! Trino-compatible function aliases for DataFusion.
//!
//! DataFusion uses `extract(YEAR FROM d)` / `date_part('year', d)` while
//! Trino provides standalone functions like `year(d)`, `month(d)`, etc.
//! These UDFs bridge the gap so Trino SQL and dbt models work unmodified.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use arrow::temporal_conversions;
use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, Timelike};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

/// Register all Trino-compatible function aliases on a SessionContext.
pub fn register_trino_functions(ctx: &datafusion::prelude::SessionContext) {
    // Date/time extract functions
    ctx.register_udf(ScalarUDF::from(ExtractYear));
    ctx.register_udf(ScalarUDF::from(ExtractMonth));
    ctx.register_udf(ScalarUDF::from(ExtractDay));
    ctx.register_udf(ScalarUDF::from(ExtractHour));
    ctx.register_udf(ScalarUDF::from(ExtractMinute));
    ctx.register_udf(ScalarUDF::from(ExtractSecond));
    ctx.register_udf(ScalarUDF::from(DayOfWeek));
    ctx.register_udf(ScalarUDF::from(DayOfYear));
    ctx.register_udf(ScalarUDF::from(Quarter));
    ctx.register_udf(ScalarUDF::from(Week));

    // Date arithmetic
    ctx.register_udf(ScalarUDF::from(DateAdd));
    ctx.register_udf(ScalarUDF::from(DateDiff));
    ctx.register_udf(ScalarUDF::from(FromUnixtime));
    ctx.register_udf(ScalarUDF::from(ToUnixtime));
    ctx.register_udf(ScalarUDF::from(TrinoDate));

    // Conditional / type functions
    ctx.register_udf(ScalarUDF::from(TrinoIf));
    ctx.register_udf(ScalarUDF::from(TypeOf));

    // Date formatting / parsing (Trino compat)
    ctx.register_udf(ScalarUDF::from(DateFormat));
    ctx.register_udf(ScalarUDF::from(DateParse));

    // now() → current timestamp
    ctx.register_udf(ScalarUDF::from(TrinoNow));

    // JSON functions
    ctx.register_udf(ScalarUDF::from(JsonObject));
    ctx.register_udf(ScalarUDF::from(JsonFormat));

    // String functions: strpos(string, substring) → integer position
    ctx.register_udf(ScalarUDF::from(Strpos));

    // Trino time aliases — these are registered as lightweight UDFs that
    // delegate to DataFusion built-ins already available.
    ctx.register_udf(ScalarUDF::from(LocalTime));
    ctx.register_udf(ScalarUDF::from(LocalTimestamp));

    // URL extraction functions
    ctx.register_udf(ScalarUDF::from(UrlExtractHost));
    ctx.register_udf(ScalarUDF::from(UrlExtractPath));
    ctx.register_udf(ScalarUDF::from(UrlExtractPort));
    ctx.register_udf(ScalarUDF::from(UrlExtractProtocol));
    ctx.register_udf(ScalarUDF::from(UrlExtractQuery));
    ctx.register_udf(ScalarUDF::from(UrlExtractParameter));
    ctx.register_udf(ScalarUDF::from(UrlEncode));
    ctx.register_udf(ScalarUDF::from(UrlDecode));

    // Encoding functions
    ctx.register_udf(ScalarUDF::from(FromBase64));
    ctx.register_udf(ScalarUDF::from(ToBase64));
    ctx.register_udf(ScalarUDF::from(FromHex));
    ctx.register_udf(ScalarUDF::from(ToHex));
    ctx.register_udf(ScalarUDF::from(FromUtf8));
    ctx.register_udf(ScalarUDF::from(ToUtf8));

    // Trino JSON aliases — map Trino names to lightweight serde_json-based impls
    ctx.register_udf(ScalarUDF::from(JsonExtract));
    ctx.register_udf(ScalarUDF::from(JsonExtractScalar));
    ctx.register_udf(ScalarUDF::from(JsonArrayLength));
    ctx.register_udf(ScalarUDF::from(JsonParse));
}

/// Extract a chrono component from a Date32 or Timestamp array.
fn extract_component(
    arg: &ColumnarValue,
    f_date: fn(NaiveDate) -> f64,
    f_ts: fn(i64) -> f64,
    f_ts_ns: fn(i64) -> f64,
) -> DFResult<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => {
            let result: Float64Array = if let Some(date_arr) = array.as_any().downcast_ref::<Date32Array>() {
                date_arr.iter().map(|opt| opt.map(|days| {
                    let date = temporal_conversions::date32_to_datetime(days).unwrap().date();
                    f_date(date)
                })).collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                ts_arr.iter().map(|opt| opt.map(f_ts)).collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                ts_arr.iter().map(|opt| opt.map(f_ts_ns)).collect()
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Expected Date32 or Timestamp, got {:?}", array.data_type()
                )));
            };
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        ColumnarValue::Scalar(scalar) => {
            use datafusion::common::ScalarValue;
            let val = match scalar {
                ScalarValue::Date32(Some(days)) => {
                    let date = temporal_conversions::date32_to_datetime(*days).unwrap().date();
                    f_date(date)
                }
                ScalarValue::TimestampMicrosecond(Some(us), _) => f_ts(*us),
                ScalarValue::TimestampNanosecond(Some(ns), _) => f_ts_ns(*ns),
                _ => return Err(DataFusionError::Internal(format!(
                    "Expected date or timestamp scalar, got {scalar:?}"
                ))),
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(val))))
        }
    }
}

fn us_to_naive(us: i64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_micros(us)
        .unwrap_or_default()
        .naive_utc()
}

fn ns_to_naive(ns: i64) -> chrono::NaiveDateTime {
    chrono::DateTime::from_timestamp_nanos(ns).naive_utc()
}

/// Macro to define a Trino date-extract function using direct chrono extraction.
macro_rules! trino_extract_fn {
    ($struct_name:ident, $fn_name:expr, $f_date:expr, $f_us:expr, $f_ns:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $struct_name;

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn std::any::Any { self }

            fn name(&self) -> &str { $fn_name }

            fn signature(&self) -> &Signature {
                static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
                    Signature::any(1, Volatility::Immutable)
                });
                &SIG
            }

            fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Float64)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                extract_component(&args.args[0], $f_date, $f_us, $f_ns)
            }
        }
    };
}

trino_extract_fn!(ExtractYear, "year",
    |d: NaiveDate| d.year() as f64,
    |us| us_to_naive(us).year() as f64,
    |ns| ns_to_naive(ns).year() as f64
);
trino_extract_fn!(ExtractMonth, "month",
    |d: NaiveDate| d.month() as f64,
    |us| us_to_naive(us).month() as f64,
    |ns| ns_to_naive(ns).month() as f64
);
trino_extract_fn!(ExtractDay, "day",
    |d: NaiveDate| d.day() as f64,
    |us| us_to_naive(us).day() as f64,
    |ns| ns_to_naive(ns).day() as f64
);
trino_extract_fn!(ExtractHour, "hour",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).hour() as f64,
    |ns| ns_to_naive(ns).hour() as f64
);
trino_extract_fn!(ExtractMinute, "minute",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).minute() as f64,
    |ns| ns_to_naive(ns).minute() as f64
);
trino_extract_fn!(ExtractSecond, "second",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).second() as f64,
    |ns| ns_to_naive(ns).second() as f64
);
trino_extract_fn!(DayOfWeek, "day_of_week",
    |d: NaiveDate| d.weekday().num_days_from_sunday() as f64,
    |us| us_to_naive(us).weekday().num_days_from_sunday() as f64,
    |ns| ns_to_naive(ns).weekday().num_days_from_sunday() as f64
);
trino_extract_fn!(DayOfYear, "day_of_year",
    |d: NaiveDate| d.ordinal() as f64,
    |us| us_to_naive(us).ordinal() as f64,
    |ns| ns_to_naive(ns).ordinal() as f64
);
trino_extract_fn!(Quarter, "quarter",
    |d: NaiveDate| ((d.month() - 1) / 3 + 1) as f64,
    |us| { let m = us_to_naive(us).month(); ((m - 1) / 3 + 1) as f64 },
    |ns| { let m = ns_to_naive(ns).month(); ((m - 1) / 3 + 1) as f64 }
);
trino_extract_fn!(Week, "week",
    |d: NaiveDate| d.iso_week().week() as f64,
    |us| us_to_naive(us).iso_week().week() as f64,
    |ns| ns_to_naive(ns).iso_week().week() as f64
);

// ─── Helper: parse time-unit string ─────────────────────────────────────────

fn parse_unit(unit: &str) -> DFResult<&'static str> {
    match unit.to_lowercase().as_str() {
        "year" | "years" => Ok("year"),
        "month" | "months" => Ok("month"),
        "day" | "days" => Ok("day"),
        "hour" | "hours" => Ok("hour"),
        "minute" | "minutes" => Ok("minute"),
        "second" | "seconds" => Ok("second"),
        other => Err(DataFusionError::Internal(format!(
            "Unsupported date unit: {other}"
        ))),
    }
}

/// Add `amount` of `unit` to a NaiveDate, returning the new NaiveDate.
fn date_add_date(d: NaiveDate, unit: &str, amount: i64) -> DFResult<NaiveDate> {
    let result = match unit {
        "year" => {
            let months = amount * 12;
            if months >= 0 {
                d.checked_add_months(Months::new(months as u32))
            } else {
                d.checked_sub_months(Months::new((-months) as u32))
            }
        }
        "month" => {
            if amount >= 0 {
                d.checked_add_months(Months::new(amount as u32))
            } else {
                d.checked_sub_months(Months::new((-amount) as u32))
            }
        }
        "day" => d.checked_add_signed(Duration::days(amount)),
        "hour" => d.checked_add_signed(Duration::hours(amount)),
        "minute" => d.checked_add_signed(Duration::minutes(amount)),
        "second" => d.checked_add_signed(Duration::seconds(amount)),
        _ => unreachable!(),
    };
    result.ok_or_else(|| DataFusionError::Internal("date_add overflow".to_string()))
}

/// Add `amount` of `unit` to a microsecond timestamp, returning updated micros.
fn ts_add_us(us: i64, unit: &str, amount: i64) -> DFResult<i64> {
    let dt = chrono::DateTime::from_timestamp_micros(us)
        .unwrap_or_default()
        .naive_utc();
    let result = match unit {
        "year" => {
            let months = amount * 12;
            let date = if months >= 0 {
                dt.date().checked_add_months(Months::new(months as u32))
            } else {
                dt.date().checked_sub_months(Months::new((-months) as u32))
            };
            date.map(|d| d.and_time(dt.time()))
        }
        "month" => {
            let date = if amount >= 0 {
                dt.date().checked_add_months(Months::new(amount as u32))
            } else {
                dt.date().checked_sub_months(Months::new((-amount) as u32))
            };
            date.map(|d| d.and_time(dt.time()))
        }
        "day" => dt.checked_add_signed(Duration::days(amount)),
        "hour" => dt.checked_add_signed(Duration::hours(amount)),
        "minute" => dt.checked_add_signed(Duration::minutes(amount)),
        "second" => dt.checked_add_signed(Duration::seconds(amount)),
        _ => unreachable!(),
    };
    let new_dt = result.ok_or_else(|| DataFusionError::Internal("date_add overflow".to_string()))?;
    new_dt
        .and_utc()
        .timestamp_micros()
        .checked_mul(1) // identity — just to keep the type
        .ok_or_else(|| DataFusionError::Internal("ts overflow".to_string()))
}

// ─── date_add(unit, value, date_or_ts) ──────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct DateAdd;

impl ScalarUDFImpl for DateAdd {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "date_add"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        // Return same type as the third argument (date or timestamp).
        match args.get(2) {
            Some(t) => Ok(t.clone()),
            None => Ok(DataType::Date32),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        // Extract unit string from first arg (must be scalar Utf8).
        let unit_str = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => parse_unit(s)?,
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => parse_unit(s)?,
            other => {
                return Err(DataFusionError::Internal(format!(
                    "date_add: first arg must be a string literal, got {other:?}"
                )))
            }
        };

        // Extract numeric amount from second arg (scalar Int64/Int32/Float64).
        let amount: i64 = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v as i64,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v as i64,
            ColumnarValue::Scalar(ScalarValue::Int8(Some(v))) => *v as i64,
            ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => *v as i64,
            ColumnarValue::Scalar(ScalarValue::UInt64(Some(v))) => *v as i64,
            ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => *v as i64,
            other => {
                return Err(DataFusionError::Internal(format!(
                    "date_add: second arg must be an integer scalar, got {other:?}"
                )))
            }
        };

        match &args.args[2] {
            ColumnarValue::Array(array) => {
                if let Some(date_arr) = array.as_any().downcast_ref::<Date32Array>() {
                    let result: Date32Array = date_arr
                        .iter()
                        .map(|opt| {
                            opt.map(|days| {
                                let d = temporal_conversions::date32_to_datetime(days)
                                    .unwrap()
                                    .date();
                                let new_d = date_add_date(d, unit_str, amount).unwrap();
                                // days since epoch
                                new_d
                                    .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                                    .num_days() as i32
                            })
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    let result: TimestampMicrosecondArray = ts_arr
                        .iter()
                        .map(|opt| opt.map(|us| ts_add_us(us, unit_str, amount).unwrap()))
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                } else {
                    Err(DataFusionError::Internal(format!(
                        "date_add: unsupported array type {:?}",
                        array.data_type()
                    )))
                }
            }
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Date32(Some(days)) => {
                    let d = temporal_conversions::date32_to_datetime(*days).unwrap().date();
                    let new_d = date_add_date(d, unit_str, amount)?;
                    let new_days = new_d
                        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32;
                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(new_days))))
                }
                ScalarValue::Date32(None) => Ok(ColumnarValue::Scalar(ScalarValue::Date32(None))),
                ScalarValue::TimestampMicrosecond(Some(us), tz) => {
                    let new_us = ts_add_us(*us, unit_str, amount)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        Some(new_us),
                        tz.clone(),
                    )))
                }
                ScalarValue::TimestampMicrosecond(None, tz) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        None,
                        tz.clone(),
                    )))
                }
                other => Err(DataFusionError::Internal(format!(
                    "date_add: unsupported scalar type {other:?}"
                ))),
            },
        }
    }
}

// ─── date_diff(unit, ts1, ts2) ───────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct DateDiff;

impl ScalarUDFImpl for DateDiff {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "date_diff"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        let unit_str = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => parse_unit(s)?,
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => parse_unit(s)?,
            other => {
                return Err(DataFusionError::Internal(format!(
                    "date_diff: first arg must be a string literal, got {other:?}"
                )))
            }
        };

        fn scalar_to_naive_date(sv: &ScalarValue) -> DFResult<Option<NaiveDate>> {
            match sv {
                ScalarValue::Date32(Some(days)) => Ok(Some(
                    temporal_conversions::date32_to_datetime(*days)
                        .unwrap()
                        .date(),
                )),
                ScalarValue::Date32(None) => Ok(None),
                ScalarValue::TimestampMicrosecond(Some(us), _) => {
                    Ok(Some(us_to_naive(*us).date()))
                }
                ScalarValue::TimestampMicrosecond(None, _) => Ok(None),
                other => Err(DataFusionError::Internal(format!(
                    "date_diff: unsupported scalar type {other:?}"
                ))),
            }
        }

        fn compute_diff(unit: &str, d1: NaiveDate, d2: NaiveDate) -> i64 {
            match unit {
                "year" => (d2.year() - d1.year()) as i64,
                "month" => {
                    (d2.year() - d1.year()) as i64 * 12 + (d2.month() as i64 - d1.month() as i64)
                }
                "day" => d2.signed_duration_since(d1).num_days(),
                "hour" => d2.signed_duration_since(d1).num_hours(),
                "minute" => d2.signed_duration_since(d1).num_minutes(),
                "second" => d2.signed_duration_since(d1).num_seconds(),
                _ => unreachable!(),
            }
        }

        // Both args must be scalars for now (the most common case in Trino SQL).
        // Array support could be added later.
        match (&args.args[1], &args.args[2]) {
            (ColumnarValue::Scalar(sv1), ColumnarValue::Scalar(sv2)) => {
                let d1 = scalar_to_naive_date(sv1)?;
                let d2 = scalar_to_naive_date(sv2)?;
                match (d1, d2) {
                    (Some(a), Some(b)) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                        compute_diff(unit_str, a, b),
                    )))),
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
                }
            }
            (ColumnarValue::Array(arr1), ColumnarValue::Array(arr2)) => {
                let get_date = |arr: &ArrayRef, i: usize| -> Option<NaiveDate> {
                    if arr.is_null(i) {
                        return None;
                    }
                    arr.as_any()
                        .downcast_ref::<Date32Array>()
                        .map(|a| temporal_conversions::date32_to_datetime(a.value(i)).unwrap().date())
                        .or_else(|| {
                            arr.as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .map(|a| us_to_naive(a.value(i)).date())
                        })
                };
                let len = arr1.len();
                let result: Int64Array = (0..len)
                    .map(|i| {
                        let d1 = get_date(arr1, i)?;
                        let d2 = get_date(arr2, i)?;
                        Some(compute_diff(unit_str, d1, d2))
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            _ => Err(DataFusionError::Internal(
                "date_diff: mixed scalar/array args not supported".to_string(),
            )),
        }
    }
}

// ─── from_unixtime(epoch_seconds) ───────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct FromUnixtime;

impl ScalarUDFImpl for FromUnixtime {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "from_unixtime"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        fn epoch_to_us(v: f64) -> i64 {
            (v * 1_000_000.0) as i64
        }

        match &args.args[0] {
            ColumnarValue::Scalar(sv) => {
                let us = match sv {
                    ScalarValue::Int64(Some(v)) => epoch_to_us(*v as f64),
                    ScalarValue::Float64(Some(v)) => epoch_to_us(*v),
                    ScalarValue::Float32(Some(v)) => epoch_to_us(*v as f64),
                    ScalarValue::Int32(Some(v)) => epoch_to_us(*v as f64),
                    _ => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                            None, None,
                        )))
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(us),
                    None,
                )))
            }
            ColumnarValue::Array(array) => {
                let result: TimestampMicrosecondArray = if let Some(arr) =
                    array.as_any().downcast_ref::<arrow::array::Int64Array>()
                {
                    arr.iter()
                        .map(|opt| opt.map(|v| epoch_to_us(v as f64)))
                        .collect()
                } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    arr.iter().map(|opt| opt.map(epoch_to_us)).collect()
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "from_unixtime: unsupported array type {:?}",
                        array.data_type()
                    )));
                };
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

// ─── to_unixtime(timestamp) → Float64 ───────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToUnixtime;

impl ScalarUDFImpl for ToUnixtime {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "to_unixtime"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        match &args.args[0] {
            ColumnarValue::Scalar(sv) => {
                let secs = match sv {
                    ScalarValue::TimestampMicrosecond(Some(us), _) => Some(*us as f64 / 1_000_000.0),
                    ScalarValue::TimestampNanosecond(Some(ns), _) => Some(*ns as f64 / 1_000_000_000.0),
                    ScalarValue::Date32(Some(days)) => {
                        let d = temporal_conversions::date32_to_datetime(*days).unwrap().date();
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        Some(d.signed_duration_since(epoch).num_seconds() as f64)
                    }
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(secs)))
            }
            ColumnarValue::Array(array) => {
                let result: Float64Array = if let Some(arr) =
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    arr.iter()
                        .map(|opt| opt.map(|us| us as f64 / 1_000_000.0))
                        .collect()
                } else if let Some(arr) =
                    array.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    arr.iter()
                        .map(|opt| opt.map(|ns| ns as f64 / 1_000_000_000.0))
                        .collect()
                } else if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    arr.iter()
                        .map(|opt| {
                            opt.map(|days| {
                                let d = temporal_conversions::date32_to_datetime(days)
                                    .unwrap()
                                    .date();
                                d.signed_duration_since(epoch).num_seconds() as f64
                            })
                        })
                        .collect()
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "to_unixtime: unsupported array type {:?}",
                        array.data_type()
                    )));
                };
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

// ─── date(timestamp_or_date) → Date32 ───────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoDate;

impl ScalarUDFImpl for TrinoDate {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "date"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        fn us_to_days(us: i64) -> i32 {
            let d = chrono::DateTime::from_timestamp_micros(us)
                .unwrap_or_default()
                .naive_utc()
                .date();
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            d.signed_duration_since(epoch).num_days() as i32
        }

        fn ns_to_days(ns: i64) -> i32 {
            let d = chrono::DateTime::from_timestamp_nanos(ns).naive_utc().date();
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            d.signed_duration_since(epoch).num_days() as i32
        }

        match &args.args[0] {
            ColumnarValue::Scalar(sv) => {
                let days = match sv {
                    ScalarValue::Date32(v) => return Ok(ColumnarValue::Scalar(ScalarValue::Date32(*v))),
                    ScalarValue::TimestampMicrosecond(Some(us), _) => Some(us_to_days(*us)),
                    ScalarValue::TimestampNanosecond(Some(ns), _) => Some(ns_to_days(*ns)),
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(days)))
            }
            ColumnarValue::Array(array) => {
                let result: Date32Array = if let Some(arr) =
                    array.as_any().downcast_ref::<Date32Array>()
                {
                    // Clone as-is
                    arr.iter().collect()
                } else if let Some(arr) =
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    arr.iter().map(|opt| opt.map(us_to_days)).collect()
                } else if let Some(arr) =
                    array.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    arr.iter().map(|opt| opt.map(ns_to_days)).collect()
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "date(): unsupported type {:?}",
                        array.data_type()
                    )));
                };
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

// ─── if(condition, then, else) ───────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoIf;

impl ScalarUDFImpl for TrinoIf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "if"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        // Return type matches the then/else args (arg[1]).
        args.get(1)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("if() needs 3 args".to_string()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        let nrows = args.number_rows;
        let condition = &args.args[0];
        let then_val = &args.args[1];
        let else_val = &args.args[2];

        // Expand condition to BooleanArray.
        let mask: BooleanArray = match condition {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("if(): condition must be boolean".to_string())
                })?
                .clone(),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => {
                BooleanArray::from(vec![*b; nrows])
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(None)) => {
                BooleanArray::from(vec![false; nrows])
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "if(): unsupported condition type {other:?}"
                )))
            }
        };

        // Expand then / else to arrays.
        let to_array = |cv: &ColumnarValue, n: usize| -> DFResult<ArrayRef> {
            match cv {
                ColumnarValue::Array(a) => Ok(Arc::clone(a)),
                ColumnarValue::Scalar(sv) => sv.to_array_of_size(n).map_err(|e| {
                    DataFusionError::Internal(format!("if(): scalar expansion failed: {e}"))
                }),
            }
        };

        let then_arr = to_array(then_val, nrows)?;
        let else_arr = to_array(else_val, nrows)?;

        let result = zip(&mask, &then_arr, &else_arr).map_err(|e| {
            DataFusionError::Internal(format!("if(): zip failed: {e}"))
        })?;

        Ok(ColumnarValue::Array(result))
    }
}

// ─── typeof(expr) → Utf8 ────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TypeOf;

impl ScalarUDFImpl for TypeOf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "typeof"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Stable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        let type_name = match &args.args[0] {
            ColumnarValue::Array(arr) => format!("{:?}", arr.data_type()),
            ColumnarValue::Scalar(sv) => format!("{:?}", sv.data_type()),
        };

        // Return a scalar string — same value for every row.
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(type_name))))
    }
}

// ─── date_format(timestamp, format_pattern) → Utf8 ─────────────────────────
//
// Trino uses Java/MySQL-style format specifiers that are *almost* identical to
// strftime.  The only divergence we handle is `%i` (minutes) → `%M` (strftime).

/// Convert a Trino / MySQL format pattern to a chrono strftime pattern.
///
/// Trino uses Java/MySQL-style specifiers:
///   `%i` → minutes (chrono `%M`)
///   `%s` → seconds (chrono `%S`)  — chrono `%s` means epoch seconds
fn trino_format_to_chrono(pattern: &str) -> String {
    pattern.replace("%i", "%M").replace("%s", "%S")
}

/// Format a `NaiveDateTime` using a Trino-style format string.
fn format_naive(dt: NaiveDateTime, pattern: &str) -> String {
    let chrono_fmt = trino_format_to_chrono(pattern);
    dt.format(&chrono_fmt).to_string()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DateFormat;

impl ScalarUDFImpl for DateFormat {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "date_format" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::any(2, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        // Second arg is the format pattern (scalar string).
        let pattern = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            other => return Err(DataFusionError::Internal(format!(
                "date_format: second arg must be a format string, got {other:?}"
            ))),
        };

        match &args.args[0] {
            ColumnarValue::Scalar(sv) => {
                let dt = scalar_to_naive_dt(sv)?;
                let result = dt.map(|d| format_naive(d, &pattern));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ColumnarValue::Array(array) => {
                let result: StringArray = if let Some(ts_arr) =
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    ts_arr
                        .iter()
                        .map(|opt| opt.map(|us| format_naive(us_to_naive(us), &pattern)))
                        .collect()
                } else if let Some(ts_arr) =
                    array.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    ts_arr
                        .iter()
                        .map(|opt| opt.map(|ns| format_naive(ns_to_naive(ns), &pattern)))
                        .collect()
                } else if let Some(date_arr) =
                    array.as_any().downcast_ref::<Date32Array>()
                {
                    date_arr
                        .iter()
                        .map(|opt| {
                            opt.map(|days| {
                                let d = temporal_conversions::date32_to_datetime(days)
                                    .unwrap()
                                    .date();
                                format_naive(d.and_hms_opt(0, 0, 0).unwrap(), &pattern)
                            })
                        })
                        .collect()
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "date_format: unsupported array type {:?}",
                        array.data_type()
                    )));
                };
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

/// Helper: extract a `NaiveDateTime` from a scalar date/timestamp value.
fn scalar_to_naive_dt(
    sv: &datafusion::common::ScalarValue,
) -> DFResult<Option<NaiveDateTime>> {
    use datafusion::common::ScalarValue;
    match sv {
        ScalarValue::TimestampMicrosecond(Some(us), _) => Ok(Some(us_to_naive(*us))),
        ScalarValue::TimestampNanosecond(Some(ns), _) => Ok(Some(ns_to_naive(*ns))),
        ScalarValue::Date32(Some(days)) => {
            let d = temporal_conversions::date32_to_datetime(*days)
                .unwrap()
                .date();
            Ok(Some(d.and_hms_opt(0, 0, 0).unwrap()))
        }
        ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::TimestampNanosecond(None, _)
        | ScalarValue::Date32(None) => Ok(None),
        other => Err(DataFusionError::Internal(format!(
            "Expected date/timestamp, got {other:?}"
        ))),
    }
}

// ─── date_parse(string, format_pattern) → Timestamp ─────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct DateParse;

impl ScalarUDFImpl for DateParse {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "date_parse" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::any(2, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        let pattern = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            other => return Err(DataFusionError::Internal(format!(
                "date_parse: second arg must be a format string, got {other:?}"
            ))),
        };
        let chrono_fmt = trino_format_to_chrono(&pattern);

        match &args.args[0] {
            ColumnarValue::Scalar(sv) => {
                let us = match sv {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        // Try full datetime parse first, then date-only
                        let dt = NaiveDateTime::parse_from_str(s, &chrono_fmt)
                            .or_else(|_| {
                                chrono::NaiveDate::parse_from_str(s, &chrono_fmt)
                                    .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                            })
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "date_parse: failed to parse '{s}' with format '{pattern}': {e}"
                                ))
                            })?;
                        Some(dt.and_utc().timestamp_micros())
                    }
                    ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => None,
                    other => return Err(DataFusionError::Internal(format!(
                        "date_parse: first arg must be a string, got {other:?}"
                    ))),
                };
                Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    us, None,
                )))
            }
            ColumnarValue::Array(array) => {
                let str_arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "date_parse: expected string array, got {:?}",
                            array.data_type()
                        ))
                    })?;
                let result: TimestampMicrosecondArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| {
                            let dt = NaiveDateTime::parse_from_str(s, &chrono_fmt)
                                .or_else(|_| {
                                    chrono::NaiveDate::parse_from_str(s, &chrono_fmt)
                                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                                })
                                .unwrap_or_default();
                            dt.and_utc().timestamp_micros()
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

// ─── now() → Timestamp ─────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoNow;

impl ScalarUDFImpl for TrinoNow {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "now" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::new(TypeSignature::Nullary, Volatility::Stable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        let us = chrono::Utc::now().timestamp_micros();
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
            Some(us),
            None,
        )))
    }
}

// ─── json_object(k1, v1, k2, v2, ...) → Utf8 ──────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObject;

impl ScalarUDFImpl for JsonObject {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "json_object" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::variadic_any(Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        if !args.args.len().is_multiple_of(2) {
            return Err(DataFusionError::Internal(
                "json_object: must have an even number of arguments (key-value pairs)".to_string(),
            ));
        }

        // Build JSON from scalar key-value pairs.
        let mut map = serde_json::Map::new();
        for pair in args.args.chunks(2) {
            let key = match &pair[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
                other => return Err(DataFusionError::Internal(format!(
                    "json_object: key must be a string, got {other:?}"
                ))),
            };
            let value = match &pair[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
                    serde_json::Value::String(s.clone())
                }
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
                    serde_json::Value::Number(serde_json::Number::from(*v))
                }
                ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => {
                    serde_json::Value::Number(serde_json::Number::from(*v))
                }
                ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                    serde_json::json!(*v)
                }
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(v))) => {
                    serde_json::Value::Bool(*v)
                }
                ColumnarValue::Scalar(ScalarValue::Null) => serde_json::Value::Null,
                other => return Err(DataFusionError::Internal(format!(
                    "json_object: unsupported value type {other:?}"
                ))),
            };
            map.insert(key, value);
        }

        let json_str = serde_json::Value::Object(map).to_string();
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(json_str))))
    }
}

// ─── json_format(json_value) → Utf8 (identity on varchar) ──────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonFormat;

impl ScalarUDFImpl for JsonFormat {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "json_format" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::any(1, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        // Identity pass-through: SQE stores JSON as Utf8 strings.
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(v.clone())))
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(v.clone())))
            }
            ColumnarValue::Array(array) => {
                // Return the array as-is if it's already a string type.
                if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(ColumnarValue::Array(
                        Arc::new(arr.clone()) as ArrayRef,
                    ))
                } else {
                    Err(DataFusionError::Internal(format!(
                        "json_format: expected string input, got {:?}",
                        array.data_type()
                    )))
                }
            }
            other => Err(DataFusionError::Internal(format!(
                "json_format: unsupported input {other:?}"
            ))),
        }
    }
}

// ─── strpos(string, substring) → Int64 ─────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct Strpos;

impl ScalarUDFImpl for Strpos {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "strpos" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::any(2, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;

        fn find_pos(haystack: &str, needle: &str) -> i64 {
            // Trino strpos returns 1-based position, 0 if not found.
            haystack
                .find(needle)
                .map(|idx| idx as i64 + 1)
                .unwrap_or(0)
        }

        let needle = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            other => return Err(DataFusionError::Internal(format!(
                "strpos: second arg must be a string, got {other:?}"
            ))),
        };

        match &args.args[0] {
            ColumnarValue::Scalar(sv) => {
                let result = match sv {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        Some(find_pos(s, &needle))
                    }
                    ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => None,
                    other => return Err(DataFusionError::Internal(format!(
                        "strpos: first arg must be a string, got {other:?}"
                    ))),
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(result)))
            }
            ColumnarValue::Array(array) => {
                let str_arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "strpos: expected string array, got {:?}",
                            array.data_type()
                        ))
                    })?;
                let result: Int64Array = str_arr
                    .iter()
                    .map(|opt| opt.map(|s| find_pos(s, &needle)))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

// ─── localtime → alias for CURRENT_TIME (returns Time64) ───────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct LocalTime;

impl ScalarUDFImpl for LocalTime {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "localtime" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::new(TypeSignature::Nullary, Volatility::Stable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        let now = chrono::Local::now().naive_local();
        let us = now.and_utc().timestamp_micros();
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
            Some(us),
            None,
        )))
    }
}

// ─── localtimestamp → alias for CURRENT_TIMESTAMP ──────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct LocalTimestamp;

impl ScalarUDFImpl for LocalTimestamp {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { "localtimestamp" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::new(TypeSignature::Nullary, Volatility::Stable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        ))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        let us = chrono::Utc::now().timestamp_micros();
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
            Some(us),
            None,
        )))
    }
}

// ---------------------------------------------------------------------------
// Shared string-transform helpers
// ---------------------------------------------------------------------------

fn string_transform(arg: &ColumnarValue, f: impl Fn(&str) -> String) -> DFResult<ColumnarValue> {
    use datafusion::common::ScalarValue;
    match arg {
        ColumnarValue::Scalar(v) => {
            let s = match v {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.as_str().to_owned(),
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                }
                other => other.to_string(),
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(f(&s)))))
        }
        ColumnarValue::Array(arr) => {
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected string array".into()))?;
            let results: StringArray = str_arr.iter().map(|opt| opt.map(&f)).collect();
            Ok(ColumnarValue::Array(Arc::new(results)))
        }
    }
}

fn string_transform_2(
    args: &[ColumnarValue],
    f: impl Fn(&str, &str) -> Option<String>,
) -> DFResult<ColumnarValue> {
    use datafusion::common::ScalarValue;
    match (&args[0], &args[1]) {
        (ColumnarValue::Scalar(v1), ColumnarValue::Scalar(v2)) => {
            let s1 = match v1 {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };
            let s2 = match v2 {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(f(&s1, &s2))))
        }
        (ColumnarValue::Array(arr1), ColumnarValue::Scalar(v2)) => {
            let str_arr = arr1
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected string array".into()))?;
            let s2 = match v2 {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                _ => {
                    let nulls: StringArray = str_arr.iter().map(|_| None::<&str>).collect();
                    return Ok(ColumnarValue::Array(Arc::new(nulls)));
                }
            };
            let results: StringArray = str_arr.iter().map(|opt| opt.and_then(|s| f(s, &s2))).collect();
            Ok(ColumnarValue::Array(Arc::new(results)))
        }
        _ => Err(DataFusionError::Internal(
            "Unsupported arg combination for 2-arg string transform".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// URL extraction functions
// ---------------------------------------------------------------------------

use url::Url;

fn extract_url_component(
    url_str: &str,
    component: &str,
    param_arg: Option<&ColumnarValue>,
) -> Option<String> {
    use datafusion::common::ScalarValue;
    let parsed = Url::parse(url_str).ok()?;
    match component {
        "host" => parsed.host_str().map(|s| s.to_string()),
        "path" => Some(parsed.path().to_string()),
        "port" => parsed.port().map(|p| p.to_string()),
        "protocol" => Some(parsed.scheme().to_string()),
        "query" => parsed.query().map(|s| s.to_string()),
        "parameter" => {
            let param_cv = param_arg?;
            let param_name = match param_cv {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
                _ => return None,
            };
            parsed
                .query_pairs()
                .find(|(k, _)| k == param_name.as_str())
                .map(|(_, v)| v.to_string())
        }
        _ => None,
    }
}

fn parse_url_component(args: &[ColumnarValue], component: &str) -> DFResult<ColumnarValue> {
    use datafusion::common::ScalarValue;
    match &args[0] {
        ColumnarValue::Scalar(v) => {
            let s = match v {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            };
            let result = extract_url_component(&s, component, args.get(1));
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
        }
        ColumnarValue::Array(arr) => {
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Internal("Expected string array".into()))?;
            let results: StringArray = str_arr
                .iter()
                .map(|opt| opt.and_then(|u| extract_url_component(u, component, args.get(1))))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(results)))
        }
    }
}

macro_rules! url_extract_udf {
    ($name:ident, $func_name:expr, $component:expr, $nargs:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $name;

        impl ScalarUDFImpl for $name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &str {
                $func_name
            }
            fn signature(&self) -> &Signature {
                static SIG: std::sync::LazyLock<Signature> =
                    std::sync::LazyLock::new(|| {
                        Signature::new(
                            TypeSignature::Exact(vec![DataType::Utf8; $nargs]),
                            Volatility::Immutable,
                        )
                    });
                &SIG
            }
            fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Utf8)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                parse_url_component(&args.args, $component)
            }
        }
    };
}

url_extract_udf!(UrlExtractHost, "url_extract_host", "host", 1);
url_extract_udf!(UrlExtractPath, "url_extract_path", "path", 1);
url_extract_udf!(UrlExtractPort, "url_extract_port", "port", 1);
url_extract_udf!(UrlExtractProtocol, "url_extract_protocol", "protocol", 1);
url_extract_udf!(UrlExtractQuery, "url_extract_query", "query", 1);
url_extract_udf!(UrlExtractParameter, "url_extract_parameter", "parameter", 2);

// url_encode / url_decode

fn percent_decode(input: &[u8]) -> String {
    let mut result = Vec::new();
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%' && i + 2 < input.len() {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&input[i + 1..i + 3]).unwrap_or(""),
                16,
            ) {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(input[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).to_string()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UrlEncode;

impl ScalarUDFImpl for UrlEncode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "url_encode"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable)
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        string_transform(&args.args[0], |s| {
            use std::fmt::Write;
            let mut result = String::new();
            for b in s.bytes() {
                if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                    result.push(b as char);
                } else {
                    write!(result, "%{:02X}", b).unwrap();
                }
            }
            result
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UrlDecode;

impl ScalarUDFImpl for UrlDecode {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "url_decode"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable)
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        string_transform(&args.args[0], |s| percent_decode(s.as_bytes()))
    }
}

// ---------------------------------------------------------------------------
// Encoding functions: base64, hex, utf8
// ---------------------------------------------------------------------------

use base64::Engine as _;

macro_rules! encoding_udf {
    ($name:ident, $func_name:expr, $transform:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $name;

        impl ScalarUDFImpl for $name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &str {
                $func_name
            }
            fn signature(&self) -> &Signature {
                static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
                    Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable)
                });
                &SIG
            }
            fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Utf8)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                string_transform(&args.args[0], $transform)
            }
        }
    };
}

encoding_udf!(ToBase64, "to_base64", |s: &str| {
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
});

encoding_udf!(FromBase64, "from_base64", |s: &str| {
    base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        .unwrap_or_default()
});

encoding_udf!(ToHex, "to_hex", |s: &str| {
    s.bytes().map(|b| format!("{:02X}", b)).collect::<String>()
});

encoding_udf!(FromHex, "from_hex", |s: &str| {
    let bytes: Vec<u8> = (0..s.len())
        .step_by(2)
        .filter_map(|i| {
            if i + 2 <= s.len() {
                u8::from_str_radix(&s[i..i + 2], 16).ok()
            } else {
                None
            }
        })
        .collect();
    String::from_utf8_lossy(&bytes).to_string()
});

encoding_udf!(ToUtf8, "to_utf8", |s: &str| {
    // Trino to_utf8 converts VARCHAR → VARBINARY; we return hex-encoded for string compat.
    s.bytes().map(|b| format!("{:02X}", b)).collect::<String>()
});

encoding_udf!(FromUtf8, "from_utf8", |s: &str| {
    // Trino from_utf8 converts VARBINARY → VARCHAR; we accept hex-encoded string.
    let bytes: Vec<u8> = (0..s.len())
        .step_by(2)
        .filter_map(|i| {
            if i + 2 <= s.len() {
                u8::from_str_radix(&s[i..i + 2], 16).ok()
            } else {
                None
            }
        })
        .collect();
    if bytes.is_empty() {
        s.to_string()
    } else {
        String::from_utf8_lossy(&bytes).to_string()
    }
});

// ---------------------------------------------------------------------------
// Trino JSON aliases — thin wrappers backed by serde_json
// ---------------------------------------------------------------------------

fn navigate_json<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path.split('.') {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        if let Some(obj) = current.as_object() {
            current = obj.get(key)?;
        } else if let Some(arr) = current.as_array() {
            let idx: usize = key.parse().ok()?;
            current = arr.get(idx)?;
        } else {
            return None;
        }
    }
    Some(current)
}

fn extract_json_value(json: &str, key: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let result = navigate_json(&v, key)?;
    Some(result.to_string())
}

fn extract_json_scalar(json: &str, key: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let result = navigate_json(&v, key)?;
    match result {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Null => None,
        _ => Some(result.to_string()),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtract;

impl ScalarUDFImpl for JsonExtract {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "json_extract"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            )
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        string_transform_2(&args.args, |json, path| {
            let key = path.trim_start_matches("$.");
            let key = if key == "$" { "" } else { key };
            extract_json_value(json, key)
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtractScalar;

impl ScalarUDFImpl for JsonExtractScalar {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "json_extract_scalar"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            )
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        string_transform_2(&args.args, |json, path| {
            let key = path.trim_start_matches("$.");
            let key = if key == "$" { "" } else { key };
            extract_json_scalar(json, key)
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayLength;

impl ScalarUDFImpl for JsonArrayLength {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "json_array_length"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable)
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        match &args.args[0] {
            ColumnarValue::Scalar(v) => {
                let s = match v {
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                    _ => return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
                };
                let len = serde_json::from_str::<serde_json::Value>(&s)
                    .ok()
                    .and_then(|v| v.as_array().map(|a| a.len() as i64));
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(len)))
            }
            ColumnarValue::Array(arr) => {
                let str_arr = arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal("Expected string array".into()))?;
                let results: Int64Array = str_arr
                    .iter()
                    .map(|opt| {
                        opt.and_then(|s| {
                            serde_json::from_str::<serde_json::Value>(s)
                                .ok()
                                .and_then(|v| v.as_array().map(|a| a.len() as i64))
                        })
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonParse;

impl ScalarUDFImpl for JsonParse {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "json_parse"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable)
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // json_parse validates and normalises the JSON string (compact form).
        string_transform(&args.args[0], |s| {
            serde_json::from_str::<serde_json::Value>(s)
                .map(|v| v.to_string())
                .unwrap_or_else(|_| "null".to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use datafusion::prelude::SessionContext;

    async fn run_query(sql: &str) -> f64 {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn year_returns_correct_value() {
        assert_eq!(run_query("SELECT year(DATE '2026-03-30')").await, 2026.0);
    }

    #[tokio::test]
    async fn month_returns_correct_value() {
        assert_eq!(run_query("SELECT month(DATE '2026-03-30')").await, 3.0);
    }

    #[tokio::test]
    async fn day_returns_correct_value() {
        assert_eq!(run_query("SELECT day(DATE '2026-03-30')").await, 30.0);
    }

    #[tokio::test]
    async fn day_of_week_monday() {
        // 2026-03-30 is Monday. num_days_from_sunday: Sunday=0, Monday=1
        assert_eq!(run_query("SELECT day_of_week(DATE '2026-03-30')").await, 1.0);
    }

    #[tokio::test]
    async fn quarter_returns_correct_value() {
        assert_eq!(run_query("SELECT quarter(DATE '2026-03-30')").await, 1.0);
        assert_eq!(run_query("SELECT quarter(DATE '2026-06-15')").await, 2.0);
    }

    #[tokio::test]
    async fn day_of_year_returns_correct_value() {
        // 2026-03-30: Jan=31 + Feb=28 + 30 = 89
        assert_eq!(run_query("SELECT day_of_year(DATE '2026-03-30')").await, 89.0);
    }

    #[tokio::test]
    async fn year_works_with_timestamp() {
        assert_eq!(
            run_query("SELECT year(TIMESTAMP '2026-03-30 14:30:00')").await,
            2026.0,
        );
    }

    #[tokio::test]
    async fn hour_works_with_timestamp() {
        assert_eq!(
            run_query("SELECT hour(TIMESTAMP '2026-03-30 14:30:00')").await,
            14.0,
        );
    }

    #[tokio::test]
    async fn week_iso() {
        // 2026-01-05 is Monday of ISO week 2
        assert_eq!(run_query("SELECT week(DATE '2026-01-05')").await, 2.0);
    }

    // ── Helpers for new function tests ────────────────────────────────────────

    async fn run_query_i64(sql: &str) -> i64 {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    async fn run_query_string(sql: &str) -> String {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    }

    // ── date_add tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn date_add_days() {
        // date_add('day', 5, DATE '2026-01-01') → DATE '2026-01-06'
        // We verify by reading the year component back out.
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(date_add('day', 5, DATE '2026-01-01'))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let year = col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(year, 2026.0);

        // Verify the day moved by checking day() component
        let ctx2 = SessionContext::new();
        register_trino_functions(&ctx2);
        let batches2 = ctx2
            .sql("SELECT day(date_add('day', 5, DATE '2026-01-01'))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let day = batches2[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(day, 6.0);
    }

    #[tokio::test]
    async fn date_add_months() {
        // date_add('month', 2, DATE '2026-01-15') → month 3
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT month(date_add('month', 2, DATE '2026-01-15'))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let m = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(m, 3.0);
    }

    #[tokio::test]
    async fn date_add_years() {
        // date_add('year', 1, DATE '2026-03-30') → year 2027
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(date_add('year', 1, DATE '2026-03-30'))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let y = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(y, 2027.0);
    }

    // ── date_diff tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn date_diff_days() {
        let v = run_query_i64(
            "SELECT date_diff('day', DATE '2026-01-01', DATE '2026-01-06')",
        )
        .await;
        assert_eq!(v, 5);
    }

    #[tokio::test]
    async fn date_diff_months() {
        let v = run_query_i64(
            "SELECT date_diff('month', DATE '2026-01-01', DATE '2026-04-01')",
        )
        .await;
        assert_eq!(v, 3);
    }

    #[tokio::test]
    async fn date_diff_years() {
        let v = run_query_i64(
            "SELECT date_diff('year', DATE '2020-01-01', DATE '2026-01-01')",
        )
        .await;
        assert_eq!(v, 6);
    }

    // ── from_unixtime tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn from_unixtime_produces_timestamp() {
        // epoch 0 → timestamp 1970-01-01 00:00:00; year() should return 1970
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(from_unixtime(0))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let y = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(y, 1970.0);
    }

    // ── to_unixtime tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn to_unixtime_produces_epoch_seconds() {
        // DATE '1970-01-01' → 0.0
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT to_unixtime(DATE '1970-01-01')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 0.0);
    }

    #[tokio::test]
    async fn to_unixtime_roundtrip() {
        // from_unixtime(to_unixtime(ts)) should give back year 2026
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(from_unixtime(to_unixtime(TIMESTAMP '2026-03-30 12:00:00')))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let y = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(y, 2026.0);
    }

    // ── typeof tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn typeof_integer() {
        let s = run_query_string("SELECT typeof(42)").await;
        // DataFusion represents integer literals as Int64
        assert!(
            s.to_lowercase().contains("int"),
            "expected type name containing 'int', got: {s}"
        );
    }

    #[tokio::test]
    async fn typeof_string() {
        let s = run_query_string("SELECT typeof('hello')").await;
        assert!(
            s.to_lowercase().contains("utf8") || s.to_lowercase().contains("str"),
            "expected type name for string, got: {s}"
        );
    }

    // ── if() tests ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn trino_if_true_branch() {
        // if(1 = 1, 10, 20) → 10
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT if(1 = 1, 10, 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        // DataFusion resolves 10 as Int64
        let v = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 10);
    }

    #[tokio::test]
    async fn trino_if_false_branch() {
        // if(1 = 2, 10, 20) → 20
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT if(1 = 2, 10, 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 20);
    }

    // ── date_format tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn date_format_iso_date() {
        let s = run_query_string(
            "SELECT date_format(TIMESTAMP '2024-01-15 10:30:45', '%Y-%m-%d')",
        )
        .await;
        assert_eq!(s, "2024-01-15");
    }

    #[tokio::test]
    async fn date_format_with_time() {
        let s = run_query_string(
            "SELECT date_format(TIMESTAMP '2024-01-15 10:30:45', '%Y-%m-%d %H:%i:%s')",
        )
        .await;
        assert_eq!(s, "2024-01-15 10:30:45");
    }

    #[tokio::test]
    async fn date_format_date_input() {
        let s = run_query_string(
            "SELECT date_format(DATE '2024-06-15', '%Y/%m/%d')",
        )
        .await;
        assert_eq!(s, "2024/06/15");
    }

    // ── date_parse tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn date_parse_iso_date() {
        // date_parse('2024-01-15', '%Y-%m-%d') → timestamp; extract year to verify
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(date_parse('2024-01-15', '%Y-%m-%d'))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let y = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(y, 2024.0);
    }

    #[tokio::test]
    async fn date_parse_with_time() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT hour(date_parse('2024-01-15 14:30:00', '%Y-%m-%d %H:%i:%s'))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let h = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(h, 14.0);
    }

    #[tokio::test]
    async fn date_parse_roundtrip() {
        // date_format(date_parse(s, fmt), fmt) should return the original string
        let s = run_query_string(
            "SELECT date_format(date_parse('2024-06-15', '%Y-%m-%d'), '%Y-%m-%d')",
        )
        .await;
        assert_eq!(s, "2024-06-15");
    }

    // ── now() tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn now_returns_current_year() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(now())")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let y = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        // Should be at least 2025 (test will keep working for years)
        assert!(y >= 2025.0, "expected current year, got {y}");
    }

    // ── json_object tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn json_object_string_values() {
        let s = run_query_string(
            "SELECT json_object('name', 'Alice', 'city', 'Amsterdam')",
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed["name"], "Alice");
        assert_eq!(parsed["city"], "Amsterdam");
    }

    #[tokio::test]
    async fn json_object_mixed_types() {
        let s = run_query_string(
            "SELECT json_object('name', 'Bob', 'age', 30)",
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed["name"], "Bob");
        assert_eq!(parsed["age"], 30);
    }

    // ── json_format tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn json_format_passthrough() {
        let s = run_query_string(
            r#"SELECT json_format('{"key":"value"}')"#,
        )
        .await;
        assert_eq!(s, r#"{"key":"value"}"#);
    }

    // ── strpos tests ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn strpos_found() {
        let v = run_query_i64("SELECT strpos('hello world', 'world')").await;
        assert_eq!(v, 7); // 1-based, 'world' starts at position 7
    }

    #[tokio::test]
    async fn strpos_not_found() {
        let v = run_query_i64("SELECT strpos('hello world', 'xyz')").await;
        assert_eq!(v, 0);
    }

    #[tokio::test]
    async fn strpos_at_start() {
        let v = run_query_i64("SELECT strpos('hello', 'hel')").await;
        assert_eq!(v, 1);
    }

    // ── localtime / localtimestamp tests ──────────────────────────────────────

    #[tokio::test]
    async fn localtimestamp_returns_current_year() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(localtimestamp())")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let y = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(y >= 2025.0, "expected current year, got {y}");
    }

    #[tokio::test]
    async fn localtime_returns_current_year() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT year(localtime())")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let y = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!(y >= 2025.0, "expected current year, got {y}");
    }

    // ── date_trunc (DataFusion built-in, verify Trino compat) ────────────────

    #[tokio::test]
    async fn date_trunc_builtin_works() {
        // DataFusion's built-in date_trunc should match Trino signature
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT date_trunc('month', TIMESTAMP '2024-06-15 10:30:00')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // Should truncate to 2024-06-01 00:00:00
        let col = batches[0].column(0);
        let ts_arr = col
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>();
        let ts_ns = col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>();
        // Just verify it ran without error; the result type may vary
        assert!(ts_arr.is_some() || ts_ns.is_some(), "date_trunc should return a timestamp");
    }

    // ── concat_ws / replace / split_part (DataFusion built-ins, verify) ──────

    #[tokio::test]
    async fn concat_ws_builtin_works() {
        let s = run_query_string("SELECT concat_ws('-', 'a', 'b', 'c')").await;
        assert_eq!(s, "a-b-c");
    }

    #[tokio::test]
    async fn replace_builtin_works() {
        let s = run_query_string("SELECT replace('hello world', 'world', 'rust')").await;
        assert_eq!(s, "hello rust");
    }

    #[tokio::test]
    async fn split_part_builtin_works() {
        let s = run_query_string("SELECT split_part('a-b-c', '-', 2)").await;
        assert_eq!(s, "b");
    }
}
