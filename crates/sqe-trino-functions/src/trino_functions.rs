//! Trino-compatible function aliases for DataFusion.
//!
//! DataFusion uses `extract(YEAR FROM d)` / `date_part('year', d)` while
//! Trino provides standalone functions like `year(d)`, `month(d)`, etc.
//! These UDFs bridge the gap so Trino SQL and dbt models work unmodified.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
    StringArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
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
    ctx.register_udf(ScalarUDF::from(TrinoIff));
    ctx.register_udf(ScalarUDF::from(TypeOf));

    // Date formatting / parsing (Trino compat)
    ctx.register_udf(ScalarUDF::from(DateFormat));
    ctx.register_udf(ScalarUDF::from(DateParse));

    // now() → current timestamp
    ctx.register_udf(ScalarUDF::from(TrinoNow));

    // JSON functions
    ctx.register_udf(ScalarUDF::from(JsonObject));
    ctx.register_udf(ScalarUDF::from(JsonFormat));

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
    // Note: ToHex intentionally NOT registered — DataFusion 52 has a built-in
    // to_hex(integer) that formats integers as hex strings. Registering our
    // string-byte variant would shadow it and break integer callers. Users can
    // use encode(s, 'hex') for binary→hex conversion.
    ctx.register_udf(ScalarUDF::from(FromUtf8));
    ctx.register_udf(ScalarUDF::from(ToUtf8));

    // Trino JSON aliases — map Trino names to lightweight serde_json-based impls
    ctx.register_udf(ScalarUDF::from(JsonExtract));
    ctx.register_udf(ScalarUDF::from(JsonExtractScalar));
    ctx.register_udf(ScalarUDF::from(JsonArrayLength));
    ctx.register_udf(ScalarUDF::from(JsonParse));

    // Trino math + string aliases — short standalone names that Trino has and
    // DataFusion does not expose under that exact spelling. Each one is a
    // small wrapper UDF over an existing primitive.
    ctx.register_udf(ScalarUDF::from(TrinoE));
    ctx.register_udf(ScalarUDF::from(TrinoMod));
    ctx.register_udf(ScalarUDF::from(TrinoTruncate));
    ctx.register_udf(ScalarUDF::from(TrinoSign));
    ctx.register_udf(ScalarUDF::from(TrinoCodepoint));

    // Trino aggregate aliases — these are not new aggregates. Each is the
    // existing DataFusion aggregate UDAF re-registered with the Trino name
    // added to its `aliases` list. The SessionContext registry inserts the
    // UDAF under both its primary name and every alias (see
    // datafusion-execution-53.1.0/src/task.rs::register_udaf), so SELECTs
    // that use the Trino spelling resolve to the same accumulator.
    register_trino_aggregate_aliases(ctx);

    // Trino split(s, delim) returns ARRAY(VARCHAR). DataFusion's
    // `string_to_array(s, delim)` returns the same shape; register `split`
    // as an alias on the same UDF so SELECT split(...) resolves to it.
    use datafusion::functions_nested::string::string_to_array_udf;
    let split = (*string_to_array_udf())
        .clone()
        .with_aliases(["split"]);
    ctx.register_udf(split);

    // Bitwise scalar functions, `sequence`, and `slice` (#346, #349).
    crate::scalar_fns::register_scalar_fns(ctx);

    // count_if, element_at (array/map), contains (array) (#356).
    crate::coverage_fns::register_coverage_fns(ctx);
}

/// Register Trino-spelled aliases for existing DataFusion aggregate UDAFs.
///
/// The DataFusion aggregates (`string_agg`, `bit_and`, `bit_or`,
/// `approx_percentile_cont`) are already registered by default in a
/// `SessionContext`. We re-register a clone with `with_aliases([trino_name])`
/// so the registry inserts the same UDAF under the Trino-spelled key.
fn register_trino_aggregate_aliases(ctx: &datafusion::prelude::SessionContext) {
    use datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf;
    use datafusion::functions_aggregate::bit_and_or_xor::{
        bit_and_udaf, bit_or_udaf, bit_xor_udaf,
    };
    use datafusion::functions_aggregate::string_agg::string_agg_udaf;

    // listagg(x, sep) → string_agg(x, sep)
    let listagg = (*string_agg_udaf())
        .clone()
        .with_aliases(["listagg"]);
    ctx.register_udaf(listagg);

    // bitwise_and_agg(x) → bit_and(x)
    let bitwise_and = (*bit_and_udaf())
        .clone()
        .with_aliases(["bitwise_and_agg"]);
    ctx.register_udaf(bitwise_and);

    // bitwise_or_agg(x) → bit_or(x)
    let bitwise_or = (*bit_or_udaf())
        .clone()
        .with_aliases(["bitwise_or_agg"]);
    ctx.register_udaf(bitwise_or);

    // bitwise_xor_agg(x) → bit_xor(x). DataFusion exposes `bit_xor`
    // natively; DuckDB and Snowflake use the explicit `bitwise_xor_agg`
    // spelling. Same pattern as the and/or aliases above.
    let bitwise_xor = (*bit_xor_udaf())
        .clone()
        .with_aliases(["bitwise_xor_agg"]);
    ctx.register_udaf(bitwise_xor);

    // approx_percentile(x, p) → approx_percentile_cont(x, p)
    let approx_pc = (*approx_percentile_cont_udaf())
        .clone()
        .with_aliases(["approx_percentile"]);
    ctx.register_udaf(approx_pc);

    // every(x) → bool_and(x). Trino spec calls this `every`; DuckDB
    // and Postgres both have `every` as a synonym for `bool_and`.
    use datafusion::functions_aggregate::bool_and_or::bool_and_udaf;
    let every = (*bool_and_udaf()).clone().with_aliases(["every"]);
    ctx.register_udaf(every);

    // variance(x) → var_samp(x). Trino's `variance` is a synonym for the
    // sample variance (mirroring `stddev` = `stddev_samp`). DataFusion's
    // sample-variance UDAF is named `var` with aliases `var_sample` / `var_samp`
    // but not `variance`, so SELECT variance(...) hit FUNCTION_NOT_FOUND. Adding
    // the alias resolves it to the same accumulator. (#333)
    use datafusion::functions_aggregate::variance::var_samp_udaf;
    let variance = (*var_samp_udaf()).clone().with_aliases(["variance"]);
    ctx.register_udaf(variance);

    // skewness(x) / kurtosis(x) — Trino higher-moment aggregates. Not
    // DataFusion built-ins; real UDAFs over shared online central moments
    // (count, m1, m2, m3, m4) in crate::central_moments, matching Trino's
    // exact update / merge / output arithmetic. (#333)
    ctx.register_udaf(crate::central_moments::CentralMoment::skewness_udaf());
    ctx.register_udaf(crate::central_moments::CentralMoment::kurtosis_udaf());

    // max_by(x, y) / min_by(x, y) — real UDAFs that pick x at the row
    // where y is max/min. arg_max / arg_min are registered as aliases
    // (DuckDB and ClickHouse spelling). Replaces the previous scalar
    // stubs that returned the first argument and produced wrong
    // results in any aggregation context.
    ctx.register_udaf(crate::aggregates::ArgExtremum::max_by_udaf());
    ctx.register_udaf(crate::aggregates::ArgExtremum::min_by_udaf());

    // Map-producing aggregates (Trino-specific). All four use the same
    // type-flexible MapArray construction path. State for multi-phase
    // aggregation is List<Struct{key, value}> across the family.
    //
    // - histogram(x): MAP<typeof(x), BIGINT> with count per distinct value.
    // - map_agg(k, v): MAP<K, V>; last-wins on duplicate keys.
    // - multimap_agg(k, v): MAP<K, ARRAY<V>>; preserves insertion order.
    // - map_union(m): merges multiple maps; last-wins on duplicate keys.
    ctx.register_udaf(crate::histogram::Histogram::udaf());
    ctx.register_udaf(crate::map_aggregates::MapAgg::udaf());
    ctx.register_udaf(crate::map_aggregates::MultimapAgg::udaf());
    ctx.register_udaf(crate::map_aggregates::MapUnion::udaf());
}

/// Extract a chrono component from a Date32, Timestamp, or Time64 array.
/// Returns Int64 to match Trino's BIGINT return type for extraction functions.
///
/// `f_time` returns `None` for extracts that don't apply to TIME columns
/// (year, month, day_of_*, etc.). The dispatch surfaces a clear error so
/// users see `year(time_col)` rejected rather than silently coerced to 0.
fn extract_component(
    arg: &ColumnarValue,
    fn_name: &str,
    f_date: fn(NaiveDate) -> f64,
    f_ts: fn(i64) -> f64,
    f_ts_ns: fn(i64) -> f64,
    f_time: fn(i64) -> Option<f64>,
) -> DFResult<ColumnarValue> {
    match arg {
        ColumnarValue::Array(array) => {
            let result: Int64Array = if let Some(date_arr) = array.as_any().downcast_ref::<Date32Array>() {
                date_arr.iter().map(|opt| opt.and_then(|days| {
                    // date32_to_datetime returns None for Date32 values whose
                    // day count overflows NaiveDateTime (near i32::MAX). Emit a
                    // NULL row instead of unwrapping into a panic.
                    temporal_conversions::date32_to_datetime(days)
                        .map(|dt| f_date(dt.date()) as i64)
                })).collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                ts_arr.iter().map(|opt| opt.map(|v| f_ts(v) as i64)).collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                ts_arr.iter().map(|opt| opt.map(|v| f_ts_ns(v) as i64)).collect()
            } else if let Some(time_arr) = array.as_any().downcast_ref::<Time64MicrosecondArray>() {
                build_time_int64(time_arr.iter(), 1, fn_name, f_time)?
            } else if let Some(time_arr) = array.as_any().downcast_ref::<Time64NanosecondArray>() {
                // DataFusion produces Time64(Nanosecond) for `CAST('HH:MM:SS' AS TIME)`
                // and friends. Convert nanoseconds-since-midnight to
                // microseconds before delegating to the common time path.
                build_time_int64(time_arr.iter(), 1_000, fn_name, f_time)?
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Expected Date32, Timestamp, or Time64, got {:?}", array.data_type()
                )));
            };
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
        ColumnarValue::Scalar(scalar) => {
            use datafusion::common::ScalarValue;
            let val = match scalar {
                ScalarValue::Date32(Some(days)) => {
                    // None for extreme Date32 values that overflow NaiveDateTime;
                    // surface a NULL rather than panicking on unwrap.
                    match temporal_conversions::date32_to_datetime(*days) {
                        Some(dt) => f_date(dt.date()) as i64,
                        None => return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
                    }
                }
                ScalarValue::TimestampMicrosecond(Some(us), _) => f_ts(*us) as i64,
                ScalarValue::TimestampNanosecond(Some(ns), _) => f_ts_ns(*ns) as i64,
                ScalarValue::Time64Microsecond(Some(us)) => match f_time(*us) {
                    Some(v) => v as i64,
                    None => return Err(DataFusionError::Plan(format!(
                        "{fn_name}() is not supported on TIME columns; use a TIMESTAMP or DATE source"
                    ))),
                },
                ScalarValue::Time64Nanosecond(Some(ns)) => {
                    // DataFusion produces Time64(Nanosecond) for `CAST('HH:MM:SS' AS TIME)`
                    // and TIME literals. Convert ns -> us.
                    let us = ns / 1_000;
                    match f_time(us) {
                        Some(v) => v as i64,
                        None => return Err(DataFusionError::Plan(format!(
                            "{fn_name}() is not supported on TIME columns; use a TIMESTAMP or DATE source"
                        ))),
                    }
                }
                _ => return Err(DataFusionError::Internal(format!(
                    "Expected date, timestamp, or time scalar, got {scalar:?}"
                ))),
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(val))))
        }
    }
}

/// Convert microseconds-since-midnight to a chrono `NaiveTime`. Returns
/// `None` if the input is out of range (>= 86_400_000_000 us).
fn time_us_to_naive(us: i64) -> Option<chrono::NaiveTime> {
    if !(0..86_400_000_000).contains(&us) {
        return None;
    }
    let secs = (us / 1_000_000) as u32;
    let nanos = ((us % 1_000_000) * 1_000) as u32;
    chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
}

/// Build an `Int64Array` from an iterator of optional time values, dividing
/// each value by `denom` to convert to microseconds-since-midnight before
/// applying `f_time`. The `fn_name` is used in the error message when
/// `f_time` returns `None` (e.g. `year(time_col)`).
fn build_time_int64<I>(
    iter: I,
    denom: i64,
    fn_name: &str,
    f_time: fn(i64) -> Option<f64>,
) -> DFResult<Int64Array>
where
    I: Iterator<Item = Option<i64>>,
{
    let mut out = Vec::new();
    for opt in iter {
        match opt {
            None => out.push(None),
            Some(raw) => match f_time(raw / denom) {
                Some(v) => out.push(Some(v as i64)),
                None => return Err(DataFusionError::Plan(format!(
                    "{fn_name}() is not supported on TIME columns; use a TIMESTAMP or DATE source"
                ))),
            },
        }
    }
    Ok(out.into_iter().collect())
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
///
/// The `$f_time` callback returns `Some(v)` for extracts that apply to TIME
/// (hour, minute, second) and `None` otherwise so `year(time_col)` etc. fail
/// loud rather than silently producing 0.
macro_rules! trino_extract_fn {
    ($struct_name:ident, $fn_name:expr, $f_date:expr, $f_us:expr, $f_ns:expr, $f_time:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $struct_name;

        impl ScalarUDFImpl for $struct_name {

            fn name(&self) -> &str { $fn_name }

            fn signature(&self) -> &Signature {
                static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
                    Signature::any(1, Volatility::Immutable)
                });
                &SIG
            }

            fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Int64)  // Trino returns BIGINT for date extraction functions
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                extract_component(&args.args[0], $fn_name, $f_date, $f_us, $f_ns, $f_time)
            }
        }
    };
}

trino_extract_fn!(ExtractYear, "year",
    |d: NaiveDate| d.year() as f64,
    |us| us_to_naive(us).year() as f64,
    |ns| ns_to_naive(ns).year() as f64,
    |_us: i64| None
);
trino_extract_fn!(ExtractMonth, "month",
    |d: NaiveDate| d.month() as f64,
    |us| us_to_naive(us).month() as f64,
    |ns| ns_to_naive(ns).month() as f64,
    |_us: i64| None
);
trino_extract_fn!(ExtractDay, "day",
    |d: NaiveDate| d.day() as f64,
    |us| us_to_naive(us).day() as f64,
    |ns| ns_to_naive(ns).day() as f64,
    |_us: i64| None
);
trino_extract_fn!(ExtractHour, "hour",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).hour() as f64,
    |ns| ns_to_naive(ns).hour() as f64,
    |us: i64| time_us_to_naive(us).map(|t| t.hour() as f64)
);
trino_extract_fn!(ExtractMinute, "minute",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).minute() as f64,
    |ns| ns_to_naive(ns).minute() as f64,
    |us: i64| time_us_to_naive(us).map(|t| t.minute() as f64)
);
trino_extract_fn!(ExtractSecond, "second",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).second() as f64,
    |ns| ns_to_naive(ns).second() as f64,
    |us: i64| time_us_to_naive(us).map(|t| t.second() as f64)
);
trino_extract_fn!(DayOfWeek, "day_of_week",
    |d: NaiveDate| d.weekday().num_days_from_sunday() as f64,
    |us| us_to_naive(us).weekday().num_days_from_sunday() as f64,
    |ns| ns_to_naive(ns).weekday().num_days_from_sunday() as f64,
    |_us: i64| None
);
trino_extract_fn!(DayOfYear, "day_of_year",
    |d: NaiveDate| d.ordinal() as f64,
    |us| us_to_naive(us).ordinal() as f64,
    |ns| ns_to_naive(ns).ordinal() as f64,
    |_us: i64| None
);
trino_extract_fn!(Quarter, "quarter",
    |d: NaiveDate| ((d.month() - 1) / 3 + 1) as f64,
    |us| { let m = us_to_naive(us).month(); ((m - 1) / 3 + 1) as f64 },
    |ns| { let m = ns_to_naive(ns).month(); ((m - 1) / 3 + 1) as f64 },
    |_us: i64| None
);
trino_extract_fn!(Week, "week",
    |d: NaiveDate| d.iso_week().week() as f64,
    |us| us_to_naive(us).iso_week().week() as f64,
    |ns| ns_to_naive(ns).iso_week().week() as f64,
    |_us: i64| None
);

// ─── Helper: time-unit enum + parser ────────────────────────────────────────

/// Time-unit selector for `date_add` / `date_diff` / `date_trunc`.
///
/// Replaces the previous stringly-typed `&'static str` dispatch where a
/// typo in one of three match arms compiled silently and reached
/// `unreachable!()` only at runtime (issue #130).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

fn parse_unit(unit: &str) -> DFResult<TimeUnit> {
    match unit.to_lowercase().as_str() {
        "year" | "years" => Ok(TimeUnit::Year),
        "month" | "months" => Ok(TimeUnit::Month),
        "day" | "days" => Ok(TimeUnit::Day),
        "hour" | "hours" => Ok(TimeUnit::Hour),
        "minute" | "minutes" => Ok(TimeUnit::Minute),
        "second" | "seconds" => Ok(TimeUnit::Second),
        other => Err(DataFusionError::Internal(format!(
            "Unsupported date unit: {other}"
        ))),
    }
}

/// Add `amount` of `unit` to a NaiveDate, returning the new NaiveDate.
fn date_add_date(d: NaiveDate, unit: TimeUnit, amount: i64) -> DFResult<NaiveDate> {
    let result = match unit {
        TimeUnit::Year => {
            let months = amount * 12;
            if months >= 0 {
                d.checked_add_months(Months::new(months as u32))
            } else {
                d.checked_sub_months(Months::new((-months) as u32))
            }
        }
        TimeUnit::Month => {
            if amount >= 0 {
                d.checked_add_months(Months::new(amount as u32))
            } else {
                d.checked_sub_months(Months::new((-amount) as u32))
            }
        }
        TimeUnit::Day => d.checked_add_signed(Duration::days(amount)),
        TimeUnit::Hour => d.checked_add_signed(Duration::hours(amount)),
        TimeUnit::Minute => d.checked_add_signed(Duration::minutes(amount)),
        TimeUnit::Second => d.checked_add_signed(Duration::seconds(amount)),
    };
    result.ok_or_else(|| DataFusionError::Internal("date_add overflow".to_string()))
}

/// Add `amount` of `unit` to a microsecond timestamp, returning updated micros.
fn ts_add_us(us: i64, unit: TimeUnit, amount: i64) -> DFResult<i64> {
    let dt = chrono::DateTime::from_timestamp_micros(us)
        .unwrap_or_default()
        .naive_utc();
    let result = match unit {
        TimeUnit::Year => {
            let months = amount * 12;
            let date = if months >= 0 {
                dt.date().checked_add_months(Months::new(months as u32))
            } else {
                dt.date().checked_sub_months(Months::new((-months) as u32))
            };
            date.map(|d| d.and_time(dt.time()))
        }
        TimeUnit::Month => {
            let date = if amount >= 0 {
                dt.date().checked_add_months(Months::new(amount as u32))
            } else {
                dt.date().checked_sub_months(Months::new((-amount) as u32))
            };
            date.map(|d| d.and_time(dt.time()))
        }
        TimeUnit::Day => dt.checked_add_signed(Duration::days(amount)),
        TimeUnit::Hour => dt.checked_add_signed(Duration::hours(amount)),
        TimeUnit::Minute => dt.checked_add_signed(Duration::minutes(amount)),
        TimeUnit::Second => dt.checked_add_signed(Duration::seconds(amount)),
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

        // Compute the epoch baseline once. `from_ymd_opt(1970, 1, 1)` is
        // guaranteed to be `Some`; treat any failure as a hard error rather
        // than panicking via .unwrap(). Issue #78.
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| DataFusionError::Internal("epoch literal invalid".into()))?;

        match &args.args[2] {
            ColumnarValue::Array(array) => {
                if let Some(date_arr) = array.as_any().downcast_ref::<Date32Array>() {
                    let mut out: Vec<Option<i32>> = Vec::with_capacity(date_arr.len());
                    for opt in date_arr.iter() {
                        match opt {
                            None => out.push(None),
                            Some(days) => {
                                let d = temporal_conversions::date32_to_datetime(days)
                                    .ok_or_else(|| {
                                        DataFusionError::Execution(format!(
                                            "date_add: Date32 value {days} out of supported range"
                                        ))
                                    })?
                                    .date();
                                let new_d = date_add_date(d, unit_str, amount)?;
                                out.push(Some(
                                    new_d.signed_duration_since(epoch).num_days() as i32,
                                ));
                            }
                        }
                    }
                    let result: Date32Array = out.into_iter().collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                    let mut out: Vec<Option<i64>> = Vec::with_capacity(ts_arr.len());
                    for opt in ts_arr.iter() {
                        match opt {
                            None => out.push(None),
                            Some(us) => out.push(Some(ts_add_us(us, unit_str, amount)?)),
                        }
                    }
                    let result: TimestampMicrosecondArray = out.into_iter().collect();
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
                    let d = temporal_conversions::date32_to_datetime(*days)
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "date_add: Date32 value {days} out of supported range"
                            ))
                        })?
                        .date();
                    let new_d = date_add_date(d, unit_str, amount)?;
                    let new_days = new_d.signed_duration_since(epoch).num_days() as i32;
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

        fn compute_diff(unit: TimeUnit, d1: NaiveDate, d2: NaiveDate) -> i64 {
            match unit {
                TimeUnit::Year => (d2.year() - d1.year()) as i64,
                TimeUnit::Month => {
                    (d2.year() - d1.year()) as i64 * 12 + (d2.month() as i64 - d1.month() as i64)
                }
                TimeUnit::Day => d2.signed_duration_since(d1).num_days(),
                TimeUnit::Hour => d2.signed_duration_since(d1).num_hours(),
                TimeUnit::Minute => d2.signed_duration_since(d1).num_minutes(),
                TimeUnit::Second => d2.signed_duration_since(d1).num_seconds(),
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
                        .and_then(|a| {
                            // None for out-of-range Date32; propagate as a NULL
                            // date rather than panicking on unwrap.
                            temporal_conversions::date32_to_datetime(a.value(i))
                                .map(|dt| dt.date())
                        })
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
                        // None for out-of-range Date32; yield a NULL result
                        // instead of unwrapping into a panic.
                        temporal_conversions::date32_to_datetime(*days).map(|dt| {
                            let d = dt.date();
                            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            d.signed_duration_since(epoch).num_seconds() as f64
                        })
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

// ─── if(condition, then, else) and iff(condition, then, else) ──────────────
//
// Both Trino's `if()` and Snowflake's `iff()` have identical 3-arg semantics:
// boolean condition, then-branch, else-branch. NULL condition resolves to the
// else branch (Snowflake spec). They differ only in the public name, so the
// invoke logic is shared.

fn invoke_if_impl(args: ScalarFunctionArgs, fn_name: &str) -> DFResult<ColumnarValue> {
    use datafusion::common::ScalarValue;

    let nrows = args.number_rows;
    let condition = &args.args[0];
    let then_val = &args.args[1];
    let else_val = &args.args[2];

    let mask: BooleanArray = match condition {
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!("{fn_name}(): condition must be boolean"))
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
                "{fn_name}(): unsupported condition type {other:?}"
            )))
        }
    };

    let to_array = |cv: &ColumnarValue, n: usize| -> DFResult<ArrayRef> {
        match cv {
            ColumnarValue::Array(a) => Ok(Arc::clone(a)),
            ColumnarValue::Scalar(sv) => sv.to_array_of_size(n).map_err(|e| {
                DataFusionError::Internal(format!("{fn_name}(): scalar expansion failed: {e}"))
            }),
        }
    };

    let then_arr = to_array(then_val, nrows)?;
    let else_arr = to_array(else_val, nrows)?;

    let result = zip(&mask, &then_arr, &else_arr).map_err(|e| {
        DataFusionError::Internal(format!("{fn_name}(): zip failed: {e}"))
    })?;

    Ok(ColumnarValue::Array(result))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoIf;

impl ScalarUDFImpl for TrinoIf {

    fn name(&self) -> &str {
        "if"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        args.get(1)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("if() needs 3 args".to_string()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_if_impl(args, "if")
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoIff;

impl ScalarUDFImpl for TrinoIff {

    fn name(&self) -> &str {
        "iff"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        args.get(1)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("iff() needs 3 args".to_string()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_if_impl(args, "iff")
    }
}

// ─── typeof(expr) → Utf8 ────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TypeOf;

impl ScalarUDFImpl for TypeOf {

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

        // `is_multiple_of` would be clearer but is stable only from 1.87;
        // workspace MSRV is 1.85.
        if args.args.len() % 2 != 0 {
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

// ─── localtime → alias for CURRENT_TIME (returns Time64) ───────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct LocalTime;

impl ScalarUDFImpl for LocalTime {
    fn name(&self) -> &str { "localtime" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::new(TypeSignature::Nullary, Volatility::Stable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        // Time-of-day, no timezone, microsecond precision. Matches Iceberg's
        // `time` primitive and the type that EXTRACT(HOUR|MINUTE|SECOND ...)
        // bridges accept.
        Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        let now = chrono::Local::now().time();
        // Microseconds since midnight.
        let us = now.num_seconds_from_midnight() as i64 * 1_000_000
            + (now.nanosecond() / 1_000) as i64;
        Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(us))))
    }
}

// ─── e() → Euler's number (Trino has it as a Nullary fn) ──────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoE;

impl ScalarUDFImpl for TrinoE {
    fn name(&self) -> &str { "e" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::new(TypeSignature::Nullary, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
            std::f64::consts::E,
        ))))
    }
}

// ─── mod(n, m) → n % m (Trino has it; DataFusion only has the % operator) ─

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoMod;

impl ScalarUDFImpl for TrinoMod {
    fn name(&self) -> &str { "mod" }

    fn signature(&self) -> &Signature {
        // Two args, any numeric type. Coerces to a common Float64 to keep
        // the return shape consistent across BIGINT and DOUBLE callers.
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::any(2, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        let [n, m] = take_function_args::<2>("mod", &args.args)?;
        let n_f = to_f64_columnar(n)?;
        let m_f = to_f64_columnar(m)?;
        match (n_f, m_f) {
            (ColumnarValue::Scalar(ScalarValue::Float64(Some(a))),
             ColumnarValue::Scalar(ScalarValue::Float64(Some(b)))) => {
                if b == 0.0 {
                    return Err(DataFusionError::Execution(
                        "mod(n, 0): division by zero".into(),
                    ));
                }
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(a % b))))
            }
            // For arrays we re-promote to Float64Array and elementwise mod.
            (a, b) => {
                let a_arr = a.into_array(args.number_rows)?;
                let b_arr = b.into_array(args.number_rows)?;
                let a_f = a_arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    DataFusionError::Internal("mod: lhs not Float64Array after coercion".into())
                })?;
                let b_f = b_arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    DataFusionError::Internal("mod: rhs not Float64Array after coercion".into())
                })?;
                let result: Float64Array = a_f
                    .iter()
                    .zip(b_f.iter())
                    .map(|(a, b)| match (a, b) {
                        (Some(av), Some(bv)) if bv != 0.0 => Some(av % bv),
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
        }
    }
}

/// Coerce any numeric ColumnarValue to Float64. Used by mod / truncate / sign
/// so users can pass BIGINT, INT, DOUBLE, or DECIMAL without surprise.
fn to_f64_columnar(v: ColumnarValue) -> DFResult<ColumnarValue> {
    use datafusion::common::ScalarValue;
    match v {
        ColumnarValue::Scalar(ScalarValue::Float64(_)) => Ok(v),
        ColumnarValue::Scalar(ScalarValue::Float32(Some(x))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(x as f64))))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(Some(x))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(x as f64))))
        }
        ColumnarValue::Scalar(ScalarValue::Int32(Some(x))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(x as f64))))
        }
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(x))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(x as f64))))
        }
        ColumnarValue::Scalar(ScalarValue::Float32(None) | ScalarValue::Int64(None) | ScalarValue::Int32(None) | ScalarValue::UInt64(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
        }
        ColumnarValue::Array(arr) => {
            let casted = arrow::compute::cast(&arr, &DataType::Float64).map_err(|e| {
                DataFusionError::Execution(format!("cannot coerce array to Float64: {e}"))
            })?;
            Ok(ColumnarValue::Array(casted))
        }
        other => Err(DataFusionError::Execution(format!(
            "expected numeric scalar, got {other:?}"
        ))),
    }
}

/// Helper used by signature-checked UDFs. Mirrors DataFusion's
/// `take_function_args` macro pattern for arity checks.
fn take_function_args<const N: usize>(
    fn_name: &str,
    args: &[ColumnarValue],
) -> DFResult<[ColumnarValue; N]> {
    if args.len() != N {
        return Err(DataFusionError::Plan(format!(
            "{fn_name}: expected {N} arguments, got {}",
            args.len()
        )));
    }
    let mut out: Vec<ColumnarValue> = Vec::with_capacity(N);
    out.extend(args.iter().cloned());
    out.try_into()
        .map_err(|_| DataFusionError::Internal(format!("{fn_name}: arity assertion failed")))
}

// ─── truncate(x [, n]) → DataFusion `trunc(x [, n])` alias ────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoTruncate;

impl ScalarUDFImpl for TrinoTruncate {
    fn name(&self) -> &str { "truncate" }

    fn signature(&self) -> &Signature {
        // 1 or 2 args. Same shape as Trino's truncate(x) and truncate(x, n).
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::variadic_any(Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        if args.args.is_empty() || args.args.len() > 2 {
            return Err(DataFusionError::Plan(format!(
                "truncate: expected 1 or 2 arguments, got {}",
                args.args.len()
            )));
        }
        let x = to_f64_columnar(args.args[0].clone())?;
        let n_decimals: i32 = if args.args.len() == 2 {
            match &args.args[1] {
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => *v as i32,
                ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => *v,
                ColumnarValue::Scalar(ScalarValue::Int64(None) | ScalarValue::Int32(None)) => 0,
                _ => {
                    return Err(DataFusionError::Plan(
                        "truncate: second argument (decimals) must be an integer".into(),
                    ))
                }
            }
        } else {
            0
        };
        let scale = 10f64.powi(n_decimals);
        let trunc_one = |v: f64| (v * scale).trunc() / scale;
        match x {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(Some(trunc_one(v))),
            )),
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
            }
            ColumnarValue::Array(arr) => {
                let f = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    DataFusionError::Internal("truncate: not Float64Array after coercion".into())
                })?;
                let result: Float64Array = f.iter().map(|v| v.map(trunc_one)).collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            other => Err(DataFusionError::Internal(format!(
                "truncate: unexpected scalar shape after coercion: {other:?}"
            ))),
        }
    }
}

// ─── sign(x) → DataFusion `signum(x)` alias ────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoSign;

impl ScalarUDFImpl for TrinoSign {
    fn name(&self) -> &str { "sign" }

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
        use datafusion::common::ScalarValue;
        let [x] = take_function_args::<1>("sign", &args.args)?;
        let x = to_f64_columnar(x)?;
        // Trino: sign(0) = 0. Rust's f64::signum(0.0) = 1.0 (positive zero
        // is signed positive). Override the zero case explicitly.
        let trino_signum = |v: f64| -> f64 {
            if v == 0.0 {
                0.0
            } else if v.is_nan() {
                f64::NAN
            } else {
                v.signum()
            }
        };
        match x {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(Some(trino_signum(v))),
            )),
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
            }
            ColumnarValue::Array(arr) => {
                let f = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    DataFusionError::Internal("sign: not Float64Array after coercion".into())
                })?;
                let result: Float64Array = f.iter().map(|v| v.map(trino_signum)).collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            other => Err(DataFusionError::Internal(format!(
                "sign: unexpected scalar shape: {other:?}"
            ))),
        }
    }
}

// ─── codepoint(s) → Unicode code point of a single-char string ─────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoCodepoint;

impl ScalarUDFImpl for TrinoCodepoint {
    fn name(&self) -> &str { "codepoint" }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::any(1, Volatility::Immutable)
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        // Trino returns INTEGER. We return Int32 to match.
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        let [arg] = take_function_args::<1>("codepoint", &args.args)?;
        let one = |s: &str| -> DFResult<i32> {
            let mut chars = s.chars();
            let c = chars.next().ok_or_else(|| {
                DataFusionError::Execution(
                    "codepoint: empty string has no code point".into(),
                )
            })?;
            if chars.next().is_some() {
                return Err(DataFusionError::Execution(format!(
                    "codepoint: input must contain exactly one Unicode character, got {} chars",
                    s.chars().count()
                )));
            }
            Ok(c as i32)
        };
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(one(&s)?))))
            }
            ColumnarValue::Scalar(
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))),
            ColumnarValue::Array(arr) => {
                let s = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Plan(
                        "codepoint: input must be Utf8 / VARCHAR".into(),
                    )
                })?;
                let mut out = Int32Array::builder(s.len());
                for v in s.iter() {
                    match v {
                        None => out.append_null(),
                        Some(text) => out.append_value(one(text)?),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
            }
            other => Err(DataFusionError::Plan(format!(
                "codepoint: input must be a string scalar or array, got {other:?}"
            ))),
        }
    }
}

// ─── localtimestamp → alias for CURRENT_TIMESTAMP ──────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct LocalTimestamp;

impl ScalarUDFImpl for LocalTimestamp {
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

use crate::helpers::{str_transform, str_transform_2};

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
        str_transform(&args.args[0], |s| {
            use std::fmt::Write;
            let mut result = String::new();
            for b in s.bytes() {
                if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                    result.push(b as char);
                } else {
                    write!(result, "%{:02X}", b).unwrap();
                }
            }
            Some(result)
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UrlDecode;

impl ScalarUDFImpl for UrlDecode {
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
        str_transform(&args.args[0], |s| Some(percent_decode(s.as_bytes())))
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
                let transform = $transform;
                str_transform(&args.args[0], |s| Some(transform(s)))
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

// Note: ToHex removed — DataFusion 52 has a built-in to_hex(integer) and
// shadowing it breaks integer hex formatting. Use encode(s, 'hex') instead.

encoding_udf!(FromHex, "from_hex", |s: &str| {
    // Slice the BYTE view, not the &str: stepping by 2 over a string with
    // multi-byte UTF-8 characters and slicing `&s[i..i+2]` lands inside a
    // character and panics ("not a char boundary"). Hex pairs are ASCII, so
    // operating on the raw bytes is both correct and panic-free.
    let raw = s.as_bytes();
    let bytes: Vec<u8> = (0..raw.len())
        .step_by(2)
        .filter_map(|i| {
            raw.get(i..i + 2)
                .and_then(|pair| std::str::from_utf8(pair).ok())
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
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
    // Slice the BYTE view, not the &str, so a non-ASCII input cannot panic on a
    // char-boundary slice.
    let raw = s.as_bytes();
    let bytes: Vec<u8> = (0..raw.len())
        .step_by(2)
        .filter_map(|i| {
            raw.get(i..i + 2)
                .and_then(|pair| std::str::from_utf8(pair).ok())
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
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

pub(crate) fn navigate_json<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
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
        str_transform_2(&args.args, |json, path| {
            let key = path.trim_start_matches("$.");
            let key = if key == "$" { "" } else { key };
            extract_json_value(json, key)
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonExtractScalar;

impl ScalarUDFImpl for JsonExtractScalar {
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
        str_transform_2(&args.args, |json, path| {
            let key = path.trim_start_matches("$.");
            let key = if key == "$" { "" } else { key };
            extract_json_scalar(json, key)
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayLength;

impl ScalarUDFImpl for JsonArrayLength {
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
        str_transform(&args.args[0], |s| {
            Some(
                serde_json::from_str::<serde_json::Value>(s)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|_| "null".to_string()),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use datafusion::prelude::SessionContext;

    /// Helper: run SQL returning an Int64 result (date extraction functions now return Int64).
    async fn run_query(sql: &str) -> i64 {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn year_returns_correct_value() {
        assert_eq!(run_query("SELECT year(DATE '2026-03-30')").await, 2026);
    }

    #[tokio::test]
    async fn month_returns_correct_value() {
        assert_eq!(run_query("SELECT month(DATE '2026-03-30')").await, 3);
    }

    #[tokio::test]
    async fn day_returns_correct_value() {
        assert_eq!(run_query("SELECT day(DATE '2026-03-30')").await, 30);
    }

    #[tokio::test]
    async fn day_of_week_monday() {
        // 2026-03-30 is Monday. Trino: Monday=1
        assert_eq!(run_query("SELECT day_of_week(DATE '2026-03-30')").await, 1);
    }

    #[tokio::test]
    async fn quarter_returns_correct_value() {
        assert_eq!(run_query("SELECT quarter(DATE '2026-03-30')").await, 1);
        assert_eq!(run_query("SELECT quarter(DATE '2026-06-15')").await, 2);
    }

    #[tokio::test]
    async fn day_of_year_returns_correct_value() {
        // 2026-03-30: Jan=31 + Feb=28 + 30 = 89
        assert_eq!(run_query("SELECT day_of_year(DATE '2026-03-30')").await, 89);
    }

    #[tokio::test]
    async fn year_works_with_timestamp() {
        assert_eq!(
            run_query("SELECT year(TIMESTAMP '2026-03-30 14:30:00')").await,
            2026,
        );
    }

    #[tokio::test]
    async fn hour_works_with_timestamp() {
        assert_eq!(
            run_query("SELECT hour(TIMESTAMP '2026-03-30 14:30:00')").await,
            14,
        );
    }

    #[tokio::test]
    async fn week_iso() {
        // 2026-01-05 is Monday of ISO week 2
        assert_eq!(run_query("SELECT week(DATE '2026-01-05')").await, 2);
    }

    // ── Helpers for new function tests ────────────────────────────────────────

    /// Run SQL returning an i64 result, handling both Int64 and UInt64 return types.
    async fn run_query_i64(sql: &str) -> i64 {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            arr.value(0)
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            arr.value(0) as i64
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
            arr.value(0) as i64
        } else {
            panic!(
                "Expected Int64/UInt64/Int32 array, got {:?}",
                col.data_type()
            );
        }
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
        assert_eq!(
            run_query("SELECT year(date_add('day', 5, DATE '2026-01-01'))").await,
            2026,
        );
        assert_eq!(
            run_query("SELECT day(date_add('day', 5, DATE '2026-01-01'))").await,
            6,
        );
    }

    #[tokio::test]
    async fn date_add_months() {
        // date_add('month', 2, DATE '2026-01-15') → month 3
        assert_eq!(
            run_query("SELECT month(date_add('month', 2, DATE '2026-01-15'))").await,
            3,
        );
    }

    #[tokio::test]
    async fn date_add_years() {
        // date_add('year', 1, DATE '2026-03-30') → year 2027
        assert_eq!(
            run_query("SELECT year(date_add('year', 1, DATE '2026-03-30'))").await,
            2027,
        );
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
        assert_eq!(
            run_query("SELECT year(from_unixtime(0))").await,
            1970,
        );
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
        assert_eq!(
            run_query("SELECT year(from_unixtime(to_unixtime(TIMESTAMP '2026-03-30 12:00:00')))").await,
            2026,
        );
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

    // ── iff() tests (Snowflake alias of if) ─────────────────────────────────

    #[tokio::test]
    async fn snowflake_iff_true_branch() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(TRUE, 'yes', 'no')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(v, "yes");
    }

    #[tokio::test]
    async fn snowflake_iff_false_branch() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(FALSE, 'yes', 'no')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(v, "no");
    }

    #[tokio::test]
    async fn snowflake_iff_null_condition_returns_else() {
        // Snowflake spec: NULL condition is treated as false, returns expr2.
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(CAST(NULL AS BOOLEAN), 'yes', 'no')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(v, "no");
    }

    #[tokio::test]
    async fn snowflake_iff_with_predicate() {
        // iff(1 = 1, 10, 20) -> 10 — same shape as the if() test for parity.
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(1 = 1, 10, 20)")
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
        assert_eq!(v, 10);
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
        assert_eq!(
            run_query("SELECT year(date_parse('2024-01-15', '%Y-%m-%d'))").await,
            2024,
        );
    }

    #[tokio::test]
    async fn date_parse_with_time() {
        assert_eq!(
            run_query("SELECT hour(date_parse('2024-01-15 14:30:00', '%Y-%m-%d %H:%i:%s'))").await,
            14,
        );
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
        let y = run_query("SELECT year(now())").await;
        assert!(y >= 2025, "expected current year, got {y}");
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
        let y = run_query("SELECT year(localtimestamp())").await;
        assert!(y >= 2025, "expected current year, got {y}");
    }

    #[tokio::test]
    async fn localtime_extracts_hour_minute_second() {
        // localtime() returns Time64(Microsecond); the EXTRACT bridges
        // accept it. Hour, minute, second land in 0..=23 / 0..=59 / 0..=59.
        let h = run_query("SELECT hour(localtime())").await;
        let m = run_query("SELECT minute(localtime())").await;
        let s = run_query("SELECT second(localtime())").await;
        assert!((0..=23).contains(&h), "hour out of range: {h}");
        assert!((0..=59).contains(&m), "minute out of range: {m}");
        assert!((0..=59).contains(&s), "second out of range: {s}");
    }

    #[tokio::test]
    async fn year_on_time_column_errors() {
        // Trino spec: year(time) is not supported. Our extract_component
        // surfaces a Plan error rather than silently returning 0. Confirm
        // we error on a Time64 input.
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let res = ctx.sql("SELECT year(localtime())").await;
        // The error can land at planning or execution depending on
        // DataFusion's lazy logical planning. Either way, it must fail.
        let failed = match res {
            Err(_) => true,
            Ok(plan) => plan.collect().await.is_err(),
        };
        assert!(failed, "year(time) should fail with a clear plan error");
    }

    // ── Trino math + string aliases ───────────────────────────────────────────

    /// Run SQL returning a Float64 result.
    async fn run_query_f64(sql: &str) -> f64 {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap_or_else(|| panic!("expected Float64Array, got {:?}", col.data_type()))
            .value(0)
    }

    #[tokio::test]
    async fn e_returns_eulers_constant() {
        let v = run_query_f64("SELECT e()").await;
        assert!(
            (v - std::f64::consts::E).abs() < 1e-15,
            "expected Euler's constant, got {v}"
        );
    }

    #[tokio::test]
    async fn mod_int_returns_remainder() {
        // Both operands integer; we coerce to Float64 internally so the
        // result lands as Float64. 10 % 3 = 1.
        let v = run_query_f64("SELECT mod(10, 3)").await;
        assert_eq!(v, 1.0);
    }

    #[tokio::test]
    async fn mod_float_returns_remainder() {
        let v = run_query_f64("SELECT mod(10.5, 3.0)").await;
        assert!((v - 1.5).abs() < 1e-12, "expected 1.5, got {v}");
    }

    #[tokio::test]
    async fn mod_zero_divisor_errors() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let res = ctx.sql("SELECT mod(10, 0)").await;
        let failed = match res {
            Err(_) => true,
            Ok(plan) => plan.collect().await.is_err(),
        };
        assert!(failed, "mod(_, 0) should error");
    }

    #[tokio::test]
    async fn truncate_drops_fractional_part() {
        let v = run_query_f64("SELECT truncate(3.7)").await;
        assert_eq!(v, 3.0);
        let v = run_query_f64("SELECT truncate(-3.7)").await;
        assert_eq!(v, -3.0);
    }

    #[tokio::test]
    async fn truncate_with_precision() {
        // Trino: truncate(2.71828, 2) = 2.71. Avoiding 3.14 because clippy
        // flags it as an approximation of std::f64::consts::PI.
        let v = run_query_f64("SELECT truncate(2.71828, 2)").await;
        assert!((v - 2.71).abs() < 1e-12, "expected 2.71, got {v}");
        let v = run_query_f64("SELECT truncate(-2.71828, 2)").await;
        assert!((v + 2.71).abs() < 1e-12, "expected -2.71, got {v}");
    }

    #[tokio::test]
    async fn sign_returns_signum() {
        assert_eq!(run_query_f64("SELECT sign(42.0)").await, 1.0);
        assert_eq!(run_query_f64("SELECT sign(-42.0)").await, -1.0);
        assert_eq!(run_query_f64("SELECT sign(0.0)").await, 0.0);
    }

    #[tokio::test]
    async fn codepoint_ascii_matches_byte() {
        // ASCII char: codepoint == byte. 'A' = 65.
        let v = run_query_i64("SELECT codepoint('A')").await;
        assert_eq!(v, 65);
    }

    #[tokio::test]
    async fn codepoint_unicode_returns_full_codepoint() {
        // Trino's codepoint() must return the full Unicode code point,
        // not the first UTF-8 byte. 'é' is U+00E9 = 233. ascii() on the
        // same input returns 195 (the first UTF-8 byte 0xC3); codepoint
        // is what callers actually want.
        let v = run_query_i64("SELECT codepoint('é')").await;
        assert_eq!(v, 233);
    }

    #[tokio::test]
    async fn codepoint_multi_char_errors() {
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let res = ctx.sql("SELECT codepoint('ab')").await;
        let failed = match res {
            Err(_) => true,
            Ok(plan) => plan.collect().await.is_err(),
        };
        assert!(failed, "codepoint of multi-char string should error");
    }

    #[tokio::test]
    async fn hour_on_typed_time_literal() {
        // A TIME-typed literal flows through Time64 -> hour() bridge.
        // We use TYPED literal via cast since DataFusion's parser may
        // not bind raw TIME 'HH:MM:SS' literals directly yet.
        let ctx = SessionContext::new();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT hour(CAST('14:30:45' AS TIME))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let val = col.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(val, 14, "hour from '14:30:45' should be 14");
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
