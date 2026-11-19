//! Trino-compatible function aliases for DataFusion.
//!
//! DataFusion uses `extract(YEAR FROM d)` / `date_part('year', d)` while
//! Trino provides standalone functions like `year(d)`, `month(d)`, etc.
//! These UDFs bridge the gap so Trino SQL and dbt models work unmodified.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int32Array, Int64Array, StringArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;
use arrow::datatypes::FieldRef;
use arrow::temporal_conversions;
use chrono::{Datelike, Duration, Months, NaiveDate, Timelike};
use datafusion::common::Result as DFResult;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use sqe_trino_protocol::encode;

/// Register all Trino-compatible function aliases on a SessionContext.
pub fn register_trino_functions(ctx: &datafusion::prelude::SessionContext) {
    // Rebuilds the session state, so it goes before any function is registered.
    crate::binary_fns::install_substring_planner(ctx);
    // Date/time extract functions
    ctx.register_udf(ScalarUDF::from(ExtractYear));
    ctx.register_udf(ScalarUDF::from(ExtractMonth));
    ctx.register_udf(ScalarUDF::from(ExtractDay).with_aliases(["day_of_month"]));
    ctx.register_udf(ScalarUDF::from(ExtractHour));
    ctx.register_udf(ScalarUDF::from(ExtractMinute));
    ctx.register_udf(ScalarUDF::from(ExtractSecond));
    ctx.register_udf(ScalarUDF::from(DayOfWeek).with_aliases(["dow"]));
    ctx.register_udf(ScalarUDF::from(DayOfYear).with_aliases(["doy"]));
    ctx.register_udf(ScalarUDF::from(Quarter));
    ctx.register_udf(ScalarUDF::from(Week).with_aliases(["week_of_year"]));
    ctx.register_udf(ScalarUDF::from(YearOfWeek).with_aliases(["yow"]));

    // Date arithmetic
    ctx.register_udf(ScalarUDF::from(DateAdd));
    ctx.register_udf(ScalarUDF::from(DateDiff));

    // Conditional / type functions
    ctx.register_udf(ScalarUDF::from(TrinoIf));
    ctx.register_udf(ScalarUDF::from(TrinoIff));
    ctx.register_udf(ScalarUDF::from(TypeOf));

    // now() → current timestamp
    ctx.register_udf(ScalarUDF::from(TrinoNow));

    // JSON functions
    ctx.register_udf(ScalarUDF::from(JsonObject));
    ctx.register_udf(ScalarUDF::from(JsonFormat));

    // Trino time aliases — these are registered as lightweight UDFs that
    // delegate to DataFusion built-ins already available.
    ctx.register_udf(ScalarUDF::from(LocalTime));
    ctx.register_udf(ScalarUDF::from(LocalTimestamp));

    // url_extract_* live in `url_fns`, registered below.
    ctx.register_udf(ScalarUDF::from(UrlEncode));
    ctx.register_udf(ScalarUDF::from(UrlDecode));

    // Trino JSON aliases — map Trino names to lightweight serde_json-based impls
    ctx.register_udf(ScalarUDF::from(JsonExtract));
    ctx.register_udf(ScalarUDF::from(JsonExtractScalar));
    ctx.register_udf(ScalarUDF::from(JsonArrayLength));
    ctx.register_udf(ScalarUDF::from(JsonParse));

    // Trino math + string aliases — short standalone names that Trino has and
    // DataFusion does not expose under that exact spelling. Each one is a
    // small wrapper UDF over an existing primitive.
    ctx.register_udf(ScalarUDF::from(TrinoE));
    ctx.register_udf(ScalarUDF::from(TrinoCodepoint));

    // Trino aggregate aliases — these are not new aggregates. Each is the
    // existing DataFusion aggregate UDAF re-registered with the Trino name
    // added to its `aliases` list. The SessionContext registry inserts the
    // UDAF under both its primary name and every alias (see
    // datafusion-execution-53.1.0/src/task.rs::register_udaf), so SELECTs
    // that use the Trino spelling resolve to the same accumulator.
    register_trino_aggregate_aliases(ctx);

    // Trino split(s, delim[, limit]) returns ARRAY(VARCHAR). This used to be
    // an alias on DataFusion's `string_to_array`, whose third argument is a
    // null-marker string, so Trino's `split(s, ',', 2)` failed to plan
    // (Athena views use the limit form). Own UDF, Trino semantics.
    ctx.register_udf(ScalarUDF::from(TrinoSplit));

    // Bitwise scalar functions, `sequence`, and `slice` (#346, #349).
    crate::scalar_fns::register_scalar_fns(ctx);

    // String functions (array-aware concat/reverse, bigint results, ...) and
    // the url_extract_* family.
    crate::string_fns::register_string_fns(ctx);
    crate::url_fns::register_url_fns(ctx);

    // count_if, element_at (array/map), contains (array) (#356).
    crate::coverage_fns::register_coverage_fns(ctx);

    // varbinary hashes and encodings, plus binary-aware md5 / to_hex / lpad /
    // rpad / substr. After the built-ins, whose names the overrides take over.
    crate::binary_fns::register_binary_fns(ctx);

    // Trino-typed math (ceil/floor/sign/truncate/mod/shifts/random and the
    // missing functions); replaces the DataFusion built-ins of those names.
    crate::math_fns::register_math_fns(ctx);

    // Date/time conversion, formatting and parsing with Trino's value and type.
    crate::datetime_fns::register_datetime_fns(ctx);

    // Trino array functions (array_last, zip, ngrams, ...). After the
    // built-ins: some of them replace one (`array_remove`, `array_first`).
    crate::array_fns::register_array_fns(ctx);
    crate::map_fns::register_map_fns(ctx);
    crate::agg_fns::register_agg_fns(ctx);

    // DataFusion's `to_char` and `map_extract` claim `date_format` and
    // `element_at` as aliases. Left claiming them, either could take the key
    // back whenever a query's session is rebuilt (see `overrides`).
    crate::overrides::release_alias(ctx, "to_char", "date_format");
    crate::overrides::release_alias(ctx, "map_extract", "element_at");
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
    let listagg = (*string_agg_udaf()).clone().with_aliases(["listagg"]);
    ctx.register_udaf(listagg);

    // bitwise_and_agg(x) → bit_and(x)
    let bitwise_and = (*bit_and_udaf()).clone().with_aliases(["bitwise_and_agg"]);
    ctx.register_udaf(bitwise_and);

    // bitwise_or_agg(x) → bit_or(x)
    let bitwise_or = (*bit_or_udaf()).clone().with_aliases(["bitwise_or_agg"]);
    ctx.register_udaf(bitwise_or);

    // bitwise_xor_agg(x) → bit_xor(x). DataFusion exposes `bit_xor`
    // natively; DuckDB and Snowflake use the explicit `bitwise_xor_agg`
    // spelling. Same pattern as the and/or aliases above.
    let bitwise_xor = (*bit_xor_udaf()).clone().with_aliases(["bitwise_xor_agg"]);
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

    // arbitrary(x) -> any_value(x). Trino returns an arbitrary non-null
    // value per group; DataFusion's any_value has the same contract. The
    // old scalar stub of the same name collapsed a whole batch to its
    // first non-null value and broke under GROUP BY.
    use datafusion::functions_aggregate::any_value::any_value_udaf;
    let arbitrary = (*any_value_udaf()).clone().with_aliases(["arbitrary"]);
    ctx.register_udaf(arbitrary);

    // skewness(x) / kurtosis(x) — Trino higher-moment aggregates. Not
    // DataFusion built-ins; real UDAFs over shared online central moments
    // (count, m1, m2, m3, m4) in crate::central_moments, matching Trino's
    // exact update / merge / output arithmetic. (#333)
    ctx.register_udaf(crate::central_moments::CentralMoment::skewness_udaf());
    ctx.register_udaf(crate::central_moments::CentralMoment::kurtosis_udaf());
    ctx.register_udaf(crate::aggregates::GeometricMean::udaf());

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

    // corr / covar_* / regr_slope / regr_intercept: `real` in, `real` out
    // (Trino 484); DataFusion's are double only.
    crate::real_aggregates::register_real_aggregates(ctx);

    // round over DECIMAL with Trino's result types.
    crate::decimal_math::register_decimal_math(ctx);
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
    let normalized;
    let arg = match crate::datetime_fns::prepare_extract(arg, fn_name)? {
        crate::datetime_fns::Prepared::Done(cv) => return Ok(cv),
        crate::datetime_fns::Prepared::Replace(cv) => {
            normalized = cv;
            &normalized
        }
        crate::datetime_fns::Prepared::Same => arg,
    };
    match arg {
        ColumnarValue::Array(array) => {
            let result: Int64Array = if let Some(date_arr) =
                array.as_any().downcast_ref::<Date32Array>()
            {
                date_arr
                    .iter()
                    .map(|opt| {
                        opt.and_then(|days| {
                            // date32_to_datetime returns None for Date32 values whose
                            // day count overflows NaiveDateTime (near i32::MAX). Emit a
                            // NULL row instead of unwrapping into a panic.
                            temporal_conversions::date32_to_datetime(days)
                                .map(|dt| f_date(dt.date()) as i64)
                        })
                    })
                    .collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>()
            {
                ts_arr
                    .iter()
                    .map(|opt| opt.map(|v| f_ts(v) as i64))
                    .collect()
            } else if let Some(ts_arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                ts_arr
                    .iter()
                    .map(|opt| opt.map(|v| f_ts_ns(v) as i64))
                    .collect()
            } else if let Some(time_arr) = array.as_any().downcast_ref::<Time64MicrosecondArray>() {
                build_time_int64(time_arr.iter(), 1, fn_name, f_time)?
            } else if let Some(time_arr) = array.as_any().downcast_ref::<Time64NanosecondArray>() {
                // DataFusion produces Time64(Nanosecond) for `CAST('HH:MM:SS' AS TIME)`
                // and friends. Convert nanoseconds-since-midnight to
                // microseconds before delegating to the common time path.
                build_time_int64(time_arr.iter(), 1_000, fn_name, f_time)?
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expected Date32, Timestamp, or Time64, got {:?}",
                    array.data_type()
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
                // NULL in, NULL out, whatever its type.
                s if s.is_null() => return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
                _ => return Err(DataFusionError::Plan(format!(
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
                None => {
                    return Err(DataFusionError::Plan(format!(
                    "{fn_name}() is not supported on TIME columns; use a TIMESTAMP or DATE source"
                )))
                }
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
            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                static SIG: std::sync::LazyLock<Signature> =
                    std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
                &SIG
            }

            fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Int64) // Trino returns BIGINT for date extraction functions
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                extract_component(&args.args[0], $fn_name, $f_date, $f_us, $f_ns, $f_time)
            }
        }
    };
}

trino_extract_fn!(
    ExtractYear,
    "year",
    |d: NaiveDate| d.year() as f64,
    |us| us_to_naive(us).year() as f64,
    |ns| ns_to_naive(ns).year() as f64,
    |_us: i64| None
);
trino_extract_fn!(
    ExtractMonth,
    "month",
    |d: NaiveDate| d.month() as f64,
    |us| us_to_naive(us).month() as f64,
    |ns| ns_to_naive(ns).month() as f64,
    |_us: i64| None
);
trino_extract_fn!(
    ExtractDay,
    "day",
    |d: NaiveDate| d.day() as f64,
    |us| us_to_naive(us).day() as f64,
    |ns| ns_to_naive(ns).day() as f64,
    |_us: i64| None
);
trino_extract_fn!(
    ExtractHour,
    "hour",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).hour() as f64,
    |ns| ns_to_naive(ns).hour() as f64,
    |us: i64| time_us_to_naive(us).map(|t| t.hour() as f64)
);
trino_extract_fn!(
    ExtractMinute,
    "minute",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).minute() as f64,
    |ns| ns_to_naive(ns).minute() as f64,
    |us: i64| time_us_to_naive(us).map(|t| t.minute() as f64)
);
trino_extract_fn!(
    ExtractSecond,
    "second",
    |_d: NaiveDate| 0.0,
    |us| us_to_naive(us).second() as f64,
    |ns| ns_to_naive(ns).second() as f64,
    |us: i64| time_us_to_naive(us).map(|t| t.second() as f64)
);
trino_extract_fn!(
    DayOfWeek,
    "day_of_week",
    // Trino uses ISO day-of-week: Monday=1 .. Sunday=7. chrono's
    // `number_from_monday()` matches; `num_days_from_sunday()` (Sunday=0) is
    // the Postgres/DataFusion `dow` convention and returned wrong values (#361).
    |d: NaiveDate| d.weekday().number_from_monday() as f64,
    |us| us_to_naive(us).weekday().number_from_monday() as f64,
    |ns| ns_to_naive(ns).weekday().number_from_monday() as f64,
    |_us: i64| None
);
trino_extract_fn!(
    DayOfYear,
    "day_of_year",
    |d: NaiveDate| d.ordinal() as f64,
    |us| us_to_naive(us).ordinal() as f64,
    |ns| ns_to_naive(ns).ordinal() as f64,
    |_us: i64| None
);
trino_extract_fn!(
    Quarter,
    "quarter",
    |d: NaiveDate| ((d.month() - 1) / 3 + 1) as f64,
    |us| {
        let m = us_to_naive(us).month();
        ((m - 1) / 3 + 1) as f64
    },
    |ns| {
        let m = ns_to_naive(ns).month();
        ((m - 1) / 3 + 1) as f64
    },
    |_us: i64| None
);
trino_extract_fn!(
    Week,
    "week",
    |d: NaiveDate| d.iso_week().week() as f64,
    |us| us_to_naive(us).iso_week().week() as f64,
    |ns| ns_to_naive(ns).iso_week().week() as f64,
    |_us: i64| None
);
trino_extract_fn!(
    YearOfWeek,
    "year_of_week",
    |d: NaiveDate| d.iso_week().year() as f64,
    |us| us_to_naive(us).iso_week().year() as f64,
    |ns| ns_to_naive(ns).iso_week().year() as f64,
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
        other => Err(DataFusionError::Plan(format!(
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
    result.ok_or_else(|| DataFusionError::Plan("date_add overflow".to_string()))
}

/// Add `amount` of `unit` to a naive datetime. Calendar units (year, month)
/// preserve the time-of-day; wall-clock units use signed durations. Shared by
/// the microsecond and nanosecond timestamp paths so both resolutions apply
/// identical semantics.
fn add_units_to_naive(
    dt: chrono::NaiveDateTime,
    unit: TimeUnit,
    amount: i64,
) -> Option<chrono::NaiveDateTime> {
    match unit {
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
    }
}

/// Add `amount` of `unit` to a microsecond timestamp, returning updated micros.
fn ts_add_us(us: i64, unit: TimeUnit, amount: i64) -> DFResult<i64> {
    let dt = chrono::DateTime::from_timestamp_micros(us)
        .unwrap_or_default()
        .naive_utc();
    let new_dt = add_units_to_naive(dt, unit, amount)
        .ok_or_else(|| DataFusionError::Plan("date_add overflow".to_string()))?;
    Ok(new_dt.and_utc().timestamp_micros())
}

/// Add `amount` of `unit` to a nanosecond timestamp, returning updated nanos.
/// DataFusion's default timestamp resolution (what `CAST(... AS timestamp)`
/// produces) is nanosecond, so this is the common path.
fn ts_add_ns(ns: i64, unit: TimeUnit, amount: i64) -> DFResult<i64> {
    let dt = chrono::DateTime::from_timestamp_nanos(ns).naive_utc();
    let new_dt = add_units_to_naive(dt, unit, amount)
        .ok_or_else(|| DataFusionError::Plan("date_add overflow".to_string()))?;
    new_dt
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| DataFusionError::Plan("ts overflow".to_string()))
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
                return Err(DataFusionError::Plan(format!(
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
                return Err(DataFusionError::Plan(format!(
                    "date_add: second arg must be an integer scalar, got {other:?}"
                )))
            }
        };

        // Second and millisecond timestamps (from_unixtime and friends): add in
        // microseconds, then cast back so the result keeps the input type.
        use arrow::datatypes::TimeUnit as AU;
        if let DataType::Timestamp(u @ (AU::Second | AU::Millisecond), tz) =
            args.args[2].data_type()
        {
            let wide = DataType::Timestamp(AU::Microsecond, tz.clone());
            let mut inner = args;
            inner.args[2] = inner.args[2].cast_to(&wide, None)?;
            let out = self.invoke_with_args(inner)?;
            return out.cast_to(&DataType::Timestamp(u, tz), None);
        }

        // Compute the epoch baseline once. `from_ymd_opt(1970, 1, 1)` is
        // guaranteed to be `Some`; treat any failure as a hard error rather
        // than panicking via .unwrap(). Issue #78.
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| DataFusionError::Plan("epoch literal invalid".into()))?;

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
                                out.push(
                                    Some(new_d.signed_duration_since(epoch).num_days() as i32),
                                );
                            }
                        }
                    }
                    let result: Date32Array = out.into_iter().collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                } else if let Some(ts_arr) =
                    array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                {
                    let mut out: Vec<Option<i64>> = Vec::with_capacity(ts_arr.len());
                    for opt in ts_arr.iter() {
                        match opt {
                            None => out.push(None),
                            Some(us) => out.push(Some(ts_add_us(us, unit_str, amount)?)),
                        }
                    }
                    let result: TimestampMicrosecondArray = out.into_iter().collect();
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                } else if let Some(ts_arr) =
                    array.as_any().downcast_ref::<TimestampNanosecondArray>()
                {
                    let tz = match ts_arr.data_type() {
                        DataType::Timestamp(_, tz) => tz.clone(),
                        _ => None,
                    };
                    let mut out: Vec<Option<i64>> = Vec::with_capacity(ts_arr.len());
                    for opt in ts_arr.iter() {
                        match opt {
                            None => out.push(None),
                            Some(ns) => out.push(Some(ts_add_ns(ns, unit_str, amount)?)),
                        }
                    }
                    let result: TimestampNanosecondArray = out
                        .into_iter()
                        .collect::<TimestampNanosecondArray>()
                        .with_timezone_opt(tz);
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                } else {
                    Err(DataFusionError::Plan(format!(
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
                ScalarValue::TimestampMicrosecond(None, tz) => Ok(ColumnarValue::Scalar(
                    ScalarValue::TimestampMicrosecond(None, tz.clone()),
                )),
                ScalarValue::TimestampNanosecond(Some(ns), tz) => {
                    let new_ns = ts_add_ns(*ns, unit_str, amount)?;
                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                        Some(new_ns),
                        tz.clone(),
                    )))
                }
                ScalarValue::TimestampNanosecond(None, tz) => Ok(ColumnarValue::Scalar(
                    ScalarValue::TimestampNanosecond(None, tz.clone()),
                )),
                other => Err(DataFusionError::Plan(format!(
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
        use crate::helpers::{arg, lift3, StrArg, TsArg};
        const EXPECTED: &str = "varchar, timestamp, timestamp";

        // Any scalar/array mix is accepted: `date_diff('day', col, CURRENT_DATE)`
        // used to fail with "mixed scalar/array args not supported".
        let unit: StrArg = arg("date_diff", EXPECTED, &args.args, 0)?;
        let t1: TsArg = arg("date_diff", EXPECTED, &args.args, 1)?;
        let t2: TsArg = arg("date_diff", EXPECTED, &args.args, 2)?;

        // Parse the unit once when it is the usual literal.
        let const_unit = match &args.args[0] {
            ColumnarValue::Scalar(sv) => match sv.try_as_str() {
                Some(Some(s)) => Some(parse_unit(s)?),
                _ => None,
            },
            ColumnarValue::Array(_) => None,
        };

        /// Whole units between two UTC-microsecond instants. Year and month
        /// are calendar differences; the sub-day units are exact so that
        /// `date_diff('hour', ts1, ts2)` no longer rounds to whole days.
        fn compute_diff(unit: TimeUnit, us1: i64, us2: i64) -> Option<i64> {
            match unit {
                TimeUnit::Year | TimeUnit::Month => {
                    let d1 = chrono::DateTime::from_timestamp_micros(us1)?.date_naive();
                    let d2 = chrono::DateTime::from_timestamp_micros(us2)?.date_naive();
                    let years = (d2.year() - d1.year()) as i64;
                    Some(match unit {
                        TimeUnit::Year => years,
                        _ => years * 12 + (d2.month() as i64 - d1.month() as i64),
                    })
                }
                TimeUnit::Day => us2.checked_sub(us1).map(|d| d / 86_400_000_000),
                TimeUnit::Hour => us2.checked_sub(us1).map(|d| d / 3_600_000_000),
                TimeUnit::Minute => us2.checked_sub(us1).map(|d| d / 60_000_000),
                TimeUnit::Second => us2.checked_sub(us1).map(|d| d / 1_000_000),
            }
        }

        lift3(&unit, &t1, &t2, |u, a, b| {
            let unit = match const_unit {
                Some(unit) => unit,
                None => parse_unit(u)?,
            };
            Ok(compute_diff(unit, a, b))
        })
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
                DataFusionError::Plan(format!("{fn_name}(): condition must be boolean"))
            })?
            .clone(),
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => BooleanArray::from(vec![*b; nrows]),
        ColumnarValue::Scalar(ScalarValue::Boolean(None)) => BooleanArray::from(vec![false; nrows]),
        other => {
            return Err(DataFusionError::Plan(format!(
                "{fn_name}(): unsupported condition type {other:?}"
            )))
        }
    };

    let to_array = |cv: &ColumnarValue, n: usize| -> DFResult<ArrayRef> {
        match cv {
            ColumnarValue::Array(a) => Ok(Arc::clone(a)),
            ColumnarValue::Scalar(sv) => sv.to_array_of_size(n).map_err(|e| {
                DataFusionError::Plan(format!("{fn_name}(): scalar expansion failed: {e}"))
            }),
        }
    };

    let then_arr = to_array(then_val, nrows)?;
    let else_arr = to_array(else_val, nrows)?;

    let result = zip(&mask, &then_arr, &else_arr)
        .map_err(|e| DataFusionError::Plan(format!("{fn_name}(): zip failed: {e}")))?;

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
            .ok_or_else(|| DataFusionError::Plan("if() needs 3 args".to_string()))
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
            .ok_or_else(|| DataFusionError::Plan("iff() needs 3 args".to_string()))
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

        // The declared Trino type name, as DESCRIBE reports it (#867). A
        // variant is struct storage tagged on the field; most expressions
        // drop the tag, so the mapper goes by the shape.
        let data_type = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.data_type().clone(),
            ColumnarValue::Scalar(sv) => sv.data_type(),
        };
        // JSON text from json_parse and friends is Utf8 with a marker (#848).
        let type_name = if args
            .arg_fields
            .first()
            .is_some_and(|f| crate::json_cast::is_json_text(f))
        {
            "json".to_string()
        } else {
            encode::arrow_to_declared_trino_type(&data_type)
        };

        // Return a scalar string — same value for every row.
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(type_name))))
    }
}

// ─── now() → Timestamp ─────────────────────────────────────────────────────

/// Fold a clock function to the query start instant, as DataFusion's own
/// `now()` does, so `now()`, `current_timestamp`, `localtimestamp` and
/// `localtime` in one statement read one instant (Trino: the query start).
/// The fold has to happen here: the constant evaluator runs before
/// `simplify` and would otherwise invoke the function and read the clock
/// once per call. So `invoke_with_args` refuses, as DataFusion's `now()`
/// does; that failure makes the evaluator keep the call for this fold.
fn query_start_literal(
    args: Vec<Expr>,
    info: &SimplifyContext,
    value: impl FnOnce(chrono::DateTime<chrono::Utc>) -> ScalarValue,
) -> ExprSimplifyResult {
    match info.query_execution_start_time() {
        Some(start) => ExprSimplifyResult::Simplified(Expr::Literal(value(start), None)),
        None => ExprSimplifyResult::Original(args),
    }
}

fn not_folded(name: &str) -> DFResult<ColumnarValue> {
    Err(DataFusionError::Internal(format!(
        "{name}() should have been folded to the query start time"
    )))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoNow;

impl ScalarUDFImpl for TrinoNow {
    fn name(&self) -> &str {
        "now"
    }

    // As in Trino, `current_timestamp` is the same function. It must be
    // claimed here: DataFusion's `now` holds it as an alias, and while it
    // held any key a session rebuild could hand it `now` back.
    fn aliases(&self) -> &[String] {
        static ALIASES: std::sync::LazyLock<Vec<String>> =
            std::sync::LazyLock::new(|| vec!["current_timestamp".to_string()]);
        &ALIASES
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::new(TypeSignature::Nullary, Volatility::Stable));
        &SIG
    }

    // Trino: `timestamp(3) with time zone` in the session zone, which is UTC.
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Millisecond,
            Some("UTC".into()),
        ))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DFResult<ExprSimplifyResult> {
        Ok(query_start_literal(args, info, |start| {
            ScalarValue::TimestampMillisecond(Some(start.timestamp_millis()), Some("UTC".into()))
        }))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_folded(self.name())
    }
}

// ─── json_object(k1, v1, k2, v2, ...) → Utf8 ──────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObject;

impl ScalarUDFImpl for JsonObject {
    fn name(&self) -> &str {
        "json_object"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::variadic_any(Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use crate::helpers::{arg, AnyArg, RowArg, StrArg};
        use datafusion::common::ScalarValue;

        // `is_multiple_of` would be clearer but is stable only from 1.87;
        // workspace MSRV is 1.85.
        if !args.args.len().is_multiple_of(2) {
            return Err(DataFusionError::Plan(
                "json_object: must have an even number of arguments (key-value pairs)".to_string(),
            ));
        }

        // Keys are strings, values are anything JSON can hold. Either side may
        // be a column: `json_object('id', id_col)` used to fail with
        // "unsupported value type Array".
        let mut keys: Vec<StrArg> = Vec::with_capacity(args.args.len() / 2);
        let mut values: Vec<AnyArg> = Vec::with_capacity(args.args.len() / 2);
        for i in 0..args.args.len() / 2 {
            keys.push(arg("json_object", "varchar, any, ...", &args.args, 2 * i)?);
            values.push(arg(
                "json_object",
                "varchar, any, ...",
                &args.args,
                2 * i + 1,
            )?);
        }
        debug_assert_eq!(keys.len(), values.len());

        fn json_value(sv: &ScalarValue) -> DFResult<serde_json::Value> {
            Ok(match sv {
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)) => serde_json::Value::String(s.clone()),
                ScalarValue::Int8(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::Int16(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::Int32(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::Int64(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::UInt8(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::UInt16(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::UInt32(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::UInt64(Some(v)) => serde_json::Value::from(*v),
                ScalarValue::Float32(Some(v)) => serde_json::json!(*v),
                ScalarValue::Float64(Some(v)) => serde_json::json!(*v),
                ScalarValue::Boolean(Some(v)) => serde_json::Value::Bool(*v),
                other if other.is_null() => serde_json::Value::Null,
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "json_object: unsupported value type {}",
                        other.data_type()
                    )))
                }
            })
        }

        let n_rows = keys
            .iter()
            .map(RowArg::array_len)
            .chain(values.iter().map(RowArg::array_len))
            .flatten()
            .max();

        let render = |i: usize| -> DFResult<Option<String>> {
            let mut map = serde_json::Map::new();
            for (k, v) in keys.iter().zip(values.iter()) {
                // A NULL key drops the pair (Trino rejects NULL keys; this is
                // the lenient reading), a NULL value is JSON null.
                let Some(key) = k.get(i) else { continue };
                let value = match v.get(i) {
                    Some(sv) => json_value(&sv)?,
                    None => serde_json::Value::Null,
                };
                map.insert(key.to_string(), value);
            }
            Ok(Some(serde_json::Value::Object(map).to_string()))
        };

        match n_rows {
            None => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(render(0)?))),
            Some(n) => {
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    out.push(render(i)?);
                }
                Ok(ColumnarValue::Array(
                    Arc::new(StringArray::from(out)) as ArrayRef
                ))
            }
        }
    }
}

// ─── json_format(json_value) → Utf8 (identity on varchar) ──────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonFormat;

impl ScalarUDFImpl for JsonFormat {
    fn name(&self) -> &str {
        "json_format"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        args.args = crate::helpers::utf8_string_args(std::mem::take(&mut args.args))?;

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
                    Ok(ColumnarValue::Array(Arc::new(arr.clone()) as ArrayRef))
                } else {
                    Err(DataFusionError::Plan(format!(
                        "json_format: expected string input, got {:?}",
                        array.data_type()
                    )))
                }
            }
            other => Err(DataFusionError::Plan(format!(
                "json_format: unsupported input {other:?}"
            ))),
        }
    }
}

// ─── localtime → alias for CURRENT_TIME (returns Time64) ───────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct LocalTime;

impl ScalarUDFImpl for LocalTime {
    fn name(&self) -> &str {
        "localtime"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::new(TypeSignature::Nullary, Volatility::Stable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        // Time-of-day, no timezone, microsecond precision. Matches Iceberg's
        // `time` primitive and the type that EXTRACT(HOUR|MINUTE|SECOND ...)
        // bridges accept.
        Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DFResult<ExprSimplifyResult> {
        Ok(query_start_literal(args, info, |start| {
            ScalarValue::Time64Microsecond(Some(micros_since_midnight(
                start.with_timezone(&chrono::Local).time(),
            )))
        }))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_folded(self.name())
    }
}

fn micros_since_midnight(t: chrono::NaiveTime) -> i64 {
    t.num_seconds_from_midnight() as i64 * 1_000_000 + (t.nanosecond() / 1_000) as i64
}

// ─── e() → Euler's number (Trino has it as a Nullary fn) ──────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoE;

impl ScalarUDFImpl for TrinoE {
    fn name(&self) -> &str {
        "e"
    }

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
        .map_err(|_| DataFusionError::Plan(format!("{fn_name}: arity assertion failed")))
}

// ─── codepoint(s) → Unicode code point of a single-char string ─────────────

/// Trino `split(string, delimiter)` and `split(string, delimiter, limit)`.
///
/// Splits on the whole delimiter; an empty delimiter splits into characters.
/// With `limit`, at most `limit` elements come back and the last one holds
/// the rest of the string, unsplit. `limit` must be positive. A NULL in any
/// argument yields NULL. An empty string splits to `['']`, as in Trino.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoSplit;

impl TrinoSplit {
    fn pieces(text: &str, delimiter: &str, limit: Option<i64>) -> DFResult<Vec<String>> {
        let limit = match limit {
            None => None,
            Some(n) if n > 0 => Some(n as usize),
            Some(n) => {
                return Err(DataFusionError::Execution(format!(
                    "split: limit must be positive, got {n}"
                )))
            }
        };
        if delimiter.is_empty() {
            let mut out: Vec<String> = Vec::new();
            let mut chars = text.chars();
            loop {
                if let Some(max) = limit {
                    if out.len() + 1 == max {
                        out.push(chars.as_str().to_string());
                        break;
                    }
                }
                match chars.next() {
                    Some(c) => out.push(c.to_string()),
                    None => break,
                }
            }
            if out.is_empty() {
                out.push(String::new());
            }
            return Ok(out);
        }
        Ok(match limit {
            Some(max) => text.splitn(max, delimiter).map(str::to_string).collect(),
            None => text.split(delimiter).map(str::to_string).collect(),
        })
    }
}

impl ScalarUDFImpl for TrinoSplit {
    fn name(&self) -> &str {
        "split"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            )
        });
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow::array::{ListBuilder, StringBuilder};
        let rows = args.number_rows;
        let as_utf8 = |value: &ColumnarValue, what: &str| -> DFResult<StringArray> {
            let array = value.to_array(rows)?;
            let array = arrow::compute::cast(&array, &DataType::Utf8).map_err(|e| {
                DataFusionError::Plan(format!("split: {what} must be a string: {e}"))
            })?;
            Ok(array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("cast to Utf8")
                .clone())
        };
        let texts = as_utf8(&args.args[0], "input")?;
        let delimiters = as_utf8(&args.args[1], "delimiter")?;
        let limits = match args.args.get(2) {
            None => None,
            Some(value) => {
                let array = value.to_array(rows)?;
                let array = arrow::compute::cast(&array, &DataType::Int64).map_err(|e| {
                    DataFusionError::Plan(format!("split: limit must be an integer: {e}"))
                })?;
                Some(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("cast to Int64")
                        .clone(),
                )
            }
        };

        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..rows {
            let limit_is_null = limits.as_ref().is_some_and(|l| l.is_null(row));
            if texts.is_null(row) || delimiters.is_null(row) || limit_is_null {
                builder.append_null();
                continue;
            }
            let limit = limits.as_ref().map(|l| l.value(row));
            for piece in Self::pieces(texts.value(row), delimiters.value(row), limit)? {
                builder.values().append_value(piece);
            }
            builder.append(true);
        }
        let list = builder.finish();
        match args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_))) {
            true => Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::try_from_array(&list, 0)?,
            )),
            false => Ok(ColumnarValue::Array(Arc::new(list) as ArrayRef)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoCodepoint;

impl ScalarUDFImpl for TrinoCodepoint {
    fn name(&self) -> &str {
        "codepoint"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        // Trino returns INTEGER. We return Int32 to match.
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use datafusion::common::ScalarValue;
        args.args = crate::helpers::utf8_string_args(std::mem::take(&mut args.args))?;
        let [arg] = take_function_args::<1>("codepoint", &args.args)?;
        let one = |s: &str| -> DFResult<i32> {
            let mut chars = s.chars();
            let c = chars.next().ok_or_else(|| {
                DataFusionError::Execution("codepoint: empty string has no code point".into())
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
            ColumnarValue::Scalar(ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)))
            }
            ColumnarValue::Array(arr) => {
                let s = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Plan("codepoint: input must be Utf8 / VARCHAR".into())
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
    fn name(&self) -> &str {
        "localtimestamp"
    }

    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::new(TypeSignature::Nullary, Volatility::Stable));
        &SIG
    }

    // Trino: `timestamp(3)` (no zone).
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Millisecond,
            None,
        ))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> DFResult<ExprSimplifyResult> {
        Ok(query_start_literal(args, info, |start| {
            ScalarValue::TimestampMillisecond(Some(start.timestamp_millis()), None)
        }))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        not_folded(self.name())
    }
}

// ---------------------------------------------------------------------------
// Shared string-transform helpers
// ---------------------------------------------------------------------------

use crate::helpers::{str_transform, str_transform_2};

// url_encode / url_decode

fn percent_decode(input: &[u8]) -> String {
    let mut result = Vec::new();
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%' && i + 2 < input.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&input[i + 1..i + 3]).unwrap_or(""), 16)
            {
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
        str_transform("url_encode", &args.args[0], |s| {
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
        str_transform("url_decode", &args.args[0], |s| {
            Some(percent_decode(s.as_bytes()))
        })
    }
}

// The base64 / hex / utf8 encoding functions live in `binary_fns` (Trino
// `varbinary` in and out).

// ---------------------------------------------------------------------------
// Trino JSON aliases — thin wrappers backed by serde_json
// ---------------------------------------------------------------------------

/// The value a Trino JSON path names (`$.items[0].name`); see
/// [`crate::json_path`].
pub(crate) fn navigate_json<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    crate::json_path::navigate(value, path)
}

fn extract_json_value(json: &str, path: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let result = navigate_json(&v, path)?;
    Some(result.to_string())
}

fn extract_json_scalar(json: &str, path: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let result = navigate_json(&v, path)?;
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
    fn return_field_from_args(&self, _: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(crate::json_cast::json_text_field(self.name()))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        str_transform_2("json_extract", &args.args, |json, path| {
            extract_json_value(json, path)
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
        str_transform_2("json_extract_scalar", &args.args, |json, path| {
            extract_json_scalar(json, path)
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
                    .ok_or_else(|| DataFusionError::Plan("Expected string array".into()))?;
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
    fn return_field_from_args(&self, _: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(crate::json_cast::json_text_field(self.name()))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // json_parse validates and normalises the JSON string to Trino's
        // canonical form: compact, object keys sorted. serde_json keeps
        // insertion order here (`preserve_order` is on workspace-wide).
        str_transform("json_parse", &args.args[0], |s| {
            Some(
                serde_json::from_str::<serde_json::Value>(s)
                    .map(|mut v| {
                        v.sort_all_objects();
                        v.to_string()
                    })
                    .unwrap_or_else(|_| "null".to_string()),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};

    /// Helper: run SQL returning an Int64 result (date extraction functions now return Int64).
    async fn run_query(sql: &str) -> i64 {
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        col.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
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
    async fn day_of_week_iso() {
        // Trino uses ISO day-of-week: Monday=1 .. Sunday=7 (#361).
        // 2026-03-30 is a Monday.
        assert_eq!(run_query("SELECT day_of_week(DATE '2026-03-30')").await, 1);
        // 2026-04-01 is a Wednesday.
        assert_eq!(run_query("SELECT day_of_week(DATE '2026-04-01')").await, 3);
        // 2024-01-07 is a Sunday. The Postgres `dow` convention returns 0 here;
        // Trino/ISO returns 7. This case is the regression guard for #361.
        assert_eq!(run_query("SELECT day_of_week(DATE '2024-01-07')").await, 7);
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
        let ctx = crate::duckdb_test_ctx();
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
        let ctx = crate::duckdb_test_ctx();
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

    #[tokio::test]
    async fn date_add_on_nanosecond_timestamp() {
        // `CAST(... AS timestamp)` produces Timestamp(Nanosecond),
        // DataFusion's default. date_add previously only handled
        // TimestampMicrosecond and fell through to an INTERNAL_ERROR
        // ("unsupported scalar type TimestampNanosecond") -- which reads to a
        // BI client as an engine bug on one of the most common dashboard
        // functions. day(date_add('day', 1, 2026-01-15 12:00:00)) == 16.
        assert_eq!(
            run_query("SELECT day(date_add('day', 1, CAST('2026-01-15 12:00:00' AS timestamp)))")
                .await,
            16,
        );
    }

    #[tokio::test]
    async fn date_add_hour_on_nanosecond_timestamp() {
        // Sub-day units must work on nanosecond timestamps too.
        assert_eq!(
            run_query("SELECT hour(date_add('hour', 3, CAST('2026-01-15 12:00:00' AS timestamp)))")
                .await,
            15,
        );
    }

    // ── date_diff tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn date_diff_on_nanosecond_timestamp() {
        // Same nanosecond-timestamp gap as date_add. date_diff('day', ...)
        // over two CAST-as-timestamp values must return the day delta, not
        // an INTERNAL_ERROR.
        let v = run_query_i64(
            "SELECT date_diff('day', CAST('2026-01-01 00:00:00' AS timestamp), \
             CAST('2026-01-06 00:00:00' AS timestamp))",
        )
        .await;
        assert_eq!(v, 5);
    }

    #[tokio::test]
    async fn date_diff_days() {
        let v =
            run_query_i64("SELECT date_diff('day', DATE '2026-01-01', DATE '2026-01-06')").await;
        assert_eq!(v, 5);
    }

    #[tokio::test]
    async fn date_diff_months() {
        let v =
            run_query_i64("SELECT date_diff('month', DATE '2026-01-01', DATE '2026-04-01')").await;
        assert_eq!(v, 3);
    }

    #[tokio::test]
    async fn date_diff_years() {
        let v =
            run_query_i64("SELECT date_diff('year', DATE '2020-01-01', DATE '2026-01-01')").await;
        assert_eq!(v, 6);
    }

    // ── from_unixtime tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn from_unixtime_produces_timestamp() {
        // epoch 0 → timestamp 1970-01-01 00:00:00; year() should return 1970
        assert_eq!(run_query("SELECT year(from_unixtime(0))").await, 1970,);
    }

    /// A decimal argument used to come back NULL: SQE parses `1700000000.5`
    /// as DECIMAL and the UDF had no arm for it. Every numeric type now
    /// reaches the same DOUBLE path, scalar and column alike.
    #[tokio::test]
    async fn from_unixtime_accepts_decimal_integer_and_double_input() {
        use datafusion::prelude::{SessionConfig, SessionContext};
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().set_bool("datafusion.sql_parser.parse_float_as_decimal", true),
        );
        register_trino_functions(&ctx);
        let sql = "SELECT CAST(from_unixtime(v) AS VARCHAR) FROM (VALUES \
                   (1700000000.5), (CAST(1700000000 AS DECIMAL(11, 1)))) AS t(v) \
                   UNION ALL SELECT CAST(from_unixtime(1700000000.5) AS VARCHAR) \
                   UNION ALL SELECT CAST(from_unixtime(CAST(1700000000 AS INT)) AS VARCHAR) \
                   UNION ALL SELECT CAST(from_unixtime(CAST(1700000000.25 AS DOUBLE)) AS VARCHAR) \
                   UNION ALL SELECT CAST(from_unixtime(CAST(NULL AS BIGINT)) AS VARCHAR)";
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut got: Vec<Option<String>> = batches
            .iter()
            .flat_map(|b| {
                // VARCHAR casts come back as Utf8View; read them as Utf8.
                let utf8 = arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
                let col = utf8.as_any().downcast_ref::<StringArray>().unwrap();
                (0..col.len())
                    .map(|i| (!col.is_null(i)).then(|| col.value(i).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                None,
                // timestamp(3) with time zone: millisecond precision, UTC.
                Some("2023-11-14T22:13:20.250Z".to_string()),
                Some("2023-11-14T22:13:20.500Z".to_string()),
                Some("2023-11-14T22:13:20.500Z".to_string()),
                Some("2023-11-14T22:13:20Z".to_string()),
                Some("2023-11-14T22:13:20Z".to_string()),
            ]
        );
    }

    // ── to_unixtime tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn to_unixtime_produces_epoch_seconds() {
        // DATE '1970-01-01' → 0.0
        let ctx = crate::duckdb_test_ctx();
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
    async fn to_unixtime_out_of_range_date_array_is_null() {
        // A Date32 beyond chrono's range used to panic in the array branch;
        // i32::MAX days is a finite double now, and nothing may panic.
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batch = arrow::record_batch::RecordBatch::try_from_iter([(
            "d",
            Arc::new(Date32Array::from(vec![Some(i32::MAX), Some(0)])) as ArrayRef,
        )])
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let batches = ctx
            .sql("SELECT to_unixtime(d) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(col.value(0), i32::MAX as f64 * 86400.0);
        assert_eq!(col.value(1), 0.0);
    }

    #[tokio::test]
    async fn to_unixtime_roundtrip() {
        // from_unixtime(to_unixtime(ts)) should give back year 2026
        assert_eq!(
            run_query("SELECT year(from_unixtime(to_unixtime(TIMESTAMP '2026-03-30 12:00:00')))")
                .await,
            2026,
        );
    }

    // ── typeof tests ──────────────────────────────────────────────────────────

    /// `typeof` returns the Trino type name, the one a client sees in the
    /// result's column metadata (#867). An integer literal is a bigint in
    /// SQE, an integer in Trino.
    #[tokio::test]
    async fn typeof_returns_trino_type_names() {
        for (expr, want) in [
            ("42", "bigint"),
            ("CAST(1 AS INTEGER)", "integer"),
            ("CAST(1 AS SMALLINT)", "smallint"),
            ("CAST(1 AS DOUBLE)", "double"),
            ("CAST(1 AS REAL)", "real"),
            ("CAST(1.5 AS DECIMAL(5,2))", "decimal(5,2)"),
            ("'hello'", "varchar"),
            ("true", "boolean"),
            ("DATE '2024-01-01'", "date"),
            // No front-door rewrite runs in this test, so DataFusion's own
            // nanosecond typing of the literal shows: the label is the unit's.
            ("TIMESTAMP '2024-01-01 00:00:00'", "timestamp(9)"),
            (
                "CAST('2024-01-01 00:00:00' AS TIMESTAMP(0))",
                "timestamp(0)",
            ),
            (
                "CAST('2024-01-01 00:00:00' AS TIMESTAMP(3))",
                "timestamp(3)",
            ),
            (
                "CAST('2024-01-01 00:00:00' AS TIMESTAMP(6))",
                "timestamp(6)",
            ),
            ("make_array(1, 2)", "array(bigint)"),
            ("NULL", "unknown"),
        ] {
            let s = run_query_string(&format!("SELECT typeof({expr})")).await;
            assert_eq!(s, want, "typeof({expr})");
        }
    }

    // ── if() tests ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn trino_if_true_branch() {
        // if(1 = 1, 10, 20) → 10
        let ctx = crate::duckdb_test_ctx();
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
        let v = col.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(v, 10);
    }

    #[tokio::test]
    async fn trino_if_false_branch() {
        // if(1 = 2, 10, 20) → 20
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT if(1 = 2, 10, 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(v, 20);
    }

    // ── iff() tests (Snowflake alias of if) ─────────────────────────────────

    #[tokio::test]
    async fn snowflake_iff_true_branch() {
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(TRUE, 'yes', 'no')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col.as_any().downcast_ref::<StringArray>().unwrap().value(0);
        assert_eq!(v, "yes");
    }

    #[tokio::test]
    async fn snowflake_iff_false_branch() {
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(FALSE, 'yes', 'no')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col.as_any().downcast_ref::<StringArray>().unwrap().value(0);
        assert_eq!(v, "no");
    }

    #[tokio::test]
    async fn snowflake_iff_null_condition_returns_else() {
        // Snowflake spec: NULL condition is treated as false, returns expr2.
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(CAST(NULL AS BOOLEAN), 'yes', 'no')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col.as_any().downcast_ref::<StringArray>().unwrap().value(0);
        assert_eq!(v, "no");
    }

    #[tokio::test]
    async fn snowflake_iff_with_predicate() {
        // iff(1 = 1, 10, 20) -> 10 — same shape as the if() test for parity.
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batches = ctx
            .sql("SELECT iff(1 = 1, 10, 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0].column(0);
        let v = col.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(v, 10);
    }

    // ── date_format tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn date_format_iso_date() {
        let s = run_query_string("SELECT date_format(TIMESTAMP '2024-01-15 10:30:45', '%Y-%m-%d')")
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
        let s = run_query_string("SELECT date_format(DATE '2024-06-15', '%Y/%m/%d')").await;
        assert_eq!(s, "2024/06/15");
    }

    #[tokio::test]
    async fn date_format_unrenderable_pattern_errors() {
        // A trailing '%' and the unsupported week specifiers error rather
        // than panic (unknown letters such as %z/%Q render as the letter).
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        for pat in ["%Y %", "%U"] {
            let sql = format!("SELECT date_format(TIMESTAMP '2024-01-15 10:30:45', '{pat}')");
            let res = match ctx.sql(&sql).await {
                Ok(df) => df.collect().await.map(|_| ()),
                Err(e) => Err(e),
            };
            assert!(res.is_err(), "{pat} should error");
        }
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

    // Trino fills fields the format does not name with their lowest value:
    // `date_parse('202601', '%Y%m')` is 2026-01-01 00:00:00. An Athena view
    // over a `YYYYMM` period column depends on it (#667 follow-up).
    #[tokio::test]
    async fn date_parse_fills_missing_fields_like_trino() {
        assert_eq!(
            run_query("SELECT month(date_parse('202603', '%Y%m'))").await,
            3,
        );
        assert_eq!(run_query("SELECT day(date_parse('202603', '%Y%m'))").await, 1);
        assert_eq!(run_query("SELECT hour(date_parse('202603', '%Y%m'))").await, 0);
        assert_eq!(run_query("SELECT month(date_parse('2026', '%Y'))").await, 1);
        assert_eq!(
            run_query("SELECT hour(date_parse('2026-03-05 07', '%Y-%m-%d %H'))").await,
            7,
        );
    }

    /// Helper: run SQL returning a list of strings from a `split(...)` call.
    async fn run_split(sql: &str) -> Option<Vec<Option<String>>> {
        use arrow::array::{Array, ListArray};
        let ctx = crate::duckdb_test_ctx();
        register_trino_functions(&ctx);
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        let list = col.as_any().downcast_ref::<ListArray>().unwrap();
        if list.is_null(0) {
            return None;
        }
        let values = list.value(0);
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        Some(values.iter().map(|v| v.map(str::to_string)).collect())
    }

    fn strs(values: &[&str]) -> Option<Vec<Option<String>>> {
        Some(values.iter().map(|v| Some(v.to_string())).collect())
    }

    #[tokio::test]
    async fn split_matches_trino_with_and_without_limit() {
        assert_eq!(run_split("SELECT split('a,b,c', ',')").await, strs(&["a", "b", "c"]));
        // The last element holds the rest, unsplit.
        assert_eq!(run_split("SELECT split('a,b,c', ',', 2)").await, strs(&["a", "b,c"]));
        assert_eq!(run_split("SELECT split('a,b,c', ',', 5)").await, strs(&["a", "b", "c"]));
        // Multi-character delimiter, and a delimiter that is absent.
        assert_eq!(run_split("SELECT split('a::b::c', '::')").await, strs(&["a", "b", "c"]));
        assert_eq!(run_split("SELECT split('abc', ',')").await, strs(&["abc"]));
        // Trino: an empty string splits to one empty element; an empty
        // delimiter splits into characters.
        assert_eq!(run_split("SELECT split('', ',')").await, strs(&[""]));
        assert_eq!(run_split("SELECT split('abc', '')").await, strs(&["a", "b", "c"]));
        assert_eq!(run_split("SELECT split('abc', '', 2)").await, strs(&["a", "bc"]));
        // NULL in, NULL out.
        assert_eq!(run_split("SELECT split(CAST(NULL AS VARCHAR), ',')").await, None);
        // Over a column, the array form.
        assert_eq!(
            run_split("SELECT split(s, '-', 2) FROM (VALUES ('x-y-z')) AS t(s)").await,
            strs(&["x", "y-z"])
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
        let s = run_query_string("SELECT json_object('name', 'Alice', 'city', 'Amsterdam')").await;
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed["name"], "Alice");
        assert_eq!(parsed["city"], "Amsterdam");
    }

    #[tokio::test]
    async fn json_object_mixed_types() {
        let s = run_query_string("SELECT json_object('name', 'Bob', 'age', 30)").await;
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed["name"], "Bob");
        assert_eq!(parsed["age"], 30);
    }

    // ── json_format tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn json_format_passthrough() {
        let s = run_query_string(r#"SELECT json_format('{"key":"value"}')"#).await;
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
        let ctx = crate::duckdb_test_ctx();
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
        let ctx = crate::duckdb_test_ctx();
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
        let ctx = crate::duckdb_test_ctx();
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
        let ctx = crate::duckdb_test_ctx();
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
        let ctx = crate::duckdb_test_ctx();
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
        let ts_arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>();
        let ts_ns = col.as_any().downcast_ref::<TimestampNanosecondArray>();
        // Just verify it ran without error; the result type may vary
        assert!(
            ts_arr.is_some() || ts_ns.is_some(),
            "date_trunc should return a timestamp"
        );
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
