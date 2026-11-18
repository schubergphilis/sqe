//! Trino date/time functions that need Trino's value *and* Trino's type.
//!
//! The older UDFs in `trino_functions.rs` returned microsecond timestamps for
//! everything. Trino 484 returns `timestamp(3)` (millisecond) for
//! `from_unixtime`, `date_parse` and friends, `timestamp(3) with time zone`
//! for the zoned ones, `timestamp(9)` for the `_nanos` variants and `date`
//! for `date_trunc(unit, date)`. The shared [`Dt`] struct below keeps each
//! function down to a return-type rule and an invoke function.
//!
//! The session zone of SQE is fixed to UTC, so every "session zone" result
//! is tagged `UTC`.

mod formats;
#[cfg(test)]
mod tests;

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, IntervalMonthDayNanoArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Field, IntervalMonthDayNano, IntervalMonthDayNanoType, IntervalUnit,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use crate::helpers::{
    arg, lift1, lift2, tag_timezone, unexpected_parameters, AnyArg, DateArg, DateDays, F64Arg,
    RowOut, StrArg, TsArg, TsMicros, Zone,
};
use formats::{
    joda_compile, joda_format, joda_parse, mysql_format, mysql_parse, offset_text, Local,
    ParsedZone, Piece,
};

/// The session zone. SQE sessions run in UTC.
const SESSION_TZ: &str = "UTC";

fn exec(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

fn plan(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(msg.into())
}

// ───────────────────────────────────────────────────────────────────
// Row output types for units the shared helpers do not cover
// ───────────────────────────────────────────────────────────────────

/// UTC epoch milliseconds, `Timestamp(Millisecond, None)` until retagged.
struct TsMs(i64);

impl RowOut for TsMs {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::TimestampMillisecond(v.map(|t| t.0), None)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(TimestampMillisecondArray::from_iter(
            vals.into_iter().map(|v| v.map(|t| t.0)),
        ))
    }
}

/// UTC epoch nanoseconds, `Timestamp(Nanosecond, None)` until retagged.
struct TsNs(i64);

impl RowOut for TsNs {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::TimestampNanosecond(v.map(|t| t.0), None)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from_iter(
            vals.into_iter().map(|v| v.map(|t| t.0)),
        ))
    }
}

impl RowOut for f64 {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::Float64(v)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(arrow::array::Float64Array::from(vals))
    }
}

struct Mdn(IntervalMonthDayNano);

impl RowOut for Mdn {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::IntervalMonthDayNano(v.map(|i| i.0))
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(IntervalMonthDayNanoArray::from_iter(
            vals.into_iter().map(|v| v.map(|i| i.0)),
        ))
    }
}

/// Set the display zone of a timestamp result of any unit. The stored epoch
/// values are untouched.
fn retag(cv: ColumnarValue, tz: Option<Arc<str>>) -> DFResult<ColumnarValue> {
    Ok(match cv {
        ColumnarValue::Scalar(sv) => ColumnarValue::Scalar(match sv {
            ScalarValue::TimestampSecond(v, _) => ScalarValue::TimestampSecond(v, tz),
            ScalarValue::TimestampMillisecond(v, _) => ScalarValue::TimestampMillisecond(v, tz),
            ScalarValue::TimestampMicrosecond(v, _) => ScalarValue::TimestampMicrosecond(v, tz),
            ScalarValue::TimestampNanosecond(v, _) => ScalarValue::TimestampNanosecond(v, tz),
            other => other,
        }),
        ColumnarValue::Array(a) => {
            let out: ArrayRef = match a.data_type() {
                DataType::Timestamp(TimeUnit::Second, _) => Arc::new(
                    a.as_primitive::<TimestampSecondType>()
                        .clone()
                        .with_timezone_opt(tz),
                ),
                DataType::Timestamp(TimeUnit::Millisecond, _) => Arc::new(
                    a.as_primitive::<TimestampMillisecondType>()
                        .clone()
                        .with_timezone_opt(tz),
                ),
                DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
                    a.as_primitive::<TimestampMicrosecondType>()
                        .clone()
                        .with_timezone_opt(tz),
                ),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Arc::new(
                    a.as_primitive::<TimestampNanosecondType>()
                        .clone()
                        .with_timezone_opt(tz),
                ),
                _ => a,
            };
            ColumnarValue::Array(out)
        }
    })
}

/// The zone the planner put on the result type.
fn out_tz(args: &ScalarFunctionArgs) -> Option<Arc<str>> {
    match args.return_field.data_type() {
        DataType::Timestamp(_, tz) => tz.clone(),
        _ => None,
    }
}

// ───────────────────────────────────────────────────────────────────
// The shared UDF shell
// ───────────────────────────────────────────────────────────────────

type RetFn = fn(&[DataType], &[Option<&ScalarValue>]) -> DFResult<DataType>;
type InvokeFn = fn(&ScalarFunctionArgs) -> DFResult<ColumnarValue>;

struct Dt {
    name: &'static str,
    aliases: Vec<String>,
    min: usize,
    max: usize,
    signature: Signature,
    ret: RetFn,
    invoke: InvokeFn,
}

impl std::fmt::Debug for Dt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dt").field("name", &self.name).finish()
    }
}

// Function pointers are not reliably comparable, and one name is one function.
impl PartialEq for Dt {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for Dt {}
impl Hash for Dt {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

fn dt(
    name: &'static str,
    aliases: &[&str],
    arity: (usize, usize),
    ret: RetFn,
    invoke: InvokeFn,
) -> ScalarUDF {
    ScalarUDF::from(Dt {
        name,
        aliases: aliases.iter().map(|a| a.to_string()).collect(),
        min: arity.0,
        max: arity.1,
        signature: Signature::variadic_any(Volatility::Immutable),
        ret,
        invoke,
    })
}

impl Dt {
    fn check_arity(&self, n: usize) -> DFResult<()> {
        if n < self.min || n > self.max {
            return Err(plan(format!(
                "Function {} expects {} to {} arguments, got {n}",
                self.name, self.min, self.max
            )));
        }
        Ok(())
    }
}

impl ScalarUDFImpl for Dt {
    fn name(&self) -> &str {
        self.name
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.check_arity(arg_types.len())?;
        (self.ret)(arg_types, &[])
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<Arc<Field>> {
        self.check_arity(args.arg_fields.len())?;
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let dt = (self.ret)(&types, args.scalar_arguments)?;
        Ok(Arc::new(Field::new(self.name, dt, true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        self.check_arity(args.args.len())?;
        (self.invoke)(&args)
    }
}

// ───────────────────────────────────────────────────────────────────
// Zones and local wall-clock times
// ───────────────────────────────────────────────────────────────────

fn zone_of(tz: Option<&str>) -> DFResult<Zone> {
    match tz {
        None => Ok(Zone::Named(chrono_tz::UTC)),
        Some(z) => Zone::parse(z),
    }
}

fn local_of(us: i64, tz: Option<&str>) -> DFResult<Local> {
    let zone = zone_of(tz)?;
    let utc = DateTime::from_timestamp_micros(us).ok_or_else(|| exec("timestamp out of range"))?;
    let (offset_secs, abbrev) = match &zone {
        Zone::Named(z) => {
            let off = z.offset_from_utc_datetime(&utc.naive_utc());
            (off.fix().local_minus_utc(), off.to_string())
        }
        Zone::Fixed(o) => (o.local_minus_utc(), offset_text(o.local_minus_utc(), true)),
    };
    Ok(Local {
        dt: zone.local_naive(utc),
        offset_secs,
        id: tz.unwrap_or(SESSION_TZ).to_string(),
        abbrev,
    })
}

fn epoch_days(d: NaiveDate) -> i32 {
    d.num_days_from_ce() - 719_163
}

fn date_from_days(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(days.checked_add(719_163)?)
}

/// A string literal argument, if the planner knows it.
fn lit_str(lit: Option<&&ScalarValue>) -> Option<String> {
    match lit.copied()? {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn lit_i64(lit: Option<&&ScalarValue>) -> Option<i64> {
    match lit.copied()?.cast_to(&DataType::Int64).ok()? {
        ScalarValue::Int64(v) => v,
        _ => None,
    }
}

fn ts_type(unit: TimeUnit, tz: Option<&str>) -> DataType {
    DataType::Timestamp(unit, tz.map(Arc::from))
}

// ───────────────────────────────────────────────────────────────────
// Intervals
// ───────────────────────────────────────────────────────────────────

/// `(months, total milliseconds of the day-time part)` per row, or `None`
/// when the argument is not an interval.
fn interval_rows(cv: &ColumnarValue) -> Option<Vec<Option<(i32, i64)>>> {
    fn mdn(v: IntervalMonthDayNano) -> (i32, i64) {
        (
            v.months,
            v.days as i64 * 86_400_000 + v.nanoseconds / 1_000_000,
        )
    }
    match cv {
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::IntervalMonthDayNano(v) => Some(vec![v.map(mdn)]),
            ScalarValue::IntervalDayTime(v) => {
                Some(vec![v.map(|v| {
                    (0, v.days as i64 * 86_400_000 + v.milliseconds as i64)
                })])
            }
            ScalarValue::IntervalYearMonth(v) => Some(vec![v.map(|m| (m, 0))]),
            _ => None,
        },
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let a = a.as_primitive::<IntervalMonthDayNanoType>();
                Some(a.iter().map(|v| v.map(mdn)).collect())
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let a = a.as_primitive::<arrow::datatypes::IntervalDayTimeType>();
                Some(
                    a.iter()
                        .map(|v| v.map(|v| (0, v.days as i64 * 86_400_000 + v.milliseconds as i64)))
                        .collect(),
                )
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let a = a.as_primitive::<arrow::datatypes::IntervalYearMonthType>();
                Some(a.iter().map(|v| v.map(|m| (m, 0))).collect())
            }
            _ => None,
        },
    }
}

fn interval_value(field: &str, months: i32, ms: i64) -> DFResult<i64> {
    Ok(match field {
        "day" => ms / 86_400_000,
        "hour" => ms / 3_600_000 % 24,
        "minute" => ms / 60_000 % 60,
        "second" => ms / 1000 % 60,
        "millisecond" => ms % 1000,
        "year" => i64::from(months / 12),
        "month" => i64::from(months % 12),
        "to_milliseconds" => {
            if months != 0 {
                return Err(plan(
                    "to_milliseconds is not defined for an interval with a year-month part",
                ));
            }
            ms
        }
        other => return Err(DataFusionError::Internal(format!("interval field {other}"))),
    })
}

/// Extract `field` from an interval argument as a bigint. `Ok(None)` when
/// the argument is not an interval.
pub(crate) fn interval_field(cv: &ColumnarValue, field: &str) -> DFResult<Option<ColumnarValue>> {
    let Some(rows) = interval_rows(cv) else {
        return Ok(None);
    };
    let vals = rows
        .into_iter()
        .map(|r| r.map(|(m, ms)| interval_value(field, m, ms)).transpose())
        .collect::<DFResult<Vec<Option<i64>>>>()?;
    Ok(Some(match cv {
        ColumnarValue::Scalar(_) => {
            ColumnarValue::Scalar(ScalarValue::Int64(vals.into_iter().next().flatten()))
        }
        ColumnarValue::Array(_) => {
            ColumnarValue::Array(Arc::new(arrow::array::Int64Array::from(vals)))
        }
    }))
}

// ───────────────────────────────────────────────────────────────────
// Extractors and zone offsets
// ───────────────────────────────────────────────────────────────────

fn ret_i64(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(DataType::Int64)
}

fn ret_utf8(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(DataType::Utf8)
}

fn ret_f64(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(DataType::Float64)
}

fn ret_date(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(DataType::Date32)
}

fn invoke_millisecond(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    if let Some(cv) = interval_field(&args.args[0], "millisecond")? {
        return Ok(cv);
    }
    let a: TsArg = arg("millisecond", "timestamp", &args.args, 0)?;
    lift1(&a, |us| Ok(Some(us.rem_euclid(1_000_000) / 1000)))
}

fn invoke_to_milliseconds(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    interval_field(&args.args[0], "to_milliseconds")?.ok_or_else(|| {
        unexpected_parameters("to_milliseconds", &args.args, "interval day to second")
    })
}

fn offset_of(name: &str, args: &ScalarFunctionArgs) -> DFResult<(TsArg, Option<Arc<str>>)> {
    let a: TsArg = arg(name, "timestamp with time zone", &args.args, 0)?;
    let tz = a.tz.clone();
    Ok((a, tz))
}

fn invoke_timezone_hour(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let (a, tz) = offset_of("timezone_hour", args)?;
    lift1(&a, |us| {
        Ok(Some(i64::from(
            local_of(us, tz.as_deref())?.offset_secs / 3600,
        )))
    })
}

fn invoke_timezone_minute(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let (a, tz) = offset_of("timezone_minute", args)?;
    lift1(&a, |us| {
        Ok(Some(i64::from(
            local_of(us, tz.as_deref())?.offset_secs % 3600 / 60,
        )))
    })
}

fn invoke_timezone(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let (a, tz) = offset_of("timezone", args)?;
    let Some(tz) = tz else {
        return Err(unexpected_parameters(
            "timezone",
            &args.args,
            "timestamp with time zone",
        ));
    };
    lift1(&a, |_| Ok(Some(tz.to_string())))
}

// ───────────────────────────────────────────────────────────────────
// from_unixtime / to_unixtime
// ───────────────────────────────────────────────────────────────────

fn ret_from_unixtime(_: &[DataType], lits: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    let tz = match lits.len() {
        0 | 1 => SESSION_TZ.to_string(),
        2 => {
            let z = lit_str(lits[1].as_ref())
                .ok_or_else(|| plan("from_unixtime: the time zone must be a constant string"))?;
            Zone::parse(&z)?;
            z
        }
        _ => {
            let (Some(h), Some(m)) = (lit_i64(lits[1].as_ref()), lit_i64(lits[2].as_ref())) else {
                return Err(plan("from_unixtime: hours and minutes must be constants"));
            };
            let total = h
                .checked_mul(60)
                .and_then(|v| v.checked_add(m))
                .filter(|t| t.abs() <= 18 * 60 + 59)
                .ok_or_else(|| exec("Invalid offset"))?;
            offset_text(total as i32 * 60, true)
        }
    };
    Ok(ts_type(TimeUnit::Millisecond, Some(&tz)))
}

fn invoke_from_unixtime(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let a: F64Arg = arg("from_unixtime", "double", &args.args, 0)?;
    let out = lift1(&a, |x| {
        let ms = (x * 1000.0 + 0.5).floor();
        if !ms.is_finite() || ms.abs() >= 9.0e15 {
            return Err(exec(format!("from_unixtime: {x} is out of range")));
        }
        Ok(Some(TsMs(ms as i64)))
    })?;
    retag(out, out_tz(args))
}

fn ret_from_unixtime_nanos(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(ts_type(TimeUnit::Nanosecond, Some(SESSION_TZ)))
}

fn scalar_to_nanos(sv: &ScalarValue) -> DFResult<i64> {
    let overflow = || exec("from_unixtime_nanos: value does not fit in timestamp(9)");
    match sv {
        ScalarValue::Int8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| overflow()),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            // Round half up to whole nanoseconds.
            let scale = *scale;
            if scale <= 0 {
                let f = 10i128
                    .checked_pow(u32::from(scale.unsigned_abs()))
                    .ok_or_else(overflow)?;
                return i64::try_from(v.checked_mul(f).ok_or_else(overflow)?).map_err(|_| overflow());
            }
            let f = 10i128.checked_pow(scale as u32).ok_or_else(overflow)?;
            let q = v.div_euclid(f);
            let r = v.rem_euclid(f);
            let q = if r * 2 >= f { q + 1 } else { q };
            i64::try_from(q).map_err(|_| overflow())
        }
        _ => Err(plan("Unexpected parameters for function from_unixtime_nanos. Expected: from_unixtime_nanos(bigint | decimal)")),
    }
}

fn invoke_from_unixtime_nanos(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let a: AnyArg = arg("from_unixtime_nanos", "bigint", &args.args, 0)?;
    let out = lift1(&a, |sv| Ok(Some(TsNs(scalar_to_nanos(&sv)?))))?;
    retag(out, out_tz(args))
}

fn invoke_to_unixtime(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    if matches!(
        args.args[0].data_type(),
        DataType::Date32 | DataType::Date64
    ) {
        let a: DateArg = arg("to_unixtime", "timestamp", &args.args, 0)?;
        return lift1(&a, |d| Ok(Some(f64::from(d) * 86_400.0)));
    }
    let a: TsArg = arg("to_unixtime", "timestamp", &args.args, 0)?;
    lift1(&a, |us| Ok(Some(us as f64 / 1e6)))
}

// ───────────────────────────────────────────────────────────────────
// ISO 8601
// ───────────────────────────────────────────────────────────────────

fn digits(b: &[u8], pos: &mut usize, n: usize) -> Option<u32> {
    let slice = b.get(*pos..*pos + n)?;
    if !slice.iter().all(u8::is_ascii_digit) {
        return None;
    }
    *pos += n;
    std::str::from_utf8(slice).ok()?.parse().ok()
}

/// Parse an ISO 8601 date or date-time. Returns the wall clock (nanosecond
/// precision) and the offset in seconds when the text names one.
fn parse_iso(s: &str) -> Option<(NaiveDateTime, Option<i32>)> {
    let b = s.as_bytes();
    let mut p = 0;
    let year = digits(b, &mut p, 4)? as i32;
    let (mut month, mut day) = (1, 1);
    match b.get(p) {
        Some(b'-') => {
            p += 1;
            month = digits(b, &mut p, 2)?;
            if b.get(p) == Some(&b'-') {
                p += 1;
                day = digits(b, &mut p, 2)?;
            }
        }
        Some(c) if c.is_ascii_digit() => {
            month = digits(b, &mut p, 2)?;
            day = digits(b, &mut p, 2)?;
        }
        _ => {}
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    if p == b.len() {
        return Some((date.and_time(NaiveTime::MIN), None));
    }
    if !matches!(b[p], b'T' | b't' | b' ') {
        return None;
    }
    p += 1;
    let hour = digits(b, &mut p, 2)?;
    let (mut minute, mut second, mut nanos) = (0, 0, 0u32);
    if b.get(p) == Some(&b':') {
        p += 1;
        minute = digits(b, &mut p, 2)?;
        if b.get(p) == Some(&b':') {
            p += 1;
            second = digits(b, &mut p, 2)?;
            if matches!(b.get(p), Some(b'.' | b',')) {
                p += 1;
                let start = p;
                while b.get(p).is_some_and(u8::is_ascii_digit) {
                    p += 1;
                }
                if p == start {
                    return None;
                }
                let frac = &s[start..p.min(start + 9)];
                nanos = format!("{frac:0<9}").parse().ok()?;
            }
        }
    }
    let offset = match b.get(p) {
        None => None,
        Some(b'Z' | b'z') => {
            p += 1;
            Some(0)
        }
        Some(&c @ (b'+' | b'-')) => {
            p += 1;
            let h = digits(b, &mut p, 2)? as i32;
            if b.get(p) == Some(&b':') {
                p += 1;
            }
            let m = if p < b.len() {
                digits(b, &mut p, 2)? as i32
            } else {
                0
            };
            if h > 23 || m > 59 {
                return None;
            }
            let secs = h * 3600 + m * 60;
            Some(if c == b'-' { -secs } else { secs })
        }
        Some(_) => return None,
    };
    if p != b.len() {
        return None;
    }
    let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)?;
    Some((date.and_time(time), offset))
}

fn ret_from_iso(unit: TimeUnit, lits: &[Option<&ScalarValue>]) -> DataType {
    let tz = lit_str(lits.first().and_then(|l| l.as_ref()))
        .and_then(|s| parse_iso(&s))
        .and_then(|(_, off)| off)
        .map(|o| {
            if o == 0 {
                SESSION_TZ.to_string()
            } else {
                offset_text(o, true)
            }
        });
    ts_type(unit, Some(tz.as_deref().unwrap_or(SESSION_TZ)))
}

fn ret_from_iso_ms(_: &[DataType], lits: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(ret_from_iso(TimeUnit::Millisecond, lits))
}

fn ret_from_iso_ns(_: &[DataType], lits: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(ret_from_iso(TimeUnit::Nanosecond, lits))
}

fn iso_to_utc_nanos(name: &str, s: &str) -> DFResult<i128> {
    let (naive, off) = parse_iso(s).ok_or_else(|| {
        exec(format!(
            "{name}: Invalid format: \"{s}\" is not a valid ISO 8601 timestamp"
        ))
    })?;
    let local = naive.and_utc();
    let nanos =
        i128::from(local.timestamp()) * 1_000_000_000 + i128::from(local.timestamp_subsec_nanos());
    Ok(nanos - i128::from(off.unwrap_or(0)) * 1_000_000_000)
}

fn invoke_from_iso8601_timestamp(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let a: StrArg = arg("from_iso8601_timestamp", "varchar", &args.args, 0)?;
    let out = lift1(&a, |s| {
        let n = iso_to_utc_nanos("from_iso8601_timestamp", s)?;
        Ok(Some(TsMs(
            i64::try_from(n.div_euclid(1_000_000)).map_err(|_| exec("timestamp out of range"))?,
        )))
    })?;
    retag(out, out_tz(args))
}

fn invoke_from_iso8601_timestamp_nanos(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let a: StrArg = arg("from_iso8601_timestamp_nanos", "varchar", &args.args, 0)?;
    let out = lift1(&a, |s| {
        let n = iso_to_utc_nanos("from_iso8601_timestamp_nanos", s)?;
        Ok(Some(TsNs(
            i64::try_from(n).map_err(|_| exec("timestamp out of range"))?,
        )))
    })?;
    retag(out, out_tz(args))
}

fn invoke_to_iso8601(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    if matches!(
        args.args[0].data_type(),
        DataType::Date32 | DataType::Date64
    ) {
        let a: DateArg = arg("to_iso8601", "date", &args.args, 0)?;
        return lift1(&a, |d| {
            let date = date_from_days(d).ok_or_else(|| exec("date out of range"))?;
            Ok(Some(date.format("%Y-%m-%d").to_string()))
        });
    }
    let a: TsArg = arg("to_iso8601", "timestamp", &args.args, 0)?;
    let tz = a.tz.clone();
    lift1(&a, |us| {
        let l = local_of(us, tz.as_deref())?;
        let Some(tz) = tz.as_deref() else {
            return Ok(Some(l.dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()));
        };
        let _ = tz;
        let sub_ms = us.rem_euclid(1000) != 0;
        let base = if sub_ms {
            l.dt.format("%Y-%m-%dT%H:%M:%S%.6f")
        } else {
            l.dt.format("%Y-%m-%dT%H:%M:%S%.3f")
        };
        let suffix = if l.offset_secs == 0 {
            "Z".to_string()
        } else {
            offset_text(l.offset_secs, true)
        };
        Ok(Some(format!("{base}{suffix}")))
    })
}

// ───────────────────────────────────────────────────────────────────
// date / date_trunc
// ───────────────────────────────────────────────────────────────────

fn invoke_date(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    match args.args[0].data_type() {
        DataType::Date32 | DataType::Date64 => {
            let a: DateArg = arg("date", "date", &args.args, 0)?;
            lift1(&a, |d| Ok(Some(DateDays(d))))
        }
        DataType::Timestamp(..) => {
            let a: TsArg = arg("date", "timestamp", &args.args, 0)?;
            let tz = a.tz.clone();
            lift1(&a, |us| {
                let l = local_of(us, tz.as_deref())?;
                Ok(Some(DateDays(epoch_days(l.dt.date()))))
            })
        }
        _ => {
            let a: StrArg = arg("date", "varchar", &args.args, 0)?;
            lift1(&a, |s| {
                let d = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
                    .map_err(|_| exec(format!("Value cannot be cast to date: {s}")))?;
                Ok(Some(DateDays(epoch_days(d))))
            })
        }
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
enum Unit {
    Micro,
    Milli,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

fn parse_unit(s: &str) -> DFResult<Unit> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "microsecond" => Unit::Micro,
        "millisecond" => Unit::Milli,
        "second" => Unit::Second,
        "minute" => Unit::Minute,
        "hour" => Unit::Hour,
        "day" => Unit::Day,
        "week" => Unit::Week,
        "month" => Unit::Month,
        "quarter" => Unit::Quarter,
        "year" => Unit::Year,
        _ => return Err(plan(format!("'{s}' is not a valid TIMESTAMP field"))),
    })
}

fn trunc_date(d: NaiveDate, u: Unit) -> Option<NaiveDate> {
    match u {
        Unit::Day => Some(d),
        Unit::Week => d.checked_sub_days(chrono::Days::new(u64::from(
            d.weekday().num_days_from_monday(),
        ))),
        Unit::Month => d.with_day(1),
        Unit::Quarter => d.with_day(1)?.with_month0(d.month0() / 3 * 3),
        Unit::Year => d.with_day(1)?.with_month(1),
        _ => None,
    }
}

fn trunc_naive(t: NaiveDateTime, u: Unit) -> Option<NaiveDateTime> {
    let ns = t.nanosecond();
    match u {
        Unit::Micro => t.with_nanosecond(ns / 1000 * 1000),
        Unit::Milli => t.with_nanosecond(ns / 1_000_000 * 1_000_000),
        Unit::Second => t.with_nanosecond(0),
        Unit::Minute => t.with_nanosecond(0)?.with_second(0),
        Unit::Hour => t.with_nanosecond(0)?.with_second(0)?.with_minute(0),
        _ => Some(trunc_date(t.date(), u)?.and_time(NaiveTime::MIN)),
    }
}

fn ret_date_trunc(types: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    match types.get(1) {
        Some(DataType::Date32 | DataType::Date64) => Ok(DataType::Date32),
        Some(t @ DataType::Timestamp(..)) => Ok(t.clone()),
        Some(DataType::Null) => Ok(DataType::Null),
        // A string is coerced to a timestamp, as before this override.
        Some(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        Some(other) => Err(plan(format!(
            "Unexpected parameters (varchar, {}) for function date_trunc. Expected: date_trunc(varchar, date), date_trunc(varchar, timestamp)",
            crate::helpers::trino_type_name(other)
        ))),
        None => Err(plan("date_trunc expects 2 arguments")),
    }
}

fn invoke_date_trunc(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    if matches!(
        args.args[1].data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        let mut coerced = args.clone();
        coerced.args[1] =
            args.args[1].cast_to(&DataType::Timestamp(TimeUnit::Microsecond, None), None)?;
        return invoke_date_trunc(&coerced);
    }
    let unit: StrArg = arg("date_trunc", "varchar, date", &args.args, 0)?;
    match args.args[1].data_type() {
        DataType::Date32 | DataType::Date64 => {
            let d: DateArg = arg("date_trunc", "varchar, date", &args.args, 1)?;
            lift2(&unit, &d, |u, days| {
                let unit = parse_unit(u)?;
                let date = date_from_days(days).ok_or_else(|| exec("date out of range"))?;
                let t = trunc_date(date, unit)
                    .ok_or_else(|| plan(format!("'{u}' is not a valid DATE field")))?;
                Ok(Some(DateDays(epoch_days(t))))
            })
        }
        DataType::Timestamp(..) => {
            let t: TsArg = arg("date_trunc", "varchar, timestamp", &args.args, 1)?;
            let tz = t.tz.clone();
            let zone = zone_of(tz.as_deref())?;
            let out = lift2(&unit, &t, |u, us| {
                let unit = parse_unit(u)?;
                let utc = DateTime::from_timestamp_micros(us)
                    .ok_or_else(|| exec("timestamp out of range"))?;
                let local = zone.local_naive(utc);
                let cut =
                    trunc_naive(local, unit).ok_or_else(|| exec("date_trunc out of range"))?;
                let micros = zone.utc_micros(cut).unwrap_or_else(|| {
                    // Truncated wall clock falls in a DST gap: keep the offset
                    // of the original instant.
                    us - (local - cut).num_microseconds().unwrap_or(0)
                });
                Ok(Some(TsMicros(micros)))
            })?;
            let out = match tz.as_deref() {
                Some(z) => tag_timezone(out, z)?,
                None => out,
            };
            if out.data_type() == *args.return_field.data_type() {
                Ok(out)
            } else {
                out.cast_to(args.return_field.data_type(), None)
            }
        }
        _ => Err(unexpected_parameters(
            "date_trunc",
            &args.args,
            "varchar, date",
        )),
    }
}

// ───────────────────────────────────────────────────────────────────
// Format and parse
// ───────────────────────────────────────────────────────────────────

fn invoke_format_datetime(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let t: TsArg = arg("format_datetime", "timestamp, varchar", &args.args, 0)?;
    let f: StrArg = arg("format_datetime", "timestamp, varchar", &args.args, 1)?;
    let tz = t.tz.clone();
    let mut cache: Option<(String, Vec<Piece>)> = None;
    lift2(&t, &f, |us, fmt| {
        if cache.as_ref().map(|(k, _)| k.as_str()) != Some(fmt) {
            cache = Some((fmt.to_string(), joda_compile(fmt)?));
        }
        let pieces = cache.as_ref().map(|(_, p)| p.as_slice()).unwrap_or(&[]);
        let l = local_of(us, tz.as_deref())?;
        Ok(Some(joda_format(pieces, &l)))
    })
}

fn naive_to_utc_millis(
    naive: NaiveDateTime,
    zone: Option<ParsedZone>,
) -> Result<i64, DataFusionError> {
    let micros = match zone {
        None => naive.and_utc().timestamp_micros(),
        Some(ParsedZone::Offset(o)) => {
            naive.and_utc().timestamp_micros() - i64::from(o) * 1_000_000
        }
        Some(ParsedZone::Named(z)) => z
            .utc_micros(naive)
            .ok_or_else(|| exec("local time does not exist in the time zone"))?,
    };
    Ok(micros.div_euclid(1000))
}

fn ret_parse_datetime(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(ts_type(TimeUnit::Millisecond, Some(SESSION_TZ)))
}

fn invoke_parse_datetime(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let s: StrArg = arg("parse_datetime", "varchar, varchar", &args.args, 0)?;
    let f: StrArg = arg("parse_datetime", "varchar, varchar", &args.args, 1)?;
    let mut cache: Option<(String, Vec<Piece>)> = None;
    let out = lift2(&s, &f, |text, fmt| {
        if cache.as_ref().map(|(k, _)| k.as_str()) != Some(fmt) {
            cache = Some((fmt.to_string(), joda_compile(fmt)?));
        }
        let pieces = cache.as_ref().map(|(_, p)| p.as_slice()).unwrap_or(&[]);
        let (naive, zone) = joda_parse(pieces, text)
            .and_then(|p| p.finish())
            .map_err(exec)?;
        Ok(Some(TsMs(naive_to_utc_millis(naive, zone)?)))
    })?;
    retag(out, out_tz(args))
}

fn ret_date_parse(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(ts_type(TimeUnit::Millisecond, None))
}

fn invoke_date_parse(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let s: StrArg = arg("date_parse", "varchar, varchar", &args.args, 0)?;
    let f: StrArg = arg("date_parse", "varchar, varchar", &args.args, 1)?;
    lift2(&s, &f, |text, fmt| {
        let (naive, _) = mysql_parse(fmt, text)
            .and_then(|p| p.finish())
            .map_err(exec)?;
        Ok(Some(TsMs(naive.and_utc().timestamp_millis())))
    })
}

fn invoke_date_format(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let t: TsArg = arg("date_format", "timestamp, varchar", &args.args, 0)?;
    let f: StrArg = arg("date_format", "timestamp, varchar", &args.args, 1)?;
    let tz = t.tz.clone();
    lift2(&t, &f, |us, fmt| {
        let l = local_of(us, tz.as_deref())?;
        Ok(Some(mysql_format(fmt, &l)?))
    })
}

fn ret_interval(_: &[DataType], _: &[Option<&ScalarValue>]) -> DFResult<DataType> {
    Ok(DataType::Interval(IntervalUnit::MonthDayNano))
}

fn parse_duration_ms(s: &str) -> DFResult<i64> {
    let bad = || exec(format!("Invalid duration: '{s}'"));
    let t = s.trim();
    let split = t.find(|c: char| c.is_ascii_alphabetic()).ok_or_else(bad)?;
    let (num, unit) = (t[..split].trim(), t[split..].trim());
    if num.is_empty() || !num.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return Err(bad());
    }
    let v: f64 = num.parse().map_err(|_| bad())?;
    let ms_per_unit = match unit {
        "ns" => 1e-6,
        "us" => 1e-3,
        "ms" => 1.0,
        "s" => 1e3,
        "m" => 6e4,
        "h" => 3.6e6,
        "d" => 8.64e7,
        _ => return Err(exec(format!("Unknown time unit: {unit}"))),
    };
    let ms = (v * ms_per_unit).round();
    if !ms.is_finite() || ms.abs() >= 8.0e18 / 1e6 {
        return Err(exec(format!("Invalid duration: '{s}' is out of range")));
    }
    Ok(ms as i64)
}

fn invoke_parse_duration(args: &ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let a: StrArg = arg("parse_duration", "varchar", &args.args, 0)?;
    lift1(&a, |s| {
        let ms = parse_duration_ms(s)?;
        let days =
            i32::try_from(ms.div_euclid(86_400_000)).map_err(|_| exec("duration out of range"))?;
        let nanos = ms.rem_euclid(86_400_000) * 1_000_000;
        Ok(Some(Mdn(IntervalMonthDayNanoType::make_value(
            0, days, nanos,
        ))))
    })
}

// ───────────────────────────────────────────────────────────────────
// sequence
// ───────────────────────────────────────────────────────────────────

/// `sequence` over DataFusion's `generate_series`, plus the two-argument date
/// form Trino defines (step is one day, negative when start is after stop).
#[derive(Debug)]
struct Sequence {
    inner: Arc<ScalarUDF>,
    signature: Signature,
}

impl PartialEq for Sequence {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for Sequence {}
impl Hash for Sequence {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "sequence".hash(state);
    }
}

/// `sequence` / `generate_series` with Trino's date forms.
pub(crate) fn sequence_udf() -> ScalarUDF {
    let inner = datafusion::functions_nested::range::gen_series_udf();
    let mut sigs = match &inner.signature().type_signature {
        TypeSignature::OneOf(v) => v.clone(),
        other => vec![other.clone()],
    };
    sigs.push(TypeSignature::Exact(vec![
        DataType::Date32,
        DataType::Date32,
    ]));
    ScalarUDF::from(Sequence {
        inner,
        signature: Signature::one_of(sigs, Volatility::Immutable),
    })
}

fn interval_ms_lower_bound(v: IntervalMonthDayNano) -> i128 {
    // A month is at least 28 days, which keeps the element count an upper bound.
    i128::from(v.months) * 28 * 86_400_000
        + i128::from(v.days) * 86_400_000
        + i128::from(v.nanoseconds) / 1_000_000
}

impl Sequence {
    /// Reject date / timestamp ranges that would build more than
    /// `MAX_GENERATED_SIZE` elements in one row.
    fn check_size(&self, args: &ScalarFunctionArgs) -> DFResult<()> {
        if args.args.len() != 3
            || !matches!(
                args.args[0].data_type(),
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
            )
        {
            return Ok(());
        }
        let n = args.number_rows;
        let millis = |cv: &ColumnarValue| -> Option<arrow::array::Int64Array> {
            let a = cv.to_array(n).ok()?;
            let t =
                arrow::compute::cast(&a, &DataType::Timestamp(TimeUnit::Millisecond, None)).ok()?;
            let i = arrow::compute::cast(&t, &DataType::Int64).ok()?;
            Some(i.as_primitive::<arrow::datatypes::Int64Type>().clone())
        };
        let (Some(start), Some(stop)) = (millis(&args.args[0]), millis(&args.args[1])) else {
            return Ok(());
        };
        let Ok(step) = args.args[2].to_array(n) else {
            return Ok(());
        };
        let Ok(step) = arrow::compute::cast(&step, &DataType::Interval(IntervalUnit::MonthDayNano))
        else {
            return Ok(());
        };
        let step = step.as_primitive::<IntervalMonthDayNanoType>();
        let mut max = 0u128;
        for i in 0..n.min(start.len()).min(stop.len()).min(step.len()) {
            if start.is_null(i) || stop.is_null(i) || step.is_null(i) {
                continue;
            }
            let delta = i128::from(stop.value(i)) - i128::from(start.value(i));
            let s = interval_ms_lower_bound(step.value(i));
            if s == 0 || (delta > 0 && s < 0) || (delta < 0 && s > 0) {
                continue;
            }
            max = max.max((delta / s).unsigned_abs() + 1);
        }
        let limit = u128::from(crate::scalar_fns::MAX_GENERATED_SIZE);
        if max > limit {
            return Err(exec(format!(
                "sequence: requested size {max} exceeds the limit of {limit}"
            )));
        }
        Ok(())
    }
}

impl ScalarUDFImpl for Sequence {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.inner().return_type(arg_types)
    }
    fn documentation(&self) -> Option<&datafusion::logical_expr::Documentation> {
        self.inner.inner().documentation()
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() == 2
            && args.args[0].data_type() == DataType::Date32
            && args.args[1].data_type() == DataType::Date32
        {
            let day = |forward: bool| {
                IntervalMonthDayNanoType::make_value(0, if forward { 1 } else { -1 }, 0)
            };
            let step = match (&args.args[0], &args.args[1]) {
                (
                    ColumnarValue::Scalar(ScalarValue::Date32(a)),
                    ColumnarValue::Scalar(ScalarValue::Date32(b)),
                ) => ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    a.zip(*b).map(|(a, b)| day(a <= b)),
                )),
                (a, b) => {
                    let n = args.number_rows;
                    let (a, b) = (a.to_array(n)?, b.to_array(n)?);
                    let (a, b) = (
                        a.as_primitive::<Date32Type>(),
                        b.as_primitive::<Date32Type>(),
                    );
                    let v: IntervalMonthDayNanoArray = (0..n)
                        .map(|i| {
                            (!a.is_null(i) && !b.is_null(i)).then(|| day(a.value(i) <= b.value(i)))
                        })
                        .collect();
                    ColumnarValue::Array(Arc::new(v))
                }
            };
            args.args.push(step);
            args.arg_fields.push(Arc::new(Field::new(
                "step",
                DataType::Interval(IntervalUnit::MonthDayNano),
                true,
            )));
        }
        self.check_size(&args)?;
        self.inner.inner().invoke_with_args(args)
    }
}

// ───────────────────────────────────────────────────────────────────
// Registration
// ───────────────────────────────────────────────────────────────────

pub(crate) fn register_datetime_fns(ctx: &SessionContext) {
    let any = (1, 1);
    for f in [
        dt("millisecond", &[], any, ret_i64, invoke_millisecond),
        dt("to_milliseconds", &[], any, ret_i64, invoke_to_milliseconds),
        dt("timezone_hour", &[], any, ret_i64, invoke_timezone_hour),
        dt("timezone_minute", &[], any, ret_i64, invoke_timezone_minute),
        dt("timezone", &[], any, ret_utf8, invoke_timezone),
        dt(
            "from_unixtime",
            &[],
            (1, 3),
            ret_from_unixtime,
            invoke_from_unixtime,
        ),
        dt(
            "from_unixtime_nanos",
            &[],
            any,
            ret_from_unixtime_nanos,
            invoke_from_unixtime_nanos,
        ),
        dt("to_unixtime", &[], any, ret_f64, invoke_to_unixtime),
        dt(
            "from_iso8601_timestamp",
            &[],
            any,
            ret_from_iso_ms,
            invoke_from_iso8601_timestamp,
        ),
        dt(
            "from_iso8601_timestamp_nanos",
            &[],
            any,
            ret_from_iso_ns,
            invoke_from_iso8601_timestamp_nanos,
        ),
        dt("to_iso8601", &[], any, ret_utf8, invoke_to_iso8601),
        dt("date", &[], any, ret_date, invoke_date),
        // Shadows DataFusion's date_trunc: Trino returns a date for a date input
        // and keeps the timestamp unit and zone, DataFusion widens to nanoseconds.
        // The `datetrunc` alias is DataFusion's own; claiming it keeps the
        // built-in out of the map, so a session rebuild cannot hand the name back.
        dt(
            "date_trunc",
            &["datetrunc"],
            (2, 2),
            ret_date_trunc,
            invoke_date_trunc,
        ),
        dt(
            "format_datetime",
            &[],
            (2, 2),
            ret_utf8,
            invoke_format_datetime,
        ),
        dt(
            "parse_datetime",
            &[],
            (2, 2),
            ret_parse_datetime,
            invoke_parse_datetime,
        ),
        dt("date_parse", &[], (2, 2), ret_date_parse, invoke_date_parse),
        dt("date_format", &[], (2, 2), ret_utf8, invoke_date_format),
        dt(
            "parse_duration",
            &[],
            any,
            ret_interval,
            invoke_parse_duration,
        ),
    ] {
        ctx.register_udf(f);
    }
}

// ───────────────────────────────────────────────────────────────────
// Hooks for the extract functions in trino_functions.rs
// ───────────────────────────────────────────────────────────────────

pub(crate) enum Prepared {
    /// The argument can go through the date/timestamp extractor as it is.
    Same,
    /// Use this argument instead (a second or millisecond timestamp widened
    /// to microseconds).
    Replace(ColumnarValue),
    /// The answer, for an interval argument.
    Done(ColumnarValue),
}

/// Adapt the argument of `year(x)`, `day(x)`, ... to what the extractors
/// read: intervals are answered here and the second and millisecond
/// timestamps produced by the functions above are widened to microseconds.
pub(crate) fn prepare_extract(arg: &ColumnarValue, fn_name: &str) -> DFResult<Prepared> {
    if let DataType::Interval(_) = arg.data_type() {
        return match fn_name {
            "year" | "month" | "day" | "hour" | "minute" | "second" => {
                Ok(Prepared::Done(interval_field(arg, fn_name)?.ok_or_else(
                    || unexpected_parameters(fn_name, std::slice::from_ref(arg), "interval"),
                )?))
            }
            other => Err(plan(format!("{other}() is not supported on intervals"))),
        };
    }
    match arg.data_type() {
        DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, tz) => Ok(Prepared::Replace(
            arg.cast_to(&DataType::Timestamp(TimeUnit::Microsecond, tz), None)?,
        )),
        _ => Ok(Prepared::Same),
    }
}
