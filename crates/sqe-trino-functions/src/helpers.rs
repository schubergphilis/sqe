//! Shared helpers for Trino-compatible UDFs.
//!
//! # Row lifting
//!
//! DataFusion hands a scalar UDF a mix of `ColumnarValue::Scalar` (a literal
//! or a constant-folded expression) and `ColumnarValue::Array` (a real column)
//! per argument. A UDF that only matches the scalar shape and falls through to
//! `Ok(Scalar(None))` for anything else silently returns NULL for every row of
//! a column-fed query (review finding 703 / #623).
//!
//! The lifters here ([`lift1`], [`lift2`], [`lift3`]) take typed argument
//! views ([`StrArg`], [`I64Arg`], [`F64Arg`], [`TsArg`], [`DateArg`],
//! [`AnyArg`]) and run a per-row closure over any scalar/array combination.
//! Output shape follows the inputs: all-scalar in gives a scalar out,
//! otherwise an array with one row per input row. NULL in any input gives
//! NULL in that output row (Trino null propagation); the closure never sees a
//! NULL.
//!
//! Type mismatches are surfaced at the argument view constructor via
//! [`unexpected_parameters`], which mirrors Trino's
//! `Unexpected parameters (...) for function f. Expected: f(...)` message, so
//! an unsupported input type is a `Plan` error and never a NULL column.
//!
//! String arguments accept `Utf8`, `LargeUtf8` and `Utf8View` (both scalar and
//! array); the previous helpers downcast to `StringArray` only and errored on
//! `Utf8View` columns.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, LargeStringArray,
    StringArray, StringViewArray, TimestampMicrosecondArray,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDateTime, TimeZone};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::ColumnarValue;

// ───────────────────────────────────────────────────────────────────
// Errors
// ───────────────────────────────────────────────────────────────────

/// Render an Arrow type the way Trino names it in error messages.
pub(crate) fn trino_type_name(dt: &DataType) -> String {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "bigint".into(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            format!("{dt}").to_lowercase()
        }
        DataType::Float32 => "real".into(),
        DataType::Float64 => "double".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, None) => "timestamp".into(),
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone".into(),
        DataType::Null => "unknown".into(),
        other => format!("{other}").to_lowercase(),
    }
}

/// Trino-style type-mismatch error:
/// `Unexpected parameters (varchar, bigint) for function f. Expected: f(varchar, varchar)`.
pub(crate) fn unexpected_parameters(
    fn_name: &str,
    args: &[ColumnarValue],
    expected: &str,
) -> DataFusionError {
    let actual: Vec<String> = args
        .iter()
        .map(|a| trino_type_name(&a.data_type()))
        .collect();
    DataFusionError::Plan(format!(
        "Unexpected parameters ({}) for function {fn_name}. Expected: {fn_name}({expected})",
        actual.join(", ")
    ))
}

// ───────────────────────────────────────────────────────────────────
// Argument views
// ───────────────────────────────────────────────────────────────────

/// A row-addressable view over one UDF argument.
pub(crate) trait RowArg {
    /// The per-row value handed to the closure.
    type Item;
    /// `Some(len)` for an array argument, `None` for a scalar broadcast.
    fn array_len(&self) -> Option<usize>;
    /// Value at row `i` (`0` for scalars); `None` for SQL NULL.
    fn get(&self, i: usize) -> Option<Self::Item>;
}

/// Build a typed argument view from a `ColumnarValue`.
///
/// `Ok(None)` means the input type is not accepted by this view; the caller
/// turns that into [`unexpected_parameters`]. `Err` is reserved for real
/// failures (e.g. a numeric cast that overflows).
pub(crate) trait FromColumnar<'a>: Sized {
    fn from_columnar(cv: &'a ColumnarValue) -> DFResult<Option<Self>>;
}

/// Build the typed view for `args[idx]`, or fail with the Trino type error.
pub(crate) fn arg<'a, T: FromColumnar<'a>>(
    fn_name: &str,
    expected: &str,
    args: &'a [ColumnarValue],
    idx: usize,
) -> DFResult<T> {
    let cv = args
        .get(idx)
        .ok_or_else(|| unexpected_parameters(fn_name, args, expected))?;
    T::from_columnar(cv)?.ok_or_else(|| unexpected_parameters(fn_name, args, expected))
}

/// String argument: `Utf8`, `LargeUtf8` or `Utf8View`, scalar or array.
pub(crate) enum StrArg<'a> {
    Scalar(Option<&'a str>),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> FromColumnar<'a> for StrArg<'a> {
    fn from_columnar(cv: &'a ColumnarValue) -> DFResult<Option<Self>> {
        Ok(match cv {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s),
            ) => Some(StrArg::Scalar(s.as_deref())),
            ColumnarValue::Scalar(ScalarValue::Null) => Some(StrArg::Scalar(None)),
            ColumnarValue::Array(arr) => {
                let any = arr.as_any();
                if let Some(a) = any.downcast_ref::<StringArray>() {
                    Some(StrArg::Utf8(a))
                } else if let Some(a) = any.downcast_ref::<LargeStringArray>() {
                    Some(StrArg::LargeUtf8(a))
                } else {
                    any.downcast_ref::<StringViewArray>().map(StrArg::Utf8View)
                }
            }
            _ => None,
        })
    }
}

impl<'a> RowArg for StrArg<'a> {
    type Item = &'a str;
    fn array_len(&self) -> Option<usize> {
        match self {
            StrArg::Scalar(_) => None,
            StrArg::Utf8(a) => Some(a.len()),
            StrArg::LargeUtf8(a) => Some(a.len()),
            StrArg::Utf8View(a) => Some(a.len()),
        }
    }
    fn get(&self, i: usize) -> Option<&'a str> {
        match self {
            StrArg::Scalar(s) => *s,
            StrArg::Utf8(a) => (!a.is_null(i)).then(|| a.value(i)),
            StrArg::LargeUtf8(a) => (!a.is_null(i)).then(|| a.value(i)),
            StrArg::Utf8View(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

/// Cast an array to `to`, failing (rather than nulling) on overflow.
fn strict_cast(arr: &ArrayRef, to: &DataType) -> DFResult<ArrayRef> {
    let opts = CastOptions {
        safe: false,
        ..Default::default()
    };
    cast_with_options(arr.as_ref(), to, &opts).map_err(|e| {
        DataFusionError::Execution(format!(
            "cannot cast {} to {}: {e}",
            arr.data_type(),
            trino_type_name(to)
        ))
    })
}

/// Integer argument: any Arrow integer type, widened to `i64`.
pub(crate) enum I64Arg {
    Scalar(Option<i64>),
    Array(Int64Array),
}

impl<'a> FromColumnar<'a> for I64Arg {
    fn from_columnar(cv: &'a ColumnarValue) -> DFResult<Option<Self>> {
        match cv {
            ColumnarValue::Scalar(sv) => Ok(match sv {
                ScalarValue::Int8(v) => Some(I64Arg::Scalar(v.map(i64::from))),
                ScalarValue::Int16(v) => Some(I64Arg::Scalar(v.map(i64::from))),
                ScalarValue::Int32(v) => Some(I64Arg::Scalar(v.map(i64::from))),
                ScalarValue::Int64(v) => Some(I64Arg::Scalar(*v)),
                ScalarValue::UInt8(v) => Some(I64Arg::Scalar(v.map(i64::from))),
                ScalarValue::UInt16(v) => Some(I64Arg::Scalar(v.map(i64::from))),
                ScalarValue::UInt32(v) => Some(I64Arg::Scalar(v.map(i64::from))),
                ScalarValue::UInt64(v) => {
                    let v = match v {
                        Some(u) => Some(i64::try_from(*u).map_err(|_| {
                            DataFusionError::Execution(format!("{u} does not fit in bigint"))
                        })?),
                        None => None,
                    };
                    Some(I64Arg::Scalar(v))
                }
                ScalarValue::Null => Some(I64Arg::Scalar(None)),
                _ => None,
            }),
            ColumnarValue::Array(arr) => {
                if !arr.data_type().is_integer() {
                    return Ok(None);
                }
                let cast = strict_cast(arr, &DataType::Int64)?;
                let a = cast
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("cast to Int64 lied".into()))?
                    .clone();
                Ok(Some(I64Arg::Array(a)))
            }
        }
    }
}

impl RowArg for I64Arg {
    type Item = i64;
    fn array_len(&self) -> Option<usize> {
        match self {
            I64Arg::Scalar(_) => None,
            I64Arg::Array(a) => Some(a.len()),
        }
    }
    fn get(&self, i: usize) -> Option<i64> {
        match self {
            I64Arg::Scalar(v) => *v,
            I64Arg::Array(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

/// Floating-point argument: any Arrow numeric type, widened to `f64`.
pub(crate) enum F64Arg {
    Scalar(Option<f64>),
    Array(Float64Array),
}

impl<'a> FromColumnar<'a> for F64Arg {
    fn from_columnar(cv: &'a ColumnarValue) -> DFResult<Option<Self>> {
        match cv {
            ColumnarValue::Scalar(ScalarValue::Null) => Ok(Some(F64Arg::Scalar(None))),
            ColumnarValue::Scalar(sv) => {
                if !sv.data_type().is_numeric() {
                    return Ok(None);
                }
                let v = match sv.cast_to(&DataType::Float64)? {
                    ScalarValue::Float64(v) => v,
                    _ => None,
                };
                Ok(Some(F64Arg::Scalar(v)))
            }
            ColumnarValue::Array(arr) => {
                if !arr.data_type().is_numeric() {
                    return Ok(None);
                }
                let cast = strict_cast(arr, &DataType::Float64)?;
                let a = cast
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| DataFusionError::Internal("cast to Float64 lied".into()))?
                    .clone();
                Ok(Some(F64Arg::Array(a)))
            }
        }
    }
}

impl RowArg for F64Arg {
    type Item = f64;
    fn array_len(&self) -> Option<usize> {
        match self {
            F64Arg::Scalar(_) => None,
            F64Arg::Array(a) => Some(a.len()),
        }
    }
    fn get(&self, i: usize) -> Option<f64> {
        match self {
            F64Arg::Scalar(v) => *v,
            F64Arg::Array(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

/// Timestamp argument, normalised to UTC epoch microseconds.
///
/// Accepts any `Timestamp(unit, tz)` and `Date32`/`Date64`. The time zone
/// the input was tagged with (if any) is kept in `tz` so formatters can
/// render the local wall clock the way Trino does.
pub(crate) struct TsArg {
    pub(crate) tz: Option<Arc<str>>,
    inner: TsInner,
}

enum TsInner {
    Scalar(Option<i64>),
    Array(TimestampMicrosecondArray),
}

const TS_US: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);

impl<'a> FromColumnar<'a> for TsArg {
    fn from_columnar(cv: &'a ColumnarValue) -> DFResult<Option<Self>> {
        let dt = cv.data_type();
        let tz = match &dt {
            DataType::Timestamp(_, tz) => tz.clone(),
            DataType::Date32 | DataType::Date64 | DataType::Null => None,
            _ => return Ok(None),
        };
        match cv {
            ColumnarValue::Scalar(ScalarValue::Null) => Ok(Some(TsArg {
                tz,
                inner: TsInner::Scalar(None),
            })),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, _)) => Ok(Some(TsArg {
                tz,
                inner: TsInner::Scalar(*v),
            })),
            ColumnarValue::Scalar(sv) => {
                let v = match sv.cast_to(&TS_US)? {
                    ScalarValue::TimestampMicrosecond(v, _) => v,
                    _ => None,
                };
                Ok(Some(TsArg {
                    tz,
                    inner: TsInner::Scalar(v),
                }))
            }
            ColumnarValue::Array(arr) => {
                let cast = strict_cast(arr, &TS_US)?;
                let a = cast
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| DataFusionError::Internal("cast to Timestamp lied".into()))?
                    .clone();
                Ok(Some(TsArg {
                    tz,
                    inner: TsInner::Array(a),
                }))
            }
        }
    }
}

impl RowArg for TsArg {
    type Item = i64;
    fn array_len(&self) -> Option<usize> {
        match &self.inner {
            TsInner::Scalar(_) => None,
            TsInner::Array(a) => Some(a.len()),
        }
    }
    fn get(&self, i: usize) -> Option<i64> {
        match &self.inner {
            TsInner::Scalar(v) => *v,
            TsInner::Array(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

/// Date argument as days since the Unix epoch (`Date32`; `Date64` is cast).
pub(crate) enum DateArg {
    Scalar(Option<i32>),
    Array(Date32Array),
}

impl<'a> FromColumnar<'a> for DateArg {
    fn from_columnar(cv: &'a ColumnarValue) -> DFResult<Option<Self>> {
        match cv {
            ColumnarValue::Scalar(ScalarValue::Null) => Ok(Some(DateArg::Scalar(None))),
            ColumnarValue::Scalar(ScalarValue::Date32(v)) => Ok(Some(DateArg::Scalar(*v))),
            ColumnarValue::Scalar(sv @ ScalarValue::Date64(_)) => {
                let v = match sv.cast_to(&DataType::Date32)? {
                    ScalarValue::Date32(v) => v,
                    _ => None,
                };
                Ok(Some(DateArg::Scalar(v)))
            }
            ColumnarValue::Scalar(_) => Ok(None),
            ColumnarValue::Array(arr) => {
                if !matches!(arr.data_type(), DataType::Date32 | DataType::Date64) {
                    return Ok(None);
                }
                let cast = strict_cast(arr, &DataType::Date32)?;
                let a = cast
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| DataFusionError::Internal("cast to Date32 lied".into()))?
                    .clone();
                Ok(Some(DateArg::Array(a)))
            }
        }
    }
}

impl RowArg for DateArg {
    type Item = i32;
    fn array_len(&self) -> Option<usize> {
        match self {
            DateArg::Scalar(_) => None,
            DateArg::Array(a) => Some(a.len()),
        }
    }
    fn get(&self, i: usize) -> Option<i32> {
        match self {
            DateArg::Scalar(v) => *v,
            DateArg::Array(a) => (!a.is_null(i)).then(|| a.value(i)),
        }
    }
}

/// Untyped argument: every row is materialised as a `ScalarValue`.
///
/// For functions whose parameter is polymorphic (`json_array_contains`'s
/// search value, `format`'s varargs). Slower than the typed views; use those
/// where the type is known.
pub(crate) enum AnyArg<'a> {
    Scalar(&'a ScalarValue),
    Array(&'a ArrayRef),
}

impl<'a> FromColumnar<'a> for AnyArg<'a> {
    fn from_columnar(cv: &'a ColumnarValue) -> DFResult<Option<Self>> {
        Ok(Some(match cv {
            ColumnarValue::Scalar(sv) => AnyArg::Scalar(sv),
            ColumnarValue::Array(arr) => AnyArg::Array(arr),
        }))
    }
}

impl<'a> RowArg for AnyArg<'a> {
    type Item = ScalarValue;
    fn array_len(&self) -> Option<usize> {
        match self {
            AnyArg::Scalar(_) => None,
            AnyArg::Array(a) => Some(a.len()),
        }
    }
    fn get(&self, i: usize) -> Option<ScalarValue> {
        match self {
            AnyArg::Scalar(sv) => (!sv.is_null()).then(|| (*sv).clone()),
            AnyArg::Array(a) => {
                if a.is_null(i) {
                    None
                } else {
                    ScalarValue::try_from_array(a.as_ref(), i).ok()
                }
            }
        }
    }
}

// ───────────────────────────────────────────────────────────────────
// Output builders
// ───────────────────────────────────────────────────────────────────

/// A per-row result type that knows how to become a scalar or an array.
pub(crate) trait RowOut: Sized {
    fn to_scalar(v: Option<Self>) -> ScalarValue;
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef;
}

impl RowOut for String {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::Utf8(v)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(StringArray::from(vals))
    }
}

impl RowOut for i64 {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::Int64(v)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(Int64Array::from(vals))
    }
}

impl RowOut for bool {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::Boolean(v)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(BooleanArray::from(vals))
    }
}

/// Days since the Unix epoch; materialises as `Date32`.
pub(crate) struct DateDays(pub i32);

impl RowOut for DateDays {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::Date32(v.map(|d| d.0))
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(Date32Array::from_iter(
            vals.into_iter().map(|v| v.map(|d| d.0)),
        ))
    }
}

/// UTC epoch microseconds; materialises as `Timestamp(Microsecond, None)`.
/// Use [`tag_timezone`] afterwards for a zoned result.
pub(crate) struct TsMicros(pub i64);

impl RowOut for TsMicros {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(v.map(|t| t.0), None)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from_iter(
            vals.into_iter().map(|v| v.map(|t| t.0)),
        ))
    }
}

/// Re-tag a `Timestamp(Microsecond, _)` result with `tz`. The stored epoch
/// values are untouched; only the display zone changes.
pub(crate) fn tag_timezone(cv: ColumnarValue, tz: &str) -> DFResult<ColumnarValue> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, _)) => Ok(
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, Some(tz.into()))),
        ),
        ColumnarValue::Array(arr) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "tag_timezone: expected Timestamp(Microsecond), got {}",
                        arr.data_type()
                    ))
                })?
                .clone()
                .with_timezone(tz);
            Ok(ColumnarValue::Array(Arc::new(a)))
        }
        other => Err(DataFusionError::Internal(format!(
            "tag_timezone: expected Timestamp(Microsecond), got {}",
            other.data_type()
        ))),
    }
}

// ───────────────────────────────────────────────────────────────────
// Lifters
// ───────────────────────────────────────────────────────────────────

/// Resolve the output row count: `None` when every input is a scalar,
/// otherwise the shared array length (mismatched lengths are an error).
fn output_rows(lens: &[Option<usize>]) -> DFResult<Option<usize>> {
    let mut n: Option<usize> = None;
    for l in lens.iter().flatten() {
        match n {
            None => n = Some(*l),
            Some(prev) if prev != *l => {
                return Err(DataFusionError::Internal(format!(
                    "argument arrays have different lengths ({prev} vs {l})"
                )))
            }
            _ => {}
        }
    }
    Ok(n)
}

/// Lift a one-argument per-row closure over a scalar or array input.
pub(crate) fn lift1<A, O>(
    a: &A,
    mut f: impl FnMut(A::Item) -> DFResult<Option<O>>,
) -> DFResult<ColumnarValue>
where
    A: RowArg,
    O: RowOut,
{
    let row = |i: usize, f: &mut dyn FnMut(A::Item) -> DFResult<Option<O>>| match a.get(i) {
        Some(x) => f(x),
        None => Ok(None),
    };
    match a.array_len() {
        None => Ok(ColumnarValue::Scalar(O::to_scalar(row(0, &mut f)?))),
        Some(n) => {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(row(i, &mut f)?);
            }
            Ok(ColumnarValue::Array(O::to_array(out)))
        }
    }
}

/// Lift a two-argument per-row closure over any scalar/array combination.
pub(crate) fn lift2<A, B, O>(
    a: &A,
    b: &B,
    mut f: impl FnMut(A::Item, B::Item) -> DFResult<Option<O>>,
) -> DFResult<ColumnarValue>
where
    A: RowArg,
    B: RowArg,
    O: RowOut,
{
    let row = |i: usize, f: &mut dyn FnMut(A::Item, B::Item) -> DFResult<Option<O>>| match (
        a.get(i),
        b.get(i),
    ) {
        (Some(x), Some(y)) => f(x, y),
        _ => Ok(None),
    };
    match output_rows(&[a.array_len(), b.array_len()])? {
        None => Ok(ColumnarValue::Scalar(O::to_scalar(row(0, &mut f)?))),
        Some(n) => {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(row(i, &mut f)?);
            }
            Ok(ColumnarValue::Array(O::to_array(out)))
        }
    }
}

/// Lift a three-argument per-row closure over any scalar/array combination.
pub(crate) fn lift3<A, B, C, O>(
    a: &A,
    b: &B,
    c: &C,
    mut f: impl FnMut(A::Item, B::Item, C::Item) -> DFResult<Option<O>>,
) -> DFResult<ColumnarValue>
where
    A: RowArg,
    B: RowArg,
    C: RowArg,
    O: RowOut,
{
    let row = |i: usize, f: &mut dyn FnMut(A::Item, B::Item, C::Item) -> DFResult<Option<O>>| match (
        a.get(i),
        b.get(i),
        c.get(i),
    ) {
        (Some(x), Some(y), Some(z)) => f(x, y, z),
        _ => Ok(None),
    };
    match output_rows(&[a.array_len(), b.array_len(), c.array_len()])? {
        None => Ok(ColumnarValue::Scalar(O::to_scalar(row(0, &mut f)?))),
        Some(n) => {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(row(i, &mut f)?);
            }
            Ok(ColumnarValue::Array(O::to_array(out)))
        }
    }
}

// ───────────────────────────────────────────────────────────────────
// String transforms (thin wrappers kept for the existing call sites)
// ───────────────────────────────────────────────────────────────────

/// Apply a per-string transform to a single string `ColumnarValue`.
///
/// `f` returns `Option<String>`: `None` produces a NULL in that row. Non-string
/// input is a Trino-style `Plan` error.
pub(crate) fn str_transform(
    fn_name: &str,
    arg0: &ColumnarValue,
    f: impl Fn(&str) -> Option<String>,
) -> DFResult<ColumnarValue> {
    let args = std::slice::from_ref(arg0);
    let a: StrArg = arg(fn_name, "varchar", args, 0)?;
    lift1(&a, |s| Ok(f(s)))
}

/// Apply a per-string transform across two string `ColumnarValue`s.
pub(crate) fn str_transform_2(
    fn_name: &str,
    args: &[ColumnarValue],
    f: impl Fn(&str, &str) -> Option<String>,
) -> DFResult<ColumnarValue> {
    let a: StrArg = arg(fn_name, "varchar, varchar", args, 0)?;
    let b: StrArg = arg(fn_name, "varchar, varchar", args, 1)?;
    lift2(&a, &b, |x, y| Ok(f(x, y)))
}

// ───────────────────────────────────────────────────────────────────
// Time zones
// ───────────────────────────────────────────────────────────────────

/// A parsed time zone: IANA name (`Europe/Amsterdam`) or fixed offset
/// (`+02:00`). `chrono_tz` only knows names; Arrow and Trino also accept
/// offsets, so both are handled here.
pub(crate) enum Zone {
    Named(chrono_tz::Tz),
    Fixed(chrono::FixedOffset),
}

impl Zone {
    pub(crate) fn parse(s: &str) -> DFResult<Zone> {
        if let Ok(tz) = s.parse::<chrono_tz::Tz>() {
            return Ok(Zone::Named(tz));
        }
        if let Some(off) = parse_fixed_offset(s) {
            return Ok(Zone::Fixed(off));
        }
        Err(DataFusionError::Plan(format!("Unknown time zone: {s}")))
    }

    /// Wall-clock time in this zone for a UTC instant.
    pub(crate) fn local_naive(&self, utc: chrono::DateTime<chrono::Utc>) -> NaiveDateTime {
        match self {
            Zone::Named(tz) => utc.with_timezone(tz).naive_local(),
            Zone::Fixed(off) => utc.with_timezone(off).naive_local(),
        }
    }

    /// UTC instant for a wall-clock time in this zone. Ambiguous (DST fold)
    /// times take the earlier instant; times in a DST gap return `None`.
    pub(crate) fn utc_micros(&self, local: NaiveDateTime) -> Option<i64> {
        match self {
            Zone::Named(tz) => tz
                .from_local_datetime(&local)
                .earliest()
                .map(|dt| dt.timestamp_micros()),
            Zone::Fixed(off) => off
                .from_local_datetime(&local)
                .earliest()
                .map(|dt| dt.timestamp_micros()),
        }
    }
}

fn parse_fixed_offset(s: &str) -> Option<chrono::FixedOffset> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    let (h, m) = match rest.split_once(':') {
        Some((h, m)) => (h, m),
        None if rest.len() == 4 => (&rest[..2], &rest[2..]),
        None => (rest, "0"),
    };
    let h: i32 = h.parse().ok()?;
    let m: i32 = m.parse().ok()?;
    if h > 23 || m > 59 {
        return None;
    }
    chrono::FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

/// Wall-clock `NaiveDateTime` for a UTC epoch microsecond value, rendered in
/// `tz` when the input carried one (Trino formats zoned timestamps in their
/// own zone) and in UTC otherwise. `None` if the instant is out of chrono's
/// range or the zone name is unknown.
pub(crate) fn micros_to_local_naive(us: i64, tz: Option<&str>) -> Option<NaiveDateTime> {
    let utc = chrono::DateTime::from_timestamp_micros(us)?;
    match tz {
        None => Some(utc.naive_utc()),
        Some(tz) => Zone::parse(tz).ok().map(|z| z.local_naive(utc)),
    }
}

/// Render `dt` with a chrono strftime pattern, returning an error instead of
/// panicking: `DelayedFormat::to_string` panics on an unknown specifier or
/// one a zoneless value cannot render (`%z` on a `NaiveDateTime`).
pub(crate) fn strftime_naive(func: &str, dt: &NaiveDateTime, fmt: &str) -> DFResult<String> {
    use chrono::format::{Item, StrftimeItems};
    use std::fmt::Write as _;
    let items: Vec<Item> = StrftimeItems::new(fmt).collect();
    let bad = || DataFusionError::Execution(format!("{func}: unsupported format pattern '{fmt}'"));
    if items.iter().any(|i| matches!(i, Item::Error)) {
        return Err(bad());
    }
    let mut out = String::new();
    write!(out, "{}", dt.format_with_items(items.iter())).map_err(|_| bad())?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringViewArray;

    #[test]
    fn strftime_naive_errors_instead_of_panicking() {
        let dt = chrono::NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        assert_eq!(strftime_naive("f", &dt, "%Y-%m-%d").unwrap(), "2024-01-15");
        // %z needs an offset a NaiveDateTime does not have.
        assert!(strftime_naive("f", &dt, "%Y %z").is_err());
        // Unknown specifier.
        assert!(strftime_naive("f", &dt, "%Q").is_err());
    }

    #[test]
    fn str_transform_rejects_non_string_scalar_with_trino_message() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));
        let err = str_transform("soundex", &input, |s| Some(s.to_uppercase())).unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)), "{err}");
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: Unexpected parameters (bigint) for function soundex. Expected: soundex(varchar)"
        );
    }

    #[test]
    fn str_transform_2_rejects_non_string_scalar() {
        let input = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello".into()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(42))),
        ];
        let err = str_transform_2("normalize", &input, |a, b| Some(format!("{a}{b}"))).unwrap_err();
        assert!(matches!(err, DataFusionError::Plan(_)), "{err}");
    }

    #[test]
    fn str_transform_accepts_utf8view_column() {
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec![Some("a"), None, Some("c")]));
        let out =
            str_transform("f", &ColumnarValue::Array(arr), |s| Some(s.to_uppercase())).unwrap();
        let ColumnarValue::Array(out) = out else {
            panic!("expected array");
        };
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), "A");
        assert!(out.is_null(1));
        assert_eq!(out.value(2), "C");
    }

    #[test]
    fn typed_null_scalar_propagates_as_null() {
        let input = ColumnarValue::Scalar(ScalarValue::Null);
        let out = str_transform("f", &input, |s| Some(s.to_string())).unwrap();
        assert!(matches!(
            out,
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
        ));
    }

    #[test]
    fn lift2_broadcasts_scalar_against_array() {
        let json: ArrayRef = Arc::new(StringArray::from(vec![Some("ab"), Some("cd"), None]));
        let args = vec![
            ColumnarValue::Array(json),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
        ];
        let a: StrArg = arg("f", "varchar, bigint", &args, 0).unwrap();
        let b: I64Arg = arg("f", "varchar, bigint", &args, 1).unwrap();
        let out = lift2(&a, &b, |s, n| Ok(Some(s.len() as i64 * n))).unwrap();
        let ColumnarValue::Array(out) = out else {
            panic!("expected array");
        };
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 4);
        assert_eq!(out.value(1), 4);
        assert!(out.is_null(2));
    }

    #[test]
    fn lift2_all_scalar_stays_scalar() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".into()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ];
        let a: StrArg = arg("f", "", &args, 0).unwrap();
        let b: I64Arg = arg("f", "", &args, 1).unwrap();
        let out = lift2(&a, &b, |s, n| Ok(Some(s.len() as i64 * n))).unwrap();
        assert!(matches!(
            out,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(6)))
        ));
    }

    #[test]
    fn ts_arg_accepts_zoned_and_nanosecond_inputs() {
        let zoned = ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
            Some(1_000_000),
            Some("Europe/Amsterdam".into()),
        ));
        let t = TsArg::from_columnar(&zoned).unwrap().unwrap();
        assert_eq!(t.get(0), Some(1_000_000));
        assert_eq!(t.tz.as_deref(), Some("Europe/Amsterdam"));

        let nanos =
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1_000_000_000), None));
        let t = TsArg::from_columnar(&nanos).unwrap().unwrap();
        assert_eq!(t.get(0), Some(1_000_000));
    }

    #[test]
    fn zone_parses_names_and_offsets() {
        assert!(matches!(
            Zone::parse("Europe/Amsterdam"),
            Ok(Zone::Named(_))
        ));
        assert!(matches!(Zone::parse("+02:00"), Ok(Zone::Fixed(_))));
        assert!(matches!(Zone::parse("-0530"), Ok(Zone::Fixed(_))));
        assert!(Zone::parse("Mars/Olympus").is_err());
    }
}
