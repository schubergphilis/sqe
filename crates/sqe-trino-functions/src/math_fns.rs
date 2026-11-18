//! Trino math functions that return Trino's value *and* Trino's type.
//!
//! DataFusion's built-ins widen integers to `Float64` (`ceil`, `floor`,
//! `signum`), keep a decimal's precision where Trino narrows it, and lack
//! several Trino functions. These UDFs follow Trino 484:
//!
//! | function | result type |
//! |---|---|
//! | `ceil`, `ceiling`, `floor` | integer in, same integer out; `decimal(p,s)` -> `decimal(p-s+1,0)`; double/real unchanged |
//! | `truncate(x)` | double/real unchanged; `decimal(p,s)` -> `decimal(p-s,0)`; integer -> double |
//! | `truncate(x, n)` | the type of `x` |
//! | `mod(a, b)` | wider integer; decimal rule of Trino; real; double |
//! | `sign(x)` | integer unchanged; `decimal(1,0)`; double/real unchanged |
//! | `bitwise_left_shift`, `bitwise_right_shift`, `bitwise_right_shift_arithmetic` | the type of the first argument, wrapped to its width |
//! | `bit_count(x, bits)` | bigint |
//! | `random(n)`, `random(m, n)` | the integer type of the bounds |
//! | `is_nan`, `is_finite`, `is_infinite` | boolean |
//! | `width_bucket` | bigint |
//! | `great_circle_distance`, `wilson_interval_*`, distributions | double |
//!
//! Unsigned integers (which Arrow files can hold, Trino cannot) count as the
//! next wider signed type. Every function returns an array unless all its
//! arguments are scalars, and passes NULL through.
//!
//! The distribution functions' numerics are in [`crate::stats_fns`].

use std::hash::BuildHasher;
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Decimal128Type, Field, Float64Type, Int64Type};
use datafusion::common::{plan_err, Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::stats_fns;

/// Mean radius of the Earth in kilometres, as Trino uses it.
const EARTH_RADIUS_KM: f64 = 6371.01;

/// Register the Trino math functions of this module. Must run after the
/// DataFusion built-ins (`ceil`, `floor`, `random` are replaced) and after
/// [`crate::scalar_fns::register_scalar_fns`].
pub fn register_math_fns(ctx: &SessionContext) {
    // Trino semantics: these replace DataFusion built-ins of the same name,
    // which widen integers to double and keep a decimal's precision.
    ctx.register_udf(ScalarUDF::from(UnaryMath::new(
        UnaryKind::Ceil,
        &["ceiling"],
    )));
    ctx.register_udf(ScalarUDF::from(UnaryMath::new(UnaryKind::Floor, &[])));
    ctx.register_udf(ScalarUDF::from(UnaryMath::new(UnaryKind::Sign, &[])));
    ctx.register_udf(ScalarUDF::from(Truncate));
    ctx.register_udf(ScalarUDF::from(TrinoMod));
    // `random` also covers the zero-argument form DataFusion has.
    ctx.register_udf(ScalarUDF::from(Random));
    for kind in [
        ShiftKind::Left,
        ShiftKind::RightLogical,
        ShiftKind::RightArithmetic,
    ] {
        ctx.register_udf(ScalarUDF::from(Shift { kind }));
    }
    ctx.register_udf(ScalarUDF::from(BitCount));
    for kind in [FloatTest::Nan, FloatTest::Finite, FloatTest::Infinite] {
        ctx.register_udf(ScalarUDF::from(FloatPredicate { kind }));
    }
    ctx.register_udf(ScalarUDF::from(WidthBucket));
    for op in [
        FloatOp::GreatCircleDistance,
        FloatOp::WilsonIntervalLower,
        FloatOp::WilsonIntervalUpper,
        FloatOp::NormalCdf,
        FloatOp::InverseNormalCdf,
        FloatOp::BetaCdf,
        FloatOp::InverseBetaCdf,
        FloatOp::TCdf,
        FloatOp::TPdf,
    ] {
        ctx.register_udf(ScalarUDF::from(FloatFn { op }));
    }
}

// ─── shared helpers ──────────────────────────────────────────────────────────

fn exec_err(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

fn all_scalar(args: &[ColumnarValue]) -> bool {
    args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)))
}

/// Wrap a result: a scalar when every argument was one, else the array.
fn finish(scalar: bool, arr: ArrayRef) -> DFResult<ColumnarValue> {
    if scalar && arr.len() == 1 {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
    } else {
        Ok(ColumnarValue::Array(arr))
    }
}

/// Argument `i` as an array of `to`, one entry per row.
fn arg_array(args: &ScalarFunctionArgs, i: usize, to: &DataType) -> DFResult<ArrayRef> {
    let arr = args.args[i].to_array(args.number_rows)?;
    if arr.data_type() == to {
        Ok(arr)
    } else {
        Ok(cast(&arr, to)?)
    }
}

fn check_arity(name: &str, args: &[DataType], expected: &[usize]) -> DFResult<()> {
    if expected.contains(&args.len()) {
        Ok(())
    } else {
        plan_err!("{name} does not take {} arguments", args.len())
    }
}

/// Row-wise `f` over two nullable columns; NULL when either side is.
fn zip_try<A: Copy, B: Copy, R>(
    a: impl Iterator<Item = Option<A>>,
    b: impl Iterator<Item = Option<B>>,
    mut f: impl FnMut(A, B) -> DFResult<R>,
) -> DFResult<Vec<Option<R>>> {
    a.zip(b)
        .map(|pair| match pair {
            (Some(x), Some(y)) => f(x, y).map(Some),
            _ => Ok(None),
        })
        .collect()
}

fn pow10(scale: i32) -> DFResult<i128> {
    if (0..=38).contains(&scale) {
        Ok(10_i128.pow(scale as u32))
    } else {
        Err(exec_err(format!("decimal scale {scale} is out of range")))
    }
}

fn decimal_array(values: Vec<Option<i128>>, precision: u8, scale: i8) -> DFResult<ArrayRef> {
    Ok(Arc::new(
        Decimal128Array::from(values).with_precision_and_scale(precision, scale)?,
    ))
}

fn float_array(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

/// What a numeric argument is, after Trino's widening of unsigned integers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Num {
    /// A signed integer of this many bits.
    Int(u32),
    Dec(u8, i8),
    F32,
    F64,
}

fn num_kind(dt: &DataType) -> DFResult<Num> {
    Ok(match dt {
        DataType::Int8 => Num::Int(8),
        DataType::Int16 | DataType::UInt8 => Num::Int(16),
        DataType::Int32 | DataType::UInt16 => Num::Int(32),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Num::Int(64),
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) => {
            Num::Dec(*p, *s)
        }
        DataType::Float16 | DataType::Float32 => Num::F32,
        DataType::Float64 | DataType::Null => Num::F64,
        other => return plan_err!("a numeric argument is required, got {other}"),
    })
}

fn int_type(bits: u32) -> DataType {
    match bits {
        8 => DataType::Int8,
        16 => DataType::Int16,
        32 => DataType::Int32,
        _ => DataType::Int64,
    }
}

fn num_type(n: Num) -> DataType {
    match n {
        Num::Int(bits) => int_type(bits),
        Num::Dec(p, s) => DataType::Decimal128(p, s),
        Num::F32 => DataType::Float32,
        Num::F64 => DataType::Float64,
    }
}

fn coerce_numeric(dt: &DataType) -> DFResult<DataType> {
    Ok(num_type(num_kind(dt)?))
}

/// The decimal type Trino gives an integer of `bits` when it meets a decimal.
fn int_as_decimal(bits: u32) -> Num {
    match bits {
        8 => Num::Dec(3, 0),
        16 => Num::Dec(5, 0),
        32 => Num::Dec(10, 0),
        _ => Num::Dec(19, 0),
    }
}

fn clamp_precision(p: i32) -> u8 {
    p.clamp(1, 38) as u8
}

/// A signature whose argument types come from `coerce_types`.
fn user_defined(volatility: Volatility) -> Signature {
    Signature::user_defined(volatility)
}

static IMMUTABLE: LazyLock<Signature> = LazyLock::new(|| user_defined(Volatility::Immutable));

// ─── ceil / floor / sign ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum UnaryKind {
    Ceil,
    Floor,
    Sign,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UnaryMath {
    kind: UnaryKind,
    aliases: Vec<String>,
}

impl UnaryMath {
    fn new(kind: UnaryKind, aliases: &[&str]) -> Self {
        Self {
            kind,
            aliases: aliases.iter().map(|a| a.to_string()).collect(),
        }
    }
}

impl ScalarUDFImpl for UnaryMath {
    fn name(&self) -> &str {
        match self.kind {
            UnaryKind::Ceil => "ceil",
            UnaryKind::Floor => "floor",
            UnaryKind::Sign => "sign",
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &IMMUTABLE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        check_arity(self.name(), arg_types, &[1])?;
        Ok(vec![coerce_numeric(&arg_types[0])?])
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        check_arity(self.name(), arg_types, &[1])?;
        Ok(match (self.kind, num_kind(&arg_types[0])?) {
            (UnaryKind::Sign, Num::Dec(..)) => DataType::Decimal128(1, 0),
            (UnaryKind::Ceil | UnaryKind::Floor, Num::Dec(p, s)) => {
                DataType::Decimal128(clamp_precision(i32::from(p) - i32::from(s) + 1), 0)
            }
            (_, other) => num_type(other),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let target = coerce_numeric(&args.args[0].data_type())?;
        let arr = arg_array(&args, 0, &target)?;
        let out: ArrayRef = match (self.kind, num_kind(&target)?) {
            // An integer is its own ceiling and floor.
            (UnaryKind::Ceil | UnaryKind::Floor, Num::Int(_)) => arr,
            (UnaryKind::Sign, Num::Int(_)) => {
                let wide = cast(&arr, &DataType::Int64)?;
                let signs: Int64Array = wide.as_primitive::<Int64Type>().unary(|v| v.signum());
                cast(&signs, &target)?
            }
            (kind, Num::F32 | Num::F64) => {
                let wide = cast(&arr, &DataType::Float64)?;
                let f: fn(f64) -> f64 = match kind {
                    UnaryKind::Ceil => f64::ceil,
                    UnaryKind::Floor => f64::floor,
                    // Java's `Math.signum`: zero keeps its sign, NaN stays NaN.
                    UnaryKind::Sign => |v| if v == 0.0 { v } else { v.signum() },
                };
                let mapped: Float64Array = wide.as_primitive::<Float64Type>().unary(f);
                cast(&mapped, &target)?
            }
            (UnaryKind::Sign, Num::Dec(..)) => {
                let signs: Vec<Option<i128>> = arr
                    .as_primitive::<Decimal128Type>()
                    .iter()
                    .map(|v| v.map(i128::signum))
                    .collect();
                decimal_array(signs, 1, 0)?
            }
            (kind, Num::Dec(p, s)) => {
                let divisor = pow10(i32::from(s))?;
                let rounded: Vec<Option<i128>> = arr
                    .as_primitive::<Decimal128Type>()
                    .iter()
                    .map(|v| {
                        v.map(|v| match kind {
                            // `div_euclid` by a positive divisor is floor division.
                            UnaryKind::Ceil => -(-v).div_euclid(divisor),
                            _ => v.div_euclid(divisor),
                        })
                    })
                    .collect();
                decimal_array(rounded, clamp_precision(i32::from(p) - i32::from(s) + 1), 0)?
            }
        };
        finish(scalar, out)
    }
}

// ─── truncate ────────────────────────────────────────────────────────────────

/// `truncate(x)` and `truncate(x, n)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Truncate;

impl Truncate {
    /// The type `x` is evaluated as: an integer becomes double.
    fn x_type(dt: &DataType) -> DFResult<Num> {
        Ok(match num_kind(dt)? {
            Num::Int(_) => Num::F64,
            other => other,
        })
    }
}

/// Truncate towards zero at `n` decimal places.
fn truncate_float(v: f64, n: i64) -> f64 {
    if !v.is_finite() {
        return v;
    }
    let factor = 10_f64.powi(n.clamp(-400, 400) as i32);
    if factor == 0.0 {
        return 0.0;
    }
    if factor.is_infinite() {
        return v;
    }
    (v * factor).trunc() / factor
}

/// Truncate a decimal of scale `scale` to `n` decimal places, keeping its scale.
fn truncate_decimal(v: i128, scale: i32, n: i64) -> i128 {
    let drop = i64::from(scale) - n;
    if drop <= 0 {
        v
    } else if drop > 38 {
        0
    } else {
        let divisor = 10_i128.pow(drop as u32);
        v - v % divisor
    }
}

impl ScalarUDFImpl for Truncate {
    fn name(&self) -> &str {
        "truncate"
    }

    fn signature(&self) -> &Signature {
        &IMMUTABLE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        check_arity("truncate", arg_types, &[1, 2])?;
        let mut out = vec![num_type(Self::x_type(&arg_types[0])?)];
        if arg_types.len() == 2 {
            if !matches!(num_kind(&arg_types[1])?, Num::Int(_)) {
                return plan_err!("truncate: the number of decimals must be an integer");
            }
            out.push(DataType::Int64);
        }
        Ok(out)
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        check_arity("truncate", arg_types, &[1, 2])?;
        Ok(match Self::x_type(&arg_types[0])? {
            Num::Dec(p, s) if arg_types.len() == 1 => {
                DataType::Decimal128(clamp_precision(i32::from(p) - i32::from(s)), 0)
            }
            other => num_type(other),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let x_kind = Self::x_type(&args.args[0].data_type())?;
        let x = arg_array(&args, 0, &num_type(x_kind))?;
        // Per-row number of decimals; None when absent.
        let n_arr = if args.args.len() == 2 {
            Some(arg_array(&args, 1, &DataType::Int64)?)
        } else {
            None
        };
        let n_at = |i: usize| -> Option<Option<i64>> {
            match &n_arr {
                None => Some(Some(0)),
                Some(a) => {
                    let a = a.as_primitive::<Int64Type>();
                    Some(a.is_valid(i).then(|| a.value(i)))
                }
            }
        };
        let out: ArrayRef = match x_kind {
            Num::F32 | Num::F64 => {
                let wide = cast(&x, &DataType::Float64)?;
                let wide = wide.as_primitive::<Float64Type>();
                let values: Vec<Option<f64>> = (0..wide.len())
                    .map(|i| match (wide.is_valid(i), n_at(i).flatten()) {
                        (true, Some(n)) => Some(truncate_float(wide.value(i), n)),
                        _ => None,
                    })
                    .collect();
                cast(&float_array(values), &num_type(x_kind))?
            }
            Num::Dec(p, s) => {
                let dec = x.as_primitive::<Decimal128Type>();
                let values: Vec<Option<i128>> = (0..dec.len())
                    .map(|i| match (dec.is_valid(i), n_at(i).flatten()) {
                        (true, Some(n)) => Some(truncate_decimal(dec.value(i), i32::from(s), n)),
                        _ => None,
                    })
                    .collect();
                if args.args.len() == 1 {
                    // The fraction is zeroed; the result has scale 0, so
                    // divide the scale out.
                    let divisor = pow10(i32::from(s))?;
                    let whole = values.into_iter().map(|v| v.map(|v| v / divisor)).collect();
                    decimal_array(whole, clamp_precision(i32::from(p) - i32::from(s)), 0)?
                } else {
                    decimal_array(values, p, s)?
                }
            }
            Num::Int(_) => return Err(exec_err("truncate: integer argument was not coerced")),
        };
        finish(scalar, out)
    }
}

// ─── mod ─────────────────────────────────────────────────────────────────────

/// `mod(a, b)`: the remainder, with the sign of `a`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TrinoMod;

/// The types `mod` evaluates its two arguments as, and its result type.
fn mod_types(a: &DataType, b: &DataType) -> DFResult<(DataType, DataType, DataType)> {
    let (mut x, mut y) = (num_kind(a)?, num_kind(b)?);
    // An integer meeting a decimal becomes the decimal that holds it.
    if let (Num::Int(bits), Num::Dec(..)) = (x, y) {
        x = int_as_decimal(bits);
    }
    if let (Num::Dec(..), Num::Int(bits)) = (x, y) {
        y = int_as_decimal(bits);
    }
    Ok(match (x, y) {
        (Num::Int(i), Num::Int(j)) => {
            let t = int_type(i.max(j));
            (t.clone(), t.clone(), t)
        }
        (Num::Dec(p1, s1), Num::Dec(p2, s2)) => {
            let scale = s1.max(s2);
            let precision = (i32::from(p2) - i32::from(s2)).min(i32::from(p1) - i32::from(s1))
                + i32::from(scale);
            (
                DataType::Decimal128(p1, s1),
                DataType::Decimal128(p2, s2),
                DataType::Decimal128(clamp_precision(precision), scale),
            )
        }
        // Real meets only real or an integer in Trino; anything else is double.
        (Num::F32, Num::F32 | Num::Int(_)) | (Num::Int(_), Num::F32) => {
            (DataType::Float32, DataType::Float32, DataType::Float32)
        }
        _ => (DataType::Float64, DataType::Float64, DataType::Float64),
    })
}

impl ScalarUDFImpl for TrinoMod {
    fn name(&self) -> &str {
        "mod"
    }

    fn signature(&self) -> &Signature {
        &IMMUTABLE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        check_arity("mod", arg_types, &[2])?;
        let (a, b, _) = mod_types(&arg_types[0], &arg_types[1])?;
        Ok(vec![a, b])
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        check_arity("mod", arg_types, &[2])?;
        Ok(mod_types(&arg_types[0], &arg_types[1])?.2)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let (ta, tb, tr) = mod_types(&args.args[0].data_type(), &args.args[1].data_type())?;
        let (a, b) = (arg_array(&args, 0, &ta)?, arg_array(&args, 1, &tb)?);
        let out: ArrayRef = match &tr {
            DataType::Decimal128(rp, scale) => {
                let (s1, s2) = match (&ta, &tb) {
                    (DataType::Decimal128(_, s1), DataType::Decimal128(_, s2)) => (*s1, *s2),
                    _ => return Err(exec_err("mod: decimal arguments expected")),
                };
                let (up_a, up_b) = (
                    pow10(i32::from(*scale) - i32::from(s1))?,
                    pow10(i32::from(*scale) - i32::from(s2))?,
                );
                let overflow = || exec_err("Decimal overflow");
                let values = zip_try(
                    a.as_primitive::<Decimal128Type>().iter(),
                    b.as_primitive::<Decimal128Type>().iter(),
                    |x, y| {
                        let x = x.checked_mul(up_a).ok_or_else(overflow)?;
                        let y = y.checked_mul(up_b).ok_or_else(overflow)?;
                        if y == 0 {
                            return Err(exec_err("Division by zero"));
                        }
                        Ok(x % y)
                    },
                )?;
                decimal_array(values, *rp, *scale)?
            }
            DataType::Float32 | DataType::Float64 => {
                let (a, b) = (cast(&a, &DataType::Float64)?, cast(&b, &DataType::Float64)?);
                // Java's `%`: NaN for a zero divisor, like Rust's.
                let values = zip_try(
                    a.as_primitive::<Float64Type>().iter(),
                    b.as_primitive::<Float64Type>().iter(),
                    |x, y| Ok(x % y),
                )?;
                cast(&float_array(values), &tr)?
            }
            _ => {
                let (a, b) = (cast(&a, &DataType::Int64)?, cast(&b, &DataType::Int64)?);
                let values = zip_try(
                    a.as_primitive::<Int64Type>().iter(),
                    b.as_primitive::<Int64Type>().iter(),
                    |x, y| {
                        if y == 0 {
                            Err(exec_err("Division by zero"))
                        } else {
                            Ok(x.wrapping_rem(y))
                        }
                    },
                )?;
                cast(&(Arc::new(Int64Array::from(values)) as ArrayRef), &tr)?
            }
        };
        finish(scalar, out)
    }
}

// ─── bit shifts and bit_count ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ShiftKind {
    Left,
    RightLogical,
    RightArithmetic,
}

/// `bitwise_left_shift`, `bitwise_right_shift` (zero fill) and
/// `bitwise_right_shift_arithmetic` (sign fill). The result has the type of
/// the first argument and wraps at its width; a shift of at least that many
/// bits is 0 (a negative value shifted right arithmetically gives -1).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Shift {
    kind: ShiftKind,
}

fn shift_value(kind: ShiftKind, value: i64, shift: i64, bits: u32) -> i64 {
    // Sign-extend the low `bits` of `v`.
    let wrap = |v: i64| (v << (64 - bits)) >> (64 - bits);
    let in_range = (0..i64::from(bits)).contains(&shift);
    match kind {
        ShiftKind::Left if in_range => wrap(((value as u64) << shift) as i64),
        ShiftKind::RightLogical if in_range => {
            let mask = if bits == 64 {
                u64::MAX
            } else {
                (1_u64 << bits) - 1
            };
            wrap((((value as u64) & mask) >> shift) as i64)
        }
        ShiftKind::RightArithmetic if in_range => value >> shift,
        ShiftKind::RightArithmetic if value < 0 => -1,
        _ => 0,
    }
}

impl ScalarUDFImpl for Shift {
    fn name(&self) -> &str {
        match self.kind {
            ShiftKind::Left => "bitwise_left_shift",
            ShiftKind::RightLogical => "bitwise_right_shift",
            ShiftKind::RightArithmetic => "bitwise_right_shift_arithmetic",
        }
    }

    fn signature(&self) -> &Signature {
        &IMMUTABLE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        check_arity(self.name(), arg_types, &[2])?;
        Ok(vec![self.value_type(&arg_types[0])?, DataType::Int64])
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        check_arity(self.name(), arg_types, &[2])?;
        self.value_type(&arg_types[0])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let target = self.value_type(&args.args[0].data_type())?;
        let Num::Int(bits) = num_kind(&target)? else {
            return Err(exec_err("shift: an integer is required"));
        };
        let value = cast(&arg_array(&args, 0, &target)?, &DataType::Int64)?;
        let shift = arg_array(&args, 1, &DataType::Int64)?;
        let shifted = zip_try(
            value.as_primitive::<Int64Type>().iter(),
            shift.as_primitive::<Int64Type>().iter(),
            |v, s| Ok(shift_value(self.kind, v, s, bits)),
        )?;
        finish(
            scalar,
            cast(&(Arc::new(Int64Array::from(shifted)) as ArrayRef), &target)?,
        )
    }
}

impl Shift {
    fn value_type(&self, dt: &DataType) -> DFResult<DataType> {
        match dt {
            DataType::Null => Ok(DataType::Int64),
            other => match num_kind(other)? {
                Num::Int(bits) => Ok(int_type(bits)),
                _ => plan_err!("{} requires an integer, got {other}", self.name()),
            },
        }
    }
}

/// `bit_count(x, bits)`: the number of set bits of `x` as a two's complement
/// number of `bits` bits (2 to 64).
#[derive(Debug, PartialEq, Eq, Hash)]
struct BitCount;

fn bit_count(value: i64, bits: i64) -> DFResult<i64> {
    if !(2..=64).contains(&bits) {
        return Err(exec_err(format!(
            "Bits specified in bit_count must be between 2 and 64, got {bits}"
        )));
    }
    if bits < 64 {
        let limit = 1_i64 << (bits - 1);
        if value < -limit || value >= limit {
            return Err(exec_err(format!(
                "Number must be representable with the bits specified. {value} can not be represented with {bits} bits"
            )));
        }
    }
    let mask = if bits == 64 {
        u64::MAX
    } else {
        (1_u64 << bits) - 1
    };
    Ok(i64::from(((value as u64) & mask).count_ones()))
}

impl ScalarUDFImpl for BitCount {
    fn name(&self) -> &str {
        "bit_count"
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> = LazyLock::new(|| {
            Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            )
        });
        &SIG
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let value = arg_array(&args, 0, &DataType::Int64)?;
        let bits = arg_array(&args, 1, &DataType::Int64)?;
        let counts = zip_try(
            value.as_primitive::<Int64Type>().iter(),
            bits.as_primitive::<Int64Type>().iter(),
            bit_count,
        )?;
        finish(scalar, Arc::new(Int64Array::from(counts)))
    }
}

// ─── random ──────────────────────────────────────────────────────────────────

/// `random()`, `random(n)` and `random(m, n)`. The integer forms are
/// uniform on `[0, n)` / `[m, n)` and have the integer type of the bounds.
/// Volatile.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Random;

impl Random {
    fn bound_type(arg_types: &[DataType]) -> DFResult<DataType> {
        let mut bits = 8;
        for dt in arg_types {
            match num_kind(dt)? {
                Num::Int(b) => bits = bits.max(b),
                _ => return plan_err!("random: the bounds must be integers, got {dt}"),
            }
        }
        Ok(int_type(bits))
    }
}

impl ScalarUDFImpl for Random {
    fn name(&self) -> &str {
        "random"
    }

    // DataFusion's `random` also owns `rand`; without it the old key survives
    // a session rebuild and brings the built-in back.
    fn aliases(&self) -> &[String] {
        static ALIASES: LazyLock<Vec<String>> = LazyLock::new(|| vec!["rand".to_string()]);
        &ALIASES
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> = LazyLock::new(|| user_defined(Volatility::Volatile));
        &SIG
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        check_arity("random", arg_types, &[0, 1, 2])?;
        if arg_types.is_empty() {
            return Ok(vec![]);
        }
        let t = Self::bound_type(arg_types)?;
        Ok(vec![t; arg_types.len()])
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        check_arity("random", arg_types, &[0, 1, 2])?;
        if arg_types.is_empty() {
            Ok(DataType::Float64)
        } else {
            Self::bound_type(arg_types)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // One randomly keyed SipHash stream per call; the row number is the input.
        let hasher = std::hash::RandomState::new();
        let rows = args.number_rows;
        if args.args.is_empty() {
            let values: Vec<Option<f64>> = (0..rows)
                .map(|i| Some((hasher.hash_one(i) >> 11) as f64 / (1_u64 << 53) as f64))
                .collect();
            return Ok(ColumnarValue::Array(float_array(values)));
        }
        let target =
            Self::bound_type(&args.args.iter().map(|a| a.data_type()).collect::<Vec<_>>())?;
        let bounds: Vec<ArrayRef> = (0..args.args.len())
            .map(|i| arg_array(&args, i, &DataType::Int64))
            .collect::<DFResult<_>>()?;
        let (start, stop) = match bounds.as_slice() {
            [stop] => (None, stop.as_primitive::<Int64Type>()),
            [start, stop] => (
                Some(start.as_primitive::<Int64Type>()),
                stop.as_primitive::<Int64Type>(),
            ),
            _ => return Err(exec_err("random takes at most two arguments")),
        };
        let mut values: Vec<Option<i64>> = Vec::with_capacity(rows);
        for i in 0..rows {
            let lower = match start {
                None => Some(0),
                Some(s) => s.is_valid(i).then(|| s.value(i)),
            };
            let upper = stop.is_valid(i).then(|| stop.value(i));
            let (Some(lower), Some(upper)) = (lower, upper) else {
                values.push(None);
                continue;
            };
            if upper <= lower {
                return Err(exec_err(if start.is_none() {
                    "bound must be positive".to_string()
                } else {
                    "start value must be less than stop value".to_string()
                }));
            }
            let span = upper.wrapping_sub(lower) as u64;
            let offset = ((u128::from(hasher.hash_one(i)) * u128::from(span)) >> 64) as u64;
            values.push(Some(lower.wrapping_add(offset as i64)));
        }
        let out: ArrayRef = Arc::new(Int64Array::from(values));
        Ok(ColumnarValue::Array(cast(&out, &target)?))
    }
}

// ─── is_nan / is_finite / is_infinite ────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FloatTest {
    Nan,
    Finite,
    Infinite,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FloatPredicate {
    kind: FloatTest,
}

impl ScalarUDFImpl for FloatPredicate {
    fn name(&self) -> &str {
        match self.kind {
            FloatTest::Nan => "is_nan",
            FloatTest::Finite => "is_finite",
            FloatTest::Infinite => "is_infinite",
        }
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Float64], Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let arr = arg_array(&args, 0, &DataType::Float64)?;
        let test: fn(f64) -> bool = match self.kind {
            FloatTest::Nan => f64::is_nan,
            FloatTest::Finite => f64::is_finite,
            FloatTest::Infinite => f64::is_infinite,
        };
        let out: BooleanArray = arr
            .as_primitive::<Float64Type>()
            .iter()
            .map(|v| v.map(test))
            .collect();
        finish(scalar, Arc::new(out))
    }
}

// ─── width_bucket ────────────────────────────────────────────────────────────

/// `width_bucket(x, bound1, bound2, n)` and `width_bucket(x, bins)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WidthBucket;

fn double_list() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)))
}

fn width_bucket_equal(x: f64, bound1: f64, bound2: f64, count: i64) -> DFResult<i64> {
    if count <= 0 {
        return Err(exec_err("bucketCount must be greater than 0"));
    }
    if x.is_nan() {
        return Err(exec_err("operand must not be NaN"));
    }
    if !bound1.is_finite() {
        return Err(exec_err("first bound must be finite"));
    }
    if !bound2.is_finite() {
        return Err(exec_err("second bound must be finite"));
    }
    if bound1 == bound2 {
        return Err(exec_err("bounds cannot equal each other"));
    }
    let (lower, upper) = (bound1.min(bound2), bound1.max(bound2));
    let bucket = if x < lower {
        0
    } else if x >= upper {
        count
            .checked_add(1)
            .ok_or_else(|| exec_err(format!("Bucket for value {x} is out of range")))?
    } else {
        (count as f64 * ((x - lower) / (upper - lower)) + 1.0) as i64
    };
    // A descending range numbers its buckets from the other end.
    Ok(if bound1 > bound2 {
        count - bucket + 1
    } else {
        bucket
    })
}

fn width_bucket_bins(x: f64, bins: &Float64Array) -> DFResult<i64> {
    if bins.is_empty() {
        return Err(exec_err("Bins cannot be an empty array"));
    }
    if x.is_nan() {
        return Err(exec_err("Operand cannot be NaN"));
    }
    let mut sorted = Vec::with_capacity(bins.len());
    for bin in bins.iter() {
        let bin = bin.ok_or_else(|| exec_err("Bin values must not be NULL"))?;
        if !bin.is_finite() {
            return Err(exec_err("Bin values must be finite"));
        }
        if sorted.last().is_some_and(|prev| *prev > bin) {
            return Err(exec_err("Bin values are not sorted in ascending order"));
        }
        sorted.push(bin);
    }
    Ok(sorted.partition_point(|bin| *bin <= x) as i64)
}

impl ScalarUDFImpl for WidthBucket {
    fn name(&self) -> &str {
        "width_bucket"
    }

    fn signature(&self) -> &Signature {
        &IMMUTABLE
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        check_arity("width_bucket", arg_types, &[2, 4])?;
        Ok(if arg_types.len() == 4 {
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Int64,
            ]
        } else {
            vec![DataType::Float64, double_list()]
        })
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        check_arity("width_bucket", arg_types, &[2, 4])?;
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let x = arg_array(&args, 0, &DataType::Float64)?;
        let x = x.as_primitive::<Float64Type>();
        let values: Vec<Option<i64>> = if args.args.len() == 4 {
            let b1 = arg_array(&args, 1, &DataType::Float64)?;
            let b2 = arg_array(&args, 2, &DataType::Float64)?;
            let n = arg_array(&args, 3, &DataType::Int64)?;
            let (b1, b2) = (
                b1.as_primitive::<Float64Type>(),
                b2.as_primitive::<Float64Type>(),
            );
            let n = n.as_primitive::<Int64Type>();
            (0..x.len())
                .map(|i| {
                    if [x.is_null(i), b1.is_null(i), b2.is_null(i), n.is_null(i)].contains(&true) {
                        Ok(None)
                    } else {
                        width_bucket_equal(x.value(i), b1.value(i), b2.value(i), n.value(i))
                            .map(Some)
                    }
                })
                .collect::<DFResult<_>>()?
        } else {
            let bins = arg_array(&args, 1, &double_list())?;
            let bins = bins.as_list::<i32>();
            (0..x.len())
                .map(|i| {
                    if x.is_null(i) || bins.is_null(i) {
                        return Ok(None);
                    }
                    let row = bins.value(i);
                    width_bucket_bins(x.value(i), row.as_primitive::<Float64Type>()).map(Some)
                })
                .collect::<DFResult<_>>()?
        };
        finish(scalar, Arc::new(Int64Array::from(values)))
    }
}

// ─── double-valued functions of doubles ──────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FloatOp {
    GreatCircleDistance,
    WilsonIntervalLower,
    WilsonIntervalUpper,
    NormalCdf,
    InverseNormalCdf,
    BetaCdf,
    InverseBetaCdf,
    TCdf,
    TPdf,
}

impl FloatOp {
    fn name(self) -> &'static str {
        match self {
            Self::GreatCircleDistance => "great_circle_distance",
            Self::WilsonIntervalLower => "wilson_interval_lower",
            Self::WilsonIntervalUpper => "wilson_interval_upper",
            Self::NormalCdf => "normal_cdf",
            Self::InverseNormalCdf => "inverse_normal_cdf",
            Self::BetaCdf => "beta_cdf",
            Self::InverseBetaCdf => "inverse_beta_cdf",
            Self::TCdf => "t_cdf",
            Self::TPdf => "t_pdf",
        }
    }

    fn arg_types(self) -> Vec<DataType> {
        let double = DataType::Float64;
        match self {
            Self::GreatCircleDistance => vec![double; 4],
            Self::WilsonIntervalLower | Self::WilsonIntervalUpper => {
                vec![DataType::Int64, DataType::Int64, double]
            }
            Self::NormalCdf | Self::InverseNormalCdf | Self::BetaCdf | Self::InverseBetaCdf => {
                vec![double; 3]
            }
            Self::TCdf | Self::TPdf => vec![double; 2],
        }
    }

    fn eval(self, a: &[f64]) -> DFResult<f64> {
        match self {
            Self::GreatCircleDistance => great_circle_distance(a[0], a[1], a[2], a[3]),
            Self::WilsonIntervalLower => wilson_interval(a[0], a[1], a[2], -1.0),
            Self::WilsonIntervalUpper => wilson_interval(a[0], a[1], a[2], 1.0),
            Self::NormalCdf => stats_fns::normal_cdf(a[0], a[1], a[2]),
            Self::InverseNormalCdf => stats_fns::inverse_normal_cdf(a[0], a[1], a[2]),
            Self::BetaCdf => stats_fns::beta_cdf(a[0], a[1], a[2]),
            Self::InverseBetaCdf => stats_fns::inverse_beta_cdf(a[0], a[1], a[2]),
            Self::TCdf => stats_fns::t_cdf(a[0], a[1]),
            Self::TPdf => stats_fns::t_pdf(a[0], a[1]),
        }
    }
}

/// Haversine distance in kilometres.
fn great_circle_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> DFResult<f64> {
    let latitude = |v: f64| (-90.0..=90.0).contains(&v);
    let longitude = |v: f64| (-180.0..=180.0).contains(&v);
    if !latitude(lat1) || !latitude(lat2) {
        return Err(exec_err("Latitude must be between -90 and 90"));
    }
    if !longitude(lon1) || !longitude(lon2) {
        return Err(exec_err("Longitude must be between -180 and 180"));
    }
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let sin_lat = ((phi2 - phi1) / 2.0).sin();
    let sin_lon = ((lon2 - lon1).to_radians() / 2.0).sin();
    let a = sin_lat * sin_lat + phi1.cos() * phi2.cos() * sin_lon * sin_lon;
    Ok(2.0 * EARTH_RADIUS_KM * a.sqrt().asin())
}

/// One end of the Wilson score interval: `sign` is -1 for the lower bound.
fn wilson_interval(successes: f64, trials: f64, z: f64, sign: f64) -> DFResult<f64> {
    if trials <= 0.0 {
        return Err(exec_err("trials must be positive"));
    }
    if successes < 0.0 {
        return Err(exec_err("successes must not be negative"));
    }
    if successes > trials {
        return Err(exec_err("successes must not exceed trials"));
    }
    if z.is_nan() || z < 0.0 {
        return Err(exec_err("z must not be negative"));
    }
    let p = successes / trials;
    let z2 = z * z;
    let centre = p + z2 / (2.0 * trials);
    let spread = z * (p * (1.0 - p) / trials + z2 / (4.0 * trials * trials)).sqrt();
    Ok((centre + sign * spread) / (1.0 + z2 / trials))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FloatFn {
    op: FloatOp,
}

impl ScalarUDFImpl for FloatFn {
    fn name(&self) -> &str {
        self.op.name()
    }

    fn signature(&self) -> &Signature {
        // Each op has one fixed argument list; `Signature` needs it owned.
        static SIGS: LazyLock<Vec<(FloatOp, Signature)>> = LazyLock::new(|| {
            [
                FloatOp::GreatCircleDistance,
                FloatOp::WilsonIntervalLower,
                FloatOp::WilsonIntervalUpper,
                FloatOp::NormalCdf,
                FloatOp::InverseNormalCdf,
                FloatOp::BetaCdf,
                FloatOp::InverseBetaCdf,
                FloatOp::TCdf,
                FloatOp::TPdf,
            ]
            .into_iter()
            .map(|op| (op, Signature::exact(op.arg_types(), Volatility::Immutable)))
            .collect()
        });
        SIGS.iter()
            .find(|(op, _)| *op == self.op)
            .map(|(_, sig)| sig)
            .unwrap_or(&IMMUTABLE)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = all_scalar(&args.args);
        let cols: Vec<ArrayRef> = (0..args.args.len())
            .map(|i| arg_array(&args, i, &DataType::Float64))
            .collect::<DFResult<_>>()?;
        let cols: Vec<&Float64Array> = cols
            .iter()
            .map(|c| c.as_primitive::<Float64Type>())
            .collect();
        let mut row = vec![0.0; cols.len()];
        let mut values = Vec::with_capacity(args.number_rows);
        for i in 0..args.number_rows {
            if cols.iter().any(|c| c.is_null(i)) {
                values.push(None);
                continue;
            }
            for (slot, col) in row.iter_mut().zip(&cols) {
                *slot = col.value(i);
            }
            values.push(Some(self.op.eval(&row)?));
        }
        finish(scalar, float_array(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::display::array_value_to_string;

    /// Run `sql` and return the type and text of its first row, first column.
    async fn run(sql: &str) -> DFResult<(DataType, String)> {
        let ctx = crate::duckdb_test_ctx();
        crate::register_trino_functions(&ctx);
        crate::register_extended_trino_functions(&ctx);
        let batches = ctx.sql(sql).await?.collect().await?;
        let col = batches[0].column(0);
        let text = if col.is_null(0) {
            "NULL".to_string()
        } else {
            array_value_to_string(col, 0)?
        };
        Ok((col.data_type().clone(), text))
    }

    async fn check(sql: &str, ty: DataType, text: &str) {
        let (got_ty, got) = run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!((got_ty, got.as_str()), (ty, text), "{sql}");
    }

    async fn fails(sql: &str, needle: &str) {
        let err = run(sql).await.expect_err(sql).to_string();
        assert!(err.contains(needle), "{sql}: {err}");
    }

    fn dec(p: u8, s: i8) -> DataType {
        DataType::Decimal128(p, s)
    }

    #[tokio::test]
    async fn ceil_and_floor_keep_trino_types() {
        use DataType::*;
        check("SELECT ceil(CAST(5 AS INTEGER))", Int32, "5").await;
        check("SELECT floor(CAST(5 AS BIGINT))", Int64, "5").await;
        check("SELECT ceiling(CAST(5 AS SMALLINT))", Int16, "5").await;
        check("SELECT ceil(CAST(1.5 AS DOUBLE))", Float64, "2.0").await;
        check("SELECT floor(CAST(-1.5 AS REAL))", Float32, "-2.0").await;
        check(
            "SELECT ceil(CAST('12.34' AS DECIMAL(4,2)))",
            dec(3, 0),
            "13",
        )
        .await;
        check(
            "SELECT floor(CAST('-12.34' AS DECIMAL(4,2)))",
            dec(3, 0),
            "-13",
        )
        .await;
        check(
            "SELECT ceil(CAST('-12.34' AS DECIMAL(4,2)))",
            dec(3, 0),
            "-12",
        )
        .await;
        check("SELECT ceil(CAST('0.5' AS DECIMAL(1,1)))", dec(1, 0), "1").await;
        check("SELECT ceil(CAST(NULL AS DECIMAL(4,2)))", dec(3, 0), "NULL").await;
    }

    #[tokio::test]
    async fn ceil_over_a_column() {
        let (ty, text) = run("SELECT ceil(x) FROM (VALUES (1.2), (-1.2), (NULL)) t(x)")
            .await
            .unwrap();
        assert_eq!(ty, DataType::Float64);
        assert_eq!(text, "2.0");
        let (_, sum) = run("SELECT sum(ceil(x)) FROM (VALUES (1.2), (-1.2), (NULL)) t(x)")
            .await
            .unwrap();
        assert_eq!(sum, "1.0");
    }

    #[tokio::test]
    async fn truncate_keeps_trino_types() {
        use DataType::*;
        check("SELECT truncate(CAST(-2.9 AS DOUBLE))", Float64, "-2.0").await;
        check("SELECT truncate(CAST(2.9 AS REAL))", Float32, "2.0").await;
        check(
            "SELECT truncate(CAST('12.34' AS DECIMAL(4,2)))",
            dec(2, 0),
            "12",
        )
        .await;
        check(
            "SELECT truncate(CAST('-12.99' AS DECIMAL(4,2)))",
            dec(2, 0),
            "-12",
        )
        .await;
        check(
            "SELECT truncate(CAST('0.5' AS DECIMAL(2,2)))",
            dec(1, 0),
            "0",
        )
        .await;
        check(
            "SELECT truncate(CAST(12.345 AS DOUBLE), 2)",
            Float64,
            "12.34",
        )
        .await;
        check(
            "SELECT truncate(CAST('12.345' AS DECIMAL(5,3)), 1)",
            dec(5, 3),
            "12.300",
        )
        .await;
        check(
            "SELECT truncate(CAST('1234.5' AS DECIMAL(5,1)), -2)",
            dec(5, 1),
            "1200.0",
        )
        .await;
        check("SELECT truncate(CAST(NULL AS DOUBLE))", Float64, "NULL").await;
    }

    #[tokio::test]
    async fn mod_keeps_trino_types() {
        use DataType::*;
        check(
            "SELECT mod(CAST(7 AS INTEGER), CAST(3 AS INTEGER))",
            Int32,
            "1",
        )
        .await;
        check(
            "SELECT mod(CAST(7 AS INTEGER), CAST(3 AS BIGINT))",
            Int64,
            "1",
        )
        .await;
        check(
            "SELECT mod(CAST(-7 AS SMALLINT), CAST(3 AS SMALLINT))",
            Int16,
            "-1",
        )
        .await;
        check(
            "SELECT mod(CAST(7.5 AS DOUBLE), CAST(2 AS DOUBLE))",
            Float64,
            "1.5",
        )
        .await;
        check(
            "SELECT mod(CAST(7.5 AS REAL), CAST(2 AS REAL))",
            Float32,
            "1.5",
        )
        .await;
        check(
            "SELECT mod(CAST('7.50' AS DECIMAL(4,2)), CAST('2.0' AS DECIMAL(3,1)))",
            dec(4, 2),
            "1.50",
        )
        .await;
        check(
            "SELECT mod(CAST('7.5' AS DECIMAL(3,1)), CAST(2 AS INTEGER))",
            dec(3, 1),
            "1.5",
        )
        .await;
        check(
            "SELECT mod(CAST(NULL AS INTEGER), CAST(2 AS INTEGER))",
            Int32,
            "NULL",
        )
        .await;
        fails("SELECT mod(5, 0)", "Division by zero").await;
        fails(
            "SELECT mod(CAST('5.0' AS DECIMAL(3,1)), CAST('0.0' AS DECIMAL(3,1)))",
            "Division by zero",
        )
        .await;
    }

    /// Plain-literal cases carried over from the double-only `mod`,
    /// `truncate` and `sign` these replaced.
    #[tokio::test]
    async fn literal_mod_truncate_sign() {
        use DataType::*;
        check("SELECT mod(10, 3)", Int64, "1").await;
        check("SELECT mod(10.5, 3.0)", Float64, "1.5").await;
        fails("SELECT mod(10, 0)", "Division by zero").await;
        check("SELECT truncate(3.7)", Float64, "3.0").await;
        check("SELECT truncate(-3.7)", Float64, "-3.0").await;
        check("SELECT truncate(2.71828, 2)", Float64, "2.71").await;
        check("SELECT truncate(-2.71828, 2)", Float64, "-2.71").await;
        check("SELECT sign(42.0)", Float64, "1.0").await;
        check("SELECT sign(-42.0)", Float64, "-1.0").await;
        check("SELECT sign(0.0)", Float64, "0.0").await;
    }

    #[tokio::test]
    async fn sign_keeps_trino_types() {
        use DataType::*;
        check("SELECT sign(CAST(-5 AS INTEGER))", Int32, "-1").await;
        check("SELECT sign(CAST(0 AS BIGINT))", Int64, "0").await;
        check("SELECT sign(CAST(-2.5 AS DOUBLE))", Float64, "-1.0").await;
        check("SELECT sign(CAST(2.5 AS REAL))", Float32, "1.0").await;
        check(
            "SELECT sign(CAST('-12.34' AS DECIMAL(4,2)))",
            dec(1, 0),
            "-1",
        )
        .await;
        check("SELECT sign(CAST(NULL AS DOUBLE))", Float64, "NULL").await;
    }

    #[tokio::test]
    async fn shifts_keep_the_first_argument_type() {
        use DataType::*;
        check("SELECT bitwise_left_shift(1, 3)", Int64, "8").await;
        check(
            "SELECT bitwise_left_shift(CAST(1 AS INTEGER), 31)",
            Int32,
            "-2147483648",
        )
        .await;
        check(
            "SELECT bitwise_left_shift(CAST(1 AS INTEGER), 32)",
            Int32,
            "0",
        )
        .await;
        check(
            "SELECT bitwise_left_shift(CAST(1 AS TINYINT), 7)",
            Int8,
            "-128",
        )
        .await;
        check(
            "SELECT bitwise_right_shift(CAST(-1 AS INTEGER), 28)",
            Int32,
            "15",
        )
        .await;
        check(
            "SELECT bitwise_right_shift(CAST(-8 AS BIGINT), 1)",
            Int64,
            "9223372036854775804",
        )
        .await;
        check(
            "SELECT bitwise_right_shift_arithmetic(CAST(-8 AS INTEGER), 1)",
            Int32,
            "-4",
        )
        .await;
        check(
            "SELECT bitwise_right_shift_arithmetic(CAST(-8 AS INTEGER), 40)",
            Int32,
            "-1",
        )
        .await;
        check(
            "SELECT bitwise_right_shift_arithmetic(CAST(8 AS BIGINT), 70)",
            Int64,
            "0",
        )
        .await;
        check(
            "SELECT bitwise_left_shift(CAST(NULL AS BIGINT), 1)",
            Int64,
            "NULL",
        )
        .await;
    }

    #[tokio::test]
    async fn bit_count_counts_in_the_given_width() {
        use DataType::Int64;
        check("SELECT bit_count(9, 64)", Int64, "2").await;
        check("SELECT bit_count(-7, 64)", Int64, "62").await;
        check("SELECT bit_count(-7, 8)", Int64, "6").await;
        check("SELECT bit_count(CAST(NULL AS BIGINT), 8)", Int64, "NULL").await;
        fails("SELECT bit_count(9, 1)", "between 2 and 64").await;
        fails("SELECT bit_count(128, 8)", "can not be represented").await;
    }

    #[tokio::test]
    async fn random_integer_forms_are_typed_and_bounded() {
        use DataType::*;
        check("SELECT random(CAST(1 AS INTEGER))", Int32, "0").await;
        check(
            "SELECT random(CAST(5 AS BIGINT), CAST(6 AS BIGINT))",
            Int64,
            "5",
        )
        .await;
        let (ty, _) = run("SELECT random()").await.unwrap();
        assert_eq!(ty, Float64);
        let (_, ok) = run(
            "SELECT bool_and(random(10) >= 0 AND random(10) < 10 AND random(-5, 5) < 5) \
             FROM generate_series(1, 200)",
        )
        .await
        .unwrap();
        assert_eq!(ok, "true");
        fails("SELECT random(0)", "bound must be positive").await;
        fails("SELECT random(5, 5)", "start value must be less").await;
    }

    #[tokio::test]
    async fn float_predicates() {
        use DataType::Boolean;
        check("SELECT is_nan(nan())", Boolean, "true").await;
        check("SELECT is_nan(1.5)", Boolean, "false").await;
        check("SELECT is_finite(infinity())", Boolean, "false").await;
        check("SELECT is_finite(1.5)", Boolean, "true").await;
        check("SELECT is_infinite(-infinity())", Boolean, "true").await;
        check("SELECT is_infinite(nan())", Boolean, "false").await;
        check("SELECT is_nan(CAST(NULL AS DOUBLE))", Boolean, "NULL").await;
        let (_, n) = run("SELECT count(*) FROM (VALUES (1.0), (NULL)) t(x) WHERE is_finite(x)")
            .await
            .unwrap();
        assert_eq!(n, "1");
    }

    #[tokio::test]
    async fn width_bucket_forms() {
        use DataType::Int64;
        check("SELECT width_bucket(3.14, 0, 4, 3)", Int64, "3").await;
        check("SELECT width_bucket(-1, 0, 4, 3)", Int64, "0").await;
        check("SELECT width_bucket(4, 0, 4, 3)", Int64, "4").await;
        check("SELECT width_bucket(3.14, 4, 0, 3)", Int64, "1").await;
        check("SELECT width_bucket(2.5, ARRAY[1.0, 2.0, 3.0])", Int64, "2").await;
        check("SELECT width_bucket(0.5, ARRAY[1.0, 2.0, 3.0])", Int64, "0").await;
        check("SELECT width_bucket(9, ARRAY[1.0, 2.0, 3.0])", Int64, "3").await;
        check(
            "SELECT width_bucket(CAST(NULL AS DOUBLE), 0, 4, 3)",
            Int64,
            "NULL",
        )
        .await;
        fails("SELECT width_bucket(1, 0, 4, 0)", "bucketCount").await;
        fails("SELECT width_bucket(1, 2, 2, 3)", "bounds cannot equal").await;
        fails("SELECT width_bucket(1, ARRAY[3.0, 1.0])", "not sorted").await;
        let (_, col) =
            run("SELECT sum(width_bucket(x, 0, 10, 5)) FROM (VALUES (1.0), (5.0), (11.0)) t(x)")
                .await
                .unwrap();
        assert_eq!(col, "10");
    }

    #[tokio::test]
    async fn geography_and_interval_functions() {
        use DataType::Float64;
        let (ty, text) = run("SELECT great_circle_distance(36.12, -86.67, 33.94, -118.40)")
            .await
            .unwrap();
        assert_eq!(ty, Float64);
        let km: f64 = text.parse().unwrap();
        assert!((km - 2886.4).abs() < 0.5, "{km}");
        check(
            "SELECT great_circle_distance(10, 20, 10, 20)",
            Float64,
            "0.0",
        )
        .await;
        check(
            "SELECT great_circle_distance(CAST(NULL AS DOUBLE), 20, 10, 20)",
            Float64,
            "NULL",
        )
        .await;
        fails("SELECT great_circle_distance(91, 0, 0, 0)", "Latitude").await;
        fails("SELECT great_circle_distance(0, 181, 0, 0)", "Longitude").await;

        let (ty, lower) = run("SELECT wilson_interval_lower(1250, 1310, 1.96)")
            .await
            .unwrap();
        assert_eq!(ty, Float64);
        let (_, upper) = run("SELECT wilson_interval_upper(1250, 1310, 1.96)")
            .await
            .unwrap();
        let (lower, upper): (f64, f64) = (lower.parse().unwrap(), upper.parse().unwrap());
        // Standard Wilson score interval, z = 1.96, 1250 of 1310.
        assert!(
            (lower - 0.94148837).abs() < 1e-6 && (upper - 0.96425247).abs() < 1e-6,
            "{lower} {upper}"
        );
        check(
            "SELECT wilson_interval_lower(CAST(NULL AS BIGINT), 10, 1.96)",
            Float64,
            "NULL",
        )
        .await;
        fails(
            "SELECT wilson_interval_lower(11, 10, 1.96)",
            "must not exceed",
        )
        .await;
    }

    #[tokio::test]
    async fn distribution_functions() {
        use DataType::Float64;
        check("SELECT normal_cdf(0, 1, 0)", Float64, "0.5").await;
        check("SELECT inverse_normal_cdf(0, 1, 0.5)", Float64, "0.0").await;
        check("SELECT beta_cdf(2, 3, 0.5)", Float64, "0.6875").await;
        check(
            "SELECT normal_cdf(0, 1, CAST(NULL AS DOUBLE))",
            Float64,
            "NULL",
        )
        .await;
        check("SELECT t_cdf(5, CAST(NULL AS DOUBLE))", Float64, "NULL").await;
        let (ty, p) = run("SELECT t_pdf(10, 0)").await.unwrap();
        assert_eq!(ty, Float64);
        assert!(
            (p.parse::<f64>().unwrap() - 0.3891083839).abs() < 1e-8,
            "{p}"
        );
        let (_, q) = run("SELECT inverse_beta_cdf(2, 3, beta_cdf(2, 3, 0.3))")
            .await
            .unwrap();
        assert!((q.parse::<f64>().unwrap() - 0.3).abs() < 1e-9, "{q}");
        let (_, c) = run("SELECT t_cdf(10, 2)").await.unwrap();
        assert!(
            (c.parse::<f64>().unwrap() - 0.9633059826).abs() < 1e-8,
            "{c}"
        );
        fails("SELECT normal_cdf(0, 0, 1)", "standardDeviation").await;
        let (_, col) = run("SELECT sum(normal_cdf(0, 1, x)) FROM (VALUES (0.0), (0.0)) t(x)")
            .await
            .unwrap();
        assert_eq!(col, "1.0");
    }

    #[tokio::test]
    async fn vector_distances() {
        use DataType::Float64;
        check(
            "SELECT euclidean_distance(ARRAY[0.0, 0.0], ARRAY[3.0, 4.0])",
            Float64,
            "5.0",
        )
        .await;
        check(
            "SELECT cosine_distance(ARRAY[0.1, 0.2, 0.3], ARRAY[0.1, 0.2, 0.3])",
            Float64,
            "0.0",
        )
        .await;
        check(
            "SELECT cosine_similarity(ARRAY[0.1, 0.2, 0.3], ARRAY[0.1, 0.2, 0.3])",
            Float64,
            "1.0",
        )
        .await;
        check(
            "SELECT cosine_distance(ARRAY[1.0, 0.0], ARRAY[0.0, 1.0])",
            Float64,
            "1.0",
        )
        .await;
        check(
            "SELECT cosine_distance(ARRAY[1.0, 0.0], ARRAY[-1.0, 0.0])",
            Float64,
            "2.0",
        )
        .await;
        check(
            "SELECT cosine_distance(ARRAY[1.0, 0.0], NULL)",
            Float64,
            "NULL",
        )
        .await;
    }

    #[tokio::test]
    async fn geometric_mean_aggregate() {
        use DataType::Float64;
        let (ty, text) = run("SELECT geometric_mean(x) FROM (VALUES (2.0), (8.0), (NULL)) t(x)")
            .await
            .unwrap();
        assert_eq!(ty, Float64);
        assert!((text.parse::<f64>().unwrap() - 4.0).abs() < 1e-12, "{text}");
        check(
            "SELECT geometric_mean(x) FROM (VALUES (CAST(NULL AS DOUBLE))) t(x)",
            Float64,
            "NULL",
        )
        .await;
        let (_, grouped) = run(
            "SELECT geometric_mean(x) FROM (VALUES (1, 2.0), (1, 8.0), (2, 9.0)) t(g, x) \
             GROUP BY g ORDER BY g",
        )
        .await
        .unwrap();
        assert!(
            (grouped.parse::<f64>().unwrap() - 4.0).abs() < 1e-12,
            "{grouped}"
        );
    }
}
