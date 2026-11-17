//! Trino `varbinary` functions: hashes, encodings, byte-order conversions.
//!
//! Trino's `varbinary` is Arrow `Binary` (the HTTP encoder already labels it
//! `varbinary` and base64-encodes it). Before this module `md5`, `to_utf8`,
//! `from_hex` and friends traded in hex or lossy varchar, so a Trino client saw
//! the wrong type and value. Every function here takes and returns the Trino
//! type of the same name.
//!
//! Two kinds of function share one row-wise engine ([`BinFn`]):
//!
//! - New functions (`sha1`, `crc32`, `xxhash64`, `murmur3`, `hmac_*`,
//!   `to_base64url`, `to_base32`, `to_ieee754_*`, `to_big_endian_*`, ...).
//! - Overrides of DataFusion built-ins (`md5`, `to_hex`, `lpad`, `rpad`,
//!   `substr`) that only take over when the first argument is binary and hand
//!   everything else (strings, integers) to the DataFusion function unchanged.
//!   They shadow the built-ins for Trino semantics: DataFusion's `md5` returns
//!   hex varchar, its `to_hex` is integer-only and its pad/substr are
//!   string-only.
//!
//! NULL in gives NULL out. Malformed input is an `Execution` error, never a
//! panic.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray, StringBuilder,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, FieldRef};
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD, URL_SAFE};
use base64::engine::DecodePaddingMode;
use base64::Engine as _;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Digest;

use crate::scalar_fns::{size_capped, SizeKind, MAX_GENERATED_SIZE};

/// Register every function in this module. Call after the DataFusion built-ins
/// are in the session: the overrides take their keys back from them.
pub fn register_binary_fns(ctx: &SessionContext) {
    for f in new_functions() {
        ctx.register_udf(ScalarUDF::new_from_impl(f));
    }
    // Overrides: a fresh built-in each time, so registering twice cannot nest.
    let overrides = [
        (
            "md5",
            datafusion::functions::crypto::md5(),
            BinFn::new("md5", BIN, DataType::Binary, md5),
        ),
        (
            "to_hex",
            datafusion::functions::string::to_hex(),
            BinFn::new("to_hex", BIN, DataType::Utf8, to_hex),
        ),
        (
            "lpad",
            Arc::new(size_capped(
                &datafusion::functions::unicode::lpad(),
                SizeKind::Pad,
            )),
            BinFn::new("lpad", BIN_PAD, DataType::Binary, lpad),
        ),
        (
            "rpad",
            Arc::new(size_capped(
                &datafusion::functions::unicode::rpad(),
                SizeKind::Pad,
            )),
            BinFn::new("rpad", BIN_PAD, DataType::Binary, rpad),
        ),
    ];
    for (_, inner, f) in overrides {
        ctx.register_udf(ScalarUDF::new_from_impl(f.with_inner(inner)));
    }
    ctx.register_udf(substr_udf());
}

fn substr_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(
        BinFn::new("substr", BIN_SUBSTR, DataType::Binary, substr)
            .with_inner(datafusion::functions::unicode::substr()),
    )
}

/// SQL `substr(..)` / `substring(..)` is parsed as a `SUBSTRING` expression,
/// which DataFusion's built-in planner turns straight into its own string-only
/// `substr`, bypassing the function registry. This planner runs first and
/// plans it to the dispatcher instead, which hands strings back unchanged.
#[derive(Debug)]
struct SubstringPlanner {
    udf: Arc<ScalarUDF>,
}

impl ExprPlanner for SubstringPlanner {
    fn plan_substring(&self, args: Vec<Expr>) -> DFResult<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(self.udf.clone(), args),
        )))
    }
}

/// Put [`SubstringPlanner`] ahead of the built-in planners. DataFusion offers
/// no way to prepend one, so the state is rebuilt, which re-registers every
/// function in map order and can hand an alias (`date_format`, `element_at`)
/// back to the built-in that also claims it. Call this FIRST, before any
/// override is registered, so there is nothing to lose; the `release_alias`
/// calls that follow settle those keys for good.
pub(crate) fn install_substring_planner(ctx: &SessionContext) {
    let udf = Arc::new(substr_udf());
    let state_ref = ctx.state_ref();
    let mut guard = state_ref.write();
    let mut planners: Vec<Arc<dyn ExprPlanner>> = vec![Arc::new(SubstringPlanner { udf })];
    planners.extend(guard.expr_planners().iter().cloned());
    *guard = SessionStateBuilder::new_from_existing(guard.clone())
        .with_expr_planners(planners)
        .build();
}

fn new_functions() -> Vec<BinFn> {
    let b = DataType::Binary;
    let s = DataType::Utf8;
    vec![
        // Hashes.
        BinFn::new("sha1", BIN, b.clone(), sha1),
        BinFn::new("crc32", BIN, DataType::Int64, crc32),
        BinFn::new("xxhash64", BIN, b.clone(), xxhash64),
        BinFn::new("murmur3", BIN, b.clone(), murmur3),
        BinFn::new("hmac_md5", BIN2, b.clone(), hmac_md5),
        BinFn::new("hmac_sha1", BIN2, b.clone(), hmac_sha1),
        BinFn::new("hmac_sha256", BIN2, b.clone(), hmac_sha256),
        BinFn::new("hmac_sha512", BIN2, b.clone(), hmac_sha512),
        // UTF-8 and hex.
        BinFn::new("to_utf8", STR, b.clone(), to_utf8),
        BinFn::new("from_utf8", FROM_UTF8, s.clone(), from_utf8),
        BinFn::new("from_hex", STR, b.clone(), from_hex),
        // Base64 / base32. The encoders also take varchar (its UTF-8 bytes),
        // which SQE accepted before; Trino itself requires varbinary.
        BinFn::new("to_base64", BIN_OR_STR, s.clone(), to_base64),
        BinFn::new("from_base64", BIN_OR_STR, b.clone(), from_base64),
        BinFn::new("to_base64url", BIN_OR_STR, s.clone(), to_base64url),
        BinFn::new("from_base64url", BIN_OR_STR, b.clone(), from_base64url),
        BinFn::new("to_base32", BIN_OR_STR, s, to_base32),
        BinFn::new("from_base32", BIN_OR_STR, b.clone(), from_base32),
        // IEEE 754 and big-endian integers.
        BinFn::new("to_ieee754_32", REAL, b.clone(), to_ieee754_32),
        BinFn::new("to_ieee754_64", DOUBLE, b.clone(), to_ieee754_64),
        BinFn::new("from_ieee754_32", BIN, DataType::Float32, from_ieee754_32),
        BinFn::new("from_ieee754_64", BIN, DataType::Float64, from_ieee754_64),
        BinFn::new("to_big_endian_32", INT32, b.clone(), to_big_endian_32),
        BinFn::new("to_big_endian_64", INT64, b, to_big_endian_64),
        BinFn::new(
            "from_big_endian_32",
            BIN,
            DataType::Int32,
            from_big_endian_32,
        ),
        BinFn::new(
            "from_big_endian_64",
            BIN,
            DataType::Int64,
            from_big_endian_64,
        ),
    ]
}

// ---------------------------------------------------------------------------
// Row-wise engine
// ---------------------------------------------------------------------------

/// What one argument position accepts, and the Arrow type it is cast to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Arg {
    Bin,
    BinOrStr,
    Str,
    Int32,
    Int64,
    Real,
    Double,
}

const BIN: &[&[Arg]] = &[&[Arg::Bin]];
const BIN2: &[&[Arg]] = &[&[Arg::Bin, Arg::Bin]];
const STR: &[&[Arg]] = &[&[Arg::Str]];
const BIN_OR_STR: &[&[Arg]] = &[&[Arg::BinOrStr]];
const REAL: &[&[Arg]] = &[&[Arg::Real]];
const DOUBLE: &[&[Arg]] = &[&[Arg::Double]];
const INT32: &[&[Arg]] = &[&[Arg::Int32]];
const INT64: &[&[Arg]] = &[&[Arg::Int64]];
const FROM_UTF8: &[&[Arg]] = &[&[Arg::Bin], &[Arg::Bin, Arg::Str], &[Arg::Bin, Arg::Int64]];
const BIN_PAD: &[&[Arg]] = &[&[Arg::Bin, Arg::Int64, Arg::Bin]];
const BIN_SUBSTR: &[&[Arg]] = &[&[Arg::Bin, Arg::Int64], &[Arg::Bin, Arg::Int64, Arg::Int64]];

fn is_binary(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

fn is_string(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_integer(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_float(dt: &DataType) -> bool {
    matches!(dt, DataType::Float32 | DataType::Float64)
}

impl Arg {
    fn accepts(self, dt: &DataType) -> bool {
        if *dt == DataType::Null {
            return true;
        }
        match self {
            Arg::Bin => is_binary(dt),
            Arg::BinOrStr => is_binary(dt) || is_string(dt),
            Arg::Str => is_string(dt),
            Arg::Int32 | Arg::Int64 => is_integer(dt),
            // Trino coerces decimal literals to real/double implicitly, which
            // SQE types as double, so a double is accepted for `real`.
            Arg::Real => is_float(dt),
            Arg::Double => is_float(dt) || is_integer(dt),
        }
    }

    fn target(self) -> DataType {
        match self {
            Arg::Bin | Arg::BinOrStr => DataType::Binary,
            Arg::Str => DataType::Utf8,
            Arg::Int32 => DataType::Int32,
            Arg::Int64 => DataType::Int64,
            Arg::Real => DataType::Float32,
            Arg::Double => DataType::Float64,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Arg::Bin => "varbinary",
            Arg::BinOrStr => "varbinary",
            Arg::Str => "varchar",
            Arg::Int32 => "integer",
            Arg::Int64 => "bigint",
            Arg::Real => "real",
            Arg::Double => "double",
        }
    }
}

/// One argument value for one row.
enum In<'a> {
    Bin(&'a [u8]),
    Str(&'a str),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

/// One result value for one row.
enum Out {
    Bin(Vec<u8>),
    Str(String),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

type RowFn = fn(&[In<'_>]) -> DFResult<Out>;

fn bad_kind() -> DataFusionError {
    DataFusionError::Internal("binary function: argument kind mismatch".into())
}

fn bin<'a>(a: &[In<'a>], i: usize) -> DFResult<&'a [u8]> {
    match a.get(i) {
        Some(In::Bin(b)) => Ok(b),
        _ => Err(bad_kind()),
    }
}

fn text<'a>(a: &[In<'a>], i: usize) -> DFResult<&'a str> {
    match a.get(i) {
        Some(In::Str(s)) => Ok(s),
        _ => Err(bad_kind()),
    }
}

fn int(a: &[In<'_>], i: usize) -> DFResult<i64> {
    match a.get(i) {
        Some(In::I64(v)) => Ok(*v),
        Some(In::I32(v)) => Ok(i64::from(*v)),
        _ => Err(bad_kind()),
    }
}

/// A cast argument column, addressable by row.
enum ArgCol {
    Bin(BinaryArray),
    Str(StringArray),
    I32(Int32Array),
    I64(Int64Array),
    F32(Float32Array),
    F64(Float64Array),
}

impl ArgCol {
    fn new(kind: Arg, arg: &ColumnarValue, rows: usize) -> DFResult<Self> {
        let arr = arg.to_array(rows)?;
        // `safe: false`: an out-of-range integer is an error, not a silent NULL.
        let opts = CastOptions {
            safe: false,
            ..Default::default()
        };
        let arr = cast_with_options(&arr, &kind.target(), &opts)?;
        let mismatch = || bad_kind();
        Ok(match kind {
            Arg::Bin | Arg::BinOrStr => {
                Self::Bin(arr.as_binary_opt::<i32>().ok_or_else(mismatch)?.clone())
            }
            Arg::Str => Self::Str(arr.as_string_opt::<i32>().ok_or_else(mismatch)?.clone()),
            Arg::Int32 => Self::I32(
                arr.as_primitive_opt::<arrow::datatypes::Int32Type>()
                    .ok_or_else(mismatch)?
                    .clone(),
            ),
            Arg::Int64 => Self::I64(
                arr.as_primitive_opt::<arrow::datatypes::Int64Type>()
                    .ok_or_else(mismatch)?
                    .clone(),
            ),
            Arg::Real => Self::F32(
                arr.as_primitive_opt::<arrow::datatypes::Float32Type>()
                    .ok_or_else(mismatch)?
                    .clone(),
            ),
            Arg::Double => Self::F64(
                arr.as_primitive_opt::<arrow::datatypes::Float64Type>()
                    .ok_or_else(mismatch)?
                    .clone(),
            ),
        })
    }

    fn len(&self) -> usize {
        match self {
            Self::Bin(a) => a.len(),
            Self::Str(a) => a.len(),
            Self::I32(a) => a.len(),
            Self::I64(a) => a.len(),
            Self::F32(a) => a.len(),
            Self::F64(a) => a.len(),
        }
    }

    fn get(&self, i: usize) -> Option<In<'_>> {
        match self {
            Self::Bin(a) => (!a.is_null(i)).then(|| In::Bin(a.value(i))),
            Self::Str(a) => (!a.is_null(i)).then(|| In::Str(a.value(i))),
            Self::I32(a) => (!a.is_null(i)).then(|| In::I32(a.value(i))),
            Self::I64(a) => (!a.is_null(i)).then(|| In::I64(a.value(i))),
            Self::F32(a) => (!a.is_null(i)).then(|| In::F32(a.value(i))),
            Self::F64(a) => (!a.is_null(i)).then(|| In::F64(a.value(i))),
        }
    }
}

fn build_output(ret: &DataType, outs: Vec<Option<Out>>) -> DFResult<ArrayRef> {
    Ok(match ret {
        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for o in outs {
                match o {
                    Some(Out::Bin(v)) => b.append_value(v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for o in outs {
                match o {
                    Some(Out::Str(v)) => b.append_value(v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => Arc::new(Int32Array::from_iter(outs.into_iter().map(|o| match o {
            Some(Out::I32(v)) => Some(v),
            _ => None,
        }))),
        DataType::Int64 => Arc::new(Int64Array::from_iter(outs.into_iter().map(|o| match o {
            Some(Out::I64(v)) => Some(v),
            _ => None,
        }))),
        DataType::Float32 => Arc::new(Float32Array::from_iter(outs.into_iter().map(|o| match o {
            Some(Out::F32(v)) => Some(v),
            _ => None,
        }))),
        DataType::Float64 => Arc::new(Float64Array::from_iter(outs.into_iter().map(|o| match o {
            Some(Out::F64(v)) => Some(v),
            _ => None,
        }))),
        other => {
            return Err(DataFusionError::Internal(format!(
                "binary function: unsupported return type {other}"
            )))
        }
    })
}

/// A scalar UDF over `varbinary`-family arguments, optionally layered over a
/// DataFusion function that handles every non-binary first argument.
#[derive(Debug)]
struct BinFn {
    name: &'static str,
    variants: &'static [&'static [Arg]],
    ret: DataType,
    f: RowFn,
    inner: Option<Arc<ScalarUDF>>,
    signature: Signature,
}

// A function is identified by its name; the fn pointer is not compared.
impl PartialEq for BinFn {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for BinFn {}
impl std::hash::Hash for BinFn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl BinFn {
    fn new(
        name: &'static str,
        variants: &'static [&'static [Arg]],
        ret: DataType,
        f: RowFn,
    ) -> Self {
        Self {
            name,
            variants,
            ret,
            f,
            inner: None,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn with_inner(mut self, inner: Arc<ScalarUDF>) -> Self {
        self.inner = Some(inner);
        self
    }

    /// The DataFusion function that owns this call, when the first argument is
    /// not binary.
    fn delegate(&self, first: Option<&DataType>) -> Option<&Arc<ScalarUDF>> {
        match (&self.inner, first) {
            (Some(inner), Some(dt)) if !is_binary(dt) => Some(inner),
            (Some(inner), None) => Some(inner),
            _ => None,
        }
    }

    /// The first variant whose arity and argument kinds fit `types`.
    fn variant_for(&self, types: &[DataType]) -> Option<&'static [Arg]> {
        self.variants
            .iter()
            .copied()
            .find(|v| v.len() == types.len() && v.iter().zip(types).all(|(a, t)| a.accepts(t)))
    }

    fn usage(&self) -> String {
        self.variants
            .iter()
            .map(|v| {
                let types: Vec<&str> = v.iter().map(|a| a.label()).collect();
                format!("{}({})", self.name, types.join(", "))
            })
            .collect::<Vec<_>>()
            .join(" or ")
    }

    fn mismatch(&self, arg_types: &[DataType]) -> DataFusionError {
        let actual: Vec<String> = arg_types.iter().map(trino_name).collect();
        DataFusionError::Plan(format!(
            "Unexpected parameters ({}) for function {}. Expected: {}",
            actual.join(", "),
            self.name,
            self.usage()
        ))
    }
}

fn trino_name(dt: &DataType) -> String {
    if is_binary(dt) {
        "varbinary".into()
    } else {
        crate::helpers::trino_type_name(dt)
    }
}

impl ScalarUDFImpl for BinFn {
    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        self.inner.as_ref().map_or(&[], |i| i.aliases())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if let Some(inner) = self.delegate(arg_types.first()) {
            let fields: Vec<FieldRef> = arg_types
                .iter()
                .map(|t| Arc::new(Field::new("a", t.clone(), true)))
                .collect();
            return Ok(fields_with_udf(&fields, inner.as_ref())?
                .iter()
                .map(|f| f.data_type().clone())
                .collect());
        }
        let variant = self
            .variant_for(arg_types)
            .ok_or_else(|| self.mismatch(arg_types))?;
        Ok(variant.iter().map(|a| a.target()).collect())
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match self.delegate(arg_types.first()) {
            Some(inner) => inner.inner().return_type(arg_types),
            None => Ok(self.ret.clone()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let first = args.args.first().map(|a| a.data_type());
        if let Some(inner) = self.delegate(first.as_ref()) {
            return inner.invoke_with_args(args);
        }
        let types: Vec<DataType> = args.args.iter().map(|a| a.data_type()).collect();
        let variant = self
            .variant_for(&types)
            .ok_or_else(|| self.mismatch(&types))?;
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let rows = if all_scalar { 1 } else { args.number_rows };
        let cols = variant
            .iter()
            .zip(&args.args)
            .map(|(kind, a)| ArgCol::new(*kind, a, rows))
            .collect::<DFResult<Vec<_>>>()?;
        let n = cols.first().map_or(rows, ArgCol::len);

        let mut outs: Vec<Option<Out>> = Vec::with_capacity(n);
        let mut cells: Vec<In<'_>> = Vec::with_capacity(cols.len());
        for i in 0..n {
            cells.clear();
            for c in &cols {
                match c.get(i) {
                    Some(v) => cells.push(v),
                    None => break,
                }
            }
            outs.push(if cells.len() == cols.len() {
                Some((self.f)(&cells)?)
            } else {
                None
            });
        }
        let arr = build_output(&self.ret, outs)?;
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
        } else {
            Ok(ColumnarValue::Array(arr))
        }
    }
}

// ---------------------------------------------------------------------------
// Hashes
// ---------------------------------------------------------------------------

fn md5(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Bin(md5::Md5::digest(bin(a, 0)?).as_slice().to_vec()))
}

fn sha1(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Bin(sha1::Sha1::digest(bin(a, 0)?).as_slice().to_vec()))
}

fn crc32(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::I64(i64::from(crc32fast::hash(bin(a, 0)?))))
}

/// Seed 0, big-endian, as Trino's `xxhash64` writes it.
fn xxhash64(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Bin(
        twox_hash::XxHash64::oneshot(0, bin(a, 0)?)
            .to_be_bytes()
            .to_vec(),
    ))
}

/// MurmurHash3 x64 128-bit, seed 0. Bytes are `h1` then `h2`, each
/// little-endian: the layout of the reference C++ implementation, which is
/// what airlift's `Murmur3Hash128` writes.
fn murmur3(a: &[In<'_>]) -> DFResult<Out> {
    let h = murmur3::murmur3_x64_128(&mut Cursor::new(bin(a, 0)?), 0)
        .map_err(|e| DataFusionError::Execution(format!("murmur3: {e}")))?;
    Ok(Out::Bin(h.to_le_bytes().to_vec()))
}

fn hmac_with<M: Mac + KeyInit>(a: &[In<'_>]) -> DFResult<Out> {
    let mut mac = <M as KeyInit>::new_from_slice(bin(a, 1)?)
        .map_err(|e| DataFusionError::Execution(format!("hmac: invalid key: {e}")))?;
    mac.update(bin(a, 0)?);
    Ok(Out::Bin(mac.finalize().into_bytes().as_slice().to_vec()))
}

fn hmac_md5(a: &[In<'_>]) -> DFResult<Out> {
    hmac_with::<Hmac<md5::Md5>>(a)
}
fn hmac_sha1(a: &[In<'_>]) -> DFResult<Out> {
    hmac_with::<Hmac<sha1::Sha1>>(a)
}
fn hmac_sha256(a: &[In<'_>]) -> DFResult<Out> {
    hmac_with::<Hmac<sha2::Sha256>>(a)
}
fn hmac_sha512(a: &[In<'_>]) -> DFResult<Out> {
    hmac_with::<Hmac<sha2::Sha512>>(a)
}

// ---------------------------------------------------------------------------
// UTF-8, hex, base64, base32
// ---------------------------------------------------------------------------

fn to_utf8(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Bin(text(a, 0)?.as_bytes().to_vec()))
}

/// Decode UTF-8, replacing each maximal invalid subsequence with `replacement`.
fn decode_utf8(mut bytes: &[u8], replacement: &str) -> String {
    let mut out = String::with_capacity(bytes.len());
    loop {
        match std::str::from_utf8(bytes) {
            Ok(s) => {
                out.push_str(s);
                return out;
            }
            Err(e) => {
                let (valid, rest) = bytes.split_at(e.valid_up_to());
                out.push_str(std::str::from_utf8(valid).unwrap_or_default());
                out.push_str(replacement);
                match e.error_len() {
                    Some(skip) => bytes = &rest[skip..],
                    // Truncated sequence at the end of the input.
                    None => return out,
                }
            }
        }
    }
}

fn from_utf8(a: &[In<'_>]) -> DFResult<Out> {
    let bytes = bin(a, 0)?;
    let mut buf = [0u8; 4];
    let replacement: &str = match a.get(1) {
        None => "\u{FFFD}",
        Some(In::Str(s)) => {
            if s.chars().count() > 1 {
                return Err(DataFusionError::Execution(
                    "Replacement string must be empty or a single character".into(),
                ));
            }
            s
        }
        Some(In::I64(cp)) => {
            let c = u32::try_from(*cp)
                .ok()
                .and_then(char::from_u32)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Invalid replacement code point: {cp}"))
                })?;
            c.encode_utf8(&mut buf)
        }
        Some(_) => return Err(bad_kind()),
    };
    Ok(Out::Str(decode_utf8(bytes, replacement)))
}

/// Upper-case hex, as Trino's `to_hex`.
fn to_hex(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Str(hex::encode_upper(bin(a, 0)?)))
}

fn from_hex(a: &[In<'_>]) -> DFResult<Out> {
    hex::decode(text(a, 0)?)
        .map(Out::Bin)
        .map_err(|e| DataFusionError::Execution(format!("from_hex: {e}")))
}

/// Java's decoders accept missing padding, which Trino inherits.
const STANDARD_LENIENT: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_padding_mode(DecodePaddingMode::Indifferent)
        .with_decode_allow_trailing_bits(true),
);
const URL_LENIENT: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::URL_SAFE,
    GeneralPurposeConfig::new()
        .with_decode_padding_mode(DecodePaddingMode::Indifferent)
        .with_decode_allow_trailing_bits(true),
);

/// Bytes of a `varbinary` or `varchar` argument (the UTF-8 bytes for text).
/// Both arrive cast to `Binary`, so this is the binary view either way.
fn input_bytes<'a>(a: &[In<'a>]) -> DFResult<&'a [u8]> {
    bin(a, 0)
}

fn to_base64(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Str(STANDARD.encode(input_bytes(a)?)))
}

fn from_base64(a: &[In<'_>]) -> DFResult<Out> {
    STANDARD_LENIENT
        .decode(input_bytes(a)?)
        .map(Out::Bin)
        .map_err(|e| DataFusionError::Execution(format!("from_base64: {e}")))
}

/// URL-safe alphabet, padded: Java's `Base64.getUrlEncoder()`.
fn to_base64url(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Str(URL_SAFE.encode(input_bytes(a)?)))
}

fn from_base64url(a: &[In<'_>]) -> DFResult<Out> {
    URL_LENIENT
        .decode(input_bytes(a)?)
        .map(Out::Bin)
        .map_err(|e| DataFusionError::Execution(format!("from_base64url: {e}")))
}

const BASE32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// RFC 4648 base32 with `=` padding.
fn base32_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(5) * 8);
    for chunk in data.chunks(5) {
        let mut buf = [0u8; 8];
        buf[3..3 + chunk.len()].copy_from_slice(chunk);
        let n = u64::from_be_bytes(buf);
        let chars = (chunk.len() * 8).div_ceil(5);
        for i in 0..8 {
            if i < chars {
                out.push(char::from(BASE32[((n >> (35 - 5 * i)) & 31) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// RFC 4648 base32 decode: upper-case alphabet, optional `=` padding.
fn base32_decode(s: &[u8]) -> DFResult<Vec<u8>> {
    let err = |m: &str| DataFusionError::Execution(format!("from_base32: {m}"));
    let end = s.iter().rposition(|&c| c != b'=').map_or(0, |p| p + 1);
    let body = &s[..end];
    if end < s.len() && !s.len().is_multiple_of(8) {
        return Err(err("incorrect padding"));
    }
    if !matches!(body.len() % 8, 0 | 2 | 4 | 5 | 7) {
        return Err(err("invalid input length"));
    }
    let mut out = Vec::with_capacity(body.len() * 5 / 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &c in body {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'2'..=b'7' => c - b'2' + 26,
            _ => return Err(err(&format!("illegal character {:?}", char::from(c)))),
        };
        acc = (acc << 5) | u32::from(v);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(out)
}

fn to_base32(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Str(base32_encode(input_bytes(a)?)))
}

fn from_base32(a: &[In<'_>]) -> DFResult<Out> {
    base32_decode(input_bytes(a)?).map(Out::Bin)
}

// ---------------------------------------------------------------------------
// IEEE 754 and big-endian integers
// ---------------------------------------------------------------------------

fn fixed<const N: usize>(name: &str, a: &[In<'_>]) -> DFResult<[u8; N]> {
    let b = bin(a, 0)?;
    <[u8; N]>::try_from(b).map_err(|_| {
        DataFusionError::Execution(format!(
            "{name}: input must be exactly {N} bytes, got {}",
            b.len()
        ))
    })
}

fn to_ieee754_32(a: &[In<'_>]) -> DFResult<Out> {
    match a.first() {
        Some(In::F32(v)) => Ok(Out::Bin(v.to_be_bytes().to_vec())),
        _ => Err(bad_kind()),
    }
}

fn to_ieee754_64(a: &[In<'_>]) -> DFResult<Out> {
    match a.first() {
        Some(In::F64(v)) => Ok(Out::Bin(v.to_be_bytes().to_vec())),
        _ => Err(bad_kind()),
    }
}

fn from_ieee754_32(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::F32(f32::from_be_bytes(fixed("from_ieee754_32", a)?)))
}

fn from_ieee754_64(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::F64(f64::from_be_bytes(fixed("from_ieee754_64", a)?)))
}

fn to_big_endian_32(a: &[In<'_>]) -> DFResult<Out> {
    let v = i32::try_from(int(a, 0)?)
        .map_err(|_| DataFusionError::Execution("to_big_endian_32: out of range".into()))?;
    Ok(Out::Bin(v.to_be_bytes().to_vec()))
}

fn to_big_endian_64(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::Bin(int(a, 0)?.to_be_bytes().to_vec()))
}

fn from_big_endian_32(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::I32(i32::from_be_bytes(fixed(
        "from_big_endian_32",
        a,
    )?)))
}

fn from_big_endian_64(a: &[In<'_>]) -> DFResult<Out> {
    Ok(Out::I64(i64::from_be_bytes(fixed(
        "from_big_endian_64",
        a,
    )?)))
}

// ---------------------------------------------------------------------------
// lpad / rpad / substr over varbinary
// ---------------------------------------------------------------------------

/// Shared body of `lpad` / `rpad`: truncate to `size` bytes or pad up to it.
fn pad(name: &str, a: &[In<'_>], left: bool) -> DFResult<Out> {
    let data = bin(a, 0)?;
    let size = int(a, 1)?;
    let filler = bin(a, 2)?;
    let Ok(size) = usize::try_from(size) else {
        return Err(DataFusionError::Execution(format!(
            "{name}: target size must be in the range [0..)"
        )));
    };
    if size as u64 > MAX_GENERATED_SIZE {
        return Err(DataFusionError::Execution(format!(
            "{name}: requested size {size} exceeds the limit of {MAX_GENERATED_SIZE}"
        )));
    }
    if size <= data.len() {
        return Ok(Out::Bin(data[..size].to_vec()));
    }
    if filler.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "{name}: padding bytes must not be empty"
        )));
    }
    let fill = (0..size - data.len()).map(|i| filler[i % filler.len()]);
    let mut out = Vec::with_capacity(size);
    if left {
        out.extend(fill);
        out.extend_from_slice(data);
    } else {
        out.extend_from_slice(data);
        out.extend(fill);
    }
    Ok(Out::Bin(out))
}

fn lpad(a: &[In<'_>]) -> DFResult<Out> {
    pad("lpad", a, true)
}

fn rpad(a: &[In<'_>]) -> DFResult<Out> {
    pad("rpad", a, false)
}

/// `substr(binary, start[, length])`: 1-based, a negative start counts from
/// the end, start 0 or past the end gives an empty result.
fn substr(a: &[In<'_>]) -> DFResult<Out> {
    let data = bin(a, 0)?;
    let start = int(a, 1)?;
    let len = if a.len() > 2 { Some(int(a, 2)?) } else { None };
    let n = data.len() as i64;
    let idx = match start {
        0 => return Ok(Out::Bin(Vec::new())),
        s if s > 0 => s - 1,
        s => n.saturating_add(s),
    };
    if idx < 0 || idx >= n || len.is_some_and(|l| l <= 0) {
        return Ok(Out::Bin(Vec::new()));
    }
    let take = len.map_or(n - idx, |l| l.min(n - idx));
    let (idx, take) = (idx as usize, take as usize);
    Ok(Out::Bin(data[idx..idx + take].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::concat;

    /// Run `sql` and return its first column across all batches.
    async fn q(sql: &str) -> ArrayRef {
        let ctx = crate::duckdb_test_ctx();
        crate::register_trino_functions(&ctx);
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let cols: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
        concat(&cols).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    async fn err(sql: &str) -> String {
        let ctx = crate::duckdb_test_ctx();
        crate::register_trino_functions(&ctx);
        match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => match df.collect().await {
                Err(e) => e.to_string(),
                Ok(_) => panic!("{sql}: expected an error"),
            },
        }
    }

    /// One binary value as lower-case hex (`None` for NULL); checks the type.
    async fn hexval(sql: &str) -> Option<String> {
        let arr = q(sql).await;
        assert_eq!(arr.data_type(), &DataType::Binary, "{sql}");
        (!arr.is_null(0)).then(|| hex::encode(arr.as_binary::<i32>().value(0)))
    }

    async fn strval(sql: &str) -> Option<String> {
        let arr = q(sql).await;
        let arr = arrow::compute::cast(&arr, &DataType::Utf8).unwrap();
        (!arr.is_null(0)).then(|| arr.as_string::<i32>().value(0).to_string())
    }

    async fn hex_col(sql: &str) -> Vec<Option<String>> {
        let arr = q(sql).await;
        assert_eq!(arr.data_type(), &DataType::Binary, "{sql}");
        let b = arr.as_binary::<i32>();
        (0..b.len())
            .map(|i| (!b.is_null(i)).then(|| hex::encode(b.value(i))))
            .collect()
    }

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    const HELLO: &str = "to_utf8('hello')";

    #[tokio::test]
    async fn md5_binary_is_varbinary_and_string_stays_hex() {
        assert_eq!(
            hexval(&format!("SELECT md5({HELLO})")).await,
            s("5d41402abc4b2a76b9719d911017c592")
        );
        // varchar input keeps DataFusion's hex varchar.
        assert_eq!(
            strval("SELECT md5('hello')").await,
            s("5d41402abc4b2a76b9719d911017c592")
        );
        let t = q("SELECT md5('hello')").await;
        assert!(is_string(t.data_type()), "{:?}", t.data_type());
        assert_eq!(hexval("SELECT md5(to_utf8(NULL))").await, None);
    }

    #[tokio::test]
    async fn sha_family_returns_varbinary() {
        assert_eq!(
            hexval(&format!("SELECT sha1({HELLO})")).await,
            s("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d")
        );
        assert_eq!(
            hexval(&format!("SELECT sha256({HELLO})")).await,
            s("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        );
        assert_eq!(
            hexval(&format!("SELECT sha512({HELLO})"))
                .await
                .map(|h| h.len()),
            Some(128)
        );
        assert_eq!(hexval("SELECT sha1(to_utf8(NULL))").await, None);
    }

    #[tokio::test]
    async fn crc32_is_bigint() {
        let arr = q(&format!("SELECT crc32({HELLO})")).await;
        assert_eq!(arr.data_type(), &DataType::Int64);
        assert_eq!(
            arr.as_primitive::<arrow::datatypes::Int64Type>().value(0),
            907060870
        );
        let arr = q("SELECT crc32(to_utf8(''))").await;
        assert_eq!(
            arr.as_primitive::<arrow::datatypes::Int64Type>().value(0),
            0
        );
    }

    #[tokio::test]
    async fn xxhash64_is_big_endian_seed_zero() {
        assert_eq!(
            hexval("SELECT xxhash64(to_utf8(''))").await,
            s("ef46db3751d8e999")
        );
        assert_eq!(
            hexval(&format!("SELECT xxhash64({HELLO})")).await,
            s("26c7827d889f6da3")
        );
    }

    #[tokio::test]
    async fn murmur3_is_x64_128_seed_zero() {
        assert_eq!(
            hexval("SELECT murmur3(to_utf8(''))").await,
            s("00000000000000000000000000000000")
        );
        // h1 = 0xcbd8a7b341bd9b02, h2 = 0x5b1e906a48ae1d19, each little-endian.
        assert_eq!(
            hexval(&format!("SELECT murmur3({HELLO})")).await,
            s("029bbd41b3a7d8cb191dae486a901e5b")
        );
    }

    #[tokio::test]
    async fn hmac_functions_match_rfc_vectors() {
        let data = "to_utf8('what do ya want for nothing?')";
        let key = "to_utf8('Jefe')";
        assert_eq!(
            hexval(&format!("SELECT hmac_md5({data}, {key})")).await,
            s("750c783e6ab0b503eaa86e310a5db738")
        );
        assert_eq!(
            hexval(&format!("SELECT hmac_sha1({data}, {key})")).await,
            s("effcdf6ae5eb2fa2d27416d5f184df9c259a7c79")
        );
        assert_eq!(
            hexval(&format!("SELECT hmac_sha256({data}, {key})")).await,
            s("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")
        );
        assert_eq!(
            hexval(&format!("SELECT hmac_sha512({data}, {key})")).await,
            s("164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea2505549758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737")
        );
        assert_eq!(
            hexval(&format!("SELECT hmac_sha256({data}, to_utf8(NULL))")).await,
            None
        );
    }

    #[tokio::test]
    async fn utf8_round_trip() {
        assert_eq!(hexval("SELECT to_utf8('héllo')").await, s("68c3a96c6c6f"));
        assert_eq!(
            strval("SELECT from_utf8(to_utf8('héllo'))").await,
            s("héllo")
        );
        let arr = q("SELECT from_utf8(from_hex('48656c6c6f'))").await;
        assert_eq!(arr.data_type(), &DataType::Utf8);
        assert_eq!(strval("SELECT to_utf8(CAST(NULL AS VARCHAR))").await, None);
    }

    #[tokio::test]
    async fn from_utf8_replaces_invalid_bytes() {
        let bad = "from_hex('48ff49')";
        assert_eq!(
            strval(&format!("SELECT from_utf8({bad})")).await,
            s("H\u{FFFD}I")
        );
        assert_eq!(
            strval(&format!("SELECT from_utf8({bad}, '?')")).await,
            s("H?I")
        );
        assert_eq!(
            strval(&format!("SELECT from_utf8({bad}, '')")).await,
            s("HI")
        );
        assert_eq!(
            strval(&format!("SELECT from_utf8({bad}, 63)")).await,
            s("H?I")
        );
        // A truncated multi-byte sequence at the end is one replacement.
        assert_eq!(
            strval("SELECT from_utf8(from_hex('41e282'))").await,
            s("A\u{FFFD}")
        );
        assert!(err(&format!("SELECT from_utf8({bad}, 'ab')"))
            .await
            .contains("single character"));
        assert!(err(&format!("SELECT from_utf8({bad}, 1114112)"))
            .await
            .contains("code point"));
    }

    #[tokio::test]
    async fn hex_functions() {
        assert_eq!(
            hexval("SELECT from_hex('68656c6C6f')").await,
            s("68656c6c6f")
        );
        let up = q(&format!("SELECT to_hex({HELLO})")).await;
        assert_eq!(up.data_type(), &DataType::Utf8);
        assert_eq!(
            strval(&format!("SELECT to_hex({HELLO})")).await,
            s("68656C6C6F")
        );
        // The integer path is DataFusion's.
        assert_eq!(strval("SELECT to_hex(255)").await, s("ff"));
        assert!(err("SELECT from_hex('abc')").await.contains("from_hex"));
        assert!(err("SELECT from_hex('zz')").await.contains("from_hex"));
        assert_eq!(hexval("SELECT from_hex(CAST(NULL AS VARCHAR))").await, None);
    }

    #[tokio::test]
    async fn base64_functions() {
        assert_eq!(
            strval(&format!("SELECT to_base64({HELLO})")).await,
            s("aGVsbG8=")
        );
        // varchar input (SQE's earlier signature) encodes its UTF-8 bytes.
        assert_eq!(strval("SELECT to_base64('hello')").await, s("aGVsbG8="));
        assert_eq!(
            hexval("SELECT from_base64('aGVsbG8=')").await,
            s("68656c6c6f")
        );
        // Unpadded input is accepted, like Java's decoder.
        assert_eq!(
            hexval("SELECT from_base64('aGVsbG8')").await,
            s("68656c6c6f")
        );
        // The varbinary overload.
        assert_eq!(
            hexval("SELECT from_base64(to_utf8('aGVsbG8='))").await,
            s("68656c6c6f")
        );
        assert!(err("SELECT from_base64('a$b')")
            .await
            .contains("from_base64"));
        assert_eq!(strval("SELECT to_base64(to_utf8(NULL))").await, None);
    }

    #[tokio::test]
    async fn base64url_functions() {
        assert_eq!(
            strval("SELECT to_base64url(from_hex('fbff'))").await,
            s("-_8=")
        );
        assert_eq!(
            strval("SELECT to_base64(from_hex('fbff'))").await,
            s("+/8=")
        );
        assert_eq!(hexval("SELECT from_base64url('-_8=')").await, s("fbff"));
        assert_eq!(hexval("SELECT from_base64url('-_8')").await, s("fbff"));
        assert!(err("SELECT from_base64url('+/8=')")
            .await
            .contains("from_base64url"));
    }

    #[tokio::test]
    async fn base32_functions() {
        for (plain, enc) in [
            ("", ""),
            ("f", "MY======"),
            ("fo", "MZXQ===="),
            ("foo", "MZXW6==="),
            ("foob", "MZXW6YQ="),
            ("fooba", "MZXW6YTB"),
            ("foobar", "MZXW6YTBOI======"),
        ] {
            assert_eq!(
                strval(&format!("SELECT to_base32(to_utf8('{plain}'))")).await,
                s(enc),
                "encode {plain:?}"
            );
            assert_eq!(
                hexval(&format!("SELECT from_base32('{enc}')")).await,
                s(&hex::encode(plain)),
                "decode {enc:?}"
            );
        }
        assert!(err("SELECT from_base32('MZ1Q====')")
            .await
            .contains("from_base32"));
        assert!(err("SELECT from_base32('M=')")
            .await
            .contains("from_base32"));
    }

    #[tokio::test]
    async fn ieee754_and_big_endian() {
        assert_eq!(
            hexval("SELECT to_ieee754_32(CAST(1.0 AS REAL))").await,
            s("3f800000")
        );
        assert_eq!(
            hexval("SELECT to_ieee754_64(1.0)").await,
            s("3ff0000000000000")
        );
        let f = q("SELECT from_ieee754_32(from_hex('3f800000'))").await;
        assert_eq!(f.data_type(), &DataType::Float32);
        assert_eq!(
            f.as_primitive::<arrow::datatypes::Float32Type>().value(0),
            1.0
        );
        let d = q("SELECT from_ieee754_64(to_ieee754_64(-2.5))").await;
        assert_eq!(d.data_type(), &DataType::Float64);
        assert_eq!(
            d.as_primitive::<arrow::datatypes::Float64Type>().value(0),
            -2.5
        );
        assert!(err("SELECT from_ieee754_32(from_hex('3f80'))")
            .await
            .contains("4 bytes"));
        assert!(err("SELECT from_ieee754_64(from_hex('3f80'))")
            .await
            .contains("8 bytes"));

        assert_eq!(hexval("SELECT to_big_endian_32(1)").await, s("00000001"));
        assert_eq!(hexval("SELECT to_big_endian_32(-2)").await, s("fffffffe"));
        assert_eq!(
            hexval("SELECT to_big_endian_64(258)").await,
            s("0000000000000102")
        );
        let i = q("SELECT from_big_endian_32(from_hex('fffffffe'))").await;
        assert_eq!(i.data_type(), &DataType::Int32);
        assert_eq!(i.as_primitive::<arrow::datatypes::Int32Type>().value(0), -2);
        let l = q("SELECT from_big_endian_64(to_big_endian_64(-9000000000))").await;
        assert_eq!(l.data_type(), &DataType::Int64);
        assert_eq!(
            l.as_primitive::<arrow::datatypes::Int64Type>().value(0),
            -9_000_000_000
        );
        assert!(err("SELECT from_big_endian_32(from_hex('00'))")
            .await
            .contains("4 bytes"));
        assert!(err("SELECT to_big_endian_32(4294967296)")
            .await
            .contains("rror"));
        assert_eq!(
            hexval("SELECT to_big_endian_64(CAST(NULL AS BIGINT))").await,
            None
        );
    }

    #[tokio::test]
    async fn pad_and_substr_on_varbinary() {
        let x = "from_hex('aabb')";
        assert_eq!(
            hexval(&format!("SELECT lpad({x}, 5, from_hex('00'))")).await,
            s("000000aabb")
        );
        assert_eq!(
            hexval(&format!("SELECT rpad({x}, 5, from_hex('0102'))")).await,
            s("aabb010201")
        );
        // Shorter target truncates on the right.
        assert_eq!(
            hexval(&format!("SELECT lpad({x}, 1, from_hex('00'))")).await,
            s("aa")
        );
        assert_eq!(
            hexval(&format!("SELECT rpad({x}, 0, from_hex('00'))")).await,
            s("")
        );
        assert!(err(&format!("SELECT lpad({x}, 5, from_hex(''))"))
            .await
            .contains("must not be empty"));
        assert!(err(&format!("SELECT rpad({x}, -1, from_hex('00'))"))
            .await
            .contains("range"));

        let y = "from_hex('0102030405')";
        assert_eq!(
            hexval(&format!("SELECT substr({y}, 2, 3)")).await,
            s("020304")
        );
        assert_eq!(
            hexval(&format!("SELECT substr({y}, 2)")).await,
            s("02030405")
        );
        assert_eq!(hexval(&format!("SELECT substr({y}, -2)")).await, s("0405"));
        assert_eq!(hexval(&format!("SELECT substr({y}, 0)")).await, s(""));
        assert_eq!(hexval(&format!("SELECT substr({y}, 6)")).await, s(""));
        assert_eq!(
            hexval(&format!("SELECT substr({y}, 4, 10)")).await,
            s("0405")
        );
        assert_eq!(hexval(&format!("SELECT substr({y}, 2, 0)")).await, s(""));
        assert_eq!(
            hexval(&format!("SELECT substring({y}, 1, 1)")).await,
            s("01")
        );
        assert_eq!(hexval("SELECT substr(to_utf8(NULL), 1, 1)").await, None);
    }

    #[tokio::test]
    async fn varchar_pad_and_substr_are_untouched() {
        assert_eq!(strval("SELECT lpad('ab', 5, 'x')").await, s("xxxab"));
        assert_eq!(strval("SELECT rpad('ab', 5, 'xy')").await, s("abxyx"));
        assert_eq!(strval("SELECT lpad('ab', 4)").await, s("  ab"));
        assert_eq!(strval("SELECT substr('hello', 2, 3)").await, s("ell"));
        assert_eq!(strval("SELECT substring('hello', 2)").await, s("ello"));
        // The size cap still applies to the string path.
        assert!(err("SELECT lpad('a', 1000000000, 'x')")
            .await
            .contains("exceeds"));
        // ...and to the binary path.
        assert!(
            err("SELECT lpad(from_hex('aa'), 1000000000, from_hex('00'))")
                .await
                .contains("exceeds")
        );
    }

    #[tokio::test]
    async fn column_fed_inputs() {
        let t = "(VALUES (to_utf8('hello')), (to_utf8('')), (to_utf8(NULL))) t(x)";
        assert_eq!(
            hex_col(&format!("SELECT md5(x) FROM {t}")).await,
            vec![
                s("5d41402abc4b2a76b9719d911017c592"),
                s("d41d8cd98f00b204e9800998ecf8427e"),
                None
            ]
        );
        assert_eq!(
            hex_col(&format!("SELECT sha1(x) FROM {t}")).await[1],
            s("da39a3ee5e6b4b0d3255bfef95601890afd80709")
        );
        assert_eq!(
            hex_col(&format!("SELECT lpad(x, 7, from_hex('00')) FROM {t}")).await,
            vec![s("000068656c6c6f"), s("00000000000000"), None]
        );
        assert_eq!(
            hex_col(&format!("SELECT substr(x, 2, 2) FROM {t}")).await,
            vec![s("656c"), s(""), None]
        );
        let strs = q(&format!("SELECT to_hex(x) FROM {t}")).await;
        let strs = strs.as_string::<i32>();
        assert_eq!(strs.value(0), "68656C6C6F");
        assert_eq!(strs.value(1), "");
        assert!(strs.is_null(2));
        let enc = q(&format!("SELECT from_utf8(x) FROM {t}")).await;
        let enc = enc.as_string::<i32>();
        assert_eq!(
            (enc.value(0), enc.value(1), enc.is_null(2)),
            ("hello", "", true)
        );
    }
}
