//! Trino string functions: the same value and the same Trino type.
//!
//! - Array-aware dispatchers. Trino overloads `concat`, `reverse` and
//!   `concat_ws` on arrays; DataFusion's built-ins are string-only. [`Dispatch`]
//!   picks the array or the string implementation from the argument types at
//!   plan time and hands everything else to the built-in unchanged.
//! - `repeat(element, n)` is an array function in Trino, for any element type.
//! - `strpos` / `position` / `index` (2 and 3 argument forms), `length` and
//!   `levenshtein_distance` return `bigint` (DataFusion: `Int32`).
//! - `split_part`, the 2-argument `replace`, `normalize`, `soundex`,
//!   `human_readable_seconds`, `luhn_check`, `format_number`, `parse_data_size`.
//!
//! Every function that replaces a DataFusion built-in claims every key the
//! built-in claimed (name and aliases), so a session rebuild cannot hand a key
//! back to the built-in (see `overrides`).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

use crate::helpers::{arg, lift1, lift2, lift3, F64Arg, I64Arg, RowArg, RowOut, StrArg};

/// Register everything in this module. Call after the DataFusion built-ins.
pub fn register_string_fns(ctx: &SessionContext) {
    // Trino semantics: concat / reverse / concat_ws / replace accept arrays or
    // the 2-argument form that the string built-ins lack.
    ctx.register_udf(Dispatch::new(
        &datafusion::functions::string::concat(),
        datafusion::functions_nested::concat::array_concat_udf()
            .as_ref()
            .clone(),
        |t| t.iter().any(is_list),
    ));
    ctx.register_udf(Dispatch::new(
        &datafusion::functions::unicode::reverse(),
        datafusion::functions_nested::reverse::array_reverse_udf()
            .as_ref()
            .clone(),
        |t| t.first().is_some_and(is_list),
    ));
    ctx.register_udf(Dispatch::new(
        &datafusion::functions::string::concat_ws(),
        ScalarUDF::from(ConcatWsArray),
        |t| t.len() == 2 && is_list(&t[1]),
    ));
    ctx.register_udf(Dispatch::new(
        &datafusion::functions::string::replace(),
        ScalarUDF::from(Replace2),
        |t| t.len() == 2,
    ));

    // Trino semantics: `repeat` builds an array of any element type.
    ctx.register_udf(ScalarUDF::from(Repeat {
        inner: datafusion::functions_nested::repeat::array_repeat_udf()
            .as_ref()
            .clone(),
    }));

    // Trino semantics: bigint results. Claims `instr`, `position` (the built-in
    // aliases) and `index`.
    ctx.register_udf(ScalarUDF::from(Strpos));
    // The built-in character_length claims `length` and `char_length`.
    ctx.register_udf(ScalarUDF::from(CastResult::new(
        &datafusion::functions::unicode::character_length(),
        None,
    )));
    ctx.register_udf(ScalarUDF::from(CastResult::new(
        &datafusion::functions::string::levenshtein(),
        Some("levenshtein_distance"),
    )));

    // Trino semantics: out-of-range index is NULL, index <= 0 is an error.
    ctx.register_udf(ScalarUDF::from(SplitPart));

    ctx.register_udf(ScalarUDF::from(Normalize));
    ctx.register_udf(ScalarUDF::from(Soundex));
    ctx.register_udf(ScalarUDF::from(HumanReadableSeconds));
    ctx.register_udf(ScalarUDF::from(LuhnCheck));
    ctx.register_udf(ScalarUDF::from(FormatNumber));
    ctx.register_udf(ScalarUDF::from(ParseDataSize));
}

fn is_list(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

// ─── Dispatch: array / alternate overload in front of a built-in ─────────────

/// A built-in with one extra overload. `use_alt` looks at the argument types
/// and decides which implementation plans and runs the call; the chosen one
/// does the coercion, the return type and the evaluation.
#[derive(Debug)]
struct Dispatch {
    name: String,
    aliases: Vec<String>,
    signature: Signature,
    base: ScalarUDF,
    alt: ScalarUDF,
    use_alt: fn(&[DataType]) -> bool,
}

// One dispatcher per name; the selector is a plain function, so identity is
// the name (comparing function pointers is not meaningful).
impl PartialEq for Dispatch {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Dispatch {}

impl std::hash::Hash for Dispatch {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Dispatch {
    // Returns the wrapped `ScalarUDF`, ready to register, not `Self`.
    #[allow(clippy::new_ret_no_self)]
    fn new(base: &ScalarUDF, alt: ScalarUDF, use_alt: fn(&[DataType]) -> bool) -> ScalarUDF {
        ScalarUDF::from(Self {
            name: base.name().to_string(),
            aliases: base.aliases().to_vec(),
            signature: Signature::user_defined(Volatility::Immutable),
            base: base.clone(),
            alt,
            use_alt,
        })
    }

    fn pick(&self, types: &[DataType]) -> &ScalarUDF {
        if (self.use_alt)(types) {
            &self.alt
        } else {
            &self.base
        }
    }
}

impl ScalarUDFImpl for Dispatch {
    fn name(&self) -> &str {
        &self.name
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let fields: Vec<FieldRef> = arg_types
            .iter()
            .map(|t| Arc::new(Field::new("f", t.clone(), true)))
            .collect();
        let coerced = fields_with_udf(&fields, self.pick(arg_types))?;
        Ok(coerced.iter().map(|f| f.data_type().clone()).collect())
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.pick(arg_types).return_type(arg_types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        self.pick(&types).return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let types: Vec<DataType> = args.args.iter().map(|a| a.data_type()).collect();
        self.pick(&types).invoke_with_args(args)
    }
}

/// `concat_ws(separator, array)`: join the elements, skipping NULLs.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ConcatWsArray;

impl ScalarUDFImpl for ConcatWsArray {
    fn name(&self) -> &str {
        "concat_ws"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        const EXPECTED: &str = "varchar, array(varchar)";
        let rows = args.number_rows;
        let sep: StrArg = arg("concat_ws", EXPECTED, &args.args, 0)?;
        let list = args.args[1].to_array(rows)?;
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let list = arrow::compute::cast(&list, &DataType::List(item))?;
        let list = list.as_list::<i32>();
        let mut out: Vec<Option<String>> = Vec::with_capacity(rows);
        for i in 0..rows {
            let (Some(sep), false) = (sep.get(i), list.is_null(i)) else {
                out.push(None);
                continue;
            };
            let values = list.value(i);
            let strings = values.as_string::<i32>();
            let parts: Vec<&str> = strings.iter().flatten().collect();
            out.push(Some(parts.join(sep)));
        }
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        if all_scalar {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                out.into_iter().next().flatten(),
            )));
        }
        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(out)) as ArrayRef
        ))
    }
}

/// `replace(string, search)`: remove every `search`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Replace2;

impl ScalarUDFImpl for Replace2 {
    fn name(&self) -> &str {
        "replace"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        const EXPECTED: &str = "varchar, varchar";
        let s: StrArg = arg("replace", EXPECTED, &args.args, 0)?;
        let search: StrArg = arg("replace", EXPECTED, &args.args, 1)?;
        lift2(&s, &search, |s, search| {
            Ok(Some(if search.is_empty() {
                s.to_string()
            } else {
                s.replace(search, "")
            }))
        })
    }
}

// ─── repeat ──────────────────────────────────────────────────────────────────

/// Trino `repeat(element, count)` -> `array(T)` for any element type.
/// DataFusion's `array_repeat` does the work; the result is capped at
/// `MAX_GENERATED_SIZE` (string elements count by byte length).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Repeat {
    inner: ScalarUDF,
}

impl Repeat {
    fn requested_size(args: &ScalarFunctionArgs) -> DFResult<u64> {
        let counts =
            arrow::compute::cast(&args.args[1].to_array(args.number_rows)?, &DataType::Int64)?;
        let counts = counts.as_primitive::<arrow::datatypes::Int64Type>();
        let element = args.args[0].to_array(args.number_rows)?;
        let element_bytes: Option<arrow::array::Int64Array> = match element.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Binary => {
                let lens = arrow::compute::kernels::length::length(&element)?;
                Some(
                    arrow::compute::cast(&lens, &DataType::Int64)?
                        .as_primitive::<arrow::datatypes::Int64Type>()
                        .clone(),
                )
            }
            _ => None,
        };
        let mut max = 0u64;
        for i in 0..counts.len() {
            if counts.is_null(i) {
                continue;
            }
            let bytes = element_bytes.as_ref().map_or(1, |b| {
                if b.is_null(i) {
                    1
                } else {
                    b.value(i).max(1) as u64
                }
            });
            max = max.max(bytes.saturating_mul(counts.value(i).max(0) as u64));
        }
        Ok(max)
    }
}

impl ScalarUDFImpl for Repeat {
    fn name(&self) -> &str {
        "repeat"
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        self.inner.return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let size = Self::requested_size(&args)?;
        if size > crate::scalar_fns::MAX_GENERATED_SIZE {
            return Err(DataFusionError::Execution(format!(
                "repeat: requested size {size} exceeds the limit of {}",
                crate::scalar_fns::MAX_GENERATED_SIZE
            )));
        }
        self.inner.invoke_with_args(args)
    }
}

// ─── bigint results over DataFusion built-ins ────────────────────────────────

/// A DataFusion function whose Int32 result is cast to Trino's bigint.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CastResult {
    inner: ScalarUDF,
    name: String,
    aliases: Vec<String>,
}

impl CastResult {
    /// `rename` gives the wrapper a new name; without it the built-in's name
    /// and aliases are kept (so every key it claimed stays ours).
    fn new(inner: &ScalarUDF, rename: Option<&str>) -> Self {
        match rename {
            Some(n) => Self {
                inner: inner.clone(),
                name: n.to_string(),
                aliases: Vec::new(),
            },
            None => Self {
                inner: inner.clone(),
                name: inner.name().to_string(),
                aliases: inner.aliases().to_vec(),
            },
        }
    }
}

impl ScalarUDFImpl for CastResult {
    fn name(&self) -> &str {
        &self.name
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.inner.inner().coerce_types(arg_types)
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalars: Vec<Option<&ScalarValue>> = args
            .args
            .iter()
            .map(|a| match a {
                ColumnarValue::Scalar(sv) => Some(sv),
                ColumnarValue::Array(_) => None,
            })
            .collect();
        let return_field = self.inner.return_field_from_args(ReturnFieldArgs {
            arg_fields: &args.arg_fields,
            scalar_arguments: &scalars,
        })?;
        let args = ScalarFunctionArgs {
            return_field,
            ..args
        };
        match self.inner.invoke_with_args(args)? {
            ColumnarValue::Scalar(sv) => Ok(ColumnarValue::Scalar(sv.cast_to(&DataType::Int64)?)),
            ColumnarValue::Array(a) => Ok(ColumnarValue::Array(arrow::compute::cast(
                &a,
                &DataType::Int64,
            )?)),
        }
    }
}

// ─── strpos / position / index ───────────────────────────────────────────────

/// Trino `strpos(string, substring[, instance])` -> bigint, 1-based in code
/// points, 0 when absent. `instance` picks the N-th occurrence; negative counts
/// from the end. Matches may overlap. An empty `substring` is position 1.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Strpos;

/// Byte offsets of every (overlapping) occurrence of `sub`, in order. Stops
/// after `limit` hits when given.
fn occurrences(s: &str, sub: &str, limit: Option<usize>) -> Vec<usize> {
    let mut out = Vec::new();
    let mut from = 0usize;
    while from <= s.len() {
        let Some(i) = s[from..].find(sub) else { break };
        let at = from + i;
        out.push(at);
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
        from = at + s[at..].chars().next().map_or(1, char::len_utf8);
    }
    out
}

fn strpos_of(s: &str, sub: &str, instance: i64) -> DFResult<i64> {
    if instance == 0 {
        return Err(DataFusionError::Execution(
            "'instance' must be a positive or negative number".into(),
        ));
    }
    if sub.is_empty() {
        return Ok(1);
    }
    let at = if instance > 0 {
        let hits = occurrences(s, sub, Some(instance as usize));
        hits.get(instance as usize - 1).copied()
    } else {
        let hits = occurrences(s, sub, None);
        let back = instance.unsigned_abs() as usize;
        hits.len().checked_sub(back).map(|i| hits[i])
    };
    Ok(at.map_or(0, |b| s[..b].chars().count() as i64 + 1))
}

impl ScalarUDFImpl for Strpos {
    fn name(&self) -> &str {
        "strpos"
    }
    fn aliases(&self) -> &[String] {
        static ALIASES: std::sync::LazyLock<Vec<String>> =
            std::sync::LazyLock::new(|| ["instr", "position", "index"].map(String::from).to_vec());
        &ALIASES
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
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        const EXPECTED: &str = "varchar, varchar";
        let s: StrArg = arg("strpos", EXPECTED, &args.args, 0)?;
        let sub: StrArg = arg("strpos", EXPECTED, &args.args, 1)?;
        if args.args.len() == 3 {
            let n: I64Arg = arg("strpos", "varchar, varchar, bigint", &args.args, 2)?;
            return lift3(&s, &sub, &n, |s, sub, n| strpos_of(s, sub, n).map(Some));
        }
        lift2(&s, &sub, |s, sub| strpos_of(s, sub, 1).map(Some))
    }
}

// ─── split_part ──────────────────────────────────────────────────────────────

/// Trino `split_part(string, delimiter, index)`: the `index`-th (1-based) piece,
/// NULL when there is no such piece, an error for `index <= 0`. An empty
/// delimiter splits into characters.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SplitPart;

fn split_part_of(s: &str, delim: &str, index: i64) -> DFResult<Option<String>> {
    if index <= 0 {
        return Err(DataFusionError::Execution(
            "Index must be greater than zero".into(),
        ));
    }
    let nth = (index - 1) as usize;
    if delim.is_empty() {
        return Ok(s.chars().nth(nth).map(String::from));
    }
    Ok(s.split(delim).nth(nth).map(str::to_string))
}

impl ScalarUDFImpl for SplitPart {
    fn name(&self) -> &str {
        "split_part"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(3, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        const EXPECTED: &str = "varchar, varchar, bigint";
        let s: StrArg = arg("split_part", EXPECTED, &args.args, 0)?;
        let d: StrArg = arg("split_part", EXPECTED, &args.args, 1)?;
        let n: I64Arg = arg("split_part", EXPECTED, &args.args, 2)?;
        lift3(&s, &d, &n, split_part_of)
    }
}

// ─── normalize, soundex ──────────────────────────────────────────────────────

/// Trino `normalize(string[, form])`; NFC by default. Trino spells the form as a
/// bare keyword (`NFD`); here it is a string (`'NFD'`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Normalize;

impl ScalarUDFImpl for Normalize {
    fn name(&self) -> &str {
        "normalize"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            )
        });
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use unicode_normalization::UnicodeNormalization;
        let s: StrArg = arg("normalize", "varchar[, varchar]", &args.args, 0)?;
        if args.args.len() == 1 {
            return lift1(&s, |s| Ok(Some(s.nfc().collect::<String>())));
        }
        let form: StrArg = arg("normalize", "varchar, varchar", &args.args, 1)?;
        lift2(&s, &form, |s, form| {
            Ok(Some(match form.to_ascii_uppercase().as_str() {
                "NFC" => s.nfc().collect::<String>(),
                "NFD" => s.nfd().collect::<String>(),
                "NFKC" => s.nfkc().collect::<String>(),
                "NFKD" => s.nfkd().collect::<String>(),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "Normalization form must be one of NFC, NFD, NFKC, NFKD, got '{other}'"
                    )))
                }
            }))
        })
    }
}

/// American Soundex: the first letter, then up to three digits, padded with
/// zeros. Letters with the same code that are adjacent, or separated only by H
/// or W, count once. Non-letters are skipped; no letters gives ''.
fn soundex_of(s: &str) -> String {
    let code = |c: char| -> char {
        match c {
            'B' | 'F' | 'P' | 'V' => '1',
            'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
            'D' | 'T' => '3',
            'L' => '4',
            'M' | 'N' => '5',
            'R' => '6',
            _ => '0',
        }
    };
    let mut letters = s
        .chars()
        .filter(char::is_ascii_alphabetic)
        .map(|c| c.to_ascii_uppercase());
    let Some(first) = letters.next() else {
        return String::new();
    };
    let mut out = String::with_capacity(4);
    out.push(first);
    let mut last = code(first);
    for c in letters {
        if out.len() == 4 {
            break;
        }
        if c == 'H' || c == 'W' {
            continue;
        }
        let k = code(c);
        if k != '0' && k != last {
            out.push(k);
        }
        last = k;
    }
    while out.len() < 4 {
        out.push('0');
    }
    out
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Soundex;

impl ScalarUDFImpl for Soundex {
    fn name(&self) -> &str {
        "soundex"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let s: StrArg = arg("soundex", "varchar", &args.args, 0)?;
        lift1(&s, |s| Ok(Some(soundex_of(s))))
    }
}

// ─── human_readable_seconds ──────────────────────────────────────────────────

/// `1 minute, 36 seconds`: whole seconds, zero units left out, `0 seconds`
/// for zero. Negative input is formatted by magnitude.
fn human_readable_seconds_of(value: f64) -> DFResult<String> {
    if !value.is_finite() {
        return Err(DataFusionError::Execution(format!(
            "Invalid argument for human_readable_seconds: {value}"
        )));
    }
    let mut left = value.abs().round() as u64;
    let mut parts: Vec<String> = Vec::new();
    for (unit, size) in [
        ("week", 604_800u64),
        ("day", 86_400),
        ("hour", 3_600),
        ("minute", 60),
        ("second", 1),
    ] {
        let n = left / size;
        left %= size;
        if n > 0 {
            parts.push(format!("{n} {unit}{}", if n == 1 { "" } else { "s" }));
        }
    }
    if parts.is_empty() {
        parts.push("0 seconds".into());
    }
    Ok(parts.join(", "))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct HumanReadableSeconds;

impl ScalarUDFImpl for HumanReadableSeconds {
    fn name(&self) -> &str {
        "human_readable_seconds"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let secs: F64Arg = arg("human_readable_seconds", "double", &args.args, 0)?;
        lift1(&secs, |s| human_readable_seconds_of(s).map(Some))
    }
}

// ─── luhn_check ──────────────────────────────────────────────────────────────

/// Trino `luhn_check(varchar)` -> boolean. NULL for an empty string or one
/// with a non-digit.
#[derive(Debug, PartialEq, Eq, Hash)]
struct LuhnCheck;

fn luhn_of(s: &str) -> Option<bool> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let sum: u32 = s
        .bytes()
        .rev()
        .enumerate()
        .map(|(i, b)| {
            let d = u32::from(b - b'0');
            if i % 2 == 1 {
                let x = d * 2;
                if x > 9 {
                    x - 9
                } else {
                    x
                }
            } else {
                d
            }
        })
        .sum();
    Some(sum.is_multiple_of(10))
}

impl ScalarUDFImpl for LuhnCheck {
    fn name(&self) -> &str {
        "luhn_check"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let s: StrArg = arg("luhn_check", "varchar", &args.args, 0)?;
        lift1(&s, |s| Ok(luhn_of(s)))
    }
}

// ─── format_number ───────────────────────────────────────────────────────────

/// Trino `format_number(bigint | double)` -> varchar with a K / M / B / T / Q
/// unit: 123 -> `123`, 1234 -> `1.23K`, 123456 -> `123K`, 1000000 -> `1M`.
/// Two decimals below 100 of the unit, none from 100, trailing zeros dropped.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FormatNumber;

fn format_number_of(value: f64) -> DFResult<String> {
    if !value.is_finite() {
        return Err(DataFusionError::Execution(format!(
            "Invalid argument for format_number: {value}"
        )));
    }
    const UNITS: [&str; 6] = ["", "K", "M", "B", "T", "Q"];
    let sign = if value < 0.0 { "-" } else { "" };
    let mut v = value.abs();
    let mut unit = 0;
    while v >= 1000.0 && unit < UNITS.len() - 1 {
        v /= 1000.0;
        unit += 1;
    }
    let render = |v: f64| {
        let text = if v < 100.0 {
            format!("{v:.2}")
        } else {
            format!("{v:.0}")
        };
        if text.contains('.') {
            text.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            text
        }
    };
    let mut text = render(v);
    // 999.9K rounds to 1000K: carry into the next unit.
    if text.parse::<f64>().is_ok_and(|r| r >= 1000.0) && unit < UNITS.len() - 1 {
        unit += 1;
        text = render(v / 1000.0);
    }
    Ok(format!("{sign}{text}{}", UNITS[unit]))
}

impl ScalarUDFImpl for FormatNumber {
    fn name(&self) -> &str {
        "format_number"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n: F64Arg = arg("format_number", "bigint | double", &args.args, 0)?;
        lift1(&n, |v| format_number_of(v).map(Some))
    }
}

// ─── parse_data_size ─────────────────────────────────────────────────────────

/// Trino `parse_data_size('2.3MB')` -> decimal(38, 0): bytes, 1024-based,
/// truncated. Units: B, kB, MB, GB, TB, PB, EB, ZB, YB.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ParseDataSize;

/// `decimal(38, 0)` row value.
struct Dec38(i128);

impl RowOut for Dec38 {
    fn to_scalar(v: Option<Self>) -> ScalarValue {
        ScalarValue::Decimal128(v.map(|d| d.0), 38, 0)
    }
    fn to_array(vals: Vec<Option<Self>>) -> ArrayRef {
        let arr: Decimal128Array = vals.into_iter().map(|v| v.map(|d| d.0)).collect();
        Arc::new(
            arr.with_precision_and_scale(38, 0)
                .expect("38,0 is a valid decimal"),
        )
    }
}

fn parse_data_size_of(text: &str) -> DFResult<i128> {
    let invalid = || DataFusionError::Execution(format!("Invalid data size: '{text}'"));
    let t = text.trim();
    let end = t
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(t.len());
    let (number, unit) = t.split_at(end);
    let unit = unit.trim_start();
    let shift: u32 = match unit {
        "B" => 0,
        "kB" => 10,
        "MB" => 20,
        "GB" => 30,
        "TB" => 40,
        "PB" => 50,
        "EB" => 60,
        "ZB" => 70,
        "YB" => 80,
        _ => return Err(invalid()),
    };
    let (int_part, frac_part) = match number.split_once('.') {
        Some((i, f)) if !i.is_empty() && !f.is_empty() => (i, f),
        None if !number.is_empty() => (number, ""),
        _ => return Err(invalid()),
    };
    if frac_part.len() > 30 || int_part.len() > 30 {
        return Err(invalid());
    }
    let digits: i128 = format!("{int_part}{frac_part}")
        .parse()
        .map_err(|_| invalid())?;
    let scale = 10i128.pow(frac_part.len() as u32);
    let bytes = digits
        .checked_mul(1i128 << shift)
        .ok_or_else(|| DataFusionError::Execution(format!("Data size too large: '{text}'")))?
        / scale;
    if bytes >= 10i128.pow(38) {
        return Err(DataFusionError::Execution(format!(
            "Data size too large: '{text}'"
        )));
    }
    Ok(bytes)
}

impl ScalarUDFImpl for ParseDataSize {
    fn name(&self) -> &str {
        "parse_data_size"
    }
    fn signature(&self) -> &Signature {
        static SIG: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIG
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Decimal128(38, 0))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let s: StrArg = arg("parse_data_size", "varchar", &args.args, 0)?;
        lift1(&s, |s| parse_data_size_of(s).map(|b| Some(Dec38(b))))
    }
}

#[cfg(test)]
mod tests;
