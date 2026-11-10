//! `to_json(x)`: the target of Trino's `CAST(x AS JSON)`.
//!
//! `sqe-sql::trino_compat` rewrites `CAST(x AS JSON)` to `to_json(x)`, so this
//! UDF carries Trino's cast semantics, checked against Trino 476:
//!
//! - compact text, no whitespace: `[1,null,3]`, `{"a":1}`
//! - a NULL input is SQL NULL, not the text `null`; nested NULLs are `null`
//! - arrays become JSON arrays, structs become objects keyed by field name,
//!   maps become objects with stringified keys sorted by key
//! - decimals keep their scale (`1.50`), doubles and reals use Java's
//!   `Double.toString` notation (`1.0E300`, `1.0E-4`), and NaN / Infinity
//!   become the strings `"NaN"` / `"Infinity"` / `"-Infinity"`
//! - dates and timestamps become strings: `"2024-01-02"`,
//!   `"2024-01-02 03:04:05.123"`
//!
//! A variant (an Iceberg V3 `JSON` or `VARIANT` column) is embedded as its
//! JSON value, at any depth (#848). Other JSON values travel as Utf8: the
//! result of `json_parse`, `json_extract`, `json_array_get` and `to_json`
//! carries the [`JSON_TEXT_MARKER`] field marker, so `to_json` returns that
//! text as it is, also through an alias, a CTE, a subquery or a view
//! (Trino's cast of a JSON value is the identity). [`JsonMarkerRule`] keeps
//! the marker on a `CASE` or `coalesce` whose results are all JSON. Any
//! other string, a stored V2 `JSON` column included, has none: those are
//! encoded as a JSON string.
//! `sqe-sql::trino_compat` rewrites `CAST(ARRAY[<json>, ...] AS JSON)` to
//! [`JsonArrayOfJson`], since an array literal drops the element marker.
//! Types Trino refuses to cast (varbinary, time, intervals) are written as
//! their quoted display text instead of failing.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use arrow::datatypes::{
    DataType, Field, FieldRef, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_err, DFSchema, Result as DFResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::scalar::ScalarValue;
use sqe_trino_protocol::encode;

pub(crate) use encode::{is_json_text, JSON_TEXT_MARKER};

/// The return field of a function whose Utf8 result is JSON text.
pub(crate) fn json_text_field(name: &str) -> FieldRef {
    Arc::new(
        Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([(
            JSON_TEXT_MARKER.to_string(),
            "true".to_string(),
        )])),
    )
}

/// to_json(x) — serialize any value to compact JSON text, as Trino's
/// `CAST(x AS JSON)` does. NULL in, NULL out.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ToJson;

impl ScalarUDFImpl for ToJson {
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

    fn return_field_from_args(&self, _: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(json_text_field(self.name()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let input = &args.args[0];
        // Already JSON (#848): the cast is the identity.
        if args.arg_fields.first().is_some_and(|f| is_json_text(f)) {
            return Ok(input.clone());
        }
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let mut array = input.to_array(if scalar { 1 } else { args.number_rows })?;
        if let DataType::Dictionary(_, value_type) = array.data_type() {
            array = arrow::compute::cast(&array, value_type)?;
        }
        let out = to_json_array(&array)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

fn to_json_array(array: &ArrayRef) -> DFResult<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 8);
    let mut text = String::new();
    for row in 0..array.len() {
        if is_null(array.as_ref(), row) {
            builder.append_null();
        } else {
            text.clear();
            write_json(array.as_ref(), row, &mut text)?;
            builder.append_value(&text);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// `CAST(ARRAY[<json>, ...] AS JSON)` (#848). An array literal drops the
/// [`JSON_TEXT_MARKER`] of its elements, so `sqe-sql::trino_compat` sends a
/// cast of an array whose elements are all JSON-returning calls (or NULL)
/// here: each element is written as the JSON text it is, a NULL element as
/// `null`, as Trino does for `array(json)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct JsonArrayOfJson;

impl ScalarUDFImpl for JsonArrayOfJson {
    fn name(&self) -> &str {
        "__sqe_json_array_of_json"
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(1), Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(json_text_field(self.name()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let input = &args.args[0];
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let array = input.to_array(if scalar { 1 } else { args.number_rows })?;
        let text_list = DataType::new_list(DataType::Utf8, true);
        let lists = match array.data_type() {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => {
                arrow::compute::cast(&array, &text_list)?
            }
            other => return exec_err!("__sqe_json_array_of_json expects an array, got {other}"),
        };
        let lists = lists.as_list::<i32>();
        let mut builder = StringBuilder::with_capacity(lists.len(), lists.len() * 16);
        let mut text = String::new();
        for row in 0..lists.len() {
            if lists.is_null(row) {
                builder.append_null();
                continue;
            }
            let values = lists.value(row);
            let values = values.as_string::<i32>();
            text.clear();
            text.push('[');
            for i in 0..values.len() {
                if i > 0 {
                    text.push(',');
                }
                text.push_str(if values.is_null(i) {
                    "null"
                } else {
                    values.value(i)
                });
            }
            text.push(']');
            builder.append_value(&text);
        }
        let out: ArrayRef = Arc::new(builder.finish());
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// `__sqe_json_text(x)`: `x` unchanged, with the [`JSON_TEXT_MARKER`] on its
/// field. [`JsonMarkerRule`] wraps a `CASE` or `coalesce` whose results are
/// all JSON in it, since DataFusion builds their field without metadata.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct JsonText;

/// Name of [`JsonText`].
const JSON_TEXT: &str = "__sqe_json_text";

impl ScalarUDFImpl for JsonText {
    fn name(&self) -> &str {
        JSON_TEXT
    }

    fn signature(&self) -> &Signature {
        static SIG: LazyLock<Signature> =
            LazyLock::new(|| Signature::new(TypeSignature::Any(1), Volatility::Immutable));
        &SIG
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(json_text_field(self.name()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

/// Keeps the JSON marker through `CASE`, `coalesce`, `nvl`, `ifnull`,
/// `nullif` and `nvl2` (#848). DataFusion gives those a field without
/// metadata, so `CAST(CASE .. THEN json_parse(s) END AS JSON)` quoted the
/// JSON text. When every result that is not a NULL literal is JSON text,
/// the expression is wrapped in [`JsonText`]. Runs after type coercion, so
/// a NULL branch is already `CAST(NULL AS Utf8)` and every result is Utf8.
#[derive(Debug, Default)]
pub struct JsonMarkerRule;

impl AnalyzerRule for JsonMarkerRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(mark_plan).data()
    }

    fn name(&self) -> &str {
        "sqe_json_marker"
    }
}

fn mark_plan(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
    let schema = merge_schema(&plan.inputs());
    let names = NamePreserver::new(&plan);
    let out = plan.map_expressions(|expr| {
        let name = names.save(&expr);
        let marked = expr.transform_up(|e| mark_expr(e, &schema))?;
        Ok(marked.update_data(|e| name.restore(e)))
    })?;
    if out.transformed {
        out.map_data(|plan| plan.recompute_schema())
    } else {
        Ok(out)
    }
}

fn mark_expr(expr: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let results: Vec<&Expr> = match &expr {
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, then)| then.as_ref())
            .chain(case.else_expr.as_deref())
            .collect(),
        Expr::ScalarFunction(f) => match f.name() {
            "coalesce" | "nvl" | "ifnull" => f.args.iter().collect(),
            "nullif" => f.args.iter().take(1).collect(),
            "nvl2" => f.args.iter().skip(1).collect(),
            _ => return Ok(Transformed::no(expr)),
        },
        _ => return Ok(Transformed::no(expr)),
    };
    let is_json = |e: &Expr| e.to_field(schema).is_ok_and(|(_, f)| is_json_text(&f));
    let mut any_json = false;
    for e in results {
        if is_null_literal(e) {
            continue;
        }
        if !is_json(e) {
            return Ok(Transformed::no(expr));
        }
        any_json = true;
    }
    let plain_utf8 = expr
        .to_field(schema)
        .is_ok_and(|(_, f)| f.data_type() == &DataType::Utf8 && !is_json_text(&f));
    if !any_json || !plain_utf8 {
        return Ok(Transformed::no(expr));
    }
    static UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| Arc::new(ScalarUDF::from(JsonText)));
    Ok(Transformed::yes(Expr::ScalarFunction(
        ScalarFunction::new_udf(Arc::clone(&UDF), vec![expr]),
    )))
}

/// `NULL`, or `NULL` cast by type coercion.
fn is_null_literal(e: &Expr) -> bool {
    match e {
        Expr::Literal(v, _) => v.is_null(),
        Expr::Cast(c) => is_null_literal(&c.expr),
        Expr::TryCast(c) => is_null_literal(&c.expr),
        _ => false,
    }
}

/// A NullArray reports no null buffer, so `is_null` alone says false for it.
fn is_null(array: &dyn Array, row: usize) -> bool {
    array.data_type() == &DataType::Null || array.is_null(row)
}

fn write_json(array: &dyn Array, row: usize, out: &mut String) -> DFResult<()> {
    if is_null(array, row) {
        out.push_str("null");
        return Ok(());
    }
    match array.data_type() {
        DataType::Boolean => {
            out.push_str(if array.as_boolean().value(row) {
                "true"
            } else {
                "false"
            });
        }
        DataType::Int8 => push_display(out, array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => push_display(out, array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => push_display(out, array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => push_display(out, array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => push_display(out, array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => push_display(out, array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => push_display(out, array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => push_display(out, array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float16 => {
            let v = array.as_primitive::<Float16Type>().value(row).to_f32();
            push_java_float(out, v)
        }
        DataType::Float32 => push_java_float(out, array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => push_java_double(out, array.as_primitive::<Float64Type>().value(row)),
        // Arrow's display text keeps the declared scale: 1.50, not 1.5.
        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => out.push_str(&display(array, row)?),
        DataType::Utf8 => push_string(out, array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => push_string(out, array.as_string::<i64>().value(row)),
        DataType::Utf8View => push_string(out, array.as_string_view().value(row)),
        DataType::Date32 | DataType::Date64 => push_string(out, &display(array, row)?),
        // Trino writes timestamps with a space, not ISO 8601's `T`.
        DataType::Timestamp(..) => push_string(out, &display(array, row)?.replacen('T', " ", 1)),
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            write_elements(&list.value(row), out)?;
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            write_elements(&list.value(row), out)?;
        }
        DataType::FixedSizeList(..) => {
            let list = array.as_fixed_size_list();
            write_elements(&list.value(row), out)?;
        }
        DataType::Struct(_) if encode::is_variant_storage(array.data_type()) => {
            let text = encode::variant_json(array, row)?;
            out.push_str(&text);
        }
        DataType::Struct(fields) => {
            let st = array.as_struct();
            out.push('{');
            for (i, field) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_string(out, field.name());
                out.push(':');
                write_json(st.column(i).as_ref(), row, out)?;
            }
            out.push('}');
        }
        DataType::Map(..) => write_map(array, row, out)?,
        DataType::Dictionary(_, value_type) => {
            let value = arrow::compute::cast(&array.slice(row, 1), value_type)?;
            write_json(value.as_ref(), 0, out)?;
        }
        _ => push_string(out, &display(array, row)?),
    }
    Ok(())
}

fn write_elements(values: &ArrayRef, out: &mut String) -> DFResult<()> {
    out.push('[');
    for i in 0..values.len() {
        if i > 0 {
            out.push(',');
        }
        write_json(values.as_ref(), i, out)?;
    }
    out.push(']');
    Ok(())
}

/// A map becomes an object. Trino stringifies the keys and orders the
/// entries by key text: `{"10":"x","9":"y"}`, `{"A":3,"B":1,"a":2}`.
fn write_map(array: &dyn Array, row: usize, out: &mut String) -> DFResult<()> {
    let map = array.as_map();
    let offsets = map.value_offsets();
    let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
    let keys = map.keys();
    let values = map.values();
    let mut entries = Vec::with_capacity(end - start);
    for i in start..end {
        entries.push((key_text(keys.as_ref(), i)?, i));
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    out.push('{');
    for (n, (key, i)) in entries.iter().enumerate() {
        if n > 0 {
            out.push(',');
        }
        push_string(out, key);
        out.push(':');
        write_json(values.as_ref(), *i, out)?;
    }
    out.push('}');
    Ok(())
}

/// The object key for a map key: a string as-is, anything else as its JSON
/// text with the quotes taken off.
fn key_text(keys: &dyn Array, i: usize) -> DFResult<String> {
    match keys.data_type() {
        DataType::Utf8 => return Ok(keys.as_string::<i32>().value(i).to_string()),
        DataType::LargeUtf8 => return Ok(keys.as_string::<i64>().value(i).to_string()),
        DataType::Utf8View => return Ok(keys.as_string_view().value(i).to_string()),
        _ => {}
    }
    let mut text = String::new();
    write_json(keys, i, &mut text)?;
    if text.starts_with('"') {
        if let Ok(unquoted) = serde_json::from_str::<String>(&text) {
            return Ok(unquoted);
        }
    }
    Ok(text)
}

fn display(array: &dyn Array, row: usize) -> DFResult<String> {
    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
    Ok(formatter.value(row).to_string())
}

fn push_display(out: &mut String, v: impl std::fmt::Display) {
    use std::fmt::Write;
    let _ = write!(out, "{v}");
}

fn push_string(out: &mut String, s: &str) {
    // serde_json escapes quotes, backslashes and control characters
    // (`\u0001`) and leaves other text as UTF-8, as Trino does.
    out.push_str(&serde_json::Value::from(s).to_string());
}

fn push_java_double(out: &mut String, v: f64) {
    if !v.is_finite() {
        return push_string(out, non_finite(v.is_nan(), v > 0.0));
    }
    out.push_str(&java_notation(
        v == 0.0 || (1e-3..1e7).contains(&v.abs()),
        format!("{v}"),
        format!("{v:e}"),
    ));
}

fn push_java_float(out: &mut String, v: f32) {
    if !v.is_finite() {
        return push_string(out, non_finite(v.is_nan(), v > 0.0));
    }
    out.push_str(&java_notation(
        v == 0.0 || (1e-3..1e7).contains(&v.abs()),
        format!("{v}"),
        format!("{v:e}"),
    ));
}

fn non_finite(nan: bool, positive: bool) -> &'static str {
    match (nan, positive) {
        (true, _) => "NaN",
        (false, true) => "Infinity",
        (false, false) => "-Infinity",
    }
}

/// Java's `Double.toString`: plain notation for 1e-3 <= |v| < 1e7 and
/// scientific (`1.0E300`) outside it, always with a fractional digit. Rust's
/// `{}` and `{:e}` already print the shortest round-tripping digits, as
/// Java 19+ does, so only the layout needs changing.
fn java_notation(plain_range: bool, plain: String, sci: String) -> String {
    if plain_range {
        return if plain.contains('.') {
            plain
        } else {
            plain + ".0"
        };
    }
    let (mantissa, exponent) = sci.split_once('e').unwrap_or((&sci, "0"));
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn double(v: f64) -> String {
        let mut s = String::new();
        push_java_double(&mut s, v);
        s
    }

    #[test]
    fn doubles_follow_java_notation() {
        assert_eq!(double(1.5), "1.5");
        assert_eq!(double(100.0), "100.0");
        assert_eq!(double(0.0), "0.0");
        assert_eq!(double(-0.0), "-0.0");
        assert_eq!(double(1e300), "1.0E300");
        assert_eq!(double(0.0001), "1.0E-4");
        assert_eq!(double(12345678.9), "1.23456789E7");
        assert_eq!(double(9999999.0), "9999999.0");
        assert_eq!(double(0.001), "0.001");
        assert_eq!(double(f64::NAN), "\"NaN\"");
        assert_eq!(double(f64::NEG_INFINITY), "\"-Infinity\"");
    }

    #[test]
    fn reals_follow_java_notation() {
        let mut s = String::new();
        push_java_float(&mut s, 1.5);
        assert_eq!(s, "1.5");
        s.clear();
        push_java_float(&mut s, 0.1);
        assert_eq!(s, "0.1");
    }
}
