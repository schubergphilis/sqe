//! Trino prepared statements over HTTP -- stateless, header-driven.
//!
//! The Trino client carries prepared statements in the
//! `X-Trino-Prepared-Statement: name=<urlencoded-sql>` header on every request
//! (it accumulates them from the `X-Trino-Added-Prepare` response headers SQE
//! already emits). So the server needs no statement store: when a body is
//! `EXECUTE <name> [USING ...]` the server looks the template up in the header
//! map and binds the `USING` arguments into its `?` placeholders. `EXECUTE
//! IMMEDIATE '<sql>' [USING ...]` carries the SQL inline.
//!
//! The statement itself is parsed by the SQL front door
//! (`sqe_sql::StatementKind::Execute` carries the name and the already-parsed
//! `USING` expressions); this module only owns the header codec and the bind
//! step. The template's `?` placeholders are numbered by
//! [`sqe_core::number_placeholders`], the one comment-aware placeholder
//! scanner shared with the Flight SQL path (finding 708), and the `USING`
//! values are bound into the plan, never into the SQL text (finding 406).

use std::collections::HashMap;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion::scalar::ScalarValue;
use sqe_core::number_placeholders;
use sqlparser::ast::{
    DataType as SqlDataType, ExactNumberInfo, Expr, Interval, TimezoneInfo, UnaryOperator, Value,
};

/// Parse `X-Trino-Prepared-Statement` header value(s) into a name->SQL map.
///
/// Each header value is a comma-separated list of `name=urlencoded-sql`
/// entries. URL-encoding escapes embedded commas (`%2C`) and `=` (`%3D`) in the
/// SQL, so splitting on top-level `,` and the first `=` is unambiguous. The SQL
/// is form-urldecoded (`+`->space, `%XX`), matching the Java `URLEncoder` the
/// Trino client uses.
pub fn parse_prepared_statements(header_values: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for value in header_values {
        // Encoded SQL never contains a raw comma (it is escaped to %2C), so a
        // plain split is unambiguous.
        for entry in value.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            if let Some((name, encoded)) = entry.split_once('=') {
                let name = name.trim();
                if name.is_empty() {
                    continue;
                }
                map.insert(name.to_string(), form_urldecode(encoded.trim()));
            }
        }
    }
    map
}

/// Decode one `application/x-www-form-urlencoded` value (`+`->space, `%XX`),
/// matching the Java `URLEncoder` the Trino client uses on the emit side.
fn form_urldecode(s: &str) -> String {
    url::form_urlencoded::parse(s.as_bytes())
        .next()
        .map(|(k, _)| k.into_owned())
        .unwrap_or_default()
}

/// Encode SQL for the `X-Trino-Added-Prepare` response header, so a real Trino
/// client can replay it verbatim in `X-Trino-Prepared-Statement`.
pub fn form_urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// Turn an `EXECUTE` into a statement to plan and the values to bind into
/// its plan. `template`'s `?` placeholders become `$1..$N`
/// ([`number_placeholders`], which skips `?` inside strings, quoted
/// identifiers, dollar quotes and `--` / `/* */` comments, so
/// `SELECT ? /* why? */` takes exactly one value) and each `USING`
/// expression becomes a typed value. A count mismatch is an error rather
/// than a partial bind.
///
/// The values never become SQL text: they are substituted into the logical
/// plan after it is built, so a string is data however it is quoted, and
/// the statement text the engine logs and audits holds the placeholders,
/// not the values (finding 406).
///
/// Only literals bind (see [`literal_value`]); a function call, column,
/// subquery or any other expression is refused, naming its position, so a
/// parameter is always a value and never SQL. Errors name the position and
/// type, never the value.
pub fn bind_execute(template: &str, params: &[Expr]) -> Result<(String, Vec<ScalarValue>), String> {
    let values = params
        .iter()
        .enumerate()
        .map(|(i, expr)| literal_value(expr, i + 1))
        .collect::<Result<Vec<_>, _>>()?;
    let (numbered, count) = number_placeholders(template);
    if count != values.len() {
        return Err(format!(
            "prepared statement expected {count} parameters but bind supplied {}",
            values.len()
        ));
    }
    Ok((numbered, values))
}

/// The value of a constant `USING` argument, typed the way Trino types the
/// literal: an integer is `BIGINT`, `1.50` is `DECIMAL(3,2)`, `1e0` is
/// `DOUBLE`, a string is `VARCHAR`, `X'..'` is `VARBINARY`, and a typed
/// literal (`DATE '...'`, `DECIMAL '...'`, `TIMESTAMP '...'`) or an
/// interval over a literal is parsed into its type. Strings with another
/// quoting (dollar, escape, raw, triple) are refused, as is any
/// non-constant expression.
fn literal_value(expr: &Expr, position: usize) -> Result<ScalarValue, String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(text, _) => number_value(text, position),
            Value::SingleQuotedString(s) | Value::NationalStringLiteral(s) => {
                Ok(ScalarValue::Utf8(Some(s.clone())))
            }
            Value::HexStringLiteral(s) => hex::decode(s)
                .map(|bytes| ScalarValue::Binary(Some(bytes)))
                .map_err(|_| invalid_literal(position, "VARBINARY")),
            Value::Boolean(b) => Ok(ScalarValue::Boolean(Some(*b))),
            Value::Null => Ok(ScalarValue::Null),
            _ => Err(not_a_literal(position)),
        },
        Expr::TypedString(ts) => match &ts.value.value {
            Value::SingleQuotedString(text) => typed_value(&ts.data_type, text, position),
            _ => Err(not_a_literal(position)),
        },
        Expr::Interval(interval) => interval_value(interval, position),
        Expr::UnaryOp {
            op: op @ (UnaryOperator::Minus | UnaryOperator::Plus),
            expr,
        } => match signed_number(expr, matches!(op, UnaryOperator::Minus)) {
            Some(text) => number_value(&text, position),
            None => Err(not_a_literal(position)),
        },
        Expr::Nested(inner) => literal_value(inner, position),
        _ => Err(not_a_literal(position)),
    }
}

/// The text of a numeric literal under a sign, or `None` when `expr` is
/// anything else.
fn signed_number(expr: &Expr, negative: bool) -> Option<String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(text, _) if negative => Some(format!("-{text}")),
            Value::Number(text, _) => Some(text.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn not_a_literal(position: usize) -> String {
    format!(
        "EXECUTE ... USING parameter {position} is not a literal; only constants \
         (numbers, strings, booleans, NULL, typed literals such as DATE '...') bind"
    )
}

fn invalid_literal(position: usize, type_name: impl std::fmt::Display) -> String {
    format!("EXECUTE ... USING parameter {position} is not a valid {type_name} literal")
}

/// A numeric literal, optionally signed: `BIGINT` when it is an integer
/// that fits, `DOUBLE` with an exponent, otherwise a `DECIMAL` sized to its
/// digits.
fn number_value(text: &str, position: usize) -> Result<ScalarValue, String> {
    if text.contains(['e', 'E']) {
        return text
            .parse::<f64>()
            .map(|v| ScalarValue::Float64(Some(v)))
            .map_err(|_| invalid_literal(position, "DOUBLE"));
    }
    if let Ok(v) = text.parse::<i64>() {
        return Ok(ScalarValue::Int64(Some(v)));
    }
    decimal_value(text, None, position)
}

/// `text` as a `DECIMAL(precision, scale)`; without an explicit size, the
/// size Trino gives the literal (`1.50` is `DECIMAL(3,2)`).
fn decimal_value(
    text: &str,
    size: Option<(u64, i64)>,
    position: usize,
) -> Result<ScalarValue, String> {
    let text = text.trim();
    let (precision, scale) = match size {
        Some(size) => size,
        None => {
            let digits = text.trim_start_matches(['-', '+']);
            let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
            let scale = frac.len();
            let precision = (int.trim_start_matches('0').len() + scale).max(1);
            (precision as u64, scale as i64)
        }
    };
    let (Ok(precision), Ok(scale)) = (u8::try_from(precision), i8::try_from(scale)) else {
        return Err(invalid_literal(position, "DECIMAL"));
    };
    if precision == 0 || precision > 38 {
        return Err(invalid_literal(position, "DECIMAL"));
    }
    cast_text(text, &DataType::Decimal128(precision, scale))
        .ok_or_else(|| invalid_literal(position, format!("DECIMAL({precision},{scale})")))
}

/// A typed literal, `<type> '<text>'`, parsed into the type DataFusion
/// plans it as.
fn typed_value(
    data_type: &SqlDataType,
    text: &str,
    position: usize,
) -> Result<ScalarValue, String> {
    let target = match data_type {
        SqlDataType::Decimal(info) | SqlDataType::Numeric(info) | SqlDataType::Dec(info) => {
            let size = match info {
                ExactNumberInfo::None => None,
                ExactNumberInfo::Precision(p) => Some((*p, 0)),
                ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p, *s)),
            };
            return decimal_value(text, size, position);
        }
        SqlDataType::Date => DataType::Date32,
        SqlDataType::Time(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            DataType::Time64(TimeUnit::Nanosecond)
        }
        SqlDataType::Timestamp(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        }
        SqlDataType::Timestamp(_, _) => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        }
        SqlDataType::TinyInt(_) => DataType::Int8,
        SqlDataType::SmallInt(_) => DataType::Int16,
        SqlDataType::Int(_) | SqlDataType::Integer(_) => DataType::Int32,
        SqlDataType::BigInt(_) => DataType::Int64,
        SqlDataType::Real => DataType::Float32,
        SqlDataType::Double(_) | SqlDataType::DoublePrecision => DataType::Float64,
        SqlDataType::Boolean | SqlDataType::Bool => DataType::Boolean,
        SqlDataType::Varchar(_)
        | SqlDataType::CharVarying(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => return Ok(ScalarValue::Utf8(Some(text.to_string()))),
        other => {
            return Err(format!(
                "EXECUTE ... USING parameter {position}: a {other} literal does not bind; \
                 bind a string and cast the placeholder in the statement"
            ))
        }
    };
    cast_text(text, &target).ok_or_else(|| invalid_literal(position, data_type))
}

/// `INTERVAL '<n>' <unit>` (or `INTERVAL '<n> <unit>'`) as a month-day-nano
/// interval. A range (`'1-2' YEAR TO MONTH`) is refused.
fn interval_value(interval: &Interval, position: usize) -> Result<ScalarValue, String> {
    if interval.last_field.is_some() {
        return Err(format!(
            "EXECUTE ... USING parameter {position}: an interval range (... TO ...) does not bind"
        ));
    }
    let amount = match interval.value.as_ref() {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::Number(s, _) => s.clone(),
            _ => return Err(not_a_literal(position)),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => signed_number(expr, true).ok_or_else(|| not_a_literal(position))?,
        _ => return Err(not_a_literal(position)),
    };
    let text = match &interval.leading_field {
        Some(unit) => format!("{amount} {unit}"),
        None => amount,
    };
    cast_text(&text, &DataType::Interval(IntervalUnit::MonthDayNano))
        .ok_or_else(|| invalid_literal(position, "INTERVAL"))
}

/// `text` cast to `target`, or `None` when it does not parse as one.
fn cast_text(text: &str, target: &DataType) -> Option<ScalarValue> {
    ScalarValue::Utf8(Some(text.to_string()))
        .cast_to(target)
        .ok()
        .filter(|v| !v.is_null())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn exprs(list: &str) -> Vec<Expr> {
        let mut parser = Parser::new(&GenericDialect {})
            .try_with_sql(list)
            .expect("tokenize");
        parser
            .parse_comma_separated(Parser::parse_expr)
            .expect("parse expressions")
    }

    #[test]
    fn parses_single_url_encoded_statement() {
        // "SELECT * FROM t WHERE x = ?" url-encoded.
        let h = vec!["q1=SELECT+%2A+FROM+t+WHERE+x+%3D+%3F".to_string()];
        let m = parse_prepared_statements(&h);
        assert_eq!(
            m.get("q1").map(String::as_str),
            Some("SELECT * FROM t WHERE x = ?")
        );
    }

    #[test]
    fn parses_multiple_comma_separated() {
        let h = vec!["a=SELECT+1,b=SELECT+2".to_string()];
        let m = parse_prepared_statements(&h);
        assert_eq!(m.get("a").unwrap(), "SELECT 1");
        assert_eq!(m.get("b").unwrap(), "SELECT 2");
    }

    #[test]
    fn parses_multiple_header_lines() {
        let h = vec!["a=SELECT+1".to_string(), "b=SELECT+2".to_string()];
        let m = parse_prepared_statements(&h);
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn encoded_comma_inside_sql_is_not_a_separator() {
        // SELECT 1, 2 -> the literal comma is %2C, so it stays one statement.
        let h = vec!["a=SELECT+1%2C+2".to_string()];
        let m = parse_prepared_statements(&h);
        assert_eq!(m.get("a").unwrap(), "SELECT 1, 2");
        assert_eq!(m.len(), 1);
    }

    fn utf8(s: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(s.to_string()))
    }

    #[test]
    fn bind_numbers_the_template_and_types_the_values() {
        let (sql, values) = bind_execute(
            "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?",
            &exprs("1, 'foo', DATE '2020-01-01'"),
        )
        .unwrap();
        assert_eq!(sql, "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3");
        assert_eq!(
            values,
            vec![
                ScalarValue::Int64(Some(1)),
                utf8("foo"),
                ScalarValue::Date32(Some(18262)),
            ]
        );
    }

    #[test]
    fn bind_without_parameters_returns_template() {
        assert_eq!(
            bind_execute("SELECT 1", &[]).unwrap(),
            ("SELECT 1".to_string(), vec![])
        );
    }

    #[test]
    fn bind_count_mismatch_is_an_error() {
        assert!(bind_execute("SELECT ?, ?", &exprs("1")).is_err());
        assert!(bind_execute("SELECT ?", &exprs("1, 2")).is_err());
    }

    /// Finding 708 acceptance: a `?` inside a comment is not a placeholder, so
    /// `EXECUTE p USING 1` against `SELECT ? /* why? */` binds exactly one.
    #[test]
    fn bind_is_comment_aware() {
        assert_eq!(
            bind_execute("SELECT ? /* why? */", &exprs("1")).unwrap().0,
            "SELECT $1 /* why? */"
        );
        assert_eq!(
            bind_execute("SELECT ? -- really?\n", &exprs("1"))
                .unwrap()
                .0,
            "SELECT $1 -- really?\n"
        );
        assert!(bind_execute("SELECT ? /* why? */", &exprs("1, 2")).is_err());
    }

    /// A string that looks like SQL is a value, not text: the statement keeps
    /// its placeholder and the value is the string with its quote undoubled.
    #[test]
    fn bind_keeps_an_injection_string_a_value() {
        let (sql, values) = bind_execute("SELECT ?", &exprs("'x''; DROP TABLE t; --'")).unwrap();
        assert_eq!(sql, "SELECT $1");
        assert_eq!(values, vec![utf8("x'; DROP TABLE t; --")]);
    }

    /// `1-?` bound to `-5` is the value -5; there is no text for a `--` to
    /// form in.
    #[test]
    fn bind_negative_number_is_a_value() {
        let (sql, values) = bind_execute("SELECT 1-? AS v, ? AS w", &exprs("-5, 2")).unwrap();
        assert_eq!(sql, "SELECT 1-$1 AS v, $2 AS w");
        assert_eq!(
            values,
            vec![ScalarValue::Int64(Some(-5)), ScalarValue::Int64(Some(2))]
        );
    }

    #[test]
    fn bind_long_string_round_trips() {
        let value = "a'b".repeat(64 * 1024 / 3);
        let quoted = format!("'{}'", value.replace('\'', "''"));
        let (_, values) = bind_execute("SELECT ?", &exprs(&quoted)).unwrap();
        assert_eq!(values, vec![utf8(&value)]);
    }

    #[test]
    fn bind_accepts_typed_and_signed_literals() {
        let (_, values) = bind_execute(
            "SELECT ?, ?, ?, ?, ?, ?, ?",
            &exprs(
                "DECIMAL '1.50', TIMESTAMP '2020-01-01 00:00:00', INTERVAL '1' DAY, +3, NULL, \
                 TRUE, X'0aff'",
            ),
        )
        .unwrap();
        assert_eq!(
            values,
            vec![
                ScalarValue::Decimal128(Some(150), 3, 2),
                ScalarValue::TimestampNanosecond(Some(1_577_836_800_000_000_000), None),
                ScalarValue::IntervalMonthDayNano(Some(
                    arrow::datatypes::IntervalMonthDayNano::new(0, 1, 0)
                )),
                ScalarValue::Int64(Some(3)),
                ScalarValue::Null,
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Binary(Some(vec![0x0a, 0xff])),
            ]
        );
    }

    /// Numbers are typed as Trino types the literal: an integer that fits is
    /// BIGINT, an exponent makes a DOUBLE, any other number is a DECIMAL
    /// sized to its digits.
    #[test]
    fn bind_types_numbers_like_trino() {
        let (_, values) = bind_execute(
            "SELECT ?, ?, ?, ?, ?",
            &exprs("1.50, 1e0, -2.5E-3, 0.5, 99999999999999999999"),
        )
        .unwrap();
        assert_eq!(
            values,
            vec![
                ScalarValue::Decimal128(Some(150), 3, 2),
                ScalarValue::Float64(Some(1.0)),
                ScalarValue::Float64(Some(-0.0025)),
                ScalarValue::Decimal128(Some(5), 1, 1),
                ScalarValue::Decimal128(Some(99_999_999_999_999_999_999), 20, 0),
            ]
        );
    }

    /// A typed literal that does not parse as its type is refused by
    /// position and type; the error never repeats the value.
    #[test]
    fn bind_refuses_an_invalid_typed_literal_without_echoing_it() {
        for (list, expected) in [
            (
                "DATE 'secret-value'",
                "parameter 1 is not a valid DATE literal",
            ),
            (
                "DECIMAL(3,1) '12345.6'",
                "parameter 1 is not a valid DECIMAL(3,1) literal",
            ),
            ("X'secret'", "parameter 1 is not a valid VARBINARY literal"),
        ] {
            let err = bind_execute("SELECT ?", &exprs(list)).unwrap_err();
            assert!(err.contains(expected), "{list}: {err}");
            assert!(
                !err.contains("secret") && !err.contains("12345"),
                "{list}: {err}"
            );
        }
    }

    /// Portal request: a function call, a subquery, a column or an
    /// expression in `USING` is refused, naming the parameter position.
    #[test]
    fn bind_refuses_non_literal_expressions() {
        for (list, position) in [
            ("1, current_user", 2),
            ("(SELECT secret FROM s)", 1),
            ("1, 2, lower('A')", 3),
            ("1 + 1", 1),
            ("-col", 1),
            ("$1", 1),
        ] {
            let template = vec!["?"; position].join(", ");
            let err = bind_execute(&format!("SELECT {template}"), &exprs(list)).unwrap_err();
            assert!(
                err.contains(&format!("parameter {position} is not a literal")),
                "{list}: {err}"
            );
        }
    }

    #[test]
    fn bind_preserves_string_argument_with_comma() {
        assert_eq!(
            bind_execute("SELECT ?, ?", &exprs("'a,b', 2")).unwrap().1,
            vec![utf8("a,b"), ScalarValue::Int64(Some(2))]
        );
    }
}
