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
//! step. Binding uses [`sqe_core::substitute_placeholders`], the one
//! comment-aware placeholder scanner shared with the Flight SQL
//! `DoPutPreparedStatement` path (finding 708).

use std::collections::HashMap;

use sqe_core::substitute_placeholders;
use sqlparser::ast::{Expr, UnaryOperator, Value};

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

/// Bind the parsed `USING` expressions of an `EXECUTE` into `template`'s `?`
/// placeholders. Each expression is rendered back to SQL text (`1`, `'foo'`,
/// `DATE '2020-01-01'`) and spliced by [`substitute_placeholders`], which
/// skips `?` inside strings, quoted identifiers, dollar quotes and `--` /
/// `/* */` comments, so `SELECT ? /* why? */` binds exactly one value. A
/// count mismatch is an error rather than a partial bind.
///
/// Only literals bind (see [`is_literal`]); a function call, column, subquery
/// or any other expression is refused, naming its position, so a parameter is
/// always a value and never SQL.
pub fn bind_execute(template: &str, params: &[Expr]) -> Result<String, String> {
    if let Some(position) = params.iter().position(|e| !is_literal(e)) {
        return Err(format!(
            "EXECUTE ... USING parameter {} is not a literal; only constants \
             (numbers, strings, booleans, NULL, typed literals such as DATE '...') bind",
            position + 1
        ));
    }
    let literals: Vec<String> = params.iter().map(Expr::to_string).collect();
    substitute_placeholders(template, &literals)
}

/// A constant `USING` argument: a number, a plain single-quoted string, a
/// boolean, NULL, a hex string, a typed literal (`DATE '...'`,
/// `DECIMAL '...'`), an interval over a literal, or a signed number. Strings
/// that render with another quoting (dollar, escape, raw, triple) are refused:
/// their text form depends on the dialect that reads it back.
fn is_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(v) => is_plain_value(&v.value),
        Expr::TypedString(ts) => matches!(ts.value.value, Value::SingleQuotedString(_)),
        Expr::Interval(interval) => is_literal(&interval.value),
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => matches!(expr.as_ref(), Expr::Value(v) if matches!(v.value, Value::Number(..))),
        Expr::Nested(inner) => is_literal(inner),
        _ => false,
    }
}

fn is_plain_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Number(..)
            | Value::SingleQuotedString(_)
            | Value::NationalStringLiteral(_)
            | Value::HexStringLiteral(_)
            | Value::Boolean(_)
            | Value::Null
    )
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

    #[test]
    fn bind_renders_expressions_as_sql_literals() {
        let out = bind_execute(
            "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?",
            &exprs("1, 'foo', DATE '2020-01-01'"),
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT * FROM t WHERE a = 1 AND b = 'foo' AND c = DATE '2020-01-01'"
        );
    }

    #[test]
    fn bind_without_parameters_returns_template() {
        assert_eq!(bind_execute("SELECT 1", &[]).unwrap(), "SELECT 1");
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
            bind_execute("SELECT ? /* why? */", &exprs("1")).unwrap(),
            "SELECT 1 /* why? */"
        );
        assert_eq!(
            bind_execute("SELECT ? -- really?\n", &exprs("1")).unwrap(),
            "SELECT 1 -- really?\n"
        );
        assert!(bind_execute("SELECT ? /* why? */", &exprs("1, 2")).is_err());
    }

    /// A string that looks like SQL stays one string literal: its quote is
    /// doubled on the way back to text, so the bound statement parses as one
    /// statement selecting the original value.
    #[test]
    fn bind_keeps_an_injection_string_a_single_literal() {
        let out = bind_execute("SELECT ?", &exprs("'x''; DROP TABLE t; --'")).unwrap();
        assert_eq!(out, "SELECT 'x''; DROP TABLE t; --'");
        let statements = Parser::parse_sql(&GenericDialect {}, &out).expect("parse bound");
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].to_string(), "SELECT 'x''; DROP TABLE t; --'");
    }

    /// `1-?` bound to `-5` reads `1-(-5)`, not the comment `1--5`.
    #[test]
    fn bind_negative_number_does_not_open_a_comment() {
        let out = bind_execute("SELECT 1-? AS v, ? AS w", &exprs("-5, 2")).unwrap();
        assert_eq!(out, "SELECT 1-(-5) AS v, 2 AS w");
    }

    #[test]
    fn bind_long_string_round_trips() {
        let value = "a'b".repeat(64 * 1024 / 3);
        let quoted = format!("'{}'", value.replace('\'', "''"));
        let out = bind_execute("SELECT ?", &exprs(&quoted)).unwrap();
        assert_eq!(out, format!("SELECT {quoted}"));
    }

    #[test]
    fn bind_accepts_typed_and_signed_literals() {
        let out = bind_execute(
            "SELECT ?, ?, ?, ?, ?, ?",
            &exprs(
                "DECIMAL '1.50', TIMESTAMP '2020-01-01 00:00:00', INTERVAL '1' DAY, +3, NULL, TRUE",
            ),
        )
        .unwrap();
        assert_eq!(
            out,
            "SELECT DECIMAL '1.50', TIMESTAMP '2020-01-01 00:00:00', INTERVAL '1' DAY, +3, NULL, true"
        );
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
            bind_execute("SELECT ?, ?", &exprs("'a,b', 2")).unwrap(),
            "SELECT 'a,b', 2"
        );
    }
}
