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
use sqlparser::ast::Expr;

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
pub fn bind_execute(template: &str, params: &[Expr]) -> Result<String, String> {
    let literals: Vec<String> = params.iter().map(Expr::to_string).collect();
    substitute_placeholders(template, &literals)
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

    #[test]
    fn bind_preserves_string_argument_with_comma() {
        assert_eq!(
            bind_execute("SELECT ?, ?", &exprs("'a,b', 2")).unwrap(),
            "SELECT 'a,b', 2"
        );
    }
}
