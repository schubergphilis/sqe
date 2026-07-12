//! Trino prepared statements over HTTP -- stateless, header-driven.
//!
//! The Trino client carries prepared statements in the
//! `X-Trino-Prepared-Statement: name=<urlencoded-sql>` header on every request
//! (it accumulates them from the `X-Trino-Added-Prepare` response headers SQE
//! already emits). So the server needs no statement store: when a body is
//! `EXECUTE <name> [USING ...]` we look the template up in the header map and
//! substitute the `USING` arguments into its `?` placeholders. `EXECUTE
//! IMMEDIATE '<sql>' [USING ...]` carries the SQL inline.

use std::collections::HashMap;

use sqe_core::substitute_placeholders;

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

/// Strip a leading uppercase keyword (with trailing space) case-insensitively,
/// returning the case-preserved remainder.
fn strip_kw<'a>(s: &'a str, upper: &str, kw: &str) -> Option<&'a str> {
    if upper.starts_with(kw) {
        Some(&s[kw.len()..])
    } else {
        None
    }
}

/// Split `s` on top-level `delim`, ignoring delimiters inside single-quoted
/// strings or `()`/`[]` groups.
fn split_top_level(s: &str, delim: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth: i32 = 0;
    let mut in_single = false;
    let mut seg_start = 0usize;
    for (idx, c) in s.char_indices() {
        if in_single {
            if c == '\'' {
                in_single = false;
            }
            continue;
        }
        match c {
            '\'' => in_single = true,
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            _ if c == delim && depth == 0 => {
                out.push(s[seg_start..idx].to_string());
                seg_start = idx + c.len_utf8();
            }
            _ => {}
        }
    }
    out.push(s[seg_start..].to_string());
    out
}

/// Parse a leading single-quoted SQL string literal (with `''` escaping).
/// Returns the unescaped content and the remainder after the closing quote.
fn parse_string_literal(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    let mut chars = s.char_indices();
    if chars.next().map(|(_, c)| c) != Some('\'') {
        return None;
    }
    let mut content = String::new();
    while let Some((idx, c)) = chars.next() {
        if c == '\'' {
            let mut peek = chars.clone();
            if let Some((_, '\'')) = peek.next() {
                content.push('\'');
                chars.next();
                continue;
            }
            return Some((content, &s[idx + c.len_utf8()..]));
        }
        content.push(c);
    }
    None
}

/// Parse an optional trailing `USING a, b, ...` clause into raw SQL-literal
/// argument strings. An empty tail yields no arguments.
fn parse_using(after: &str) -> Result<Vec<String>, String> {
    let after = after.trim();
    if after.is_empty() {
        return Ok(vec![]);
    }
    let upper = after.to_uppercase();
    match strip_kw(after, &upper, "USING ") {
        Some(rest) => Ok(split_top_level(rest, ',')
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()),
        None => Err(format!("expected USING clause but found: {after}")),
    }
}

/// If `sql` is an `EXECUTE` statement, resolve it to concrete SQL; otherwise
/// return `Ok(None)` so the caller runs `sql` unchanged.
///
/// - `EXECUTE <name> [USING a, b, ...]` -> look up `<name>` (error if absent),
///   substitute the `USING` args into the template's `?` placeholders.
/// - `EXECUTE IMMEDIATE '<sql>' [USING ...]` -> bind into the inline SQL.
pub fn rewrite_execute(
    sql: &str,
    prepared: &HashMap<String, String>,
) -> Result<Option<String>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // EXECUTE IMMEDIATE '<sql>' [USING ...] -- inline SQL, checked first since
    // "EXECUTE IMMEDIATE " also starts with "EXECUTE ".
    if let Some(rest) = strip_kw(trimmed, &upper, "EXECUTE IMMEDIATE ") {
        let (inline_sql, after) = parse_string_literal(rest)
            .ok_or_else(|| "EXECUTE IMMEDIATE requires a quoted SQL string".to_string())?;
        let args = parse_using(after)?;
        return substitute_placeholders(&inline_sql, &args).map(Some);
    }

    // EXECUTE <name> [USING ...] -- resolve <name> from the header-carried map.
    if let Some(rest) = strip_kw(trimmed, &upper, "EXECUTE ") {
        let rest = rest.trim();
        let (name, after) = match rest.find(char::is_whitespace) {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, ""),
        };
        let name = name.trim();
        if name.is_empty() {
            return Err("EXECUTE requires a statement name".to_string());
        }
        let template = prepared.get(name).ok_or_else(|| {
            format!(
                "prepared statement '{name}' not found; the client must carry it \
                 in the X-Trino-Prepared-Statement header"
            )
        })?;
        let args = parse_using(after)?;
        return substitute_placeholders(template, &args).map(Some);
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
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
    fn non_execute_returns_none() {
        let m = HashMap::new();
        assert_eq!(rewrite_execute("SELECT 1", &m).unwrap(), None);
        assert_eq!(
            rewrite_execute("INSERT INTO t VALUES (1)", &m).unwrap(),
            None
        );
    }

    #[test]
    fn execute_named_substitutes_using_args() {
        let m = map(&[("q1", "SELECT * FROM t WHERE a = ? AND b = ?")]);
        let out = rewrite_execute("EXECUTE q1 USING 1, 'foo'", &m).unwrap();
        assert_eq!(
            out.as_deref(),
            Some("SELECT * FROM t WHERE a = 1 AND b = 'foo'")
        );
    }

    #[test]
    fn execute_named_without_using() {
        let m = map(&[("q1", "SELECT 1")]);
        assert_eq!(
            rewrite_execute("EXECUTE q1", &m).unwrap().as_deref(),
            Some("SELECT 1")
        );
    }

    #[test]
    fn execute_unknown_name_errors() {
        let m = HashMap::new();
        assert!(rewrite_execute("EXECUTE missing USING 1", &m).is_err());
    }

    #[test]
    fn execute_immediate_inline_sql() {
        let m = HashMap::new();
        let out = rewrite_execute("EXECUTE IMMEDIATE 'SELECT ? + ?' USING 1, 2", &m).unwrap();
        assert_eq!(out.as_deref(), Some("SELECT 1 + 2"));
    }

    #[test]
    fn execute_immediate_without_using() {
        let m = HashMap::new();
        let out = rewrite_execute("EXECUTE IMMEDIATE 'SELECT 1'", &m).unwrap();
        assert_eq!(out.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn using_arg_comma_inside_string_is_not_a_separator() {
        let m = map(&[("q", "SELECT ?, ?")]);
        let out = rewrite_execute("EXECUTE q USING 'a,b', 2", &m).unwrap();
        assert_eq!(out.as_deref(), Some("SELECT 'a,b', 2"));
    }

    #[test]
    fn execute_keyword_is_case_insensitive() {
        let m = map(&[("q", "SELECT 1")]);
        assert_eq!(
            rewrite_execute("execute q", &m).unwrap().as_deref(),
            Some("SELECT 1")
        );
    }

    #[test]
    fn trailing_semicolon_tolerated() {
        let m = map(&[("q", "SELECT 1")]);
        assert_eq!(
            rewrite_execute("EXECUTE q;", &m).unwrap().as_deref(),
            Some("SELECT 1")
        );
    }
}
