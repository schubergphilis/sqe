//! Trino nested column types in CREATE TABLE and ALTER TABLE ADD COLUMN.
//!
//! Trino spells nested types `array(bigint)`, `map(varchar, bigint)` and
//! `row(x decimal(10,2), y varchar)`; SHOW CREATE TABLE prints them that
//! way. sqlparser 0.62 (GenericDialect) rejects `ARRAY(..)` (it wants
//! `ARRAY<..>`), `MAP<..>` (it wants `MAP(..)`), and any ROW field type with
//! parameters (`Expected: type modifiers, found: (`).
//!
//! This module rewrites the column types of a CREATE TABLE column list, or
//! of the column an ALTER TABLE ADD COLUMN adds, into the spellings sqlparser
//! parses: `ARRAY<T>`, `MAP(K, V)` and `STRUCT<f T, ...>`. Only the type that
//! follows a column name is touched; DEFAULT expressions, constraints and a
//! CTAS query are copied as written. It also runs on a statement that parses:
//! `row(t timestamp with time zone)` parses as a `Custom` type whose flattened
//! modifier list cannot be split back into fields. The rewrite is returned
//! only when it parses.

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Rewrite Trino nested column types in CREATE TABLE / ALTER TABLE ADD
/// COLUMN; any other statement comes back unchanged.
pub fn rewrite_trino_ddl_types(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if !(lower.contains("array") || lower.contains("row") || lower.contains("map")) {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    match rewrite_statement(sql) {
        Some(candidate) if candidate != sql && Parser::parse_sql(&dialect, &candidate).is_ok() => {
            candidate
        }
        _ => sql.to_string(),
    }
}

fn rewrite_statement(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let (first, mut i) = next_word(sql, 0)?;
    match first.to_ascii_lowercase().as_str() {
        "create" => {
            // CREATE [OR REPLACE] [TEMP|TEMPORARY|EXTERNAL|TRANSIENT] TABLE
            loop {
                let (w, end) = next_word(sql, i)?;
                i = end;
                match w.to_ascii_lowercase().as_str() {
                    "table" => break,
                    "or" | "replace" | "temp" | "temporary" | "external" | "transient" => {}
                    _ => return None,
                }
            }
            i = skip_if_exists(sql, i, &["if", "not", "exists"]);
            i = skip_name(sql, i)?;
            i = skip_ws(b, i);
            if b.get(i) != Some(&b'(') {
                return None; // CTAS without a column list
            }
            let close = matching_bracket(b, i)?;
            let mut out = String::with_capacity(sql.len());
            out.push_str(&sql[..=i]);
            let mut pos = i + 1;
            while pos < close {
                let end = column_def_end(b, pos, close);
                out.push_str(&rewrite_column_def(&sql[pos..end])?);
                if end < close {
                    out.push(',');
                }
                pos = end + 1;
            }
            out.push_str(&sql[close..]);
            Some(out)
        }
        "alter" => {
            let (w, end) = next_word(sql, i)?;
            if !w.eq_ignore_ascii_case("table") {
                return None;
            }
            i = skip_if_exists(sql, end, &["if", "exists"]);
            i = skip_name(sql, i)?;
            let (w, end) = next_word(sql, i)?;
            if !w.eq_ignore_ascii_case("add") {
                return None;
            }
            i = end;
            if let Some((w, end)) = next_word(sql, i) {
                if w.eq_ignore_ascii_case("column") {
                    i = end;
                }
            }
            i = skip_if_exists(sql, i, &["if", "not", "exists"]);
            let mut out = sql[..i].to_string();
            out.push_str(&rewrite_column_def(&sql[i..])?);
            Some(out)
        }
        _ => None,
    }
}

/// `name type rest`: rewrite `type` when it is a nested type, keep the rest.
fn rewrite_column_def(def: &str) -> Option<String> {
    let b = def.as_bytes();
    let start = skip_ws(b, 0);
    let name_end = skip_ident(b, start)?;
    let type_start = skip_ws(b, name_end);
    let Some(type_end) = nested_type_end(def, type_start) else {
        return Some(def.to_string());
    };
    Some(format!(
        "{}{}{}",
        &def[..type_start],
        rewrite_type(&def[type_start..type_end])?,
        &def[type_end..]
    ))
}

/// End of the nested type (`array(..)`, `row(..)`, `map<..>`, `struct<..>`)
/// starting at `i`, or `None` when the type there is not nested.
fn nested_type_end(s: &str, i: usize) -> Option<usize> {
    let b = s.as_bytes();
    let (w, end) = next_word(s, i)?;
    if !is_nested_kw(w) {
        return None;
    }
    let open = skip_ws(b, end);
    if !matches!(b.get(open), Some(b'(') | Some(b'<')) {
        return None;
    }
    Some(matching_bracket(b, open)? + 1)
}

fn is_nested_kw(w: &str) -> bool {
    ["array", "row", "map", "struct"]
        .iter()
        .any(|k| w.eq_ignore_ascii_case(k))
}

/// Rewrite one type into sqlparser's spelling, recursing into nested types.
/// A type that is not nested is returned as written.
fn rewrite_type(t: &str) -> Option<String> {
    let t = t.trim();
    let b = t.as_bytes();
    let Some(end) = nested_type_end(t, 0) else {
        return Some(t.to_string());
    };
    if end != t.len() {
        return None;
    }
    let (kw, kw_end) = next_word(t, 0)?;
    let open = skip_ws(b, kw_end);
    let body = &t[open + 1..end - 1];
    match kw.to_ascii_lowercase().as_str() {
        "array" => Some(format!("ARRAY<{}>", rewrite_type(body)?)),
        "map" => {
            let parts = split_top_level(body);
            let [k, v] = parts.as_slice() else {
                return None;
            };
            Some(format!("MAP({}, {})", rewrite_type(k)?, rewrite_type(v)?))
        }
        _ => {
            let fields = split_top_level(body)
                .iter()
                .map(|f| {
                    let fb = f.as_bytes();
                    let start = skip_ws(fb, 0);
                    let name_end = skip_ident(fb, start)?;
                    let ty = f[name_end..].trim().trim_start_matches(':');
                    Some(format!("{} {}", &f[start..name_end], rewrite_type(ty)?))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(format!("STRUCT<{}>", fields.join(", ")))
        }
    }
}

/// End (exclusive) of the column definition starting at `pos`: the next
/// comma outside parentheses and quotes, or `close`.
fn column_def_end(b: &[u8], pos: usize, close: usize) -> usize {
    // The type may hold commas inside `<..>`; step over it first.
    let s = std::str::from_utf8(&b[pos..close]).unwrap_or("");
    let mut i = pos;
    let sb = s.as_bytes();
    let start = skip_ws(sb, 0);
    if let Some(name_end) = skip_ident(sb, start) {
        let type_start = skip_ws(sb, name_end);
        if let Some(end) = nested_type_end(s, type_start) {
            i = pos + end;
        }
    }
    let mut depth = 0i32;
    while i < close {
        match b[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => return i,
            _ => {}
        }
        i += 1;
    }
    close
}

/// Split on commas outside any `(..)` / `<..>` and quotes.
fn split_top_level(s: &str) -> Vec<&str> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            b'(' | b'<' => depth += 1,
            b')' | b'>' => depth -= 1,
            b',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

/// Index of the bracket closing the `(` or `<` at `open`; `(`/`<` and
/// `)`/`>` nest together, which holds inside a type.
fn matching_bracket(b: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            b'(' | b'<' => depth += 1,
            b')' | b'>' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Index of the quote closing the one at `i` (a doubled quote escapes).
fn skip_quoted(b: &[u8], i: usize) -> usize {
    let q = b[i];
    let mut k = i + 1;
    while k < b.len() {
        if b[k] == q {
            if b.get(k + 1) == Some(&q) {
                k += 2;
                continue;
            }
            return k;
        }
        k += 1;
    }
    b.len()
}

/// End of the identifier (bare or quoted) at `i`.
fn skip_ident(b: &[u8], i: usize) -> Option<usize> {
    match b.get(i)? {
        b'"' | b'`' => {
            let close = skip_quoted(b, i);
            (close < b.len()).then_some(close + 1)
        }
        _ => {
            let mut k = i;
            while k < b.len() && (b[k].is_ascii_alphanumeric() || b[k] == b'_' || b[k] >= 0x80) {
                k += 1;
            }
            (k > i).then_some(k)
        }
    }
}

/// End of a dotted, possibly quoted, table name at `i`.
fn skip_name(s: &str, i: usize) -> Option<usize> {
    let b = s.as_bytes();
    let mut k = skip_ident(b, skip_ws(b, i))?;
    while b.get(k) == Some(&b'.') {
        k = skip_ident(b, k + 1)?;
    }
    Some(k)
}

/// Skip `words` (e.g. IF NOT EXISTS) when they all follow `i`.
fn skip_if_exists(s: &str, i: usize, words: &[&str]) -> usize {
    let mut k = i;
    for w in words {
        match next_word(s, k) {
            Some((got, end)) if got.eq_ignore_ascii_case(w) => k = end,
            _ => return i,
        }
    }
    k
}

/// The bare word after whitespace at `i`, and its end.
fn next_word(s: &str, i: usize) -> Option<(&str, usize)> {
    let b = s.as_bytes();
    let start = skip_ws(b, i);
    let mut k = start;
    while k < b.len() && (b[k].is_ascii_alphanumeric() || b[k] == b'_') {
        k += 1;
    }
    (k > start).then(|| (&s[start..k], k))
}

fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewritten(sql: &str) -> String {
        let out = rewrite_trino_ddl_types(sql);
        assert!(
            Parser::parse_sql(&GenericDialect {}, &out).is_ok(),
            "does not parse: {out}"
        );
        out
    }

    #[test]
    fn trino_nested_types_parse() {
        assert_eq!(
            rewritten("CREATE TABLE t (a array(bigint), m map(varchar,bigint))"),
            "CREATE TABLE t (a ARRAY<bigint>, m MAP(varchar, bigint))"
        );
        assert_eq!(
            rewritten("CREATE TABLE t (m MAP<VARCHAR, ARRAY<INT>>)"),
            "CREATE TABLE t (m MAP(VARCHAR, ARRAY<INT>))"
        );
        assert_eq!(
            rewritten("create table s.t (r row(x decimal(10,2),y varchar) NOT NULL, i int)"),
            "create table s.t (r STRUCT<x decimal(10,2), y varchar> NOT NULL, i int)"
        );
        assert_eq!(
            rewritten("CREATE TABLE t (r row(ts timestamp(6) with time zone, \"Foo\" bigint))"),
            "CREATE TABLE t (r STRUCT<ts timestamp(6) with time zone, \"Foo\" bigint>)"
        );
        assert_eq!(
            rewritten("CREATE TABLE IF NOT EXISTS t (a array(row(b array(bigint))))"),
            "CREATE TABLE IF NOT EXISTS t (a ARRAY<STRUCT<b ARRAY<bigint>>>)"
        );
        // parses, but as a flattened `Custom("row", ..)` type
        assert_eq!(
            rewritten("CREATE TABLE t (r row(t timestamp with time zone))"),
            "CREATE TABLE t (r STRUCT<t timestamp with time zone>)"
        );
        assert_eq!(
            rewritten("ALTER TABLE t ADD COLUMN l array(integer)"),
            "ALTER TABLE t ADD COLUMN l ARRAY<integer>"
        );
    }

    #[test]
    fn leaves_everything_but_column_types() {
        for sql in [
            // already in sqlparser's spelling
            "CREATE TABLE t (a ARRAY<INT>, m MAP(VARCHAR, BIGINT), s STRUCT<x INT>)",
            // a CTAS query is not a column list
            "CREATE TABLE t AS SELECT array(1, 2) AS a FROM (SELECT 1",
            // broken for another reason: kept with its own error
            "CREATE TABLE t (a array(bigint) DEFAULT)",
            "SELECT CAST(x AS row(a decimal(10,2))) FROM t",
        ] {
            assert_eq!(rewrite_trino_ddl_types(sql), sql);
        }
        // a DEFAULT and a string holding commas and brackets are copied
        assert_eq!(
            rewritten("CREATE TABLE t (s varchar DEFAULT 'a, map(b', a array(int))"),
            "CREATE TABLE t (s varchar DEFAULT 'a, map(b', a ARRAY<int>)"
        );
    }
}
