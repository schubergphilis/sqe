//! Trino type syntax in `CAST` / `TRY_CAST`.
//!
//! [`crate::ddl_types`] already spells Trino's nested column types the way
//! sqlparser parses them. A CAST target has the same problem and one more:
//!
//! - `array(T)` and a nested `row(..)` do not parse at all;
//! - `map(K, V)` parses, but DataFusion's SQL planner refuses a `Map` cast
//!   target;
//! - `time(p)` and `time with time zone` parse, but DataFusion only plans a
//!   bare `TIME` (`Time64(ns)`).
//!
//! This module rewrites the target type of each `CAST(<expr> AS <type>)` in
//! the source text: `array(T)` becomes `ARRAY<T>`, `row(..)` becomes
//! `STRUCT<..>`, `time(p) [with time zone]` becomes `TIME`, and a type that
//! holds a `map` becomes `arrow_cast(<expr>, '<arrow type>')`, the one cast
//! DataFusion plans for a map. String literals and quoted identifiers are
//! skipped. A `CAST(ROW(..) AS row(..))` is left to
//! [`crate::nested_row_cast`] and the AST rewriter (#335). The result is
//! returned only when it parses.
//!
//! A cast of a string or JSON value to `array(..)` plans, but fails at run
//! time in Arrow: there is no JSON-to-array cast.

use crate::ddl_types::{
    matching_bracket, next_word, rewrite_type, skip_ident, skip_quoted, skip_ws, split_top_level,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Rewrite Trino type syntax in CAST / TRY_CAST targets; any statement
/// without one comes back unchanged.
pub fn rewrite_trino_cast_types(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("cast")
        || !["array", "map", "row", "time"]
            .iter()
            .any(|k| lower.contains(k))
    {
        return sql.to_string();
    }
    let candidate = rewrite_all(sql);
    if candidate != sql && Parser::parse_sql(&GenericDialect {}, &candidate).is_ok() {
        candidate
    } else {
        sql.to_string()
    }
}

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// Rewrite every `CAST(..)` / `TRY_CAST(..)` call in `s`, innermost included.
fn rewrite_all(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut last = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => i = (skip_quoted(b, i) + 1).min(b.len()),
            c if is_ident_byte(c) => {
                let start = i;
                while i < b.len() && is_ident_byte(b[i]) {
                    i += 1;
                }
                let word = &s[start..i];
                let is_try = word.eq_ignore_ascii_case("try_cast");
                let boundary = start == 0 || !matches!(b[start - 1], b'.' | b'$');
                if boundary && (is_try || word.eq_ignore_ascii_case("cast")) {
                    let open = skip_ws(b, i);
                    if b.get(open) == Some(&b'(') {
                        if let Some(close) = matching_paren(b, open) {
                            match rewrite_call(&s[open + 1..close], is_try) {
                                Call::Inner(text) => {
                                    out.push_str(&s[last..=open]);
                                    out.push_str(&text);
                                    last = close;
                                }
                                Call::Arrow { expr, ty } => {
                                    out.push_str(&s[last..start]);
                                    out.push_str(&format!("arrow_cast({expr}, '{ty}'"));
                                    last = close;
                                }
                            }
                            i = close + 1;
                        }
                    }
                }
            }
            _ => i += 1,
        }
    }
    out.push_str(&s[last..]);
    out
}

enum Call {
    /// The text between the CAST parentheses, rewritten.
    Inner(String),
    /// The whole call becomes `arrow_cast(expr, 'ty')`.
    Arrow { expr: String, ty: String },
}

fn rewrite_call(inner: &str, is_try: bool) -> Call {
    let Some(as_pos) = last_top_level_as(inner) else {
        return Call::Inner(rewrite_all(inner));
    };
    let expr = rewrite_all(&inner[..as_pos]);
    let kw = &inner[as_pos..as_pos + 2];
    let ty = &inner[as_pos + 2..];
    let tail = ty.trim_end();
    let ws = &ty[..ty.len() - ty.trim_start().len()];
    match rewrite_target(&inner[..as_pos], tail.trim_start()) {
        Some(Target::Type(t)) => Call::Inner(format!("{expr}{kw}{ws}{t}{}", &ty[tail.len()..])),
        // `arrow_cast` has no TRY form: a failing value would raise instead
        // of giving NULL, so a TRY_CAST to a map is left as written.
        Some(Target::ArrowCast(ty)) if !is_try => Call::Arrow {
            expr: expr.trim_end().to_string(),
            ty,
        },
        _ => Call::Inner(format!("{expr}{kw}{ty}")),
    }
}

enum Target {
    /// A type sqlparser parses and DataFusion plans.
    Type(String),
    /// An Arrow type text for `arrow_cast`.
    ArrowCast(String),
}

fn rewrite_target(operand: &str, target: &str) -> Option<Target> {
    let (word, end) = next_word(target, 0)?;
    let word = word.to_ascii_lowercase();
    let b = target.as_bytes();
    match word.as_str() {
        "time" => is_time_variant(&target[end..]).then(|| Target::Type("TIME".to_string())),
        "array" | "row" | "map" => {
            let open = skip_ws(b, end);
            if b.get(open) != Some(&b'(') {
                return None; // `ARRAY<..>` is already sqlparser's spelling
            }
            if word == "row" {
                let op = operand.trim_start().to_ascii_lowercase();
                let row_ctor = op
                    .strip_prefix("row")
                    .is_some_and(|r| r.trim_start().starts_with('('));
                if row_ctor || op.starts_with("struct(") {
                    return None; // #335: the constructor idiom
                }
            }
            if has_map(target) {
                return Some(Target::ArrowCast(parse_type(target)?.arrow()?));
            }
            let t = rewrite_type(target)?;
            (t != target).then_some(Target::Type(t))
        }
        _ => None,
    }
}

/// `(p)`, `(p) with time zone` and `with time zone` after the word `time`.
/// A bare `TIME` is not a variant.
fn is_time_variant(rest: &str) -> bool {
    let rest = rest.trim();
    if rest.is_empty() {
        return false;
    }
    let after = match rest.strip_prefix('(') {
        Some(r) => match r.find(')') {
            Some(close) if close > 0 && r[..close].trim().bytes().all(|c| c.is_ascii_digit()) => {
                &r[close + 1..]
            }
            _ => return false,
        },
        None => rest,
    };
    let words: Vec<String> = after
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect();
    matches!(
        words
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice(),
        ["with" | "without", "time", "zone"] | []
    )
}

/// Whether a type text holds a `map` type anywhere.
fn has_map(t: &str) -> bool {
    let b = t.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if !is_ident_byte(b[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < b.len() && is_ident_byte(b[i]) {
            i += 1;
        }
        if t[start..i].eq_ignore_ascii_case("map") && b.get(skip_ws(b, i)) == Some(&b'(') {
            return true;
        }
    }
    false
}

/// A Trino type, parsed far enough to print its Arrow type.
enum Ty {
    Scalar(String),
    Array(Box<Ty>),
    Map(Box<Ty>, Box<Ty>),
    Row(Vec<(String, Ty)>),
}

fn parse_type(t: &str) -> Option<Ty> {
    let t = t.trim();
    let b = t.as_bytes();
    let (word, end) = next_word(t, 0)?;
    let word = word.to_ascii_lowercase();
    let open = skip_ws(b, end);
    let args = if b.get(open) == Some(&b'(') {
        let close = matching_bracket(b, open)?;
        Some((&t[open + 1..close], close + 1))
    } else {
        None
    };
    let rest = args.map_or(&t[end..], |(_, e)| &t[e..]).trim();
    let rest_lower = rest.to_ascii_lowercase();
    let arg_list = args.map(|(a, _)| split_top_level(a));
    let scalar = |s: &str| (rest.is_empty()).then(|| Ty::Scalar(s.to_string()));
    match word.as_str() {
        "array" if rest.is_empty() => {
            let args = arg_list?;
            let [e] = args.as_slice() else {
                return None;
            };
            Some(Ty::Array(Box::new(parse_type(e)?)))
        }
        "map" if rest.is_empty() => {
            let args = arg_list?;
            let [k, v] = args.as_slice() else {
                return None;
            };
            Some(Ty::Map(Box::new(parse_type(k)?), Box::new(parse_type(v)?)))
        }
        "row" if rest.is_empty() => {
            let fields = arg_list?
                .iter()
                .map(|f| {
                    let fb = f.as_bytes();
                    let start = skip_ws(fb, 0);
                    let name_end = skip_ident(fb, start)?;
                    let name = f[start..name_end].trim_matches(|c| c == '"' || c == '`');
                    if name.contains('"') || name.contains('\\') {
                        return None;
                    }
                    Some((name.to_string(), parse_type(&f[name_end..])?))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(Ty::Row(fields))
        }
        "boolean" => scalar("Boolean"),
        "tinyint" => scalar("Int8"),
        "smallint" => scalar("Int16"),
        "int" | "integer" => scalar("Int32"),
        "bigint" => scalar("Int64"),
        "real" | "float" => scalar("Float32"),
        "double" if rest.is_empty() || rest_lower == "precision" => {
            Some(Ty::Scalar("Float64".to_string()))
        }
        "varchar" | "char" | "text" | "string" => scalar("Utf8"),
        "varbinary" | "binary" => scalar("Binary"),
        "date" => scalar("Date32"),
        "time" => (rest.is_empty()
            || ["with time zone", "without time zone"].contains(&rest_lower.as_str()))
        .then(|| Ty::Scalar("Time64(Nanosecond)".to_string())),
        "decimal" | "numeric" => {
            let (p, s) = match arg_list.as_deref() {
                None => (38, 0),
                Some([p]) => (p.trim().parse::<u8>().ok()?, 0),
                Some([p, s]) => (p.trim().parse::<u8>().ok()?, s.trim().parse::<i8>().ok()?),
                Some(_) => return None,
            };
            scalar(&format!("Decimal128({p}, {s})"))
        }
        "timestamp" => {
            let tz = match rest_lower.as_str() {
                "" | "without time zone" => "None",
                "with time zone" => "Some(\"UTC\")",
                _ => return None,
            };
            let unit = match arg_list.as_deref() {
                None => "Microsecond",
                Some([p]) => match p.trim().parse::<u8>().ok()? {
                    0 => "Second",
                    1..=3 => "Millisecond",
                    4..=6 => "Microsecond",
                    7..=9 => "Nanosecond",
                    _ => return None,
                },
                Some(_) => return None,
            };
            Some(Ty::Scalar(format!("Timestamp({unit}, {tz})")))
        }
        _ => None,
    }
}

impl Ty {
    /// The text `arrow_cast` parses (Arrow's `Display` form).
    fn arrow(&self) -> Option<String> {
        Some(match self {
            Ty::Scalar(s) => s.clone(),
            Ty::Array(e) => format!("List({})", e.arrow()?),
            Ty::Map(k, v) => format!(
                "Map(\"entries\": non-null Struct(\"key\": non-null {}, \"value\": {}), unsorted)",
                k.arrow()?,
                v.arrow()?
            ),
            Ty::Row(fields) => {
                let parts = fields
                    .iter()
                    .map(|(n, t)| Some(format!("\"{n}\": {}", t.arrow()?)))
                    .collect::<Option<Vec<_>>>()?;
                format!("Struct({})", parts.join(", "))
            }
        })
    }
}

/// Index of the parenthesis closing the `(` at `open`; quotes are skipped.
fn matching_paren(b: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            b'(' => depth += 1,
            b')' => {
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

/// Byte index of the last `AS` keyword outside parentheses and quotes.
fn last_top_level_as(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut depth = 0i32;
    let mut found = None;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i),
            b'(' => depth += 1,
            b')' => depth -= 1,
            c if is_ident_byte(c) => {
                let start = i;
                while i + 1 < b.len() && is_ident_byte(b[i + 1]) {
                    i += 1;
                }
                if depth == 0 && s[start..=i].eq_ignore_ascii_case("as") {
                    found = Some(start);
                }
            }
            _ => {}
        }
        i += 1;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::front_door::{SqlFrontDoor, WireDialect};

    /// The text DataFusion is handed for `sql`, on every wire dialect.
    fn door(sql: &str) -> String {
        let outs: Vec<String> = [
            WireDialect::Trino,
            WireDialect::Flight,
            WireDialect::Embedded,
        ]
        .into_iter()
        .map(|d| {
            SqlFrontDoor::new(d)
                .normalize(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .sql()
                .to_string()
        })
        .collect();
        assert!(outs.windows(2).all(|w| w[0] == w[1]), "{sql}: {outs:?}");
        outs[0].clone()
    }

    #[test]
    fn array_targets() {
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(x AS array(integer))"),
            "SELECT CAST(x AS ARRAY<integer>)"
        );
        assert_eq!(
            rewrite_trino_cast_types("SELECT TRY_CAST(x AS array(array(varchar))) FROM t"),
            "SELECT TRY_CAST(x AS ARRAY<ARRAY<varchar>>) FROM t"
        );
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(json_parse('[1,2]') AS array(bigint))"),
            "SELECT CAST(json_parse('[1,2]') AS ARRAY<bigint>)"
        );
        assert_eq!(
            door("SELECT CAST(x AS array(integer))"),
            "SELECT CAST(x AS ARRAY<INTEGER>) AS _col0"
        );
    }

    #[test]
    fn row_targets() {
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(x AS row(x row(y decimal(10,2))))"),
            "SELECT CAST(x AS STRUCT<x STRUCT<y decimal(10,2)>>)"
        );
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(x AS array(row(a bigint, b varchar)))"),
            "SELECT CAST(x AS ARRAY<STRUCT<a bigint, b varchar>>)"
        );
        assert_eq!(
            door("SELECT CAST(x AS row(a integer, b varchar))"),
            "SELECT CAST(x AS STRUCT<a INTEGER, b VARCHAR>) AS _col0"
        );
    }

    #[test]
    fn row_constructor_keeps_its_named_struct() {
        // #335: single-level and nested
        assert_eq!(
            door("SELECT CAST(ROW(1, 'a') AS row(a integer, b varchar))"),
            "SELECT named_struct('a', CAST(1 AS INTEGER), 'b', CAST('a' AS VARCHAR)) AS _col0"
        );
        let nested = door("SELECT CAST(ROW(1, ROW(2)) AS row(a integer, b row(c integer)))");
        assert!(nested.contains("'b', named_struct('c'"), "{nested}");
    }

    #[test]
    fn map_targets_become_arrow_cast() {
        let key = "\"entries\": non-null Struct(\"key\": non-null Utf8, \"value\": ";
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(m AS map(varchar, bigint))"),
            format!("SELECT arrow_cast(m, 'Map({key}Int64), unsorted)')")
        );
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(m AS map(varchar, array(double))) FROM t"),
            format!("SELECT arrow_cast(m, 'Map({key}List(Float64)), unsorted)') FROM t")
        );
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(m AS array(map(varchar, decimal(10,2))))"),
            "SELECT arrow_cast(m, 'List(Map(\"entries\": non-null Struct(\"key\": non-null Utf8, \
             \"value\": Decimal128(10, 2)), unsorted))')"
        );
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST(m AS map(varchar, row(a int, b timestamp(3))))"),
            format!(
                "SELECT arrow_cast(m, 'Map({key}Struct(\"a\": Int32, \"b\": \
                 Timestamp(Millisecond, None))), unsorted)')"
            )
        );
        // arrow_cast cannot return NULL on failure
        let try_cast = "SELECT TRY_CAST(m AS map(varchar, bigint))";
        assert_eq!(rewrite_trino_cast_types(try_cast), try_cast);
        // a key type with no Arrow spelling here
        let odd = "SELECT CAST(m AS map(varchar, interval))";
        assert_eq!(rewrite_trino_cast_types(odd), odd);
        assert!(door("SELECT CAST(m AS map(varchar, bigint))").starts_with("SELECT arrow_cast("));
    }

    #[test]
    fn time_targets_become_time() {
        for t in ["time(3)", "time(6) with time zone", "TIME WITH TIME ZONE"] {
            assert_eq!(
                rewrite_trino_cast_types(&format!("SELECT CAST(x AS {t})")),
                "SELECT CAST(x AS TIME)",
                "{t}"
            );
        }
        assert_eq!(
            door("SELECT CAST(x AS time(3) with time zone)"),
            "SELECT CAST(x AS TIME) AS _col0"
        );
        // bare TIME, a timestamp and a time-named column are not touched
        for sql in [
            "SELECT CAST(x AS TIME)",
            "SELECT CAST(x AS timestamp(3) with time zone)",
            "SELECT CAST(time AS varchar) FROM t",
        ] {
            assert_eq!(rewrite_trino_cast_types(sql), sql);
        }
    }

    #[test]
    fn nested_casts_and_expressions() {
        assert_eq!(
            rewrite_trino_cast_types(
                "SELECT CAST(CAST(a AS array(integer)) AS array(bigint)), CAST(b AS array(int))"
            ),
            "SELECT CAST(CAST(a AS ARRAY<integer>) AS ARRAY<bigint>), CAST(b AS ARRAY<int>)"
        );
        // the last top-level AS is the target; an inner one is not
        assert_eq!(
            rewrite_trino_cast_types("SELECT CAST((SELECT x AS y) AS array(int))"),
            "SELECT CAST((SELECT x AS y) AS ARRAY<int>)"
        );
        assert_eq!(
            rewrite_trino_cast_types("SELECT cast(x as ARRAY(int))"),
            "SELECT cast(x as ARRAY<int>)"
        );
    }

    #[test]
    fn leaves_everything_else_unchanged() {
        for sql in [
            // already sqlparser's spelling
            "SELECT CAST(x AS ARRAY<INT>)",
            "SELECT CAST(x AS STRUCT<a INT>)",
            // not a cast target
            "SELECT array(1, 2), map(ARRAY['a'], ARRAY[1]) FROM t",
            "SELECT 'CAST(x AS array(int))'",
            "SELECT \"cast(x as array(int))\" FROM t",
            "SELECT x AS \"array(int)\" FROM t",
            "SELECT my.cast(x AS array(int))",
            // broken for another reason
            "SELECT CAST(x AS array(int)",
            "SELECT CAST(x AS array())",
            // plain casts
            "SELECT CAST(x AS bigint), CAST(y AS varchar)",
        ] {
            assert_eq!(rewrite_trino_cast_types(sql), sql);
        }
        // the string literal survives next to a real rewrite
        assert_eq!(
            rewrite_trino_cast_types("SELECT 'cast(a as array(int))', CAST(x AS array(int))"),
            "SELECT 'cast(a as array(int))', CAST(x AS ARRAY<int>)"
        );
    }
}
