//! Trino-compat rewrite for nested / parameterized ROW-typed CAST targets.
//!
//! Trino spells a named row cast `CAST(ROW(1, ROW(10)) AS ROW(a int, b
//! ROW(x int)))`. sqlparser 0.62 parses the *single-level* form
//! `CAST(ROW(1) AS ROW(a int))` into a `Custom` type whose modifier list is
//! the flattened `[name, type, ...]` sequence, which the AST-level
//! [`crate::trino_compat`] rewriter turns into `named_struct(...)`. But the
//! *nested* form fails to parse outright: sqlparser cannot consume the inner
//! `ROW(x int)` inside the type-modifier list and reports
//! `Expected: type modifiers, found: (`. It never reaches the AST rewrite.
//!
//! This module rewrites the whole `CAST(<row-ctor> AS ROW(<fields>))`
//! expression at the source level into a nested `named_struct(...)` call,
//! recursing through nested ROW field types. The result parses and plans as
//! DataFusion's `named_struct`, which serializes over the Trino wire as a ROW,
//! so no additional wire work is required.
//!
//! See issue #335.

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Rewrite nested/parameterized ROW-typed CASTs to nested `named_struct(...)`.
///
/// Gating keeps this safe: the rewrite fires only when the SQL does not
/// already parse (the single-level ROW cast parses and is handled downstream
/// by the AST rewriter, so it is untouched here), and the result is re-parsed
/// and returned only if it parses. A statement broken for an unrelated reason
/// keeps its original error. The transform is Trino's exact named-row
/// semantics (label each positional field, coerce its value), so it preserves
/// semantics by construction; the re-parse only confirms well-formedness.
pub fn rewrite_nested_row_cast(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    // Fast path: need a CAST and a ROW( constructor/type to do anything.
    if !lower.contains("cast") || !lower.contains("row(") {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    // Only act when the original does not parse; the single-level ROW cast
    // parses fine and is handled by the AST rewriter, so leave it alone.
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return sql.to_string();
    }
    match rewrite_all_row_casts(sql) {
        Some(candidate) if candidate != sql && Parser::parse_sql(&dialect, &candidate).is_ok() => {
            candidate
        }
        _ => sql.to_string(),
    }
}

/// Find every `CAST( ... AS ROW( ... ) )` span and replace it with the
/// equivalent nested `named_struct(...)`. Returns `None` if no span was
/// rewritten (so the caller keeps the original).
fn rewrite_all_row_casts(sql: &str) -> Option<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut rewrote = false;

    while i < chars.len() {
        // Detect a `CAST(` keyword (case-insensitive), not preceded by an
        // identifier char (so it's the CAST function, not `foocast(`).
        if matches_kw(&chars, i, "cast") {
            let prev_ok = i == 0 || !is_ident_char(chars[i - 1]);
            let after = skip_ws(&chars, i + 4);
            if prev_ok && after < chars.len() && chars[after] == '(' {
                // Find the matching close paren of the CAST call.
                if let Some(close) = matching_paren(&chars, after) {
                    let inner: String = chars[after + 1..close].iter().collect();
                    if let Some(rewritten) = rewrite_cast_inner(&inner) {
                        out.push_str(&rewritten);
                        rewrote = true;
                        i = close + 1;
                        continue;
                    }
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }

    if rewrote {
        Some(out)
    } else {
        None
    }
}

/// Given the text inside `CAST( <here> )`, i.e. `<expr> AS <type>`, produce a
/// replacement expression string when `<type>` is a ROW type; otherwise
/// `None`. Recurses so a ROW type with nested ROW fields is fully expanded.
fn rewrite_cast_inner(inner: &str) -> Option<String> {
    let (expr_part, type_part) = split_top_level_as(inner)?;
    let expr_part = expr_part.trim();
    let type_part = type_part.trim();

    // Only handle a ROW(...) target type. Anything else is left to the normal
    // (parseable) path.
    let fields = parse_row_type(type_part)?;

    // The value expression must be a ROW(...)/struct(...) constructor with one
    // argument per declared field, matching Trino's named-row cast contract.
    let ctor_args = parse_row_constructor(expr_part)?;
    if ctor_args.len() != fields.len() {
        return None;
    }

    let mut parts: Vec<String> = Vec::with_capacity(fields.len());
    for (arg, (fname, ftype)) in ctor_args.iter().zip(fields.iter()) {
        let name_escaped = fname.replace('\'', "''");
        let value = build_field_value(arg, ftype)?;
        parts.push(format!("'{name_escaped}', {value}"));
    }
    Some(format!("named_struct({})", parts.join(", ")))
}

/// A parsed ROW field type: either a scalar type token sequence (`int`,
/// `decimal(10,2)`, `varchar`) or a nested ROW with its own fields.
enum FieldType {
    Scalar(String),
    Row(Vec<(String, FieldType)>),
}

/// Build the value expression for one field: recurse for a nested ROW,
/// otherwise emit `CAST(<arg> AS <type>)`.
fn build_field_value(arg: &str, ftype: &FieldType) -> Option<String> {
    match ftype {
        FieldType::Scalar(t) => Some(format!("CAST({} AS {})", arg.trim(), t.trim())),
        FieldType::Row(sub_fields) => {
            let ctor_args = parse_row_constructor(arg.trim())?;
            if ctor_args.len() != sub_fields.len() {
                return None;
            }
            let mut parts: Vec<String> = Vec::with_capacity(sub_fields.len());
            for (a, (fname, ft)) in ctor_args.iter().zip(sub_fields.iter()) {
                let name_escaped = fname.replace('\'', "''");
                let value = build_field_value(a, ft)?;
                parts.push(format!("'{name_escaped}', {value}"));
            }
            Some(format!("named_struct({})", parts.join(", ")))
        }
    }
}

/// Parse a ROW type declaration `ROW(a int, b ROW(x int))` into its field
/// list. Returns `None` if the string is not a `ROW(...)` type.
fn parse_row_type(s: &str) -> Option<Vec<(String, FieldType)>> {
    let s = s.trim();
    let rest = strip_row_prefix(s)?;
    // rest starts at the '(' of ROW(...); take the balanced group.
    let chars: Vec<char> = rest.chars().collect();
    if chars.first() != Some(&'(') {
        return None;
    }
    let close = matching_paren(&chars, 0)?;
    if close != chars.len() - 1 {
        // Trailing content after the ROW(...) close means this is not a plain
        // ROW type declaration.
        return None;
    }
    let body: String = chars[1..close].iter().collect();
    parse_field_list(&body)
}

/// Parse a comma-separated field list `a int, b ROW(x int)` (top-level commas
/// only) into `(name, FieldType)` pairs.
fn parse_field_list(body: &str) -> Option<Vec<(String, FieldType)>> {
    let segments = split_top_level_commas(body);
    if segments.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(segments.len());
    for seg in segments {
        let seg = seg.trim();
        // Field name is the leading identifier; the rest is the type.
        let name_end = seg.find(char::is_whitespace)?;
        let name = seg[..name_end].trim().trim_matches('"').to_string();
        let type_str = seg[name_end..].trim();
        if name.is_empty() || type_str.is_empty() {
            return None;
        }
        let ftype = if strip_row_prefix(type_str).is_some() {
            FieldType::Row(parse_row_type(type_str)?)
        } else {
            FieldType::Scalar(type_str.to_string())
        };
        out.push((name, ftype));
    }
    Some(out)
}

/// Parse a ROW/struct constructor `ROW(1, ROW(10))` into its top-level
/// argument expressions. Returns `None` if `s` is not a `ROW(...)`/`struct(...)`
/// call.
fn parse_row_constructor(s: &str) -> Option<Vec<String>> {
    let s = s.trim();
    let rest = strip_row_prefix(s).or_else(|| strip_struct_prefix(s))?;
    let chars: Vec<char> = rest.chars().collect();
    if chars.first() != Some(&'(') {
        return None;
    }
    let close = matching_paren(&chars, 0)?;
    if close != chars.len() - 1 {
        return None;
    }
    let body: String = chars[1..close].iter().collect();
    Some(
        split_top_level_commas(&body)
            .into_iter()
            .map(|s| s.trim().to_string())
            .collect(),
    )
}

/// If `s` starts (case-insensitively) with the `ROW` keyword followed by
/// optional whitespace and a `(`, return the slice starting at that `(`.
fn strip_row_prefix(s: &str) -> Option<&str> {
    strip_ctor_prefix(s, "row")
}

fn strip_struct_prefix(s: &str) -> Option<&str> {
    strip_ctor_prefix(s, "struct")
}

fn strip_ctor_prefix<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let bytes = s.as_bytes();
    if bytes.len() < kw.len() || !bytes[..kw.len()].eq_ignore_ascii_case(kw.as_bytes()) {
        return None;
    }
    // The char after the keyword must not be an identifier char (so `row` is
    // not the prefix of `rowid`).
    let after = &s[kw.len()..];
    let trimmed = after.trim_start();
    if trimmed.starts_with('(') {
        Some(trimmed)
    } else {
        None
    }
}

/// Split `<expr> AS <type>` at the top-level (paren-depth-0) `AS` keyword.
fn split_top_level_as(s: &str) -> Option<(&str, &str)> {
    let chars: Vec<char> = s.chars().collect();
    let mut depth = 0i32;
    let mut i = 0usize;
    // Track byte offsets alongside char scanning for slicing.
    let mut byte_off = 0usize;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth == 0
            && (c == 'a' || c == 'A')
            && matches_kw(&chars, i, "as")
            && (i == 0 || !is_ident_char(chars[i - 1]))
            && (i + 2 >= chars.len() || !is_ident_char(chars[i + 2]))
        {
            let as_byte = byte_off;
            let expr = &s[..as_byte];
            // ` AS ` is ASCII, 2 bytes for "as".
            let type_start = as_byte + 2;
            let type_part = &s[type_start..];
            return Some((expr, type_part));
        }
        byte_off += c.len_utf8();
        i += 1;
    }
    None
}

/// Split a string on top-level (depth-0) commas.
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() || !out.is_empty() {
        out.push(cur);
    }
    out
}

/// Index of the paren that matches the `(` at `open` (which must be `(`).
fn matching_paren(chars: &[char], open: usize) -> Option<usize> {
    if chars.get(open) != Some(&'(') {
        return None;
    }
    let mut depth = 0i32;
    let mut in_str = false;
    for (k, &c) in chars.iter().enumerate().skip(open) {
        if in_str {
            if c == '\'' {
                in_str = false;
            }
            continue;
        }
        match c {
            '\'' => in_str = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(k);
                }
            }
            _ => {}
        }
    }
    None
}

/// Case-insensitive keyword match at position `i` in `chars`.
fn matches_kw(chars: &[char], i: usize, kw: &str) -> bool {
    let kw_chars: Vec<char> = kw.chars().collect();
    if i + kw_chars.len() > chars.len() {
        return false;
    }
    for (k, &kc) in kw_chars.iter().enumerate() {
        if !chars[i + k].eq_ignore_ascii_case(&kc) {
            return false;
        }
    }
    true
}

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn skip_ws(chars: &[char], mut i: usize) -> usize {
    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parses(sql: &str) -> bool {
        Parser::parse_sql(&GenericDialect {}, sql).is_ok()
    }

    #[test]
    fn rewrites_nested_row_cast() {
        let out =
            rewrite_nested_row_cast("SELECT CAST(row(1, row(10)) AS row(a int, b row(x int)))");
        assert!(parses(&out), "did not parse: {out}");
        assert!(out.contains("named_struct("), "no named_struct: {out}");
        // Inner nesting must be present.
        let count = out.matches("named_struct(").count();
        assert_eq!(count, 2, "expected 2 named_struct calls: {out}");
        assert!(
            out.contains("'a'") && out.contains("'b'") && out.contains("'x'"),
            "{out}"
        );
    }

    #[test]
    fn rewrites_parameterized_field_type() {
        // decimal(10,2) is a parameterized (nested-paren) field type that the
        // single-level path cannot parse; the rewrite lifts it out.
        let out =
            rewrite_nested_row_cast("SELECT CAST(row(1, 2.5) AS row(a int, b decimal(10,2)))");
        assert!(parses(&out), "did not parse: {out}");
        assert!(out.contains("named_struct("), "{out}");
        assert!(
            out.contains("decimal(10,2)") || out.contains("decimal(10, 2)"),
            "{out}"
        );
    }

    #[test]
    fn single_level_row_cast_is_untouched() {
        // Parses as-is (handled by the AST rewriter downstream), so this
        // module leaves it alone.
        let sql = "SELECT CAST(ROW(1) AS ROW(x int, y varchar))";
        assert_eq!(rewrite_nested_row_cast(sql), sql);
    }

    #[test]
    fn deeply_nested_row_cast() {
        let out =
            rewrite_nested_row_cast("SELECT CAST(row(row(row(1))) AS row(a row(b row(c int))))");
        assert!(parses(&out), "did not parse: {out}");
        assert_eq!(out.matches("named_struct(").count(), 3, "{out}");
    }

    #[test]
    fn non_row_cast_untouched() {
        let sql = "SELECT CAST(x AS BIGINT) FROM t";
        assert_eq!(rewrite_nested_row_cast(sql), sql);
    }

    #[test]
    fn no_cast_untouched() {
        let sql = "SELECT row(1, 2)";
        assert_eq!(rewrite_nested_row_cast(sql), sql);
    }

    #[test]
    fn arg_count_mismatch_left_alone() {
        // 1 constructor arg, 2 declared fields: not a valid named-row cast, so
        // no rewrite (and the original, which does not parse, is returned).
        let sql = "SELECT CAST(row(1) AS row(a int, b row(x int)))";
        let out = rewrite_nested_row_cast(sql);
        assert!(!out.contains("named_struct("), "{out}");
    }

    #[test]
    fn split_top_level_commas_respects_parens() {
        let parts = split_top_level_commas("a int, b row(x int, y int)");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].trim(), "a int");
        assert_eq!(parts[1].trim(), "b row(x int, y int)");
    }

    #[test]
    fn matching_paren_finds_close() {
        let chars: Vec<char> = "(a, (b, c), d)x".chars().collect();
        assert_eq!(matching_paren(&chars, 0), Some(13));
    }

    // ── Adversarial inputs: comments, quoted identifiers, literals (#630) ──

    const NESTED: &str = "CAST(row(1, row(10)) AS row(a int, b row(x int)))";

    #[test]
    fn leading_line_comment_is_preserved_and_rewrite_still_fires() {
        let sql = format!("-- leading ) comment\nSELECT {NESTED}");
        let out = rewrite_nested_row_cast(&sql);
        assert!(out.starts_with("-- leading ) comment\n"), "{out}");
        assert!(parses(&out), "did not parse: {out}");
        assert_eq!(out.matches("named_struct(").count(), 2, "{out}");
    }

    #[test]
    fn leading_block_comment_is_preserved_and_rewrite_still_fires() {
        let sql = format!("/* lead ) */ SELECT {NESTED}");
        let out = rewrite_nested_row_cast(&sql);
        assert!(out.starts_with("/* lead ) */ "), "{out}");
        assert!(parses(&out), "did not parse: {out}");
    }

    #[test]
    fn cast_and_row_keywords_inside_a_literal_are_untouched() {
        // Parses as-is, so the gate returns it byte-identical.
        let sql = "SELECT 'cast(row(1) as row(a int))' AS s";
        assert_eq!(rewrite_nested_row_cast(sql), sql);
    }

    #[test]
    fn trailing_comment_with_close_paren_does_not_disturb_the_span() {
        let sql = format!("SELECT {NESTED} AS c -- )");
        let out = rewrite_nested_row_cast(&sql);
        assert!(out.ends_with("-- )"), "{out}");
        assert!(parses(&out), "did not parse: {out}");
    }

    /// `matching_paren` tracks single quotes but not comments, so a `)` in a
    /// block comment inside the CAST span closes the scan early; the rewrite
    /// silently does not fire and the client gets the raw parse error.
    #[test]
    #[ignore = "rewriter bug (#630): `)` inside a block comment within the CAST span mis-balances matching_paren; enable once the scanner skips comments"]
    fn close_paren_inside_comment_within_span_is_not_a_paren() {
        let sql = "SELECT CAST(row(1, /* ) */ row(10)) AS row(a int, b row(x int)))";
        let out = rewrite_nested_row_cast(sql);
        assert!(parses(&out), "rewrite did not fire: {out}");
        assert!(out.contains("/* ) */"), "comment bytes changed: {out}");
    }

    /// Same scan, double-quoted identifier: `"a)b"` is one identifier to the
    /// tokenizer but a close paren to the character scanner.
    #[test]
    #[ignore = "rewriter bug (#630): `)` inside a quoted identifier within the CAST span mis-balances matching_paren; enable once the scanner skips quoted identifiers"]
    fn close_paren_inside_quoted_identifier_within_span_is_not_a_paren() {
        let sql = "SELECT CAST(row(\"a)b\", row(10)) AS row(a int, b row(x int))) FROM t";
        let out = rewrite_nested_row_cast(sql);
        assert!(parses(&out), "rewrite did not fire: {out}");
        assert!(out.contains("\"a)b\""), "identifier bytes changed: {out}");
    }

    /// `matching_paren` skips single-quoted literals but `split_top_level_as`
    /// and `split_top_level_commas` do not, so a `)` or `,` inside a literal
    /// field value defeats the split and the rewrite does not fire.
    #[test]
    #[ignore = "rewriter bug (#630): `)` inside a string literal within the CAST span defeats split_top_level_as (no quote tracking); enable once the splitters skip literals"]
    fn close_paren_inside_literal_within_span_is_not_a_paren() {
        let sql = "SELECT CAST(row('a)b', row(10)) AS row(a varchar, b row(x int)))";
        let out = rewrite_nested_row_cast(sql);
        assert!(parses(&out), "rewrite did not fire: {out}");
        assert!(out.contains("'a)b'"), "literal bytes changed: {out}");
    }

    #[test]
    #[ignore = "rewriter bug (#630): `,` inside a string literal within the CAST span is split as a field separator by split_top_level_commas; enable once the splitters skip literals"]
    fn comma_inside_literal_within_span_is_not_a_separator() {
        let sql = "SELECT CAST(row('a,b', row(10)) AS row(a varchar, b row(x int)))";
        let out = rewrite_nested_row_cast(sql);
        assert!(parses(&out), "rewrite did not fire: {out}");
        assert!(out.contains("'a,b'"), "literal bytes changed: {out}");
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        /// Bytes that trip a character scanner: parens, commas, the keywords
        /// this module keys on, comment openers. No quotes, so the payload
        /// can be dropped into a literal, a quoted identifier or a comment
        /// without further escaping (and no `*` so it cannot form `*/`).
        fn payload() -> impl Strategy<Value = String> {
            prop::collection::vec(
                prop_oneof![
                    Just(")".to_string()),
                    Just("(".to_string()),
                    Just(",".to_string()),
                    Just(" cast(".to_string()),
                    Just(" row(".to_string()),
                    Just(" as ".to_string()),
                    Just(" -- ".to_string()),
                    Just(" /* ".to_string()),
                    "[a-z0-9 ]{1,4}",
                ],
                1..6,
            )
            .prop_map(|parts| parts.concat())
        }

        fn literal() -> impl Strategy<Value = String> {
            payload().prop_map(|p| format!("'{p}'"))
        }

        fn quoted_ident() -> impl Strategy<Value = String> {
            payload().prop_map(|p| format!("\"{p}\""))
        }

        fn line_comment() -> impl Strategy<Value = String> {
            payload().prop_map(|p| format!("--{p}\n"))
        }

        /// sqlparser nests block comments, so an opener inside the body would
        /// leave the comment unterminated; defuse it.
        fn block_comment() -> impl Strategy<Value = String> {
            payload().prop_map(|p| format!("/*{}*/", p.replace("/*", "/ *")))
        }

        /// Statements that parse as written (so the gate must return them
        /// unchanged), carrying the adversarial payload in every position a
        /// BI tool would put it.
        fn parseable_statement() -> impl Strategy<Value = String> {
            (literal(), quoted_ident(), line_comment(), block_comment()).prop_flat_map(
                |(l, q, lc, bc)| {
                    prop_oneof![
                        Just(format!("SELECT {l}, {q} FROM t {bc}")),
                        Just(format!("{lc}SELECT CAST({l} AS VARCHAR) AS {q}")),
                        Just(format!("SELECT CAST(ROW(1) AS ROW(x int)) {bc}, {l}")),
                        Just(format!("INSERT INTO {q} VALUES ({l}, 1) {bc}")),
                        Just(format!("SELECT * FROM t WHERE {q} = {l} {lc}")),
                    ]
                },
            )
        }

        proptest! {
            /// (a) Already-parseable SQL is returned byte-identical.
            #[test]
            fn parseable_input_is_returned_byte_identical(sql in parseable_statement()) {
                if parses(&sql) {
                    prop_assert_eq!(rewrite_nested_row_cast(&sql), sql);
                }
            }

            /// (b) Literal and comment bytes outside the CAST span survive a
            /// rewrite that fires, and the result parses.
            #[test]
            fn literal_and_comment_bytes_survive_the_rewrite(
                l in literal(),
                lc in line_comment(),
                bc in block_comment(),
            ) {
                let sql = format!("{lc}SELECT {NESTED} AS c, {l} {bc}");
                let out = rewrite_nested_row_cast(&sql);
                prop_assert!(out.contains(&l), "literal changed: {out}");
                prop_assert!(out.contains(&lc), "line comment changed: {out}");
                prop_assert!(out.contains(&bc), "block comment changed: {out}");
                prop_assert!(parses(&out), "rewrite did not fire: {out}");
            }
        }
    }
}
