//! Source-level fallback for `TRIM(BOTH FROM x)` and its `LEADING` /
//! `TRAILING` forms without a removal character.
//!
//! SQL:2016 allows the trim character to be omitted: `TRIM(BOTH FROM x)`
//! strips spaces from both ends. Trino prints exactly that shape when it
//! re-serializes a view body (`trim(BOTH FROM tag.name)` in Athena view
//! definitions), and sqlparser 0.62 rejects it: it expects either
//! `TRIM(BOTH 'c' FROM x)` or `TRIM(x)`. The three forms lower onto
//! `TRIM(x)`, `LTRIM(x)` and `RTRIM(x)`, which parse everywhere and mean the
//! same thing for the default (space) character.
//!
//! Text-level, like `rewrite_paren_less_values`: the input does not parse,
//! so there is no AST to work on. The scan skips string literals and quoted
//! identifiers, so `'TRIM(BOTH FROM'` inside a literal is left alone, and
//! matches are case-insensitive with any whitespace between the tokens.

/// Rewrite `TRIM(BOTH FROM ` / `TRIM(LEADING FROM ` / `TRIM(TRAILING FROM `
/// into `TRIM(` / `LTRIM(` / `RTRIM(`. Returns the input unchanged when no
/// such shape is present.
pub fn rewrite_trim_both_from(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut changed = false;
    while i < bytes.len() {
        let c = bytes[i];
        // Skip string literals and quoted identifiers verbatim.
        if c == b'\'' || c == b'"' {
            let end = skip_quoted(bytes, i, c);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if let Some((consumed, replacement)) = match_trim_prefix(&sql[i..]) {
            // Only at a word boundary, so `xtrim(` is not touched.
            let at_boundary = i == 0 || !is_ident_byte(bytes[i - 1]);
            if at_boundary {
                out.push_str(replacement);
                i += consumed;
                changed = true;
                continue;
            }
        }
        // Copy one char (UTF-8 aware).
        let ch_len = utf8_len(c);
        out.push_str(&sql[i..i + ch_len]);
        i += ch_len;
    }
    if changed {
        out
    } else {
        sql.to_string()
    }
}

/// If `rest` starts with `TRIM ( <BOTH|LEADING|TRAILING> FROM` (any case,
/// any whitespace), return how many bytes that prefix spans and the text to
/// emit instead.
fn match_trim_prefix(rest: &str) -> Option<(usize, &'static str)> {
    let mut pos = 0;
    pos += take_keyword(&rest[pos..], "TRIM")?;
    pos += skip_ws(&rest[pos..]);
    if !rest[pos..].starts_with('(') {
        return None;
    }
    pos += 1;
    pos += skip_ws(&rest[pos..]);
    let (kw_len, replacement) = [("BOTH", "TRIM("), ("LEADING", "LTRIM("), ("TRAILING", "RTRIM(")]
        .into_iter()
        .find_map(|(kw, replacement)| take_keyword(&rest[pos..], kw).map(|n| (n, replacement)))?;
    pos += kw_len;
    let ws = skip_ws(&rest[pos..]);
    if ws == 0 {
        return None;
    }
    pos += ws;
    pos += take_keyword(&rest[pos..], "FROM")?;
    let ws = skip_ws(&rest[pos..]);
    if ws == 0 {
        return None;
    }
    pos += ws;
    Some((pos, replacement))
}

/// Length of `kw` if `rest` starts with it case-insensitively and the match
/// ends at a word boundary.
fn take_keyword(rest: &str, kw: &str) -> Option<usize> {
    if rest.len() < kw.len() || !rest.is_char_boundary(kw.len()) {
        return None;
    }
    if !rest[..kw.len()].eq_ignore_ascii_case(kw) {
        return None;
    }
    match rest.as_bytes().get(kw.len()) {
        Some(&b) if is_ident_byte(b) => None,
        _ => Some(kw.len()),
    }
}

fn skip_ws(rest: &str) -> usize {
    rest.len() - rest.trim_start().len()
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn utf8_len(first: u8) -> usize {
    match first {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

/// Index just past the closing quote of the literal/identifier starting at
/// `start` (a doubled quote is an escape). Runs to the end on an unterminated
/// one, which the parser will report on its own.
fn skip_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_from_becomes_plain_trim() {
        assert_eq!(
            rewrite_trim_both_from("SELECT trim(BOTH FROM tag.name) FROM t"),
            "SELECT TRIM(tag.name) FROM t"
        );
    }

    #[test]
    fn leading_and_trailing_map_to_ltrim_rtrim() {
        assert_eq!(
            rewrite_trim_both_from("SELECT TRIM(LEADING FROM x), trim( trailing   from y) FROM t"),
            "SELECT LTRIM(x), RTRIM(y) FROM t"
        );
    }

    #[test]
    fn explicit_character_and_plain_forms_are_untouched() {
        for sql in [
            "SELECT TRIM(BOTH 'x' FROM y) FROM t",
            "SELECT TRIM(y) FROM t",
            "SELECT TRIM(  y ) FROM t",
            "SELECT xtrim(BOTH FROM y) FROM t",
            "SELECT 'trim(BOTH FROM y)' FROM t",
            "SELECT \"trim(BOTH FROM y)\" FROM t",
        ] {
            assert_eq!(rewrite_trim_both_from(sql), sql, "{sql}");
        }
    }

    #[test]
    fn rewritten_shape_parses() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let sql = "SELECT LOWER(trim(BOTH FROM t.name)) = 'flagged' FROM t";
        assert!(Parser::parse_sql(&GenericDialect {}, sql).is_err(), "precondition: sqlparser rejects it");
        let rewritten = rewrite_trim_both_from(sql);
        Parser::parse_sql(&GenericDialect {}, &rewritten).expect("rewritten parses");
    }
}
