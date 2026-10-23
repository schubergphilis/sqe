//! Placeholder scanning for prepared statements.
//!
//! The Trino HTTP path (`EXECUTE <name> USING ...`) binds parameters by
//! replacing `?` placeholders with SQL literals
//! ([`substitute_placeholders`]). The Flight SQL path plans the statement once
//! with numbered `$1..$N` placeholders ([`number_placeholders`]) and binds typed
//! values into the logical plan instead of splicing text (finding 406).
//!
//! Both are driven by one lexer, [`placeholder_offsets`], which finds every `?`
//! that is real SQL: outside single-quoted strings (including `E'...'` escape
//! strings, where a backslash escapes the next character), double-quoted
//! identifiers, dollar-quoted strings (`$$...$$`, `$tag$...$tag$`), `--`
//! line comments and `/* ... */` block comments. A `?` in any of those used to
//! consume a bind value and shift every later parameter.

/// Byte offsets of every `?` in `sql` that is a bind placeholder.
///
/// See the module docs for what is skipped. Nested block comments are not
/// tracked (the first `*/` closes the comment), matching sqlparser's
/// tokenizer for the Generic dialect.
pub fn placeholder_offsets(sql: &str) -> Vec<usize> {
    let bytes = sql.as_bytes();
    let mut offsets = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'?' => {
                offsets.push(i);
                i += 1;
            }
            // Line comment: skip to end of line.
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = memchr_from(bytes, i, b'\n').unwrap_or(bytes.len());
            }
            // Block comment: skip to the closing `*/`.
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = find_from(bytes, i + 2, b"*/")
                    .map(|p| p + 2)
                    .unwrap_or(bytes.len());
            }
            // Escape string `E'...'`: backslash escapes the next byte.
            b'E' | b'e'
                if bytes.get(i + 1) == Some(&b'\'')
                    && !is_ident_byte(bytes.get(i.wrapping_sub(1)).copied()) =>
            {
                i += 2;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
                        b'\'' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            // Plain single-quoted string; `''` is an escaped quote and simply
            // closes and reopens, which this loop handles by returning here.
            b'\'' => {
                i += 1;
                i = memchr_from(bytes, i, b'\'')
                    .map(|p| p + 1)
                    .unwrap_or(bytes.len());
            }
            b'"' => {
                i += 1;
                i = memchr_from(bytes, i, b'"')
                    .map(|p| p + 1)
                    .unwrap_or(bytes.len());
            }
            // Dollar-quoted string: `$$...$$` or `$tag$...$tag$`. A `$` followed
            // by a digit is a numbered placeholder, never a tag.
            b'$' => match dollar_tag_end(bytes, i) {
                Some(tag_end) => {
                    let tag = &bytes[i..tag_end];
                    i = find_from(bytes, tag_end, tag)
                        .map(|p| p + tag.len())
                        .unwrap_or(bytes.len());
                }
                None => i += 1,
            },
            _ => i += 1,
        }
    }
    offsets
}

fn is_ident_byte(b: Option<u8>) -> bool {
    matches!(b, Some(b) if b.is_ascii_alphanumeric() || b == b'_')
}

/// If a dollar-quote opening tag starts at `start`, return the byte offset just
/// past its closing `$`.
fn dollar_tag_end(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes[start], b'$');
    let mut j = start + 1;
    match bytes.get(j) {
        Some(b'$') => return Some(j + 1),
        Some(b) if b.is_ascii_alphabetic() || *b == b'_' => {}
        _ => return None,
    }
    while let Some(b) = bytes.get(j) {
        if *b == b'$' {
            return Some(j + 1);
        }
        if !(b.is_ascii_alphanumeric() || *b == b'_') {
            return None;
        }
        j += 1;
    }
    None
}

fn memchr_from(bytes: &[u8], from: usize, needle: u8) -> Option<usize> {
    bytes[from.min(bytes.len())..]
        .iter()
        .position(|b| *b == needle)
        .map(|p| p + from)
}

fn find_from(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || from >= bytes.len() {
        return None;
    }
    bytes[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| p + from)
}

/// Replace `?` placeholders in `sql` with the bound literals, in order.
///
/// Placeholders inside strings, quoted identifiers, dollar-quoted strings and
/// comments are ignored (see [`placeholder_offsets`]). Returns an error if the
/// placeholder count differs from the number of bound values, mirroring a JDBC
/// `SQLException` rather than executing with a partial bind. Each value in
/// `params` is inserted verbatim, so callers must pass already-quoted SQL
/// literals (e.g. `'foo'`, `42`, `DATE '2020-01-01'`). The one exception is a
/// literal starting with `-`, which goes in parenthesized: `1-?` bound to `-5`
/// would otherwise read `1--5`, a line comment that swallows the rest of the
/// line (and with it any predicate after the placeholder).
pub fn substitute_placeholders(sql: &str, params: &[String]) -> Result<String, String> {
    let offsets = placeholder_offsets(sql);
    if offsets.len() != params.len() {
        return Err(format!(
            "prepared statement expected {} parameters but bind supplied {}",
            offsets.len(),
            params.len()
        ));
    }
    let mut out = String::with_capacity(sql.len() + params.iter().map(|s| s.len()).sum::<usize>());
    let mut last = 0usize;
    for (offset, literal) in offsets.iter().zip(params) {
        out.push_str(&sql[last..*offset]);
        if literal.starts_with('-') {
            out.push('(');
            out.push_str(literal);
            out.push(')');
        } else {
            out.push_str(literal);
        }
        last = offset + 1;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// Replace each `?` placeholder in `sql` with a numbered `$1`, `$2`, ...
/// placeholder, returning the rewritten SQL and the placeholder count.
///
/// Same lexer as [`substitute_placeholders`]. Numbered placeholders let
/// DataFusion plan a prepared statement (inferring placeholder types and the
/// output schema) without bound values, which `DESCRIBE OUTPUT` /
/// `DESCRIBE INPUT` and the Flight SQL prepared-statement store need.
pub fn number_placeholders(sql: &str) -> (String, usize) {
    let offsets = placeholder_offsets(sql);
    let mut out = String::with_capacity(sql.len() + offsets.len() * 3);
    let mut last = 0usize;
    for (n, offset) in offsets.iter().enumerate() {
        out.push_str(&sql[last..*offset]);
        out.push('$');
        out.push_str(&(n + 1).to_string());
        last = offset + 1;
    }
    out.push_str(&sql[last..]);
    (out, offsets.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_placeholders_basic() {
        let (sql, n) = number_placeholders("SELECT * FROM t WHERE a = ? AND b = ?");
        assert_eq!(sql, "SELECT * FROM t WHERE a = $1 AND b = $2");
        assert_eq!(n, 2);
    }

    #[test]
    fn number_placeholders_skips_inside_quotes() {
        let (sql, n) = number_placeholders("SELECT '? lit', c FROM t WHERE c = ?");
        assert_eq!(sql, "SELECT '? lit', c FROM t WHERE c = $1");
        assert_eq!(n, 1);
    }

    #[test]
    fn number_placeholders_none() {
        let (sql, n) = number_placeholders("SELECT 1");
        assert_eq!(sql, "SELECT 1");
        assert_eq!(n, 0);
    }

    #[test]
    fn number_placeholders_skips_comments() {
        let (sql, n) = number_placeholders("SELECT ? -- really?\n/* or? */ FROM t WHERE a = ?");
        assert_eq!(sql, "SELECT $1 -- really?\n/* or? */ FROM t WHERE a = $2");
        assert_eq!(n, 2);
    }

    #[test]
    fn substitute_placeholders_basic() {
        let sql = "SELECT * FROM t WHERE a = ? AND b = ?";
        let out = substitute_placeholders(sql, &["1".into(), "'foo'".into()]).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE a = 1 AND b = 'foo'");
    }

    #[test]
    fn substitute_placeholders_skips_inside_quotes() {
        let sql = "SELECT '? literal', c FROM t WHERE c = ?";
        let out = substitute_placeholders(sql, &["42".into()]).unwrap();
        assert_eq!(out, "SELECT '? literal', c FROM t WHERE c = 42");
    }

    #[test]
    fn substitute_placeholders_mismatch_errors() {
        assert!(substitute_placeholders("?", &[]).is_err());
        assert!(substitute_placeholders("?", &["a".into(), "b".into()]).is_err());
    }

    /// Finding 406 acceptance: a `?` in a line comment is not a parameter.
    #[test]
    fn substitute_placeholders_ignores_line_comment() {
        let out = substitute_placeholders("SELECT 1 -- what?", &[]).unwrap();
        assert_eq!(out, "SELECT 1 -- what?");
    }

    /// Finding 406 acceptance: a `?` in a block comment binds nothing; the
    /// one real placeholder binds exactly one value.
    #[test]
    fn substitute_placeholders_ignores_block_comment() {
        let out = substitute_placeholders("SELECT ? /* why? */", &["7".into()]).unwrap();
        assert_eq!(out, "SELECT 7 /* why? */");
        assert!(substitute_placeholders("SELECT ? /* why? */", &["7".into(), "8".into()]).is_err());
    }

    #[test]
    fn substitute_placeholders_ignores_escape_string() {
        // The escaped quote inside E'...' must not end the string early.
        let sql = r"SELECT E'it\'s ?', ? FROM t";
        let out = substitute_placeholders(sql, &["1".into()]).unwrap();
        assert_eq!(out, r"SELECT E'it\'s ?', 1 FROM t");
        // `e` at the end of an identifier is not an escape-string prefix.
        let (numbered, n) = number_placeholders("SELECT one'x' , ?");
        assert_eq!(n, 1);
        assert!(numbered.ends_with("$1"));
    }

    #[test]
    fn substitute_placeholders_ignores_dollar_quoted() {
        let sql = "SELECT $$who?$$, $tag$ what? $tag$, ? FROM t";
        let out = substitute_placeholders(sql, &["1".into()]).unwrap();
        assert_eq!(out, "SELECT $$who?$$, $tag$ what? $tag$, 1 FROM t");
        // A numbered placeholder is not a dollar-quote tag.
        assert_eq!(placeholder_offsets("SELECT $1, ?"), vec![11]);
    }

    #[test]
    fn substitute_placeholders_doubled_quote_inside_string() {
        let sql = "SELECT 'it''s ?', ?";
        let out = substitute_placeholders(sql, &["1".into()]).unwrap();
        assert_eq!(out, "SELECT 'it''s ?', 1");
    }

    /// `1-?` bound to `-5` must not become `1--5`, a line comment that drops
    /// the predicate after it.
    #[test]
    fn substitute_placeholders_parenthesizes_negative_literals() {
        let sql = "DELETE FROM t WHERE a = 1-? AND b = ?\nAND c = 1";
        let out = substitute_placeholders(sql, &["-5".into(), "-2.5".into()]).unwrap();
        assert_eq!(
            out,
            "DELETE FROM t WHERE a = 1-(-5) AND b = (-2.5)\nAND c = 1"
        );
        assert_eq!(placeholder_offsets(&out), Vec::<usize>::new());
        assert!(!out.contains("--"));
    }

    #[test]
    fn unterminated_constructs_do_not_panic() {
        assert!(placeholder_offsets("SELECT '?").is_empty());
        assert!(placeholder_offsets("SELECT /* ?").is_empty());
        assert!(placeholder_offsets("SELECT $$ ?").is_empty());
        assert!(placeholder_offsets("SELECT E'?").is_empty());
        assert_eq!(placeholder_offsets("SELECT $"), Vec::<usize>::new());
    }
}
