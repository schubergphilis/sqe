//! Trino-compat rewrite for parenthesis-less `VALUES` rows.
//!
//! Trino accepts a `VALUES` row that is a single bare expression:
//! `INSERT INTO t VALUES 1` and `VALUES 1, 2` are valid and mean
//! `VALUES (1)` / `VALUES (1), (2)` (each comma-separated bare expression is a
//! one-column row). sqlparser-rs (and therefore both SQE's `parse_and_classify`
//! and DataFusion's planner, which share sqlparser 0.62) require the
//! parentheses and reject the bare form with `Expected: (, found: 1`. On the
//! Trino wire path we normalize the bare form by wrapping each bare row.
//!
//! See issue #315.

use std::collections::HashSet;

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// Wrap bare `VALUES` rows in parentheses so sqlparser accepts them.
///
/// Gating keeps this safe: we only touch SQL that does not already parse, and
/// only return the rewrite when it makes the SQL parse. Well-formed SQL is
/// returned untouched, including a query with a column literally named
/// `values` (which parses, so it is never rewritten). A query that is broken
/// for an unrelated reason keeps its original error. Wrapping a bare expression
/// in parentheses is the exact Trino normalization, so the transform preserves
/// semantics by construction; the re-parse only confirms the result is
/// well-formed.
pub fn rewrite_paren_less_values(sql: &str) -> String {
    // Fast path: a `VALUES` keyword must be present for any rewrite to fire.
    if !sql.to_ascii_lowercase().contains("values") {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    // Only act when the original does not parse; otherwise leave it untouched.
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return sql.to_string();
    }
    match wrap_bare_values(sql) {
        Some(candidate) if candidate != sql && Parser::parse_sql(&dialect, &candidate).is_ok() => {
            candidate
        }
        _ => sql.to_string(),
    }
}

/// Keywords that can follow a `VALUES` row list and so terminate it
/// (`VALUES 1, 2 ORDER BY 1`, set operations, etc.).
fn is_terminator_keyword(k: Keyword) -> bool {
    matches!(
        k,
        Keyword::ORDER
            | Keyword::LIMIT
            | Keyword::OFFSET
            | Keyword::FETCH
            | Keyword::UNION
            | Keyword::INTERSECT
            | Keyword::EXCEPT
            | Keyword::WINDOW
    )
}

fn wrap_bare_values(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;

    // Indices into `tokens` of the non-whitespace tokens, in order.
    let meaningful: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();

    // Token indices to emit a `(` before / a `)` after, during rebuild.
    let mut prepend: HashSet<usize> = HashSet::new();
    let mut append: HashSet<usize> = HashSet::new();

    let mut m = 0usize;
    let mut depth: i32 = 0;
    while m < meaningful.len() {
        match &tokens[meaningful[m]] {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(w) if w.keyword == Keyword::VALUES => {
                let (next_m, next_depth) = process_values_list(
                    &tokens,
                    &meaningful,
                    m + 1,
                    depth,
                    &mut prepend,
                    &mut append,
                );
                m = next_m;
                depth = next_depth;
                continue;
            }
            _ => {}
        }
        m += 1;
    }

    if prepend.is_empty() && append.is_empty() {
        return None;
    }

    let mut out = String::with_capacity(sql.len() + prepend.len() + append.len());
    for (i, t) in tokens.iter().enumerate() {
        if prepend.contains(&i) {
            out.push('(');
        }
        out.push_str(&t.to_string());
        if append.contains(&i) {
            out.push(')');
        }
    }
    Some(out)
}

/// Process the row list that follows a `VALUES` keyword. `start_m` is the
/// meaningful index of the first token after `VALUES`; `base_depth` is the
/// paren depth at the `VALUES` keyword. Returns the meaningful index for the
/// main scan to resume at and the paren depth there.
fn process_values_list(
    tokens: &[Token],
    meaningful: &[usize],
    start_m: usize,
    base_depth: i32,
    prepend: &mut HashSet<usize>,
    append: &mut HashSet<usize>,
) -> (usize, i32) {
    let mut m = start_m;
    loop {
        if m >= meaningful.len() {
            return (m, base_depth);
        }
        // A token that cannot begin a row means this is not a row list we can
        // normalize (or the list is already over). Hand control back.
        match &tokens[meaningful[m]] {
            Token::Word(w) if is_terminator_keyword(w.keyword) => return (m, base_depth),
            Token::SemiColon | Token::RParen | Token::Comma => return (m, base_depth),
            _ => {}
        }

        // Collect this row: advance until a top-level comma (next row), a
        // closing paren that drops below base depth (end of an enclosing
        // scope), a terminator keyword, a semicolon, or EOF.
        let row_start_m = m;
        let mut row_depth = base_depth;
        let mut j = m;
        let mut ended_by_comma = false;
        while j < meaningful.len() {
            match &tokens[meaningful[j]] {
                Token::LParen => {
                    row_depth += 1;
                    j += 1;
                }
                Token::RParen => {
                    if row_depth == base_depth {
                        break;
                    }
                    row_depth -= 1;
                    j += 1;
                }
                Token::Comma if row_depth == base_depth => {
                    ended_by_comma = true;
                    break;
                }
                Token::SemiColon if row_depth == base_depth => break,
                Token::Word(w) if row_depth == base_depth && is_terminator_keyword(w.keyword) => {
                    break;
                }
                _ => j += 1,
            }
        }

        // The row spans meaningful indices [row_start_m, j).
        if j > row_start_m && !is_fully_parenthesized(tokens, meaningful, row_start_m, j) {
            prepend.insert(meaningful[row_start_m]);
            append.insert(meaningful[j - 1]);
        }

        if ended_by_comma {
            m = j + 1;
            continue;
        }
        // `j` points at a terminator / closing paren / EOF: let the main scan
        // resume there so it accounts for that token's depth effect.
        return (j, base_depth);
    }
}

/// Is the row at meaningful indices `[start_m, end_m)` a single parenthesized
/// group spanning the whole row (`(1)`, `(1, 2)`), versus a bare expression
/// that merely starts with a paren (`(1) + 1`)?
fn is_fully_parenthesized(
    tokens: &[Token],
    meaningful: &[usize],
    start_m: usize,
    end_m: usize,
) -> bool {
    if !matches!(tokens[meaningful[start_m]], Token::LParen) {
        return false;
    }
    let mut d = 0i32;
    for (k, mi) in meaningful.iter().enumerate().take(end_m).skip(start_m) {
        match tokens[*mi] {
            Token::LParen => d += 1,
            Token::RParen => {
                d -= 1;
                if d == 0 {
                    // Fully parenthesized only if the matching close is the row's
                    // last token.
                    return k == end_m - 1;
                }
            }
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parses(sql: &str) -> bool {
        Parser::parse_sql(&GenericDialect {}, sql).is_ok()
    }

    #[test]
    fn wraps_single_bare_value_in_insert() {
        let out = rewrite_paren_less_values("INSERT INTO t VALUES 1");
        assert_eq!(out, "INSERT INTO t VALUES (1)");
        assert!(parses(&out));
    }

    #[test]
    fn wraps_multiple_bare_values() {
        let out = rewrite_paren_less_values("VALUES 1, 2");
        assert!(parses(&out), "got: {out}");
        assert_eq!(out, "VALUES (1), (2)");
    }

    #[test]
    fn wraps_bare_values_in_insert_multi_row() {
        let out = rewrite_paren_less_values("INSERT INTO t VALUES 1, 2");
        assert_eq!(out, "INSERT INTO t VALUES (1), (2)");
        assert!(parses(&out));
    }

    #[test]
    fn already_parenthesized_is_untouched() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "VALUES (1), (2)",
            "INSERT INTO t VALUES (1, 2), (3, 4)",
        ] {
            assert_eq!(rewrite_paren_less_values(sql), sql);
        }
    }

    #[test]
    fn mixed_bare_and_parenthesized_rows() {
        // Second row is already a parenthesized single-column row; only the
        // first needs wrapping.
        let out = rewrite_paren_less_values("INSERT INTO t VALUES 1, (2)");
        assert!(parses(&out), "got: {out}");
        assert_eq!(out, "INSERT INTO t VALUES (1), (2)");
    }

    #[test]
    fn bare_function_call_is_one_row_not_split_on_inner_comma() {
        let out = rewrite_paren_less_values("VALUES coalesce(1, 2)");
        assert!(parses(&out), "got: {out}");
        // The inner comma is inside parens, so it is one row, not two.
        assert_eq!(out, "VALUES (coalesce(1, 2))");
    }

    #[test]
    fn string_literal_with_comma_is_one_value() {
        let out = rewrite_paren_less_values("INSERT INTO t VALUES 'a,b'");
        assert!(parses(&out), "got: {out}");
        assert_eq!(out, "INSERT INTO t VALUES ('a,b')");
    }

    #[test]
    fn bare_values_in_subquery() {
        let out = rewrite_paren_less_values("SELECT * FROM (VALUES 1, 2) AS x(a)");
        assert!(parses(&out), "got: {out}");
        assert_eq!(out, "SELECT * FROM (VALUES (1), (2)) AS x(a)");
    }

    #[test]
    fn column_named_values_is_untouched() {
        // Parses fine as-is (a column reference), so it is never rewritten.
        let sql = "SELECT values FROM t";
        assert_eq!(rewrite_paren_less_values(sql), sql);
    }

    #[test]
    fn no_values_keyword_is_untouched() {
        let sql = "SELECT a, b FROM t WHERE a = 1";
        assert_eq!(rewrite_paren_less_values(sql), sql);
    }

    #[test]
    fn unrelated_parse_error_keeps_original() {
        // No bare-VALUES rewrite makes this parse, so the original is returned
        // and the downstream planner surfaces its own error.
        let sql = "INSERT INTO t VALUES";
        assert_eq!(rewrite_paren_less_values(sql), sql);
    }

    #[test]
    fn bare_values_with_order_by_terminator() {
        let out = rewrite_paren_less_values("VALUES 1, 2 ORDER BY 1");
        assert!(parses(&out), "got: {out}");
        assert_eq!(out, "VALUES (1), (2) ORDER BY 1");
    }
}
