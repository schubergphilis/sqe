//! Trino-compat rewrite for the bare `TABLE <name>` statement.
//!
//! Trino (and standard SQL) accept `TABLE t` as shorthand for
//! `SELECT * FROM t`, including trailing clauses: `TABLE t ORDER BY 1`,
//! `TABLE t LIMIT 10`, `TABLE t OFFSET 5`. sqlparser-rs 0.62 does not model
//! the bare form and rejects it with `Expected: an SQL statement, found:
//! TABLE`. On the Trino wire path we normalize a leading `TABLE` keyword into
//! `SELECT * FROM`, leaving the rest of the statement untouched.
//!
//! See issue #351c.

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// Rewrite a leading `TABLE <name>` statement to `SELECT * FROM <name>`.
///
/// Gating keeps this safe: the rewrite fires only when the SQL does not
/// already parse (so `CREATE TABLE`, `DROP TABLE`, `SHOW CREATE TABLE`, and a
/// real column named `table` are never touched, since those parse) and only
/// when the *first meaningful token* is the `TABLE` keyword. The result is
/// re-parsed and returned only if it parses, so a statement broken for an
/// unrelated reason keeps its original error. Replacing the leading `TABLE`
/// keyword with `SELECT * FROM` is the exact SQL-standard expansion, so the
/// transform preserves semantics by construction.
pub fn rewrite_bare_table(sql: &str) -> String {
    // Fast path: a `TABLE` keyword must be present for any rewrite to fire.
    if !sql.to_ascii_lowercase().contains("table") {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    // Only act when the original does not parse; otherwise leave it untouched.
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return sql.to_string();
    }
    match rewrite_leading_table(sql) {
        Some(candidate)
            if candidate != sql && Parser::parse_sql(&dialect, &candidate).is_ok() =>
        {
            candidate
        }
        _ => sql.to_string(),
    }
}

/// If the first meaningful token is the `TABLE` keyword, splice `SELECT * FROM`
/// in its place, preserving everything before it (leading whitespace) and the
/// rest of the statement verbatim.
fn rewrite_leading_table(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;

    // Index of the first non-whitespace token.
    let first = tokens
        .iter()
        .position(|t| !matches!(t, Token::Whitespace(_)))?;

    // The first meaningful token must be the bare `TABLE` keyword; anything
    // else (CREATE, DROP, ALTER, SHOW, an identifier, ...) is not this shape.
    match &tokens[first] {
        Token::Word(w) if w.keyword == Keyword::TABLE => {}
        _ => return None,
    }

    // Rebuild: emit every token before the `TABLE` keyword (only whitespace,
    // by construction), replace the keyword with `SELECT * FROM`, then emit the
    // remainder unchanged. A space after `FROM` is guaranteed because the
    // tokenizer keeps the whitespace that followed the original `TABLE`.
    let mut out = String::with_capacity(sql.len() + 12);
    for (i, t) in tokens.iter().enumerate() {
        if i == first {
            out.push_str("SELECT * FROM");
        } else {
            out.push_str(&t.to_string());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parses(sql: &str) -> bool {
        Parser::parse_sql(&GenericDialect {}, sql).is_ok()
    }

    #[test]
    fn rewrites_bare_table() {
        let out = rewrite_bare_table("TABLE nation");
        assert_eq!(out, "SELECT * FROM nation");
        assert!(parses(&out));
    }

    #[test]
    fn rewrites_bare_table_with_limit() {
        let out = rewrite_bare_table("TABLE nation LIMIT 10");
        assert_eq!(out, "SELECT * FROM nation LIMIT 10");
        assert!(parses(&out));
    }

    #[test]
    fn rewrites_bare_table_with_order_by() {
        let out = rewrite_bare_table("TABLE nation ORDER BY 1");
        assert_eq!(out, "SELECT * FROM nation ORDER BY 1");
        assert!(parses(&out));
    }

    #[test]
    fn rewrites_qualified_bare_table() {
        let out = rewrite_bare_table("TABLE tpch.sf1.nation");
        assert_eq!(out, "SELECT * FROM tpch.sf1.nation");
        assert!(parses(&out));
    }

    #[test]
    fn preserves_leading_whitespace() {
        let out = rewrite_bare_table("  TABLE nation");
        assert_eq!(out, "  SELECT * FROM nation");
        assert!(parses(&out));
    }

    #[test]
    fn create_table_is_untouched() {
        // Parses as-is, so it is never rewritten.
        let sql = "CREATE TABLE t (a int)";
        assert_eq!(rewrite_bare_table(sql), sql);
    }

    #[test]
    fn drop_table_is_untouched() {
        let sql = "DROP TABLE t";
        assert_eq!(rewrite_bare_table(sql), sql);
    }

    #[test]
    fn show_create_table_is_untouched() {
        let sql = "SHOW CREATE TABLE t";
        assert_eq!(rewrite_bare_table(sql), sql);
    }

    #[test]
    fn select_with_table_alias_is_untouched() {
        // `TABLE` appears but not as the first token; and this parses anyway.
        let sql = "SELECT * FROM my_table";
        assert_eq!(rewrite_bare_table(sql), sql);
    }

    #[test]
    fn no_table_keyword_is_untouched() {
        let sql = "SELECT a, b FROM t WHERE a = 1";
        assert_eq!(rewrite_bare_table(sql), sql);
    }

    #[test]
    fn unrelated_parse_error_keeps_original() {
        // Not a leading-TABLE shape; no rewrite makes it parse, so the original
        // is returned and the downstream planner surfaces its own error.
        let sql = "TABLE";
        assert_eq!(rewrite_bare_table(sql), sql);
    }
}
