//! `INSERT INTO t (WITH ... SELECT ...)`: a parenthesised source query.
//!
//! A query in parentheses is a query, so `INSERT INTO t (SELECT 1)` and
//! `INSERT INTO t (WITH x AS (...) SELECT * FROM x)` mean the same as the
//! forms without the parentheses. sqlparser 0.62 only recognises `(SELECT`
//! after the table name (`peek_subquery_start`); `(WITH`, `(VALUES` and
//! `((SELECT` are read as a column list and fail with "Expected: ), found:
//! x". dbt's append strategy emits `INSERT INTO t (<compiled model>)`, so
//! every model that starts with a CTE failed (#889).
//!
//! The rewrite drops the one pair of parentheses around the source query.
//! Parse-gated like the other source-level fallbacks: SQL that already
//! parses, or that still fails after the rewrite, is returned unchanged. A
//! real column list (`INSERT INTO t (a, b) SELECT ...`) never starts with a
//! query keyword, so it is never touched.

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// Drop the parentheses around the source query of an `INSERT` when they
/// make the statement fail to parse.
pub fn rewrite_insert_paren_query(sql: &str) -> String {
    if !sql.to_ascii_lowercase().contains("insert") {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return sql.to_string();
    }
    match unwrap_source_query(sql) {
        Some(candidate) if Parser::parse_sql(&dialect, &candidate).is_ok() => candidate,
        _ => sql.to_string(),
    }
}

fn unwrap_source_query(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let meaningful: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    let token = |m: usize| meaningful.get(m).map(|&i| &tokens[i]);
    let keyword = |m: usize| match token(m) {
        Some(Token::Word(w)) => Some(w.keyword),
        _ => None,
    };

    // INSERT [OVERWRITE] [INTO] [TABLE] <name>[.<name>]* (
    if keyword(0) != Some(Keyword::INSERT) {
        return None;
    }
    let mut m = 1;
    for kw in [Keyword::OVERWRITE, Keyword::INTO, Keyword::TABLE] {
        if keyword(m) == Some(kw) {
            m += 1;
        }
    }
    if !matches!(token(m), Some(Token::Word(_))) {
        return None;
    }
    m += 1;
    while matches!(token(m), Some(Token::Period)) && matches!(token(m + 1), Some(Token::Word(_))) {
        m += 2;
    }
    let open = m;
    if !matches!(token(open), Some(Token::LParen)) {
        return None;
    }
    let starts_query = matches!(token(open + 1), Some(Token::LParen))
        || matches!(
            keyword(open + 1),
            Some(Keyword::WITH | Keyword::SELECT | Keyword::VALUES)
        );
    if !starts_query {
        return None;
    }

    // The matching `)` must end the statement.
    let mut depth = 0usize;
    let mut close = None;
    for (m, &i) in meaningful.iter().enumerate().skip(open) {
        match tokens[i] {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    close = Some(m);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close?;
    let rest_is_end = (close + 1..meaningful.len())
        .all(|m| matches!(token(m), Some(Token::SemiColon | Token::EOF)));
    if !rest_is_end {
        return None;
    }

    let (skip_open, skip_close) = (meaningful[open], meaningful[close]);
    let mut out = String::with_capacity(sql.len());
    for (i, t) in tokens.iter().enumerate() {
        if i != skip_open && i != skip_close {
            out.push_str(&t.to_string());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parenthesised_source_queries_lose_their_parentheses() {
        for (sql, want) in [
            (
                "INSERT INTO s.t (WITH x AS (SELECT 1 AS a) SELECT a FROM x)",
                "INSERT INTO s.t WITH x AS (SELECT 1 AS a) SELECT a FROM x",
            ),
            (
                "insert into \"c\".\"s\".\"t\"\n(\n  with x as (select 1) select * from x\n);",
                "insert into \"c\".\"s\".\"t\"\n\n  with x as (select 1) select * from x\n;",
            ),
            (
                "INSERT INTO t (VALUES (1), (2))",
                "INSERT INTO t VALUES (1), (2)",
            ),
            ("INSERT INTO t ((SELECT 1))", "INSERT INTO t (SELECT 1)"),
            (
                "INSERT OVERWRITE TABLE t (WITH x AS (SELECT 1) SELECT * FROM x)",
                "INSERT OVERWRITE TABLE t WITH x AS (SELECT 1) SELECT * FROM x",
            ),
        ] {
            assert_eq!(rewrite_insert_paren_query(sql), want, "{sql}");
        }
    }

    #[test]
    fn column_lists_and_sql_that_parses_are_left_alone() {
        for sql in [
            // Already parses: sqlparser takes `(SELECT` as a subquery.
            "INSERT INTO t (SELECT 1)",
            // A real column list, as dbt's seed emits.
            "INSERT INTO t (a, b) VALUES (1, 2)",
            "INSERT INTO t (a, b) SELECT 1, 2",
            "INSERT INTO t WITH x AS (SELECT 1) SELECT * FROM x",
            // Something follows the closing parenthesis: not this shape.
            "INSERT INTO t (WITH x AS (SELECT 1) SELECT * FROM x) ORDER BY 1 junk",
            // Broken for another reason: keep the original error.
            "INSERT INTO t (WITH x AS (SELECT 1 SELECT * FROM x)",
            "SELECT (WITH x AS (SELECT 1) SELECT * FROM x)",
        ] {
            assert_eq!(rewrite_insert_paren_query(sql), sql, "{sql}");
        }
    }
}
