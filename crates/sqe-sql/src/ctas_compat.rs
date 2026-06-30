//! Trino-compat rewrites for `CREATE TABLE ... AS SELECT` modifiers that
//! sqlparser-rs rejects.
//!
//! Two Trino CTAS forms fail to parse and are normalized here before the SQL
//! reaches the parser:
//!
//! - **`WITH [NO] DATA` suffix (#322).** Trino accepts a trailing `WITH DATA`
//!   (materialize rows, the default) or `WITH NO DATA` (create the table
//!   structure with no rows). sqlparser stops at the `WITH` with
//!   `Expected: end of statement, found: WITH`. `WITH DATA` is stripped;
//!   `WITH NO DATA` is turned into an empty result by wrapping the query and
//!   appending `LIMIT 0`, which yields the correct output schema with zero
//!   rows.
//!
//! - **Column-alias list `(a, b)` (#328).** Trino lets a CTAS rename the
//!   query's output columns with a name-only list:
//!   `CREATE TABLE t (id, part) AS <query>`. sqlparser reads the parenthesized
//!   list as a column-definition list and demands a type after each name
//!   (`Expected: a data type name`). The list is rewritten to a derived-table
//!   column-alias list (`... AS SELECT * FROM (<query>) AS x(id, part)`), which
//!   renames positionally exactly as Trino does and works for any query shape
//!   (SELECT, VALUES, set operations).
//!
//! Both are parse-gated like [`crate::paren_less_values`]: SQL that already
//! parses is returned untouched, and a rewrite is only adopted when it makes
//! the SQL parse. A query broken for an unrelated reason keeps its original
//! error.

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// Derived-table alias used when rewriting a CTAS column-alias list or a
/// `WITH NO DATA` wrap. Unlikely to collide with a user identifier.
const CTA_ALIAS: &str = "__sqe_cta_cols";
const NO_DATA_ALIAS: &str = "__sqe_no_data";

/// Normalize Trino CTAS `WITH [NO] DATA` suffixes and column-alias lists into
/// SQL sqlparser accepts. A no-op for SQL that already parses.
pub fn rewrite_ctas_compat(sql: &str) -> String {
    // Fast path: only a CREATE statement can be a CTAS.
    if !sql.to_ascii_lowercase().contains("create") {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return sql.to_string();
    }
    match rewrite_ctas(sql) {
        Some(candidate)
            if candidate != sql && Parser::parse_sql(&dialect, &candidate).is_ok() =>
        {
            candidate
        }
        _ => sql.to_string(),
    }
}

/// Is this word a meaningful (non-whitespace) token? Helper for the scan.
fn is_word(token: &Token, kw: Keyword) -> bool {
    matches!(token, Token::Word(w) if w.keyword == kw)
}

/// Render a contiguous token slice back to source text.
fn render(tokens: &[Token]) -> String {
    tokens.iter().map(Token::to_string).collect()
}

fn rewrite_ctas(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;

    // Meaningful (non-whitespace) token indices, in order.
    let meaningful: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    if meaningful.is_empty() {
        return None;
    }

    // Must start with CREATE.
    if !is_word(&tokens[meaningful[0]], Keyword::CREATE) {
        return None;
    }

    // Find the TABLE keyword (skipping OR REPLACE / TEMPORARY / EXTERNAL / ...).
    let mut p = 1;
    while p < meaningful.len() && !is_word(&tokens[meaningful[p]], Keyword::TABLE) {
        // Bail if we hit AS or `(` before TABLE: not a CREATE TABLE.
        if is_word(&tokens[meaningful[p]], Keyword::AS)
            || matches!(tokens[meaningful[p]], Token::LParen)
        {
            return None;
        }
        p += 1;
    }
    if p >= meaningful.len() {
        return None;
    }
    p += 1; // past TABLE

    // Skip optional IF NOT EXISTS.
    if p < meaningful.len() && is_word(&tokens[meaningful[p]], Keyword::IF) {
        p += 1;
        if p < meaningful.len() && is_word(&tokens[meaningful[p]], Keyword::NOT) {
            p += 1;
        }
        if p < meaningful.len() && is_word(&tokens[meaningful[p]], Keyword::EXISTS) {
            p += 1;
        }
    }

    // Table name: a run of identifier words and periods. Stops at `(` or AS.
    while p < meaningful.len() {
        match &tokens[meaningful[p]] {
            Token::Word(w) if w.keyword != Keyword::AS => p += 1,
            Token::Period => p += 1,
            _ => break,
        }
    }
    if p >= meaningful.len() {
        return None;
    }

    // Optional parenthesized list right after the name. An identifier-only list
    // is a Trino column-alias list (#328); a list containing types is a normal
    // column-definition list and is left in place.
    let mut alias_open_mi: Option<usize> = None;
    let mut aliases: Vec<String> = Vec::new();
    if matches!(tokens[meaningful[p]], Token::LParen) {
        let open_mi = p;
        // Find the matching close paren at the same depth.
        let mut depth = 0i32;
        let mut close_mi = None;
        let mut q = p;
        while q < meaningful.len() {
            match tokens[meaningful[q]] {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        close_mi = Some(q);
                        break;
                    }
                }
                _ => {}
            }
            q += 1;
        }
        let close_mi = close_mi?;

        if let Some(idents) = parse_ident_only_list(&tokens, &meaningful, open_mi, close_mi) {
            alias_open_mi = Some(open_mi);
            aliases = idents;
        }
        // Continue after the paren group regardless (a typed coldef list stays).
        p = close_mi + 1;
    }

    // Expect the CTAS `AS`.
    if p >= meaningful.len() || !is_word(&tokens[meaningful[p]], Keyword::AS) {
        return None;
    }
    let as_idx = meaningful[p];

    // Detect a trailing `WITH DATA` / `WITH NO DATA` (depth 0, statement tail,
    // before an optional `;`).
    let mut last = meaningful.len() - 1;
    if matches!(tokens[meaningful[last]], Token::SemiColon) {
        if last == 0 {
            return None;
        }
        last -= 1;
    }
    let mut with_data = false;
    let mut with_no_data = false;
    let mut with_mi: Option<usize> = None;
    // `... WITH DATA`
    if last >= 1
        && word_eq(&tokens[meaningful[last]], "DATA")
        && word_eq(&tokens[meaningful[last - 1]], "WITH")
    {
        with_data = true;
        with_mi = Some(last - 1);
    } else if last >= 2
        && word_eq(&tokens[meaningful[last]], "DATA")
        && word_eq(&tokens[meaningful[last - 1]], "NO")
        && word_eq(&tokens[meaningful[last - 2]], "WITH")
    {
        with_no_data = true;
        with_mi = Some(last - 2);
    }

    let has_alias = alias_open_mi.is_some();
    if !has_alias && !with_data && !with_no_data {
        return None; // nothing this rewrite handles
    }

    // The query body spans the tokens after `AS`, up to the WITH clause (if any)
    // or the trailing `;` / end.
    let query_end_tok = match with_mi {
        Some(wmi) => meaningful[wmi],
        None => {
            // up to (and excluding) a trailing semicolon, else end of tokens.
            if matches!(tokens[meaningful[meaningful.len() - 1]], Token::SemiColon) {
                meaningful[meaningful.len() - 1]
            } else {
                tokens.len()
            }
        }
    };
    let query_str = render(&tokens[(as_idx + 1)..query_end_tok]);
    let query_str = query_str.trim();
    if query_str.is_empty() {
        return None;
    }

    // Header: everything up to the alias list `(` (if rewriting it) or up to
    // `AS` (preserving any typed column-def list verbatim).
    let header_end_tok = match alias_open_mi {
        Some(open_mi) => meaningful[open_mi],
        None => as_idx,
    };
    let header = render(&tokens[0..header_end_tok]);
    let header = header.trim_end();

    // Build the body, applying column aliases and/or emptiness.
    let mut body = if has_alias {
        format!(
            "SELECT * FROM ({query_str}) AS {CTA_ALIAS}({})",
            aliases.join(", ")
        )
    } else {
        query_str.to_string()
    };
    if with_no_data {
        body = format!("SELECT * FROM ({body}) AS {NO_DATA_ALIAS} LIMIT 0");
    }

    let mut out = format!("{header} AS {body}");
    if matches!(tokens[meaningful[meaningful.len() - 1]], Token::SemiColon) {
        out.push(';');
    }
    Some(out)
}

/// True if `token` is a (possibly keyword) word whose text equals `name`
/// ignoring case. Matches by text, not keyword, so non-reserved words like
/// `DATA` are recognized regardless of how the tokenizer classifies them.
fn word_eq(token: &Token, name: &str) -> bool {
    matches!(token, Token::Word(w) if w.value.eq_ignore_ascii_case(name))
}

/// If the parenthesized group at meaningful indices `[open_mi, close_mi]` is a
/// comma-separated list of bare identifiers (no types, no nested parens),
/// return the identifier values. Returns `None` for a typed column-definition
/// list, which must be left untouched.
fn parse_ident_only_list(
    tokens: &[Token],
    meaningful: &[usize],
    open_mi: usize,
    close_mi: usize,
) -> Option<Vec<String>> {
    // Inner meaningful indices, exclusive of the parens.
    let inner = &meaningful[(open_mi + 1)..close_mi];
    if inner.is_empty() {
        return None;
    }
    let mut idents = Vec::new();
    // Each segment between top-level commas must be exactly one Word.
    let mut segment: Vec<&Token> = Vec::new();
    let flush = |seg: &mut Vec<&Token>, out: &mut Vec<String>| -> Option<()> {
        if seg.len() != 1 {
            return None; // empty or multi-token (has a type) => not an alias list
        }
        match seg[0] {
            Token::Word(w) => {
                out.push(w.to_string());
                seg.clear();
                Some(())
            }
            _ => None,
        }
    };
    for &ti in inner {
        match &tokens[ti] {
            Token::Comma => {
                flush(&mut segment, &mut idents)?;
            }
            // Any paren inside means it is not a simple alias list.
            Token::LParen | Token::RParen => return None,
            other => segment.push(other),
        }
    }
    flush(&mut segment, &mut idents)?;
    Some(idents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parses(sql: &str) -> bool {
        Parser::parse_sql(&GenericDialect {}, sql).is_ok()
    }

    #[test]
    fn with_data_suffix_is_stripped() {
        let out = rewrite_ctas_compat("CREATE TABLE iceberg.default.t AS SELECT 1 a WITH DATA");
        assert!(parses(&out), "rewrite must parse: {out}");
        assert!(out.contains("SELECT 1 a"), "query preserved: {out}");
        assert!(!out.to_ascii_uppercase().contains("WITH DATA"), "suffix gone: {out}");
    }

    #[test]
    fn with_no_data_becomes_empty_via_limit_zero() {
        let out = rewrite_ctas_compat("CREATE TABLE t AS SELECT 1 a WITH NO DATA");
        assert!(parses(&out), "rewrite must parse: {out}");
        assert!(out.to_ascii_uppercase().contains("LIMIT 0"), "empties result: {out}");
        assert!(!out.to_ascii_uppercase().contains("NO DATA"), "suffix gone: {out}");
    }

    #[test]
    fn column_alias_list_becomes_derived_table_aliases() {
        let out =
            rewrite_ctas_compat("CREATE TABLE iceberg.default.t (id, part_col) AS VALUES (0, 'a'), (1, 'b')");
        assert!(parses(&out), "rewrite must parse: {out}");
        assert!(out.contains(&format!("{CTA_ALIAS}(id, part_col)")), "aliases applied: {out}");
        assert!(out.contains("VALUES (0, 'a'), (1, 'b')"), "query preserved: {out}");
        // The typed-coldef interpretation (which demanded a type) is gone.
        assert!(!out.contains("(id, part_col) AS"), "alias list moved off the name: {out}");
    }

    #[test]
    fn alias_list_and_with_no_data_combine() {
        let out = rewrite_ctas_compat("CREATE TABLE t (a, b) AS VALUES (1, 2) WITH NO DATA");
        assert!(parses(&out), "rewrite must parse: {out}");
        assert!(out.contains(&format!("{CTA_ALIAS}(a, b)")), "aliases applied: {out}");
        assert!(out.to_ascii_uppercase().contains("LIMIT 0"), "empties result: {out}");
    }

    #[test]
    fn typed_column_def_list_keeps_definition_but_strips_with_data() {
        // A typed coldef list is NOT an alias list: leave it in place, only the
        // WITH DATA suffix is stripped.
        let out = rewrite_ctas_compat("CREATE TABLE t (a int) AS SELECT 1 WITH DATA");
        assert!(parses(&out), "rewrite must parse: {out}");
        assert!(out.contains("(a int)"), "typed coldef preserved: {out}");
        assert!(!out.to_ascii_uppercase().contains("WITH DATA"), "suffix gone: {out}");
        assert!(!out.contains(CTA_ALIAS), "must not alias-wrap a typed list: {out}");
    }

    #[test]
    fn quoted_name_with_alias_list() {
        let out = rewrite_ctas_compat(r#"CREATE TABLE "cat"."sch"."t" (x, y) AS SELECT 1, 2"#);
        assert!(parses(&out), "rewrite must parse: {out}");
        assert!(out.contains(r#""cat"."sch"."t""#), "quoted name preserved: {out}");
        assert!(out.contains(&format!("{CTA_ALIAS}(x, y)")), "aliases applied: {out}");
    }

    #[test]
    fn already_parseable_ctas_is_untouched() {
        let sql = "CREATE TABLE t AS SELECT 1 AS a";
        assert_eq!(rewrite_ctas_compat(sql), sql);
    }

    #[test]
    fn non_ctas_statements_untouched() {
        for sql in [
            "SELECT 1",
            "CREATE TABLE t (a int, b varchar)",
            "INSERT INTO t VALUES (1)",
            "DROP TABLE t",
        ] {
            assert_eq!(rewrite_ctas_compat(sql), sql, "should be untouched: {sql}");
        }
    }

    #[test]
    fn unrelated_parse_error_is_not_masked() {
        // Broken for a reason this rewrite does not handle: returned unchanged.
        let sql = "CREATE TABLE t AS SELECT FROM";
        assert_eq!(rewrite_ctas_compat(sql), sql);
    }

    #[test]
    fn or_replace_and_if_not_exists_modifiers_preserved() {
        let out = rewrite_ctas_compat(
            "CREATE OR REPLACE TABLE t (a, b) AS SELECT 1, 2 WITH DATA",
        );
        assert!(parses(&out), "rewrite must parse: {out}");
        assert!(out.to_ascii_uppercase().contains("OR REPLACE"), "modifier kept: {out}");
        assert!(out.contains(&format!("{CTA_ALIAS}(a, b)")), "aliases applied: {out}");
    }
}
