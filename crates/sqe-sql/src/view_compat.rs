//! Trino / ANSI `CREATE VIEW` header clauses that sqlparser-rs rejects.
//!
//! Trino's grammar is
//!
//! ```text
//! CREATE [OR REPLACE] VIEW name [COMMENT '<text>'] [SECURITY {DEFINER|INVOKER}] AS <query>
//! ```
//!
//! and sqlparser accepts neither optional clause in that spelling. It wants
//! `COMMENT = '<text>'` (the MySQL form) and has no concept of `SECURITY` at
//! all, so a view carrying either dies with a parser error pointing at a column
//! number. `view` is dbt's DEFAULT materialization and dbt-trino emits
//! `security definer` on every one of them, so this blocks every dbt view model
//! rather than an exotic corner of the grammar.
//!
//! Both clauses are normalized into shapes sqlparser already stores, which is
//! what keeps this to a pre-parse rewrite instead of a parser fork:
//!
//! - `COMMENT '<text>'` becomes `COMMENT = '<text>'`, landing in
//!   `CreateView::comment`.
//! - `SECURITY DEFINER` becomes `WITH (sqe_view_security = 'definer')`, landing
//!   in `CreateView::options`.
//!
//! Nothing is invented and nothing is silently dropped: both survive into the
//! AST, and `catalog_ops::create_view` writes them onto the Iceberg view's
//! `properties` map so the intent travels with the view.
//!
//! **`SECURITY DEFINER` is recorded, not honoured.** Trino's DEFINER runs a view
//! with its creator's privileges. SQE has no service account by design, every
//! query runs as the authenticated user via bearer-token passthrough, so there
//! is no credential to run as the definer with. SQE evaluates views as INVOKER,
//! which is STRICTER than what DEFINER asks for: a reader who was meant to be
//! shielded from base-table grants is denied rather than let in. It fails
//! closed, so accepting the clause cannot widen access. The caller warns.
//!
//! Like [`crate::ctas_compat`], every rewrite here is parse-gated: SQL that
//! already parses is returned untouched, and a candidate is adopted only when it
//! makes the SQL parse. A view broken for an unrelated reason keeps its own
//! error rather than a confusing one from this module.

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// The `WITH` option key `SECURITY` is folded into. Chosen to be a legal bare
/// identifier; `catalog_ops` maps it to the `sqe.view-security` view property.
pub const VIEW_SECURITY_OPTION: &str = "sqe_view_security";

/// Trino's view security mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewSecurity {
    /// Run with the view creator's privileges. Recorded, never honoured.
    Definer,
    /// Run with the querying user's privileges. What SQE always does.
    Invoker,
}

impl ViewSecurity {
    /// The lowercase spelling stored in the view property.
    pub fn as_str(self) -> &'static str {
        match self {
            ViewSecurity::Definer => "definer",
            ViewSecurity::Invoker => "invoker",
        }
    }

    fn from_keyword(word: &str) -> Option<Self> {
        if word.eq_ignore_ascii_case("DEFINER") {
            Some(ViewSecurity::Definer)
        } else if word.eq_ignore_ascii_case("INVOKER") {
            Some(ViewSecurity::Invoker)
        } else {
            None
        }
    }
}

/// Result of the rewrite: SQL sqlparser can consume, plus what was folded in.
#[derive(Debug, Clone)]
pub struct ViewCompat {
    /// SQL to hand to the parser. Equal to the input when nothing applied.
    pub sql: String,
    /// The `SECURITY` mode, when the statement carried one.
    pub security: Option<ViewSecurity>,
}

/// Normalize Trino `CREATE VIEW` header clauses. A no-op for SQL that parses.
pub fn rewrite_view_compat(sql: &str) -> ViewCompat {
    let unchanged = || ViewCompat { sql: sql.to_string(), security: None };

    // Fast path: only a CREATE ... VIEW statement can carry these clauses.
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("view") || !lower.contains("create") {
        return unchanged();
    }
    let dialect = GenericDialect {};
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return unchanged();
    }
    match rewrite(sql) {
        Some((candidate, security))
            if candidate != sql && Parser::parse_sql(&dialect, &candidate).is_ok() =>
        {
            ViewCompat { sql: candidate, security }
        }
        _ => unchanged(),
    }
}

fn is_kw(token: &Token, kw: Keyword) -> bool {
    matches!(token, Token::Word(w) if w.keyword == kw)
}

fn word_eq(token: &Token, text: &str) -> bool {
    matches!(token, Token::Word(w) if w.value.eq_ignore_ascii_case(text) && w.quote_style.is_none())
}

fn rewrite(sql: &str) -> Option<(String, Option<ViewSecurity>)> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;

    // Meaningful (non-whitespace) token indices, in order. Edits are expressed
    // against the FULL token vector so every space and newline the user wrote is
    // preserved in the output.
    let meaningful: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    if meaningful.is_empty() || !is_kw(&tokens[meaningful[0]], Keyword::CREATE) {
        return None;
    }

    // Walk to VIEW, past OR REPLACE / TEMPORARY / SECURE / ... . Hitting AS or a
    // paren first means this is not a CREATE VIEW.
    let mut p = 1;
    while p < meaningful.len() && !is_kw(&tokens[meaningful[p]], Keyword::VIEW) {
        if is_kw(&tokens[meaningful[p]], Keyword::AS)
            || matches!(tokens[meaningful[p]], Token::LParen)
        {
            return None;
        }
        p += 1;
    }
    if p >= meaningful.len() {
        return None;
    }
    p += 1; // past VIEW

    // Optional IF NOT EXISTS.
    for kw in [Keyword::IF, Keyword::NOT, Keyword::EXISTS] {
        if p < meaningful.len() && is_kw(&tokens[meaningful[p]], kw) {
            p += 1;
        }
    }

    // The view name is `ident (. ident)*` and nothing more. Consuming any word
    // that merely is not AS would swallow the header clauses themselves, since
    // `SECURITY` and `COMMENT` are words too.
    if p < meaningful.len() && matches!(tokens[meaningful[p]], Token::Word(_)) {
        p += 1;
        while p + 1 < meaningful.len()
            && matches!(tokens[meaningful[p]], Token::Period)
            && matches!(tokens[meaningful[p + 1]], Token::Word(_))
        {
            p += 2;
        }
    }

    // Optional parenthesized column list, skipped as a balanced group.
    if p < meaningful.len() && matches!(tokens[meaningful[p]], Token::LParen) {
        let mut depth = 0usize;
        while p < meaningful.len() {
            match tokens[meaningful[p]] {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        p += 1;
                        break;
                    }
                }
                _ => {}
            }
            p += 1;
        }
    }

    // First token after the name: where an injected WITH list has to go, because
    // sqlparser accepts `WITH (...) COMMENT = '...'` but not the reverse order.
    let header_start = p;

    // Header clauses live between here and the body's AS. `edits` maps a token
    // index to its replacement text; "" deletes.
    let mut edits: std::collections::BTreeMap<usize, String> = std::collections::BTreeMap::new();
    let mut security = None;
    let mut existing_with_lparen: Option<usize> = None;
    let mut as_index: Option<usize> = None;

    while p < meaningful.len() {
        let idx = meaningful[p];
        if is_kw(&tokens[idx], Keyword::AS) {
            as_index = Some(idx);
            break;
        }
        if word_eq(&tokens[idx], "SECURITY") {
            // `SECURITY <mode>`: drop both words and remember the mode.
            let next = meaningful.get(p + 1).copied();
            let mode = next
                .and_then(|n| match &tokens[n] {
                    Token::Word(w) => ViewSecurity::from_keyword(&w.value),
                    _ => None,
                });
            if let (Some(mode), Some(next)) = (mode, next) {
                edits.insert(idx, String::new());
                edits.insert(next, String::new());
                security = Some(mode);
                p += 2;
                continue;
            }
        }
        if is_kw(&tokens[idx], Keyword::COMMENT) {
            // Trino spells it `COMMENT '<text>'`; sqlparser wants an `=`.
            if let Some(next) = meaningful.get(p + 1).copied() {
                if matches!(tokens[next], Token::SingleQuotedString(_)) {
                    edits.insert(idx, "COMMENT =".to_string());
                    p += 2;
                    continue;
                }
            }
        }
        if is_kw(&tokens[idx], Keyword::WITH) {
            // A WITH list already present: fold the security option into it
            // rather than emitting a second, which would not parse.
            if let Some(next) = meaningful.get(p + 1).copied() {
                if matches!(tokens[next], Token::LParen) {
                    existing_with_lparen = Some(next);
                }
            }
        }
        p += 1;
    }

    // A COMMENT-only statement still needs its rewrite, so this must not bail
    // just because there was no SECURITY clause.
    if let Some(mode) = security {
        let opt = format!("{VIEW_SECURITY_OPTION} = '{}'", mode.as_str());
        match existing_with_lparen {
            // Extend the user's list; two WITH clauses in one header do not parse.
            Some(lparen) => {
                edits.insert(lparen, format!("({opt}, "));
            }
            None => {
                // Prefix the first header token, composing with any edit already
                // recorded there (the header may start with COMMENT).
                let anchor = meaningful.get(header_start).copied().or(as_index)?;
                let base = edits
                    .get(&anchor)
                    .cloned()
                    .unwrap_or_else(|| tokens[anchor].to_string());
                edits.insert(anchor, format!("WITH ({opt}) {base}"));
            }
        }
    }
    if edits.is_empty() {
        return None;
    }

    let mut out: Vec<String> = tokens.iter().map(Token::to_string).collect();
    for (idx, text) in edits {
        out[idx] = text;
    }
    Some((out.concat(), security))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Statement;

    fn parse_view(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap_or_else(|e| panic!("must parse after rewrite: {e}\n{sql}"))
            .pop()
            .expect("one statement")
    }

    fn view_parts(sql: &str) -> (Option<String>, String) {
        match parse_view(sql) {
            Statement::CreateView(cv) => (cv.comment.clone(), format!("{:?}", cv.options)),
            other => panic!("expected CreateView, got {other:?}"),
        }
    }

    /// The statement dbt-trino actually emits, captured verbatim from
    /// `dbt run --debug`. Before this module it died with
    /// `Expected: AS, found: security at Line: 3, Column: 5`.
    const DBT_VIEW: &str = r#"create or replace view
      "ws_viewrepro2_1786444430"."dev"."stg_example"
    security definer
    as
      with source as (
          select * from "ws_viewrepro2_1786444430"."dev_raw"."example_table"
      ),
      renamed as (
          select * from source
      )
      select * from renamed"#;

    #[test]
    fn dbt_view_ddl_parses_and_keeps_its_security_mode() {
        let got = rewrite_view_compat(DBT_VIEW);
        assert_eq!(got.security, Some(ViewSecurity::Definer));
        let (_, options) = view_parts(&got.sql);
        assert!(
            options.contains(VIEW_SECURITY_OPTION) && options.contains("definer"),
            "security must survive into the AST options, got: {options}"
        );
    }

    /// The body is a CTE chain whose text contains `as` and `with` repeatedly.
    /// The scan must stop at the header's AS and leave every one of them alone.
    #[test]
    fn rewrite_does_not_disturb_the_view_body() {
        let got = rewrite_view_compat(DBT_VIEW);
        for fragment in [
            "with source as (",
            "renamed as (",
            "select * from renamed",
            "\"dev_raw\".\"example_table\"",
        ] {
            assert!(got.sql.contains(fragment), "body lost {fragment:?}:\n{}", got.sql);
        }
    }

    #[test]
    fn security_invoker_is_recognized_too() {
        let got = rewrite_view_compat("CREATE VIEW v SECURITY INVOKER AS SELECT 1 AS x");
        assert_eq!(got.security, Some(ViewSecurity::Invoker));
        assert!(view_parts(&got.sql).1.contains("invoker"));
    }

    /// Trino spells the view comment without `=`; sqlparser demands one.
    #[test]
    fn trino_comment_form_lands_in_the_comment_field() {
        let got = rewrite_view_compat("CREATE VIEW v COMMENT 'hello' AS SELECT 1 AS x");
        assert_eq!(got.security, None, "no SECURITY clause here");
        assert_eq!(view_parts(&got.sql).0.as_deref(), Some("hello"));
    }

    #[test]
    fn comment_and_security_compose() {
        let got =
            rewrite_view_compat("CREATE VIEW v COMMENT 'c' SECURITY DEFINER AS SELECT 1 AS x");
        assert_eq!(got.security, Some(ViewSecurity::Definer));
        let (comment, options) = view_parts(&got.sql);
        assert_eq!(comment.as_deref(), Some("c"));
        assert!(options.contains("definer"));
    }

    /// A user-written WITH list must be extended, not duplicated: two WITH
    /// clauses in one header do not parse.
    #[test]
    fn security_folds_into_an_existing_with_list() {
        let got = rewrite_view_compat(
            "CREATE VIEW v WITH (format = 'parquet') SECURITY DEFINER AS SELECT 1 AS x",
        );
        assert_eq!(got.security, Some(ViewSecurity::Definer));
        assert_eq!(got.sql.to_ascii_lowercase().matches("with (").count(), 1);
        let options = view_parts(&got.sql).1;
        assert!(options.contains("format"), "user option lost: {options}");
        assert!(options.contains("definer"), "security option lost: {options}");
    }

    /// Parse-gating: SQL that already parses is returned byte-identical, so the
    /// MySQL `COMMENT =` spelling and ordinary views are never touched.
    #[test]
    fn already_parsing_sql_is_untouched() {
        for sql in [
            "CREATE VIEW v AS SELECT 1 AS x",
            "CREATE VIEW v COMMENT = 'c' AS SELECT 1 AS x",
            "SELECT 1",
            "CREATE TABLE t (id INT)",
        ] {
            let got = rewrite_view_compat(sql);
            assert_eq!(got.sql, sql, "rewrote SQL that already parsed: {sql}");
            assert_eq!(got.security, None);
        }
    }

    /// A view broken for an unrelated reason must keep its own parser error
    /// rather than be masked by a rewrite that cannot help it.
    #[test]
    fn unrelated_syntax_error_is_left_alone() {
        let broken = "CREATE VIEW v AS SELECT FROM";
        assert_eq!(rewrite_view_compat(broken).sql, broken);
    }

    /// `SECURITY` without a mode is not Trino's clause. Leave it, so the parser
    /// reports the real problem.
    #[test]
    fn bare_security_word_is_not_treated_as_the_clause() {
        let sql = "CREATE VIEW v SECURITY AS SELECT 1 AS x";
        let got = rewrite_view_compat(sql);
        assert_eq!(got.security, None);
        assert_eq!(got.sql, sql);
    }

    /// A column named `security` in the body must not be mistaken for the
    /// clause: the scan stops at the header's AS.
    #[test]
    fn a_column_called_security_in_the_body_is_safe() {
        let sql = "CREATE VIEW v SECURITY DEFINER AS SELECT security, definer FROM t";
        let got = rewrite_view_compat(sql);
        assert_eq!(got.security, Some(ViewSecurity::Definer));
        assert!(
            got.sql.contains("SELECT security, definer FROM t"),
            "body columns were rewritten: {}",
            got.sql
        );
    }
}
