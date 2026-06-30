//! Trino `ALTER TABLE ... EXECUTE <procedure>` compatibility (#331).
//!
//! Trino exposes table maintenance through `ALTER TABLE t EXECUTE optimize`
//! (and `expire_snapshots`, `remove_orphan_files`, ...). sqlparser-rs has no
//! grammar for the `EXECUTE` form and rejects it outright, so this pre-parse
//! rewrite translates the one operation SQE can map faithfully:
//!
//! ```text
//! ALTER TABLE ns.t EXECUTE optimize[(file_size_threshold => '..')]
//!   ->  CALL system.rewrite_data_files(table => 'ns.t')
//! ```
//!
//! `optimize` maps to the existing `rewrite_data_files` maintenance procedure.
//! Trino's only `optimize` argument, `file_size_threshold` (compact files
//! *below* this size), has no faithful counterpart in `rewrite_data_files`
//! (whose `target_file_size_bytes` is the desired *output* size, a different
//! knob). Rather than mis-map it, the argument is dropped: compaction is
//! non-destructive, so compacting a superset of files still yields a correct,
//! optimized table.
//!
//! The destructive maintenance procedures (`expire_snapshots`,
//! `remove_orphan_files`) are deliberately NOT rewritten here: their retention
//! arguments carry data-deletion semantics, and silently dropping them could
//! expire more than the caller asked for. Use `CALL system.expire_snapshots(..)`
//! directly for those.

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// Rewrite a Trino `ALTER TABLE ... EXECUTE optimize` statement into the
/// equivalent `CALL system.rewrite_data_files(...)`. Parse-gated: only touches
/// SQL that does not already parse, and only adopts the rewrite if the result
/// parses. Any other statement is returned unchanged.
pub fn rewrite_alter_execute(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    // Fast path: must look like `ALTER TABLE ... EXECUTE`.
    if !lower.contains("alter") || !lower.contains("execute") {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return sql.to_string();
    }
    match rewrite(sql) {
        Some(candidate)
            if candidate != sql && Parser::parse_sql(&dialect, &candidate).is_ok() =>
        {
            candidate
        }
        _ => sql.to_string(),
    }
}

fn is_kw(token: &Token, kw: Keyword) -> bool {
    matches!(token, Token::Word(w) if w.keyword == kw)
}

/// Case-insensitive match on a word's text (regardless of keyword class), so
/// non-reserved words like `EXECUTE` / `optimize` are recognized.
fn word_eq(token: &Token, name: &str) -> bool {
    matches!(token, Token::Word(w) if w.value.eq_ignore_ascii_case(name))
}

fn render(tokens: &[Token]) -> String {
    tokens.iter().map(Token::to_string).collect()
}

fn rewrite(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let meaningful: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    if meaningful.len() < 4 {
        return None;
    }

    // ALTER TABLE ...
    if !is_kw(&tokens[meaningful[0]], Keyword::ALTER) || !is_kw(&tokens[meaningful[1]], Keyword::TABLE)
    {
        return None;
    }
    let mut p = 2;

    // Optional IF EXISTS.
    if p < meaningful.len() && is_kw(&tokens[meaningful[p]], Keyword::IF) {
        p += 1;
        if p < meaningful.len() && is_kw(&tokens[meaningful[p]], Keyword::EXISTS) {
            p += 1;
        }
    }

    // Table name: a run of identifier words and periods, up to `EXECUTE`.
    let name_start = p;
    while p < meaningful.len() && !word_eq(&tokens[meaningful[p]], "EXECUTE") {
        match &tokens[meaningful[p]] {
            Token::Word(_) | Token::Period => p += 1,
            _ => return None,
        }
    }
    if p >= meaningful.len() || p == name_start {
        return None; // no name, or no EXECUTE
    }
    let name = render(&tokens[meaningful[name_start]..=meaningful[p - 1]]);
    let name = name.trim();
    if name.is_empty() {
        return None;
    }

    // `EXECUTE <proc>`
    p += 1; // past EXECUTE
    if p >= meaningful.len() {
        return None;
    }
    let proc = match &tokens[meaningful[p]] {
        Token::Word(w) => w.value.to_ascii_lowercase(),
        _ => return None,
    };
    // Only `optimize` is mapped (non-destructive compaction). Others are left
    // for the parser to reject so destructive ops are never silently rewritten.
    if proc != "optimize" {
        return None;
    }
    p += 1;

    // An optional `(...)` argument group follows. It is dropped (see module
    // docs); verify it is a balanced paren group at the statement tail.
    if p < meaningful.len() && matches!(tokens[meaningful[p]], Token::LParen) {
        let mut depth = 0i32;
        let mut closed = false;
        while p < meaningful.len() {
            match tokens[meaningful[p]] {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        closed = true;
                        p += 1;
                        break;
                    }
                }
                _ => {}
            }
            p += 1;
        }
        if !closed {
            return None;
        }
    }

    // Only a trailing `;` may remain.
    if p < meaningful.len() && !matches!(tokens[meaningful[p]], Token::SemiColon) {
        return None;
    }

    let trailing = if matches!(tokens[meaningful[meaningful.len() - 1]], Token::SemiColon) {
        ";"
    } else {
        ""
    };
    Some(format!(
        "CALL system.rewrite_data_files(table => '{name}'){trailing}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parses(sql: &str) -> bool {
        Parser::parse_sql(&GenericDialect {}, sql).is_ok()
    }

    #[test]
    fn optimize_bare_becomes_rewrite_data_files() {
        let out = rewrite_alter_execute("ALTER TABLE iceberg.default.t EXECUTE optimize");
        assert_eq!(out, "CALL system.rewrite_data_files(table => 'iceberg.default.t')");
        assert!(parses(&out));
    }

    #[test]
    fn optimize_with_threshold_drops_arg() {
        // file_size_threshold has no faithful map; it is dropped (the table is
        // still compacted, just over a superset of files).
        let out = rewrite_alter_execute(
            "ALTER TABLE ns.t EXECUTE optimize(file_size_threshold => '10MB')",
        );
        assert_eq!(out, "CALL system.rewrite_data_files(table => 'ns.t')");
        assert!(parses(&out));
    }

    #[test]
    fn optimize_two_part_name() {
        let out = rewrite_alter_execute("ALTER TABLE analytics.events EXECUTE optimize;");
        assert_eq!(out, "CALL system.rewrite_data_files(table => 'analytics.events');");
        assert!(parses(&out));
    }

    #[test]
    fn destructive_procedures_left_alone() {
        // expire_snapshots / remove_orphan_files carry deletion semantics; never
        // silently rewrite them (dropping their retention args is unsafe).
        for sql in [
            "ALTER TABLE ns.t EXECUTE expire_snapshots(retention_threshold => '7d')",
            "ALTER TABLE ns.t EXECUTE remove_orphan_files(retention_threshold => '7d')",
        ] {
            assert_eq!(rewrite_alter_execute(sql), sql, "left unchanged: {sql}");
        }
    }

    #[test]
    fn optimize_classifies_as_rewrite_data_files_procedure_end_to_end() {
        // End-to-end: the full pre-parse pipeline rewrites the EXECUTE form, and
        // the result classifies as the rewrite_data_files maintenance procedure
        // the coordinator already dispatches.
        use crate::classifier::{parse_and_classify_typed, StatementKind};
        use crate::procedures::ProcedureCall;
        use crate::{pre_parse_pipeline, UserSql};

        let sql = UserSql::from("ALTER TABLE iceberg.default.t EXECUTE optimize");
        let classifiable = pre_parse_pipeline(&sql).expect("pre-parse ok");
        match parse_and_classify_typed(&classifiable).expect("classifies") {
            StatementKind::Procedure(p) => {
                assert!(
                    matches!(*p, ProcedureCall::RewriteDataFiles { .. }),
                    "optimize maps to rewrite_data_files, got {p:?}"
                );
            }
            other => panic!("expected Procedure, got {other:?}"),
        }
    }

    #[test]
    fn non_alter_execute_untouched() {
        let sql = "ALTER TABLE ns.t ADD COLUMN x int";
        assert_eq!(rewrite_alter_execute(sql), sql);
    }

    #[test]
    fn already_parseable_untouched() {
        // A plain SELECT mentioning the words must not be rewritten.
        let sql = "SELECT 'execute alter' AS x";
        assert_eq!(rewrite_alter_execute(sql), sql);
    }
}
