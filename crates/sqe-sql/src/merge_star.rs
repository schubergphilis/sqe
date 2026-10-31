//! `MERGE ... UPDATE SET *` and `INSERT *` (Spark and Databricks).
//!
//! `WHEN MATCHED THEN UPDATE SET *` sets every target column from the source
//! column of the same name; `WHEN NOT MATCHED THEN INSERT *` inserts every
//! target column from the source column of the same name. sqlparser 0.62
//! rejects both. The target's columns are only known once the table is
//! loaded, so the parser cannot expand the star itself. Instead the star is
//! replaced by a marker that parses:
//!
//! - `UPDATE SET *` becomes `UPDATE SET __sqe_merge_star__ = NULL`
//! - `INSERT *` becomes `INSERT (__sqe_merge_star__) VALUES (NULL)`
//!
//! and the MERGE handler expands a clause that consists of the marker alone
//! (`sqe_write::merge_sql`), against the target schema it loaded.

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// The column name that stands for `*` in a rewritten MERGE clause.
pub const MERGE_STAR_MARKER: &str = "__sqe_merge_star__";

/// Rewrite `THEN UPDATE SET *` and `THEN INSERT *` in a MERGE statement to
/// the marker forms. Parse-gated like the other source-level fallbacks: SQL
/// that already parses, or that still fails after the rewrite, is returned
/// unchanged.
pub fn rewrite_merge_star(sql: &str) -> String {
    if !sql.to_ascii_lowercase().contains("merge") || !sql.contains('*') {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    if Parser::parse_sql(&dialect, sql).is_ok() {
        return sql.to_string();
    }
    match replace_stars(sql) {
        Some(candidate) if Parser::parse_sql(&dialect, &candidate).is_ok() => candidate,
        _ => sql.to_string(),
    }
}

fn replace_stars(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let meaningful: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    let keyword = |m: usize| match meaningful.get(m).map(|&i| &tokens[i]) {
        Some(Token::Word(w)) => Some(w.keyword),
        _ => None,
    };
    let is_star = |m: usize| matches!(meaningful.get(m).map(|&i| &tokens[i]), Some(Token::Mul));

    if keyword(0) != Some(Keyword::MERGE) {
        return None;
    }

    // Token index of a `*` and the text that replaces it.
    let mut replace: Vec<(usize, String)> = Vec::new();
    for m in 0..meaningful.len() {
        if keyword(m) != Some(Keyword::THEN) {
            continue;
        }
        match keyword(m + 1) {
            Some(Keyword::UPDATE) if keyword(m + 2) == Some(Keyword::SET) && is_star(m + 3) => {
                replace.push((meaningful[m + 3], format!("{MERGE_STAR_MARKER} = NULL")));
            }
            Some(Keyword::INSERT) if is_star(m + 2) => {
                replace.push((
                    meaningful[m + 2],
                    format!("({MERGE_STAR_MARKER}) VALUES (NULL)"),
                ));
            }
            _ => {}
        }
    }
    if replace.is_empty() {
        return None;
    }

    let mut out = String::with_capacity(sql.len() + 64 * replace.len());
    for (i, t) in tokens.iter().enumerate() {
        match replace.iter().find(|(at, _)| *at == i) {
            Some((_, text)) => out.push_str(text),
            None => out.push_str(&t.to_string()),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_set_star_and_insert_star_become_markers() {
        let sql = "MERGE INTO t USING s ON t.id = s.id \
                   WHEN MATCHED THEN UPDATE SET * \
                   WHEN NOT MATCHED THEN INSERT *";
        let out = rewrite_merge_star(sql);
        assert_eq!(
            out,
            "MERGE INTO t USING s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET __sqe_merge_star__ = NULL \
             WHEN NOT MATCHED THEN INSERT (__sqe_merge_star__) VALUES (NULL)"
        );
    }

    #[test]
    fn lowercase_and_predicated_clauses_are_rewritten() {
        let sql = "merge into t using s on t.id = s.id \
                   when matched and s.v > 1 then update set * \
                   when not matched and s.v * 2 > 0 then insert *";
        let out = rewrite_merge_star(sql);
        assert!(
            out.contains("update set __sqe_merge_star__ = NULL"),
            "{out}"
        );
        assert!(
            out.contains("insert (__sqe_merge_star__) VALUES (NULL)"),
            "{out}"
        );
        // The multiplication in the predicate is left alone.
        assert!(out.contains("s.v * 2 > 0"), "{out}");
    }

    #[test]
    fn parsing_sql_and_non_merge_sql_are_untouched() {
        let ok = "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v";
        assert_eq!(rewrite_merge_star(ok), ok);
        let select = "SELECT * FROM merge_log";
        assert_eq!(rewrite_merge_star(select), select);
        let broken = "MERGE INTO t USING s ON WHEN MATCHED THEN UPDATE SET *";
        assert_eq!(rewrite_merge_star(broken), broken);
    }

    #[test]
    fn the_front_door_accepts_the_star_forms() {
        let door = crate::SqlFrontDoor::new(crate::WireDialect::Flight);
        let out = door
            .normalize(
                "MERGE INTO t USING s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
            )
            .expect("normalize");
        assert!(matches!(
            out.statement(),
            Some(sqlparser::ast::Statement::Merge(_))
        ));
        assert!(out.sql().contains(MERGE_STAR_MARKER), "{}", out.sql());
    }
}
