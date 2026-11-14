//! Pre-parser for `PARTITIONED BY (...)` in `CREATE TABLE`.
//!
//! sqlparser-rs treats `PARTITIONED BY` (Hive/Spark/Trino style) as a
//! list of column definitions: `(col1 STRING, col2 INT)`. Iceberg's
//! transform syntax lives in expressions: `(year(ts), bucket(16, id))`.
//! Native sqlparser rejects the second form on `PARTITIONED BY`.
//!
//! sqlparser DOES accept `PARTITION BY <expr>` (BigQuery / Postgres /
//! Generic dialects) and stores the parsed `Expr` on
//! `CreateTable.partition_by`. We rewrite `PARTITIONED BY` to
//! `PARTITION BY` before handing the SQL to the parser, keeping the
//! Iceberg-style transform syntax intact.
//!
//! Only the keyword pair is substituted; the parenthesised expression
//! list is left untouched. The substitution is whole-token (case
//! insensitive) and respects string literals.
use crate::token_scan::{byte_offset, is_ident_byte};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

/// Rewrite the `PARTITIONED BY` keyword pair to `PARTITION BY` so
/// sqlparser-rs accepts the Iceberg-style transform list. Only matches
/// the keyword pair when it appears as a whole token, outside string
/// literals. Returns the original string unchanged when the clause is
/// absent.
///
/// `CREATE EXTERNAL TABLE` (Hive/Athena/Spark external tables, see
/// `sqe_sql::hive_ddl`) is skipped entirely, even when the clause is
/// present: this module's own doc comment explains why the rewrite exists
/// at all -- sqlparser-rs already accepts Hive-style `PARTITIONED BY (col1
/// STRING, col2 INT)` natively as a column-definition list, which is
/// exactly the form `CREATE EXTERNAL TABLE` uses. Rewriting it to
/// `PARTITION BY` sends it down the Iceberg transform-expression grammar
/// instead, where a typed column def like `dt string` is not a valid
/// expression and the parse fails with a confusing "found: PARTITION"
/// error that has nothing to do with the actual problem. The two grammars
/// are mutually exclusive by construction (a `CREATE EXTERNAL TABLE` is
/// never an Iceberg CTAS), so the statement kind alone decides which one
/// applies.
pub fn normalize_partitioned_by(sql: &str) -> String {
    if !contains_partitioned_by_keyword(sql) || is_create_external_table(sql) {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < bytes.len() {
        let c = bytes[i];

        // Track string literal boundaries so we never rewrite inside them.
        if !in_double_quote && c == b'\'' {
            // sqlparser handles `''` as an escape; we mirror that.
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' && in_single_quote {
                out.push('\'');
                out.push('\'');
                i += 2;
                continue;
            }
            in_single_quote = !in_single_quote;
            out.push('\'');
            i += 1;
            continue;
        }
        if !in_single_quote && c == b'"' {
            in_double_quote = !in_double_quote;
            out.push('"');
            i += 1;
            continue;
        }
        if in_single_quote || in_double_quote {
            out.push(c as char);
            i += 1;
            continue;
        }

        // Outside strings: try to match `PARTITIONED BY` (case insensitive).
        // The match must be bounded by non-identifier characters on both
        // sides so we do not corrupt identifiers like `MY_PARTITIONED_TABLE`.
        if (c == b'P' || c == b'p') && i + 14 <= bytes.len() {
            let before_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            if before_ok
                && case_insensitive_eq(&bytes[i..i + 13], b"PARTITIONED B")
                && bytes[i + 13].eq_ignore_ascii_case(&b'Y')
                && (i + 14 == bytes.len() || !is_ident_byte(bytes[i + 14]))
            {
                // Replace `PARTITIONED BY` with `PARTITION BY`.
                out.push_str("PARTITION BY");
                i += 14;
                continue;
            }
        }

        out.push(c as char);
        i += 1;
    }

    out
}

fn case_insensitive_eq(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

fn contains_partitioned_by_keyword(sql: &str) -> bool {
    // Cheap pre-flight: avoid the full pass when the clause clearly is
    // not present.
    let upper = sql.to_ascii_uppercase();
    upper.contains("PARTITIONED BY")
}

/// Whether `sql`, after skipping leading whitespace and SQL comments,
/// begins with `CREATE EXTERNAL TABLE` (case-insensitive). A textual prefix
/// check rather than a parse attempt, deliberately: this runs before any
/// parser sees the SQL, so there is no AST to inspect yet.
///
/// Shared with `sqe-cli`'s `embedded.rs` (its own dual-parse-path dispatch
/// needs the identical check), so this is the one copy; both call sites
/// must agree on what counts as a `CREATE EXTERNAL TABLE`, including how a
/// leading `-- ...`/`/* ... */` comment is skipped, or one path silently
/// diverges from the other.
pub fn is_create_external_table(sql: &str) -> bool {
    let mut s = sql;
    loop {
        let trimmed = s.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            s = rest.split_once('\n').map(|(_, r)| r).unwrap_or("");
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            s = rest.split_once("*/").map(|(_, r)| r).unwrap_or("");
            continue;
        }
        s = trimmed;
        break;
    }
    let head: String = s
        .chars()
        .take(64)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    head.starts_with("CREATE EXTERNAL TABLE")
}

/// Put the table clauses of a `CREATE TABLE` in the order sqlparser-rs
/// accepts, and drop a `USING iceberg` provider clause.
///
/// Spark and Databricks take `PARTITIONED BY`, `TBLPROPERTIES`, `LOCATION`,
/// `COMMENT` and `USING` in any order. sqlparser's Generic dialect only takes
/// `LOCATION`, `WITH (...)` and `TBLPROPERTIES` before `PARTITION BY`, and
/// `COMMENT` after it, so `PARTITIONED BY (region) TBLPROPERTIES (...)` failed
/// with "found: TBLPROPERTIES". Every table SQE creates is an Iceberg table,
/// so `USING iceberg` says nothing; any other provider is left in place and
/// keeps its parse error.
///
/// sqlparser also keeps `COMMENT`, `WITH (...)` and `TBLPROPERTIES` in one
/// options slot. A comment beside an options list becomes its `comment`
/// property, and a `WITH` list beside `TBLPROPERTIES` joins it, where
/// sqlparser would keep only the last list (#897).
///
/// Runs after [`normalize_partitioned_by`]. Parse-gated: SQL that already
/// parses is returned untouched unless it holds both lists, and the
/// reordered text is only adopted when it parses. A clause list holding
/// anything else (`STORED AS`, `CLUSTER BY`) is left as written.
pub fn reorder_create_table_clauses(sql: &str) -> String {
    if !sql.to_ascii_lowercase().contains("create") || is_create_external_table(sql) {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    let parses = Parser::parse_sql(&dialect, sql).is_ok();
    if parses && !sql.to_ascii_uppercase().contains("TBLPROPERTIES") {
        return sql.to_string();
    }
    match reorder_clauses(sql) {
        Some((candidate, joined))
            if (joined || !parses) && Parser::parse_sql(&dialect, &candidate).is_ok() =>
        {
            candidate
        }
        _ => sql.to_string(),
    }
}

/// A table clause, by the slot sqlparser wants it in.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TableClause {
    Location,
    With,
    TblProperties,
    PartitionBy,
    Comment,
    UsingIceberg,
}

/// The reordered statement, and whether a `WITH` list joined `TBLPROPERTIES`.
fn reorder_clauses(sql: &str) -> Option<(String, bool)> {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let sig: Vec<usize> = (0..tokens.len())
        .filter(|&i| !matches!(tokens[i].token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let word = |p: usize| match sig.get(p).map(|&i| &tokens[i].token) {
        Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
        _ => None,
    };
    let is = |p: usize, t: &Token| sig.get(p).map(|&i| &tokens[i].token) == Some(t);
    let is_string = |p: usize| {
        matches!(
            sig.get(p).map(|&i| &tokens[i].token),
            Some(Token::SingleQuotedString(_))
        )
    };
    // Position just past the parenthesised group opening at `p`.
    let skip_group = |p: usize| {
        if !is(p, &Token::LParen) {
            return None;
        }
        let mut depth = 0usize;
        for q in p..sig.len() {
            match tokens[sig[q]].token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(q + 1);
                    }
                }
                _ => {}
            }
        }
        None
    };

    // CREATE [OR REPLACE] [TEMPORARY] TABLE [IF NOT EXISTS] <name> [(<columns>)]
    if word(0).as_deref() != Some("CREATE") {
        return None;
    }
    let mut p = 1;
    while word(p).as_deref() != Some("TABLE") {
        match word(p).as_deref() {
            Some("OR" | "REPLACE" | "TEMP" | "TEMPORARY") => p += 1,
            _ => return None,
        }
    }
    p += 1;
    if word(p).as_deref() == Some("IF") {
        p += 3;
    }
    // Dotted name: a word, then `.word` repeated.
    let is_name_part =
        |p: usize| matches!(sig.get(p).map(|&i| &tokens[i].token), Some(Token::Word(_)));
    if !is_name_part(p) {
        return None;
    }
    p += 1;
    while is(p, &Token::Period) && is_name_part(p + 1) {
        p += 2;
    }
    if is(p, &Token::LParen) {
        p = skip_group(p)?;
    }

    let mut clauses: Vec<(TableClause, usize, usize)> = Vec::new();
    loop {
        let start = p;
        let kind = match word(p).as_deref() {
            None if p >= sig.len() || is(p, &Token::SemiColon) => break,
            Some("AS") => break,
            Some("LOCATION") if is_string(p + 1) => {
                p += 2;
                TableClause::Location
            }
            Some("WITH") => {
                p = skip_group(p + 1)?;
                TableClause::With
            }
            Some("TBLPROPERTIES") => {
                p = skip_group(p + 1)?;
                TableClause::TblProperties
            }
            Some("PARTITION") if word(p + 1).as_deref() == Some("BY") => {
                p = skip_group(p + 2)?;
                TableClause::PartitionBy
            }
            Some("COMMENT") => {
                p += usize::from(is(p + 1, &Token::Eq)) + 1;
                if !is_string(p) {
                    return None;
                }
                p += 1;
                TableClause::Comment
            }
            Some("USING") if word(p + 1).as_deref() == Some("ICEBERG") => {
                p += 2;
                TableClause::UsingIceberg
            }
            _ => return None,
        };
        clauses.push((kind, start, p));
    }
    if clauses.is_empty() {
        return None;
    }

    // Byte offset of significant token `p`; the end of the input past the last.
    let offset = |p: usize| {
        sig.get(p)
            .map_or(sql.len(), |&i| byte_offset(sql, tokens[i].span.start))
    };
    // Token `p` runs to where the token after it begins.
    let end_of = |p: usize| {
        tokens
            .get(sig[p] + 1)
            .map_or(sql.len(), |t| byte_offset(sql, t.span.start))
    };
    let text = |(_, start, end): (TableClause, usize, usize)| {
        sql[offset(start)..end_of(end - 1)].to_string()
    };
    let mut sorted = clauses.clone();
    sorted.sort_by_key(|c| c.0);
    let find = |kind: TableClause| sorted.iter().position(|c| c.0 == kind);
    let mut texts: Vec<String> = sorted.iter().map(|&c| text(c)).collect();
    // sqlparser keeps `COMMENT`, `WITH (...)` and `TBLPROPERTIES` in one
    // options slot (#897). `WITH (...) TBLPROPERTIES (...)` parses, but the
    // second list replaces the first: `WITH (format_version = 3)` was
    // dropped without a word. Both are table properties, so the WITH items
    // join the TBLPROPERTIES list.
    let joined = match (find(TableClause::With), find(TableClause::TblProperties)) {
        (Some(w), Some(t)) => {
            let (_, start, end) = sorted[w];
            let items = sql[offset(start + 2)..offset(end - 1)].trim();
            texts[t] = append_option(&texts[t], items);
            texts[w].clear();
            true
        }
        _ => false,
    };
    // No order of `COMMENT` and an options list parses; dbt-trino writes
    // `comment '...' WITH (...)`. The comment joins the options as the
    // `comment` property, where Trino's Iceberg connector and `COMMENT ON
    // TABLE` keep a table comment.
    let options = find(TableClause::With)
        .filter(|&w| !texts[w].is_empty())
        .or(find(TableClause::TblProperties));
    if let (Some(o), Some(c)) = (options, find(TableClause::Comment)) {
        let literal = &sql[offset(sorted[c].2 - 1)..end_of(sorted[c].2 - 1)];
        let key = match sorted[o].0 {
            TableClause::With => "comment",
            _ => "'comment'",
        };
        texts[o] = append_option(&texts[o], &format!("{key} = {literal}"));
        texts[c].clear();
    }
    let reordered: Vec<String> = sorted
        .iter()
        .zip(texts)
        .filter(|(c, t)| c.0 != TableClause::UsingIceberg && !t.is_empty())
        .map(|(_, t)| t)
        .collect();
    let head = sql[..offset(clauses[0].1)].trim_end();
    let tail = &sql[offset(p)..];
    let mut out = head.to_string();
    for clause in reordered {
        out.push(' ');
        out.push_str(&clause);
    }
    if !tail.is_empty() {
        out.push(' ');
        out.push_str(tail);
    }
    Some((out, joined))
}

/// `clause` (`WITH (...)` or `TBLPROPERTIES (...)`) with `item` added as its
/// last option.
fn append_option(clause: &str, item: &str) -> String {
    match clause.rfind(')') {
        Some(close) if !item.is_empty() => {
            format!("{}, {item}{}", clause[..close].trim_end(), &clause[close..])
        }
        _ => clause.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_when_clause_absent() {
        let sql = "CREATE TABLE t (id BIGINT)";
        assert_eq!(normalize_partitioned_by(sql), sql);
    }

    #[test]
    fn rewrites_uppercase_keyword() {
        let sql = "CREATE TABLE t (id BIGINT, ts TIMESTAMP) PARTITIONED BY (day(ts))";
        let out = normalize_partitioned_by(sql);
        assert!(out.contains("PARTITION BY (day(ts))"));
        assert!(!out.contains("PARTITIONED"));
    }

    #[test]
    fn rewrites_lowercase_keyword() {
        let sql = "create table t (id bigint, ts timestamp) partitioned by (day(ts))";
        let out = normalize_partitioned_by(sql);
        assert!(out.contains("PARTITION BY (day(ts))"));
        assert!(!out.to_lowercase().contains("partitioned"));
    }

    #[test]
    fn rewrites_mixed_case_keyword() {
        let sql = "CREATE TABLE t (id BIGINT) PartiTioned bY (id)";
        let out = normalize_partitioned_by(sql);
        assert!(out.contains("PARTITION BY (id)"));
    }

    /// A Hive `CREATE EXTERNAL TABLE`'s `PARTITIONED BY (col type)` clause
    /// must survive untouched: rewriting it to `PARTITION BY` sends a typed
    /// column def into the Iceberg transform-expression grammar, which does
    /// not accept it (`dt string` is not a valid expression), breaking a
    /// statement sqlparser-rs already parses correctly on its own.
    #[test]
    fn create_external_table_partitioned_by_is_left_untouched() {
        let sql = "CREATE EXTERNAL TABLE t (id BIGINT) PARTITIONED BY (dt STRING) \
                    LOCATION 's3://bucket/t/'";
        assert_eq!(normalize_partitioned_by(sql), sql);
    }

    /// A leading comment must not defeat the `CREATE EXTERNAL TABLE`
    /// detection, the same way `looks_like_create_external_table` in
    /// `sqe-cli::embedded` skips comments before its own prefix check.
    #[test]
    fn create_external_table_partitioned_by_survives_a_leading_comment() {
        let sql = "-- a comment\nCREATE EXTERNAL TABLE t (id BIGINT) \
                    PARTITIONED BY (dt STRING) LOCATION 's3://bucket/t/'";
        assert_eq!(normalize_partitioned_by(sql), sql);
    }

    #[test]
    fn does_not_corrupt_identifiers_with_partitioned_substring() {
        let sql = "CREATE TABLE my_partitioned_table (id BIGINT)";
        assert_eq!(normalize_partitioned_by(sql), sql);
    }

    #[test]
    fn does_not_rewrite_inside_string_literal() {
        let sql = "CREATE TABLE t (id BIGINT) COMMENT 'a partitioned by table'";
        assert_eq!(normalize_partitioned_by(sql), sql);
    }

    #[test]
    fn handles_multiple_columns() {
        let sql = "CREATE TABLE t (id BIGINT, ts TIMESTAMP, region STRING) \
                   PARTITIONED BY (day(ts), bucket(8, id), region)";
        let out = normalize_partitioned_by(sql);
        assert!(out.contains("PARTITION BY (day(ts), bucket(8, id), region)"));
    }

    fn normalize(sql: &str) -> String {
        reorder_create_table_clauses(&normalize_partitioned_by(sql))
    }

    fn parses(sql: &str) -> bool {
        Parser::parse_sql(&GenericDialect {}, sql).is_ok()
    }

    /// `PARTITIONED BY (...) TBLPROPERTIES (...)` is the Spark order; the
    /// rewritten `PARTITION BY` must follow `TBLPROPERTIES` for sqlparser.
    #[test]
    fn tblproperties_after_partitioned_by_moves_in_front() {
        let sql = "CREATE TABLE t (id BIGINT, region STRING) PARTITIONED BY (region) \
                   TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')";
        assert_eq!(
            normalize(sql),
            "CREATE TABLE t (id BIGINT, region STRING) \
             TBLPROPERTIES ('write.delete.mode' = 'merge-on-read') PARTITION BY (region)"
        );
    }

    #[test]
    fn every_spark_clause_order_parses() {
        let failures: Vec<String> = [
            "CREATE TABLE t (id BIGINT, ts TIMESTAMP) PARTITIONED BY (day(ts), bucket(4, id)) \
             TBLPROPERTIES ('a' = 'b')",
            "CREATE TABLE t (id BIGINT, r STRING) PARTITIONED BY (r) LOCATION 's3://b/t'",
            "CREATE TABLE t (id BIGINT, r STRING) PARTITIONED BY (r) WITH (format = 'PARQUET')",
            "CREATE TABLE t (id BIGINT, r STRING) WITH (format = 'PARQUET') LOCATION 's3://b/t'",
            "CREATE TABLE t (id BIGINT, r STRING) TBLPROPERTIES ('a' = 'b') LOCATION 's3://b/t'",
            "CREATE TABLE t (id BIGINT, r STRING) COMMENT 'c' LOCATION 's3://b/t'",
            "CREATE TABLE t (id BIGINT, r STRING) USING iceberg PARTITIONED BY (r)",
            "CREATE TABLE t (id BIGINT, r STRING) PARTITIONED BY (r) USING ICEBERG",
            "CREATE TABLE IF NOT EXISTS db.t (id BIGINT) USING iceberg",
            "CREATE OR REPLACE TABLE t USING iceberg PARTITIONED BY (r) \
             TBLPROPERTIES ('a' = 'b') AS SELECT 1 AS id, 'x' AS r",
            "CREATE TABLE t (id BIGINT, r STRING) PARTITIONED BY (r) \
             TBLPROPERTIES ('a' = 'b') LOCATION 's3://b/t';",
        ]
        .into_iter()
        .filter_map(|sql| {
            let out = normalize(sql);
            (!parses(&out)).then(|| format!("{sql}\n  -> {out}"))
        })
        .collect();
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    /// Clause text moves whole: quote-doubled strings, the transform list and
    /// the query after `AS` reach the parser unchanged.
    #[test]
    fn moved_clauses_keep_their_text() {
        let sql = "CREATE TABLE t PARTITIONED BY (bucket(4, id)) LOCATION 's3://it''s' \
                   TBLPROPERTIES ('k' = 'a''b') AS SELECT 1 AS id";
        assert_eq!(
            normalize(sql),
            "CREATE TABLE t LOCATION 's3://it''s' TBLPROPERTIES ('k' = 'a''b') \
             PARTITION BY (bucket(4, id)) AS SELECT 1 AS id"
        );
    }

    /// sqlparser keeps `COMMENT`, `WITH (...)` and `TBLPROPERTIES` in one
    /// options slot, so no order of two of them parses: the comment joins
    /// the options as the `comment` property (#897).
    #[test]
    fn a_comment_beside_options_becomes_the_comment_property() {
        let sql = "CREATE TABLE t (id BIGINT) PARTITION BY (id) COMMENT 'it''s' \
                   TBLPROPERTIES ('a' = 'b')";
        assert_eq!(
            reorder_create_table_clauses(sql),
            "CREATE TABLE t (id BIGINT) TBLPROPERTIES ('a' = 'b', 'comment' = 'it''s') \
             PARTITION BY (id)"
        );
        // dbt-trino's table materialization with `persist_docs`.
        let sql = "create table s.t\n  comment 'a mart'\n  WITH (\n    format_version = 2\n  )\n\
                   as (\n  select 1 as id\n);";
        let out = reorder_create_table_clauses(sql);
        assert_eq!(
            out,
            "create table s.t WITH (\n    format_version = 2, comment = 'a mart') as (\n  \
             select 1 as id\n);"
        );
        // The Trino door executes the re-printed statement: it parses again.
        let stmt = Parser::parse_sql(&GenericDialect {}, &out)
            .expect("parse")
            .remove(0);
        Parser::parse_sql(&GenericDialect {}, &stmt.to_string()).expect("re-parse");
    }

    /// `WITH (...) TBLPROPERTIES (...)` parses, but sqlparser keeps only the
    /// last list: the WITH items join TBLPROPERTIES instead (#897).
    #[test]
    fn a_with_list_beside_tblproperties_joins_it() {
        let sql = "CREATE TABLE t WITH (format_version = 3) TBLPROPERTIES ('b' = '2') AS SELECT 1";
        assert_eq!(
            reorder_create_table_clauses(sql),
            "CREATE TABLE t TBLPROPERTIES ('b' = '2', format_version = 3) AS SELECT 1"
        );
        let sql = "CREATE TABLE t (id BIGINT) COMMENT 'c' WITH (a = 1) TBLPROPERTIES ('b' = '2')";
        let out = reorder_create_table_clauses(sql);
        assert_eq!(
            out,
            "CREATE TABLE t (id BIGINT) TBLPROPERTIES ('b' = '2', a = 1, 'comment' = 'c')"
        );
        let sqlparser::ast::Statement::CreateTable(ct) =
            Parser::parse_sql(&GenericDialect {}, &out)
                .expect("parse")
                .remove(0)
        else {
            panic!("not a CREATE TABLE");
        };
        let keys = option_keys(&ct.table_options);
        assert_eq!(keys, ["b", "a", "comment"]);
    }

    fn option_keys(options: &sqlparser::ast::CreateTableOptions) -> Vec<String> {
        use sqlparser::ast::{CreateTableOptions, SqlOption};
        let CreateTableOptions::TableProperties(opts) = options else {
            panic!("not TBLPROPERTIES: {options:?}");
        };
        opts.iter()
            .filter_map(|o| match o {
                SqlOption::KeyValue { key, .. } => Some(key.value.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn sql_that_parses_is_left_alone() {
        let sql = "CREATE TABLE t (id BIGINT) TBLPROPERTIES ('a' = 'b') PARTITION BY (id)";
        assert_eq!(reorder_create_table_clauses(sql), sql);
    }

    /// Only `USING iceberg` is dropped. Another provider keeps its parse error
    /// rather than silently becoming an Iceberg table.
    #[test]
    fn other_providers_are_not_dropped() {
        let sql = "CREATE TABLE t (id BIGINT) USING delta";
        assert_eq!(reorder_create_table_clauses(sql), sql);
    }

    #[test]
    fn unknown_clause_leaves_the_statement_as_written() {
        let sql = "CREATE TABLE t (id BIGINT) PARTITION BY (id) STORED AS PARQUET \
                   TBLPROPERTIES ('a' = 'b')";
        assert_eq!(reorder_create_table_clauses(sql), sql);
    }

    /// The `rewrite_data_files_distributed_parity` IT's DDL, through the whole
    /// pre-parse pipeline and the classifier.
    #[test]
    fn partitioned_mor_table_classifies() {
        let sql = crate::pipeline_types::UserSql::new(
            "CREATE TABLE default.p (id BIGINT, region STRING, v BIGINT) PARTITIONED BY (region) \
             TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')",
        );
        let classifiable = crate::pipeline_types::pre_parse_pipeline(&sql).expect("pre-parse");
        crate::classifier::parse_and_classify_typed(&classifiable)
            .unwrap_or_else(|e| panic!("{}: {e}", classifiable.as_str()));
    }
}
