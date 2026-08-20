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

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
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
/// parser sees the SQL, so there is no AST to inspect yet. Mirrors
/// `sqe-cli::embedded`'s `looks_like_create_external_table` (comment-skip
/// included, so the two agree on a leading `-- ...`/`/* ... */` the same
/// way; keep them in sync rather than letting one drift narrower than the
/// other).
/// Detect a `CREATE EXTERNAL TABLE` statement by its leading keywords,
/// skipping past any leading comments first. Shared with `sqe-cli`'s
/// `embedded.rs` (its own dual-parse-path dispatch needs the identical
/// check), so this is the one copy; both call sites must agree on what
/// counts as a `CREATE EXTERNAL TABLE` or one of the two paths silently
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
}
