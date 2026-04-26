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
pub fn normalize_partitioned_by(sql: &str) -> String {
    if !contains_partitioned_by_keyword(sql) {
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
            if before_ok && case_insensitive_eq(&bytes[i..i + 13], b"PARTITIONED B")
                && bytes[i + 13].to_ascii_uppercase() == b'Y'
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
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x.to_ascii_uppercase() == y.to_ascii_uppercase())
}

fn contains_partitioned_by_keyword(sql: &str) -> bool {
    // Cheap pre-flight: avoid the full pass when the clause clearly is
    // not present.
    let upper = sql.to_ascii_uppercase();
    upper.contains("PARTITIONED BY")
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
