//! Parser for `ALTER TABLE ... SET | DROP IDENTIFIER FIELDS <cols>`.
//!
//! Iceberg identifier fields are the row key that merge-on-read UPDATE,
//! MERGE and DELETE write equality deletes against. The SQL surface is
//! Spark's, with or without parentheses:
//!
//! ```sql
//! ALTER TABLE ns.t SET IDENTIFIER FIELDS id
//! ALTER TABLE ns.t SET IDENTIFIER FIELDS (id, region)
//! ALTER TABLE ns.t DROP IDENTIFIER FIELDS region
//! ```
//!
//! SET replaces the whole set; DROP removes the listed columns from it.
//! sqlparser-rs has no AST for either, so they are pre-parsed here.

use sqe_core::{Result, SqeError, SqeErrorCode};

use crate::partition_evolution::{split_identifier, strip_keyword};

/// A parsed `ALTER TABLE ... SET | DROP IDENTIFIER FIELDS` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct IdentifierFieldsDdl {
    /// The table as written, possibly qualified and quoted.
    pub table: String,
    /// `true` for SET (replace the set), `false` for DROP (remove from it).
    pub set: bool,
    /// Full Iceberg field names (`s.x` for a struct member). An unquoted
    /// part is ASCII-lowercased, a quoted part keeps its case.
    pub columns: Vec<String>,
}

/// Try to parse a SET / DROP IDENTIFIER FIELDS statement. `Ok(None)` when
/// the input is some other statement.
pub fn try_parse_identifier_fields(sql: &str) -> Result<Option<IdentifierFieldsDdl>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("ALTER TABLE ") {
        return Ok(None);
    }
    let rest = trimmed["ALTER TABLE ".len()..].trim_start();
    let (table, rest) = split_identifier(rest)?;
    let rest = rest.trim_start();
    let rest_upper = rest.to_ascii_uppercase();

    let (set, cols) = if let Some(after) = strip_keyword(&rest_upper, rest, "SET IDENTIFIER FIELDS")
    {
        (true, after)
    } else if let Some(after) = strip_keyword(&rest_upper, rest, "DROP IDENTIFIER FIELDS") {
        (false, after)
    } else {
        return Ok(None);
    };
    let verb = if set { "SET" } else { "DROP" };

    let mut cols = cols.trim();
    if let Some(inner) = cols.strip_prefix('(') {
        cols = inner.strip_suffix(')').ok_or_else(|| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("ALTER TABLE {verb} IDENTIFIER FIELDS: unbalanced parentheses"),
            )
        })?;
    }
    let columns = split_columns(cols)?
        .into_iter()
        .map(|c| fold_column(&c))
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("ALTER TABLE {verb} IDENTIFIER FIELDS: missing column list"),
        ));
    }
    Ok(Some(IdentifierFieldsDdl {
        table,
        set,
        columns,
    }))
}

/// Split on commas outside double quotes; empty items are an error.
fn split_columns(input: &str) -> Result<Vec<String>> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in input.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                cur.push(c);
            }
            ',' if !in_quotes => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    let out: Vec<String> = out.into_iter().map(|s| s.trim().to_string()).collect();
    if in_quotes || out.iter().any(|s| s.is_empty()) {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            "IDENTIFIER FIELDS: malformed column list",
        ));
    }
    Ok(out)
}

/// `S."Mixed".x` -> `s.Mixed.x`: dot-separated parts, unquoted ones folded.
fn fold_column(col: &str) -> Result<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut in_quotes = false;
    for c in col.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                quoted = true;
            }
            '.' if !in_quotes => {
                parts.push(fold_part(std::mem::take(&mut cur), quoted));
                quoted = false;
            }
            c if c.is_whitespace() && !in_quotes => {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("IDENTIFIER FIELDS: '{col}' is not a column name"),
                ));
            }
            _ => cur.push(c),
        }
    }
    parts.push(fold_part(cur, quoted));
    if parts.iter().any(String::is_empty) {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("IDENTIFIER FIELDS: '{col}' is not a column name"),
        ));
    }
    Ok(parts.join("."))
}

fn fold_part(part: String, quoted: bool) -> String {
    if quoted {
        part
    } else {
        part.to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> IdentifierFieldsDdl {
        try_parse_identifier_fields(sql)
            .expect("parses")
            .expect("matches")
    }

    #[test]
    fn set_single_column_without_parentheses() {
        let ddl = parse("ALTER TABLE ns.t SET IDENTIFIER FIELDS id");
        assert_eq!(
            ddl,
            IdentifierFieldsDdl {
                table: "ns.t".into(),
                set: true,
                columns: vec!["id".into()],
            }
        );
    }

    #[test]
    fn set_list_in_parentheses_folds_unquoted_names() {
        let ddl = parse("alter table ns.t set identifier fields (ID, \"Region\", s.X);");
        assert!(ddl.set);
        assert_eq!(ddl.columns, vec!["id", "Region", "s.x"]);
    }

    #[test]
    fn drop_list() {
        let ddl = parse("ALTER TABLE \"Ns\".t DROP IDENTIFIER FIELDS a, b");
        assert_eq!(ddl.table, "\"Ns\".t");
        assert!(!ddl.set);
        assert_eq!(ddl.columns, vec!["a", "b"]);
    }

    #[test]
    fn quoted_name_may_hold_a_comma_or_dot() {
        let ddl = parse("ALTER TABLE t SET IDENTIFIER FIELDS \"a,b\", \"c.d\"");
        assert_eq!(ddl.columns, vec!["a,b", "c.d"]);
    }

    #[test]
    fn other_alter_forms_do_not_match() {
        for sql in [
            "ALTER TABLE t ADD COLUMN c INT",
            "ALTER TABLE t SET TBLPROPERTIES ('a' = 'b')",
            "ALTER TABLE t DROP PARTITION FIELD region",
            "ALTER TABLE t SET PROPERTIES a = 'b'",
            "SELECT 1",
        ] {
            assert_eq!(try_parse_identifier_fields(sql).unwrap(), None, "{sql}");
        }
    }

    #[test]
    fn malformed_lists_are_errors() {
        for sql in [
            "ALTER TABLE t SET IDENTIFIER FIELDS",
            "ALTER TABLE t SET IDENTIFIER FIELDS ()",
            "ALTER TABLE t SET IDENTIFIER FIELDS (a, b",
            "ALTER TABLE t SET IDENTIFIER FIELDS a,,b",
            "ALTER TABLE t DROP IDENTIFIER FIELDS a b",
            "ALTER TABLE t SET IDENTIFIER FIELDS \"a",
        ] {
            assert!(try_parse_identifier_fields(sql).is_err(), "{sql}");
        }
    }
}
