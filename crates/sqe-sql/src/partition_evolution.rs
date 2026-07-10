//! Parser for `ALTER TABLE ... ADD/DROP/REPLACE PARTITION FIELD <transform>`.
//!
//! Iceberg supports evolving the partition spec of a table after creation.
//! The SQL surface mirrors Trino's:
//!
//! ```sql
//! ALTER TABLE ns.events ADD PARTITION FIELD year(ts)
//! ALTER TABLE ns.events ADD PARTITION FIELD bucket(16, user_id)
//! ALTER TABLE ns.events ADD PARTITION FIELD region            -- identity
//! ALTER TABLE ns.events DROP PARTITION FIELD region
//! ALTER TABLE ns.events REPLACE PARTITION FIELD region WITH bucket(8, region)
//! ```
//!
//! sqlparser-rs models `AlterTableOperation::AddPartitions` for Hive-style
//! `PARTITION (col=val)` (concrete partition values). Our `PARTITION FIELD`
//! is a different concept (a transform on a column), and sqlparser does
//! not have an AST node for it. We pre-parse here so the classifier can
//! dispatch through a dedicated handler without ever sending the SQL to
//! sqlparser.

use sqe_core::{Result, SqeError};

/// A parsed `ALTER TABLE ... PARTITION FIELD` statement ready for the
/// coordinator to translate into Iceberg `TableUpdate::AddSpec` /
/// `SetDefaultSpec` calls.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionEvolution {
    /// `ALTER TABLE <table> ADD PARTITION FIELD <transform_sql>`
    AddField {
        table: String,
        /// The raw transform expression (e.g. `year(ts)`, `bucket(16, id)`,
        /// or a bare column name `region` for the identity transform).
        /// The handler passes this through the same `parse_partition_transform`
        /// path the CREATE TABLE handler uses.
        transform_sql: String,
    },
    /// `ALTER TABLE <table> DROP PARTITION FIELD <transform_sql>`
    DropField {
        table: String,
        transform_sql: String,
    },
    /// `ALTER TABLE <table> REPLACE PARTITION FIELD <old_transform> WITH <new_transform>`
    ReplaceField {
        table: String,
        old_transform_sql: String,
        new_transform_sql: String,
    },
}

/// Try to parse one of the partition evolution statements. Returns
/// `Ok(None)` when the input does not look like one of these statements,
/// so the classifier can fall through to the next pre-parser.
pub fn try_parse_partition_evolution(sql: &str) -> Result<Option<PartitionEvolution>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();

    if !upper.starts_with("ALTER TABLE ") {
        return Ok(None);
    }

    // Skip "ALTER TABLE "
    let rest = trimmed["ALTER TABLE ".len()..].trim_start();
    let (table, rest_after_table) = split_identifier(rest)?;
    let rest_after_table = rest_after_table.trim_start();
    let rest_upper = rest_after_table.to_uppercase();

    // ADD PARTITION FIELD <transform>
    if let Some(after_kw) = strip_keyword(&rest_upper, rest_after_table, "ADD PARTITION FIELD") {
        let transform_sql = after_kw.trim().trim_end_matches(';').to_string();
        if transform_sql.is_empty() {
            return Err(SqeError::Execution(
                "ALTER TABLE ADD PARTITION FIELD: missing transform expression".into(),
            ));
        }
        return Ok(Some(PartitionEvolution::AddField {
            table,
            transform_sql,
        }));
    }

    // DROP PARTITION FIELD <transform>
    if let Some(after_kw) = strip_keyword(&rest_upper, rest_after_table, "DROP PARTITION FIELD") {
        let transform_sql = after_kw.trim().trim_end_matches(';').to_string();
        if transform_sql.is_empty() {
            return Err(SqeError::Execution(
                "ALTER TABLE DROP PARTITION FIELD: missing transform expression".into(),
            ));
        }
        return Ok(Some(PartitionEvolution::DropField {
            table,
            transform_sql,
        }));
    }

    // REPLACE PARTITION FIELD <old> WITH <new>
    if let Some(after_kw) = strip_keyword(&rest_upper, rest_after_table, "REPLACE PARTITION FIELD") {
        let after_kw = after_kw.trim();
        // Split on " WITH " case-insensitively.
        let (old, new) = split_with(after_kw)?;
        return Ok(Some(PartitionEvolution::ReplaceField {
            table,
            old_transform_sql: old,
            new_transform_sql: new,
        }));
    }

    Ok(None)
}

/// Split a possibly-qualified identifier off the front of `input` and
/// return `(identifier, remainder)`. Handles `name`, `schema.name`,
/// `cat.schema.name`, and double-quoted identifiers.
fn split_identifier(input: &str) -> Result<(String, &str)> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(SqeError::Execution(
            "ALTER TABLE: missing table identifier".into(),
        ));
    }
    let bytes = trimmed.as_bytes();
    let mut i = 0;
    let mut in_quotes = false;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'"' {
            in_quotes = !in_quotes;
            i += 1;
            continue;
        }
        if !in_quotes && (c.is_ascii_whitespace() || c == b';') {
            break;
        }
        i += 1;
    }
    let ident = trimmed[..i].to_string();
    let rest = &trimmed[i..];
    if ident.trim_matches('"').is_empty() {
        return Err(SqeError::Execution(
            "ALTER TABLE: empty table identifier".into(),
        ));
    }
    Ok((ident, rest))
}

/// If `upper_rest` starts with `kw` followed by whitespace, return the
/// slice of `original_rest` that comes after the keyword. Whitespace
/// between words inside `kw` is matched flexibly (each space character
/// in `kw` matches one or more whitespace characters in input).
fn strip_keyword<'a>(upper_rest: &str, original_rest: &'a str, kw: &str) -> Option<&'a str> {
    let kw_words: Vec<&str> = kw.split_whitespace().collect();
    let mut idx = 0usize;
    let upper_bytes = upper_rest.as_bytes();
    for (i, word) in kw_words.iter().enumerate() {
        // Skip leading whitespace.
        while idx < upper_bytes.len() && upper_bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        let end = idx + word.len();
        if end > upper_bytes.len() {
            return None;
        }
        if &upper_bytes[idx..end] != word.as_bytes() {
            return None;
        }
        idx = end;
        // After the last word, require a word-boundary (whitespace or end).
        if i == kw_words.len() - 1 {
            if idx < upper_bytes.len() && !upper_bytes[idx].is_ascii_whitespace() {
                return None;
            }
        } else {
            // Between words, must have at least one whitespace character.
            if idx >= upper_bytes.len() || !upper_bytes[idx].is_ascii_whitespace() {
                return None;
            }
        }
    }
    Some(&original_rest[idx..])
}

/// Split an input string on the first standalone ` WITH ` (case insensitive).
/// Used for REPLACE PARTITION FIELD <old> WITH <new>.
fn split_with(input: &str) -> Result<(String, String)> {
    let upper = input.to_uppercase();
    // Find " WITH " as a whole token.
    let mut search_start = 0;
    while let Some(pos) = upper[search_start..].find("WITH") {
        let abs = search_start + pos;
        let before_ok = abs == 0 || upper.as_bytes()[abs - 1].is_ascii_whitespace();
        let after_idx = abs + 4;
        let after_ok = after_idx < upper.len()
            && upper.as_bytes()[after_idx].is_ascii_whitespace();
        if before_ok && after_ok {
            // Found a real " WITH " keyword.
            let old = input[..abs].trim().to_string();
            let new = input[after_idx..].trim().trim_end_matches(';').to_string();
            if old.is_empty() || new.is_empty() {
                return Err(SqeError::Execution(
                    "ALTER TABLE REPLACE PARTITION FIELD: missing old or new transform".into(),
                ));
            }
            return Ok((old, new));
        }
        search_start = abs + 4;
    }
    Err(SqeError::Execution(
        "ALTER TABLE REPLACE PARTITION FIELD: expected `WITH <new_transform>`".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_match_for_unrelated_alter() {
        assert!(try_parse_partition_evolution("ALTER TABLE t ADD COLUMN x INT")
            .unwrap()
            .is_none());
    }

    #[test]
    fn add_field_with_function_transform() {
        let pe = try_parse_partition_evolution(
            "ALTER TABLE default.events ADD PARTITION FIELD year(ts)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            pe,
            PartitionEvolution::AddField {
                table: "default.events".into(),
                transform_sql: "year(ts)".into(),
            }
        );
    }

    #[test]
    fn add_field_with_bucket() {
        let pe = try_parse_partition_evolution(
            "alter table events add partition field bucket(16, user_id)",
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            pe,
            PartitionEvolution::AddField { transform_sql, .. } if transform_sql == "bucket(16, user_id)"
        ));
    }

    #[test]
    fn add_field_with_identity() {
        let pe = try_parse_partition_evolution(
            "ALTER TABLE events ADD PARTITION FIELD region",
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            pe,
            PartitionEvolution::AddField { transform_sql, .. } if transform_sql == "region"
        ));
    }

    #[test]
    fn drop_field() {
        let pe = try_parse_partition_evolution(
            "ALTER TABLE default.events DROP PARTITION FIELD region",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            pe,
            PartitionEvolution::DropField {
                table: "default.events".into(),
                transform_sql: "region".into(),
            }
        );
    }

    #[test]
    fn replace_field_with_keyword() {
        let pe = try_parse_partition_evolution(
            "ALTER TABLE default.events REPLACE PARTITION FIELD region WITH bucket(16, region)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            pe,
            PartitionEvolution::ReplaceField {
                table: "default.events".into(),
                old_transform_sql: "region".into(),
                new_transform_sql: "bucket(16, region)".into(),
            }
        );
    }

    #[test]
    fn missing_transform_returns_error() {
        let err = try_parse_partition_evolution("ALTER TABLE t ADD PARTITION FIELD")
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing transform"));
    }

    #[test]
    fn replace_without_with_returns_error() {
        let err = try_parse_partition_evolution(
            "ALTER TABLE t REPLACE PARTITION FIELD region",
        )
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WITH"));
    }
}
