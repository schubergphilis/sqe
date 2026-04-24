//! Pre-parser for `FOR VERSION AS OF` and `FOR INCREMENTAL BETWEEN` clauses.
//!
//! sqlparser-rs 0.53 only models `FOR SYSTEM_TIME AS OF <expr>`. Trino/Iceberg
//! also accept `FOR VERSION AS OF <snapshot_id_or_ref>` and the Iceberg CDC
//! extension `FOR INCREMENTAL BETWEEN SNAPSHOT <x> AND SNAPSHOT <y>`.
//!
//! We pre-scan the SQL text, extract table name and clause arguments, then
//! strip the clause so sqlparser can parse the query. The coordinator
//! resolves snapshot ids against table metadata:
//!
//! 1. If the value is an integer literal, treat it as a snapshot id.
//! 2. If the value is a string literal, look it up in the table's named refs.
//! 3. If both a branch and tag exist with the same name, prefer the tag
//!    (it's immutable) and log a warning.

use sqe_core::{Result, SqeError};

/// A version reference extracted from `FOR VERSION AS OF <x>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionRef {
    /// Numeric snapshot id, e.g. `FOR VERSION AS OF 12345`.
    SnapshotId(i64),
    /// String ref name, e.g. `FOR VERSION AS OF 'feature_x'`.
    Named(String),
}

/// The parsed time-travel specification for one table reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeTravelSpec {
    /// Table name as it appears in SQL (may be qualified, e.g. `ns.t`).
    pub table: String,
    /// The extracted version reference.
    pub version: VersionRef,
}

/// The parsed incremental-range specification for one table reference.
///
/// Returned by [`extract_incremental_spec`] when the user supplies
/// `FOR INCREMENTAL BETWEEN SNAPSHOT <start> AND SNAPSHOT <end>`.
///
/// The coordinator resolves `start` and `end` against table metadata, then
/// builds a scan over only the data files added in the open-closed interval
/// `(start, end]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalSpec {
    /// Table name as it appears in SQL (may be qualified, e.g. `ns.t`).
    pub table: String,
    /// Starting snapshot id, exclusive.
    pub start: i64,
    /// Ending snapshot id, inclusive.
    pub end: i64,
}

/// Extract all `FOR VERSION AS OF` clauses from the SQL text.
///
/// Returns the rewritten SQL with those clauses removed, plus a list of
/// `(table, version)` pairs the caller must resolve against table metadata.
///
/// `FOR SYSTEM_TIME AS OF` clauses are left alone; the existing DataFusion
/// path handles them.
pub fn extract_time_travel_spec(sql: &str) -> Result<(String, Vec<TimeTravelSpec>)> {
    let upper = sql.to_uppercase();
    let needle = " FOR VERSION AS OF ";
    if !upper.contains(needle.trim()) {
        return Ok((sql.to_string(), vec![]));
    }

    let mut out = String::with_capacity(sql.len());
    let mut specs = Vec::new();
    let bytes = sql.as_bytes();
    let upper_bytes = upper.as_bytes();
    let mut cursor = 0;

    while cursor < bytes.len() {
        // Find next occurrence of " FOR VERSION AS OF " (case-insensitive).
        let window_start = cursor;
        let remaining = &upper_bytes[cursor..];
        let needle_upper = needle.as_bytes();
        let hit = find_needle(remaining, needle_upper);
        match hit {
            None => {
                // No more time-travel clauses; copy the tail.
                out.push_str(&sql[window_start..]);
                break;
            }
            Some(offset) => {
                let clause_start = cursor + offset;
                // We need to find the start of the preceding table token.
                // Walk backward over whitespace and one identifier (possibly dotted
                // and/or quoted).
                let table_end = sql[..clause_start].trim_end().len();
                let table_start = find_table_start(&sql[..table_end]);
                let table = sql[table_start..table_end].trim().to_string();

                // Copy everything from window_start up to the table start and
                // from table_start..clause_start (the table name itself).
                out.push_str(&sql[window_start..clause_start]);

                // Move cursor past the clause header.
                let after_kw = clause_start + needle.len();
                let (version, consumed) = parse_version_token(&sql[after_kw..])?;
                cursor = after_kw + consumed;

                specs.push(TimeTravelSpec {
                    table,
                    version,
                });
            }
        }
    }

    Ok((out, specs))
}

fn find_needle(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if haystack.len() < needle.len() {
        return None;
    }
    (0..=haystack.len() - needle.len())
        .find(|&i| &haystack[i..i + needle.len()] == needle)
}

/// Walk back over a table reference. Supports:
/// - `foo`
/// - `schema.table`
/// - `cat.schema.table`
/// - `"quoted name"`
///
/// Returns the byte index where the table reference starts.
fn find_table_start(prefix: &str) -> usize {
    let bytes = prefix.as_bytes();
    let mut idx = bytes.len();
    // Walk through at most three dotted segments.
    for segment in 0..3 {
        while idx > 0 {
            let c = bytes[idx - 1];
            if c == b'"' {
                // Consume a quoted identifier.
                let mut j = idx - 1;
                while j > 0 && bytes[j - 1] != b'"' {
                    j -= 1;
                }
                if j > 0 {
                    idx = j - 1;
                }
                break;
            } else if c.is_ascii_alphanumeric() || c == b'_' {
                idx -= 1;
            } else {
                break;
            }
        }
        if segment < 2 && idx > 0 && bytes[idx - 1] == b'.' {
            idx -= 1;
            continue;
        }
        break;
    }
    idx
}

/// Parse the version argument after `FOR VERSION AS OF `. Accepts:
/// - An unquoted integer: `12345`
/// - A single-quoted string: `'feature_x'`
/// - A double-quoted string: `"feature_x"`
///
/// Returns the version and the number of bytes consumed from `input`.
fn parse_version_token(input: &str) -> Result<(VersionRef, usize)> {
    let trimmed = input.trim_start();
    let leading_ws = input.len() - trimmed.len();

    if trimmed.is_empty() {
        return Err(SqeError::Execution(
            "FOR VERSION AS OF requires a snapshot id or ref name".to_string(),
        ));
    }

    let first = trimmed.as_bytes()[0];
    if first == b'\'' || first == b'"' {
        let quote = first;
        let bytes = trimmed.as_bytes();
        let mut end = 1;
        while end < bytes.len() && bytes[end] != quote {
            end += 1;
        }
        if end >= bytes.len() {
            return Err(SqeError::Execution(
                "FOR VERSION AS OF: unterminated string literal".to_string(),
            ));
        }
        let name = trimmed[1..end].to_string();
        Ok((VersionRef::Named(name), leading_ws + end + 1))
    } else if first.is_ascii_digit() || first == b'-' {
        let end = trimmed
            .find(|c: char| !c.is_ascii_digit() && c != '-')
            .unwrap_or(trimmed.len());
        let num_str = &trimmed[..end];
        let id: i64 = num_str.parse().map_err(|_| {
            SqeError::Execution(format!(
                "FOR VERSION AS OF: '{num_str}' is not a valid snapshot id"
            ))
        })?;
        Ok((VersionRef::SnapshotId(id), leading_ws + end))
    } else {
        Err(SqeError::Execution(format!(
            "FOR VERSION AS OF: expected integer or quoted ref name, got '{}'",
            trimmed.split_whitespace().next().unwrap_or("")
        )))
    }
}

/// Extract all `FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y` clauses.
///
/// Returns the rewritten SQL with those clauses stripped plus a vector of
/// resolved specs. Each spec carries the table name and the pair of snapshot
/// ids. The caller is responsible for validating both ids exist on the
/// target table.
///
/// The clause must be exactly `FOR INCREMENTAL BETWEEN SNAPSHOT <id1> AND
/// SNAPSHOT <id2>` (case insensitive). Descending ranges (start greater than
/// end) or non-integer arguments are rejected here with a clear error.
pub fn extract_incremental_spec(sql: &str) -> Result<(String, Vec<IncrementalSpec>)> {
    let upper = sql.to_uppercase();
    let needle = " FOR INCREMENTAL BETWEEN SNAPSHOT ";
    if !upper.contains(needle.trim()) {
        return Ok((sql.to_string(), vec![]));
    }

    let mut out = String::with_capacity(sql.len());
    let mut specs = Vec::new();
    let bytes = sql.as_bytes();
    let upper_bytes = upper.as_bytes();
    let mut cursor = 0;

    while cursor < bytes.len() {
        let window_start = cursor;
        let remaining = &upper_bytes[cursor..];
        let needle_upper = needle.as_bytes();
        let hit = find_needle(remaining, needle_upper);
        match hit {
            None => {
                out.push_str(&sql[window_start..]);
                break;
            }
            Some(offset) => {
                let clause_start = cursor + offset;
                let table_end = sql[..clause_start].trim_end().len();
                let table_start = find_table_start(&sql[..table_end]);
                let table = sql[table_start..table_end].trim().to_string();

                out.push_str(&sql[window_start..clause_start]);

                let after_kw = clause_start + needle.len();
                let (start_id, consumed_start) = parse_integer_token(&sql[after_kw..])?;
                let after_start = after_kw + consumed_start;

                // Expect `AND SNAPSHOT` (with leading + trailing whitespace).
                let rest = &sql[after_start..];
                let rest_upper = rest.to_uppercase();
                let and_marker = "AND SNAPSHOT";
                let rest_trimmed = rest_upper.trim_start();
                let leading_ws = rest_upper.len() - rest_trimmed.len();
                if leading_ws == 0 || !rest_trimmed.starts_with(and_marker) {
                    return Err(SqeError::Execution(
                        "FOR INCREMENTAL BETWEEN SNAPSHOT: expected 'AND SNAPSHOT <id>'"
                            .to_string(),
                    ));
                }
                let after_and_kw = after_start + leading_ws + and_marker.len();
                let (end_id, consumed_end) = parse_integer_token(&sql[after_and_kw..])?;
                cursor = after_and_kw + consumed_end;

                if start_id > end_id {
                    return Err(SqeError::Execution(format!(
                        "FOR INCREMENTAL BETWEEN SNAPSHOT {start_id} AND SNAPSHOT {end_id}: start must be older than end"
                    )));
                }

                specs.push(IncrementalSpec {
                    table,
                    start: start_id,
                    end: end_id,
                });
            }
        }
    }

    Ok((out, specs))
}

/// Parse an integer literal, returning (value, bytes_consumed).
///
/// Accepts an optional leading `-`. Iceberg's `UNASSIGNED_SNAPSHOT_ID` is -1,
/// so negatives are tolerated at the parser layer.
fn parse_integer_token(input: &str) -> Result<(i64, usize)> {
    let trimmed = input.trim_start();
    let leading_ws = input.len() - trimmed.len();

    if trimmed.is_empty() {
        return Err(SqeError::Execution(
            "FOR INCREMENTAL BETWEEN SNAPSHOT: expected snapshot id".to_string(),
        ));
    }

    let first = trimmed.as_bytes()[0];
    if !first.is_ascii_digit() && first != b'-' {
        return Err(SqeError::Execution(format!(
            "FOR INCREMENTAL BETWEEN SNAPSHOT: expected integer, got '{}'",
            trimmed.split_whitespace().next().unwrap_or("")
        )));
    }
    let end = trimmed
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(trimmed.len());
    let num_str = &trimmed[..end];
    let id: i64 = num_str.parse().map_err(|_| {
        SqeError::Execution(format!(
            "FOR INCREMENTAL BETWEEN SNAPSHOT: '{num_str}' is not a valid snapshot id"
        ))
    })?;
    Ok((id, leading_ws + end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_time_travel_passes_through() {
        let (sql, specs) = extract_time_travel_spec("SELECT * FROM t").unwrap();
        assert_eq!(sql, "SELECT * FROM t");
        assert!(specs.is_empty());
    }

    #[test]
    fn extract_numeric_version() {
        let input = "SELECT * FROM t FOR VERSION AS OF 12345";
        let (sql, specs) = extract_time_travel_spec(input).unwrap();
        assert_eq!(sql, "SELECT * FROM t");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].table, "t");
        assert_eq!(specs[0].version, VersionRef::SnapshotId(12345));
    }

    #[test]
    fn extract_named_ref() {
        let input = "SELECT * FROM ns.t FOR VERSION AS OF 'feature_x'";
        let (sql, specs) = extract_time_travel_spec(input).unwrap();
        assert_eq!(sql, "SELECT * FROM ns.t");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].table, "ns.t");
        assert_eq!(
            specs[0].version,
            VersionRef::Named("feature_x".to_string())
        );
    }

    #[test]
    fn extract_with_join() {
        let input = "SELECT * FROM a FOR VERSION AS OF 1 JOIN b ON a.id = b.id";
        let (sql, specs) = extract_time_travel_spec(input).unwrap();
        assert_eq!(sql, "SELECT * FROM a JOIN b ON a.id = b.id");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].table, "a");
        assert_eq!(specs[0].version, VersionRef::SnapshotId(1));
    }

    #[test]
    fn extract_multiple_refs() {
        let input = "SELECT * FROM a FOR VERSION AS OF 'b1', t FOR VERSION AS OF 99";
        let (_sql, specs) = extract_time_travel_spec(input).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].version, VersionRef::Named("b1".to_string()));
        assert_eq!(specs[1].version, VersionRef::SnapshotId(99));
    }

    #[test]
    fn case_insensitive_match() {
        let input = "SELECT * FROM t for version as of 42";
        let (sql, specs) = extract_time_travel_spec(input).unwrap();
        assert_eq!(sql, "SELECT * FROM t");
        assert_eq!(specs[0].version, VersionRef::SnapshotId(42));
    }

    #[test]
    fn reject_unterminated_string() {
        let input = "SELECT * FROM t FOR VERSION AS OF 'oops";
        assert!(extract_time_travel_spec(input).is_err());
    }

    #[test]
    fn reject_missing_version_arg() {
        let input = "SELECT * FROM t FOR VERSION AS OF ";
        assert!(extract_time_travel_spec(input).is_err());
    }

    #[test]
    fn negative_snapshot_id() {
        // iceberg uses UNASSIGNED_SNAPSHOT_ID = -1; accept negatives at the parser.
        let input = "SELECT * FROM t FOR VERSION AS OF -1";
        let (_sql, specs) = extract_time_travel_spec(input).unwrap();
        assert_eq!(specs[0].version, VersionRef::SnapshotId(-1));
    }

    // ── Incremental scan clause (Phase G) ────────────────────────────────

    #[test]
    fn no_incremental_passes_through() {
        let (sql, specs) = extract_incremental_spec("SELECT * FROM t").unwrap();
        assert_eq!(sql, "SELECT * FROM t");
        assert!(specs.is_empty());
    }

    #[test]
    fn extract_incremental_between_snapshots() {
        let input = "SELECT * FROM ns.t FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 105";
        let (sql, specs) = extract_incremental_spec(input).unwrap();
        assert_eq!(sql, "SELECT * FROM ns.t");
        assert_eq!(specs.len(), 1);
        assert_eq!(
            specs[0],
            IncrementalSpec {
                table: "ns.t".to_string(),
                start: 100,
                end: 105,
            }
        );
    }

    #[test]
    fn extract_incremental_case_insensitive() {
        let input = "select * from t for incremental between snapshot 1 and snapshot 2";
        let (_sql, specs) = extract_incremental_spec(input).unwrap();
        assert_eq!(specs[0].start, 1);
        assert_eq!(specs[0].end, 2);
    }

    #[test]
    fn incremental_rejects_descending_range() {
        let input = "SELECT * FROM t FOR INCREMENTAL BETWEEN SNAPSHOT 102 AND SNAPSHOT 100";
        let err = extract_incremental_spec(input).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("start must be older than end"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn incremental_requires_and_snapshot() {
        let input = "SELECT * FROM t FOR INCREMENTAL BETWEEN SNAPSHOT 1 SNAPSHOT 2";
        let err = extract_incremental_spec(input).unwrap_err();
        assert!(err.to_string().contains("AND SNAPSHOT"));
    }

    #[test]
    fn incremental_rejects_non_integer() {
        let input = "SELECT * FROM t FOR INCREMENTAL BETWEEN SNAPSHOT 'abc' AND SNAPSHOT 2";
        let err = extract_incremental_spec(input).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("expected integer"));
    }

    #[test]
    fn incremental_allows_equal_snapshots() {
        // Equal start/end is a valid empty range (open-closed interval).
        let input = "SELECT * FROM t FOR INCREMENTAL BETWEEN SNAPSHOT 5 AND SNAPSHOT 5";
        let (_sql, specs) = extract_incremental_spec(input).unwrap();
        assert_eq!(specs[0].start, 5);
        assert_eq!(specs[0].end, 5);
    }
}
