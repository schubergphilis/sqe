//! Extended DDL parsing for Iceberg branching and tagging.
//!
//! These statements are not part of standard SQL and are not parsed by
//! sqlparser-rs. We parse them with a small hand-rolled tokenizer so we
//! can keep the syntax close to Trino's dialect:
//!
//! ```sql
//! ALTER TABLE ns.t CREATE BRANCH feature_x
//!   [AS OF VERSION 12345]
//!   [WITH RETENTION (min_snapshots_to_keep => 10, max_snapshot_age_ms => 86400000)]
//!
//! ALTER TABLE ns.t CREATE [OR REPLACE] TAG v1
//!   [AS OF VERSION 12345]
//!   [WITH RETENTION (max_ref_age_ms => 86400000)]
//!
//! ALTER TABLE ns.t DROP BRANCH [IF EXISTS] feature_x
//! ALTER TABLE ns.t DROP TAG [IF EXISTS] v1
//! ALTER TABLE ns.t DROP TAG v1 [IF EXISTS]   -- also accepted
//! ```

use sqe_core::{Result, SqeError, SqeErrorCode};

/// A parsed branch/tag DDL statement ready for execution by the coordinator.
#[derive(Debug, Clone, PartialEq)]
pub enum RefDdl {
    CreateBranch {
        table: String,
        name: String,
        /// When Some, pin the new branch to this snapshot id. When None,
        /// the branch tracks the table's current snapshot.
        snapshot_id: Option<i64>,
        /// Retention options parsed from `WITH RETENTION (...)`.
        retention: BranchRetention,
    },
    CreateTag {
        table: String,
        name: String,
        snapshot_id: Option<i64>,
        /// `CREATE OR REPLACE TAG` overwrites an existing tag rather than failing.
        create_or_replace: bool,
        max_ref_age_ms: Option<i64>,
    },
    DropBranch {
        table: String,
        name: String,
        if_exists: bool,
    },
    DropTag {
        table: String,
        name: String,
        if_exists: bool,
    },
}

/// Retention parameters for a branch. All three fields default to `None`,
/// which means the engine falls back to table-level `history.expire.*` properties.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BranchRetention {
    pub min_snapshots_to_keep: Option<i32>,
    pub max_snapshot_age_ms: Option<i64>,
    pub max_ref_age_ms: Option<i64>,
}

/// Try to parse a branch/tag DDL statement. Returns `Ok(None)` if the input
/// does not look like one of these statements; the caller should then fall
/// through to the default sqlparser path.
pub fn try_parse_ref_ddl(sql: &str) -> Result<Option<RefDdl>> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_ascii_uppercase();

    // Must start with ALTER TABLE to be any of our statements.
    if !upper.starts_with("ALTER TABLE ") {
        return Ok(None);
    }

    // Skip "ALTER TABLE "
    let rest = trimmed[12..].trim_start();
    let (table, rest_after_table) = split_identifier(rest)?;
    let rest_upper = rest_after_table.to_ascii_uppercase();
    let rest_upper_trimmed = rest_upper.trim_start();
    let rest_trimmed = rest_after_table.trim_start();

    if rest_upper_trimmed.starts_with("CREATE ") {
        // CREATE [OR REPLACE] (BRANCH|TAG) name ...
        let after_create = rest_trimmed[6..].trim_start();
        let after_create_upper = after_create.to_ascii_uppercase();

        let (create_or_replace, after_maybe_replace) =
            if after_create_upper.starts_with("OR REPLACE ") {
                (true, after_create[10..].trim_start())
            } else {
                (false, after_create)
            };

        let after_upper = after_maybe_replace.to_ascii_uppercase();
        if after_upper.starts_with("BRANCH ") {
            if create_or_replace {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                    "CREATE OR REPLACE BRANCH is not supported (branches are mutable; use CREATE BRANCH)"
                        .to_string(),
                ));
            }
            let after_kw = after_maybe_replace[7..].trim_start();
            let (name, tail) = split_identifier(after_kw)?;
            let (snapshot_id, retention) = parse_branch_tail(tail)?;
            return Ok(Some(RefDdl::CreateBranch {
                table,
                name,
                snapshot_id,
                retention,
            }));
        }
        if after_upper.starts_with("TAG ") {
            let after_kw = after_maybe_replace[4..].trim_start();
            let (name, tail) = split_identifier(after_kw)?;
            let (snapshot_id, max_ref_age_ms) = parse_tag_tail(tail)?;
            return Ok(Some(RefDdl::CreateTag {
                table,
                name,
                snapshot_id,
                create_or_replace,
                max_ref_age_ms,
            }));
        }
        return Ok(None);
    }

    if rest_upper_trimmed.starts_with("DROP ") {
        let after_drop = rest_trimmed[5..].trim_start();
        let after_drop_upper = after_drop.to_ascii_uppercase();
        if after_drop_upper.starts_with("BRANCH ") {
            let (name, if_exists) = parse_drop_target(after_drop[7..].trim_start())?;
            return Ok(Some(RefDdl::DropBranch {
                table,
                name,
                if_exists,
            }));
        }
        if after_drop_upper.starts_with("TAG ") {
            let (name, if_exists) = parse_drop_target(after_drop[4..].trim_start())?;
            return Ok(Some(RefDdl::DropTag {
                table,
                name,
                if_exists,
            }));
        }
    }

    Ok(None)
}

/// The ref name and IF EXISTS of a DROP BRANCH / DROP TAG. IF EXISTS goes
/// before the name in Iceberg's Spark syntax (`DROP TAG IF EXISTS v1`) and
/// after it in SQE's original form (`DROP TAG v1 IF EXISTS`); both parse.
fn parse_drop_target(after_kw: &str) -> Result<(String, bool)> {
    if let Some(rest) = strip_keyword(after_kw, "IF").and_then(|r| strip_keyword(r, "EXISTS")) {
        let (name, tail) = split_identifier(rest)?;
        if !tail.trim().is_empty() {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("unexpected tokens after branch/tag name: '{}'", tail.trim()),
            ));
        }
        return Ok((name, true));
    }
    let (name, tail) = split_identifier(after_kw)?;
    Ok((name, parse_if_exists(tail)?))
}

/// `s` after a leading `keyword` (any case) and the whitespace that follows
/// it. A keyword with nothing after it is not stripped, so a ref named `if`
/// still parses as a name.
fn strip_keyword<'a>(s: &'a str, keyword: &str) -> Option<&'a str> {
    let head = s.get(..keyword.len())?;
    let rest = &s[keyword.len()..];
    if !head.eq_ignore_ascii_case(keyword) || !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let rest = rest.trim_start();
    (!rest.is_empty()).then_some(rest)
}

fn parse_if_exists(tail: &str) -> Result<bool> {
    let tail_trim = tail.trim();
    if tail_trim.is_empty() {
        return Ok(false);
    }
    if tail_trim.eq_ignore_ascii_case("IF EXISTS") {
        return Ok(true);
    }
    Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
        "unexpected tokens after branch/tag name: '{tail_trim}'"
    )))
}

fn parse_branch_tail(tail: &str) -> Result<(Option<i64>, BranchRetention)> {
    let (snapshot_id, after_as_of) = parse_as_of_version(tail)?;
    let retention_str = after_as_of.trim();
    let retention = if retention_str.is_empty() {
        BranchRetention::default()
    } else {
        parse_branch_retention(retention_str)?
    };
    Ok((snapshot_id, retention))
}

fn parse_tag_tail(tail: &str) -> Result<(Option<i64>, Option<i64>)> {
    let (snapshot_id, after_as_of) = parse_as_of_version(tail)?;
    let retention_str = after_as_of.trim();
    let max_ref_age_ms = if retention_str.is_empty() {
        None
    } else {
        parse_tag_retention(retention_str)?
    };
    Ok((snapshot_id, max_ref_age_ms))
}

/// Parse the optional `AS OF VERSION <n>` clause. Returns `(snapshot_id, remainder)`.
fn parse_as_of_version(input: &str) -> Result<(Option<i64>, &str)> {
    let trimmed = input.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("AS OF VERSION ") {
        return Ok((None, trimmed));
    }
    let after = trimmed[14..].trim_start();
    // Read the integer token
    let end = after
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(after.len());
    if end == 0 {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
            "AS OF VERSION requires an integer snapshot id".to_string(),
        ));
    }
    let num_str = &after[..end];
    let snapshot_id: i64 = num_str.parse().map_err(|_| {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "AS OF VERSION: '{num_str}' is not a valid integer snapshot id"
        ))
    })?;
    Ok((Some(snapshot_id), after[end..].trim_start()))
}

fn parse_branch_retention(input: &str) -> Result<BranchRetention> {
    let args = parse_retention_args(input)?;
    let mut out = BranchRetention::default();
    for (k, v) in args {
        match k.to_lowercase().as_str() {
            "min_snapshots_to_keep" => {
                out.min_snapshots_to_keep = Some(v.parse().map_err(|_| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "min_snapshots_to_keep requires an integer, got '{v}'"
                    ))
                })?);
            }
            "max_snapshot_age_ms" => {
                out.max_snapshot_age_ms = Some(v.parse().map_err(|_| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "max_snapshot_age_ms requires an integer, got '{v}'"
                    ))
                })?);
            }
            "max_ref_age_ms" => {
                out.max_ref_age_ms = Some(v.parse().map_err(|_| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("max_ref_age_ms requires an integer, got '{v}'"))
                })?);
            }
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "unknown retention option for branch: '{other}'"
                )));
            }
        }
    }
    Ok(out)
}

fn parse_tag_retention(input: &str) -> Result<Option<i64>> {
    let args = parse_retention_args(input)?;
    let mut out: Option<i64> = None;
    for (k, v) in args {
        match k.to_lowercase().as_str() {
            "max_ref_age_ms" => {
                out = Some(v.parse().map_err(|_| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("max_ref_age_ms requires an integer, got '{v}'"))
                })?);
            }
            other => {
                return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "unknown retention option for tag: '{other}' (tags only accept max_ref_age_ms)"
                )));
            }
        }
    }
    Ok(out)
}

/// Parse `WITH RETENTION (k => v, ...)` into a list of (key, value) strings.
fn parse_retention_args(input: &str) -> Result<Vec<(String, String)>> {
    let trimmed = input.trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("WITH RETENTION") {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
            "expected WITH RETENTION clause, got: '{trimmed}'"
        )));
    }
    let after_kw = trimmed[14..].trim_start();
    if !after_kw.starts_with('(') || !after_kw.ends_with(')') {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
            "WITH RETENTION requires parentheses around options".to_string(),
        ));
    }
    let inner = &after_kw[1..after_kw.len() - 1];
    let mut out = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut it = part.splitn(2, "=>");
        let k = it
            .next()
            .ok_or_else(|| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("bad retention option: '{part}'")))?
            .trim()
            .to_string();
        let v = it
            .next()
            .ok_or_else(|| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "retention option '{k}' missing value (use 'k => v')"
                ))
            })?
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        if k.is_empty() {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "empty retention key in: '{part}'"
            )));
        }
        out.push((k, v));
    }
    Ok(out)
}

/// Split the first whitespace-delimited identifier off the input, returning
/// (identifier, rest). Handles dotted identifiers (ns.table) and quoted
/// identifiers ("name with space") as a single token.
fn split_identifier(input: &str) -> Result<(String, &str)> {
    let trimmed = input.trim_start();
    if trimmed.is_empty() {
        return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, "expected an identifier".to_string()));
    }
    let bytes = trimmed.as_bytes();
    if bytes[0] == b'"' {
        let mut end = 1;
        while end < bytes.len() && bytes[end] != b'"' {
            end += 1;
        }
        if end >= bytes.len() {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed,
                "unterminated quoted identifier".to_string(),
            ));
        }
        let name = trimmed[1..end].to_string();
        Ok((name, &trimmed[end + 1..]))
    } else {
        let end = trimmed
            .find(|c: char| c.is_whitespace())
            .unwrap_or(trimmed.len());
        let name = trimmed[..end].to_string();
        Ok((name, &trimmed[end..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_branch_simple() {
        let ddl = try_parse_ref_ddl("ALTER TABLE ns.t CREATE BRANCH feature_x")
            .unwrap()
            .unwrap();
        assert_eq!(
            ddl,
            RefDdl::CreateBranch {
                table: "ns.t".to_string(),
                name: "feature_x".to_string(),
                snapshot_id: None,
                retention: BranchRetention::default(),
            }
        );
    }

    #[test]
    fn create_branch_at_version() {
        let ddl = try_parse_ref_ddl("ALTER TABLE ns.t CREATE BRANCH hist AS OF VERSION 12345")
            .unwrap()
            .unwrap();
        match ddl {
            RefDdl::CreateBranch { snapshot_id, .. } => {
                assert_eq!(snapshot_id, Some(12345));
            }
            other => panic!("expected CreateBranch, got {other:?}"),
        }
    }

    #[test]
    fn create_branch_with_retention() {
        let sql = "ALTER TABLE ns.t CREATE BRANCH feature_x WITH RETENTION (min_snapshots_to_keep => 5, max_snapshot_age_ms => 100, max_ref_age_ms => 200)";
        let ddl = try_parse_ref_ddl(sql).unwrap().unwrap();
        match ddl {
            RefDdl::CreateBranch { retention, .. } => {
                assert_eq!(retention.min_snapshots_to_keep, Some(5));
                assert_eq!(retention.max_snapshot_age_ms, Some(100));
                assert_eq!(retention.max_ref_age_ms, Some(200));
            }
            other => panic!("expected CreateBranch, got {other:?}"),
        }
    }

    #[test]
    fn create_branch_at_version_with_retention() {
        let sql = "ALTER TABLE t CREATE BRANCH b AS OF VERSION 77 WITH RETENTION (min_snapshots_to_keep => 3)";
        let ddl = try_parse_ref_ddl(sql).unwrap().unwrap();
        match ddl {
            RefDdl::CreateBranch {
                snapshot_id,
                retention,
                ..
            } => {
                assert_eq!(snapshot_id, Some(77));
                assert_eq!(retention.min_snapshots_to_keep, Some(3));
            }
            other => panic!("expected CreateBranch, got {other:?}"),
        }
    }

    #[test]
    fn create_tag_simple() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t CREATE TAG v1")
            .unwrap()
            .unwrap();
        assert_eq!(
            ddl,
            RefDdl::CreateTag {
                table: "t".to_string(),
                name: "v1".to_string(),
                snapshot_id: None,
                create_or_replace: false,
                max_ref_age_ms: None,
            }
        );
    }

    #[test]
    fn create_or_replace_tag() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t CREATE OR REPLACE TAG v1 AS OF VERSION 100")
            .unwrap()
            .unwrap();
        match ddl {
            RefDdl::CreateTag {
                create_or_replace,
                snapshot_id,
                ..
            } => {
                assert!(create_or_replace);
                assert_eq!(snapshot_id, Some(100));
            }
            other => panic!("expected CreateTag, got {other:?}"),
        }
    }

    #[test]
    fn create_or_replace_branch_rejected() {
        let result = try_parse_ref_ddl("ALTER TABLE t CREATE OR REPLACE BRANCH b");
        assert!(result.is_err());
    }

    #[test]
    fn create_tag_with_retention() {
        let sql = "ALTER TABLE t CREATE TAG v1 WITH RETENTION (max_ref_age_ms => 86400000)";
        let ddl = try_parse_ref_ddl(sql).unwrap().unwrap();
        match ddl {
            RefDdl::CreateTag { max_ref_age_ms, .. } => {
                assert_eq!(max_ref_age_ms, Some(86_400_000));
            }
            other => panic!("expected CreateTag, got {other:?}"),
        }
    }

    #[test]
    fn tag_rejects_branch_options() {
        let sql = "ALTER TABLE t CREATE TAG v1 WITH RETENTION (min_snapshots_to_keep => 5)";
        let err = try_parse_ref_ddl(sql).unwrap_err();
        assert!(err.to_string().contains("tags only accept max_ref_age_ms"));
    }

    #[test]
    fn drop_branch_if_exists() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t DROP BRANCH stale IF EXISTS")
            .unwrap()
            .unwrap();
        match ddl {
            RefDdl::DropBranch {
                name, if_exists, ..
            } => {
                assert_eq!(name, "stale");
                assert!(if_exists);
            }
            other => panic!("expected DropBranch, got {other:?}"),
        }
    }

    #[test]
    fn drop_branch_without_if_exists() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t DROP BRANCH stale")
            .unwrap()
            .unwrap();
        match ddl {
            RefDdl::DropBranch { if_exists, .. } => assert!(!if_exists),
            other => panic!("expected DropBranch, got {other:?}"),
        }
    }

    #[test]
    fn drop_tag_if_exists() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t DROP TAG v1 IF EXISTS")
            .unwrap()
            .unwrap();
        match ddl {
            RefDdl::DropTag {
                name, if_exists, ..
            } => {
                assert_eq!(name, "v1");
                assert!(if_exists);
            }
            other => panic!("expected DropTag, got {other:?}"),
        }
    }

    // Iceberg's Spark syntax puts IF EXISTS before the name (#865).
    #[test]
    fn drop_ref_if_exists_before_the_name() {
        let tag = try_parse_ref_ddl("ALTER TABLE t DROP TAG if  exists v1")
            .unwrap()
            .unwrap();
        assert_eq!(
            tag,
            RefDdl::DropTag {
                table: "t".to_string(),
                name: "v1".to_string(),
                if_exists: true,
            }
        );
        let branch = try_parse_ref_ddl("ALTER TABLE t DROP BRANCH IF EXISTS stale")
            .unwrap()
            .unwrap();
        assert_eq!(
            branch,
            RefDdl::DropBranch {
                table: "t".to_string(),
                name: "stale".to_string(),
                if_exists: true,
            }
        );
    }

    #[test]
    fn drop_ref_named_if_is_a_name() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t DROP TAG if")
            .unwrap()
            .unwrap();
        assert_eq!(
            ddl,
            RefDdl::DropTag {
                table: "t".to_string(),
                name: "if".to_string(),
                if_exists: false,
            }
        );
    }

    #[test]
    fn drop_ref_if_exists_on_both_sides_is_refused() {
        let err = try_parse_ref_ddl("ALTER TABLE t DROP TAG IF EXISTS v1 IF EXISTS").unwrap_err();
        assert!(err.to_string().contains("unexpected tokens"), "{err}");
    }

    #[test]
    fn non_branch_ddl_returns_none() {
        let ddl = try_parse_ref_ddl("SELECT 1").unwrap();
        assert!(ddl.is_none());
        let ddl = try_parse_ref_ddl("ALTER TABLE t ADD COLUMN x INT").unwrap();
        assert!(ddl.is_none());
    }

    #[test]
    fn quoted_branch_name() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t CREATE BRANCH \"feature branch\"")
            .unwrap()
            .unwrap();
        match ddl {
            RefDdl::CreateBranch { name, .. } => {
                assert_eq!(name, "feature branch");
            }
            other => panic!("expected CreateBranch, got {other:?}"),
        }
    }

    #[test]
    fn trailing_semicolon_ok() {
        let ddl = try_parse_ref_ddl("ALTER TABLE t CREATE BRANCH x;")
            .unwrap()
            .unwrap();
        matches!(ddl, RefDdl::CreateBranch { .. });
    }

    #[test]
    fn as_of_version_requires_integer() {
        let result = try_parse_ref_ddl("ALTER TABLE t CREATE BRANCH b AS OF VERSION 'abc'");
        assert!(result.is_err());
    }
}
