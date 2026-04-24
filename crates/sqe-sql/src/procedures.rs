//! Iceberg maintenance procedure call AST.
//!
//! SQE exposes `CALL system.<proc>(table => 'ns.t', ...)` as a SQL surface for
//! the vendored iceberg-rust transaction actions (rewrite files, expire
//! snapshots, remove orphan files, rewrite manifests). The parser produces
//! a [`ProcedureCall`] that the coordinator dispatches to a handler; handlers
//! in turn wrap the vendored action, commit the transaction, and return a
//! `RecordBatch` summary.
//!
//! ## Supported procedures
//!
//! - `system.rewrite_data_files(table => 'ns.t'[, target_file_size_bytes => N,
//!   min_input_files => N, max_concurrent_file_group_rewrites => N])`
//! - `system.expire_snapshots(table => 'ns.t'[, older_than => TIMESTAMP,
//!   retain_last => N])`
//! - `system.remove_orphan_files(table => 'ns.t'[, older_than => TIMESTAMP])`
//! - `system.rewrite_manifests(table => 'ns.t')`
//!
//! Options use Iceberg's named-argument syntax (`name => value`). Unknown
//! options produce a parse error so typos fail fast instead of being silently
//! accepted.
//!
//! Table references accept 1-part (`t`), 2-part (`ns.t`), or 3-part
//! (`cat.ns.t`) identifiers. The catalog prefix is ignored; the handler
//! resolves namespace + name against the session's bound catalog.

use chrono::{DateTime, Utc};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Statement, Value,
};

use sqe_core::SqeError;

/// A parsed `CALL system.<proc>(...)` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcedureCall {
    /// Compact small data files via vendored `RewriteFilesAction`.
    RewriteDataFiles {
        table: TableRef,
        /// Target size for rewritten data files. Default is 512 MiB.
        target_file_size_bytes: Option<u64>,
        /// Minimum number of input files before a group is eligible.
        /// Default is 5.
        min_input_files: Option<usize>,
        /// Maximum concurrent rewrite groups. Default is 4.
        max_concurrent_file_group_rewrites: Option<usize>,
    },
    /// Drop old snapshots via vendored `RemoveSnapshotAction`.
    ExpireSnapshots {
        table: TableRef,
        /// Expire snapshots older than this timestamp.
        older_than: Option<DateTime<Utc>>,
        /// Keep at least this many snapshots regardless of age.
        retain_last: Option<usize>,
    },
    /// Delete files under the table prefix not referenced by any live snapshot.
    RemoveOrphanFiles {
        table: TableRef,
        /// Only consider files older than this timestamp.
        /// Default is 3 days before now, to avoid races with in-flight writes.
        older_than: Option<DateTime<Utc>>,
    },
    /// Consolidate small manifest files via vendored `RewriteManifestsAction`.
    RewriteManifests { table: TableRef },
}

impl ProcedureCall {
    /// Lowercase identifier for metrics and audit logging.
    pub fn name(&self) -> &'static str {
        match self {
            ProcedureCall::RewriteDataFiles { .. } => "rewrite_data_files",
            ProcedureCall::ExpireSnapshots { .. } => "expire_snapshots",
            ProcedureCall::RemoveOrphanFiles { .. } => "remove_orphan_files",
            ProcedureCall::RewriteManifests { .. } => "rewrite_manifests",
        }
    }

    /// The target table for the procedure. All maintenance procedures target
    /// a single table, so this is always present.
    pub fn table(&self) -> &TableRef {
        match self {
            ProcedureCall::RewriteDataFiles { table, .. }
            | ProcedureCall::ExpireSnapshots { table, .. }
            | ProcedureCall::RemoveOrphanFiles { table, .. }
            | ProcedureCall::RewriteManifests { table } => table,
        }
    }
}

/// A parsed 1-, 2-, or 3-part table reference.
///
/// The catalog prefix is retained for display and audit but handlers resolve
/// against the session's bound catalog regardless. Namespace defaults to
/// `default` for single-part references, mirroring the engine's CREATE TABLE
/// behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub catalog: Option<String>,
    pub namespace: String,
    pub name: String,
}

impl TableRef {
    /// Construct from a 1-, 2-, or 3-part dotted string.
    pub fn parse(s: &str) -> sqe_core::Result<Self> {
        let parts: Vec<&str> = s.split('.').map(|p| p.trim()).collect();
        match parts.as_slice() {
            [name] => Ok(Self {
                catalog: None,
                namespace: "default".to_string(),
                name: (*name).to_string(),
            }),
            [ns, name] => Ok(Self {
                catalog: None,
                namespace: (*ns).to_string(),
                name: (*name).to_string(),
            }),
            [cat, ns, name] => Ok(Self {
                catalog: Some((*cat).to_string()),
                namespace: (*ns).to_string(),
                name: (*name).to_string(),
            }),
            _ => Err(SqeError::Execution(format!(
                "Invalid table reference in CALL: '{s}' (expected 1, 2, or 3 dotted parts)"
            ))),
        }
    }

    /// Render as `ns.table` or `cat.ns.table`.
    pub fn as_string(&self) -> String {
        match &self.catalog {
            Some(c) => format!("{c}.{}.{}", self.namespace, self.name),
            None => format!("{}.{}", self.namespace, self.name),
        }
    }
}

/// Try to interpret a sqlparser [`Statement::Call`] as an Iceberg maintenance
/// procedure. Returns `Ok(Some(_))` on a match, `Ok(None)` if the statement is
/// not a `system.<known>` call (handler should treat it as unsupported), and
/// `Err(_)` for malformed arguments to a known procedure.
pub fn try_parse_call(stmt: &Statement) -> sqe_core::Result<Option<ProcedureCall>> {
    let func = match stmt {
        Statement::Call(func) => func,
        _ => return Ok(None),
    };

    let Some((schema, proc)) = split_system_name(&func.name) else {
        return Ok(None);
    };

    if !schema.eq_ignore_ascii_case("system") {
        return Ok(None);
    }

    let args = extract_args(&func.args)?;

    match proc.to_ascii_lowercase().as_str() {
        "rewrite_data_files" => parse_rewrite_data_files(args).map(Some),
        "expire_snapshots" => parse_expire_snapshots(args).map(Some),
        "remove_orphan_files" => parse_remove_orphan_files(args).map(Some),
        "rewrite_manifests" => parse_rewrite_manifests(args).map(Some),
        _ => Ok(None),
    }
}

/// Split `system.rewrite_data_files` or similar into (schema, proc).
fn split_system_name(name: &ObjectName) -> Option<(String, String)> {
    let parts: Vec<String> = name.0.iter().map(|i| i.value.clone()).collect();
    match parts.as_slice() {
        [a, b] => Some((a.clone(), b.clone())),
        _ => None,
    }
}

/// Extract `name => value` pairs from a `Function::args` field. Unnamed
/// positional arguments are rejected because the Iceberg contract expects
/// named args and silent positional acceptance is a foot-gun.
fn extract_args(args: &FunctionArguments) -> sqe_core::Result<Vec<(String, Expr)>> {
    let list = match args {
        FunctionArguments::List(list) => list,
        FunctionArguments::None => return Ok(Vec::new()),
        FunctionArguments::Subquery(_) => {
            return Err(SqeError::Execution(
                "CALL system.* does not accept subquery arguments".to_string(),
            ));
        }
    };

    let mut out = Vec::with_capacity(list.args.len());
    for arg in &list.args {
        match arg {
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => out.push((name.value.clone(), expr.clone())),
            FunctionArg::ExprNamed {
                name: Expr::Identifier(ident),
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => out.push((ident.value.clone(), expr.clone())),
            FunctionArg::Unnamed(_) => {
                return Err(SqeError::Execution(
                    "CALL system.* requires named arguments like `table => 'ns.t'`".to_string(),
                ));
            }
            _ => {
                return Err(SqeError::Execution(format!(
                    "Unsupported argument shape for CALL system.*: {arg}"
                )));
            }
        }
    }
    Ok(out)
}

fn take_table(args: &mut Vec<(String, Expr)>) -> sqe_core::Result<TableRef> {
    let pos = args
        .iter()
        .position(|(k, _)| k.eq_ignore_ascii_case("table"))
        .ok_or_else(|| {
            SqeError::Execution("CALL system.* requires a `table => 'ns.t'` argument".to_string())
        })?;
    let (_, expr) = args.remove(pos);
    let s = expect_string(&expr, "table")?;
    TableRef::parse(&s)
}

fn take_option<T, F>(
    args: &mut Vec<(String, Expr)>,
    key: &str,
    parse: F,
) -> sqe_core::Result<Option<T>>
where
    F: FnOnce(&Expr) -> sqe_core::Result<T>,
{
    if let Some(pos) = args
        .iter()
        .position(|(k, _)| k.eq_ignore_ascii_case(key))
    {
        let (_, expr) = args.remove(pos);
        let parsed = parse(&expr)?;
        Ok(Some(parsed))
    } else {
        Ok(None)
    }
}

fn expect_no_remaining(args: &[(String, Expr)], proc: &str) -> sqe_core::Result<()> {
    if let Some((k, _)) = args.first() {
        return Err(SqeError::Execution(format!(
            "Unknown argument '{k}' for CALL system.{proc}"
        )));
    }
    Ok(())
}

fn expect_string(expr: &Expr, field: &str) -> sqe_core::Result<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s)) => Ok(s.clone()),
        other => Err(SqeError::Execution(format!(
            "Expected string literal for '{field}', got: {other}"
        ))),
    }
}

fn expect_u64(expr: &Expr, field: &str) -> sqe_core::Result<u64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse::<u64>().map_err(|e| {
            SqeError::Execution(format!("Invalid integer for '{field}': {n} ({e})"))
        }),
        other => Err(SqeError::Execution(format!(
            "Expected integer for '{field}', got: {other}"
        ))),
    }
}

fn expect_usize(expr: &Expr, field: &str) -> sqe_core::Result<usize> {
    expect_u64(expr, field).and_then(|v| {
        usize::try_from(v).map_err(|_| {
            SqeError::Execution(format!("Value for '{field}' does not fit in usize: {v}"))
        })
    })
}

fn expect_timestamp(expr: &Expr, field: &str) -> sqe_core::Result<DateTime<Utc>> {
    let s = match expr {
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s)) => s.clone(),
        Expr::TypedString { value, .. } => value.clone(),
        other => {
            return Err(SqeError::Execution(format!(
                "Expected timestamp literal for '{field}', got: {other}"
            )));
        }
    };

    // Accept RFC 3339 directly, or a bare "YYYY-MM-DD HH:MM:SS" at UTC.
    if let Ok(ts) = DateTime::parse_from_rfc3339(&s) {
        return Ok(ts.with_timezone(&Utc));
    }
    if let Ok(ts) = DateTime::parse_from_str(&format!("{s} +0000"), "%Y-%m-%d %H:%M:%S %z") {
        return Ok(ts.with_timezone(&Utc));
    }

    Err(SqeError::Execution(format!(
        "Could not parse timestamp for '{field}': '{s}' (expected RFC 3339 or 'YYYY-MM-DD HH:MM:SS')"
    )))
}

fn parse_rewrite_data_files(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let target_file_size_bytes =
        take_option(&mut args, "target_file_size_bytes", |e| expect_u64(e, "target_file_size_bytes"))?;
    let min_input_files =
        take_option(&mut args, "min_input_files", |e| expect_usize(e, "min_input_files"))?;
    let max_concurrent_file_group_rewrites = take_option(
        &mut args,
        "max_concurrent_file_group_rewrites",
        |e| expect_usize(e, "max_concurrent_file_group_rewrites"),
    )?;
    expect_no_remaining(&args, "rewrite_data_files")?;

    Ok(ProcedureCall::RewriteDataFiles {
        table,
        target_file_size_bytes,
        min_input_files,
        max_concurrent_file_group_rewrites,
    })
}

fn parse_expire_snapshots(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let older_than = take_option(&mut args, "older_than", |e| expect_timestamp(e, "older_than"))?;
    let retain_last =
        take_option(&mut args, "retain_last", |e| expect_usize(e, "retain_last"))?;
    expect_no_remaining(&args, "expire_snapshots")?;

    Ok(ProcedureCall::ExpireSnapshots {
        table,
        older_than,
        retain_last,
    })
}

fn parse_remove_orphan_files(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let older_than = take_option(&mut args, "older_than", |e| expect_timestamp(e, "older_than"))?;
    expect_no_remaining(&args, "remove_orphan_files")?;

    Ok(ProcedureCall::RemoveOrphanFiles { table, older_than })
}

fn parse_rewrite_manifests(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    expect_no_remaining(&args, "rewrite_manifests")?;
    Ok(ProcedureCall::RewriteManifests { table })
}

// The `_` bindings below exist to silence "unused" lints if we later restrict
// which sqlparser types we import. Keeping this hook avoids churn.
#[allow(dead_code)]
fn _sanity(_: &Function) {}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_first(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .remove(0)
    }

    #[test]
    fn tableref_parses_two_part() {
        let r = TableRef::parse("ns.t").unwrap();
        assert_eq!(r.namespace, "ns");
        assert_eq!(r.name, "t");
        assert_eq!(r.catalog, None);
    }

    #[test]
    fn tableref_parses_three_part() {
        let r = TableRef::parse("cat.ns.t").unwrap();
        assert_eq!(r.catalog.as_deref(), Some("cat"));
        assert_eq!(r.namespace, "ns");
        assert_eq!(r.name, "t");
    }

    #[test]
    fn tableref_single_part_uses_default_namespace() {
        let r = TableRef::parse("orders").unwrap();
        assert_eq!(r.namespace, "default");
        assert_eq!(r.name, "orders");
    }

    #[test]
    fn tableref_rejects_four_part() {
        assert!(TableRef::parse("a.b.c.d").is_err());
    }

    #[test]
    fn parses_rewrite_data_files_bare() {
        let stmt = parse_first("CALL system.rewrite_data_files(table => 'ns.t')");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::RewriteDataFiles {
                table,
                target_file_size_bytes,
                min_input_files,
                max_concurrent_file_group_rewrites,
            } => {
                assert_eq!(table.namespace, "ns");
                assert_eq!(table.name, "t");
                assert!(target_file_size_bytes.is_none());
                assert!(min_input_files.is_none());
                assert!(max_concurrent_file_group_rewrites.is_none());
            }
            other => panic!("Expected RewriteDataFiles, got {other:?}"),
        }
    }

    #[test]
    fn parses_rewrite_data_files_with_options() {
        let stmt = parse_first(
            "CALL system.rewrite_data_files(table => 'ns.t', target_file_size_bytes => 268435456, \
             min_input_files => 10, max_concurrent_file_group_rewrites => 2)",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::RewriteDataFiles {
                target_file_size_bytes,
                min_input_files,
                max_concurrent_file_group_rewrites,
                ..
            } => {
                assert_eq!(target_file_size_bytes, Some(268_435_456));
                assert_eq!(min_input_files, Some(10));
                assert_eq!(max_concurrent_file_group_rewrites, Some(2));
            }
            other => panic!("Expected RewriteDataFiles, got {other:?}"),
        }
    }

    #[test]
    fn parses_expire_snapshots_time_and_count() {
        let stmt = parse_first(
            "CALL system.expire_snapshots(table => 'ns.t', older_than => '2026-04-01T00:00:00Z', \
             retain_last => 5)",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::ExpireSnapshots {
                older_than,
                retain_last,
                ..
            } => {
                assert!(older_than.is_some());
                assert_eq!(retain_last, Some(5));
            }
            other => panic!("Expected ExpireSnapshots, got {other:?}"),
        }
    }

    #[test]
    fn parses_remove_orphan_files_defaults() {
        let stmt = parse_first("CALL system.remove_orphan_files(table => 'ns.t')");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::RemoveOrphanFiles { older_than, .. } => {
                assert!(older_than.is_none());
            }
            other => panic!("Expected RemoveOrphanFiles, got {other:?}"),
        }
    }

    #[test]
    fn parses_rewrite_manifests() {
        let stmt = parse_first("CALL system.rewrite_manifests(table => 'ns.t')");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        assert!(matches!(call, ProcedureCall::RewriteManifests { .. }));
    }

    #[test]
    fn unknown_option_is_error() {
        let stmt = parse_first(
            "CALL system.rewrite_data_files(table => 'ns.t', bogus_flag => 'x')",
        );
        let err = try_parse_call(&stmt).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Unknown argument 'bogus_flag'"), "{msg}");
    }

    #[test]
    fn unnamed_arg_is_error() {
        let stmt = parse_first("CALL system.rewrite_data_files('ns.t')");
        let err = try_parse_call(&stmt).unwrap_err();
        assert!(err.to_string().contains("named arguments"));
    }

    #[test]
    fn missing_table_is_error() {
        let stmt = parse_first("CALL system.rewrite_data_files(retain_last => 5)");
        let err = try_parse_call(&stmt).unwrap_err();
        assert!(err.to_string().contains("requires a `table =>"));
    }

    #[test]
    fn unknown_procedure_returns_none() {
        let stmt = parse_first("CALL system.do_something(table => 'ns.t')");
        let out = try_parse_call(&stmt).unwrap();
        assert!(out.is_none(), "unknown procedure should not match");
    }

    #[test]
    fn non_system_schema_returns_none() {
        let stmt = parse_first("CALL other.rewrite_data_files(table => 'ns.t')");
        let out = try_parse_call(&stmt).unwrap();
        assert!(out.is_none(), "non-system schema should not match");
    }

    #[test]
    fn timestamp_without_tz_defaults_to_utc() {
        let stmt = parse_first(
            "CALL system.expire_snapshots(table => 'ns.t', older_than => '2026-04-01 12:00:00')",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        if let ProcedureCall::ExpireSnapshots { older_than, .. } = call {
            let ts = older_than.expect("parsed");
            assert_eq!(ts.to_rfc3339(), "2026-04-01T12:00:00+00:00");
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn procedure_name_label_stable() {
        let stmt = parse_first("CALL system.rewrite_data_files(table => 'ns.t')");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        assert_eq!(call.name(), "rewrite_data_files");
        assert_eq!(call.table().as_string(), "ns.t");
    }
}
