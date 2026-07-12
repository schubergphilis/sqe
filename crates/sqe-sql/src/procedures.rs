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
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Statement, Value,
    ValueWithSpan,
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
    /// Suggest columns for `write.parquet.bloom-filter-columns` based on
    /// recent query history (Phase F task 7.10).
    SuggestBloomFilterColumns {
        table: TableRef,
        /// Optional history window (default 1000). Caps the number of
        /// finished-query records examined.
        history_limit: Option<usize>,
    },
    /// Sweep a namespace's warehouse prefix for S3 directories that are not
    /// registered as tables in the catalog. Default `dry_run = true` reports
    /// what would be deleted; `dry_run => false` actually deletes via the
    /// table-namespace's FileIO. Targets one namespace at a time; the catalog
    /// component is resolved against the session's bound catalog.
    PurgeOrphanLocations {
        namespace: NamespaceRef,
        /// When true (default), only enumerate and report. When false,
        /// delete the orphan prefixes.
        dry_run: bool,
    },
    /// Register an existing Iceberg table (data files + metadata already on
    /// the object store) into the session's catalog by recording a pointer
    /// to its current `metadata.json`. No data movement. Mirrors Spark's
    /// `CALL <catalog>.system.register_table(...)`.
    RegisterTable {
        table: TableRef,
        /// Absolute URI of the table's current `metadata.json`. The catalog
        /// backend reads it to validate schema + partition spec + current
        /// snapshot before committing the registration.
        metadata_location: String,
    },
    /// Drop a table from the catalog. Default `purge => false` removes the
    /// catalog entry only; data and metadata files on the object store are
    /// preserved (the table can be re-registered with [`RegisterTable`]).
    /// `purge => true` additionally deletes underlying files via the
    /// backend's `purge_table` operation.
    DropTable {
        table: TableRef,
        /// When true, delete data + metadata files in addition to the catalog
        /// entry. Default false (catalog-only drop, safe for migration and
        /// recovery workflows).
        purge: bool,
    },
    /// Move a table's current snapshot pointer to a previous snapshot id.
    /// Subsequent reads without an explicit `FOR VERSION AS OF` see the
    /// chosen snapshot. Used for pinning to a known-good state for
    /// reproducible testing. Does NOT append a new snapshot — the history
    /// is rewritten to mark `snapshot_id` as current.
    SetCurrentSnapshot {
        table: TableRef,
        /// Target snapshot id. Must exist in the table's snapshot log.
        snapshot_id: i64,
    },
    /// Roll a table back to a previous snapshot by appending a new snapshot
    /// whose state matches the target. Preserves the snapshot log (audit
    /// trail) unlike [`SetCurrentSnapshot`].
    RollbackToSnapshot {
        table: TableRef,
        /// Target snapshot id. Must exist in the table's snapshot log.
        snapshot_id: i64,
    },
}

impl ProcedureCall {
    /// Lowercase identifier for metrics and audit logging.
    pub fn name(&self) -> &'static str {
        match self {
            ProcedureCall::RewriteDataFiles { .. } => "rewrite_data_files",
            ProcedureCall::ExpireSnapshots { .. } => "expire_snapshots",
            ProcedureCall::RemoveOrphanFiles { .. } => "remove_orphan_files",
            ProcedureCall::RewriteManifests { .. } => "rewrite_manifests",
            ProcedureCall::SuggestBloomFilterColumns { .. } => "suggest_bloom_filter_columns",
            ProcedureCall::PurgeOrphanLocations { .. } => "purge_orphan_locations",
            ProcedureCall::RegisterTable { .. } => "register_table",
            ProcedureCall::DropTable { .. } => "drop_table",
            ProcedureCall::SetCurrentSnapshot { .. } => "set_current_snapshot",
            ProcedureCall::RollbackToSnapshot { .. } => "rollback_to_snapshot",
        }
    }

    /// The target table for table-targeted procedures. Returns `None` for
    /// namespace-targeted procedures like `PurgeOrphanLocations`.
    pub fn table(&self) -> Option<&TableRef> {
        match self {
            ProcedureCall::RewriteDataFiles { table, .. }
            | ProcedureCall::ExpireSnapshots { table, .. }
            | ProcedureCall::RemoveOrphanFiles { table, .. }
            | ProcedureCall::RewriteManifests { table }
            | ProcedureCall::SuggestBloomFilterColumns { table, .. }
            | ProcedureCall::RegisterTable { table, .. }
            | ProcedureCall::DropTable { table, .. }
            | ProcedureCall::SetCurrentSnapshot { table, .. }
            | ProcedureCall::RollbackToSnapshot { table, .. } => Some(table),
            ProcedureCall::PurgeOrphanLocations { .. } => None,
        }
    }

    /// Display label for audit logs. Returns the table identifier for
    /// table-targeted procedures, the namespace identifier for
    /// namespace-targeted ones.
    pub fn target_label(&self) -> String {
        match self {
            ProcedureCall::PurgeOrphanLocations { namespace, .. } => namespace.as_string(),
            _ => self.table().map(|t| t.as_string()).unwrap_or_default(),
        }
    }
}

/// A parsed 1- or 2-part namespace reference. Mirrors [`TableRef`] but with
/// no `name` segment. Catalog prefix is retained for display; handlers
/// resolve against the session's bound catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceRef {
    pub catalog: Option<String>,
    pub namespace: String,
}

impl NamespaceRef {
    pub fn parse(s: &str) -> sqe_core::Result<Self> {
        let parts: Vec<&str> = s.split('.').map(|p| p.trim()).collect();
        match parts.as_slice() {
            [ns] => Ok(Self {
                catalog: None,
                namespace: (*ns).to_string(),
            }),
            [cat, ns] => Ok(Self {
                catalog: Some((*cat).to_string()),
                namespace: (*ns).to_string(),
            }),
            _ => Err(SqeError::Execution(format!(
                "Invalid namespace reference in CALL: '{s}' (expected 1 or 2 dotted parts)"
            ))),
        }
    }

    pub fn as_string(&self) -> String {
        match &self.catalog {
            Some(c) => format!("{c}.{}", self.namespace),
            None => self.namespace.clone(),
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

    let proc_lower = proc.to_ascii_lowercase();
    let args = extract_args(&proc_lower, &func.args)?;

    match proc_lower.as_str() {
        "rewrite_data_files" => parse_rewrite_data_files(args).map(Some),
        "expire_snapshots" => parse_expire_snapshots(args).map(Some),
        "remove_orphan_files" => parse_remove_orphan_files(args).map(Some),
        "rewrite_manifests" => parse_rewrite_manifests(args).map(Some),
        "suggest_bloom_filter_columns" => parse_suggest_bloom_filter_columns(args).map(Some),
        "purge_orphan_locations" => parse_purge_orphan_locations(args).map(Some),
        "register_table" => parse_register_table(args).map(Some),
        "drop_table" => parse_drop_table(args).map(Some),
        "set_current_snapshot" => parse_set_current_snapshot(args).map(Some),
        "rollback_to_snapshot" => parse_rollback_to_snapshot(args).map(Some),
        _ => Ok(None),
    }
}

/// Parse `CALL system.register_table(table => 'ns.t', metadata_location => 's3://...')`.
fn parse_register_table(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let metadata_location = take_option(&mut args, "metadata_location", |e| {
        expect_string(e, "metadata_location")
    })?
    .ok_or_else(|| {
        SqeError::Execution(
            "CALL system.register_table requires `metadata_location => 's3://...'`".into(),
        )
    })?;
    expect_no_remaining(&args, "register_table")?;
    Ok(ProcedureCall::RegisterTable {
        table,
        metadata_location,
    })
}

/// Parse `CALL system.drop_table(table => 'ns.t'[, purge => true|false])`.
///
/// `purge` defaults to `false` so the safe operation (catalog-only drop,
/// data files preserved) is the default. A typo cannot cause data loss.
fn parse_drop_table(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let purge = take_option(&mut args, "purge", |e| expect_bool(e, "purge"))?.unwrap_or(false);
    expect_no_remaining(&args, "drop_table")?;
    Ok(ProcedureCall::DropTable { table, purge })
}

/// Parse `CALL system.set_current_snapshot(table => 'ns.t', snapshot_id => 1234567890)`.
fn parse_set_current_snapshot(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let snapshot_id = take_option(&mut args, "snapshot_id", |e| expect_i64(e, "snapshot_id"))?
        .ok_or_else(|| {
            SqeError::Execution(
                "CALL system.set_current_snapshot requires `snapshot_id => <id>`".into(),
            )
        })?;
    expect_no_remaining(&args, "set_current_snapshot")?;
    Ok(ProcedureCall::SetCurrentSnapshot { table, snapshot_id })
}

/// Parse `CALL system.rollback_to_snapshot(table => 'ns.t', snapshot_id => 1234567890)`.
fn parse_rollback_to_snapshot(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let snapshot_id = take_option(&mut args, "snapshot_id", |e| expect_i64(e, "snapshot_id"))?
        .ok_or_else(|| {
            SqeError::Execution(
                "CALL system.rollback_to_snapshot requires `snapshot_id => <id>`".into(),
            )
        })?;
    expect_no_remaining(&args, "rollback_to_snapshot")?;
    Ok(ProcedureCall::RollbackToSnapshot { table, snapshot_id })
}

/// Parse a signed integer literal. Iceberg snapshot ids are i64; sqlparser
/// emits them through `Value::Number`. Negative literals show up as
/// `UnaryOp(Minus, Number)` because sqlparser does not fold unary minus
/// into the number itself.
fn expect_i64(expr: &Expr, field: &str) -> sqe_core::Result<i64> {
    use sqlparser::ast::UnaryOperator;
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => n
            .parse::<i64>()
            .map_err(|e| SqeError::Execution(format!("Invalid integer for '{field}': {n} ({e})"))),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            let inner_val = expect_i64(inner, field)?;
            inner_val
                .checked_neg()
                .ok_or_else(|| SqeError::Execution(format!("Value for '{field}' overflows i64")))
        }
        other => Err(SqeError::Execution(format!(
            "Expected integer for '{field}', got: {other}"
        ))),
    }
}

/// Parse `CALL system.purge_orphan_locations(namespace => 'ns'[, dry_run => true|false])`.
///
/// `dry_run` defaults to `true` so a typo like `CALL system.purge_orphan_locations(namespace => 'ns')`
/// is a safe report-only operation. Operators must explicitly pass `dry_run => false`
/// to actually delete.
fn parse_purge_orphan_locations(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let namespace = take_namespace(&mut args)?;
    let dry_run = take_option(&mut args, "dry_run", |e| expect_bool(e, "dry_run"))?.unwrap_or(true);
    expect_no_remaining(&args, "purge_orphan_locations")?;
    Ok(ProcedureCall::PurgeOrphanLocations { namespace, dry_run })
}

fn take_namespace(args: &mut Vec<(String, Expr)>) -> sqe_core::Result<NamespaceRef> {
    let pos = args
        .iter()
        .position(|(k, _)| k.eq_ignore_ascii_case("namespace"))
        .ok_or_else(|| {
            SqeError::Execution(
                "CALL system.purge_orphan_locations requires a `namespace => 'ns'` argument".into(),
            )
        })?;
    let (_, expr) = args.remove(pos);
    let s = expect_string(&expr, "namespace")?;
    NamespaceRef::parse(&s)
}

fn expect_bool(expr: &Expr, field: &str) -> sqe_core::Result<bool> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(b),
            ..
        }) => Ok(*b),
        other => Err(SqeError::Execution(format!(
            "Expected boolean literal for '{field}', got: {other}"
        ))),
    }
}

/// Split `system.rewrite_data_files` or similar into (schema, proc).
fn split_system_name(name: &ObjectName) -> Option<(String, String)> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.clone())
        .collect();
    match parts.as_slice() {
        [a, b] => Some((a.clone(), b.clone())),
        _ => None,
    }
}

/// Resolve a `CALL system.<proc>(...)` argument list into the `name => value`
/// pairs the per-procedure parsers consume.
///
/// Trino accepts both named (`table => 'ns.t'`) and positional
/// (`'ns', 't', <id>`) procedure arguments, but a single CALL must be one or
/// the other: Trino rejects any mix with "Named and positional arguments
/// cannot be mixed", and we match that. Named args pass through. Positional
/// args are mapped to names via `positional_spec` for the procedures whose
/// Trino signature we have verified; folding Trino's leading `(schema, table)`
/// pair into SQE's single `table => 'schema.table'` ref. Procedures without a
/// verified positional signature stay named-only.
fn extract_args(proc: &str, args: &FunctionArguments) -> sqe_core::Result<Vec<(String, Expr)>> {
    let list = match args {
        FunctionArguments::List(list) => list,
        FunctionArguments::None => return Ok(Vec::new()),
        FunctionArguments::Subquery(_) => {
            return Err(SqeError::Execution(
                "CALL system.* does not accept subquery arguments".to_string(),
            ));
        }
    };

    let mut named = Vec::with_capacity(list.args.len());
    let mut positional = Vec::with_capacity(list.args.len());
    for arg in &list.args {
        match arg {
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => named.push((name.value.clone(), expr.clone())),
            FunctionArg::ExprNamed {
                name: Expr::Identifier(ident),
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => named.push((ident.value.clone(), expr.clone())),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => positional.push(expr.clone()),
            _ => {
                return Err(SqeError::Execution(format!(
                    "Unsupported argument shape for CALL system.*: {arg}"
                )));
            }
        }
    }

    if !named.is_empty() && !positional.is_empty() {
        return Err(SqeError::Execution(
            "Named and positional arguments cannot be mixed".to_string(),
        ));
    }

    if positional.is_empty() {
        return Ok(named);
    }
    positional_to_named(proc, positional)
}

/// One positional parameter slot in a procedure's Trino signature, expressed in
/// terms of how SQE consumes it.
enum PosSlot {
    /// Trino's leading `(schema_name, table_name)` pair, folded into SQE's
    /// single `table => 'schema.table'` reference.
    SchemaThenTable,
    /// A single positional argument bound to the named key SQE expects.
    Named(&'static str),
}

/// Trino positional signatures for the procedures we have verified against the
/// Trino 465 connector source. Procedures absent here stay named-only: shipping
/// an unverified positional order would silently bind arguments to the wrong
/// parameter, which is worse than requiring named args.
fn positional_spec(proc: &str) -> Option<&'static [PosSlot]> {
    match proc {
        // io.trino.plugin.iceberg.procedure.RollbackToSnapshotProcedure:
        // (SCHEMA, TABLE, SNAPSHOT_ID).
        "rollback_to_snapshot" => Some(&[PosSlot::SchemaThenTable, PosSlot::Named("snapshot_id")]),
        _ => None,
    }
}

/// Map positional arguments onto their procedure's named parameters.
fn positional_to_named(proc: &str, exprs: Vec<Expr>) -> sqe_core::Result<Vec<(String, Expr)>> {
    let spec = positional_spec(proc).ok_or_else(|| {
        SqeError::Execution(format!(
            "CALL system.{proc} does not support positional arguments; use named arguments like `table => 'ns.t'`"
        ))
    })?;

    let mut it = exprs.into_iter();
    let mut out = Vec::new();
    for slot in spec {
        match slot {
            PosSlot::SchemaThenTable => {
                let schema = next_positional(&mut it, proc)?;
                let table = next_positional(&mut it, proc)?;
                let schema = expect_non_null_string(&schema, "schema")?;
                let table = expect_non_null_string(&table, "table")?;
                out.push((
                    "table".to_string(),
                    string_expr(format!("{schema}.{table}")),
                ));
            }
            PosSlot::Named(key) => {
                let expr = next_positional(&mut it, proc)?;
                reject_null(&expr, key)?;
                out.push(((*key).to_string(), expr));
            }
        }
    }
    if it.next().is_some() {
        return Err(SqeError::Execution(format!(
            "Too many positional arguments for CALL system.{proc}"
        )));
    }
    Ok(out)
}

fn next_positional(it: &mut std::vec::IntoIter<Expr>, proc: &str) -> sqe_core::Result<Expr> {
    it.next().ok_or_else(|| {
        SqeError::Execution(format!(
            "Too few positional arguments for CALL system.{proc}"
        ))
    })
}

fn is_null(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(ValueWithSpan {
            value: Value::Null,
            ..
        })
    )
}

/// Reject a NULL positional argument with Trino's `<field> cannot be null`
/// phrasing (asserted by testRollbackToSnapshotWithNullArgument).
fn reject_null(expr: &Expr, field: &str) -> sqe_core::Result<()> {
    if is_null(expr) {
        return Err(SqeError::Execution(format!("{field} cannot be null")));
    }
    Ok(())
}

fn expect_non_null_string(expr: &Expr, field: &str) -> sqe_core::Result<String> {
    reject_null(expr, field)?;
    expect_string(expr, field)
}

fn string_expr(s: String) -> Expr {
    Expr::Value(Value::SingleQuotedString(s).with_empty_span())
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
    if let Some(pos) = args.iter().position(|(k, _)| k.eq_ignore_ascii_case(key)) {
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
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ..
        }) => Ok(s.clone()),
        other => Err(SqeError::Execution(format!(
            "Expected string literal for '{field}', got: {other}"
        ))),
    }
}

fn expect_u64(expr: &Expr, field: &str) -> sqe_core::Result<u64> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n, _),
            ..
        }) => n
            .parse::<u64>()
            .map_err(|e| SqeError::Execution(format!("Invalid integer for '{field}': {n} ({e})"))),
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
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ..
        }) => s.clone(),
        Expr::TypedString(ts) => match ts.value.clone().into_string() {
            Some(s) => s,
            None => {
                return Err(SqeError::Execution(format!(
                    "Expected timestamp literal for '{field}', got: {expr}"
                )));
            }
        },
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
    let target_file_size_bytes = take_option(&mut args, "target_file_size_bytes", |e| {
        expect_u64(e, "target_file_size_bytes")
    })?;
    let min_input_files = take_option(&mut args, "min_input_files", |e| {
        expect_usize(e, "min_input_files")
    })?;
    let max_concurrent_file_group_rewrites =
        take_option(&mut args, "max_concurrent_file_group_rewrites", |e| {
            expect_usize(e, "max_concurrent_file_group_rewrites")
        })?;
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
    let older_than = take_option(&mut args, "older_than", |e| {
        expect_timestamp(e, "older_than")
    })?;
    let retain_last = take_option(&mut args, "retain_last", |e| expect_usize(e, "retain_last"))?;
    expect_no_remaining(&args, "expire_snapshots")?;

    Ok(ProcedureCall::ExpireSnapshots {
        table,
        older_than,
        retain_last,
    })
}

fn parse_remove_orphan_files(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let older_than = take_option(&mut args, "older_than", |e| {
        expect_timestamp(e, "older_than")
    })?;
    expect_no_remaining(&args, "remove_orphan_files")?;

    Ok(ProcedureCall::RemoveOrphanFiles { table, older_than })
}

fn parse_rewrite_manifests(mut args: Vec<(String, Expr)>) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    expect_no_remaining(&args, "rewrite_manifests")?;
    Ok(ProcedureCall::RewriteManifests { table })
}

fn parse_suggest_bloom_filter_columns(
    mut args: Vec<(String, Expr)>,
) -> sqe_core::Result<ProcedureCall> {
    let table = take_table(&mut args)?;
    let history_limit = take_option(&mut args, "history_limit", |e| {
        expect_usize(e, "history_limit")
    })?;
    expect_no_remaining(&args, "suggest_bloom_filter_columns")?;
    Ok(ProcedureCall::SuggestBloomFilterColumns {
        table,
        history_limit,
    })
}

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
        let stmt =
            parse_first("CALL system.rewrite_data_files(table => 'ns.t', bogus_flag => 'x')");
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
        assert_eq!(call.table().unwrap().as_string(), "ns.t");
    }

    #[test]
    fn parses_purge_orphan_locations_defaults_dry_run_true() {
        let stmt = parse_first("CALL system.purge_orphan_locations(namespace => 'dev_silver')");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        assert_eq!(call.name(), "purge_orphan_locations");
        assert!(call.table().is_none());
        match call {
            ProcedureCall::PurgeOrphanLocations { namespace, dry_run } => {
                assert_eq!(namespace.as_string(), "dev_silver");
                assert!(dry_run, "dry_run must default to true");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn parses_purge_orphan_locations_with_dry_run_false() {
        let stmt = parse_first(
            "CALL system.purge_orphan_locations(namespace => 'main_warehouse.dev_silver', dry_run => false)",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::PurgeOrphanLocations { namespace, dry_run } => {
                assert_eq!(namespace.as_string(), "main_warehouse.dev_silver");
                assert_eq!(namespace.catalog.as_deref(), Some("main_warehouse"));
                assert_eq!(namespace.namespace, "dev_silver");
                assert!(!dry_run);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn purge_orphan_locations_rejects_unknown_arg() {
        let stmt =
            parse_first("CALL system.purge_orphan_locations(namespace => 'ns', force => true)");
        let err = try_parse_call(&stmt).expect_err("unknown arg should reject");
        assert!(
            err.to_string().contains("force"),
            "error must name the offending arg: {err}"
        );
    }

    #[test]
    fn purge_orphan_locations_requires_namespace() {
        let stmt = parse_first("CALL system.purge_orphan_locations(dry_run => true)");
        let err = try_parse_call(&stmt).expect_err("missing namespace should reject");
        assert!(err.to_string().contains("namespace"));
    }

    #[test]
    fn parses_suggest_bloom_filter_columns_bare() {
        let stmt = parse_first("CALL system.suggest_bloom_filter_columns(table => 'ns.t')");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::SuggestBloomFilterColumns {
                table,
                history_limit,
            } => {
                assert_eq!(table.as_string(), "ns.t");
                assert_eq!(history_limit, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_suggest_bloom_filter_columns_with_limit() {
        let stmt = parse_first(
            "CALL system.suggest_bloom_filter_columns(table => 'ns.t', history_limit => 500)",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::SuggestBloomFilterColumns { history_limit, .. } => {
                assert_eq!(history_limit, Some(500))
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // ── Catalog procedures: register_table / drop_table / snapshots ──────

    #[test]
    fn parses_register_table() {
        let stmt = parse_first(
            "CALL system.register_table(table => 'ns.t', metadata_location => 's3://bucket/ns/t/metadata/v1.metadata.json')",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::RegisterTable {
                table,
                metadata_location,
            } => {
                assert_eq!(table.namespace, "ns");
                assert_eq!(table.name, "t");
                assert_eq!(
                    metadata_location,
                    "s3://bucket/ns/t/metadata/v1.metadata.json"
                );
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn register_table_requires_metadata_location() {
        let stmt = parse_first("CALL system.register_table(table => 'ns.t')");
        let err = try_parse_call(&stmt).expect_err("missing metadata_location should reject");
        assert!(
            err.to_string().contains("metadata_location"),
            "error must name the missing arg: {err}"
        );
    }

    #[test]
    fn register_table_rejects_unknown_arg() {
        let stmt = parse_first(
            "CALL system.register_table(table => 'ns.t', metadata_location => 's3://x', \
             properties => 'foo')",
        );
        let err = try_parse_call(&stmt).expect_err("unknown arg should reject");
        assert!(err.to_string().contains("properties"));
    }

    #[test]
    fn parses_drop_table_defaults_purge_false() {
        let stmt = parse_first("CALL system.drop_table(table => 'ns.t')");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::DropTable { table, purge } => {
                assert_eq!(table.as_string(), "ns.t");
                assert!(!purge, "purge must default to false (safe)");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn parses_drop_table_with_purge_true() {
        let stmt = parse_first("CALL system.drop_table(table => 'ns.t', purge => true)");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::DropTable { purge, .. } => assert!(purge),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn parses_set_current_snapshot() {
        let stmt = parse_first(
            "CALL system.set_current_snapshot(table => 'ns.t', snapshot_id => 1234567890)",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::SetCurrentSnapshot { table, snapshot_id } => {
                assert_eq!(table.as_string(), "ns.t");
                assert_eq!(snapshot_id, 1_234_567_890);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn set_current_snapshot_requires_snapshot_id() {
        let stmt = parse_first("CALL system.set_current_snapshot(table => 'ns.t')");
        let err = try_parse_call(&stmt).expect_err("missing snapshot_id should reject");
        assert!(err.to_string().contains("snapshot_id"));
    }

    #[test]
    fn parses_rollback_to_snapshot() {
        let stmt = parse_first(
            "CALL system.rollback_to_snapshot(table => 'ns.t', snapshot_id => 9876543210)",
        );
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::RollbackToSnapshot { table, snapshot_id } => {
                assert_eq!(table.as_string(), "ns.t");
                assert_eq!(snapshot_id, 9_876_543_210);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn snapshot_id_accepts_negative() {
        // Iceberg snapshot ids are i64 and can be negative when generated
        // by hashing. The parser must accept the unary-minus form.
        let stmt =
            parse_first("CALL system.set_current_snapshot(table => 'ns.t', snapshot_id => -42)");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::SetCurrentSnapshot { snapshot_id, .. } => assert_eq!(snapshot_id, -42),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    fn call_err(sql: &str) -> String {
        let stmt = parse_first(sql);
        try_parse_call(&stmt)
            .err()
            .unwrap_or_else(|| panic!("expected error for {sql}"))
            .to_string()
    }

    #[test]
    fn positional_rollback_to_snapshot() {
        // Trino sends positional args (schema, table, snapshot_id). SQE folds
        // schema+table into its single `table` ref. See issue #316.
        let stmt = parse_first("CALL system.rollback_to_snapshot('default', 'orders', 9876543210)");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::RollbackToSnapshot { table, snapshot_id } => {
                assert_eq!(table.as_string(), "default.orders");
                assert_eq!(snapshot_id, 9_876_543_210);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn positional_rollback_null_args_match_trino_messages() {
        // testRollbackToSnapshotWithNullArgument asserts these exact phrases.
        assert!(call_err(
            "CALL system.rollback_to_snapshot(NULL, 'customer_orders', 8954597067493422955)"
        )
        .contains("schema cannot be null"));
        assert!(
            call_err("CALL system.rollback_to_snapshot('testdb', NULL, 8954597067493422955)")
                .contains("table cannot be null")
        );
        assert!(
            call_err("CALL system.rollback_to_snapshot('testdb', 'customer_orders', NULL)")
                .contains("snapshot_id cannot be null")
        );
    }

    #[test]
    fn mixed_named_and_positional_rejected() {
        let err = call_err("CALL system.rollback_to_snapshot('default', table => 'orders', 1)");
        assert!(
            err.contains("Named and positional arguments cannot be mixed"),
            "got: {err}"
        );
    }

    #[test]
    fn positional_unsupported_procedure_errors_clearly() {
        // A procedure without a verified Trino positional signature stays
        // named-only; the error must name the procedure and point at named args.
        let err = call_err("CALL system.drop_table('ns', 'orders')");
        assert!(err.contains("drop_table"), "got: {err}");
        assert!(err.contains("named arguments"), "got: {err}");
    }

    #[test]
    fn named_rollback_to_snapshot_still_works() {
        // Regression: the named form must keep working unchanged.
        let stmt =
            parse_first("CALL system.rollback_to_snapshot(table => 'ns.t', snapshot_id => 7)");
        let call = try_parse_call(&stmt).unwrap().expect("match");
        match call {
            ProcedureCall::RollbackToSnapshot { table, snapshot_id } => {
                assert_eq!(table.as_string(), "ns.t");
                assert_eq!(snapshot_id, 7);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn new_procedures_name_label_stable() {
        for (sql, expected) in [
            (
                "CALL system.register_table(table => 'ns.t', metadata_location => 's3://x')",
                "register_table",
            ),
            ("CALL system.drop_table(table => 'ns.t')", "drop_table"),
            (
                "CALL system.set_current_snapshot(table => 'ns.t', snapshot_id => 1)",
                "set_current_snapshot",
            ),
            (
                "CALL system.rollback_to_snapshot(table => 'ns.t', snapshot_id => 1)",
                "rollback_to_snapshot",
            ),
        ] {
            let stmt = parse_first(sql);
            let call = try_parse_call(&stmt).unwrap().expect("match");
            assert_eq!(call.name(), expected, "for {sql}");
            assert_eq!(call.table().unwrap().as_string(), "ns.t");
        }
    }
}
