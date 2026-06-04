use sqlparser::ast::{AlterTableOperation, ObjectType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::attach::{
    is_show_secrets, try_parse_attach, try_parse_create_secret, try_parse_detach,
    try_parse_drop_secret, AttachStatement, CreateSecretStatement, DetachStatement,
    DropSecretStatement,
};
use crate::ddl::{try_parse_ref_ddl, RefDdl};
use crate::partition_evolution::{try_parse_partition_evolution, PartitionEvolution};
use crate::procedures::{try_parse_call, ProcedureCall};

/// Target for SHOW GRANTS statements.
#[derive(Debug)]
pub enum ShowGrantsTarget {
    /// SHOW GRANTS ON [catalog.]namespace[.table]
    OnResource {
        catalog: Option<String>,
        namespace: Option<String>,
        table: Option<String>,
    },
    /// SHOW GRANTS TO ROLE/USER "name"
    ToGrantee {
        grantee_type: String,
        grantee_name: String,
    },
}

/// Parameters for CHECK ACCESS statements.
#[derive(Debug)]
pub struct CheckAccessParams {
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub user: String,
}

/// Classifies a parsed SQL statement into a high-level kind
/// for routing to the appropriate handler in the coordinator.
#[derive(Debug)]
pub enum StatementKind {
    Query(Box<Statement>),
    CreateTable(Box<Statement>),
    Ctas(Box<Statement>),
    Insert(Box<Statement>),
    Merge(Box<Statement>),
    Delete(Box<Statement>),
    Update(Box<Statement>),
    Drop(Box<Statement>),
    Rename(Box<Statement>),
    AlterSchema(Box<Statement>),
    CreateView(Box<Statement>),
    DropView(Box<Statement>),
    CreateSchema(Box<Statement>),
    DropSchema(Box<Statement>),
    ShowCatalogs,
    ShowSchemas(String),
    ShowTables(String),
    /// GRANT or DENY privilege statement
    Grant(Box<Statement>),
    /// REVOKE privilege statement
    Revoke(Box<Statement>),
    /// SHOW GRANTS ON resource / SHOW GRANTS TO grantee
    ShowGrants(ShowGrantsTarget),
    /// SHOW EFFECTIVE GRANTS FOR USER "name"
    ShowEffectiveGrants(String),
    /// CHECK ACCESS privilege ON resource FOR USER "name"
    CheckAccess(CheckAccessParams),
    Utility(Box<Statement>),
    ExplainFull(String), // inner SQL string (EXPLAIN FULL pre-processed)
    // Transaction stubs — no-ops for JDBC tools that use setAutoCommit(false).
    Begin,
    Commit,
    Rollback,
    /// USE catalog.schema — switch default catalog/schema for session
    Use(String),
    /// SHOW CREATE TABLE name — reconstruct DDL from metadata
    ShowCreateTable(Box<Statement>),
    /// TRUNCATE TABLE name — routes to DELETE FROM without WHERE
    Truncate(String),
    /// CALL procedure — not supported, returns informative error
    Call(Box<Statement>),
    /// CALL system.<maintenance procedure>(...) matched against the
    /// Iceberg maintenance procedure registry.
    Procedure(Box<ProcedureCall>),
    /// ALTER TABLE ... SET TBLPROPERTIES (...) — update Iceberg table properties
    AlterTableProps(Box<Statement>),
    /// COMMENT ON TABLE/COLUMN — store comment as Iceberg table property
    Comment(Box<Statement>),
    /// SHOW STATS FOR table — return row/file/size stats from snapshot summary
    ShowStats(String),
    /// ALTER TABLE ... CREATE/DROP BRANCH/TAG — branching and tagging DDL
    RefDdl(Box<RefDdl>),
    /// SET WRITE_BRANCH = 'name' — route writes in this session to a named branch.
    /// SET WRITE_BRANCH = DEFAULT (or unquoted) resets to main.
    SetWriteBranch(Option<String>),
    /// ALTER TABLE ... ADD/DROP/REPLACE PARTITION FIELD — partition spec evolution.
    /// sqlparser-rs has no AST for transform-based partition fields, so this
    /// variant carries the pre-parsed `PartitionEvolution` and routes through
    /// a dedicated coordinator handler.
    PartitionEvolution(Box<PartitionEvolution>),
    /// `ATTACH '<location>' AS <name> (TYPE <kind>, ...)` — register a new
    /// Iceberg catalog at runtime. sqlparser-rs has no native AST for the
    /// SQE/DuckDB option list, so this variant carries the pre-parsed
    /// `AttachStatement` and routes through a dedicated coordinator handler.
    Attach(Box<AttachStatement>),
    /// `DETACH <name>` — unregister a runtime-attached catalog.
    Detach(Box<DetachStatement>),
    /// `CREATE SECRET <name> (TYPE <kind>, ...)` — store credentials in the
    /// process-global in-memory secret store.
    CreateSecret(Box<CreateSecretStatement>),
    /// `DROP SECRET <name>` — remove a secret. Errors if any attached catalog
    /// still references it.
    DropSecret(Box<DropSecretStatement>),
    /// `SHOW SECRETS` — list secret names and their kinds (never the values).
    ShowSecrets,
}

impl StatementKind {
    /// Return a stable lowercase label for metrics and audit logging.
    pub fn name(&self) -> &'static str {
        match self {
            StatementKind::Query(_) => "query",
            StatementKind::CreateTable(_) => "createtable",
            StatementKind::Ctas(_) => "ctas",
            StatementKind::Insert(_) => "insert",
            StatementKind::Merge(_) => "merge",
            StatementKind::Delete(_) => "delete",
            StatementKind::Update(_) => "update",
            StatementKind::Drop(_) => "drop",
            StatementKind::Rename(_) => "rename",
            StatementKind::AlterSchema(_) => "alterschema",
            StatementKind::CreateView(_) => "createview",
            StatementKind::DropView(_) => "dropview",
            StatementKind::CreateSchema(_) => "createschema",
            StatementKind::DropSchema(_) => "dropschema",
            StatementKind::ShowCatalogs => "showcatalogs",
            StatementKind::ShowSchemas(_) => "showschemas",
            StatementKind::ShowTables(_) => "showtables",
            StatementKind::Grant(_) => "grant",
            StatementKind::Revoke(_) => "revoke",
            StatementKind::ShowGrants(_) => "showgrants",
            StatementKind::ShowEffectiveGrants(_) => "showeffectivegrants",
            StatementKind::CheckAccess(_) => "checkaccess",
            StatementKind::Utility(_) => "utility",
            StatementKind::ExplainFull(_) => "explain_full",
            StatementKind::Begin => "begin",
            StatementKind::Commit => "commit",
            StatementKind::Rollback => "rollback",
            StatementKind::Use(_) => "use",
            StatementKind::ShowCreateTable(_) => "showcreatetable",
            StatementKind::Truncate(_) => "truncate",
            StatementKind::Call(_) => "call",
            StatementKind::Procedure(_) => "procedure",
            StatementKind::AlterTableProps(_) => "altertableprops",
            StatementKind::Comment(_) => "comment",
            StatementKind::ShowStats(_) => "showstats",
            StatementKind::RefDdl(_) => "refddl",
            StatementKind::SetWriteBranch(_) => "setwritebranch",
            StatementKind::PartitionEvolution(_) => "partitionevolution",
            StatementKind::Attach(_) => "attach",
            StatementKind::Detach(_) => "detach",
            StatementKind::CreateSecret(_) => "create_secret",
            StatementKind::DropSecret(_) => "drop_secret",
            StatementKind::ShowSecrets => "show_secrets",
        }
    }

    /// Borrow the underlying parsed `sqlparser::ast::Statement` when
    /// this kind wraps one. Returns `None` for variants that are
    /// pre-parsed into custom shapes (RefDdl, PartitionEvolution,
    /// Procedure, ExplainFull, ShowSchemas, ShowTables, ShowStats,
    /// ShowGrants, SetWriteBranch, Use, Truncate, Begin/Commit/Rollback).
    ///
    /// Used by the coordinator to walk the AST for cross-cutting
    /// validations (e.g. unknown catalog qualifier in 3-part names)
    /// without enumerating every variant by hand.
    pub fn statement(&self) -> Option<&Statement> {
        match self {
            StatementKind::Query(s)
            | StatementKind::CreateTable(s)
            | StatementKind::Ctas(s)
            | StatementKind::Insert(s)
            | StatementKind::Merge(s)
            | StatementKind::Delete(s)
            | StatementKind::Update(s)
            | StatementKind::Drop(s)
            | StatementKind::Rename(s)
            | StatementKind::AlterSchema(s)
            | StatementKind::CreateView(s)
            | StatementKind::DropView(s)
            | StatementKind::CreateSchema(s)
            | StatementKind::DropSchema(s)
            | StatementKind::Grant(s)
            | StatementKind::Revoke(s)
            | StatementKind::Utility(s)
            | StatementKind::ShowCreateTable(s)
            | StatementKind::Call(s)
            | StatementKind::AlterTableProps(s)
            | StatementKind::Comment(s) => Some(s.as_ref()),
            _ => None,
        }
    }
}

/// Parse a `ClassifiableSql` and classify the first statement. Prefer
/// this over `parse_and_classify` at the trust boundary: the type bound
/// proves the input ran through the pre-parse pipeline (issue #117).
pub fn parse_and_classify_typed(
    sql: &crate::pipeline_types::ClassifiableSql,
) -> sqe_core::Result<StatementKind> {
    parse_and_classify(sql.as_str())
}

/// Strip a case-insensitive ASCII `prefix` from `s`, returning the remainder.
///
/// The previous pre-scan tested `prefix` against `s.to_uppercase()` but sliced
/// the ORIGINAL string by the prefix's byte length. `to_uppercase()` can change
/// a string's byte length for some Unicode code points, so the matched-prefix
/// length on the uppercased copy could differ from the byte span in the
/// original and slice on a non-char-boundary (panic). Matching the prefix and
/// slicing against the SAME string removes that mismatch entirely. All the
/// keyword prefixes here are pure ASCII, so a byte-wise case-insensitive
/// comparison is correct.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let bytes = s.as_bytes();
    let pfx = prefix.as_bytes();
    if bytes.len() >= pfx.len() && bytes[..pfx.len()].eq_ignore_ascii_case(pfx) {
        // `prefix` is ASCII, so its byte length is a valid char boundary in `s`.
        s.get(pfx.len()..)
    } else {
        None
    }
}

/// Parse a SQL string and classify the first statement.
pub fn parse_and_classify(sql: &str) -> sqe_core::Result<StatementKind> {
    // Before parsing with sqlparser, check for SHOW CATALOGS which sqlparser
    // may not natively support.
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if upper == "SHOW CATALOGS" || upper.starts_with("SHOW CATALOGS ") {
        return Ok(StatementKind::ShowCatalogs);
    }

    // Pre-scan for EXPLAIN FULL — not standard SQL, sqlparser won't parse it.
    if let Some(rest) = strip_prefix_ci(trimmed, "EXPLAIN FULL ") {
        let inner = rest.trim().to_string();
        return Ok(StatementKind::ExplainFull(inner));
    }

    // Pre-scan for SHOW STATS FOR — sqlparser parses this as ShowVariable,
    // but we intercept it here for direct table name extraction.
    if let Some(rest) = strip_prefix_ci(trimmed, "SHOW STATS FOR ") {
        let table = rest.trim().trim_end_matches(';').to_string();
        return Ok(StatementKind::ShowStats(table));
    }

    // Pre-scan for SHOW EFFECTIVE GRANTS FOR USER "name"
    if let Some(rest) = strip_prefix_ci(trimmed, "SHOW EFFECTIVE GRANTS FOR USER ") {
        let user = rest
            .trim()
            .trim_end_matches(';')
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        return Ok(StatementKind::ShowEffectiveGrants(user));
    }

    // Pre-scan for SHOW GRANTS ON resource / SHOW GRANTS TO type "name"
    if let Some(rest) = strip_prefix_ci(trimmed, "SHOW GRANTS ON ") {
        let rest = rest.trim().trim_end_matches(';').to_string();
        let (catalog, namespace, table) = parse_resource_reference(&rest)?;
        return Ok(StatementKind::ShowGrants(ShowGrantsTarget::OnResource {
            catalog,
            namespace,
            table,
        }));
    }
    if let Some(rest) = strip_prefix_ci(trimmed, "SHOW GRANTS TO ") {
        let rest = rest.trim().trim_end_matches(';');
        let parts: Vec<&str> = rest.splitn(2, char::is_whitespace).collect();
        if parts.len() == 2 {
            let grantee_type = parts[0].to_uppercase();
            let grantee_name = parts[1]
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
            return Ok(StatementKind::ShowGrants(ShowGrantsTarget::ToGrantee {
                grantee_type,
                grantee_name,
            }));
        }
        return Err(sqe_core::SqeError::Execution(
            "SHOW GRANTS TO requires: SHOW GRANTS TO GROUP|USER \"name\"".to_string(),
        ));
    }

    // Pre-scan for CHECK ACCESS privilege ON resource FOR USER "name"
    if let Some(rest) = strip_prefix_ci(trimmed, "CHECK ACCESS ") {
        let rest = rest.trim().trim_end_matches(';');
        return parse_check_access(rest);
    }

    // Pre-scan for ALTER TABLE ... CREATE/DROP BRANCH|TAG. These are not part of
    // standard SQL and sqlparser-rs will either reject them or classify them as
    // generic AlterTable statements, losing the branch/tag semantics.
    if upper.starts_with("ALTER TABLE ") {
        if let Some(ref_ddl) = try_parse_ref_ddl(trimmed)? {
            return Ok(StatementKind::RefDdl(Box::new(ref_ddl)));
        }
        // Pre-scan for ALTER TABLE ... ADD/DROP/REPLACE PARTITION FIELD. The
        // transform expression form is non-standard and sqlparser-rs models
        // only Hive-style PARTITION (col=val), so we intercept here.
        if let Some(pe) = try_parse_partition_evolution(trimmed)? {
            return Ok(StatementKind::PartitionEvolution(Box::new(pe)));
        }
    }

    // ATTACH/DETACH/CREATE SECRET/DROP SECRET/SHOW SECRETS: SQE extensions
    // that sqlparser-rs does not understand. Match these before falling
    // through to sqlparser so the user sees a precise diagnostic if they
    // get the option-list shape wrong, and so SHOW SECRETS doesn't get
    // mis-classified as a generic SHOW variable.
    if is_show_secrets(trimmed) {
        return Ok(StatementKind::ShowSecrets);
    }
    if upper.starts_with("ATTACH ") {
        if let Some(stmt) = try_parse_attach(trimmed)? {
            return Ok(StatementKind::Attach(Box::new(stmt)));
        }
        // Fell through: legacy SQLite `ATTACH '<file>' AS <name>` shape, or a
        // malformed input. Hand to sqlparser; it will produce
        // `Statement::AttachDatabase` which the classifier currently has no
        // arm for, so the existing NotImplemented error surfaces (preserving
        // old behaviour for any SQL that worked before this change).
    }
    if upper.starts_with("DETACH ") || upper == "DETACH" {
        if let Some(stmt) = try_parse_detach(trimmed)? {
            return Ok(StatementKind::Detach(Box::new(stmt)));
        }
    }
    if upper.starts_with("CREATE SECRET ") {
        if let Some(stmt) = try_parse_create_secret(trimmed)? {
            return Ok(StatementKind::CreateSecret(Box::new(stmt)));
        }
    }
    if upper.starts_with("DROP SECRET ") {
        if let Some(stmt) = try_parse_drop_secret(trimmed)? {
            return Ok(StatementKind::DropSecret(Box::new(stmt)));
        }
    }

    // SET WRITE_BRANCH = '<name>' routes writes to a named Iceberg branch.
    // We intercept it here so the coordinator can update session state
    // without going through DataFusion's generic SET handling.
    if let Some(rest) = strip_prefix_ci(trimmed, "SET WRITE_BRANCH") {
        let rest = rest.trim().trim_end_matches(';').trim();
        // Accept: SET WRITE_BRANCH = 'name', SET WRITE_BRANCH 'name',
        //         SET WRITE_BRANCH = DEFAULT, SET WRITE_BRANCH = NULL
        let stripped = rest.strip_prefix('=').unwrap_or(rest).trim();
        let upper_val = stripped.to_uppercase();
        if upper_val == "DEFAULT" || upper_val == "NULL" || stripped.is_empty() {
            return Ok(StatementKind::SetWriteBranch(None));
        }
        let name = stripped
            .trim_start_matches('\'')
            .trim_end_matches('\'')
            .trim_start_matches('"')
            .trim_end_matches('"')
            .to_string();
        return Ok(StatementKind::SetWriteBranch(Some(name)));
    }

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| sqe_core::SqeError::Execution(format!("Parse error: {e}")))?;

    // Reject pathologically deep expression trees up front, before any
    // recursive AST visitor (the Trino-compat rewrite, then DataFusion's
    // analyzer) walks them and overflows the stack. A flat `a OR a OR ...`
    // chain parses cleanly but builds a depth-N tree; the guard turns that
    // into a clean parse error instead of an uncatchable process abort.
    crate::trino_compat::check_expression_depth(&statements)
        .map_err(|e| sqe_core::SqeError::Execution(format!("Parse error: {e}")))?;

    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| sqe_core::SqeError::Execution("Empty SQL".to_string()))?;

    classify(stmt)
}

fn classify(stmt: Statement) -> sqe_core::Result<StatementKind> {
    match stmt {
        // SELECT / WITH ... SELECT
        Statement::Query(_) => Ok(StatementKind::Query(Box::new(stmt))),

        // CREATE TABLE ... AS SELECT (CTAS) vs regular CREATE TABLE (columns)
        Statement::CreateTable(ref ct) => {
            if ct.query.is_some() {
                Ok(StatementKind::Ctas(Box::new(stmt)))
            } else {
                Ok(StatementKind::CreateTable(Box::new(stmt)))
            }
        }

        // INSERT INTO
        Statement::Insert(_) => Ok(StatementKind::Insert(Box::new(stmt))),

        // MERGE INTO
        Statement::Merge { .. } => Ok(StatementKind::Merge(Box::new(stmt))),

        // DELETE FROM
        Statement::Delete(_) => Ok(StatementKind::Delete(Box::new(stmt))),

        // DROP TABLE / DROP VIEW / DROP SCHEMA / DROP other
        Statement::Drop {
            object_type: ObjectType::View,
            ..
        } => Ok(StatementKind::DropView(Box::new(stmt))),

        Statement::Drop {
            object_type: ObjectType::Table,
            ..
        } => Ok(StatementKind::Drop(Box::new(stmt))),

        Statement::Drop {
            object_type: ObjectType::Schema,
            ..
        } => Ok(StatementKind::DropSchema(Box::new(stmt))),

        // For other DROP types (index, etc.), treat as utility
        Statement::Drop { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // ALTER TABLE — check for RENAME operations
        Statement::AlterTable {
            ref operations, ..
        } => {
            let is_rename = operations.iter().any(|op| {
                matches!(op, AlterTableOperation::RenameTable { .. })
            });
            let is_schema_change = operations.iter().any(|op| {
                matches!(
                    op,
                    AlterTableOperation::AddColumn { .. }
                        | AlterTableOperation::DropColumn { .. }
                        | AlterTableOperation::RenameColumn { .. }
                        | AlterTableOperation::AlterColumn { .. }
                )
            });
            let is_set_properties = operations.iter().any(|op| {
                matches!(op, AlterTableOperation::SetTblProperties { .. })
            });
            if is_rename {
                Ok(StatementKind::Rename(Box::new(stmt)))
            } else if is_schema_change {
                Ok(StatementKind::AlterSchema(Box::new(stmt)))
            } else if is_set_properties {
                Ok(StatementKind::AlterTableProps(Box::new(stmt)))
            } else {
                Ok(StatementKind::Utility(Box::new(stmt)))
            }
        }

        // CREATE VIEW
        Statement::CreateView { .. } => Ok(StatementKind::CreateView(Box::new(stmt))),

        // CREATE SCHEMA
        Statement::CreateSchema { .. } => Ok(StatementKind::CreateSchema(Box::new(stmt))),

        // GRANT → dedicated variant for access control
        Statement::Grant { .. } => Ok(StatementKind::Grant(Box::new(stmt))),

        // REVOKE → dedicated variant for access control
        Statement::Revoke { .. } => Ok(StatementKind::Revoke(Box::new(stmt))),

        // EXPLAIN → Utility
        Statement::Explain { .. } => Ok(StatementKind::Utility(Box::new(stmt))),
        Statement::ExplainTable { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // SET → Utility (sqlparser 0.53 uses separate variants per SET flavour)
        Statement::SetVariable { .. }
        | Statement::SetTimeZone { .. }
        | Statement::SetNames { .. }
        | Statement::SetNamesDefault { .. }
        | Statement::SetTransaction { .. }
        | Statement::SetRole { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // SHOW SCHEMAS — sqlparser has a ShowSchemas variant
        Statement::ShowSchemas { ref show_options, .. } => {
            let filter = show_options
                .show_in
                .as_ref()
                .map(|si| si.to_string())
                .unwrap_or_default();
            Ok(StatementKind::ShowSchemas(filter))
        }

        // SHOW TABLES — sqlparser has a ShowTables variant
        Statement::ShowTables { ref show_options, .. } => {
            let filter = show_options
                .show_in
                .as_ref()
                .map(|si| si.to_string())
                .unwrap_or_default();
            Ok(StatementKind::ShowTables(filter))
        }

        // SHOW DATABASES — treat like ShowCatalogs
        Statement::ShowDatabases { .. } => Ok(StatementKind::ShowCatalogs),

        // SHOW <variable> — could be "SHOW CATALOGS" parsed as variable
        Statement::ShowVariable { ref variable } => {
            let var_str: String = variable
                .iter()
                .map(|i| i.value.to_uppercase())
                .collect::<Vec<_>>()
                .join(" ");
            if var_str == "CATALOGS" {
                Ok(StatementKind::ShowCatalogs)
            } else if var_str.starts_with("SCHEMAS") {
                let rest = var_str.strip_prefix("SCHEMAS").unwrap_or("").trim().to_string();
                Ok(StatementKind::ShowSchemas(rest))
            } else if var_str.starts_with("TABLES") {
                let rest = var_str.strip_prefix("TABLES").unwrap_or("").trim().to_string();
                Ok(StatementKind::ShowTables(rest))
            } else {
                Ok(StatementKind::Utility(Box::new(stmt)))
            }
        }

        // SHOW CREATE TABLE — reconstruct DDL from metadata
        Statement::ShowCreate { .. } => Ok(StatementKind::ShowCreateTable(Box::new(stmt))),

        // Other SHOW variants → Utility
        Statement::ShowFunctions { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCollation { .. }
        | Statement::ShowViews { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // UPDATE → dedicated variant for routing to the write handler
        Statement::Update { .. } => Ok(StatementKind::Update(Box::new(stmt))),

        // Transaction stubs — no-ops so JDBC tools can call setAutoCommit(false)
        Statement::StartTransaction { .. } => Ok(StatementKind::Begin),
        Statement::Commit { .. } => Ok(StatementKind::Commit),
        Statement::Rollback { .. } => Ok(StatementKind::Rollback),

        // USE catalog.schema — session context switching
        Statement::Use(ref use_stmt) => {
            let target = use_stmt.to_string();
            // Strip the "USE " prefix that Display adds
            let target = target.strip_prefix("USE ").unwrap_or(&target).to_string();
            Ok(StatementKind::Use(target))
        }

        // TRUNCATE TABLE — routes to DELETE FROM without WHERE
        Statement::Truncate { ref table_names, .. } => {
            let name = table_names
                .first()
                .map(|t| t.name.to_string())
                .unwrap_or_default();
            Ok(StatementKind::Truncate(name))
        }

        // CALL procedure — dispatch to Iceberg maintenance procedures when the
        // name is `system.<known>`. Unknown calls fall through to the generic
        // `Call(_)` variant so the existing "not supported" error triggers.
        Statement::Call(_) => match try_parse_call(&stmt)? {
            Some(proc) => Ok(StatementKind::Procedure(Box::new(proc))),
            None => Ok(StatementKind::Call(Box::new(stmt))),
        },

        // COMMENT ON TABLE/COLUMN — store as Iceberg table property
        Statement::Comment { .. } => Ok(StatementKind::Comment(Box::new(stmt))),

        _ => Err(sqe_core::SqeError::NotImplemented(format!(
            "Statement type not supported: {stmt}"
        ))),
    }
}

/// Parse a dotted resource reference like `catalog.namespace.table` into
/// (catalog, namespace, table). Supports 1-part, 2-part, and 3-part names.
/// Returns an error for 4+ part names (e.g. `a.b.c.d`).
fn parse_resource_reference(
    s: &str,
) -> sqe_core::Result<(Option<String>, Option<String>, Option<String>)> {
    let parts: Vec<&str> = s.split('.').map(|p| p.trim()).collect();
    match parts.len() {
        1 => Ok((Some(parts[0].to_string()), None, None)),
        2 => Ok((Some(parts[0].to_string()), Some(parts[1].to_string()), None)),
        3 => Ok((
            Some(parts[0].to_string()),
            Some(parts[1].to_string()),
            Some(parts[2].to_string()),
        )),
        n => Err(sqe_core::SqeError::Execution(format!(
            "Resource reference has {n} parts (max 3): {s}"
        ))),
    }
}

/// Parse `CHECK ACCESS privilege ON resource FOR USER "name"`.
fn parse_check_access(rest: &str) -> sqe_core::Result<StatementKind> {
    let upper = rest.to_uppercase();
    // Find " ON " to split privilege from the rest
    let on_pos = upper
        .find(" ON ")
        .ok_or_else(|| sqe_core::SqeError::Execution(
            "CHECK ACCESS requires: CHECK ACCESS <privilege> ON <resource> FOR USER \"<name>\"".to_string(),
        ))?;
    let privilege = rest[..on_pos].trim().to_string();
    let after_on = rest[on_pos + 4..].trim();

    // Find " FOR USER " to split resource from user
    let after_on_upper = after_on.to_uppercase();
    let for_user_pos = after_on_upper
        .find(" FOR USER ")
        .ok_or_else(|| sqe_core::SqeError::Execution(
            "CHECK ACCESS requires: CHECK ACCESS <privilege> ON <resource> FOR USER \"<name>\"".to_string(),
        ))?;
    let resource = after_on[..for_user_pos].trim();
    let user = after_on[for_user_pos + " FOR USER ".len()..]
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();

    let (catalog, namespace, table) = parse_resource_reference(resource)?;
    Ok(StatementKind::CheckAccess(CheckAccessParams {
        privilege,
        catalog,
        namespace,
        table,
        user,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_is_query() {
        let result = parse_and_classify("SELECT 1");
        assert!(matches!(result, Ok(StatementKind::Query(_))));
    }

    // ── SQL-08: prefix pre-scan slices the matched string, not a copy ───

    #[test]
    fn strip_prefix_ci_matches_case_insensitively() {
        assert_eq!(
            strip_prefix_ci("show stats for ns.t", "SHOW STATS FOR "),
            Some("ns.t")
        );
        assert_eq!(
            strip_prefix_ci("SHOW STATS FOR ns.t", "SHOW STATS FOR "),
            Some("ns.t")
        );
        assert_eq!(strip_prefix_ci("SELECT 1", "SHOW STATS FOR "), None);
    }

    #[test]
    fn strip_prefix_ci_does_not_panic_on_non_ascii_remainder() {
        // The remainder after the ASCII prefix can contain multi-byte chars;
        // slicing must land on a char boundary (the prefix is ASCII, so its
        // byte length is always a boundary). No panic.
        let s = "SHOW STATS FOR ☃ns";
        assert_eq!(strip_prefix_ci(s, "SHOW STATS FOR "), Some("☃ns"));
    }

    #[test]
    fn show_effective_grants_parses_with_lowercase_keyword() {
        // Previously the match was on the uppercased copy but the slice came
        // from the original; a lowercase input still classifies correctly now.
        let result = parse_and_classify("show effective grants for user \"alice\"");
        assert!(matches!(
            result,
            Ok(StatementKind::ShowEffectiveGrants(ref u)) if u == "alice"
        ));
    }

    // ── SQL-01: deep expression chains rejected with a clean error ──────

    #[test]
    fn classifier_rejects_deep_expression_chain() {
        // 2000 OR-terms is far above the depth cap (256) and far below the
        // ~16k stack-overflow threshold, so the guard rejects it cleanly.
        let chain = std::iter::repeat("a")
            .take(2000)
            .collect::<Vec<_>>()
            .join(" OR ");
        let sql = format!("SELECT {chain} FROM t");
        let result = parse_and_classify(&sql);
        assert!(result.is_err(), "deep chain must be rejected, not overflow");
    }

    #[test]
    fn classifier_accepts_normal_query() {
        let result = parse_and_classify("SELECT a OR b OR c FROM t WHERE x > 1");
        assert!(matches!(result, Ok(StatementKind::Query(_))));
    }

    #[test]
    fn test_select_with_cte_is_query() {
        let result = parse_and_classify("WITH cte AS (SELECT 1) SELECT * FROM cte");
        assert!(matches!(result, Ok(StatementKind::Query(_))));
    }

    #[test]
    fn test_ctas() {
        let result = parse_and_classify("CREATE TABLE foo AS SELECT 1");
        assert!(matches!(result, Ok(StatementKind::Ctas(_))));
    }

    #[test]
    fn test_create_or_replace_table_as_select() {
        let result = parse_and_classify("CREATE OR REPLACE TABLE foo AS SELECT 1");
        assert!(matches!(result, Ok(StatementKind::Ctas(_))));
    }

    #[test]
    fn test_create_table_without_query_is_create_table() {
        let result = parse_and_classify("CREATE TABLE foo (id INT)");
        assert!(matches!(result, Ok(StatementKind::CreateTable(_))));
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let result = parse_and_classify(
            "CREATE TABLE IF NOT EXISTS ns.table (id BIGINT, name VARCHAR)",
        );
        assert!(matches!(result, Ok(StatementKind::CreateTable(_))));
    }

    #[test]
    fn test_insert() {
        let result = parse_and_classify("INSERT INTO foo SELECT 1");
        assert!(matches!(result, Ok(StatementKind::Insert(_))));
    }

    #[test]
    fn test_delete() {
        let result = parse_and_classify("DELETE FROM foo WHERE id = 1");
        assert!(matches!(result, Ok(StatementKind::Delete(_))));
    }

    #[test]
    fn classify_update() {
        let result = parse_and_classify("UPDATE ns.t SET col1 = 1 WHERE id = 5").unwrap();
        assert!(matches!(result, StatementKind::Update(_)));
    }

    #[test]
    fn classify_update_no_where() {
        let result = parse_and_classify("UPDATE ns.t SET val = val + 1").unwrap();
        assert!(matches!(result, StatementKind::Update(_)));
    }

    #[test]
    fn classify_update_name() {
        let kind = StatementKind::Update(Box::new(
            Parser::parse_sql(&GenericDialect {}, "UPDATE t SET x = 1")
                .unwrap()
                .remove(0),
        ));
        assert_eq!(kind.name(), "update");
    }

    #[test]
    fn test_drop_table() {
        let result = parse_and_classify("DROP TABLE foo");
        assert!(matches!(result, Ok(StatementKind::Drop(_))));
    }

    #[test]
    fn test_drop_table_if_exists() {
        let result = parse_and_classify("DROP TABLE IF EXISTS foo");
        assert!(matches!(result, Ok(StatementKind::Drop(_))));
    }

    #[test]
    fn test_create_view() {
        let result = parse_and_classify("CREATE VIEW v AS SELECT 1");
        assert!(matches!(result, Ok(StatementKind::CreateView(_))));
    }

    #[test]
    fn test_drop_view() {
        let result = parse_and_classify("DROP VIEW v");
        assert!(matches!(result, Ok(StatementKind::DropView(_))));
    }

    #[test]
    fn test_grant_is_grant() {
        let result = parse_and_classify("GRANT SELECT ON foo TO bar");
        assert!(matches!(result, Ok(StatementKind::Grant(_))));
    }

    #[test]
    fn test_revoke_is_revoke() {
        let result = parse_and_classify("REVOKE SELECT ON foo FROM bar");
        assert!(matches!(result, Ok(StatementKind::Revoke(_))));
    }

    #[test]
    fn test_show_grants_on_resource() {
        let result = parse_and_classify("SHOW GRANTS ON my_catalog.my_ns.my_table");
        assert!(
            matches!(result, Ok(StatementKind::ShowGrants(ShowGrantsTarget::OnResource { .. }))),
            "Expected ShowGrants OnResource, got: {result:?}"
        );
    }

    #[test]
    fn test_show_grants_to_group() {
        let result = parse_and_classify("SHOW GRANTS TO GROUP \"SG-Risk-Analysts\"");
        assert!(result.is_ok());
        match result.unwrap() {
            StatementKind::ShowGrants(ShowGrantsTarget::ToGrantee { grantee_type, grantee_name }) => {
                assert_eq!(grantee_type, "GROUP");
                assert_eq!(grantee_name, "SG-Risk-Analysts");
            }
            other => panic!("Expected ShowGrants(ToGrantee), got {other:?}"),
        }
    }

    #[test]
    fn test_show_grants_to_role() {
        let result = parse_and_classify("SHOW GRANTS TO ROLE \"admin\"");
        assert!(
            matches!(result, Ok(StatementKind::ShowGrants(ShowGrantsTarget::ToGrantee { .. }))),
            "Expected ShowGrants ToGrantee, got: {result:?}"
        );
    }

    #[test]
    fn test_show_effective_grants() {
        let result = parse_and_classify("SHOW EFFECTIVE GRANTS FOR USER \"alice\"");
        assert!(
            matches!(result, Ok(StatementKind::ShowEffectiveGrants(_))),
            "Expected ShowEffectiveGrants, got: {result:?}"
        );
    }

    #[test]
    fn test_show_effective_grants_extracts_user() {
        let result = parse_and_classify("SHOW EFFECTIVE GRANTS FOR USER \"alice\"").unwrap();
        if let StatementKind::ShowEffectiveGrants(user) = result {
            assert_eq!(user, "alice");
        } else {
            panic!("Expected ShowEffectiveGrants");
        }
    }

    #[test]
    fn test_check_access() {
        let result = parse_and_classify("CHECK ACCESS SELECT ON my_catalog.my_ns.orders FOR USER \"alice\"");
        assert!(
            matches!(result, Ok(StatementKind::CheckAccess(_))),
            "Expected CheckAccess, got: {result:?}"
        );
    }

    #[test]
    fn test_check_access_extracts_params() {
        let result = parse_and_classify("CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"bob\"").unwrap();
        if let StatementKind::CheckAccess(params) = result {
            assert_eq!(params.privilege, "SELECT");
            assert_eq!(params.catalog.as_deref(), Some("cat"));
            assert_eq!(params.namespace.as_deref(), Some("ns"));
            assert_eq!(params.table.as_deref(), Some("tbl"));
            assert_eq!(params.user, "bob");
        } else {
            panic!("Expected CheckAccess");
        }
    }

    #[test]
    fn test_grant_name() {
        let stmt = Parser::parse_sql(&GenericDialect {}, "GRANT SELECT ON foo TO bar")
            .unwrap()
            .remove(0);
        let kind = StatementKind::Grant(Box::new(stmt));
        assert_eq!(kind.name(), "grant");
    }

    #[test]
    fn test_revoke_name() {
        let stmt = Parser::parse_sql(&GenericDialect {}, "REVOKE SELECT ON foo FROM bar")
            .unwrap()
            .remove(0);
        let kind = StatementKind::Revoke(Box::new(stmt));
        assert_eq!(kind.name(), "revoke");
    }

    #[test]
    fn test_show_grants_name() {
        let kind = StatementKind::ShowGrants(ShowGrantsTarget::OnResource {
            catalog: None,
            namespace: None,
            table: None,
        });
        assert_eq!(kind.name(), "showgrants");
    }

    #[test]
    fn test_show_effective_grants_name() {
        let kind = StatementKind::ShowEffectiveGrants("alice".to_string());
        assert_eq!(kind.name(), "showeffectivegrants");
    }

    #[test]
    fn test_check_access_name() {
        let kind = StatementKind::CheckAccess(CheckAccessParams {
            privilege: "SELECT".to_string(),
            catalog: None,
            namespace: None,
            table: None,
            user: "alice".to_string(),
        });
        assert_eq!(kind.name(), "checkaccess");
    }

    #[test]
    fn test_show_grants_on_four_part_name_errors() {
        let result = parse_and_classify("SHOW GRANTS ON a.b.c.d");
        assert!(result.is_err(), "4-part resource name should be rejected");
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("4 parts"),
            "Error should mention part count, got: {err}"
        );
    }

    #[test]
    fn test_check_access_four_part_name_errors() {
        let result =
            parse_and_classify("CHECK ACCESS SELECT ON a.b.c.d FOR USER \"alice\"");
        assert!(result.is_err(), "4-part resource name should be rejected");
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("4 parts"),
            "Error should mention part count, got: {err}"
        );
    }

    #[test]
    fn test_explain_is_utility() {
        let result = parse_and_classify("EXPLAIN SELECT 1");
        assert!(matches!(result, Ok(StatementKind::Utility(_))));
    }

    #[test]
    fn test_set_is_utility() {
        let result = parse_and_classify("SET search_path = public");
        assert!(matches!(result, Ok(StatementKind::Utility(_))));
    }

    #[test]
    fn test_show_catalogs() {
        let result = parse_and_classify("SHOW CATALOGS");
        assert!(matches!(result, Ok(StatementKind::ShowCatalogs)));
    }

    #[test]
    fn test_show_schemas() {
        let result = parse_and_classify("SHOW SCHEMAS");
        assert!(
            matches!(result, Ok(StatementKind::ShowSchemas(_))),
            "Expected ShowSchemas, got: {result:?}",
        );
    }

    #[test]
    fn test_show_tables() {
        let result = parse_and_classify("SHOW TABLES");
        assert!(
            matches!(result, Ok(StatementKind::ShowTables(_))),
            "Expected ShowTables, got: {result:?}",
        );
    }

    #[test]
    fn test_merge() {
        let result = parse_and_classify(
            "MERGE INTO target USING source ON target.id = source.id \
             WHEN MATCHED THEN UPDATE SET target.val = source.val",
        );
        assert!(matches!(result, Ok(StatementKind::Merge(_))));
    }

    #[test]
    fn test_alter_table_rename_is_rename() {
        let result = parse_and_classify("ALTER TABLE foo RENAME TO bar");
        assert!(matches!(result, Ok(StatementKind::Rename(_))));
    }

    #[test]
    fn test_alter_table_add_column_is_alter_schema() {
        let result = parse_and_classify("ALTER TABLE foo ADD COLUMN bar INT");
        assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
    }

    #[test]
    fn test_alter_table_rename_column_is_alter_schema() {
        let result = parse_and_classify("ALTER TABLE foo RENAME COLUMN old_col TO new_col");
        assert!(
            matches!(result, Ok(StatementKind::AlterSchema(_))),
            "RENAME COLUMN should route to AlterSchema, not Rename: {result:?}"
        );
    }

    #[test]
    fn test_alter_table_drop_column_is_alter_schema() {
        let result = parse_and_classify("ALTER TABLE foo DROP COLUMN bar");
        assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
    }

    #[test]
    fn test_alter_table_alter_column_set_not_null() {
        let result = parse_and_classify("ALTER TABLE foo ALTER COLUMN bar SET NOT NULL");
        assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
    }

    #[test]
    fn test_alter_table_alter_column_drop_not_null() {
        let result = parse_and_classify("ALTER TABLE foo ALTER COLUMN bar DROP NOT NULL");
        assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
    }

    #[test]
    fn test_alter_table_alter_column_set_data_type() {
        let result = parse_and_classify("ALTER TABLE foo ALTER COLUMN bar SET DATA TYPE BIGINT");
        assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
    }

    #[test]
    fn test_alter_table_rename_still_works() {
        let result = parse_and_classify("ALTER TABLE foo RENAME TO bar");
        assert!(matches!(result, Ok(StatementKind::Rename(_))));
    }

    #[test]
    fn test_alter_schema_name() {
        let kind = parse_and_classify("ALTER TABLE foo ADD COLUMN bar INT").unwrap();
        assert_eq!(kind.name(), "alterschema");
    }

    #[test]
    fn test_create_schema() {
        let result = parse_and_classify("CREATE SCHEMA my_schema");
        assert!(matches!(result, Ok(StatementKind::CreateSchema(_))));
    }

    #[test]
    fn test_create_schema_if_not_exists() {
        let result = parse_and_classify("CREATE SCHEMA IF NOT EXISTS my_schema");
        assert!(matches!(result, Ok(StatementKind::CreateSchema(_))));
    }

    #[test]
    fn test_drop_schema() {
        let result = parse_and_classify("DROP SCHEMA my_schema");
        assert!(matches!(result, Ok(StatementKind::DropSchema(_))));
    }

    #[test]
    fn test_drop_schema_if_exists() {
        let result = parse_and_classify("DROP SCHEMA IF EXISTS my_schema");
        assert!(matches!(result, Ok(StatementKind::DropSchema(_))));
    }

    #[test]
    fn test_show_databases_is_show_catalogs() {
        let result = parse_and_classify("SHOW DATABASES");
        assert!(matches!(result, Ok(StatementKind::ShowCatalogs)));
    }

    // ── Transaction stub tests ─────────────────────────────────────────────

    #[test]
    fn test_begin_is_begin() {
        let result = parse_and_classify("BEGIN");
        assert!(
            matches!(result, Ok(StatementKind::Begin)),
            "Expected Begin, got: {result:?}"
        );
    }

    #[test]
    fn test_start_transaction_is_begin() {
        let result = parse_and_classify("START TRANSACTION");
        assert!(
            matches!(result, Ok(StatementKind::Begin)),
            "Expected Begin, got: {result:?}"
        );
    }

    #[test]
    fn test_commit_is_commit() {
        let result = parse_and_classify("COMMIT");
        assert!(
            matches!(result, Ok(StatementKind::Commit)),
            "Expected Commit, got: {result:?}"
        );
    }

    #[test]
    fn test_rollback_is_rollback() {
        let result = parse_and_classify("ROLLBACK");
        assert!(
            matches!(result, Ok(StatementKind::Rollback)),
            "Expected Rollback, got: {result:?}"
        );
    }

    #[test]
    fn test_begin_name() {
        assert_eq!(StatementKind::Begin.name(), "begin");
    }

    #[test]
    fn test_commit_name() {
        assert_eq!(StatementKind::Commit.name(), "commit");
    }

    #[test]
    fn test_rollback_name() {
        assert_eq!(StatementKind::Rollback.name(), "rollback");
    }

    #[test]
    fn test_empty_sql_is_error() {
        let result = parse_and_classify("");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_sql_is_error() {
        let result = parse_and_classify("NOT VALID SQL AT ALL %%$#@");
        assert!(result.is_err());
    }

    #[test]
    fn test_explain_analyze_is_utility() {
        let result = parse_and_classify("EXPLAIN ANALYZE SELECT 1");
        assert!(matches!(result, Ok(StatementKind::Utility(_))));
    }

    #[test]
    fn test_explain_full_is_explain_full() {
        let result = parse_and_classify("EXPLAIN FULL SELECT 1");
        assert!(
            matches!(result, Ok(StatementKind::ExplainFull(_))),
            "Expected ExplainFull, got: {result:?}"
        );
    }

    #[test]
    fn test_explain_full_lowercase() {
        let result = parse_and_classify("explain full SELECT 1");
        assert!(matches!(result, Ok(StatementKind::ExplainFull(_))));
    }

    #[test]
    fn test_explain_full_extracts_inner_sql() {
        let result = parse_and_classify("EXPLAIN FULL SELECT 1 AS n").unwrap();
        if let StatementKind::ExplainFull(inner) = result {
            assert_eq!(inner, "SELECT 1 AS n");
        } else {
            panic!("Expected ExplainFull");
        }
    }

    #[test]
    fn test_explain_full_name() {
        let kind = StatementKind::ExplainFull("SELECT 1".to_string());
        assert_eq!(kind.name(), "explain_full");
    }

    // ── USE / ShowCreateTable / Truncate / Call tests ──────────────────────

    #[test]
    fn test_use_catalog_schema() {
        let result = parse_and_classify("USE my_catalog.my_schema");
        assert!(
            matches!(result, Ok(StatementKind::Use(_))),
            "Expected Use, got: {result:?}"
        );
    }

    #[test]
    fn test_use_schema_only() {
        let result = parse_and_classify("USE my_schema");
        assert!(
            matches!(result, Ok(StatementKind::Use(_))),
            "Expected Use, got: {result:?}"
        );
    }

    #[test]
    fn test_use_name() {
        let kind = StatementKind::Use("catalog.schema".to_string());
        assert_eq!(kind.name(), "use");
    }

    #[test]
    fn test_show_create_table() {
        let result = parse_and_classify("SHOW CREATE TABLE my_schema.my_table");
        assert!(
            matches!(result, Ok(StatementKind::ShowCreateTable(_))),
            "Expected ShowCreateTable, got: {result:?}"
        );
    }

    #[test]
    fn test_show_create_table_name() {
        let stmt = Parser::parse_sql(&GenericDialect {}, "SHOW CREATE TABLE foo")
            .unwrap()
            .remove(0);
        let kind = StatementKind::ShowCreateTable(Box::new(stmt));
        assert_eq!(kind.name(), "showcreatetable");
    }

    #[test]
    fn test_truncate_table() {
        let result = parse_and_classify("TRUNCATE TABLE my_schema.my_table");
        assert!(
            matches!(result, Ok(StatementKind::Truncate(_))),
            "Expected Truncate, got: {result:?}"
        );
    }

    #[test]
    fn test_truncate_table_name_extracted() {
        let result = parse_and_classify("TRUNCATE TABLE orders").unwrap();
        if let StatementKind::Truncate(name) = result {
            assert_eq!(name, "orders");
        } else {
            panic!("Expected Truncate");
        }
    }

    #[test]
    fn test_truncate_name() {
        let kind = StatementKind::Truncate("orders".to_string());
        assert_eq!(kind.name(), "truncate");
    }

    #[test]
    fn test_call_is_call() {
        let result = parse_and_classify("CALL my_procedure()");
        assert!(
            matches!(result, Ok(StatementKind::Call(_))),
            "Expected Call, got: {result:?}"
        );
    }

    #[test]
    fn test_call_name() {
        let stmt = Parser::parse_sql(&GenericDialect {}, "CALL foo()")
            .unwrap()
            .remove(0);
        let kind = StatementKind::Call(Box::new(stmt));
        assert_eq!(kind.name(), "call");
    }

    #[test]
    fn test_call_system_rewrite_data_files_is_procedure() {
        let result = parse_and_classify("CALL system.rewrite_data_files(table => 'ns.t')");
        assert!(
            matches!(result, Ok(StatementKind::Procedure(_))),
            "Expected Procedure, got: {result:?}"
        );
    }

    #[test]
    fn test_call_system_unknown_is_plain_call() {
        let result = parse_and_classify("CALL system.unknown_proc(table => 'ns.t')");
        assert!(
            matches!(result, Ok(StatementKind::Call(_))),
            "Expected Call (unknown system.*), got: {result:?}"
        );
    }

    #[test]
    fn test_procedure_name() {
        use crate::procedures::{ProcedureCall, TableRef};
        let kind = StatementKind::Procedure(Box::new(ProcedureCall::RewriteManifests {
            table: TableRef::parse("ns.t").unwrap(),
        }));
        assert_eq!(kind.name(), "procedure");
    }

    #[test]
    fn test_create_or_replace_view_is_create_view() {
        let result = parse_and_classify("CREATE OR REPLACE VIEW v AS SELECT 1");
        assert!(
            matches!(result, Ok(StatementKind::CreateView(_))),
            "Expected CreateView for CREATE OR REPLACE VIEW, got: {result:?}"
        );
    }

    #[test]
    fn test_alter_table_set_tblproperties_is_alter_table_props() {
        let result = parse_and_classify(
            "ALTER TABLE my_table SET TBLPROPERTIES ('write.format.default' = 'parquet')",
        );
        assert!(
            matches!(result, Ok(StatementKind::AlterTableProps(_))),
            "Expected AlterTableProps for SET TBLPROPERTIES, got: {result:?}"
        );
    }

    #[test]
    fn test_alter_table_props_name() {
        let stmt = Parser::parse_sql(
            &GenericDialect {},
            "ALTER TABLE t SET TBLPROPERTIES ('k' = 'v')",
        )
        .unwrap()
        .remove(0);
        let kind = StatementKind::AlterTableProps(Box::new(stmt));
        assert_eq!(kind.name(), "altertableprops");
    }

    // ── COMMENT ON tests ──────────────────────────────────────────────────────

    #[test]
    fn test_comment_on_table() {
        let result = parse_and_classify("COMMENT ON TABLE my_schema.my_table IS 'A description'");
        assert!(
            matches!(result, Ok(StatementKind::Comment(_))),
            "Expected Comment, got: {result:?}"
        );
    }

    #[test]
    fn test_comment_on_column() {
        let result = parse_and_classify("COMMENT ON COLUMN my_table.my_col IS 'Col description'");
        assert!(
            matches!(result, Ok(StatementKind::Comment(_))),
            "Expected Comment, got: {result:?}"
        );
    }

    #[test]
    fn test_comment_on_table_null() {
        // IS NULL removes an existing comment
        let result = parse_and_classify("COMMENT ON TABLE my_table IS NULL");
        assert!(
            matches!(result, Ok(StatementKind::Comment(_))),
            "Expected Comment, got: {result:?}"
        );
    }

    #[test]
    fn test_comment_name() {
        let stmt = Parser::parse_sql(
            &GenericDialect {},
            "COMMENT ON TABLE t IS 'desc'",
        )
        .unwrap()
        .remove(0);
        let kind = StatementKind::Comment(Box::new(stmt));
        assert_eq!(kind.name(), "comment");
    }

    // ── SHOW STATS FOR tests ──────────────────────────────────────────────────

    #[test]
    fn test_show_stats_for() {
        let result = parse_and_classify("SHOW STATS FOR orders");
        assert!(
            matches!(result, Ok(StatementKind::ShowStats(_))),
            "Expected ShowStats, got: {result:?}"
        );
    }

    #[test]
    fn test_show_stats_for_qualified() {
        let result = parse_and_classify("SHOW STATS FOR my_schema.orders");
        assert!(
            matches!(result, Ok(StatementKind::ShowStats(_))),
            "Expected ShowStats, got: {result:?}"
        );
    }

    #[test]
    fn test_show_stats_extracts_table_name() {
        let result = parse_and_classify("SHOW STATS FOR orders").unwrap();
        if let StatementKind::ShowStats(name) = result {
            assert_eq!(name, "orders");
        } else {
            panic!("Expected ShowStats");
        }
    }

    #[test]
    fn test_show_stats_name() {
        let kind = StatementKind::ShowStats("orders".to_string());
        assert_eq!(kind.name(), "showstats");
    }

    #[test]
    fn test_show_stats_case_insensitive() {
        let result = parse_and_classify("show stats for orders");
        assert!(
            matches!(result, Ok(StatementKind::ShowStats(_))),
            "Expected ShowStats, got: {result:?}"
        );
    }

    // ── Branch/Tag DDL tests ──────────────────────────────────────────────

    #[test]
    fn test_create_branch_classified_as_ref_ddl() {
        let kind = parse_and_classify("ALTER TABLE t CREATE BRANCH feature_x").unwrap();
        match kind {
            StatementKind::RefDdl(b) => match *b {
                RefDdl::CreateBranch { name, .. } => assert_eq!(name, "feature_x"),
                other => panic!("expected CreateBranch, got {other:?}"),
            },
            other => panic!("expected RefDdl, got: {other:?}"),
        }
    }

    #[test]
    fn test_create_tag_classified_as_ref_ddl() {
        let kind = parse_and_classify("ALTER TABLE t CREATE TAG v1").unwrap();
        assert!(matches!(kind, StatementKind::RefDdl(_)));
    }

    #[test]
    fn test_drop_branch_classified_as_ref_ddl() {
        let kind = parse_and_classify("ALTER TABLE t DROP BRANCH stale").unwrap();
        assert!(matches!(kind, StatementKind::RefDdl(_)));
    }

    #[test]
    fn test_drop_tag_if_exists_classified_as_ref_ddl() {
        let kind = parse_and_classify("ALTER TABLE t DROP TAG v1 IF EXISTS").unwrap();
        assert!(matches!(kind, StatementKind::RefDdl(_)));
    }

    #[test]
    fn test_alter_table_add_column_still_alter_schema() {
        // Regression: branch pre-scan must not steal regular ALTER TABLE.
        let kind = parse_and_classify("ALTER TABLE t ADD COLUMN x INT").unwrap();
        assert!(matches!(kind, StatementKind::AlterSchema(_)));
    }

    #[test]
    fn test_ref_ddl_name() {
        let kind = parse_and_classify("ALTER TABLE t CREATE BRANCH b").unwrap();
        assert_eq!(kind.name(), "refddl");
    }

    // ── SET WRITE_BRANCH tests ────────────────────────────────────────────

    #[test]
    fn test_set_write_branch_quoted() {
        let kind = parse_and_classify("SET WRITE_BRANCH = 'feature_x'").unwrap();
        match kind {
            StatementKind::SetWriteBranch(Some(name)) => assert_eq!(name, "feature_x"),
            other => panic!("expected SetWriteBranch, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_write_branch_without_equals() {
        let kind = parse_and_classify("SET WRITE_BRANCH 'trunk'").unwrap();
        match kind {
            StatementKind::SetWriteBranch(Some(name)) => assert_eq!(name, "trunk"),
            other => panic!("expected SetWriteBranch, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_write_branch_default_clears() {
        let kind = parse_and_classify("SET WRITE_BRANCH = DEFAULT").unwrap();
        assert!(matches!(kind, StatementKind::SetWriteBranch(None)));
    }

    #[test]
    fn test_set_write_branch_null_clears() {
        let kind = parse_and_classify("SET WRITE_BRANCH = NULL").unwrap();
        assert!(matches!(kind, StatementKind::SetWriteBranch(None)));
    }

    #[test]
    fn test_set_write_branch_name() {
        let kind = parse_and_classify("SET WRITE_BRANCH = 'x'").unwrap();
        assert_eq!(kind.name(), "setwritebranch");
    }

    // ── PARTITION FIELD evolution tests ───────────────────────────────────

    #[test]
    fn classify_add_partition_field() {
        let kind =
            parse_and_classify("ALTER TABLE ns.t ADD PARTITION FIELD year(ts)").unwrap();
        match kind {
            StatementKind::PartitionEvolution(b) => match *b {
                PartitionEvolution::AddField { table, transform_sql } => {
                    assert_eq!(table, "ns.t");
                    assert_eq!(transform_sql, "year(ts)");
                }
                other => panic!("expected AddField, got {other:?}"),
            },
            other => panic!("expected PartitionEvolution, got {other:?}"),
        }
    }

    #[test]
    fn classify_drop_partition_field() {
        let kind =
            parse_and_classify("ALTER TABLE ns.t DROP PARTITION FIELD region").unwrap();
        assert!(matches!(
            kind,
            StatementKind::PartitionEvolution(b)
                if matches!(*b, PartitionEvolution::DropField { .. })
        ));
    }

    #[test]
    fn classify_replace_partition_field() {
        let kind = parse_and_classify(
            "ALTER TABLE ns.t REPLACE PARTITION FIELD region WITH bucket(8, region)",
        )
        .unwrap();
        assert!(matches!(
            kind,
            StatementKind::PartitionEvolution(b)
                if matches!(*b, PartitionEvolution::ReplaceField { .. })
        ));
    }

    #[test]
    fn classify_partition_evolution_name() {
        let kind = parse_and_classify("ALTER TABLE t ADD PARTITION FIELD region").unwrap();
        assert_eq!(kind.name(), "partitionevolution");
    }

    #[test]
    fn alter_add_column_still_alter_schema_after_partition_pre_scan() {
        // Regression: the partition pre-scan must not steal regular ALTER TABLE.
        let kind = parse_and_classify("ALTER TABLE t ADD COLUMN x INT").unwrap();
        assert!(matches!(kind, StatementKind::AlterSchema(_)));
    }
}
