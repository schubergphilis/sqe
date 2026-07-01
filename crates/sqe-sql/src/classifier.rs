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
use crate::tags::{try_parse_set_tags, SetTagsStatement};

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

/// Parameters for `SHOW EFFECTIVE POLICY [FOR USER <u>] ON <table>`.
///
/// `user` is `None` for the self form (`SHOW EFFECTIVE POLICY ON t`); the
/// coordinator substitutes the session user. The `FOR USER u` form is gated
/// `require_self_or_admin` exactly like SHOW EFFECTIVE GRANTS. `table` is the
/// raw (possibly dotted/quoted) reference; the handler parses it into a
/// `(namespace, table)` policy key using the same scheme as the plan rewriter.
#[derive(Debug)]
pub struct ShowEffectivePolicyParams {
    pub user: Option<String>,
    pub table: String,
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
    /// SHOW EFFECTIVE POLICY [FOR USER "name"] ON <table> — introspect the
    /// row filters, column masks, and restrictions that the policy resolver
    /// would apply for (user, table). Diagnostic read path (issue
    /// no-show-effective-policy-or-tags).
    ShowEffectivePolicy(ShowEffectivePolicyParams),
    /// SHOW TAGS ON <table> — read back the `sqe.column-tags` property as
    /// (column, tag) rows. Diagnostic round-trip for ALTER TABLE ... SET TAGS.
    ShowTags(String),
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
    /// SHOW CREATE SCHEMA name — reconstruct namespace DDL from metadata.
    /// sqlparser 0.62 rejects the SCHEMA form outright (it only models
    /// SHOW CREATE TABLE/VIEW/...), so this is detected by prefix before the
    /// parser runs. Carries the raw (possibly dotted/quoted) schema reference.
    /// (#351a)
    ShowCreateSchema(String),
    /// TRUNCATE TABLE name — routes to DELETE FROM without WHERE
    Truncate(String),
    /// ANALYZE [catalog.][schema.]table [WITH (...)] — Trino table-statistics
    /// command. Carries the raw (possibly dotted/quoted) table reference; the
    /// handler resolves it against the catalog (so a missing table errors) and
    /// treats stats collection as a no-op (#329).
    Analyze(String),
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
    /// `ALTER TABLE ... SET TAGS / UNSET TAGS` and the Snowflake-compatible
    /// `MODIFY|ALTER COLUMN ... SET TAG / UNSET TAG` column-tag authoring DDL.
    SetTags(Box<SetTagsStatement>),
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
            StatementKind::ShowEffectivePolicy(_) => "showeffectivepolicy",
            StatementKind::ShowTags(_) => "showtags",
            StatementKind::Utility(_) => "utility",
            StatementKind::ExplainFull(_) => "explain_full",
            StatementKind::Begin => "begin",
            StatementKind::Commit => "commit",
            StatementKind::Rollback => "rollback",
            StatementKind::Use(_) => "use",
            StatementKind::ShowCreateTable(_) => "showcreatetable",
            StatementKind::ShowCreateSchema(_) => "showcreateschema",
            StatementKind::Truncate(_) => "truncate",
            StatementKind::Analyze(_) => "analyze",
            StatementKind::Call(_) => "call",
            StatementKind::Procedure(_) => "procedure",
            StatementKind::AlterTableProps(_) => "altertableprops",
            StatementKind::Comment(_) => "comment",
            StatementKind::ShowStats(_) => "showstats",
            StatementKind::RefDdl(_) => "refddl",
            StatementKind::SetWriteBranch(_) => "setwritebranch",
            StatementKind::PartitionEvolution(_) => "partitionevolution",
            StatementKind::SetTags(_) => "settags",
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

/// Detect the Trino/SQL `ALTER TABLE ... DROP COLUMN [IF EXISTS] <a.b...>`
/// dotted-path form (a nested struct-subfield drop) and return the dotted path.
/// Returns `None` for a plain (non-dotted) `DROP COLUMN`, which sqlparser
/// parses natively. Matching is tolerant of case and whitespace; the path is
/// the token sequence after `DROP COLUMN [IF EXISTS]` up to the next clause
/// separator (comma, whitespace-then-clause, or end). Only fires when a `.`
/// appears inside that path. (#336)
fn detect_nested_drop_column(trimmed: &str) -> Option<String> {
    let upper = trimmed.to_uppercase();
    // Find the `DROP COLUMN` keyword pair (byte offset in the uppercased copy,
    // which lines up with the original since the prefix is ASCII).
    let idx = upper.find("DROP COLUMN")?;
    let after = &trimmed[idx + "DROP COLUMN".len()..];
    let after = after.trim_start();
    // Skip an optional `IF EXISTS`.
    let after = strip_prefix_ci(after, "IF EXISTS")
        .map(|r| r.trim_start())
        .unwrap_or(after);
    // The column reference runs until a delimiter that ends it: whitespace,
    // comma, or the statement terminator. A dotted path uses `.` internally.
    let end = after
        .find(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .unwrap_or(after.len());
    let path = after[..end].trim();
    if path.contains('.') && !path.is_empty() {
        Some(path.to_string())
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

    // Pre-scan for SHOW CREATE SCHEMA <name>. sqlparser 0.62 only models
    // SHOW CREATE for TABLE/VIEW/... and rejects the SCHEMA form at parse time,
    // so it never reaches the AST match below. Intercept it here and carry the
    // raw schema reference for the handler to resolve. (#351a). SHOW CREATE
    // TABLE / VIEW still parse normally and flow through the AST match.
    if let Some(rest) = strip_prefix_ci(trimmed, "SHOW CREATE SCHEMA ") {
        let schema = rest.trim().trim_end_matches(';').trim().to_string();
        if schema.is_empty() {
            return Err(sqe_core::SqeError::Execution(
                "SHOW CREATE SCHEMA requires a schema name".into(),
            ));
        }
        return Ok(StatementKind::ShowCreateSchema(schema));
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

    // Pre-scan for ANALYZE [TABLE] <table> [WITH (...)]. sqlparser's
    // `parse_analyze` stops at the `WITH` keyword, so Trino's
    // `ANALYZE t WITH (...)` form leaves trailing tokens and fails to parse.
    // Intercept the whole statement here, extract the table reference, and
    // drop any WITH/PARTITION/FOR-COLUMNS clause: SQE treats ANALYZE as a
    // stats no-op (#329), so the analyze properties do not affect behaviour.
    if let Some(rest) = strip_prefix_ci(trimmed, "ANALYZE ") {
        return parse_analyze(rest);
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

    // Pre-scan for SHOW EFFECTIVE POLICY [FOR USER "name"] ON <table>.
    // Check the FOR USER form before the bare form so the longer prefix wins.
    if let Some(rest) = strip_prefix_ci(trimmed, "SHOW EFFECTIVE POLICY FOR USER ") {
        let rest = rest.trim().trim_end_matches(';');
        return parse_show_effective_policy(rest, true);
    }
    if let Some(rest) = strip_prefix_ci(trimmed, "SHOW EFFECTIVE POLICY ") {
        let rest = rest.trim().trim_end_matches(';');
        return parse_show_effective_policy(rest, false);
    }

    // Pre-scan for SHOW TAGS ON <table>. sqlparser would mis-classify "SHOW
    // TAGS" as a generic SHOW variable, so intercept it here. The bare
    // `SHOW TAGS ON` (no table) is also matched so it yields a precise error
    // rather than falling through to sqlparser.
    let show_tags_rest = strip_prefix_ci(trimmed, "SHOW TAGS ON ").or_else(|| {
        if trimmed.trim_end_matches(';').trim_end().eq_ignore_ascii_case("SHOW TAGS ON") {
            Some("")
        } else {
            None
        }
    });
    if let Some(rest) = show_tags_rest {
        let table = rest.trim().trim_end_matches(';').trim().to_string();
        if table.is_empty() {
            return Err(sqe_core::SqeError::Execution(
                "SHOW TAGS requires: SHOW TAGS ON <table>".to_string(),
            ));
        }
        return Ok(StatementKind::ShowTags(table));
    }

    // Pre-scan for ALTER TABLE ... CREATE/DROP BRANCH|TAG. These are not part of
    // standard SQL and sqlparser-rs will either reject them or classify them as
    // generic AlterTable statements, losing the branch/tag semantics.
    if upper.starts_with("ALTER TABLE ") {
        // Trino spells Iceberg table-property updates
        // `ALTER TABLE t SET PROPERTIES k = v [, ...]` (no parentheses, no
        // TBLPROPERTIES keyword). sqlparser only accepts the parenthesized
        // `SET TBLPROPERTIES (k = v, ...)` form, so rewrite the Trino form and
        // re-classify; the existing AlterTableProps handler then commits them
        // as Iceberg SetProperties (#338).
        if let Some(rewritten) = rewrite_set_properties(trimmed) {
            return parse_and_classify(&rewritten);
        }
        if let Some(ref_ddl) = try_parse_ref_ddl(trimmed)? {
            return Ok(StatementKind::RefDdl(Box::new(ref_ddl)));
        }
        // Pre-scan for ALTER TABLE ... ADD/DROP/REPLACE PARTITION FIELD. The
        // transform expression form is non-standard and sqlparser-rs models
        // only Hive-style PARTITION (col=val), so we intercept here.
        if let Some(pe) = try_parse_partition_evolution(trimmed)? {
            return Ok(StatementKind::PartitionEvolution(Box::new(pe)));
        }
        // ALTER TABLE ... SET TAGS / UNSET TAGS / MODIFY|ALTER COLUMN ... SET TAG.
        // Column-tag authoring; distinct from Iceberg snapshot CREATE/DROP TAG above.
        if let Some(set_tags) = try_parse_set_tags(trimmed)? {
            return Ok(StatementKind::SetTags(Box::new(set_tags)));
        }
        // ALTER TABLE t DROP COLUMN a.b — a dotted path drops a nested struct
        // subfield. sqlparser 0.62 rejects the `.` with a baffling
        // `Expected: end of statement, found: .`. Intercept the dotted form and
        // surface a clear NotImplemented rather than the parser noise, so a
        // client sees an actionable message. Nested Iceberg schema surgery
        // (removing a subfield from a struct) is not yet implemented. A
        // non-dotted DROP COLUMN still flows to sqlparser and the normal
        // AlterSchema handler. (#336)
        if let Some(path) = detect_nested_drop_column(trimmed) {
            return Err(sqe_core::SqeError::NotImplemented(format!(
                "dropping a nested column ('{path}') is not yet supported; \
                 only top-level columns can be dropped"
            )));
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

    // Trino's `CREATE TABLE new (LIKE src)` copies a table's schema. sqlparser's
    // GenericDialect does not support the parenthesized LIKE form (it parses the
    // body as a column named `LIKE` of type `src`), so rewrite the pure form to
    // the plain `CREATE TABLE new LIKE src`, which sqlparser records in
    // `CreateTable.like`. The coordinator's create handler copies the source
    // schema (#343).
    if let Some(rewritten) = rewrite_create_table_like(trimmed) {
        return parse_and_classify(&rewritten);
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
        Statement::AlterTable(ref alter) => {
            let operations = &alter.operations;
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

        // SET → Utility (sqlparser 0.62 consolidates every SET flavour
        // (SetVariable / SetTimeZone / SetNames / SetNamesDefault /
        // SetTransaction / SetRole) into a single Statement::Set(Set) variant)
        Statement::Set(_) => Ok(StatementKind::Utility(Box::new(stmt))),

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
        Statement::Truncate(ref truncate) => {
            let name = truncate
                .table_names
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

/// Rewrite Trino's `ALTER TABLE t SET PROPERTIES k = v [, ...]` (no
/// parentheses, no `TBLPROPERTIES` keyword) into the parenthesized
/// `ALTER TABLE t SET TBLPROPERTIES (k = v, ...)` form that sqlparser parses
/// into `AlterTableOperation::SetTblProperties`. Returns `None` when the
/// statement is not a bare `SET PROPERTIES` (so `SET TBLPROPERTIES (...)` and
/// every other ALTER TABLE flavour fall through untouched). See #338.
fn rewrite_set_properties(trimmed: &str) -> Option<String> {
    // Match ` SET PROPERTIES ` case-insensitively on the byte-equal uppercased
    // copy (the statement is ASCII up to this keyword). The leading space plus
    // the literal `PROPERTIES` (not `TBLPROPERTIES`) means the already-valid
    // `SET TBLPROPERTIES` form never matches here, so re-classification of the
    // rewritten string does not loop.
    const NEEDLE: &str = " SET PROPERTIES ";
    let idx = trimmed.to_ascii_uppercase().find(NEEDLE)?;
    let head = &trimmed[..idx];
    let body = trimmed[idx + NEEDLE.len()..]
        .trim()
        .trim_end_matches(';')
        .trim();
    if body.is_empty() {
        return None;
    }
    // Split into `key = value` pairs on top-level commas and double-quote each
    // key. Iceberg property names are dotted/hyphenated (e.g.
    // `commit.retry.num-retries`) and do not parse as bare identifiers; quoting
    // them as delimited identifiers makes sqlparser accept the list. Values are
    // kept verbatim (they may themselves contain commas inside `ARRAY[...]`,
    // which the top-level split preserves).
    let mut pairs: Vec<String> = Vec::new();
    for segment in split_top_level(body, ',') {
        let seg = segment.trim();
        if seg.is_empty() {
            return None;
        }
        let eq_parts = split_top_level(seg, '=');
        if eq_parts.len() < 2 {
            return None;
        }
        let key = eq_parts[0].trim().trim_matches('"').trim_matches('\'');
        let value = eq_parts[1..].join("=");
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            return None;
        }
        let key_esc = key.replace('"', "\"\"");
        pairs.push(format!("\"{key_esc}\" = {value}"));
    }
    if pairs.is_empty() {
        return None;
    }
    Some(format!("{head} SET TBLPROPERTIES ({})", pairs.join(", ")))
}

/// Split `s` on top-level occurrences of `delim` -- occurrences at bracket
/// depth zero and outside single/double quotes. Used to break a
/// `key = value, key = value` property list without tripping on commas or `=`
/// nested inside `ARRAY[...]`, `(...)`, or quoted string values.
fn split_top_level(s: &str, delim: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut in_single = false;
    let mut in_double = false;
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '(' | '[' if !in_single && !in_double => depth += 1,
            ')' | ']' if !in_single && !in_double => depth -= 1,
            _ if c == delim && depth == 0 && !in_single && !in_double => {
                out.push(s[start..i].to_string());
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    out.push(s[start..].to_string());
    out
}

/// Rewrite Trino's parenthesized `CREATE TABLE new (LIKE src)` into the plain
/// `CREATE TABLE new LIKE src` form that sqlparser's GenericDialect records in
/// `CreateTable.like`. Only the pure LIKE-only body is rewritten: a mixed
/// `(LIKE src, extra_col ...)` list would silently drop the extra columns, so
/// it is declined (returns `None`) and surfaces as a normal error. Any
/// `INCLUDING`/`EXCLUDING PROPERTIES` suffix is dropped (SQE copies the schema
/// only). See #343.
fn rewrite_create_table_like(trimmed: &str) -> Option<String> {
    if !trimmed.to_ascii_uppercase().starts_with("CREATE TABLE ") {
        return None;
    }
    // The parenthesized body begins at the first `(`.
    let lparen = trimmed.find('(')?;
    let inner = trimmed[lparen + 1..].trim_start();
    let after_like = strip_prefix_ci(inner, "LIKE ")?;
    // Reject a mixed column list: only the pure `(LIKE src ...)` form is safe.
    let close = after_like.find(')')?;
    let clause = &after_like[..close];
    if clause.contains(',') {
        return None;
    }
    let src = clause.split_whitespace().next()?;
    if src.is_empty() {
        return None;
    }
    let head = trimmed[..lparen].trim_end();
    Some(format!("{head} LIKE {src}"))
}

/// Parse the remainder of an `ANALYZE ...` statement (everything after the
/// `ANALYZE ` prefix) into [`StatementKind::Analyze`] carrying the table
/// reference. Accepts the optional `TABLE` keyword and ignores any trailing
/// `WITH (...)`, `PARTITION (...)`, or `FOR COLUMNS ...` clause: the table
/// name ends at the first whitespace or `(`. The reference is kept raw
/// (possibly dotted) and resolved by the coordinator handler.
fn parse_analyze(rest: &str) -> sqe_core::Result<StatementKind> {
    let rest = rest.trim().trim_end_matches(';').trim();
    // Optional `TABLE` keyword (Hive-style `ANALYZE TABLE t`).
    let rest = strip_prefix_ci(rest, "TABLE ").map(str::trim).unwrap_or(rest);
    let table = rest
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .trim();
    if table.is_empty() {
        return Err(sqe_core::SqeError::Execution(
            "ANALYZE requires a table name: ANALYZE [catalog.][schema.]table [WITH (...)]"
                .to_string(),
        ));
    }
    Ok(StatementKind::Analyze(table.to_string()))
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

/// Parse the remainder of a `SHOW EFFECTIVE POLICY ...` statement.
///
/// When `has_user` is true the input is `"<name>" ON <table>` (the
/// `FOR USER ` prefix was already stripped); otherwise it is just `ON <table>`
/// and the user defaults to the session user (`None`).
fn parse_show_effective_policy(rest: &str, has_user: bool) -> sqe_core::Result<StatementKind> {
    let bad_syntax = || {
        sqe_core::SqeError::Execution(
            "SHOW EFFECTIVE POLICY requires: SHOW EFFECTIVE POLICY [FOR USER \"<name>\"] ON <table>"
                .to_string(),
        )
    };

    let (user, table) = if has_user {
        // Input is `<name> ON <table>` (the `FOR USER ` prefix was stripped).
        let upper = rest.to_uppercase();
        let on_pos = upper.find(" ON ").ok_or_else(bad_syntax)?;
        let user = rest[..on_pos]
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if user.is_empty() {
            return Err(sqe_core::SqeError::Execution(
                "SHOW EFFECTIVE POLICY FOR USER requires a non-empty user name".to_string(),
            ));
        }
        (Some(user), rest[on_pos + 4..].trim().to_string())
    } else {
        // Input is `ON <table>` (the bare-form remainder starts with `ON`).
        let table = strip_prefix_ci(rest, "ON ").ok_or_else(bad_syntax)?;
        (None, table.trim().to_string())
    };

    if table.is_empty() {
        return Err(sqe_core::SqeError::Execution(
            "SHOW EFFECTIVE POLICY requires a table after ON".to_string(),
        ));
    }

    Ok(StatementKind::ShowEffectivePolicy(ShowEffectivePolicyParams {
        user,
        table,
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
        let chain = std::iter::repeat_n("a", 2000)
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

    // ── SHOW EFFECTIVE POLICY ───────────────────────────────────────────

    #[test]
    fn test_show_effective_policy_self_form() {
        let result = parse_and_classify("SHOW EFFECTIVE POLICY ON cat.ns.orders").unwrap();
        match result {
            StatementKind::ShowEffectivePolicy(p) => {
                assert_eq!(p.user, None, "self form has no explicit user");
                assert_eq!(p.table, "cat.ns.orders");
            }
            other => panic!("Expected ShowEffectivePolicy, got {other:?}"),
        }
    }

    #[test]
    fn test_show_effective_policy_for_user_form() {
        let result =
            parse_and_classify("SHOW EFFECTIVE POLICY FOR USER \"alice\" ON ns.orders").unwrap();
        match result {
            StatementKind::ShowEffectivePolicy(p) => {
                assert_eq!(p.user.as_deref(), Some("alice"));
                assert_eq!(p.table, "ns.orders");
            }
            other => panic!("Expected ShowEffectivePolicy, got {other:?}"),
        }
    }

    #[test]
    fn test_show_effective_policy_case_insensitive_and_bare_table() {
        let result = parse_and_classify("show effective policy on orders").unwrap();
        match result {
            StatementKind::ShowEffectivePolicy(p) => {
                assert_eq!(p.user, None);
                assert_eq!(p.table, "orders");
            }
            other => panic!("Expected ShowEffectivePolicy, got {other:?}"),
        }
    }

    #[test]
    fn test_show_effective_policy_quoted_user() {
        let result =
            parse_and_classify("SHOW EFFECTIVE POLICY FOR USER 'bob' ON \"my ns\".\"my tbl\"")
                .unwrap();
        match result {
            StatementKind::ShowEffectivePolicy(p) => {
                assert_eq!(p.user.as_deref(), Some("bob"));
                assert_eq!(p.table, "\"my ns\".\"my tbl\"");
            }
            other => panic!("Expected ShowEffectivePolicy, got {other:?}"),
        }
    }

    #[test]
    fn test_show_effective_policy_missing_on_errors() {
        let result = parse_and_classify("SHOW EFFECTIVE POLICY orders");
        assert!(result.is_err(), "missing ON should be rejected");
    }

    #[test]
    fn test_show_effective_policy_name() {
        let kind = StatementKind::ShowEffectivePolicy(ShowEffectivePolicyParams {
            user: None,
            table: "t".to_string(),
        });
        assert_eq!(kind.name(), "showeffectivepolicy");
    }

    // ── SHOW TAGS ────────────────────────────────────────────────────────

    #[test]
    fn test_show_tags_dotted_name() {
        let result = parse_and_classify("SHOW TAGS ON cat.ns.customers").unwrap();
        match result {
            StatementKind::ShowTags(table) => assert_eq!(table, "cat.ns.customers"),
            other => panic!("Expected ShowTags, got {other:?}"),
        }
    }

    #[test]
    fn test_show_tags_case_insensitive() {
        let result = parse_and_classify("show tags on orders").unwrap();
        assert!(matches!(result, StatementKind::ShowTags(ref t) if t == "orders"));
    }

    #[test]
    fn test_show_tags_quoted_name() {
        let result = parse_and_classify("SHOW TAGS ON \"my ns\".\"my tbl\"").unwrap();
        match result {
            StatementKind::ShowTags(table) => assert_eq!(table, "\"my ns\".\"my tbl\""),
            other => panic!("Expected ShowTags, got {other:?}"),
        }
    }

    #[test]
    fn test_show_tags_missing_table_errors() {
        let result = parse_and_classify("SHOW TAGS ON ");
        assert!(result.is_err(), "missing table should be rejected");
    }

    #[test]
    fn test_show_tags_name() {
        let kind = StatementKind::ShowTags("t".to_string());
        assert_eq!(kind.name(), "showtags");
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
    fn test_show_create_schema_bare() {
        // sqlparser rejects SHOW CREATE SCHEMA at parse time, so the prefix
        // detection must intercept it and carry the raw schema name. (#351a)
        let result = parse_and_classify("SHOW CREATE SCHEMA my_schema").unwrap();
        match result {
            StatementKind::ShowCreateSchema(name) => assert_eq!(name, "my_schema"),
            other => panic!("Expected ShowCreateSchema, got: {other:?}"),
        }
    }

    #[test]
    fn test_show_create_schema_qualified_and_trailing_semicolon() {
        let result = parse_and_classify("show create schema iceberg.tpch_demo;").unwrap();
        match result {
            StatementKind::ShowCreateSchema(name) => assert_eq!(name, "iceberg.tpch_demo"),
            other => panic!("Expected ShowCreateSchema, got: {other:?}"),
        }
    }

    #[test]
    fn test_show_create_schema_missing_name_errors() {
        assert!(parse_and_classify("SHOW CREATE SCHEMA").is_err());
        assert!(parse_and_classify("SHOW CREATE SCHEMA   ").is_err());
    }

    #[test]
    fn test_show_create_table_still_parses_after_schema_prefix_added() {
        // Ensure the new SHOW CREATE SCHEMA prefix does not swallow the TABLE
        // form, which still flows through the sqlparser AST path.
        let result = parse_and_classify("SHOW CREATE TABLE my_schema.t");
        assert!(
            matches!(result, Ok(StatementKind::ShowCreateTable(_))),
            "Expected ShowCreateTable, got: {result:?}"
        );
    }

    #[test]
    fn test_analyze_bare_table() {
        let result = parse_and_classify("ANALYZE t").unwrap();
        match result {
            StatementKind::Analyze(table) => assert_eq!(table, "t"),
            other => panic!("Expected Analyze, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_properties_classifies_as_alter_table_props() {
        // Trino's bare `SET PROPERTIES k = v` must be rewritten to
        // `SET TBLPROPERTIES (k = v)` and classified as AlterTableProps (#338).
        let result =
            parse_and_classify("ALTER TABLE nation SET PROPERTIES format_version = 2").unwrap();
        assert!(
            matches!(result, StatementKind::AlterTableProps(_)),
            "Expected AlterTableProps, got: {result:?}"
        );
    }

    #[test]
    fn test_set_properties_multiple_pairs() {
        let result = parse_and_classify(
            "ALTER TABLE ns.t SET PROPERTIES format_version = 2, commit.retry.num-retries = 4",
        )
        .unwrap();
        assert!(
            matches!(result, StatementKind::AlterTableProps(_)),
            "Expected AlterTableProps, got: {result:?}"
        );
    }

    #[test]
    fn test_set_tblproperties_still_classifies() {
        // The already-valid parenthesized form must keep working and must not
        // be mangled by the SET PROPERTIES rewrite.
        let result =
            parse_and_classify("ALTER TABLE nation SET TBLPROPERTIES ('format_version' = '2')")
                .unwrap();
        assert!(
            matches!(result, StatementKind::AlterTableProps(_)),
            "Expected AlterTableProps, got: {result:?}"
        );
    }

    #[test]
    fn test_create_table_like_parenthesized_rewritten() {
        // Trino's `(LIKE src)` must be rewritten so sqlparser records it in
        // CreateTable.like rather than mis-parsing it as a column (#343).
        let result = parse_and_classify("CREATE TABLE nation_copy (LIKE nation)").unwrap();
        match result {
            StatementKind::CreateTable(stmt) => {
                let Statement::CreateTable(ct) = *stmt else {
                    panic!("expected inner CreateTable");
                };
                assert!(
                    ct.like.is_some(),
                    "expected like clause populated, columns={:?}",
                    ct.columns
                );
            }
            other => panic!("Expected CreateTable, got: {other:?}"),
        }
    }

    #[test]
    fn test_plain_create_table_not_treated_as_like() {
        // A normal column-def CREATE TABLE opens with `(` but does not start
        // with LIKE, so the rewrite must decline and leave it a CreateTable
        // with real columns (like clause absent).
        let result = parse_and_classify("CREATE TABLE t (a INT, b VARCHAR)").unwrap();
        match result {
            StatementKind::CreateTable(stmt) => {
                let Statement::CreateTable(ct) = *stmt else {
                    panic!("expected inner CreateTable");
                };
                assert!(ct.like.is_none(), "plain create must not gain a LIKE clause");
                assert_eq!(ct.columns.len(), 2, "columns should be preserved");
            }
            other => panic!("Expected CreateTable, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_properties_dotted_hyphenated_key() {
        // Dotted + hyphenated Iceberg property names must be quoted so they
        // parse; the whole statement classifies as AlterTableProps (#338).
        let result = parse_and_classify(
            "ALTER TABLE t SET PROPERTIES \"write.format.default\" = 'PARQUET'",
        )
        .unwrap();
        assert!(
            matches!(result, StatementKind::AlterTableProps(_)),
            "Expected AlterTableProps, got: {result:?}"
        );
    }

    #[test]
    fn test_analyze_schema_qualified() {
        let result = parse_and_classify("ANALYZE myschema.orders").unwrap();
        match result {
            StatementKind::Analyze(table) => assert_eq!(table, "myschema.orders"),
            other => panic!("Expected Analyze, got: {other:?}"),
        }
    }

    #[test]
    fn test_analyze_catalog_qualified_with_properties() {
        // sqlparser cannot parse the trailing WITH (...); the pre-scan must.
        let result =
            parse_and_classify("ANALYZE iceberg.default.t WITH (partitioning = ARRAY['x'])")
                .unwrap();
        match result {
            StatementKind::Analyze(table) => assert_eq!(table, "iceberg.default.t"),
            other => panic!("Expected Analyze, got: {other:?}"),
        }
    }

    #[test]
    fn test_analyze_table_keyword_stripped() {
        let result = parse_and_classify("ANALYZE TABLE cat.sch.tbl").unwrap();
        match result {
            StatementKind::Analyze(table) => assert_eq!(table, "cat.sch.tbl"),
            other => panic!("Expected Analyze, got: {other:?}"),
        }
    }

    #[test]
    fn test_analyze_name_label() {
        let kind = StatementKind::Analyze("t".to_string());
        assert_eq!(kind.name(), "analyze");
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
    fn set_tags_classifies_as_set_tags() {
        let result = parse_and_classify("ALTER TABLE t SET TAGS (email = ('PII'))");
        assert!(
            matches!(result, Ok(StatementKind::SetTags(_))),
            "expected SetTags, got: {result:?}"
        );
    }

    #[test]
    fn modify_column_set_tag_classifies_as_set_tags() {
        let result = parse_and_classify("ALTER TABLE t MODIFY COLUMN email SET TAG PII = 'true'");
        assert!(matches!(result, Ok(StatementKind::SetTags(_))));
    }

    #[test]
    fn set_tblproperties_still_classifies_as_alter_table_props() {
        // Guard: the new pre-scan must not steal SET TBLPROPERTIES.
        let result = parse_and_classify(
            "ALTER TABLE t SET TBLPROPERTIES ('write.format.default' = 'parquet')",
        );
        assert!(matches!(result, Ok(StatementKind::AlterTableProps(_))));
    }

    #[test]
    fn create_tag_still_classifies_as_refddl() {
        // Guard: Iceberg snapshot CREATE TAG must not be stolen.
        let result = parse_and_classify("ALTER TABLE t CREATE TAG v1");
        assert!(matches!(result, Ok(StatementKind::RefDdl(_))));
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
    fn test_drop_nested_column_returns_clear_not_implemented() {
        // #336: a dotted DROP COLUMN path must surface a clear NotImplemented
        // instead of sqlparser's `Expected: end of statement, found: .`.
        let err = parse_and_classify("ALTER TABLE t DROP COLUMN nested.subfield")
            .expect_err("dotted DROP COLUMN should error");
        assert!(matches!(err, sqe_core::SqeError::NotImplemented(_)), "got: {err:?}");
        let msg = err.to_string();
        assert!(msg.contains("nested.subfield"), "message should name the path: {msg}");
    }

    #[test]
    fn test_drop_nested_column_if_exists() {
        let err = parse_and_classify("ALTER TABLE t DROP COLUMN IF EXISTS a.b.c")
            .expect_err("dotted DROP COLUMN IF EXISTS should error");
        assert!(matches!(err, sqe_core::SqeError::NotImplemented(_)));
        assert!(err.to_string().contains("a.b.c"));
    }

    #[test]
    fn test_drop_top_level_column_still_alter_schema() {
        // Regression: the nested-drop pre-scan must not steal a plain
        // (non-dotted) DROP COLUMN, which sqlparser parses natively.
        let kind = parse_and_classify("ALTER TABLE t DROP COLUMN x").unwrap();
        assert!(matches!(kind, StatementKind::AlterSchema(_)), "got: {kind:?}");
    }

    #[test]
    fn detect_nested_drop_column_unit() {
        assert_eq!(
            detect_nested_drop_column("ALTER TABLE t DROP COLUMN a.b"),
            Some("a.b".to_string())
        );
        assert_eq!(
            detect_nested_drop_column("alter table t drop column IF EXISTS a.b.c"),
            Some("a.b.c".to_string())
        );
        assert_eq!(detect_nested_drop_column("ALTER TABLE t DROP COLUMN x"), None);
        assert_eq!(detect_nested_drop_column("ALTER TABLE t ADD COLUMN x INT"), None);
    }

    #[test]
    fn test_ref_ddl_name() {
        let kind = parse_and_classify("ALTER TABLE t CREATE BRANCH b").unwrap();
        assert_eq!(kind.name(), "refddl");
    }

    #[test]
    fn test_set_time_zone_classifies_as_utility_settimezone() {
        // Precondition for the coordinator's SET TIME ZONE no-op handling
        // (#351b): the statement must classify as Utility carrying a
        // Statement::Set(Set::SetTimeZone { .. }) so the handler can match it
        // narrowly and accept it. `SET WRITE_BRANCH` must still not be caught
        // here (it has its own dedicated kind).
        let kind = parse_and_classify("SET TIME ZONE 'UTC'").unwrap();
        match kind {
            StatementKind::Utility(stmt) => assert!(
                matches!(
                    stmt.as_ref(),
                    sqlparser::ast::Statement::Set(sqlparser::ast::Set::SetTimeZone { .. })
                ),
                "expected Set(SetTimeZone), got: {stmt:?}"
            ),
            other => panic!("expected Utility(SetTimeZone), got: {other:?}"),
        }
    }

    #[test]
    fn test_set_time_zone_local_classifies_as_utility_settimezone() {
        let kind = parse_and_classify("SET TIME ZONE LOCAL").unwrap();
        assert!(matches!(kind, StatementKind::Utility(_)));
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
