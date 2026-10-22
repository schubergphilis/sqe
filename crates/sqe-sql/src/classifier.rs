use sqlparser::ast::{
    AlterTableOperation, Expr, ObjectName, ObjectNamePart, ObjectType, Statement, Use, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::attach::{
    try_parse_attach, try_parse_create_secret, try_parse_detach, try_parse_drop_secret,
    AttachStatement, CreateSecretStatement, DetachStatement, DropSecretStatement,
};
use crate::ddl::{try_parse_ref_ddl, RefDdl};
use crate::grant_policy::{try_parse_grant_policy, GrantWithPolicy};
use crate::identifier_fields::{try_parse_identifier_fields, IdentifierFieldsDdl};
use crate::metadata::UpdateType;
use crate::partition_evolution::{try_parse_partition_evolution, PartitionEvolution};
use crate::policy_ddl::{try_parse_policy_ddl, PolicyDdlStatement};
use crate::procedures::{try_parse_call, ProcedureCall};
use crate::tags::{
    try_parse_masking_policy, try_parse_set_tags, MaskingPolicyOp, SetTagsStatement,
};
use crate::token_scan::{
    at_end, consume_word, consume_words, expect_end, expect_word, leading_words, parse_error,
    parse_name_ref, parse_principal_name, remaining_text, strip_prefix_ci, syntax_error, word_at,
};

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
    /// `CREATE EXTERNAL TABLE ... LOCATION ...` (Hive/Athena external table).
    /// Routed away from the Iceberg create path, which would otherwise try to
    /// create a managed table in the catalog.
    CreateExternalTable(Box<crate::hive_ddl::HiveTableSpec>),
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
    /// `SHOW VIEWS [FROM <catalog>.<schema>]`: the views among a schema's
    /// tables, same `IN`/`FROM` filter string as `ShowTables`.
    ShowViews(String),
    /// GRANT or DENY privilege statement
    Grant(Box<Statement>),
    /// `GRANT SELECT ... MASKED WITH (...)` / `ROWS WHERE ...`: a grant that
    /// also writes the grantee's column masks and row filter.
    GrantWithPolicy(Box<GrantWithPolicy>),
    /// REVOKE privilege statement
    Revoke(Box<Statement>),
    /// `DENY <priv> ON <object> TO <grantee>`: an explicit denial, distinct from
    /// revoking an allow. Ranger carries it as `denyPolicyItems`.
    Deny(Box<Statement>),
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
    /// `SHOW MASKING POLICIES` / `SHOW MASKING POLICY <name>`. `None` lists all;
    /// `Some(name)` filters to one policy.
    ShowMaskingPolicies(Option<String>),
    /// `CREATE [OR REPLACE] POLICY` / `DROP POLICY [IF EXISTS]`: SQL-managed
    /// Ranger row filters and column masks shared with Spark/Kyuubi.
    PolicyDdl(Box<PolicyDdlStatement>),
    Utility(Box<Statement>),
    ExplainFull(String), // inner SQL string (EXPLAIN FULL pre-processed)
    // Explicit transactions (#820). SQE writes only in autocommit: a
    // transaction may read, and any write or DDL inside it is refused (see
    // `allowed_in_transaction`). The isolation level and access mode are
    // validated at classification and carry no further meaning.
    Begin,
    Commit,
    Rollback,
    /// `SET TRANSACTION ...`: modes validated, otherwise a no-op.
    SetTransaction,
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
    /// ALTER TABLE t DROP COLUMN a.b — drop a nested struct subfield. The
    /// dotted path is rejected by sqlparser 0.62 (`Expected: end of statement,
    /// found: .`), so it is detected by prefix before the parser runs. Carries
    /// the raw table reference and the dotted field path split into components
    /// (e.g. `["info", "age"]`). (#336)
    DropNestedColumn {
        table: String,
        path: Vec<String>,
        if_exists: bool,
    },
    /// ALTER TABLE t ALTER COLUMN a.b [SET DATA] TYPE T — promote a nested
    /// struct member. sqlparser rejects the dotted name like it does for
    /// DROP COLUMN, so it is detected the same way. (#859)
    AlterNestedColumnType {
        table: String,
        path: Vec<String>,
        data_type: sqlparser::ast::DataType,
    },
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
    /// `ALTER TABLE ... SET | DROP IDENTIFIER FIELDS <cols>` (Spark syntax).
    /// sqlparser-rs has no AST for it.
    IdentifierFields(Box<IdentifierFieldsDdl>),
    /// `ALTER TABLE ... SET TAGS / UNSET TAGS` and the Snowflake-compatible
    /// `MODIFY|ALTER COLUMN ... SET TAG / UNSET TAG` column-tag authoring DDL.
    SetTags(Box<SetTagsStatement>),
    /// `ALTER TABLE ... MODIFY|ALTER COLUMN <col> SET|UNSET MASKING POLICY <name>`.
    /// Snowflake-compatible. Resolved against the policy store, not here: the
    /// named policy has to be looked up to find the tag it is keyed on.
    SetMaskingPolicy(Box<MaskingPolicyOp>),
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
    /// `MSCK REPAIR TABLE <t>` — rebuild a Hive external table's partition
    /// index from storage. sqlparser parses this natively (`Statement::Msck`);
    /// only `REPAIR` (and no ADD/DROP/SYNC PARTITIONS suffix) is accepted.
    MsckRepairTable(String),
    /// `SHOW PARTITIONS <t>` — list a Hive external table's partitions.
    /// sqlparser delivers this as a generic `ShowVariable`, so it is
    /// intercepted by prefix before parsing.
    ShowPartitions(String),
    /// `ALTER TABLE ... ADD/DROP PARTITION` on a Hive external table.
    /// Pre-parsed because sqlparser 0.62 cannot express `LOCATION` after an
    /// added partition spec or `PURGE` after a dropped one; see
    /// [`crate::hive_partitions`].
    HivePartitionDdl(Box<crate::hive_partitions::HivePartitionDdl>),
    /// `SET CATALOG <name>` -- switch the session's default catalog. Parsed
    /// from the token stream (sqlparser has no form for it). (#628)
    SetCatalog(String),
    /// `SET SESSION <name> = <value>` -- Trino session property. `value` is
    /// the literal's unquoted text (`'ZSTD'` -> `ZSTD`, `1` -> `1`); the
    /// protocol layer validates the property name against its allow-list.
    SetSession {
        name: String,
        value: String,
    },
    /// `RESET SESSION <name>`.
    ResetSession(String),
    /// `PREPARE <name> FROM <statement>` (Trino) or `PREPARE <name> AS
    /// <statement>` (PostgreSQL). The body is parsed here, so `PREPARE p
    /// FROM SELEC 1` fails at PREPARE time, as it does in Trino.
    Prepare {
        name: String,
        body: Box<Statement>,
    },
    /// `DEALLOCATE [PREPARE] <name>`.
    Deallocate(String),
    /// `EXECUTE <name> [USING <expr>, ...]`. `params` are the `USING`
    /// arguments (or the parenthesised argument list), already parsed, so
    /// the protocol layer binds expressions rather than splicing text.
    Execute {
        name: String,
        params: Vec<Expr>,
    },
    /// `DESCRIBE OUTPUT <prepared-statement>`.
    DescribeOutput(String),
    /// `DESCRIBE INPUT <prepared-statement>`.
    DescribeInput(String),
    /// `SHOW SESSION [LIKE '<pattern>']`.
    ShowSession {
        like: Option<String>,
    },
}

impl StatementKind {
    /// Return a stable lowercase label for metrics and audit logging.
    pub fn name(&self) -> &'static str {
        match self {
            StatementKind::Query(_) => "query",
            StatementKind::CreateTable(_) => "createtable",
            StatementKind::CreateExternalTable(_) => "createexternaltable",
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
            StatementKind::ShowViews(_) => "showviews",
            StatementKind::Grant(_) => "grant",
            StatementKind::GrantWithPolicy(_) => "grant",
            StatementKind::Revoke(_) => "revoke",
            StatementKind::Deny(_) => "deny",
            StatementKind::ShowGrants(_) => "showgrants",
            StatementKind::ShowEffectiveGrants(_) => "showeffectivegrants",
            StatementKind::CheckAccess(_) => "checkaccess",
            StatementKind::ShowEffectivePolicy(_) => "showeffectivepolicy",
            StatementKind::ShowTags(_) => "showtags",
            StatementKind::ShowMaskingPolicies(_) => "showmaskingpolicies",
            StatementKind::PolicyDdl(_) => "policyddl",
            StatementKind::Utility(_) => "utility",
            StatementKind::ExplainFull(_) => "explain_full",
            StatementKind::Begin => "begin",
            StatementKind::Commit => "commit",
            StatementKind::Rollback => "rollback",
            StatementKind::SetTransaction => "settransaction",
            StatementKind::Use(_) => "use",
            StatementKind::ShowCreateTable(_) => "showcreatetable",
            StatementKind::ShowCreateSchema(_) => "showcreateschema",
            StatementKind::DropNestedColumn { .. } => "dropnestedcolumn",
            StatementKind::AlterNestedColumnType { .. } => "alternestedcolumntype",
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
            StatementKind::IdentifierFields(_) => "identifierfields",
            StatementKind::SetTags(_) => "settags",
            StatementKind::SetMaskingPolicy(_) => "setmaskingpolicy",
            StatementKind::Attach(_) => "attach",
            StatementKind::Detach(_) => "detach",
            StatementKind::CreateSecret(_) => "create_secret",
            StatementKind::DropSecret(_) => "drop_secret",
            StatementKind::ShowSecrets => "show_secrets",
            StatementKind::MsckRepairTable(_) => "msckrepairtable",
            StatementKind::ShowPartitions(_) => "showpartitions",
            StatementKind::HivePartitionDdl(_) => "hivepartitionddl",
            StatementKind::SetCatalog(_) => "setcatalog",
            StatementKind::SetSession { .. } => "setsession",
            StatementKind::ResetSession(_) => "resetsession",
            StatementKind::Prepare { .. } => "prepare",
            StatementKind::Deallocate(_) => "deallocate",
            StatementKind::Execute { .. } => "execute",
            StatementKind::DescribeOutput(_) => "describeoutput",
            StatementKind::DescribeInput(_) => "describeinput",
            StatementKind::ShowSession { .. } => "showsession",
        }
    }

    /// Statements that change table data: the audit log's DML category.
    pub fn is_data_write(&self) -> bool {
        self.reports_rows_affected() || matches!(self, StatementKind::Truncate(_))
    }

    /// Data writes whose result is a one-row `rows_affected` count.
    /// TRUNCATE is a data write that reports no count.
    pub fn reports_rows_affected(&self) -> bool {
        matches!(
            self,
            StatementKind::Insert(_)
                | StatementKind::Ctas(_)
                | StatementKind::Update(_)
                | StatementKind::Delete(_)
                | StatementKind::Merge(_)
        )
    }

    /// Statements that only read: queries and the catalog / policy SHOW
    /// forms. A `Query` never hides a write; [`classify`] refuses the shapes
    /// that would ([`hidden_write`]).
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            StatementKind::Query(_)
                | StatementKind::ShowCatalogs
                | StatementKind::ShowSchemas(_)
                | StatementKind::ShowTables(_)
                | StatementKind::ShowViews(_)
                | StatementKind::ShowGrants(_)
                | StatementKind::ShowEffectiveGrants(_)
                | StatementKind::ShowEffectivePolicy(_)
                | StatementKind::ShowTags(_)
                | StatementKind::ShowMaskingPolicies(_)
                | StatementKind::ShowCreateTable(_)
                | StatementKind::ShowCreateSchema(_)
                | StatementKind::ShowStats(_)
                | StatementKind::ShowPartitions(_)
        )
    }

    /// Statements an explicit transaction may run: reads, EXPLAIN, session
    /// and transaction control, and the DML that writes to the
    /// transaction's branch (#821). CTAS, DDL, grants and procedures stay
    /// autocommit only and are refused with `AUTOCOMMIT_WRITE_CONFLICT`
    /// (#820). An allow-list, so a statement kind added later is refused
    /// until someone decides how it behaves in a transaction.
    pub fn allowed_in_transaction(&self) -> bool {
        if self.is_read_only() {
            return true;
        }
        match self {
            StatementKind::Insert(_)
            | StatementKind::Update(_)
            | StatementKind::Delete(_)
            | StatementKind::Merge(_)
            | StatementKind::Truncate(_) => true,
            StatementKind::Begin
            | StatementKind::Commit
            | StatementKind::Rollback
            | StatementKind::SetTransaction
            | StatementKind::Use(_)
            | StatementKind::SetCatalog(_)
            | StatementKind::SetSession { .. }
            | StatementKind::ResetSession(_)
            | StatementKind::ShowSession { .. }
            | StatementKind::SetWriteBranch(_)
            | StatementKind::Prepare { .. }
            | StatementKind::Deallocate(_)
            | StatementKind::DescribeOutput(_)
            | StatementKind::DescribeInput(_)
            | StatementKind::CheckAccess(_)
            | StatementKind::ShowSecrets
            | StatementKind::ExplainFull(_) => true,
            // `EXECUTE` is bound to its template before it runs, and the
            // bound statement is what gets checked.
            StatementKind::Execute { .. } => true,
            // EXPLAIN (queries only, see `require_explainable`), DESCRIBE,
            // the SHOW variants and `SET <variable>`. Named rather than
            // excluded: a DROP of an unmodelled object kind and the ALTER
            // TABLE operations without their own kind also land in Utility.
            StatementKind::Utility(stmt) => matches!(
                stmt.as_ref(),
                Statement::Explain { .. }
                    | Statement::ExplainTable { .. }
                    | Statement::Set(_)
                    | Statement::ShowVariable { .. }
                    | Statement::ShowFunctions { .. }
                    | Statement::ShowStatus { .. }
                    | Statement::ShowVariables { .. }
                    | Statement::ShowColumns { .. }
                    | Statement::ShowCollation { .. }
            ),
            _ => false,
        }
    }

    /// The Trino `updateType` for this statement, or `None` for reads. Kinds
    /// that wrap a sqlparser statement defer to [`crate::metadata::update_type`];
    /// SQE-only kinds map to the closest Trino name (a partition-spec change
    /// is an `ALTER TABLE`, a maintenance procedure is a `CALL`).
    pub fn update_type(&self) -> Option<UpdateType> {
        use UpdateType as U;
        if let Some(stmt) = self.statement() {
            return crate::metadata::update_type(stmt);
        }
        Some(match self {
            StatementKind::CreateExternalTable(_) => U::CreateTable,
            StatementKind::Begin => U::StartTransaction,
            StatementKind::Commit => U::Commit,
            StatementKind::Rollback => U::Rollback,
            StatementKind::Use(_) | StatementKind::SetCatalog(_) => U::Use,
            StatementKind::DropNestedColumn { .. }
            | StatementKind::AlterNestedColumnType { .. }
            | StatementKind::RefDdl(_)
            | StatementKind::PartitionEvolution(_)
            | StatementKind::IdentifierFields(_)
            | StatementKind::SetTags(_)
            | StatementKind::SetMaskingPolicy(_)
            | StatementKind::HivePartitionDdl(_) => U::AlterTable,
            StatementKind::Truncate(_) => U::Truncate,
            StatementKind::Analyze(_) => U::Analyze,
            StatementKind::Procedure(_) => U::Call,
            StatementKind::SetWriteBranch(_) | StatementKind::SetSession { .. } => U::SetSession,
            StatementKind::ResetSession(_) => U::ResetSession,
            StatementKind::Prepare { .. } => U::Prepare,
            StatementKind::Deallocate(_) => U::Deallocate,
            _ => return None,
        })
    }

    /// Borrow the underlying parsed `sqlparser::ast::Statement` when
    /// this kind wraps one. Returns `None` for variants that are
    /// pre-parsed into custom shapes (RefDdl, PartitionEvolution,
    /// IdentifierFields, Procedure, ExplainFull, ShowSchemas, ShowTables, ShowStats,
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
            | StatementKind::Deny(s)
            | StatementKind::Utility(s)
            | StatementKind::ShowCreateTable(s)
            | StatementKind::Call(s)
            | StatementKind::AlterTableProps(s)
            | StatementKind::Comment(s) => Some(s.as_ref()),
            StatementKind::GrantWithPolicy(g) => Some(&g.grant),
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

/// Detect the Trino/SQL `ALTER TABLE ... DROP COLUMN [IF EXISTS] <a.b...>`
/// dotted-path form (a nested struct-subfield drop) and return the dotted path.
/// Returns `None` for a plain (non-dotted) `DROP COLUMN`, which sqlparser
/// parses natively. Matching is tolerant of case and whitespace; the path is
/// the token sequence after `DROP COLUMN [IF EXISTS]` up to the next clause
/// separator (comma, whitespace-then-clause, or end). Only fires when a `.`
/// appears inside that path. (#336)
/// Detect `ALTER TABLE <table> DROP COLUMN [IF EXISTS] <a.b[.c]>` (a dotted
/// nested-field drop). Returns `(table_ref, path_components, if_exists)` where
/// `table_ref` is the raw (possibly qualified/quoted) table reference and
/// `path_components` are the dot-split, unquoted field names. Returns `None`
/// for a non-dotted DROP COLUMN (which sqlparser handles normally). (#336)
fn detect_nested_drop_column(trimmed: &str) -> Option<(String, Vec<String>, bool)> {
    let upper = trimmed.to_ascii_uppercase();
    let idx = upper.find("DROP COLUMN")?;
    // The table reference sits between `ALTER TABLE [IF EXISTS]` and `DROP COLUMN`.
    let prefix = trimmed[..idx].trim_end();
    let prefix_rest = strip_prefix_ci(prefix.trim_start(), "ALTER TABLE")?;
    let table_ref = strip_prefix_ci(prefix_rest.trim_start(), "IF EXISTS")
        .map(|r| r.trim())
        .unwrap_or_else(|| prefix_rest.trim());
    if table_ref.is_empty() {
        return None;
    }

    let after = trimmed[idx + "DROP COLUMN".len()..].trim_start();
    // Optional `IF EXISTS` on the column itself.
    let (if_exists, after) = match strip_prefix_ci(after, "IF EXISTS") {
        Some(r) => (true, r.trim_start()),
        None => (false, after),
    };
    // The column reference runs until a delimiter: whitespace, comma, terminator.
    let end = after
        .find(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .unwrap_or(after.len());
    let path_str = after[..end].trim();
    if !path_str.contains('.') || path_str.is_empty() {
        return None;
    }
    let path: Vec<String> = path_str
        .split('.')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if path.len() < 2 {
        return None;
    }
    Some((table_ref.to_string(), path, if_exists))
}

/// Detect `ALTER TABLE <table> ALTER COLUMN <a.b[.c]> [SET DATA] TYPE <type>`
/// (a nested struct-member type change) and return the raw table reference,
/// the dot-split member path and the parsed type. Returns `None` for a
/// non-dotted column, which sqlparser handles. (#859)
fn detect_nested_alter_column_type(
    trimmed: &str,
) -> sqe_core::Result<Option<(String, Vec<String>, sqlparser::ast::DataType)>> {
    let upper = trimmed.to_ascii_uppercase();
    let Some(idx) = upper.find("ALTER COLUMN") else {
        return Ok(None);
    };
    let Some(table_ref) = strip_prefix_ci(trimmed[..idx].trim(), "ALTER TABLE") else {
        return Ok(None);
    };
    let table_ref = strip_prefix_ci(table_ref.trim_start(), "IF EXISTS")
        .unwrap_or(table_ref)
        .trim();
    let after = trimmed[idx + "ALTER COLUMN".len()..].trim_start();
    let end = after.find(char::is_whitespace).unwrap_or(after.len());
    let path_str = &after[..end];
    if table_ref.is_empty() || !path_str.contains('.') {
        return Ok(None);
    }
    let rest = after[end..].trim_start();
    let Some(type_sql) =
        strip_prefix_ci(rest, "SET DATA TYPE ").or_else(|| strip_prefix_ci(rest, "TYPE "))
    else {
        return Ok(None);
    };
    let path: Vec<String> = path_str
        .split('.')
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect();
    if path.iter().any(String::is_empty) {
        return Ok(None);
    }
    let type_sql = type_sql.trim().trim_end_matches(';');
    let parse_error = crate::token_scan::parse_error;
    let mut parser = Parser::new(&GenericDialect {})
        .try_with_sql(type_sql)
        .map_err(parse_error)?;
    let data_type = parser.parse_data_type().map_err(parse_error)?;
    if parser.peek_token().token != Token::EOF {
        return Err(parse_error(ParserError::ParserError(format!(
            "unexpected text after the type in ALTER COLUMN {path_str}: {type_sql}"
        ))));
    }
    Ok(Some((table_ref.to_string(), path, data_type)))
}

/// Outcome of the shared tokenize-and-dispatch step: either an SQE-only
/// statement classified from the token stream, or the sqlparser result for
/// everything else (the parser error is kept so the front door can decide
/// whether a source-level fallback applies).
pub(crate) enum Dispatch {
    Custom(StatementKind),
    Standard(Result<Vec<Statement>, ParserError>),
}

/// Tokenize once, reject text the execution dialect reads differently and
/// multi-statement input, classify SQE-only grammar
/// from the tokens, and otherwise parse with sqlparser. Both
/// [`parse_and_classify`] and the SQL front door start here.
pub(crate) fn dispatch(sql: &str) -> sqe_core::Result<Dispatch> {
    let dialect = GenericDialect {};
    let tokens = crate::token_scan::tokenize(&dialect, sql)?;
    crate::token_scan::reject_dialect_divergence(sql, &tokens)?;
    crate::token_scan::reject_multi_statement(&tokens)?;
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    if let Some(kind) = try_classify_custom(&mut parser, sql)? {
        return Ok(Dispatch::Custom(kind));
    }
    Ok(Dispatch::Standard(parser.parse_statements()))
}

/// Parse a SQL string and classify its statement.
///
/// Exactly one statement is accepted: `SELECT 1; SELECT 2` is a
/// `SyntaxError` naming the position of the second statement, never a
/// silent execution of the first (finding 107). SQE-only grammar is
/// recognised on the token stream, so comments, quoting and case are
/// handled the way the parser handles them; standard statements are
/// classified from the sqlparser AST. Errors carry `SqeErrorCode::SyntaxError`
/// with the parser error as source.
pub fn parse_and_classify(sql: &str) -> sqe_core::Result<StatementKind> {
    match dispatch(sql)? {
        Dispatch::Custom(kind) => Ok(kind),
        Dispatch::Standard(parsed) => {
            let statements = parsed.map_err(parse_error)?;
            classify_single(statements)
        }
    }
}

/// Classify the one statement in `statements` after the expression-depth
/// guard. Shared by [`parse_and_classify`] and the front door.
pub(crate) fn classify_single(statements: Vec<Statement>) -> sqe_core::Result<StatementKind> {
    // Reject pathologically deep expression trees up front, before any
    // recursive AST visitor (the Trino-compat rewrite, then DataFusion's
    // analyzer) walks them and overflows the stack. A flat `a OR a OR ...`
    // chain parses cleanly but builds a depth-N tree; the guard turns that
    // into a clean parse error instead of an uncatchable process abort.
    crate::trino_compat::check_expression_depth(&statements)
        .map_err(|e| syntax_error(format!("Parse error: {e}")))?;
    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| syntax_error("Empty SQL"))?;
    classify(stmt)
}

/// Recognise SQE-only grammar from the leading tokens and parse it.
///
/// Dispatch is on the leading unquoted words, so a quoted identifier such as
/// `"show"` never matches, a leading `-- comment` is skipped, and `ShOw TaGs`
/// is the same as `SHOW TAGS`. Every arm that consumes tokens returns; the
/// arms that hand the statement text to a string-based sub-parser only peek,
/// so on `Ok(None)` the parser is still at the first token and the caller
/// can run the standard parse on the same token stream.
///
/// `body` is the statement text from its first significant token (leading
/// comments and whitespace removed). It is what the remaining string-based
/// sub-parsers (`ALTER TABLE` extensions, `ATTACH`, secrets, policy DDL)
/// receive.
fn try_classify_custom(parser: &mut Parser, sql: &str) -> sqe_core::Result<Option<StatementKind>> {
    let words = leading_words(parser, 5);
    let w: Vec<&str> = words.iter().map(String::as_str).collect();
    let body = remaining_text(parser, sql);

    let kind = match w.as_slice() {
        // `CREATE [OR REPLACE] POLICY ...` / `DROP POLICY ...` (string-based
        // sub-parser; returns None for every other CREATE/DROP).
        ["CREATE", ..] | ["DROP", ..] if try_parse_policy_ddl(body)?.is_some() => {
            try_parse_policy_ddl(body)?.map(|p| StatementKind::PolicyDdl(Box::new(p)))
        }
        // GRANT ... MASKED WITH (...) / ROWS WHERE ...: peek-only; a GRANT
        // without either clause is left to the standard parse.
        ["GRANT", ..] => {
            try_parse_grant_policy(body)?.map(|g| StatementKind::GrantWithPolicy(Box::new(g)))
        }
        // SHOW CATALOGS [anything]: sqlparser reads it as a variable.
        ["SHOW", "CATALOGS", ..] => Some(StatementKind::ShowCatalogs),
        // SHOW CREATE SCHEMA <name>: sqlparser 0.62 only models SHOW CREATE
        // for TABLE/VIEW/... and rejects the SCHEMA form outright. (#351a)
        ["SHOW", "CREATE", "SCHEMA", ..] => {
            consume_words(parser, &["SHOW", "CREATE", "SCHEMA"]);
            if at_end(parser) {
                return Err(syntax_error("SHOW CREATE SCHEMA requires a schema name"));
            }
            let schema = parse_name_ref(parser)?;
            expect_end(parser)?;
            Some(StatementKind::ShowCreateSchema(schema))
        }
        // EXPLAIN FULL <statement>: not standard SQL. The inner statement is
        // carried as text, but classified here so only a query is explained.
        ["EXPLAIN", "FULL", ..] => {
            consume_words(parser, &["EXPLAIN", "FULL"]);
            let inner = remaining_text(parser, sql);
            if inner.is_empty() {
                return Err(syntax_error("EXPLAIN FULL requires a statement"));
            }
            require_explainable(&parse_and_classify(inner)?)?;
            Some(StatementKind::ExplainFull(inner.to_string()))
        }
        // SHOW STATS FOR <table>: sqlparser reads it as a variable.
        ["SHOW", "STATS", "FOR", ..] => {
            consume_words(parser, &["SHOW", "STATS", "FOR"]);
            let table = parse_name_ref(parser)?;
            expect_end(parser)?;
            Some(StatementKind::ShowStats(table))
        }
        // SHOW PARTITIONS <table>: sqlparser reads it as a variable. LIKE /
        // WHERE / extra clauses are not implemented and are rejected rather
        // than swallowed into the table name.
        ["SHOW", "PARTITIONS", ..] => {
            consume_words(parser, &["SHOW", "PARTITIONS"]);
            if at_end(parser) {
                return Err(syntax_error(
                    "SHOW PARTITIONS requires: SHOW PARTITIONS <table>",
                ));
            }
            let table = parse_name_ref(parser)?;
            if !at_end(parser) {
                return Err(syntax_error(
                    "SHOW PARTITIONS does not support LIKE or extra clauses; use \
                     SHOW PARTITIONS <table>",
                ));
            }
            Some(StatementKind::ShowPartitions(table))
        }
        // ANALYZE [TABLE] <table> [WITH (...)]: sqlparser's parse_analyze
        // stops at WITH. SQE treats ANALYZE as a stats no-op (#329), so the
        // trailing properties clause is accepted and ignored.
        ["ANALYZE", ..] => {
            consume_word(parser, "ANALYZE");
            consume_word(parser, "TABLE");
            if at_end(parser) {
                return Err(syntax_error(
                    "ANALYZE requires a table name: ANALYZE [catalog.][schema.]table [WITH (...)]",
                ));
            }
            let table = parse_name_ref(parser)?;
            Some(StatementKind::Analyze(table))
        }
        // SHOW EFFECTIVE GRANTS FOR USER "name"
        ["SHOW", "EFFECTIVE", "GRANTS", "FOR", "USER"] => {
            consume_words(parser, &["SHOW", "EFFECTIVE", "GRANTS", "FOR", "USER"]);
            let user = parse_principal_name(parser)?;
            expect_end(parser)?;
            Some(StatementKind::ShowEffectiveGrants(user))
        }
        // SHOW GRANTS ON [catalog.]namespace[.table]
        ["SHOW", "GRANTS", "ON", ..] => {
            consume_words(parser, &["SHOW", "GRANTS", "ON"]);
            let name = parser.parse_object_name(false).map_err(parse_error)?;
            expect_end(parser)?;
            let (catalog, namespace, table) = resource_parts(&name)?;
            Some(StatementKind::ShowGrants(ShowGrantsTarget::OnResource {
                catalog,
                namespace,
                table,
            }))
        }
        // SHOW GRANTS TO GROUP|USER|ROLE "name"
        ["SHOW", "GRANTS", "TO", ..] => {
            consume_words(parser, &["SHOW", "GRANTS", "TO"]);
            let Some(grantee_type) = word_at(parser, 0) else {
                return Err(syntax_error(
                    "SHOW GRANTS TO requires: SHOW GRANTS TO GROUP|USER \"name\"",
                ));
            };
            parser.next_token();
            if at_end(parser) {
                return Err(syntax_error(
                    "SHOW GRANTS TO requires: SHOW GRANTS TO GROUP|USER \"name\"",
                ));
            }
            let grantee_name = parse_principal_name(parser)?;
            expect_end(parser)?;
            Some(StatementKind::ShowGrants(ShowGrantsTarget::ToGrantee {
                grantee_type,
                grantee_name,
            }))
        }
        // CHECK ACCESS <privilege> ON <resource> FOR USER "name"
        ["CHECK", "ACCESS", ..] => {
            consume_words(parser, &["CHECK", "ACCESS"]);
            Some(parse_check_access(parser)?)
        }
        // SHOW EFFECTIVE POLICY [FOR USER "name"] ON <table>
        ["SHOW", "EFFECTIVE", "POLICY", ..] => {
            consume_words(parser, &["SHOW", "EFFECTIVE", "POLICY"]);
            Some(parse_show_effective_policy(parser)?)
        }
        // SHOW TAGS ON <table>: sqlparser reads `SHOW TAGS` as a variable.
        ["SHOW", "TAGS", ..] => {
            consume_words(parser, &["SHOW", "TAGS"]);
            if !consume_word(parser, "ON") || at_end(parser) {
                return Err(syntax_error("SHOW TAGS requires: SHOW TAGS ON <table>"));
            }
            let table = parse_name_ref(parser)?;
            expect_end(parser)?;
            Some(StatementKind::ShowTags(table))
        }
        // SHOW MASKING POLICIES / SHOW MASKING POLICY [<name>]. Both spellings
        // are accepted with or without a name: operators type both, and the
        // name simply filters.
        ["SHOW", "MASKING", "POLICIES" | "POLICY", ..] => {
            parser.next_token();
            parser.next_token();
            parser.next_token();
            let name = if at_end(parser) {
                None
            } else {
                let n = parse_principal_name(parser)?;
                expect_end(parser)?;
                Some(n)
            };
            Some(StatementKind::ShowMaskingPolicies(name))
        }
        // SHOW SECRETS: names and kinds only, never values.
        ["SHOW", "SECRETS", ..] => {
            consume_words(parser, &["SHOW", "SECRETS"]);
            expect_end(parser)?;
            Some(StatementKind::ShowSecrets)
        }
        // SHOW SESSION [LIKE '<pattern>'] (Trino session properties). (#628)
        ["SHOW", "SESSION", ..] => {
            consume_words(parser, &["SHOW", "SESSION"]);
            let like = if consume_word(parser, "LIKE") {
                Some(parser.parse_literal_string().map_err(parse_error)?)
            } else {
                None
            };
            expect_end(parser)?;
            Some(StatementKind::ShowSession { like })
        }
        // SET WRITE_BRANCH [=] 'name' | DEFAULT | NULL: route this session's
        // writes to a named Iceberg branch, bypassing DataFusion's generic SET.
        ["SET", "WRITE_BRANCH", ..] => {
            consume_words(parser, &["SET", "WRITE_BRANCH"]);
            // The `=` is optional: `SET WRITE_BRANCH 'name'` is accepted too.
            let _ = parser.consume_token(&Token::Eq);
            let branch = parse_write_branch_value(parser)?;
            expect_end(parser)?;
            Some(StatementKind::SetWriteBranch(branch))
        }
        // SET SESSION <name> = <value> (Trino session property). (#628)
        ["SET", "SESSION", ..] => {
            consume_words(parser, &["SET", "SESSION"]);
            let name = parse_name_ref(parser)?;
            parser.expect_token(&Token::Eq).map_err(parse_error)?;
            let value = literal_text(parser.parse_expr().map_err(parse_error)?);
            expect_end(parser)?;
            Some(StatementKind::SetSession { name, value })
        }
        // SET CATALOG <name>. (#628)
        ["SET", "CATALOG", ..] => {
            consume_words(parser, &["SET", "CATALOG"]);
            let catalog = parse_name_ref(parser)?;
            expect_end(parser)?;
            Some(StatementKind::SetCatalog(catalog))
        }
        // RESET SESSION <name>. (#628)
        ["RESET", "SESSION", ..] => {
            consume_words(parser, &["RESET", "SESSION"]);
            let name = parse_name_ref(parser)?;
            expect_end(parser)?;
            Some(StatementKind::ResetSession(name))
        }
        // DESCRIBE OUTPUT|INPUT <prepared>: sqlparser would read these as
        // `DESCRIBE <table>`. (#628)
        ["DESCRIBE" | "DESC", "OUTPUT", ..] => {
            parser.next_token();
            parser.next_token();
            let name = parser.parse_identifier().map_err(parse_error)?.value;
            expect_end(parser)?;
            Some(StatementKind::DescribeOutput(name))
        }
        ["DESCRIBE" | "DESC", "INPUT", ..] => {
            parser.next_token();
            parser.next_token();
            let name = parser.parse_identifier().map_err(parse_error)?.value;
            expect_end(parser)?;
            Some(StatementKind::DescribeInput(name))
        }
        // PREPARE <name> FROM <statement> (Trino). The PostgreSQL `AS` form
        // parses natively and is classified from the AST. The body is parsed
        // here so a malformed statement fails at PREPARE time. (#628)
        ["PREPARE", _, "FROM", ..] => {
            consume_word(parser, "PREPARE");
            let name = parser.parse_identifier().map_err(parse_error)?.value;
            expect_word(parser, "FROM")?;
            let body = parser.parse_statement().map_err(parse_error)?;
            expect_end(parser)?;
            crate::trino_compat::check_expression_depth(std::slice::from_ref(&body))
                .map_err(|e| syntax_error(format!("Parse error: {e}")))?;
            Some(StatementKind::Prepare {
                name,
                body: Box::new(body),
            })
        }
        // ALTER TABLE extensions that sqlparser rejects or flattens
        // (branch/tag DDL, partition evolution, Hive partitions, tags,
        // masking policies, nested DROP COLUMN, Trino SET PROPERTIES).
        ["ALTER", "TABLE", ..] => classify_alter_table_extension(body)?,
        // ATTACH / DETACH / CREATE SECRET / DROP SECRET: SQE extensions with a
        // DuckDB-style option list. A legacy SQLite `ATTACH '<file>' AS
        // <name>` falls through to sqlparser and its NotImplemented error.
        ["ATTACH", ..] => try_parse_attach(body)?.map(|s| StatementKind::Attach(Box::new(s))),
        ["DETACH", ..] => try_parse_detach(body)?.map(|s| StatementKind::Detach(Box::new(s))),
        ["CREATE", "SECRET", ..] => {
            try_parse_create_secret(body)?.map(|s| StatementKind::CreateSecret(Box::new(s)))
        }
        ["DROP", "SECRET", ..] => {
            try_parse_drop_secret(body)?.map(|s| StatementKind::DropSecret(Box::new(s)))
        }
        // Trino's `CREATE TABLE new (LIKE src)`: rewritten to the plain
        // `CREATE TABLE new LIKE src` sqlparser records in `CreateTable.like`,
        // then re-classified. (#343)
        ["CREATE", "TABLE", ..] => match rewrite_create_table_like(body) {
            Some(rewritten) => Some(parse_and_classify(&rewritten)?),
            None => None,
        },
        _ => None,
    };
    Ok(kind)
}

/// The `ALTER TABLE` family that sqlparser cannot express. Each sub-parser
/// returns `Ok(None)` for the others' shapes; the order is belt-and-braces
/// (most specific first) rather than load-bearing. These sub-parsers still
/// take the statement text; `body` has leading comments removed.
fn classify_alter_table_extension(body: &str) -> sqe_core::Result<Option<StatementKind>> {
    // Trino spells Iceberg table-property updates
    // `ALTER TABLE t SET PROPERTIES k = v [, ...]` (no parentheses, no
    // TBLPROPERTIES keyword). sqlparser only accepts the parenthesized
    // `SET TBLPROPERTIES (k = v, ...)` form, so rewrite the Trino form and
    // re-classify; the existing AlterTableProps handler then commits them
    // as Iceberg SetProperties (#338).
    if let Some(rewritten) = rewrite_set_properties(body) {
        return parse_and_classify(&rewritten).map(Some);
    }
    if let Some(ref_ddl) = try_parse_ref_ddl(body)? {
        return Ok(Some(StatementKind::RefDdl(Box::new(ref_ddl))));
    }
    // Hive-style ADD/DROP PARTITION (concrete values, optional LOCATION /
    // PURGE) before the partition-evolution form: both start with ADD/DROP
    // PARTITION, the evolution form continues with FIELD and this one with
    // `(`.
    if let Some(ddl) = crate::hive_partitions::try_parse_hive_partition_ddl(body)? {
        return Ok(Some(StatementKind::HivePartitionDdl(Box::new(ddl))));
    }
    if let Some(pe) = try_parse_partition_evolution(body)? {
        return Ok(Some(StatementKind::PartitionEvolution(Box::new(pe))));
    }
    if let Some(ddl) = try_parse_identifier_fields(body)? {
        return Ok(Some(StatementKind::IdentifierFields(Box::new(ddl))));
    }
    // MODIFY|ALTER COLUMN ... SET|UNSET MASKING POLICY before the tag forms:
    // the tag parser returns Ok(None) for these (asserted by
    // `the_tag_parser_does_not_claim_the_masking_policy_forms`), so keeping
    // the more specific form first means a future loosening of the tag parser
    // cannot silently capture a masking-policy statement.
    if let Some(op) = try_parse_masking_policy(body)? {
        return Ok(Some(StatementKind::SetMaskingPolicy(Box::new(op))));
    }
    if let Some(set_tags) = try_parse_set_tags(body)? {
        return Ok(Some(StatementKind::SetTags(Box::new(set_tags))));
    }
    // DROP COLUMN a.b drops a nested struct subfield; sqlparser 0.62 rejects
    // the `.`. A non-dotted DROP COLUMN flows to sqlparser. (#336)
    if let Some((table, path, if_exists)) = detect_nested_drop_column(body) {
        return Ok(Some(StatementKind::DropNestedColumn {
            table,
            path,
            if_exists,
        }));
    }
    if let Some((table, path, data_type)) = detect_nested_alter_column_type(body)? {
        return Ok(Some(StatementKind::AlterNestedColumnType {
            table,
            path,
            data_type,
        }));
    }
    Ok(None)
}

/// `SET WRITE_BRANCH` value: a quoted or bare name, or `DEFAULT` / `NULL` /
/// nothing to reset to main.
fn parse_write_branch_value(parser: &mut Parser) -> sqe_core::Result<Option<String>> {
    let tok = parser.peek_token();
    let value = match tok.token {
        Token::EOF | Token::SemiColon => return Ok(None),
        Token::Word(w) if w.quote_style.is_none() => {
            if w.value.eq_ignore_ascii_case("DEFAULT") || w.value.eq_ignore_ascii_case("NULL") {
                parser.next_token();
                return Ok(None);
            }
            w.value
        }
        Token::Word(w) => w.value,
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => s,
        other => {
            return Err(syntax_error(format!(
                "SET WRITE_BRANCH expects a branch name, DEFAULT or NULL, found: {other}"
            )));
        }
    };
    parser.next_token();
    Ok(Some(value))
}

/// The user-facing text of a literal expression: the unquoted string for
/// string literals, the SQL text for everything else (`1`, `true`, `1.5`).
fn literal_text(expr: Expr) -> String {
    match expr {
        Expr::Value(v) => match v.value {
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::EscapedStringLiteral(s)
            | Value::NationalStringLiteral(s) => s,
            other => other.to_string(),
        },
        other => other.to_string(),
    }
}

/// Split a parsed `[catalog.][namespace.]table` reference into its unquoted
/// parts. One part is a catalog, two are catalog.namespace, three are
/// catalog.namespace.table; four or more is an error.
fn resource_parts(
    name: &ObjectName,
) -> sqe_core::Result<(Option<String>, Option<String>, Option<String>)> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => i.value.clone(),
            ObjectNamePart::Function(f) => f.to_string(),
        })
        .collect();
    let mut it = parts.iter().cloned();
    match parts.len() {
        1 => Ok((it.next(), None, None)),
        2 => Ok((it.next(), it.next(), None)),
        3 => Ok((it.next(), it.next(), it.next())),
        n => Err(syntax_error(format!(
            "Resource reference has {n} parts (max 3): {name}"
        ))),
    }
}

/// Finds a write inside a statement that reads: `SELECT ... INTO` or a
/// data statement used as a query body (`WITH x AS (...) INSERT ...`, a
/// nested `(DELETE ...)`).
struct HiddenWrite(Option<&'static str>);

impl sqlparser::ast::Visitor for HiddenWrite {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> std::ops::ControlFlow<()> {
        use sqlparser::ast::SetExpr;
        self.0 = match query.body.as_ref() {
            SetExpr::Insert(_) => Some("insert statement"),
            SetExpr::Update(_) => Some("update statement"),
            SetExpr::Delete(_) => Some("delete statement"),
            SetExpr::Merge(_) => Some("merge statement"),
            _ => None,
        };
        if self.0.is_some() {
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    }

    fn pre_visit_select(&mut self, select: &sqlparser::ast::Select) -> std::ops::ControlFlow<()> {
        if select.into.is_some() {
            self.0 = Some("select into statement");
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    }
}

/// The write a query or EXPLAIN hides, if any (#811). These shapes classify
/// as reads by their outer statement, so every check keyed on the kind
/// (read-only tools, audit category, routing) would take them for one;
/// DataFusion would then run `SELECT INTO` as DDL while planning.
pub fn hidden_write(stmt: &Statement) -> Option<&'static str> {
    use sqlparser::ast::Visit;
    let mut finder = HiddenWrite(None);
    let _ = stmt.visit(&mut finder);
    finder.0
}

/// Refuse a query or EXPLAIN that hides a write ([`hidden_write`]).
fn refuse_hidden_write(stmt: &Statement) -> sqe_core::Result<()> {
    match hidden_write(stmt) {
        Some(shape) => Err(sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::NotSupported,
            format!("a {shape} inside a query is not supported; run the write as its own statement"),
        )),
        None => Ok(()),
    }
}

/// Refuse an EXPLAIN whose inner statement is not a query (#711, #811).
/// Every EXPLAIN form plans the inner statement through `ctx.sql`, and
/// ANALYZE / FULL then execute it. DataFusion runs DDL while planning and DML
/// on collect, so an explained write would skip SQE's write gates. Refusing
/// here, by kind, holds for every door and every EXPLAIN form alike.
fn require_explainable(inner: &StatementKind) -> sqe_core::Result<()> {
    if matches!(inner, StatementKind::Query(_)) {
        return Ok(());
    }
    Err(sqe_core::SqeError::with_code(
        sqe_core::SqeErrorCode::NotSupported,
        format!(
            "EXPLAIN only accepts a query; {} statements are not explained (run them directly)",
            inner.name()
        ),
    ))
}

/// Validate `START TRANSACTION` / `SET TRANSACTION` modes. Both access modes
/// and the four standard isolation levels are accepted: writes are refused
/// inside any transaction, so they never need isolating (#820). `SNAPSHOT`
/// is not standard or Trino grammar and is refused.
fn check_transaction_modes(modes: &[sqlparser::ast::TransactionMode]) -> sqe_core::Result<()> {
    use sqlparser::ast::{TransactionIsolationLevel, TransactionMode};
    for mode in modes {
        if let TransactionMode::IsolationLevel(TransactionIsolationLevel::Snapshot) = mode {
            return Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::NotSupported,
                "Unsupported isolation level: SNAPSHOT (supported: READ UNCOMMITTED, \
                 READ COMMITTED, REPEATABLE READ, SERIALIZABLE)"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Classify a parsed statement. Custom SQE grammar never reaches this
/// function; see [`try_classify_custom`].
pub(crate) fn classify(stmt: Statement) -> sqe_core::Result<StatementKind> {
    match stmt {
        // SELECT / WITH ... SELECT
        Statement::Query(_) => {
            refuse_hidden_write(&stmt)?;
            Ok(StatementKind::Query(Box::new(stmt)))
        }

        // CREATE TABLE ... AS SELECT (CTAS) vs regular CREATE TABLE (columns)
        Statement::CreateTable(ref ct) => {
            // EXTERNAL is checked before CTAS, on purpose: sqlparser's
            // GenericDialect does not currently accept `external: true`
            // together with a trailing `AS SELECT` at all (it errors at
            // parse time, verified for several clause orderings), so this
            // ordering is defensive against a future sqlparser version that
            // does accept the combination. If that ever happens, this
            // ordering makes it route to the Hive path (and be rejected
            // there) instead of silently falling through to CTAS and
            // creating an Iceberg table.
            if ct.external {
                if let Some(spec) = crate::hive_ddl::try_extract_hive_table(&stmt)? {
                    return Ok(StatementKind::CreateExternalTable(Box::new(spec)));
                }
            }
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
            let is_rename = operations
                .iter()
                .any(|op| matches!(op, AlterTableOperation::RenameTable { .. }));
            let is_schema_change = operations.iter().any(|op| {
                matches!(
                    op,
                    AlterTableOperation::AddColumn { .. }
                        | AlterTableOperation::DropColumn { .. }
                        | AlterTableOperation::RenameColumn { .. }
                        | AlterTableOperation::AlterColumn { .. }
                )
            });
            let is_set_properties = operations
                .iter()
                .any(|op| matches!(op, AlterTableOperation::SetTblProperties { .. }));
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
        Statement::Deny { .. } => Ok(StatementKind::Deny(Box::new(stmt))),

        // EXPLAIN → Utility. The inner statement is classified like any
        // other, so a query hiding a write is refused by the Query arm and
        // anything that is not a query by `require_explainable`.
        Statement::Explain { ref statement, .. } => {
            require_explainable(&classify((**statement).clone())?)?;
            Ok(StatementKind::Utility(Box::new(stmt)))
        }
        Statement::ExplainTable { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // SET → Utility (sqlparser 0.62 consolidates every SET flavour
        // (SetVariable / SetTimeZone / SetNames / SetNamesDefault /
        // SetTransaction / SetRole) into a single Statement::Set(Set) variant).
        // SET TRANSACTION is transaction control (#820), the rest Utility.
        Statement::Set(sqlparser::ast::Set::SetTransaction {
            ref modes,
            ref snapshot,
            ..
        }) => {
            if snapshot.is_some() {
                return Err(sqe_core::SqeError::with_code(
                    sqe_core::SqeErrorCode::NotSupported,
                    "SET TRANSACTION SNAPSHOT is not supported".to_string(),
                ));
            }
            check_transaction_modes(modes)?;
            Ok(StatementKind::SetTransaction)
        }
        Statement::Set(_) => Ok(StatementKind::Utility(Box::new(stmt))),

        // SHOW SCHEMAS — sqlparser has a ShowSchemas variant
        Statement::ShowSchemas {
            ref show_options, ..
        } => Ok(StatementKind::ShowSchemas(show_filter_text(show_options))),

        // SHOW TABLES — sqlparser has a ShowTables variant
        Statement::ShowTables {
            ref show_options, ..
        } => Ok(StatementKind::ShowTables(show_filter_text(show_options))),

        // SHOW DATABASES — treat like ShowCatalogs
        Statement::ShowDatabases { .. } => Ok(StatementKind::ShowCatalogs),

        // SHOW <variable> — could be "SHOW CATALOGS" parsed as variable
        Statement::ShowVariable { ref variable } => {
            let words: Vec<String> = variable
                .iter()
                .map(|i| i.value.to_ascii_uppercase())
                .collect();
            match words.split_first() {
                Some((first, [])) if first == "CATALOGS" => Ok(StatementKind::ShowCatalogs),
                Some((first, rest)) if first == "SCHEMAS" => {
                    Ok(StatementKind::ShowSchemas(rest.join(" ")))
                }
                Some((first, rest)) if first == "TABLES" => {
                    Ok(StatementKind::ShowTables(rest.join(" ")))
                }
                _ => Ok(StatementKind::Utility(Box::new(stmt))),
            }
        }

        // SHOW CREATE TABLE — reconstruct DDL from metadata
        Statement::ShowCreate { .. } => Ok(StatementKind::ShowCreateTable(Box::new(stmt))),

        // SHOW VIEWS — the views of one schema, same filter shape as
        // SHOW TABLES. Routed rather than left to DataFusion, which has no
        // SHOW VIEWS and answered with a parse error.
        Statement::ShowViews {
            ref show_options, ..
        } => {
            let filter = show_options
                .show_in
                .as_ref()
                .map(|si| si.to_string())
                .unwrap_or_default();
            Ok(StatementKind::ShowViews(filter))
        }

        // Other SHOW variants → Utility
        Statement::ShowFunctions { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCollation { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // UPDATE → dedicated variant for routing to the write handler
        Statement::Update { .. } => Ok(StatementKind::Update(Box::new(stmt))),

        // Explicit transactions (#820). Trino JDBC with setAutoCommit(false)
        // sends `START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, READ
        // WRITE`. A `BEGIN ... END` block parses into the same variant and
        // is a procedural block, not a transaction start.
        Statement::StartTransaction {
            ref modes,
            ref statements,
            ref exception,
            has_end_keyword,
            ..
        } => {
            if !statements.is_empty() || exception.is_some() || has_end_keyword {
                return Err(sqe_core::SqeError::with_code(
                    sqe_core::SqeErrorCode::NotSupported,
                    "BEGIN ... END blocks are not supported".to_string(),
                ));
            }
            check_transaction_modes(modes)?;
            Ok(StatementKind::Begin)
        }
        Statement::Commit { .. } => Ok(StatementKind::Commit),
        Statement::Rollback {
            savepoint: Some(ref name),
            ..
        } => Err(sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::NotSupported,
            format!("ROLLBACK TO SAVEPOINT {name}: savepoints are not supported"),
        )),
        Statement::Rollback { .. } => Ok(StatementKind::Rollback),

        // USE [CATALOG|SCHEMA|DATABASE] catalog.schema — session context
        // switching. Read from the AST: the target is the object name as
        // written (quotes preserved). Role / warehouse / secondary-role forms
        // have no meaning here.
        Statement::Use(ref use_stmt) => match use_stmt {
            Use::Object(name) | Use::Catalog(name) | Use::Schema(name) | Use::Database(name) => {
                Ok(StatementKind::Use(name.to_string()))
            }
            Use::Warehouse(_) | Use::Role(_) | Use::SecondaryRoles(_) | Use::Default => Err(
                sqe_core::SqeError::NotImplemented(format!("Statement type not supported: {stmt}")),
            ),
        },

        // TRUNCATE TABLE — routes to DELETE FROM without WHERE. Exactly one
        // table: `TRUNCATE a, b` used to truncate `a` and report success
        // (finding 107).
        Statement::Truncate(ref truncate) => match truncate.table_names.as_slice() {
            [single] => Ok(StatementKind::Truncate(single.name.to_string())),
            many => Err(syntax_error(format!(
                "TRUNCATE accepts exactly one table; got {} ({}). Run one TRUNCATE per table.",
                many.len(),
                many.iter()
                    .map(|t| t.name.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        },

        // PREPARE name AS <statement> (PostgreSQL form; the Trino FROM form
        // is parsed from the token stream). The body is already parsed, so a
        // malformed body never gets this far. (#628)
        Statement::Prepare {
            name, statement, ..
        } => {
            crate::trino_compat::check_expression_depth(std::slice::from_ref(&statement))
                .map_err(|e| syntax_error(format!("Parse error: {e}")))?;
            Ok(StatementKind::Prepare {
                name: name.value,
                body: statement,
            })
        }

        // EXECUTE name [USING a, b] / EXECUTE name (a, b). (#628)
        Statement::Execute {
            name: Some(name),
            parameters,
            using,
            immediate: false,
            ..
        } => {
            let params = if using.is_empty() {
                parameters
            } else {
                using.into_iter().map(|e| e.expr).collect()
            };
            Ok(StatementKind::Execute {
                name: name.to_string(),
                params,
            })
        }

        // DEALLOCATE [PREPARE] name. (#628)
        Statement::Deallocate { name, .. } => Ok(StatementKind::Deallocate(name.value)),

        // CALL procedure — dispatch to Iceberg maintenance procedures when the
        // name is `system.<known>`. Unknown calls fall through to the generic
        // `Call(_)` variant so the existing "not supported" error triggers.
        Statement::Call(_) => match try_parse_call(&stmt)? {
            Some(proc) => Ok(StatementKind::Procedure(Box::new(proc))),
            None => Ok(StatementKind::Call(Box::new(stmt))),
        },

        // COMMENT ON TABLE/COLUMN — store as Iceberg table property
        Statement::Comment { .. } => Ok(StatementKind::Comment(Box::new(stmt))),

        // MSCK [REPAIR] TABLE — Hive partition maintenance. Only the REPAIR
        // form with no ADD/DROP/SYNC PARTITIONS suffix is a cycle-2
        // statement; the other shapes get a rejection naming what was
        // actually written rather than "Statement type not supported".
        Statement::Msck(ref msck) => {
            let table = msck.table_name.to_string();
            if !msck.repair {
                return Err(sqe_core::SqeError::NotImplemented(
                    "bare MSCK TABLE is not supported; use MSCK REPAIR TABLE to rebuild the \
                     hive_external partition index"
                        .to_string(),
                ));
            }
            if msck.partition_action.is_some() {
                return Err(sqe_core::SqeError::NotImplemented(
                    "MSCK REPAIR TABLE ... ADD/DROP/SYNC PARTITIONS is not supported; \
                     plain MSCK REPAIR TABLE rebuilds the whole hive_external partition index"
                        .to_string(),
                ));
            }
            Ok(StatementKind::MsckRepairTable(table))
        }

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
    // The caller has already established `CREATE TABLE` from the token
    // stream. The parenthesized body begins at the first `(`.
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

/// Parse the tail of `CHECK ACCESS <privilege> ON <resource> FOR USER "name"`
/// (the parser is positioned after `CHECK ACCESS`). The privilege is one or
/// more words up to `ON`, kept as written.
fn parse_check_access(parser: &mut Parser) -> sqe_core::Result<StatementKind> {
    let bad_syntax = || {
        syntax_error(
            "CHECK ACCESS requires: CHECK ACCESS <privilege> ON <resource> FOR USER \"<name>\"",
        )
    };
    let mut privilege_words: Vec<String> = Vec::new();
    loop {
        match parser.peek_token().token {
            Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("ON") => {
                parser.next_token();
                break;
            }
            Token::Word(w) => {
                parser.next_token();
                privilege_words.push(w.value);
            }
            _ => return Err(bad_syntax()),
        }
    }
    if privilege_words.is_empty() {
        return Err(bad_syntax());
    }
    let resource = parser.parse_object_name(false).map_err(|_| bad_syntax())?;
    if !consume_words(parser, &["FOR", "USER"]) || at_end(parser) {
        return Err(bad_syntax());
    }
    let user = parse_principal_name(parser)?;
    expect_end(parser)?;
    let (catalog, namespace, table) = resource_parts(&resource)?;
    Ok(StatementKind::CheckAccess(CheckAccessParams {
        privilege: privilege_words.join(" "),
        catalog,
        namespace,
        table,
        user,
    }))
}

/// Parse the tail of `SHOW EFFECTIVE POLICY [FOR USER "name"] ON <table>`
/// (the parser is positioned after `SHOW EFFECTIVE POLICY`). Without `FOR
/// USER` the user is `None` and the coordinator substitutes the session user.
fn parse_show_effective_policy(parser: &mut Parser) -> sqe_core::Result<StatementKind> {
    let bad_syntax = || {
        syntax_error(
            "SHOW EFFECTIVE POLICY requires: SHOW EFFECTIVE POLICY [FOR USER \"<name>\"] ON <table>",
        )
    };
    let user = if consume_words(parser, &["FOR", "USER"]) {
        if at_end(parser) || word_at(parser, 0).as_deref() == Some("ON") {
            return Err(syntax_error(
                "SHOW EFFECTIVE POLICY FOR USER requires a non-empty user name",
            ));
        }
        Some(parse_principal_name(parser)?)
    } else {
        None
    };
    if !consume_word(parser, "ON") {
        return Err(bad_syntax());
    }
    if at_end(parser) {
        return Err(syntax_error(
            "SHOW EFFECTIVE POLICY requires a table after ON",
        ));
    }
    let table = parse_name_ref(parser)?;
    expect_end(parser)?;
    Ok(StatementKind::ShowEffectivePolicy(
        ShowEffectivePolicyParams { user, table },
    ))
}

/// The `SHOW SCHEMAS` / `SHOW TABLES` filter as text: the `FROM`/`IN` scope
/// followed by any `LIKE` / `ILIKE` pattern, e.g. `FROM ws_cbs_energy LIKE
/// 'cbs%'`. The pattern used to be dropped, so `SHOW SCHEMAS FROM c LIKE 'x%'`
/// listed every schema and dbt-trino's existence check saw a match for
/// anything. The handler splits the pattern back off.
fn show_filter_text(options: &sqlparser::ast::ShowStatementOptions) -> String {
    use sqlparser::ast::{ShowStatementFilter, ShowStatementFilterPosition};
    let mut text = options
        .show_in
        .as_ref()
        .map(|si| si.to_string())
        .unwrap_or_default();
    let filter = match &options.filter_position {
        Some(ShowStatementFilterPosition::Infix(f) | ShowStatementFilterPosition::Suffix(f)) => Some(f),
        None => None,
    };
    let pattern = match filter {
        Some(ShowStatementFilter::Like(p)) => Some(("LIKE", p)),
        Some(ShowStatementFilter::ILike(p)) => Some(("ILIKE", p)),
        _ => None,
    };
    if let Some((keyword, pattern)) = pattern {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&format!("{keyword} '{}'", pattern.replace('\'', "''")));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_case_expansion_never_changes_prescan_offsets() {
        for sql in [
            "ſHOW TAGS FROM t",
            "SELECT 'ß' FROM t FOR VERSION AS OF 1",
            "ALTER TABLE café CREATE TAG v1",
            "ALTER TABLE ß ADD PARTITION FIELD x",
            "ALTER TABLE t SET TAGS ('ß' = 'ſ')",
            "SELECT 'ıſßﬃ'",
        ] {
            // The parser may reject unsupported syntax; none may panic.
            let _ = parse_and_classify(sql);
            let _ = crate::time_travel::extract_time_travel_spec(sql);
            let _ = crate::time_travel::extract_incremental_spec(sql);
            let _ = crate::ddl::try_parse_ref_ddl(sql);
            let _ = crate::tags::try_parse_set_tags(sql);
            let _ = crate::partition_evolution::try_parse_partition_evolution(sql);
            let _ = crate::identifier_fields::try_parse_identifier_fields(sql);
            let _ = crate::hive_partitions::try_parse_hive_partition_ddl(sql);
        }
    }

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
        let result =
            parse_and_classify("CREATE TABLE IF NOT EXISTS ns.table (id BIGINT, name VARCHAR)");
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
    fn test_grant_masked_with_and_rows_where() {
        let result = parse_and_classify(
            "-- lead comment\n grant select on s.t to role r masked with (a = sha256(a)) rows where b = 1",
        );
        let Ok(StatementKind::GrantWithPolicy(g)) = result else {
            panic!("expected GrantWithPolicy, got {result:?}");
        };
        assert_eq!(g.masks[0].column, "a");
        assert_eq!(g.row_filter.as_deref(), Some("b = 1"));
        assert!(parse_and_classify("GRANT SELECT ON s.t TO ROLE r MASKED WITH (a = )").is_err());
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
            matches!(
                result,
                Ok(StatementKind::ShowGrants(
                    ShowGrantsTarget::OnResource { .. }
                ))
            ),
            "Expected ShowGrants OnResource, got: {result:?}"
        );
    }

    #[test]
    fn test_show_grants_to_group() {
        let result = parse_and_classify("SHOW GRANTS TO GROUP \"SG-Risk-Analysts\"");
        assert!(result.is_ok());
        match result.unwrap() {
            StatementKind::ShowGrants(ShowGrantsTarget::ToGrantee {
                grantee_type,
                grantee_name,
            }) => {
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
            matches!(
                result,
                Ok(StatementKind::ShowGrants(
                    ShowGrantsTarget::ToGrantee { .. }
                ))
            ),
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
        let result =
            parse_and_classify("CHECK ACCESS SELECT ON my_catalog.my_ns.orders FOR USER \"alice\"");
        assert!(
            matches!(result, Ok(StatementKind::CheckAccess(_))),
            "Expected CheckAccess, got: {result:?}"
        );
    }

    #[test]
    fn test_check_access_extracts_params() {
        let result =
            parse_and_classify("CHECK ACCESS SELECT ON cat.ns.tbl FOR USER \"bob\"").unwrap();
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
        let result = parse_and_classify("CHECK ACCESS SELECT ON a.b.c.d FOR USER \"alice\"");
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
    fn show_views_carries_the_same_filter_as_show_tables() {
        let bare = parse_and_classify("SHOW VIEWS");
        assert!(
            matches!(bare, Ok(StatementKind::ShowViews(ref f)) if f.is_empty()),
            "Expected ShowViews with no filter, got: {bare:?}",
        );
        let qualified = parse_and_classify("SHOW VIEWS FROM \"glue\".\"bi\"");
        let tables = parse_and_classify("SHOW TABLES FROM \"glue\".\"bi\"");
        match (qualified, tables) {
            (Ok(StatementKind::ShowViews(views)), Ok(StatementKind::ShowTables(tables))) => {
                assert_eq!(views, tables, "one filter parser serves both statements");
                assert!(views.contains("bi"), "{views}");
            }
            other => panic!("expected ShowViews and ShowTables, got: {other:?}"),
        }
        assert_eq!(StatementKind::ShowViews(String::new()).name(), "showviews");
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
    fn jdbc_start_transaction_classifies_through_the_trino_front_door() {
        // The exact text Trino JDBC sends after setAutoCommit(false) (#820).
        for sql in [
            "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED, READ WRITE",
            "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY",
            "START TRANSACTION READ ONLY",
        ] {
            let normalized = crate::SqlFrontDoor::new(crate::WireDialect::Trino)
                .normalize(sql)
                .expect("normalize");
            let kind = crate::classify_normalized(normalized).expect("classify");
            assert!(matches!(kind, StatementKind::Begin), "{sql}: {kind:?}");
        }
    }

    #[test]
    fn set_transaction_is_transaction_control() {
        for sql in [
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET TRANSACTION READ ONLY",
        ] {
            let kind = parse_and_classify(sql).expect(sql);
            assert!(
                matches!(kind, StatementKind::SetTransaction),
                "{sql}: {kind:?}"
            );
        }
    }

    #[test]
    fn snapshot_isolation_savepoints_and_blocks_are_refused() {
        for sql in [
            "START TRANSACTION ISOLATION LEVEL SNAPSHOT",
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
            "ROLLBACK TO SAVEPOINT sp1",
        ] {
            let err = parse_and_classify(sql).expect_err(sql);
            assert_eq!(
                err.error_code(),
                sqe_core::SqeErrorCode::NotSupported,
                "{sql}"
            );
        }
    }

    #[test]
    fn transactions_allow_reads_control_and_dml_but_not_ddl() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "TRUNCATE TABLE t",
            "SELECT 1",
            "EXPLAIN SELECT 1",
            "SHOW TABLES",
            "DESCRIBE t",
            "USE c.s",
            "COMMIT",
            "ROLLBACK",
            "START TRANSACTION",
            "SET TRANSACTION READ ONLY",
            "SET datafusion.execution.batch_size = 1024",
            "SHOW COLUMNS FROM t",
            "SHOW FUNCTIONS",
        ] {
            let kind = parse_and_classify(sql).expect(sql);
            assert!(
                kind.allowed_in_transaction(),
                "{sql} must run in a transaction"
            );
        }
        for sql in [
            "CREATE TABLE t2 AS SELECT 1 AS a",
            "CREATE TABLE t3 (a INT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN b INT",
            "CREATE VIEW v AS SELECT 1",
            "GRANT SELECT ON t TO alice",
            // ALTER TABLE operations and DROPs with no kind of their own
            // classify as Utility, which only allows named statements.
            "ALTER TABLE t ADD PARTITION (p = 1)",
            "DROP SEQUENCE s",
            "ALTER TABLE t OWNER TO bob",
            "ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (a)",
            "CALL system.expire_snapshots(table => 't')",
            "ANALYZE TABLE t COMPUTE STATISTICS",
        ] {
            let kind = parse_and_classify(sql).expect(sql);
            assert!(
                !kind.allowed_in_transaction(),
                "{sql} must be refused in a transaction"
            );
        }
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
                assert!(
                    ct.like.is_none(),
                    "plain create must not gain a LIKE clause"
                );
                assert_eq!(ct.columns.len(), 2, "columns should be preserved");
            }
            other => panic!("Expected CreateTable, got: {other:?}"),
        }
    }

    #[test]
    fn test_set_properties_dotted_hyphenated_key() {
        // Dotted + hyphenated Iceberg property names must be quoted so they
        // parse; the whole statement classifies as AlterTableProps (#338).
        let result =
            parse_and_classify("ALTER TABLE t SET PROPERTIES \"write.format.default\" = 'PARQUET'")
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
        let stmt = Parser::parse_sql(&GenericDialect {}, "COMMENT ON TABLE t IS 'desc'")
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
    fn test_drop_nested_column_classifies_to_dropnestedcolumn() {
        // #336: a dotted DROP COLUMN path routes to the nested-drop handler
        // instead of sqlparser's `Expected: end of statement, found: .`.
        let kind = parse_and_classify("ALTER TABLE t DROP COLUMN nested.subfield").unwrap();
        match kind {
            StatementKind::DropNestedColumn {
                table,
                path,
                if_exists,
            } => {
                assert_eq!(table, "t");
                assert_eq!(path, vec!["nested".to_string(), "subfield".to_string()]);
                assert!(!if_exists);
            }
            other => panic!("expected DropNestedColumn, got: {other:?}"),
        }
    }

    #[test]
    fn a_dotted_alter_column_type_classifies_to_alternestedcolumntype() {
        for sql in [
            "ALTER TABLE ns.t ALTER COLUMN s.a SET DATA TYPE BIGINT",
            "alter table ns.t alter column s.a type bigint;",
        ] {
            match parse_and_classify(sql).unwrap() {
                StatementKind::AlterNestedColumnType {
                    table,
                    path,
                    data_type,
                } => {
                    assert_eq!(table, "ns.t", "{sql}");
                    assert_eq!(path, ["s", "a"], "{sql}");
                    assert_eq!(data_type.to_string().to_uppercase(), "BIGINT", "{sql}");
                }
                other => panic!("{sql}: expected AlterNestedColumnType, got {other:?}"),
            }
        }
        let kind =
            parse_and_classify("ALTER TABLE t ALTER COLUMN s.d SET DATA TYPE DECIMAL(12, 2)");
        assert_eq!(kind.unwrap().name(), "alternestedcolumntype");
        // A top-level column still goes to sqlparser.
        let kind = parse_and_classify("ALTER TABLE t ALTER COLUMN a SET DATA TYPE BIGINT");
        assert_eq!(kind.unwrap().name(), "alterschema");
        // Trailing text after the type is a syntax error, not ignored.
        let err =
            parse_and_classify("ALTER TABLE t ALTER COLUMN s.a TYPE BIGINT junk").unwrap_err();
        assert_eq!(
            err.error_code(),
            sqe_core::SqeErrorCode::SyntaxError,
            "{err}"
        );
    }

    #[test]
    fn test_drop_nested_column_if_exists() {
        let kind = parse_and_classify("ALTER TABLE t DROP COLUMN IF EXISTS a.b.c").unwrap();
        match kind {
            StatementKind::DropNestedColumn {
                table,
                path,
                if_exists,
            } => {
                assert_eq!(table, "t");
                assert_eq!(
                    path,
                    vec!["a".to_string(), "b".to_string(), "c".to_string()]
                );
                assert!(if_exists);
            }
            other => panic!("expected DropNestedColumn, got: {other:?}"),
        }
    }

    #[test]
    fn test_drop_top_level_column_still_alter_schema() {
        // Regression: the nested-drop pre-scan must not steal a plain
        // (non-dotted) DROP COLUMN, which sqlparser parses natively.
        let kind = parse_and_classify("ALTER TABLE t DROP COLUMN x").unwrap();
        assert!(
            matches!(kind, StatementKind::AlterSchema(_)),
            "got: {kind:?}"
        );
    }

    #[test]
    fn detect_nested_drop_column_unit() {
        assert_eq!(
            detect_nested_drop_column("ALTER TABLE t DROP COLUMN a.b"),
            Some((
                "t".to_string(),
                vec!["a".to_string(), "b".to_string()],
                false
            ))
        );
        assert_eq!(
            detect_nested_drop_column("alter table ns.t drop column IF EXISTS a.b.c"),
            Some((
                "ns.t".to_string(),
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                true
            ))
        );
        assert_eq!(
            detect_nested_drop_column("ALTER TABLE t DROP COLUMN x"),
            None
        );
        assert_eq!(
            detect_nested_drop_column("ALTER TABLE t ADD COLUMN x INT"),
            None
        );
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
        let kind = parse_and_classify("ALTER TABLE ns.t ADD PARTITION FIELD year(ts)").unwrap();
        match kind {
            StatementKind::PartitionEvolution(b) => match *b {
                PartitionEvolution::AddField {
                    table,
                    transform_sql,
                } => {
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
        let kind = parse_and_classify("ALTER TABLE ns.t DROP PARTITION FIELD region").unwrap();
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
    fn classify_identifier_fields() {
        let kind = parse_and_classify("ALTER TABLE ns.t SET IDENTIFIER FIELDS (id)").unwrap();
        assert_eq!(kind.name(), "identifierfields");
        assert!(!kind.allowed_in_transaction());
        match kind {
            StatementKind::IdentifierFields(ddl) => {
                assert_eq!(ddl.table, "ns.t");
                assert!(ddl.set);
                assert_eq!(ddl.columns, vec!["id"]);
            }
            other => panic!("expected IdentifierFields, got {other:?}"),
        }
        let kind = parse_and_classify("ALTER TABLE ns.t DROP IDENTIFIER FIELDS id").unwrap();
        assert!(matches!(kind, StatementKind::IdentifierFields(d) if !d.set));
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

#[cfg(test)]
mod show_masking_policy_tests {
    use super::*;

    #[test]
    fn the_plural_with_no_name_lists_everything() {
        for sql in [
            "SHOW MASKING POLICIES",
            "show masking policies",
            "SHOW MASKING POLICIES;",
        ] {
            match parse_and_classify(sql).expect(sql) {
                StatementKind::ShowMaskingPolicies(None) => {}
                other => panic!("{sql}: expected list-all, got {other:?}"),
            }
        }
    }

    /// The singular with no name lists everything too. Refusing it would be a
    /// papercut: operators type both spellings and neither is ambiguous.
    #[test]
    fn the_singular_with_no_name_also_lists_everything() {
        match parse_and_classify("SHOW MASKING POLICY").expect("singular") {
            StatementKind::ShowMaskingPolicies(None) => {}
            other => panic!("expected list-all, got {other:?}"),
        }
    }

    #[test]
    fn a_name_filters_to_one_policy() {
        for sql in [
            "SHOW MASKING POLICY mask_ssn",
            "SHOW MASKING POLICY \"mask_ssn\"",
            "SHOW MASKING POLICIES mask_ssn;",
        ] {
            match parse_and_classify(sql).expect(sql) {
                StatementKind::ShowMaskingPolicies(Some(n)) => assert_eq!(n, "mask_ssn"),
                other => panic!("{sql}: expected a name, got {other:?}"),
            }
        }
    }

    /// The plural form is never read as "singular plus a name".
    ///
    /// I first wrote this test believing the prefix ORDER was load-bearing, and that
    /// checking the singular first would leave a stray "IES" as the policy name.
    /// It cannot: "POLICIES" is "POLIC" + "IES" while the singular prefix ends
    /// "POLICY", so the two diverge at that character and the singular prefix does
    /// not match the plural input at all. Mutation-checked, and the mutation
    /// SURVIVED, which is what exposed the wrong reasoning.
    ///
    /// The assertion is kept as a plain regression guard on the list-all form. The
    /// order of the array above is cosmetic.
    #[test]
    fn the_plural_is_not_mistaken_for_a_singular_with_a_name() {
        assert!(matches!(
            parse_and_classify("SHOW MASKING POLICIES").expect("plural"),
            StatementKind::ShowMaskingPolicies(None)
        ));
    }

    /// SHOW TAGS must keep working; the new branch sits right beside it.
    #[test]
    fn show_tags_still_parses() {
        assert!(matches!(
            parse_and_classify("SHOW TAGS ON cat.ns.t").expect("show tags"),
            StatementKind::ShowTags(ref t) if t == "cat.ns.t"
        ));
    }
}

/// Token-based dispatch (finding 107) and the Trino session statements
/// (finding 708, item 1).
#[cfg(test)]
mod token_dispatch_tests {
    use super::*;
    use sqe_core::SqeErrorCode;

    #[test]
    fn multi_statement_input_is_rejected_with_a_typed_syntax_error() {
        let err =
            parse_and_classify("CREATE TABLE t (id INT); INSERT INTO t VALUES (1)").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        assert!(
            err.to_string().contains("line 1, column 26"),
            "must name the second statement's position: {err}"
        );
        let err = parse_and_classify("SELECT 1; SELECT 2").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        // Custom grammar goes through the same guard.
        let err = parse_and_classify("SHOW TAGS ON t; SELECT 1").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
    }

    #[test]
    fn trailing_semicolons_and_semicolons_inside_literals_are_fine() {
        assert!(matches!(
            parse_and_classify("SELECT 1;"),
            Ok(StatementKind::Query(_))
        ));
        assert!(matches!(
            parse_and_classify("SELECT 'a; b';;"),
            Ok(StatementKind::Query(_))
        ));
        assert!(matches!(
            parse_and_classify("SHOW TAGS ON t;"),
            Ok(StatementKind::ShowTags(ref t)) if t == "t"
        ));
    }

    #[test]
    fn truncate_of_several_tables_errors_instead_of_truncating_the_first() {
        let err = parse_and_classify("TRUNCATE a, b").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        assert!(err.to_string().contains("a, b"), "{err}");
        assert!(matches!(
            parse_and_classify("TRUNCATE TABLE a"),
            Ok(StatementKind::Truncate(ref t)) if t == "a"
        ));
    }

    #[test]
    fn use_is_read_from_the_ast() {
        assert!(matches!(
            parse_and_classify("USE cat.sch"),
            Ok(StatementKind::Use(ref t)) if t == "cat.sch"
        ));
        assert!(matches!(
            parse_and_classify("use \"My Cat\".\"my schema\""),
            Ok(StatementKind::Use(ref t)) if t == "\"My Cat\".\"my schema\""
        ));
        // Snowflake-style role/warehouse forms are not SQE grammar. The
        // Generic dialect rejects them outright; either way, not a `Use`.
        assert!(parse_and_classify("USE ROLE admin").is_err());
    }

    #[test]
    fn a_leading_comment_does_not_hide_the_statement() {
        assert!(matches!(
            parse_and_classify("-- switch\nUSE x"),
            Ok(StatementKind::Use(ref t)) if t == "x"
        ));
        assert!(matches!(
            parse_and_classify("/* c */ show tags on t"),
            Ok(StatementKind::ShowTags(ref t)) if t == "t"
        ));
        assert!(matches!(
            parse_and_classify("-- c\nALTER TABLE t CREATE BRANCH b"),
            Ok(StatementKind::RefDdl(_))
        ));
        assert!(matches!(
            parse_and_classify("-- c\nSET WRITE_BRANCH = 'b'"),
            Ok(StatementKind::SetWriteBranch(Some(ref b))) if b == "b"
        ));
    }

    #[test]
    fn parse_errors_are_typed_syntax_errors_with_the_parser_error_as_source() {
        let err = parse_and_classify("SELEC 1").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        assert!(err.to_string().contains("Parse error"), "{err}");
        assert!(std::error::Error::source(&err).is_some());
        let err = parse_and_classify("").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        let err = parse_and_classify("SHOW TAGS").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        let err = parse_and_classify("SELECT 'unterminated").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
    }

    #[test]
    fn quoted_identifiers_never_match_the_custom_dispatch() {
        // `"show"` is a table name, not the SHOW keyword.
        let err = parse_and_classify("\"show\" tags on t").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        // A statement whose first word only looks like a keyword.
        assert!(matches!(
            parse_and_classify("SELECT * FROM analyze"),
            Ok(StatementKind::Query(_))
        ));
    }

    #[test]
    fn custom_statements_reject_trailing_garbage() {
        for sql in [
            "SHOW STATS FOR t extra",
            "SHOW TAGS ON t LIKE 'x'",
            "SHOW SECRETS please",
            "SHOW CREATE SCHEMA s t",
            "SET WRITE_BRANCH = 'b' extra",
        ] {
            let err = parse_and_classify(sql).unwrap_err();
            assert_eq!(err.error_code(), SqeErrorCode::SyntaxError, "{sql}");
        }
    }

    #[test]
    fn show_grants_and_check_access_unquote_their_parts() {
        match parse_and_classify("SHOW GRANTS ON \"cat\".\"my ns\".t").unwrap() {
            StatementKind::ShowGrants(ShowGrantsTarget::OnResource {
                catalog,
                namespace,
                table,
            }) => {
                assert_eq!(catalog.as_deref(), Some("cat"));
                assert_eq!(namespace.as_deref(), Some("my ns"));
                assert_eq!(table.as_deref(), Some("t"));
            }
            other => panic!("{other:?}"),
        }
        match parse_and_classify("check access CREATE TABLE on cat.ns for user 'al ice'").unwrap() {
            StatementKind::CheckAccess(p) => {
                assert_eq!(p.privilege, "CREATE TABLE");
                assert_eq!(p.catalog.as_deref(), Some("cat"));
                assert_eq!(p.namespace.as_deref(), Some("ns"));
                assert_eq!(p.table, None);
                assert_eq!(p.user, "al ice");
            }
            other => panic!("{other:?}"),
        }
        let err = parse_and_classify("SHOW GRANTS TO").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        let err = parse_and_classify("CHECK ACCESS ON t FOR USER x").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
    }

    #[test]
    fn explain_full_keeps_the_inner_text_verbatim() {
        match parse_and_classify("-- c\nexplain full SELECT  a,b FROM t WHERE s = 'x;y';").unwrap()
        {
            StatementKind::ExplainFull(inner) => {
                assert_eq!(inner, "SELECT  a,b FROM t WHERE s = 'x;y'")
            }
            other => panic!("{other:?}"),
        }
        assert!(parse_and_classify("EXPLAIN FULL").is_err());
    }

    #[test]
    fn analyze_accepts_and_ignores_trino_properties() {
        assert!(matches!(
            parse_and_classify("ANALYZE \"c\".\"s\".t WITH (columns = ARRAY['x'])"),
            Ok(StatementKind::Analyze(ref t)) if t == "\"c\".\"s\".t"
        ));
        assert!(parse_and_classify("ANALYZE").is_err());
        assert!(parse_and_classify("ANALYZE TABLE").is_err());
    }

    // -- Trino session statements (finding 708, item 1) --------------------

    #[test]
    fn prepare_from_parses_the_body_at_prepare_time() {
        let err = parse_and_classify("PREPARE p FROM SELEC 1").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        match parse_and_classify("PREPARE p FROM SELECT * FROM t WHERE x = ?").unwrap() {
            StatementKind::Prepare { name, body } => {
                assert_eq!(name, "p");
                assert!(matches!(*body, Statement::Query(_)));
            }
            other => panic!("{other:?}"),
        }
        // PostgreSQL spelling classifies the same way.
        assert!(matches!(
            parse_and_classify("PREPARE p AS SELECT 1"),
            Ok(StatementKind::Prepare { .. })
        ));
        assert_eq!(
            parse_and_classify("PREPARE p FROM SELECT 1")
                .unwrap()
                .name(),
            "prepare"
        );
    }

    #[test]
    fn execute_and_deallocate_come_from_the_ast() {
        match parse_and_classify("EXECUTE p USING 1, 'a'").unwrap() {
            StatementKind::Execute { name, params } => {
                assert_eq!(name, "p");
                assert_eq!(params.len(), 2);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            parse_and_classify("EXECUTE p"),
            Ok(StatementKind::Execute { ref name, ref params }) if name == "p" && params.is_empty()
        ));
        assert!(matches!(
            parse_and_classify("DEALLOCATE PREPARE p"),
            Ok(StatementKind::Deallocate(ref n)) if n == "p"
        ));
        assert!(matches!(
            parse_and_classify("deallocate p"),
            Ok(StatementKind::Deallocate(ref n)) if n == "p"
        ));
    }

    #[test]
    fn session_property_statements() {
        match parse_and_classify("SET SESSION iceberg.compression_codec = 'ZSTD'").unwrap() {
            StatementKind::SetSession { name, value } => {
                assert_eq!(name, "iceberg.compression_codec");
                assert_eq!(value, "ZSTD");
            }
            other => panic!("{other:?}"),
        }
        match parse_and_classify("set session query_max_memory = 1").unwrap() {
            StatementKind::SetSession { name, value } => {
                assert_eq!(name, "query_max_memory");
                assert_eq!(value, "1");
            }
            other => panic!("{other:?}"),
        }
        let err = parse_and_classify("SET SESSION x").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::SyntaxError);
        assert!(matches!(
            parse_and_classify("RESET SESSION iceberg.compression_codec"),
            Ok(StatementKind::ResetSession(ref n)) if n == "iceberg.compression_codec"
        ));
        assert!(matches!(
            parse_and_classify("SHOW SESSION"),
            Ok(StatementKind::ShowSession { like: None })
        ));
        assert!(matches!(
            parse_and_classify("SHOW SESSION LIKE 'iceberg%'"),
            Ok(StatementKind::ShowSession { like: Some(ref p) }) if p == "iceberg%"
        ));
        assert!(matches!(
            parse_and_classify("SET CATALOG iceberg"),
            Ok(StatementKind::SetCatalog(ref c)) if c == "iceberg"
        ));
        // Plain SET stays a utility statement for DataFusion.
        assert!(matches!(
            parse_and_classify("SET x = 1"),
            Ok(StatementKind::Utility(_))
        ));
        // SET WRITE_BRANCH is still its own kind.
        assert!(matches!(
            parse_and_classify("SET WRITE_BRANCH = \"dev\""),
            Ok(StatementKind::SetWriteBranch(Some(ref b))) if b == "dev"
        ));
    }

    #[test]
    fn describe_output_and_input() {
        assert!(matches!(
            parse_and_classify("DESCRIBE OUTPUT p"),
            Ok(StatementKind::DescribeOutput(ref n)) if n == "p"
        ));
        assert!(matches!(
            parse_and_classify("desc input p"),
            Ok(StatementKind::DescribeInput(ref n)) if n == "p"
        ));
        // A plain DESCRIBE <table> still reaches sqlparser.
        assert!(matches!(
            parse_and_classify("DESCRIBE t"),
            Ok(StatementKind::Utility(_))
        ));
    }

    #[test]
    fn update_type_of_kinds() {
        use crate::metadata::UpdateType;
        assert_eq!(
            parse_and_classify("CREATE VIEW v AS SELECT 1")
                .unwrap()
                .update_type(),
            Some(UpdateType::CreateView)
        );
        assert_eq!(
            parse_and_classify("ALTER TABLE t ADD PARTITION FIELD bucket(16, id)")
                .unwrap()
                .update_type(),
            Some(UpdateType::AlterTable)
        );
        assert_eq!(
            parse_and_classify("SET SESSION a = 1")
                .unwrap()
                .update_type(),
            Some(UpdateType::SetSession)
        );
        assert_eq!(
            parse_and_classify("SHOW TAGS ON t").unwrap().update_type(),
            None
        );
        assert_eq!(parse_and_classify("SELECT 1").unwrap().update_type(), None);
    }

    /// The LIKE pattern used to be dropped, so dbt's existence check
    /// `SHOW SCHEMAS FROM c LIKE 'x'` matched every schema.
    #[test]
    fn show_filters_keep_the_like_pattern() {
        let kind = |sql: &str| parse_and_classify(sql).unwrap();
        match kind("SHOW SCHEMAS FROM ws_cbs_energy LIKE 'cbs%'") {
            StatementKind::ShowSchemas(f) => assert_eq!(f, "FROM ws_cbs_energy LIKE 'cbs%'"),
            other => panic!("{other:?}"),
        }
        match kind("SHOW TABLES FROM ws.cbs_silver LIKE 'o''x%'") {
            StatementKind::ShowTables(f) => assert_eq!(f, "FROM ws.cbs_silver LIKE 'o''x%'"),
            other => panic!("{other:?}"),
        }
        match kind("SHOW SCHEMAS FROM ws_cbs_energy") {
            StatementKind::ShowSchemas(f) => assert_eq!(f, "FROM ws_cbs_energy"),
            other => panic!("{other:?}"),
        }
    }

    /// #811: a write hiding inside a query or EXPLAIN is refused at
    /// classification, so no kind-keyed check takes it for a read.
    #[test]
    fn queries_hiding_a_write_are_refused() {
        for (sql, shape) in [
            ("SELECT 1 INTO t", "select into"),
            ("SELECT * INTO datafusion.public.t FROM ns.src", "select into"),
            ("SELECT a FROM (SELECT 1 AS a) s UNION ALL SELECT 2 INTO t", "select into"),
            ("WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT * FROM x", "insert"),
            ("WITH x AS (SELECT 1 AS a) DELETE FROM t", "delete"),
            ("WITH x AS (SELECT 1 AS a) UPDATE t SET a = 1", "update"),
            ("EXPLAIN SELECT 1 INTO t", "select into"),
            ("EXPLAIN ANALYZE WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT * FROM x", "insert"),
        ] {
            let err = parse_and_classify(sql).expect_err(sql);
            assert_eq!(err.error_code(), SqeErrorCode::NotSupported, "{sql}");
            assert!(err.to_string().contains(&format!("{shape} statement")), "{sql}: {err}");
        }
        for sql in [
            "SELECT 1",
            "WITH x AS (SELECT 1 AS a) SELECT * FROM x",
            "SELECT * FROM t WHERE a IN (SELECT a FROM u)",
            "EXPLAIN SELECT 1",
            "EXPLAIN ANALYZE SELECT 1",
        ] {
            parse_and_classify(sql).expect(sql);
        }
        // A data statement is not a hidden write.
        assert!(matches!(
            parse_and_classify("INSERT INTO t SELECT * FROM u").unwrap(),
            StatementKind::Insert(_)
        ));
    }

    /// Every EXPLAIN form classifies its inner statement and explains only a
    /// query (#711, #811).
    #[test]
    fn explain_refuses_anything_but_a_query() {
        for sql in [
            "EXPLAIN SELECT 1",
            "EXPLAIN ANALYZE WITH t AS (SELECT 1 AS n) SELECT n FROM t",
            "EXPLAIN FULL SELECT 1",
            "EXPLAIN FULL WITH t AS (SELECT 1 AS n) SELECT n FROM t",
        ] {
            parse_and_classify(sql).expect(sql);
        }
        for (sql, inner) in [
            ("EXPLAIN INSERT INTO lake.sales.orders SELECT 1, 2.0, 'x'", "insert"),
            ("EXPLAIN ANALYZE DELETE FROM lake.sales.orders WHERE id = 1", "delete"),
            ("EXPLAIN DROP TABLE lake.sales.orders", "drop"),
            ("EXPLAIN SHOW TABLES", "showtables"),
            ("EXPLAIN FULL UPDATE t SET a = 1", "update"),
            (
                "EXPLAIN FULL CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION 's3://b/p'",
                "createexternaltable",
            ),
            ("EXPLAIN FULL SHOW TAGS ON ns.t", "showtags"),
            ("EXPLAIN FULL EXPLAIN FULL SELECT 1", "explain_full"),
        ] {
            let err = parse_and_classify(sql).expect_err(sql);
            assert_eq!(err.error_code(), SqeErrorCode::NotSupported, "{sql}");
            assert_eq!(
                err.to_string(),
                format!(
                    "EXPLAIN only accepts a query; {inner} statements are not explained (run them directly)"
                ),
                "{sql}"
            );
        }
    }

    #[test]
    fn write_and_read_predicates_agree() {
        for (sql, data_write, rows_affected, read_only) in [
            ("SELECT 1", false, false, true),
            ("SHOW TABLES", false, false, true),
            ("INSERT INTO t VALUES (1)", true, true, false),
            ("CREATE TABLE t AS SELECT 1 AS a", true, true, false),
            ("UPDATE t SET a = 1", true, true, false),
            ("DELETE FROM t", true, true, false),
            ("MERGE INTO t USING u ON t.a = u.a WHEN MATCHED THEN DELETE", true, true, false),
            ("TRUNCATE TABLE t", true, false, false),
            ("CREATE TABLE t (a INT)", false, false, false),
            ("EXPLAIN SELECT 1", false, false, false),
        ] {
            let k = parse_and_classify(sql).expect(sql);
            assert_eq!(k.is_data_write(), data_write, "{sql}");
            assert_eq!(k.reports_rows_affected(), rows_affected, "{sql}");
            assert_eq!(k.is_read_only(), read_only, "{sql}");
        }
    }
}
