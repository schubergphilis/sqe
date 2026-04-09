use sqlparser::ast::{AlterTableOperation, ObjectType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
    Policy(Box<Statement>),
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
    /// ALTER TABLE ... SET TBLPROPERTIES (...) — update Iceberg table properties
    AlterTableProps(Box<Statement>),
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
            StatementKind::Policy(_) => "policy",
            StatementKind::Utility(_) => "utility",
            StatementKind::ExplainFull(_) => "explain_full",
            StatementKind::Begin => "begin",
            StatementKind::Commit => "commit",
            StatementKind::Rollback => "rollback",
            StatementKind::Use(_) => "use",
            StatementKind::ShowCreateTable(_) => "showcreatetable",
            StatementKind::Truncate(_) => "truncate",
            StatementKind::Call(_) => "call",
            StatementKind::AlterTableProps(_) => "altertableprops",
        }
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
    if upper.starts_with("EXPLAIN FULL ") {
        let inner = trimmed["EXPLAIN FULL ".len()..].trim().to_string();
        return Ok(StatementKind::ExplainFull(inner));
    }

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
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

        // GRANT → Policy
        Statement::Grant { .. } => Ok(StatementKind::Policy(Box::new(stmt))),

        // REVOKE → Policy
        Statement::Revoke { .. } => Ok(StatementKind::Policy(Box::new(stmt))),

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

        // CALL procedure — not supported, returns informative error
        Statement::Call(_) => Ok(StatementKind::Call(Box::new(stmt))),

        _ => Err(sqe_core::SqeError::NotImplemented(format!(
            "Statement type not supported: {stmt}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_is_query() {
        let result = parse_and_classify("SELECT 1");
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
    fn test_grant_is_policy() {
        let result = parse_and_classify("GRANT SELECT ON foo TO bar");
        assert!(matches!(result, Ok(StatementKind::Policy(_))));
    }

    #[test]
    fn test_revoke_is_policy() {
        let result = parse_and_classify("REVOKE SELECT ON foo FROM bar");
        assert!(matches!(result, Ok(StatementKind::Policy(_))));
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
}
