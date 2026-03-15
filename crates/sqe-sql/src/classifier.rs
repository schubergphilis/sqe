use sqlparser::ast::{AlterTableOperation, ObjectType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Classifies a parsed SQL statement into a high-level kind
/// for routing to the appropriate handler in the coordinator.
#[derive(Debug)]
pub enum StatementKind {
    Query(Box<Statement>),
    Ctas(Box<Statement>),
    Insert(Box<Statement>),
    Merge(Box<Statement>),
    Delete(Box<Statement>),
    Drop(Box<Statement>),
    Rename(Box<Statement>),
    CreateView(Box<Statement>),
    DropView(Box<Statement>),
    ShowCatalogs,
    ShowSchemas(String),
    ShowTables(String),
    Policy(Box<Statement>),
    Utility(Box<Statement>),
}

impl StatementKind {
    /// Return a stable lowercase label for metrics and audit logging.
    pub fn name(&self) -> &'static str {
        match self {
            StatementKind::Query(_) => "query",
            StatementKind::Ctas(_) => "ctas",
            StatementKind::Insert(_) => "insert",
            StatementKind::Merge(_) => "merge",
            StatementKind::Delete(_) => "delete",
            StatementKind::Drop(_) => "drop",
            StatementKind::Rename(_) => "rename",
            StatementKind::CreateView(_) => "createview",
            StatementKind::DropView(_) => "dropview",
            StatementKind::ShowCatalogs => "showcatalogs",
            StatementKind::ShowSchemas(_) => "showschemas",
            StatementKind::ShowTables(_) => "showtables",
            StatementKind::Policy(_) => "policy",
            StatementKind::Utility(_) => "utility",
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

        // CREATE TABLE ... AS SELECT (CTAS) vs regular CREATE TABLE
        Statement::CreateTable(ref ct) => {
            if ct.query.is_some() {
                Ok(StatementKind::Ctas(Box::new(stmt)))
            } else {
                Ok(StatementKind::Utility(Box::new(stmt)))
            }
        }

        // INSERT INTO
        Statement::Insert(_) => Ok(StatementKind::Insert(Box::new(stmt))),

        // MERGE INTO
        Statement::Merge { .. } => Ok(StatementKind::Merge(Box::new(stmt))),

        // DELETE FROM
        Statement::Delete(_) => Ok(StatementKind::Delete(Box::new(stmt))),

        // DROP TABLE / DROP VIEW / DROP other
        Statement::Drop {
            object_type: ObjectType::View,
            ..
        } => Ok(StatementKind::DropView(Box::new(stmt))),

        Statement::Drop {
            object_type: ObjectType::Table,
            ..
        } => Ok(StatementKind::Drop(Box::new(stmt))),

        // For other DROP types (index, schema, etc.), treat as utility
        Statement::Drop { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // ALTER TABLE — check for RENAME operations
        Statement::AlterTable {
            ref operations, ..
        } => {
            let is_rename = operations.iter().any(|op| {
                matches!(
                    op,
                    AlterTableOperation::RenameTable { .. }
                        | AlterTableOperation::RenameColumn { .. }
                )
            });
            if is_rename {
                Ok(StatementKind::Rename(Box::new(stmt)))
            } else {
                Ok(StatementKind::Utility(Box::new(stmt)))
            }
        }

        // CREATE VIEW
        Statement::CreateView { .. } => Ok(StatementKind::CreateView(Box::new(stmt))),

        // GRANT → Policy
        Statement::Grant { .. } => Ok(StatementKind::Policy(Box::new(stmt))),

        // REVOKE → Policy
        Statement::Revoke { .. } => Ok(StatementKind::Policy(Box::new(stmt))),

        // EXPLAIN → Utility
        Statement::Explain { .. } => Ok(StatementKind::Utility(Box::new(stmt))),
        Statement::ExplainTable { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // SET → Utility
        Statement::SetVariable { .. } => Ok(StatementKind::Utility(Box::new(stmt))),
        Statement::SetRole { .. } => Ok(StatementKind::Utility(Box::new(stmt))),
        Statement::SetTimeZone { .. } => Ok(StatementKind::Utility(Box::new(stmt))),
        Statement::SetNames { .. } => Ok(StatementKind::Utility(Box::new(stmt))),
        Statement::SetNamesDefault {} => Ok(StatementKind::Utility(Box::new(stmt))),

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

        // Other SHOW variants → Utility
        Statement::ShowFunctions { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCollation { .. }
        | Statement::ShowViews { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

        // UPDATE → Utility (not in the spec but common)
        Statement::Update { .. } => Ok(StatementKind::Utility(Box::new(stmt))),

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
    fn test_create_table_without_query_is_utility() {
        let result = parse_and_classify("CREATE TABLE foo (id INT)");
        assert!(matches!(result, Ok(StatementKind::Utility(_))));
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
    fn test_alter_table_add_column_is_utility() {
        let result = parse_and_classify("ALTER TABLE foo ADD COLUMN bar INT");
        assert!(matches!(result, Ok(StatementKind::Utility(_))));
    }

    #[test]
    fn test_show_databases_is_show_catalogs() {
        let result = parse_and_classify("SHOW DATABASES");
        assert!(matches!(result, Ok(StatementKind::ShowCatalogs)));
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
}
