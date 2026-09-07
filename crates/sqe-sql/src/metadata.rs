//! AST-level facts about a statement that the wire protocols used to derive
//! from the SQL text (review finding 704).
//!
//! The Trino HTTP layer decided "is this a metadata query" with a substring
//! test on the raw statement and reported the `updateType` from the first
//! word. Both fire on literals and comments and both misreport anything that
//! is not the common case. Everything here reads the parsed statement.

use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_relations, visit_relations_mut, DescribeAlias, Ident, ObjectName, ObjectNamePart,
    ObjectType, Statement,
};

const INFORMATION_SCHEMA: &str = "information_schema";

fn ident_value(part: &ObjectNamePart) -> Option<&str> {
    match part {
        ObjectNamePart::Identifier(Ident { value, .. }) => Some(value.as_str()),
        ObjectNamePart::Function(_) => None,
    }
}

/// True when a relation reference in the statement names the
/// `information_schema` schema: `information_schema.tables`,
/// `cat.information_schema.columns`, inside CTEs, subqueries, joins and
/// `INSERT ... SELECT` sources alike. String literals and comments never
/// match because they are not relations.
pub fn reads_information_schema(stmt: &Statement) -> bool {
    let mut found = false;
    let _ = visit_relations(stmt, |name: &ObjectName| {
        if names_information_schema(name) {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

fn names_information_schema(name: &ObjectName) -> bool {
    let parts = &name.0;
    // The schema is the second-to-last part of a qualified table reference.
    parts.len() >= 2
        && ident_value(&parts[parts.len() - 2])
            .is_some_and(|v| v.eq_ignore_ascii_case(INFORMATION_SCHEMA))
}

/// True when the statement is a catalog-metadata read for which the Trino
/// layer reshapes the result (catalog scoping, Trino type names):
/// `information_schema` reads, `SHOW COLUMNS`, and `DESCRIBE`/`DESC <table>`.
/// Replaces the text-based `is_metadata_query`.
pub fn is_metadata_query(stmt: &Statement) -> bool {
    match stmt {
        Statement::ShowColumns { .. } => true,
        Statement::ExplainTable { describe_alias, .. } => {
            matches!(
                describe_alias,
                DescribeAlias::Describe | DescribeAlias::Desc
            )
        }
        _ => reads_information_schema(stmt),
    }
}

/// Qualify every two-part `information_schema.<table>` relation with
/// `catalog` (quoted), so the read resolves against the session catalog
/// instead of the engine default. Three-part names are left alone: the user
/// already chose a catalog. Returns true when at least one name changed.
///
/// This is an AST pass over relation names, so `SELECT
/// 'information_schema.tables'` and `-- information_schema.tables` comments
/// are never touched (finding 704).
pub fn qualify_information_schema(stmt: &mut Statement, catalog: &str) -> bool {
    let mut changed = false;
    let _ = visit_relations_mut(stmt, |name: &mut ObjectName| {
        if name.0.len() == 2
            && ident_value(&name.0[0]).is_some_and(|v| v.eq_ignore_ascii_case(INFORMATION_SCHEMA))
        {
            name.0.insert(
                0,
                ObjectNamePart::Identifier(Ident::with_quote('"', catalog)),
            );
            changed = true;
        }
        ControlFlow::<()>::Continue(())
    });
    changed
}

/// The Trino `updateType` of a statement: what kind of state change it makes.
/// `None` from [`update_type`] means the statement is a read (query, SHOW,
/// EXPLAIN, DESCRIBE) and carries no update type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateType {
    CreateTable,
    CreateView,
    CreateSchema,
    CreateRole,
    CreateMaterializedView,
    Insert,
    Update,
    Delete,
    Merge,
    DropTable,
    DropView,
    DropSchema,
    DropRole,
    AlterTable,
    AlterSchema,
    RenameTable,
    Truncate,
    Grant,
    Revoke,
    Deny,
    Comment,
    Analyze,
    Call,
    Use,
    SetSession,
    ResetSession,
    Prepare,
    Deallocate,
    StartTransaction,
    Commit,
    Rollback,
}

impl UpdateType {
    /// The string Trino puts in `QueryResults.updateType`.
    pub fn as_trino_str(self) -> &'static str {
        match self {
            UpdateType::CreateTable => "CREATE TABLE",
            UpdateType::CreateView => "CREATE VIEW",
            UpdateType::CreateSchema => "CREATE SCHEMA",
            UpdateType::CreateRole => "CREATE ROLE",
            UpdateType::CreateMaterializedView => "CREATE MATERIALIZED VIEW",
            UpdateType::Insert => "INSERT",
            UpdateType::Update => "UPDATE",
            UpdateType::Delete => "DELETE",
            UpdateType::Merge => "MERGE",
            UpdateType::DropTable => "DROP TABLE",
            UpdateType::DropView => "DROP VIEW",
            UpdateType::DropSchema => "DROP SCHEMA",
            UpdateType::DropRole => "DROP ROLE",
            UpdateType::AlterTable => "ALTER TABLE",
            UpdateType::AlterSchema => "ALTER SCHEMA",
            UpdateType::RenameTable => "RENAME TABLE",
            UpdateType::Truncate => "TRUNCATE TABLE",
            UpdateType::Grant => "GRANT",
            UpdateType::Revoke => "REVOKE",
            UpdateType::Deny => "DENY",
            UpdateType::Comment => "COMMENT",
            UpdateType::Analyze => "ANALYZE",
            UpdateType::Call => "CALL",
            UpdateType::Use => "USE",
            UpdateType::SetSession => "SET SESSION",
            UpdateType::ResetSession => "RESET SESSION",
            UpdateType::Prepare => "PREPARE",
            // Trino's DeallocateTask reports "DEALLOCATE", not the full verb.
            UpdateType::Deallocate => "DEALLOCATE",
            UpdateType::StartTransaction => "START TRANSACTION",
            UpdateType::Commit => "COMMIT",
            UpdateType::Rollback => "ROLLBACK",
        }
    }
}

/// The Trino `updateType` for a parsed statement, or `None` for reads.
/// Replaces the first-word `classify_update_type`, which reported `CREATE
/// VIEW`, `CREATE SCHEMA` and `CREATE ROLE` all as `CREATE TABLE`.
pub fn update_type(stmt: &Statement) -> Option<UpdateType> {
    use UpdateType as U;
    Some(match stmt {
        Statement::CreateTable(_) => U::CreateTable,
        Statement::CreateView(view) => {
            if view.materialized {
                U::CreateMaterializedView
            } else {
                U::CreateView
            }
        }
        Statement::CreateSchema { .. } => U::CreateSchema,
        Statement::CreateRole(_) => U::CreateRole,
        Statement::Insert(_) => U::Insert,
        Statement::Update { .. } => U::Update,
        Statement::Delete(_) => U::Delete,
        Statement::Merge { .. } => U::Merge,
        Statement::Drop { object_type, .. } => match object_type {
            ObjectType::View | ObjectType::MaterializedView => U::DropView,
            ObjectType::Schema | ObjectType::Database => U::DropSchema,
            ObjectType::Role => U::DropRole,
            _ => U::DropTable,
        },
        Statement::AlterTable(alter) => {
            if alter
                .operations
                .iter()
                .any(|op| matches!(op, sqlparser::ast::AlterTableOperation::RenameTable { .. }))
            {
                U::RenameTable
            } else {
                U::AlterTable
            }
        }
        Statement::AlterSchema(_) => U::AlterSchema,
        Statement::Truncate(_) => U::Truncate,
        Statement::Grant(_) => U::Grant,
        Statement::Revoke(_) => U::Revoke,
        Statement::Deny(_) => U::Deny,
        Statement::Comment { .. } => U::Comment,
        Statement::Analyze(_) => U::Analyze,
        Statement::Call(_) => U::Call,
        Statement::Use(_) => U::Use,
        Statement::Prepare { .. } => U::Prepare,
        Statement::Deallocate { .. } => U::Deallocate,
        Statement::StartTransaction { .. } => U::StartTransaction,
        Statement::Commit { .. } => U::Commit,
        Statement::Rollback { .. } => U::Rollback,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .remove(0)
    }

    #[test]
    fn information_schema_relation_is_detected_everywhere() {
        for sql in [
            "SELECT * FROM information_schema.tables",
            "SELECT * FROM INFORMATION_SCHEMA.columns WHERE table_name = 't'",
            "SELECT * FROM cat.information_schema.tables",
            "WITH c AS (SELECT * FROM information_schema.columns) SELECT * FROM c",
            "SELECT * FROM t WHERE x IN (SELECT table_name FROM information_schema.tables)",
            "SELECT * FROM t JOIN information_schema.tables i ON t.n = i.table_name",
            "INSERT INTO audit SELECT table_name FROM information_schema.tables",
        ] {
            assert!(reads_information_schema(&parse(sql)), "{sql}");
        }
    }

    #[test]
    fn literal_and_comment_do_not_count_as_information_schema() {
        for sql in [
            "SELECT 'information_schema.tables'",
            "SELECT * FROM audit.events WHERE note LIKE '%information_schema%'",
            "SELECT 1 -- information_schema.tables",
            "SELECT /* information_schema */ * FROM t",
            "SELECT * FROM information_schema_backup.tables",
        ] {
            assert!(!reads_information_schema(&parse(sql)), "{sql}");
        }
    }

    #[test]
    fn describe_and_show_columns_are_metadata_queries() {
        assert!(is_metadata_query(&parse("DESCRIBE t")));
        assert!(is_metadata_query(&parse("DESC ns.t")));
        assert!(is_metadata_query(&parse("SHOW COLUMNS FROM t")));
        assert!(!is_metadata_query(&parse("SELECT * FROM t")));
        assert!(!is_metadata_query(&parse("EXPLAIN SELECT 1")));
    }

    #[test]
    fn qualification_touches_relations_only() {
        let mut stmt = parse(
            "SELECT 'information_schema.tables' AS lit, table_name \
             FROM information_schema.tables -- information_schema.x",
        );
        assert!(qualify_information_schema(&mut stmt, "iceberg"));
        assert_eq!(
            stmt.to_string(),
            "SELECT 'information_schema.tables' AS lit, table_name FROM \"iceberg\".information_schema.tables"
        );
    }

    #[test]
    fn qualification_leaves_literal_only_statement_verbatim() {
        let mut stmt = parse("SELECT 'information_schema.tables'");
        assert!(!qualify_information_schema(&mut stmt, "iceberg"));
        assert_eq!(stmt.to_string(), "SELECT 'information_schema.tables'");
    }

    #[test]
    fn qualification_skips_already_qualified_names() {
        let mut stmt = parse("SELECT * FROM other.information_schema.tables");
        assert!(!qualify_information_schema(&mut stmt, "iceberg"));
    }

    #[test]
    fn qualification_reaches_ctes_and_subqueries() {
        let mut stmt = parse(
            "WITH c AS (SELECT * FROM information_schema.columns) \
             SELECT * FROM c WHERE x IN (SELECT 1 FROM information_schema.tables)",
        );
        assert!(qualify_information_schema(&mut stmt, "cat"));
        let out = stmt.to_string();
        assert_eq!(
            out.matches("\"cat\".information_schema").count(),
            2,
            "{out}"
        );
    }

    #[test]
    fn update_type_distinguishes_create_forms() {
        assert_eq!(
            update_type(&parse("CREATE VIEW v AS SELECT 1")),
            Some(UpdateType::CreateView)
        );
        assert_eq!(
            update_type(&parse("CREATE SCHEMA s")),
            Some(UpdateType::CreateSchema)
        );
        assert_eq!(
            update_type(&parse("CREATE ROLE r")),
            Some(UpdateType::CreateRole)
        );
        assert_eq!(
            update_type(&parse("CREATE TABLE t (id INT)")),
            Some(UpdateType::CreateTable)
        );
        assert_eq!(
            update_type(&parse("CREATE TABLE t AS SELECT 1")),
            Some(UpdateType::CreateTable)
        );
        assert_eq!(UpdateType::CreateView.as_trino_str(), "CREATE VIEW");
    }

    #[test]
    fn update_type_covers_dml_and_drops() {
        assert_eq!(
            update_type(&parse("INSERT INTO t VALUES (1)")),
            Some(UpdateType::Insert)
        );
        assert_eq!(
            update_type(&parse("DELETE FROM t")),
            Some(UpdateType::Delete)
        );
        assert_eq!(
            update_type(&parse("DROP VIEW v")),
            Some(UpdateType::DropView)
        );
        assert_eq!(
            update_type(&parse("DROP SCHEMA s")),
            Some(UpdateType::DropSchema)
        );
        assert_eq!(
            update_type(&parse("ALTER TABLE t RENAME TO u")),
            Some(UpdateType::RenameTable)
        );
        assert_eq!(update_type(&parse("SELECT 1")), None);
        assert_eq!(update_type(&parse("SHOW TABLES")), None);
    }
}
