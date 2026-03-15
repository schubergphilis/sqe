use std::sync::Arc;

use iceberg::{Catalog, NamespaceIdent, TableIdent};
use sqlparser::ast::{AlterTableOperation, ObjectName, ObjectType, Statement};
use tracing::info;

use sqe_catalog::SessionCatalog;
use sqe_core::{Session, SqeConfig, SqeError};

/// Handles catalog DDL operations (DROP TABLE, ALTER TABLE RENAME, views).
///
/// These operations go directly through the Iceberg REST catalog API
/// rather than through DataFusion's query engine.
pub struct CatalogOps {
    config: SqeConfig,
}

impl CatalogOps {
    pub fn new(config: SqeConfig) -> Self {
        Self { config }
    }

    /// Drop a table via the Iceberg REST catalog.
    ///
    /// Extracts the table name from a `DROP TABLE` statement and calls
    /// the catalog's `drop_table` method. If `IF EXISTS` is specified
    /// and the table is not found, this returns `Ok(())`.
    pub async fn drop_table(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let (names, if_exists) = match stmt {
            Statement::Drop {
                names, if_exists, ..
            } => (names, *if_exists),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DROP TABLE statement, got: {other}"
                )));
            }
        };

        // DROP TABLE supports multiple names, but we handle one at a time
        let table_name = names.first().ok_or_else(|| {
            SqeError::Execution("DROP TABLE requires at least one table name".to_string())
        })?;

        let (namespace, name) = parse_table_ref(table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            if_exists = if_exists,
            "Dropping table"
        );

        let catalog = self.create_catalog_bridge(session).await?;

        match catalog.drop_table(&table_ident).await {
            Ok(()) => Ok(()),
            Err(e) if if_exists && is_table_not_found(&e) => {
                info!(
                    table = %table_ident,
                    "Table not found, IF EXISTS specified — ignoring"
                );
                Ok(())
            }
            Err(e) => Err(SqeError::Catalog(format!("Failed to drop table: {e}"))),
        }
    }

    /// Rename a table via the Iceberg REST catalog.
    ///
    /// Extracts the source and destination table names from an
    /// `ALTER TABLE ... RENAME TO` statement.
    pub async fn rename_table(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let (source_name, operations) = match stmt {
            Statement::AlterTable {
                name, operations, ..
            } => (name, operations),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected ALTER TABLE statement, got: {other}"
                )));
            }
        };

        // Find the RenameTable operation
        let dest_name = operations
            .iter()
            .find_map(|op| match op {
                AlterTableOperation::RenameTable { table_name } => Some(table_name),
                _ => None,
            })
            .ok_or_else(|| {
                SqeError::Execution(
                    "ALTER TABLE statement does not contain a RENAME TO operation".to_string(),
                )
            })?;

        let (src_namespace, src_name) = parse_table_ref(source_name)?;
        let src_ident = TableIdent::new(src_namespace, src_name);

        // For the destination, if only a bare name is given (1 part), inherit
        // the source namespace so that a simple `RENAME TO new_name` stays in
        // the same namespace.
        let (dest_namespace, dest_table) = parse_table_ref(dest_name)?;
        let dest_ident = if dest_name.0.len() == 1 {
            // Inherit source namespace
            TableIdent::new(src_ident.namespace().clone(), dest_table)
        } else {
            TableIdent::new(dest_namespace, dest_table)
        };

        info!(
            username = %session.user.username,
            src = %src_ident,
            dest = %dest_ident,
            "Renaming table"
        );

        let catalog = self.create_catalog_bridge(session).await?;
        catalog
            .rename_table(&src_ident, &dest_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to rename table: {e}")))?;

        Ok(())
    }

    /// Create a view via the Polaris REST API.
    ///
    /// Extracts the view name and SELECT query from a `CREATE VIEW` statement,
    /// infers the output schema by planning the SELECT via DataFusion, converts
    /// it to the Iceberg REST API schema format, and calls `SessionCatalog::create_view()`.
    pub async fn create_view(
        &self,
        session: &Session,
        stmt: &Statement,
        schema_json: &serde_json::Value,
    ) -> sqe_core::Result<()> {
        let (view_name, query) = match stmt {
            Statement::CreateView { name, query, .. } => (name, query),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CREATE VIEW statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(view_name)?;
        let select_sql = format!("{query}");

        info!(
            username = %session.user.username,
            view = %name,
            namespace = ?namespace,
            "Creating view"
        );

        let session_catalog = SessionCatalog::new(
            &self.config.catalog.polaris_url,
            &self.config.catalog.warehouse,
            &session.access_token,
            &self.config.storage,
        )
        .await?;

        session_catalog
            .create_view(&namespace, &name, &select_sql, schema_json)
            .await
    }

    /// Drop a view via the Polaris REST API.
    ///
    /// Extracts the view name from a `DROP VIEW` statement and calls
    /// `SessionCatalog::drop_view()`. If `IF EXISTS` is specified and the
    /// view is not found, this returns `Ok(())`.
    pub async fn drop_view(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let (names, if_exists) = match stmt {
            Statement::Drop {
                names,
                if_exists,
                object_type: ObjectType::View,
                ..
            } => (names, *if_exists),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DROP VIEW statement, got: {other}"
                )));
            }
        };

        let view_name = names.first().ok_or_else(|| {
            SqeError::Execution("DROP VIEW requires at least one view name".to_string())
        })?;

        let (namespace, name) = parse_table_ref(view_name)?;

        info!(
            username = %session.user.username,
            view = %name,
            namespace = ?namespace,
            if_exists = if_exists,
            "Dropping view"
        );

        let session_catalog = SessionCatalog::new(
            &self.config.catalog.polaris_url,
            &self.config.catalog.warehouse,
            &session.access_token,
            &self.config.storage,
        )
        .await?;

        match session_catalog.drop_view(&namespace, &name).await {
            Ok(()) => Ok(()),
            Err(e) if if_exists && e.to_string().contains("404") => {
                info!(
                    view = %name,
                    "View not found, IF EXISTS specified — ignoring"
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Create a `SessionCatalogBridge` (which implements `iceberg::Catalog`)
    /// for the given session.
    async fn create_catalog_bridge(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Arc<dyn Catalog>> {
        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
            )
            .await?,
        );

        Ok(session_catalog.as_catalog())
    }
}

/// Parse a sqlparser `ObjectName` into an iceberg `(NamespaceIdent, table_name)`.
///
/// - 1 part  → namespace = "default", table = name
/// - 2 parts → namespace = parts[0], table = parts[1]
/// - 3 parts → ignore catalog prefix, namespace = parts[1], table = parts[2]
pub(crate) fn parse_table_ref(name: &ObjectName) -> sqe_core::Result<(NamespaceIdent, String)> {
    let parts: Vec<String> = name.0.iter().map(|ident| ident.value.clone()).collect();

    match parts.len() {
        1 => Ok((
            NamespaceIdent::new("default".to_string()),
            parts[0].clone(),
        )),
        2 => Ok((
            NamespaceIdent::new(parts[0].clone()),
            parts[1].clone(),
        )),
        3 => Ok((
            NamespaceIdent::new(parts[1].clone()),
            parts[2].clone(),
        )),
        n => Err(SqeError::Execution(format!(
            "Invalid table reference with {n} parts: {name}"
        ))),
    }
}

/// Check if an iceberg error indicates a table was not found.
fn is_table_not_found(err: &iceberg::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("not found")
        || msg.contains("no such table")
        || msg.contains("does not exist")
        || msg.contains("404")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Ident;

    #[test]
    fn test_parse_table_ref_one_part() {
        let name = ObjectName(vec![Ident::new("my_table")]);
        let (ns, table) = parse_table_ref(&name).unwrap();
        assert_eq!(ns, NamespaceIdent::new("default".to_string()));
        assert_eq!(table, "my_table");
    }

    #[test]
    fn test_parse_table_ref_two_parts() {
        let name = ObjectName(vec![Ident::new("my_schema"), Ident::new("my_table")]);
        let (ns, table) = parse_table_ref(&name).unwrap();
        assert_eq!(ns, NamespaceIdent::new("my_schema".to_string()));
        assert_eq!(table, "my_table");
    }

    #[test]
    fn test_parse_table_ref_three_parts() {
        let name = ObjectName(vec![
            Ident::new("my_catalog"),
            Ident::new("my_schema"),
            Ident::new("my_table"),
        ]);
        let (ns, table) = parse_table_ref(&name).unwrap();
        assert_eq!(ns, NamespaceIdent::new("my_schema".to_string()));
        assert_eq!(table, "my_table");
    }

    #[test]
    fn test_parse_table_ref_empty_is_error() {
        let name = ObjectName(vec![]);
        assert!(parse_table_ref(&name).is_err());
    }

    #[test]
    fn test_parse_table_ref_four_parts_is_error() {
        let name = ObjectName(vec![
            Ident::new("a"),
            Ident::new("b"),
            Ident::new("c"),
            Ident::new("d"),
        ]);
        assert!(parse_table_ref(&name).is_err());
    }
}
