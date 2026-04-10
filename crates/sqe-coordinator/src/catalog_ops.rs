use std::collections::HashMap;
use std::sync::Arc as StdArc;
use std::sync::Arc;

use iceberg::spec::{NestedField, Schema as IcebergSchema};
use iceberg::{Catalog, NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, Expr, ObjectName, ObjectType, SchemaName, SqlOption, Statement, Value};
use tracing::info;

use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::{Session, SqeConfig, SqeError};
use tracing::instrument;

use crate::write_handler::sql_type_to_arrow;

/// Handles catalog DDL operations (DROP TABLE, ALTER TABLE RENAME, views).
///
/// These operations go directly through the Iceberg REST catalog API
/// rather than through DataFusion's query engine.
pub struct CatalogOps {
    config: SqeConfig,
    /// Shared global table metadata cache threaded from the coordinator.
    table_cache: Option<TableMetadataCache>,
}

impl CatalogOps {
    pub fn new(config: SqeConfig) -> Self {
        Self { config, table_cache: None }
    }

    /// Attach a global table metadata cache so DDL operations invalidate the right entry.
    pub fn with_table_cache(mut self, cache: TableMetadataCache) -> Self {
        self.table_cache = Some(cache);
        self
    }

    /// Drop a table via the Iceberg REST catalog.
    ///
    /// Extracts the table name from a `DROP TABLE` statement and calls
    /// the catalog's `drop_table` method. If `IF EXISTS` is specified
    /// and the table is not found, this returns `Ok(())`.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
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

    /// Create a schema (namespace) via the Iceberg REST catalog.
    ///
    /// Maps SQL `CREATE SCHEMA` to Iceberg `create_namespace`.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn create_schema(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let (schema_name, if_not_exists) = match stmt {
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => (schema_name, *if_not_exists),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CREATE SCHEMA statement, got: {other}"
                )));
            }
        };

        let namespace = parse_schema_name(schema_name)?;

        info!(
            username = %session.user.username,
            namespace = ?namespace,
            if_not_exists = if_not_exists,
            "Creating schema"
        );

        let catalog = self.create_catalog_bridge(session).await?;

        match catalog
            .create_namespace(&namespace, HashMap::new())
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if if_not_exists && is_namespace_already_exists(&e) => {
                info!(
                    namespace = ?namespace,
                    "Schema already exists, IF NOT EXISTS specified — ignoring"
                );
                Ok(())
            }
            Err(e) => Err(SqeError::Catalog(format!("Failed to create schema: {e}"))),
        }
    }

    /// Drop a schema (namespace) via the Iceberg REST catalog.
    ///
    /// Maps SQL `DROP SCHEMA` to Iceberg `drop_namespace`.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn drop_schema(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let (names, if_exists) = match stmt {
            Statement::Drop {
                names,
                if_exists,
                object_type: ObjectType::Schema,
                ..
            } => (names, *if_exists),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected DROP SCHEMA statement, got: {other}"
                )));
            }
        };

        let schema_name_obj = names.first().ok_or_else(|| {
            SqeError::Execution("DROP SCHEMA requires at least one schema name".to_string())
        })?;

        let namespace = parse_namespace_from_object_name(schema_name_obj)?;

        info!(
            username = %session.user.username,
            namespace = ?namespace,
            if_exists = if_exists,
            "Dropping schema"
        );

        let catalog = self.create_catalog_bridge(session).await?;

        match catalog.drop_namespace(&namespace).await {
            Ok(()) => Ok(()),
            Err(e) if if_exists && is_namespace_not_found(&e) => {
                info!(
                    namespace = ?namespace,
                    "Schema not found, IF EXISTS specified — ignoring"
                );
                Ok(())
            }
            Err(e) => Err(SqeError::Catalog(format!("Failed to drop schema: {e}"))),
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

        // Find the RenameTable operation and extract the destination ObjectName
        let dest_obj_name = operations
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
        let (dest_namespace, dest_table) = parse_table_ref(dest_obj_name)?;
        let dest_ident = if dest_obj_name.0.len() == 1 {
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
    ///
    /// When `or_replace` is `true` (i.e. `CREATE OR REPLACE VIEW`), the existing
    /// view is dropped first if it exists, then the new view is created. This is
    /// the simple non-atomic approach — there is a brief window between drop and
    /// create where the view does not exist.
    #[instrument(skip(self, session, stmt, schema_json), fields(username = %session.user.username))]
    pub async fn create_view(
        &self,
        session: &Session,
        stmt: &Statement,
        schema_json: &serde_json::Value,
    ) -> sqe_core::Result<()> {
        let (view_name, query, or_replace) = match stmt {
            Statement::CreateView { name, query, or_replace, .. } => (name, query, *or_replace),
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
            or_replace = or_replace,
            "Creating view"
        );

        let session_catalog = SessionCatalog::new(
            &self.config.catalog.polaris_url,
            &self.config.catalog.warehouse,
            &session.access_token,
            &self.config.storage,
            self.table_cache.clone(),
            None, None,
        )
        .await?;

        // For CREATE OR REPLACE VIEW: drop the existing view first (if it exists).
        if or_replace {
            match session_catalog.drop_view(&namespace, &name).await {
                Ok(()) => {
                    info!(view = %name, "Dropped existing view for CREATE OR REPLACE VIEW");
                }
                Err(e) if e.is_not_found() => {
                    // View didn't exist — nothing to drop
                }
                Err(e) => return Err(e),
            }
        }

        session_catalog
            .create_view(&namespace, &name, &select_sql, schema_json)
            .await
    }

    /// Drop a view via the Polaris REST API.
    ///
    /// Extracts the view name from a `DROP VIEW` statement and calls
    /// `SessionCatalog::drop_view()`. If `IF EXISTS` is specified and the
    /// view is not found, this returns `Ok(())`.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
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
            self.table_cache.clone(),
            None, None,
        )
        .await?;

        match session_catalog.drop_view(&namespace, &name).await {
            Ok(()) => Ok(()),
            Err(e) if if_exists && e.is_not_found() => {
                info!(
                    view = %name,
                    "View not found, IF EXISTS specified — ignoring"
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Evolve a table schema via the Iceberg REST catalog.
    ///
    /// Handles `ALTER TABLE ... ADD/DROP/RENAME COLUMN` and `ALTER COLUMN` operations
    /// by loading the current schema, applying each operation in order, then committing
    /// the new schema via the Iceberg Transaction API.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn alter_table_schema(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let (table_name, operations) = match stmt {
            Statement::AlterTable {
                name, operations, ..
            } => (name, operations),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected ALTER TABLE statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        info!(
            username = %session.user.username,
            table = %table_ident,
            "Evolving table schema"
        );

        // Use SessionCatalog directly so we can call commit_schema_update, which
        // bypasses the crate-private TableCommit::build() method.
        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
                self.table_cache.clone(),
                None, None,
            )
            .await?,
        );

        // Load the table via the catalog bridge (iceberg's load_table returns a Table)
        let catalog = session_catalog.as_catalog();
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table '{table_ident}': {e}")))?;

        let metadata = table.metadata();
        let current_schema = metadata.current_schema();
        let last_assigned_field_id = metadata.last_column_id();
        let current_schema_id = current_schema.schema_id();

        // Get mutable copy of current fields
        let mut fields: Vec<StdArc<NestedField>> = current_schema.as_struct().fields().to_vec();

        // Track the maximum field ID so new fields get unique IDs
        let mut max_field_id = last_assigned_field_id;

        for op in operations {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    let not_null = column_def
                        .options
                        .iter()
                        .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull));

                    let arrow_type = sql_type_to_arrow(&column_def.data_type)?;
                    let iceberg_type = iceberg::arrow::arrow_type_to_type(&arrow_type)
                        .map_err(|e| SqeError::Execution(format!(
                            "Cannot convert type for column '{}': {e}",
                            column_def.name.value
                        )))?;

                    max_field_id += 1;
                    let new_field = if not_null {
                        NestedField::required(max_field_id, &column_def.name.value, iceberg_type)
                    } else {
                        NestedField::optional(max_field_id, &column_def.name.value, iceberg_type)
                    };
                    fields.push(StdArc::new(new_field));
                }

                AlterTableOperation::DropColumn { column_name, if_exists, .. } => {
                    let col = column_name.value.as_str();
                    let pos = fields.iter().position(|f| f.name == col);
                    match pos {
                        Some(idx) => { fields.remove(idx); }
                        None if *if_exists => {}
                        None => {
                            return Err(SqeError::Execution(format!(
                                "Column '{col}' not found in table '{table_ident}'"
                            )));
                        }
                    }
                }

                AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                    let old_name = old_column_name.value.as_str();
                    let pos = fields.iter().position(|f| f.name == old_name).ok_or_else(|| {
                        SqeError::Execution(format!(
                            "Column '{old_name}' not found in table '{table_ident}'"
                        ))
                    })?;
                    let old_field = &fields[pos];
                    let renamed = NestedField::new(
                        old_field.id,
                        new_column_name.value.clone(),
                        *old_field.field_type.clone(),
                        old_field.required,
                    );
                    fields[pos] = StdArc::new(renamed);
                }

                AlterTableOperation::AlterColumn { column_name, op } => {
                    let col = column_name.value.as_str();
                    let pos = fields.iter().position(|f| f.name == col).ok_or_else(|| {
                        SqeError::Execution(format!(
                            "Column '{col}' not found in table '{table_ident}'"
                        ))
                    })?;
                    let old_field = &fields[pos];
                    let new_field = match op {
                        AlterColumnOperation::SetNotNull => NestedField::new(
                            old_field.id,
                            old_field.name.clone(),
                            *old_field.field_type.clone(),
                            true,
                        ),
                        AlterColumnOperation::DropNotNull => NestedField::new(
                            old_field.id,
                            old_field.name.clone(),
                            *old_field.field_type.clone(),
                            false,
                        ),
                        AlterColumnOperation::SetDataType { data_type, .. } => {
                            let arrow_type = sql_type_to_arrow(data_type)?;
                            let iceberg_type = iceberg::arrow::arrow_type_to_type(&arrow_type)
                                .map_err(|e| SqeError::Execution(format!(
                                    "Cannot convert type for column '{col}': {e}"
                                )))?;
                            NestedField::new(
                                old_field.id,
                                old_field.name.clone(),
                                iceberg_type,
                                old_field.required,
                            )
                        }
                        other => {
                            return Err(SqeError::NotImplemented(format!(
                                "ALTER COLUMN operation not supported: {other}"
                            )));
                        }
                    };
                    fields[pos] = StdArc::new(new_field);
                }

                other => {
                    return Err(SqeError::NotImplemented(format!(
                        "ALTER TABLE operation not supported: {other}"
                    )));
                }
            }
        }

        // Build new schema with incremented schema ID
        let new_schema_id = metadata
            .schemas_iter()
            .map(|s| s.schema_id())
            .max()
            .unwrap_or(0)
            + 1;

        let new_schema = IcebergSchema::builder()
            .with_schema_id(new_schema_id)
            .with_fields(fields)
            .with_identifier_field_ids(current_schema.identifier_field_ids())
            .build()
            .map_err(|e| SqeError::Execution(format!("Failed to build new schema: {e}")))?;

        // Commit via SessionCatalog::commit_schema_update which makes a direct REST
        // POST call. We use this rather than TableCommit::builder().build() because
        // the TypedBuilder `build()` is pub(crate) in the upstream iceberg crate.
        let updates = vec![
            TableUpdate::AddSchema { schema: new_schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        let requirements = vec![
            TableRequirement::LastAssignedFieldIdMatch { last_assigned_field_id },
            TableRequirement::CurrentSchemaIdMatch { current_schema_id },
        ];

        session_catalog
            .commit_schema_update(&table_ident, updates, requirements)
            .await?;

        info!(
            table = %table_ident,
            "Schema evolution committed successfully"
        );

        Ok(())
    }

    /// Set table properties via the Iceberg REST catalog.
    ///
    /// Handles `ALTER TABLE ... SET TBLPROPERTIES (...)` by extracting the
    /// key-value pairs and committing them as `TableUpdate::SetProperties`.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn set_table_properties(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<()> {
        let (table_name, operations) = match stmt {
            Statement::AlterTable {
                name, operations, ..
            } => (name, operations),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected ALTER TABLE statement, got: {other}"
                )));
            }
        };

        let (namespace, name) = parse_table_ref(table_name)?;
        let table_ident = TableIdent::new(namespace, name);

        // Extract SET TBLPROPERTIES key-value pairs
        let mut updates: HashMap<String, String> = HashMap::new();
        for op in operations {
            if let AlterTableOperation::SetTblProperties { table_properties } = op {
                for prop in table_properties {
                    if let SqlOption::KeyValue { key, value } = prop {
                        let k = key.value.clone();
                        let v = sql_expr_to_string(value);
                        updates.insert(k, v);
                    }
                }
            }
        }

        if updates.is_empty() {
            return Err(SqeError::Execution(
                "ALTER TABLE SET TBLPROPERTIES: no valid key-value properties found".to_string(),
            ));
        }

        info!(
            username = %session.user.username,
            table = %table_ident,
            num_props = updates.len(),
            "Setting table properties"
        );

        let session_catalog = Arc::new(
            SessionCatalog::new(
                &self.config.catalog.polaris_url,
                &self.config.catalog.warehouse,
                &session.access_token,
                &self.config.storage,
                self.table_cache.clone(),
                None, None,
            )
            .await?,
        );

        let table_updates = vec![TableUpdate::SetProperties { updates }];
        let requirements = vec![];

        session_catalog
            .commit_schema_update(&table_ident, table_updates, requirements)
            .await?;

        info!(
            table = %table_ident,
            "Table properties committed successfully"
        );

        Ok(())
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
                self.table_cache.clone(),
                None, None,
            )
            .await?,
        );

        Ok(session_catalog.as_catalog())
    }
}

/// Convert a sqlparser `Expr` (used as a property value) to a plain String.
///
/// For quoted string literals (single or double quoted) the inner string is
/// returned directly. For everything else the Display representation is used.
fn sql_expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
        Expr::Value(Value::DoubleQuotedString(s)) => s.clone(),
        other => format!("{other}"),
    }
}

/// Parse a sqlparser `ObjectName` into an iceberg `(NamespaceIdent, table_name)`.
///
/// - 1 part  → namespace = "default", table = name
/// - 2 parts → namespace = parts[0], table = parts[1]
/// - 3 parts → ignore catalog prefix, namespace = parts[1], table = parts[2]
pub(crate) fn parse_table_ref(name: &ObjectName) -> sqe_core::Result<(NamespaceIdent, String)> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|ident| ident.value.clone())
        .collect();

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

/// Parse a sqlparser `SchemaName` into an iceberg `NamespaceIdent`.
fn parse_schema_name(schema_name: &SchemaName) -> sqe_core::Result<NamespaceIdent> {
    match schema_name {
        SchemaName::Simple(name) => parse_namespace_from_object_name(name),
        SchemaName::UnnamedAuthorization(ident) => {
            Ok(NamespaceIdent::new(ident.value.clone()))
        }
        SchemaName::NamedAuthorization(name, _) => parse_namespace_from_object_name(name),
    }
}

/// Parse a sqlparser `ObjectName` into an iceberg `NamespaceIdent`.
///
/// - 1 part  → namespace = name
/// - 2 parts → ignore catalog prefix, namespace = parts[1]
fn parse_namespace_from_object_name(name: &ObjectName) -> sqe_core::Result<NamespaceIdent> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|ident| ident.value.clone())
        .collect();

    match parts.len() {
        1 => Ok(NamespaceIdent::new(parts[0].clone())),
        2 => Ok(NamespaceIdent::new(parts[1].clone())),
        n => Err(SqeError::Execution(format!(
            "Invalid schema reference with {n} parts: {name}"
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

/// Check if an iceberg error indicates a namespace was not found.
fn is_namespace_not_found(err: &iceberg::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("not found")
        || msg.contains("no such namespace")
        || msg.contains("does not exist")
        || msg.contains("404")
}

/// Check if an iceberg error indicates a namespace already exists.
fn is_namespace_already_exists(err: &iceberg::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("already exists")
        || msg.contains("409")
        || msg.contains("conflict")
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
        let name = ObjectName(vec![] as Vec<Ident>);
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
