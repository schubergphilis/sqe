use std::collections::HashMap;
use std::sync::Arc as StdArc;
use std::sync::Arc;

use iceberg::spec::{
    MAIN_BRANCH, NestedField, Schema as IcebergSchema, SnapshotReference, SnapshotRetention,
};
use iceberg::{Catalog, NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, Expr, ObjectName, ObjectType, SchemaName, SqlOption, Statement, Value};
use tracing::info;

use sqe_catalog::{SessionCatalog, TableMetadataCache};
use sqe_core::{Session, SqeConfig, SqeError};
use sqe_sql::{BranchRetention, PartitionEvolution, RefDdl};
use tracing::instrument;

use crate::write_handler::{
    default_to_iceberg_literal as alter_default_to_iceberg_literal, parse_partition_transform_sql,
    sql_type_to_arrow,
};

/// Per-table async lock used to serialize read-merge-commit sequences (see
/// `keyed_lock`). The outer `std::Mutex` guards only the map; the inner
/// `tokio::Mutex` is the per-table critical-section lock.
type TableLockMap = StdArc<std::sync::Mutex<HashMap<String, StdArc<tokio::sync::Mutex<()>>>>>;

/// Handles catalog DDL operations (DROP TABLE, ALTER TABLE RENAME, views).
///
/// These operations go directly through the Iceberg REST catalog API
/// rather than through DataFusion's query engine.
pub struct CatalogOps {
    config: SqeConfig,
    /// Shared global table metadata cache threaded from the coordinator.
    table_cache: Option<TableMetadataCache>,
    /// Per-table async locks serializing read-merge-commit sequences that
    /// cannot be protected by an Iceberg `TableRequirement` (notably the plain
    /// `SetProperties` write in `set_column_tags`). Keyed by the table ident
    /// string. `CatalogOps` is held as a single shared `Arc<QueryHandler>`
    /// across all sessions, so this serializes concurrent writers cluster-wide
    /// for THIS coordinator. The outer `std::Mutex` only guards map lookup and
    /// is never held across an await.
    table_locks: TableLockMap,
}

impl CatalogOps {
    pub fn new(config: SqeConfig) -> Self {
        Self {
            config,
            table_cache: None,
            table_locks: StdArc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Attach a global table metadata cache so DDL operations invalidate the right entry.
    #[must_use = "with_table_cache consumes self; bind the returned ops"]
    pub fn with_table_cache(mut self, cache: TableMetadataCache) -> Self {
        self.table_cache = Some(cache);
        self
    }

    /// Evict every token's metadata cache entry for `table_ident`.
    ///
    /// See `TableMetadataCache::invalidate_table_all_tokens`. No-op when no
    /// cache is wired (e.g. in unit tests constructed via `CatalogOps::new`).
    async fn invalidate_table_all_tokens(&self, table_ident: &TableIdent) {
        if let Some(cache) = &self.table_cache {
            let suffix = format!("{}.{}", table_ident.namespace(), table_ident.name());
            cache.invalidate_table_all_tokens(&suffix).await;
        }
    }

    /// Return the per-table async lock for `table_ident`, creating it on first
    /// use. The outer `std::Mutex` guards only the map insert/lookup and is
    /// dropped before the caller awaits the returned per-table lock, so two
    /// callers contending on DIFFERENT tables never block each other and the
    /// std mutex is never held across an await point.
    fn table_lock_for(&self, table_ident: &TableIdent) -> StdArc<tokio::sync::Mutex<()>> {
        let key = format!("{}.{}", table_ident.namespace(), table_ident.name());
        keyed_lock(&self.table_locks, key)
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

        let table_ident = resolve_table_ident(table_name, session)?;

        info!(
            username = %session.user.username,
            table = %table_ident,
            if_exists = if_exists,
            "Dropping table"
        );

        let catalog = self
            .create_catalog_bridge(session, catalog_qualifier(table_name).as_deref())
            .await?;

        match catalog.drop_table(&table_ident).await {
            Ok(()) => Ok(()),
            // IF EXISTS only swallows "table missing inside an existing
            // namespace". A missing namespace is a different failure that
            // typically points at an upstream CREATE SCHEMA that didn't
            // land — surfacing it here saves the operator from chasing a
            // mysterious downstream CTAS error.
            Err(e) if if_exists && is_namespace_not_found(&e) => {
                Err(SqeError::Catalog(format!(
                    "Failed to drop table {table_ident}: namespace is missing. \
                     IF EXISTS does not cover missing namespaces. \
                     Verify CREATE SCHEMA succeeded. Underlying error: {e}"
                )))
            }
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

        // CREATE SCHEMA `catalog.schema` carries the target catalog as the first
        // part; peel it off so the namespace is just `schema` and route the
        // create to that catalog (mirrors the write path for tables). Without
        // this, dbt's `CREATE SCHEMA ws_team_a.dev_raw` created a `ws_team_a.dev_raw`
        // two-level namespace in the DEFAULT warehouse, so the later CREATE TABLE
        // into the discovered `ws_team_a` catalog failed "namespace does not exist".
        let (explicit_catalog, namespace) = match schema_name {
            SchemaName::Simple(obj) | SchemaName::NamedAuthorization(obj, _) => {
                (schema_catalog_qualifier(obj), namespace_without_catalog(obj)?)
            }
            SchemaName::UnnamedAuthorization(ident) => {
                (None, NamespaceIdent::new(ident.value.clone()))
            }
        };
        // An explicit `catalog.schema` wins; otherwise fall back to the session's
        // connection catalog (Trino `catalog=` header / Flight default). A
        // dbt/Trino client sets the catalog once on the connection, so a
        // session-relative CREATE SCHEMA must land there, not the default warehouse.
        let target_catalog = explicit_catalog.or_else(|| session.default_catalog.clone());

        info!(
            username = %session.user.username,
            namespace = ?namespace,
            target_catalog = ?target_catalog,
            if_not_exists = if_not_exists,
            "Creating schema"
        );

        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;

        match catalog
            .create_namespace(&namespace, HashMap::new())
            .await
        {
            Ok(_) => {
                info!(
                    namespace = ?namespace,
                    "Schema created"
                );
                Ok(())
            }
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

        let target_catalog =
            schema_catalog_qualifier(schema_name_obj).or_else(|| session.default_catalog.clone());
        let namespace = namespace_without_catalog(schema_name_obj)?;

        info!(
            username = %session.user.username,
            namespace = ?namespace,
            target_catalog = ?target_catalog,
            if_exists = if_exists,
            "Dropping schema"
        );

        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;

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
            Statement::AlterTable(at) => (&at.name, &at.operations),
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
                // sqlparser 0.62 wraps the rename target in RenameTableNameKind
                // (`AS name` / `TO name`); both carry the destination ObjectName.
                AlterTableOperation::RenameTable { table_name } => match table_name {
                    sqlparser::ast::RenameTableNameKind::As(name)
                    | sqlparser::ast::RenameTableNameKind::To(name) => Some(name),
                },
                _ => None,
            })
            .ok_or_else(|| {
                SqeError::Execution(
                    "ALTER TABLE statement does not contain a RENAME TO operation".to_string(),
                )
            })?;

        let src_ident = resolve_table_ident(source_name, session)?;

        // For the destination, if only a bare name is given (1 part), inherit
        // the source namespace so that a simple `RENAME TO new_name` stays in
        // the same namespace.
        let parsed_dest = parse_table_ref(dest_obj_name)?;
        let dest_ident = if dest_obj_name.0.len() == 1 {
            // Inherit source namespace
            TableIdent::new(src_ident.namespace().clone(), parsed_dest.name().to_string())
        } else {
            parsed_dest
        };

        // Resolve against the SOURCE catalog. Iceberg REST cannot rename across
        // catalogs, so reject a destination that explicitly names a different
        // one rather than silently renaming within the source.
        let target_catalog = catalog_qualifier(source_name);
        let src_catalog = target_catalog
            .as_deref()
            .unwrap_or(self.config.catalog.warehouse.as_str());
        if let Some(dest_catalog) = catalog_qualifier(dest_obj_name) {
            if dest_catalog != src_catalog {
                return Err(SqeError::Execution(format!(
                    "RENAME across catalogs is not supported: source catalog \
                     '{src_catalog}', destination catalog '{dest_catalog}'"
                )));
            }
        }

        info!(
            username = %session.user.username,
            src = %src_ident,
            dest = %dest_ident,
            target_catalog = ?target_catalog,
            "Renaming table"
        );

        let catalog = self
            .create_catalog_bridge(session, target_catalog.as_deref())
            .await?;
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
            Statement::CreateView(cv) => (&cv.name, &cv.query, cv.or_replace),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected CREATE VIEW statement, got: {other}"
                )));
            }
        };

        let view_ident = resolve_table_ident(view_name, session)?;
        let namespace = view_ident.namespace();
        let name = view_ident.name();
        let select_sql = format!("{query}");

        info!(
            username = %session.user.username,
            view = %name,
            namespace = ?namespace,
            or_replace = or_replace,
            "Creating view"
        );

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(view_name).as_deref())
            .await?;

        // For CREATE OR REPLACE VIEW: drop the existing view first (if it exists).
        if or_replace {
            match session_catalog.drop_view(namespace, name).await {
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
            .create_view(namespace, name, &select_sql, schema_json)
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

        let view_ident = resolve_table_ident(view_name, session)?;
        let namespace = view_ident.namespace();
        let name = view_ident.name();

        info!(
            username = %session.user.username,
            view = %name,
            namespace = ?namespace,
            if_exists = if_exists,
            "Dropping view"
        );

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(view_name).as_deref())
            .await?;

        match session_catalog.drop_view(namespace, name).await {
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
            Statement::AlterTable(at) => (&at.name, &at.operations),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected ALTER TABLE statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;

        info!(
            username = %session.user.username,
            table = %table_ident,
            "Evolving table schema"
        );

        // Use SessionCatalog directly so we can call commit_schema_update, which
        // bypasses the crate-private TableCommit::build() method.
        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(table_name).as_deref())
            .await?;

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
                    // Fold the new column name (unquoted -> lowercase) so it is
                    // consistent with CREATE TABLE and query-side folding (#337).
                    let col_name = fold_unquoted_ident(&column_def.name);
                    let mut new_field = if not_null {
                        NestedField::required(
                            max_field_id,
                            &col_name,
                            iceberg_type.clone(),
                        )
                    } else {
                        NestedField::optional(
                            max_field_id,
                            &col_name,
                            iceberg_type.clone(),
                        )
                    };

                    // Extract a DEFAULT literal, if any, and set both defaults.
                    // `initial_default` fills existing rows retroactively;
                    // `write_default` applies to new inserts.
                    let default_expr =
                        column_def.options.iter().find_map(|o| match &o.option {
                            sqlparser::ast::ColumnOption::Default(e) => Some(e),
                            _ => None,
                        });
                    if let Some(expr) = default_expr {
                        let sql_literal = sqe_sql::extract_default_literal(expr).map_err(|e| {
                            SqeError::Execution(format!(
                                "Invalid DEFAULT for column '{}': {e}",
                                column_def.name.value
                            ))
                        })?;
                        match alter_default_to_iceberg_literal(&sql_literal, &iceberg_type) {
                            Ok(Some(lit)) => {
                                new_field = new_field
                                    .with_initial_default(lit.clone())
                                    .with_write_default(lit);
                            }
                            Ok(None) => {}
                            Err(msg) => {
                                return Err(SqeError::Execution(format!(
                                    "DEFAULT literal for column '{}': {msg}",
                                    column_def.name.value
                                )));
                            }
                        }
                    }

                    fields.push(StdArc::new(new_field));
                }

                AlterTableOperation::DropColumn { column_names, if_exists, .. } => {
                    // sqlparser 0.62 carries a Vec of column names (the old
                    // single `column_name` field). Drop each in turn, keeping
                    // the same not-found / IF EXISTS semantics per column.
                    for column_name in column_names {
                        let col_folded = fold_unquoted_ident(column_name);
                        let col = col_folded.as_str();
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
                }

                AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                    // Fold both: the source lookup and the destination name (#337).
                    let old_name_folded = fold_unquoted_ident(old_column_name);
                    let old_name = old_name_folded.as_str();
                    let pos = fields.iter().position(|f| f.name == old_name).ok_or_else(|| {
                        SqeError::Execution(format!(
                            "Column '{old_name}' not found in table '{table_ident}'"
                        ))
                    })?;
                    let old_field = &fields[pos];
                    let renamed = NestedField::new(
                        old_field.id,
                        fold_unquoted_ident(new_column_name),
                        *old_field.field_type.clone(),
                        old_field.required,
                    );
                    fields[pos] = StdArc::new(renamed);
                }

                AlterTableOperation::AlterColumn { column_name, op } => {
                    let col_folded = fold_unquoted_ident(column_name);
                    let col = col_folded.as_str();
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

    /// Drop a nested struct subfield: `ALTER TABLE t DROP COLUMN a.b[.c]` (#336).
    ///
    /// `path[0]` is the top-level column; `path[1..]` walks into nested structs.
    /// The struct field is rebuilt without the leaf subfield, preserving every
    /// other field's Iceberg id (so the commit is a pure delete). A missing leaf
    /// errors unless `if_exists` is set, in which case it is a no-op.
    #[instrument(skip(self, session, path), fields(username = %session.user.username))]
    pub async fn drop_nested_column(
        &self,
        session: &Session,
        table: &str,
        path: &[String],
        if_exists: bool,
    ) -> sqe_core::Result<()> {
        use iceberg::spec::{NestedField, Type};

        let table_name = parse_object_name(table)?;
        let table_ident = resolve_table_ident(&table_name, session)?;

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(&table_name).as_deref())
            .await?;
        let catalog = session_catalog.as_catalog();
        let iceberg_table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load table '{table_ident}': {e}")))?;

        let metadata = iceberg_table.metadata();
        let current_schema = metadata.current_schema();
        let last_assigned_field_id = metadata.last_column_id();
        let current_schema_id = current_schema.schema_id();
        let mut fields: Vec<StdArc<NestedField>> = current_schema.as_struct().fields().to_vec();

        let dotted = path.join(".");
        let (top, rest) = path.split_first().expect("nested drop path has >= 2 parts");
        let Some(idx) = fields.iter().position(|f| &f.name == top) else {
            if if_exists {
                return Ok(());
            }
            return Err(SqeError::Execution(format!(
                "Column '{top}' not found in table '{table_ident}'"
            )));
        };

        let field = &fields[idx];
        let Type::Struct(inner) = &*field.field_type else {
            return Err(SqeError::Execution(format!(
                "Cannot drop '{dotted}': column '{top}' is not a struct"
            )));
        };
        match remove_struct_subfield(inner, rest, if_exists)? {
            // Subfield removed: rebuild the struct column, keeping id/required.
            Some(new_inner) => {
                let f = &fields[idx];
                fields[idx] = StdArc::new(NestedField::new(
                    f.id,
                    f.name.clone(),
                    Type::Struct(new_inner),
                    f.required,
                ));
            }
            // Leaf absent + IF EXISTS: nothing to commit.
            None => return Ok(()),
        }

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

        info!(table = %table_ident, column = %dotted, "Nested column dropped");
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
            Statement::AlterTable(at) => (&at.name, &at.operations),
            other => {
                return Err(SqeError::Execution(format!(
                    "Expected ALTER TABLE statement, got: {other}"
                )));
            }
        };

        let table_ident = resolve_table_ident(table_name, session)?;

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

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(table_name).as_deref())
            .await?;

        let table_updates = vec![TableUpdate::SetProperties { updates }];
        let requirements = vec![];

        session_catalog
            .commit_schema_update(&table_ident, table_updates, requirements)
            .await?;

        // Explicitly evict the table metadata cache entry so the updated
        // properties (e.g. sqe.column-tags) take effect on the next query.
        // commit_schema_update already evicts the entry internally; this call
        // is redundant but makes the intent visible at the DDL site and mirrors
        // how SessionCatalog's own DDL handlers signal freshness.
        //
        // Note: policy-store cache invalidation (Arc<dyn PolicyStore>::invalidate_all)
        // is handled one level up in QueryHandler::handle_statement, which has
        // the policy store in scope. CatalogOps does not hold a PolicyStore.
        //
        // Properties (incl. sqe.column-tags) are read user-independently via
        // `properties_for`, which reads the first entry matching `|{ns}.{table}`
        // from ANY token. Evict every token's entry so the updated properties
        // are visible to all users immediately, not just the writer.
        self.invalidate_table_all_tokens(&table_ident).await;

        info!(
            table = %table_ident,
            "Table properties committed successfully"
        );

        Ok(())
    }

    /// Execute a parsed `RefDdl` (CREATE/DROP BRANCH/TAG) against the catalog.
    ///
    /// Builds the appropriate `TableUpdate::SetSnapshotRef` or `RemoveSnapshotRef`
    /// and commits it via the Iceberg REST API. Rejects DROP BRANCH on main
    /// and CREATE TAG on a duplicate name (unless `CREATE OR REPLACE`).
    #[instrument(skip(self, session, ddl), fields(username = %session.user.username))]
    pub async fn apply_ref_ddl(
        &self,
        session: &Session,
        ddl: &RefDdl,
    ) -> sqe_core::Result<()> {
        let (table_ref, ref_name, is_drop) = match ddl {
            RefDdl::CreateBranch { table, name, .. }
            | RefDdl::CreateTag { table, name, .. } => (table.as_str(), name.as_str(), false),
            RefDdl::DropBranch { table, name, .. }
            | RefDdl::DropTag { table, name, .. } => (table.as_str(), name.as_str(), true),
        };

        // Reject drop of the reserved main branch early, before any round-trip.
        if is_drop && ref_name == MAIN_BRANCH {
            return Err(SqeError::Execution(
                "cannot drop the main branch".to_string(),
            ));
        }

        let object_name = parse_object_name(table_ref)?;
        let table_ident = resolve_table_ident(&object_name, session)?;

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(&object_name).as_deref())
            .await?;

        let table = session_catalog.load_table(&table_ident).await?;
        let metadata = table.metadata();
        let existing = metadata.reference_by_name(ref_name);

        let updates = match ddl {
            RefDdl::CreateBranch {
                snapshot_id,
                retention,
                ..
            } => {
                let snap_id = resolve_snapshot_id(&table, *snapshot_id)?;
                let retention_spec = branch_retention(retention);
                vec![TableUpdate::SetSnapshotRef {
                    ref_name: ref_name.to_string(),
                    reference: SnapshotReference {
                        snapshot_id: snap_id,
                        retention: retention_spec,
                    },
                }]
            }
            RefDdl::CreateTag {
                snapshot_id,
                create_or_replace,
                max_ref_age_ms,
                ..
            } => {
                if existing.is_some() && !create_or_replace {
                    return Err(SqeError::Execution(format!(
                        "tag '{ref_name}' already exists (use CREATE OR REPLACE TAG)"
                    )));
                }
                let snap_id = resolve_snapshot_id(&table, *snapshot_id)?;
                vec![TableUpdate::SetSnapshotRef {
                    ref_name: ref_name.to_string(),
                    reference: SnapshotReference {
                        snapshot_id: snap_id,
                        retention: SnapshotRetention::Tag {
                            max_ref_age_ms: *max_ref_age_ms,
                        },
                    },
                }]
            }
            RefDdl::DropBranch {
                if_exists, ..
            }
            | RefDdl::DropTag {
                if_exists, ..
            } => {
                if existing.is_none() {
                    if *if_exists {
                        info!(
                            table = %table_ident,
                            ref_name,
                            "Ref not found; IF EXISTS specified, ignoring"
                        );
                        return Ok(());
                    }
                    return Err(SqeError::Execution(format!(
                        "reference '{ref_name}' does not exist"
                    )));
                }
                // If a DropBranch targets a tag (or vice versa), report a helpful error
                // so users don't silently clobber a reference of the wrong kind.
                if let Some(existing) = existing {
                    match (ddl, &existing.retention) {
                        (RefDdl::DropBranch { .. }, SnapshotRetention::Tag { .. }) => {
                            return Err(SqeError::Execution(format!(
                                "'{ref_name}' is a tag, not a branch; use DROP TAG"
                            )));
                        }
                        (RefDdl::DropTag { .. }, SnapshotRetention::Branch { .. }) => {
                            return Err(SqeError::Execution(format!(
                                "'{ref_name}' is a branch, not a tag; use DROP BRANCH"
                            )));
                        }
                        _ => {}
                    }
                }
                vec![TableUpdate::RemoveSnapshotRef {
                    ref_name: ref_name.to_string(),
                }]
            }
        };

        info!(
            username = %session.user.username,
            table = %table_ident,
            ref_name,
            action = ddl_action_label(ddl),
            "Applying branch/tag DDL"
        );

        session_catalog
            .commit_schema_update(&table_ident, updates, vec![])
            .await?;

        Ok(())
    }

    /// Author column tags (`ALTER TABLE ... SET TAGS / UNSET TAGS` and the
    /// Snowflake column forms). Reads the current `sqe.column-tags` property,
    /// applies merge semantics, and commits the new map as a single
    /// `TableUpdate::SetProperties`.
    #[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
    pub async fn set_column_tags(
        &self,
        session: &Session,
        stmt: &sqe_sql::tags::SetTagsStatement,
    ) -> sqe_core::Result<()> {
        let object_name = parse_object_name(&stmt.table)?;
        let table_ident = resolve_table_ident(&object_name, session)?;

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(&object_name).as_deref())
            .await?;

        // Serialize the read-merge-commit per table. `set_column_tags` does a
        // read-current -> apply_tag_ops -> SetProperties commit. Two concurrent
        // calls read the same base map and the second commit overwrites the
        // first (last-writer-wins), silently dropping a tag change. An Iceberg
        // `TableRequirement` CANNOT protect this: there is no property-CAS
        // variant, and a plain `SetProperties` bumps no checkable assertion, so
        // Polaris returns no 409. The only correct fix is to serialize on the
        // coordinator. Hold the per-table lock across the whole read-modify-
        // write-invalidate sequence; the prior writer's `invalidate_table_all_
        // tokens` (inside the lock) forces the next `load_table` to refetch the
        // post-commit properties, so no update is lost.
        let table_lock = self.table_lock_for(&table_ident);
        let _guard = table_lock.lock().await;

        let table = session_catalog.load_table(&table_ident).await?;
        let current = crate::tag_source_impl::parse_column_tags(table.metadata().properties());

        let new_map = crate::tag_source_impl::apply_tag_ops(&current, &stmt.ops);
        let json = serde_json::to_string(&new_map).map_err(|e| {
            SqeError::Execution(format!("failed to serialize column tags: {e}"))
        })?;

        let mut updates = HashMap::new();
        updates.insert(crate::tag_source_impl::PROP_KEY.to_string(), json);

        info!(
            username = %session.user.username,
            table = %table_ident,
            num_cols = new_map.len(),
            "Authoring column tags"
        );

        session_catalog
            .commit_schema_update(&table_ident, vec![TableUpdate::SetProperties { updates }], vec![])
            .await?;
        // Tags are read user-independently via `properties_for`, which returns
        // the first entry matching `|{ns}.{table}` from ANY token. The single
        // -token `invalidate_table` (done inside commit_schema_update) only
        // evicts the writer's own key, so other users would keep reading the
        // stale tag map until TTL. Evict every token's entry so the new tags
        // are visible to all users immediately. Still under `_guard`, so the
        // next serialized writer's `load_table` sees the fresh map.
        self.invalidate_table_all_tokens(&table_ident).await;
        Ok(())
    }

    /// Read back the `sqe.column-tags` property of a table as a
    /// `column -> [tags]` map. Powers `SHOW TAGS ON <table>` and the tag-merge
    /// step of `SHOW EFFECTIVE POLICY`. Loads the table via the caller's token,
    /// so the catalog's own read-access enforcement gates the call (no extra
    /// SQE gate needed). Returns an empty map for a table with no tags.
    ///
    /// Also returns the resolved `TableIdent` so callers can derive the policy
    /// key without re-parsing the reference.
    #[instrument(skip(self, session), fields(username = %session.user.username))]
    pub async fn load_column_tags(
        &self,
        session: &Session,
        table: &str,
    ) -> sqe_core::Result<(TableIdent, HashMap<String, Vec<String>>)> {
        let object_name = parse_object_name(table)?;
        let table_ident = resolve_table_ident(&object_name, session)?;

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(&object_name).as_deref())
            .await?;

        let table = session_catalog.load_table(&table_ident).await?;
        let tags = crate::tag_source_impl::parse_column_tags(table.metadata().properties());
        Ok((table_ident, tags))
    }

    /// Execute a parsed `PartitionEvolution` (ALTER TABLE ... ADD/DROP/REPLACE
    /// PARTITION FIELD) against the catalog.
    ///
    /// Builds a fresh `UnboundPartitionSpec` derived from the table's current
    /// default spec, applies the requested change, and commits via
    /// `TableUpdate::AddSpec` + `TableUpdate::SetDefaultSpec { spec_id: -1 }`.
    /// `-1` instructs the catalog to set the just-added spec as the default,
    /// matching the convention used by upstream iceberg-rust.
    ///
    /// Field IDs for newly added partition fields start at
    /// `metadata.last_partition_id() + 1`, preserving global field-ID
    /// uniqueness as required by the Iceberg V2 spec.
    #[instrument(skip(self, session, evolution), fields(username = %session.user.username))]
    pub async fn apply_partition_evolution(
        &self,
        session: &Session,
        evolution: &PartitionEvolution,
    ) -> sqe_core::Result<()> {
        use iceberg::spec::{UnboundPartitionField, UnboundPartitionSpec};

        let table_ref = match evolution {
            PartitionEvolution::AddField { table, .. }
            | PartitionEvolution::DropField { table, .. }
            | PartitionEvolution::ReplaceField { table, .. } => table.as_str(),
        };

        let object_name = parse_object_name(table_ref)?;
        let table_ident = resolve_table_ident(&object_name, session)?;

        let session_catalog = self
            .session_catalog_for(session, catalog_qualifier(&object_name).as_deref())
            .await?;

        let table = session_catalog.load_table(&table_ident).await?;
        let metadata = table.metadata();
        let current_spec = metadata.default_partition_spec();
        let current_spec_id = metadata.default_partition_spec_id();
        let mut next_field_id = metadata.last_partition_id() + 1;
        // First-ever partition field on a previously unpartitioned table:
        // start at the standard 1000 to match `build_partition_spec`.
        if next_field_id <= 0 {
            next_field_id = 1000;
        }
        let schema = metadata.current_schema();

        // Carry over the existing fields as the starting point for every
        // operation, then mutate the list in place. Each existing field is
        // converted from `PartitionField` (bound) to `UnboundPartitionField`
        // (the form `UnboundPartitionSpec` accepts).
        let mut fields: Vec<UnboundPartitionField> = current_spec
            .fields()
            .iter()
            .cloned()
            .map(|f| f.into_unbound())
            .collect();

        // Resolve a transform-SQL fragment to `(source_id, target_name, transform)`,
        // verifying the source column exists in the current schema.
        let resolve_transform =
            |transform_sql: &str| -> sqe_core::Result<UnboundPartitionField> {
                let (source_name, target_name, transform) =
                    parse_partition_transform_sql(transform_sql)?;
                let source_id = schema
                    .as_struct()
                    .fields()
                    .iter()
                    .find(|f| f.name == source_name)
                    .map(|f| f.id)
                    .ok_or_else(|| {
                        SqeError::Execution(format!(
                            "ALTER TABLE PARTITION FIELD: column '{source_name}' not found in table schema"
                        ))
                    })?;
                Ok(UnboundPartitionField {
                    source_id,
                    field_id: None, // assigned below
                    name: target_name,
                    transform,
                })
            };

        // Locate an existing field by transform-SQL fragment, matching on the
        // canonical Iceberg field name produced by `parse_partition_transform_sql`.
        let find_field_pos = |fields: &[UnboundPartitionField],
                              transform_sql: &str|
         -> sqe_core::Result<usize> {
            let (_src, target_name, _transform) = parse_partition_transform_sql(transform_sql)?;
            fields
                .iter()
                .position(|f| f.name == target_name)
                .ok_or_else(|| {
                    SqeError::Execution(format!(
                        "ALTER TABLE PARTITION FIELD: no existing partition field matches '{transform_sql}'"
                    ))
                })
        };

        let action_label = match evolution {
            PartitionEvolution::AddField { transform_sql, .. } => {
                let mut new_field = resolve_transform(transform_sql)?;
                if fields.iter().any(|f| f.name == new_field.name) {
                    return Err(SqeError::Execution(format!(
                        "ALTER TABLE ADD PARTITION FIELD: a partition field named '{}' already exists",
                        new_field.name
                    )));
                }
                new_field.field_id = Some(next_field_id);
                fields.push(new_field);
                "add_partition_field"
            }
            PartitionEvolution::DropField { transform_sql, .. } => {
                let pos = find_field_pos(&fields, transform_sql)?;
                fields.remove(pos);
                "drop_partition_field"
            }
            PartitionEvolution::ReplaceField {
                old_transform_sql,
                new_transform_sql,
                ..
            } => {
                let pos = find_field_pos(&fields, old_transform_sql)?;
                let mut new_field = resolve_transform(new_transform_sql)?;
                // Reject a no-op REPLACE so the user gets a clear error rather
                // than a silently identical spec.
                if fields[pos].name == new_field.name {
                    return Err(SqeError::Execution(format!(
                        "ALTER TABLE REPLACE PARTITION FIELD: old and new fields are identical ('{}')",
                        new_field.name
                    )));
                }
                fields.remove(pos);
                if fields.iter().any(|f| f.name == new_field.name) {
                    return Err(SqeError::Execution(format!(
                        "ALTER TABLE REPLACE PARTITION FIELD: target field '{}' already exists",
                        new_field.name
                    )));
                }
                new_field.field_id = Some(next_field_id);
                fields.push(new_field);
                "replace_partition_field"
            }
        };

        let new_spec_id = current_spec_id + 1;
        let new_spec: UnboundPartitionSpec = UnboundPartitionSpec::builder()
            .with_spec_id(new_spec_id)
            .add_partition_fields(fields)
            .map_err(|e| {
                SqeError::Execution(format!("Invalid partition spec after evolution: {e}"))
            })?
            .build();

        info!(
            username = %session.user.username,
            table = %table_ident,
            action = action_label,
            new_spec_id,
            "Applying partition evolution"
        );

        // SetDefaultSpec { spec_id: -1 } instructs the catalog to use the
        // just-added spec, matching the upstream iceberg-rust convention.
        let updates = vec![
            TableUpdate::AddSpec { spec: new_spec },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ];

        session_catalog
            .commit_schema_update(&table_ident, updates, vec![])
            .await?;

        Ok(())
    }

    /// Create a `SessionCatalogBridge` (which implements `iceberg::Catalog`)
    /// for the given session's DDL target.
    ///
    /// `target_warehouse` is the catalog qualifier of the statement's target
    /// (e.g. `ws_team_a` from `DROP TABLE ws_team_a.ns.t` or
    /// `CREATE SCHEMA ws_team_a.s`), or `None` for an unqualified target. When it
    /// names a non-default catalog and `catalog_discovery = polaris-auto` is on,
    /// the bridge is built against THAT catalog -- discovered via Polaris with the
    /// caller's bearer -- instead of the configured default warehouse. Mirrors
    /// `WriteHandler::create_catalog_bridge` (MR !285); without it, DDL on a
    /// workspace catalog silently resolved against the default warehouse.
    async fn create_catalog_bridge(
        &self,
        session: &Session,
        target_warehouse: Option<&str>,
    ) -> sqe_core::Result<Arc<dyn Catalog>> {
        let session_catalog = self.session_catalog_for(session, target_warehouse).await?;

        // Warm up the REST catalog by listing namespaces (RisingWave fork's
        // RestCatalog needs an initial API call before load/commit work).
        let _ = session_catalog.list_namespaces().await;

        Ok(session_catalog.as_catalog())
    }

    /// Resolve the per-session `SessionCatalog` for a DDL/view target's catalog
    /// qualifier, returning the `SessionCatalog` itself (not just the iceberg
    /// `Catalog` bridge) so callers can use REST-only methods (`create_view`,
    /// `commit_schema_update`, ref/partition evolution). Discovers a non-default
    /// catalog via Polaris under `polaris-auto`; otherwise the default warehouse.
    /// `create_catalog_bridge` wraps this for the plain `iceberg::Catalog` path.
    async fn session_catalog_for(
        &self,
        session: &Session,
        target_warehouse: Option<&str>,
    ) -> sqe_core::Result<Arc<SessionCatalog>> {
        match target_warehouse {
            Some(warehouse)
                if warehouse != self.config.catalog.warehouse
                    && self.config.query.catalog_discovery
                        == sqe_core::config::CatalogDiscovery::PolarisAuto =>
            {
                crate::session_context::discover_session_catalog(
                    warehouse,
                    &self.config,
                    session,
                    self.table_cache.as_ref(),
                )
                .await
                .ok_or_else(|| {
                    SqeError::Catalog(format!(
                        "Unknown catalog '{warehouse}': not resolvable via Polaris \
                         (nonexistent or not authorized for this user)"
                    ))
                })
            }
            _ => Ok(Arc::new(
                SessionCatalog::for_session(
                    &self.config,
                    self.table_cache.clone(),
                    session.access_token().expose(),
                )
                .await?,
            )),
        }
    }
}

/// Convert a sqlparser `Expr` (used as a property value) to a plain String.
///
/// For quoted string literals (single or double quoted) the inner string is
/// returned directly. For everything else the Display representation is used.
fn sql_expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            _ => format!("{expr}"),
        },
        other => format!("{other}"),
    }
}

/// Fold a column identifier the way Trino does: unquoted names fold to
/// lowercase, double-quoted names are preserved. This matches DataFusion's
/// query-side identifier normalization (`to_lowercase`), so a column stored
/// from unquoted DDL resolves against an unquoted (folded) query, and a
/// double-quoted column keeps its case and must be quoted to reference (#337).
pub(crate) fn fold_unquoted_ident(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Rebuild `struct_type` with the subfield at `path` removed, preserving the
/// Iceberg id/type/required of every retained (and every ancestor) field so the
/// schema change is a pure delete. `path[0]` is a direct field of this struct;
/// deeper paths recurse. Returns `Ok(None)` when the leaf is absent and
/// `if_exists` is set (a no-op the caller skips), otherwise errors on a missing
/// field or a non-struct ancestor. (#336)
fn remove_struct_subfield(
    struct_type: &iceberg::spec::StructType,
    path: &[String],
    if_exists: bool,
) -> sqe_core::Result<Option<iceberg::spec::StructType>> {
    use iceberg::spec::{NestedField, StructType, Type};

    let (head, rest) = path.split_first().expect("path must be non-empty");
    let mut new_fields: Vec<StdArc<NestedField>> = Vec::with_capacity(struct_type.fields().len());
    let mut found = false;

    for f in struct_type.fields() {
        if &f.name != head {
            new_fields.push(f.clone());
            continue;
        }
        found = true;
        if rest.is_empty() {
            // Leaf: drop it by not pushing it.
            continue;
        }
        // Recurse into the nested struct.
        let Type::Struct(inner) = &*f.field_type else {
            return Err(SqeError::Execution(format!(
                "Cannot drop nested column: '{head}' is not a struct"
            )));
        };
        match remove_struct_subfield(inner, rest, if_exists)? {
            Some(new_inner) => new_fields.push(StdArc::new(NestedField::new(
                f.id,
                f.name.clone(),
                Type::Struct(new_inner),
                f.required,
            ))),
            // Deeper leaf absent + IF EXISTS: whole op is a no-op.
            None => return Ok(None),
        }
    }

    if !found {
        if if_exists {
            return Ok(None);
        }
        return Err(SqeError::Execution(format!(
            "Nested field '{head}' not found"
        )));
    }
    Ok(Some(StructType::new(new_fields)))
}

/// Parse a sqlparser `ObjectName` into an iceberg `TableIdent`.
///
/// Returning a `TableIdent` end-to-end removes the chance of an
/// argument-order swap at the `TableIdent::new(namespace, name)` call sites.
///
/// - 1 part  -> namespace = "default", table = name
/// - 2 parts -> namespace = parts[0], table = parts[1]
/// - 3 parts -> ignore catalog prefix, namespace = parts[1], table = parts[2]
pub(crate) fn parse_table_ref(name: &ObjectName) -> sqe_core::Result<TableIdent> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|ident| ident.value.clone())
        .collect();

    match parts.len() {
        1 => Ok(TableIdent::new(
            NamespaceIdent::new("default".to_string()),
            parts[0].clone(),
        )),
        2 => Ok(TableIdent::new(
            NamespaceIdent::new(parts[0].clone()),
            parts[1].clone(),
        )),
        3 => Ok(TableIdent::new(
            NamespaceIdent::new(parts[1].clone()),
            parts[2].clone(),
        )),
        n => Err(SqeError::Execution(format!(
            "Invalid table reference with {n} parts: {name}"
        ))),
    }
}

/// Resolve a table reference to a `TableIdent`, honoring the session schema
/// (`X-Trino-Schema`) for an unqualified 1-part name.
///
/// `parse_table_ref` defaults an unqualified name to the `default` namespace,
/// but the read path (and Trino) resolve `t` against the session's default
/// schema. The write/DDL handlers must match that so a client on
/// `X-Trino-Schema: tpch_demo` targets `tpch_demo.t`, not `default.t` (#357).
/// A 2- or 3-part name already names its namespace and is returned unchanged;
/// when the session schema is unset or empty we fall back to `default`.
pub(crate) fn resolve_table_ident(
    name: &ObjectName,
    session: &Session,
) -> sqe_core::Result<TableIdent> {
    let ident = parse_table_ref(name)?;
    let is_unqualified = name.0.iter().filter_map(|p| p.as_ident()).count() == 1;
    if is_unqualified {
        if let Some(schema) = session.default_schema.as_deref().filter(|s| !s.is_empty()) {
            return Ok(TableIdent::new(
                NamespaceIdent::new(schema.to_string()),
                ident.name().to_string(),
            ));
        }
    }
    Ok(ident)
}

/// Return the per-key async lock, creating it on first use.
///
/// The `std::Mutex` guards only the map insert/lookup and is dropped before the
/// caller awaits the returned lock, so it is never held across an await point
/// and two callers contending on DIFFERENT keys never block each other. Keys
/// are never removed (tag authoring is rare; the map stays small), so two calls
/// with the same key always return the SAME `Arc<tokio::Mutex>` and therefore
/// serialize.
fn keyed_lock(locks: &TableLockMap, key: String) -> StdArc<tokio::sync::Mutex<()>> {
    let mut map = locks.lock().expect("table_locks mutex poisoned");
    StdArc::clone(map.entry(key).or_default())
}

/// Extract the catalog qualifier from a 3-part table reference
/// (`catalog.namespace.table`). Returns `None` for 1- or 2-part names, which
/// resolve in the session's default warehouse as before.
///
/// `parse_table_ref` deliberately drops the catalog prefix (the iceberg
/// `TableIdent` is namespace+table only); the write path uses this to resolve
/// the *target* catalog instead of the configured default warehouse. Mirrors
/// the read path's `sqe_sql::extract_catalog_qualifiers` access (`.value`, no
/// quote/case normalisation) so reads and writes resolve the same catalog name.
pub(crate) fn catalog_qualifier(name: &ObjectName) -> Option<String> {
    let parts: Vec<&str> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|ident| ident.value.as_str())
        .collect();
    match parts.as_slice() {
        [catalog, _namespace, _table] => Some((*catalog).to_string()),
        _ => None,
    }
}

/// Extract the catalog qualifier from a 2-part *schema* reference
/// (`catalog.schema`). Returns `None` for a 1-part bare schema (resolves in the
/// default warehouse). This is the schema-DDL analogue of [`catalog_qualifier`]:
/// `CREATE/DROP SCHEMA catalog.schema` is two parts (catalog + namespace),
/// whereas a table reference is three (catalog + namespace + table).
pub(crate) fn schema_catalog_qualifier(name: &ObjectName) -> Option<String> {
    let parts: Vec<&str> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|ident| ident.value.as_str())
        .collect();
    match parts.as_slice() {
        [catalog, _schema] => Some((*catalog).to_string()),
        _ => None,
    }
}

/// Strip a leading catalog qualifier from a 2-part schema `ObjectName`, yielding
/// the bare namespace. `catalog.schema` -> namespace `schema`; a 1-part name is
/// returned as-is. Used so `CREATE/DROP SCHEMA catalog.schema` creates/drops the
/// `schema` namespace in the resolved catalog rather than a `catalog.schema`
/// two-level namespace in the default warehouse.
fn namespace_without_catalog(name: &ObjectName) -> sqe_core::Result<NamespaceIdent> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.clone())
        .collect();
    match parts.as_slice() {
        [_catalog, schema] => Ok(NamespaceIdent::new(schema.clone())),
        _ => parse_namespace_from_object_name(name),
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
        .filter_map(|p| p.as_ident())
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

/// Short label for a RefDdl, used in logs.
fn ddl_action_label(ddl: &RefDdl) -> &'static str {
    match ddl {
        RefDdl::CreateBranch { .. } => "create_branch",
        RefDdl::CreateTag { .. } => "create_tag",
        RefDdl::DropBranch { .. } => "drop_branch",
        RefDdl::DropTag { .. } => "drop_tag",
    }
}

/// Resolve a user-provided snapshot id, falling back to the current snapshot
/// when `given` is None. Fails when the snapshot id does not exist in the
/// table's history, or when the table has no snapshots at all.
fn resolve_snapshot_id(
    table: &iceberg::table::Table,
    given: Option<i64>,
) -> sqe_core::Result<i64> {
    match given {
        Some(id) => {
            if table.metadata().snapshot_by_id(id).is_none() {
                return Err(SqeError::Execution(format!(
                    "snapshot id {id} not found in table history"
                )));
            }
            Ok(id)
        }
        None => table.metadata().current_snapshot_id().ok_or_else(|| {
            SqeError::Execution(
                "cannot create a ref on a table with no snapshots; run INSERT first"
                    .to_string(),
            )
        }),
    }
}

fn branch_retention(spec: &BranchRetention) -> SnapshotRetention {
    SnapshotRetention::Branch {
        min_snapshots_to_keep: spec.min_snapshots_to_keep,
        max_snapshot_age_ms: spec.max_snapshot_age_ms,
        max_ref_age_ms: spec.max_ref_age_ms,
    }
}

/// Parse a dotted string like `ns.t` into a sqlparser `ObjectName`.
fn parse_object_name(s: &str) -> sqe_core::Result<ObjectName> {
    let idents: Vec<sqlparser::ast::Ident> = s
        .split('.')
        .map(|p| sqlparser::ast::Ident::new(p.trim_matches('"')))
        .collect();
    if idents.is_empty() {
        return Err(SqeError::Execution(format!(
            "invalid table reference: '{s}'"
        )));
    }
    Ok(ObjectName::from(idents))
}

/// Check if an iceberg error indicates a table was not found.
///
/// Distinct from `is_namespace_not_found`. A "namespace does not exist"
/// error means the schema is missing entirely, which is a different
/// failure class from a missing table inside an existing schema.
/// The two must not both swallow the same error or DROP TABLE IF EXISTS
/// will mask schema-creation failures (see Polaris bench incident
/// 2026-05-06: silent CREATE SCHEMA failure surfaced 37s later as a
/// CATALOG_ERROR on the next CTAS).
fn is_table_not_found(err: &iceberg::Error) -> bool {
    let msg = err.to_string().to_lowercase();

    // Reject namespace errors first so the "does not exist" / "404"
    // generic patterns below cannot match them.
    if is_namespace_not_found_msg(&msg) {
        return false;
    }

    msg.contains("table not found")
        || msg.contains("no such table")
        || msg.contains("table does not exist")
        || msg.contains("not found")
        || msg.contains("does not exist")
        || msg.contains("404")
}

/// Check if an iceberg error indicates a namespace was not found.
fn is_namespace_not_found(err: &iceberg::Error) -> bool {
    is_namespace_not_found_msg(&err.to_string().to_lowercase())
}

/// Internal helper. Matches the namespace-specific phrasings Polaris
/// and other Iceberg REST catalogs return for a missing namespace.
/// Caller must lower-case the message.
fn is_namespace_not_found_msg(msg: &str) -> bool {
    msg.contains("namespace not found")
        || msg.contains("no such namespace")
        || msg.contains("namespace does not exist")
        || msg.contains("under a namespace that does not exist")
        || msg.contains("schema not found")
        || msg.contains("schema does not exist")
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

    fn empty_locks() -> TableLockMap {
        StdArc::new(std::sync::Mutex::new(HashMap::new()))
    }

    #[test]
    fn fold_unquoted_ident_folds_unquoted_preserves_quoted() {
        // Unquoted -> lowercase (matches Trino + DataFusion query-side folding).
        assert_eq!(
            fold_unquoted_ident(&Ident::new("testInteger")),
            "testinteger"
        );
        assert_eq!(fold_unquoted_ident(&Ident::new("ALREADYUP")), "alreadyup");
        // Already lowercase is unchanged.
        assert_eq!(fold_unquoted_ident(&Ident::new("plain")), "plain");
        // Double-quoted -> case preserved.
        assert_eq!(
            fold_unquoted_ident(&Ident::with_quote('"', "KeepMe")),
            "KeepMe"
        );
    }

    #[test]
    fn remove_struct_subfield_drops_leaf_and_recurses() {
        use iceberg::spec::{NestedField, PrimitiveType, StructType, Type};
        let int_ty = || Type::Primitive(PrimitiveType::Int);
        // struct { a (id 2), b (id 3) }
        let st = StructType::new(vec![
            StdArc::new(NestedField::required(2, "a", int_ty())),
            StdArc::new(NestedField::required(3, "b", int_ty())),
        ]);
        let out = remove_struct_subfield(&st, &["b".to_string()], false)
            .unwrap()
            .unwrap();
        assert_eq!(
            out.fields().iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["a"]
        );
        assert_eq!(out.fields()[0].id, 2, "retained field keeps its id");

        // Missing leaf: errors without IF EXISTS, no-op (None) with it.
        assert!(remove_struct_subfield(&st, &["z".to_string()], false).is_err());
        assert!(remove_struct_subfield(&st, &["z".to_string()], true)
            .unwrap()
            .is_none());

        // Nested: struct { s: struct { x (6), y (7) } } drop s.y
        let nested = StructType::new(vec![StdArc::new(NestedField::required(
            5,
            "s",
            Type::Struct(StructType::new(vec![
                StdArc::new(NestedField::required(6, "x", int_ty())),
                StdArc::new(NestedField::required(7, "y", int_ty())),
            ])),
        ))]);
        let out2 = remove_struct_subfield(&nested, &["s".to_string(), "y".to_string()], false)
            .unwrap()
            .unwrap();
        let Type::Struct(inner) = &*out2.fields()[0].field_type else {
            panic!("s should still be a struct");
        };
        assert_eq!(
            inner.fields().iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            vec!["x"]
        );
        assert_eq!(inner.fields()[0].id, 6, "nested retained field keeps its id");
    }

    #[test]
    fn keyed_lock_same_key_returns_same_arc() {
        let locks = empty_locks();
        let a = keyed_lock(&locks, "sales.orders".to_string());
        let b = keyed_lock(&locks, "sales.orders".to_string());
        // Same underlying Mutex => the two callers serialize on it.
        assert!(StdArc::ptr_eq(&a, &b), "same key must share one lock");
    }

    #[test]
    fn keyed_lock_different_keys_return_distinct_arcs() {
        let locks = empty_locks();
        let a = keyed_lock(&locks, "sales.orders".to_string());
        let b = keyed_lock(&locks, "sales.customers".to_string());
        // Different tables get independent locks => no false contention.
        assert!(!StdArc::ptr_eq(&a, &b), "different keys must not share a lock");
    }

    /// Two tasks contending on the SAME keyed lock observe mutual exclusion:
    /// the read-merge-commit critical section never interleaves, which is what
    /// prevents the lost-update on concurrent SET TAGS. We model the critical
    /// section with a non-atomic read-modify-write of a shared counter under the
    /// lock; if the lock failed to serialize, the final value would be < the
    /// number of increments.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn keyed_lock_serializes_same_key_critical_section() {
        let locks = empty_locks();
        let counter = StdArc::new(std::sync::Mutex::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..50 {
            let lock = keyed_lock(&locks, "ns.tbl".to_string());
            let counter = StdArc::clone(&counter);
            handles.push(tokio::spawn(async move {
                let _guard = lock.lock().await;
                // Non-atomic RMW across an await: only safe because _guard
                // serializes. Read, yield, write-back.
                let cur = *counter.lock().unwrap();
                tokio::task::yield_now().await;
                *counter.lock().unwrap() = cur + 1;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            *counter.lock().unwrap(),
            50,
            "serialized critical section must not lose updates"
        );
    }

    #[test]
    fn test_parse_table_ref_one_part() {
        let name = ObjectName::from(vec![Ident::new("my_table")]);
        let ident = parse_table_ref(&name).unwrap();
        assert_eq!(ident.namespace(), &NamespaceIdent::new("default".to_string()));
        assert_eq!(ident.name(), "my_table");
    }

    #[test]
    fn test_parse_table_ref_two_parts() {
        let name = ObjectName::from(vec![Ident::new("my_schema"), Ident::new("my_table")]);
        let ident = parse_table_ref(&name).unwrap();
        assert_eq!(ident.namespace(), &NamespaceIdent::new("my_schema".to_string()));
        assert_eq!(ident.name(), "my_table");
    }

    #[test]
    fn test_parse_table_ref_three_parts() {
        let name = ObjectName::from(vec![
            Ident::new("my_catalog"),
            Ident::new("my_schema"),
            Ident::new("my_table"),
        ]);
        let ident = parse_table_ref(&name).unwrap();
        assert_eq!(ident.namespace(), &NamespaceIdent::new("my_schema".to_string()));
        assert_eq!(ident.name(), "my_table");
    }

    #[test]
    fn test_parse_table_ref_empty_is_error() {
        let name = ObjectName::from(vec![] as Vec<Ident>);
        assert!(parse_table_ref(&name).is_err());
    }

    #[test]
    fn test_parse_table_ref_four_parts_is_error() {
        let name = ObjectName::from(vec![
            Ident::new("a"),
            Ident::new("b"),
            Ident::new("c"),
            Ident::new("d"),
        ]);
        assert!(parse_table_ref(&name).is_err());
    }

    #[test]
    fn test_catalog_qualifier_one_part_is_none() {
        let name = ObjectName::from(vec![Ident::new("my_table")]);
        assert_eq!(catalog_qualifier(&name), None);
    }

    #[test]
    fn test_catalog_qualifier_two_parts_is_none() {
        let name = ObjectName::from(vec![Ident::new("my_schema"), Ident::new("my_table")]);
        assert_eq!(catalog_qualifier(&name), None);
    }

    #[test]
    fn test_catalog_qualifier_three_parts_returns_catalog() {
        let name = ObjectName::from(vec![
            Ident::new("team_a_data"),
            Ident::new("public"),
            Ident::new("events"),
        ]);
        assert_eq!(catalog_qualifier(&name), Some("team_a_data".to_string()));
    }

    #[test]
    fn test_catalog_qualifier_four_parts_is_none() {
        let name = ObjectName::from(vec![
            Ident::new("a"),
            Ident::new("b"),
            Ident::new("c"),
            Ident::new("d"),
        ]);
        assert_eq!(catalog_qualifier(&name), None);
    }

    // ─── schema_catalog_qualifier: 2-part catalog.schema ──────────────
    #[test]
    fn test_schema_catalog_qualifier_one_part_is_none() {
        let name = ObjectName::from(vec![Ident::new("dev_raw")]);
        assert_eq!(schema_catalog_qualifier(&name), None);
    }

    #[test]
    fn test_schema_catalog_qualifier_two_parts_returns_catalog() {
        let name = ObjectName::from(vec![Ident::new("ws_team_a"), Ident::new("dev_raw")]);
        assert_eq!(schema_catalog_qualifier(&name), Some("ws_team_a".to_string()));
    }

    #[test]
    fn test_schema_catalog_qualifier_three_parts_is_none() {
        let name = ObjectName::from(vec![Ident::new("a"), Ident::new("b"), Ident::new("c")]);
        assert_eq!(schema_catalog_qualifier(&name), None);
    }

    // ─── namespace_without_catalog: peel the catalog off catalog.schema ──
    #[test]
    fn test_namespace_without_catalog_strips_catalog() {
        let name = ObjectName::from(vec![Ident::new("ws_team_a"), Ident::new("dev_raw")]);
        let ns = namespace_without_catalog(&name).unwrap();
        assert_eq!(ns, NamespaceIdent::new("dev_raw".to_string()));
    }

    #[test]
    fn test_namespace_without_catalog_one_part_unchanged() {
        let name = ObjectName::from(vec![Ident::new("dev_raw")]);
        let ns = namespace_without_catalog(&name).unwrap();
        assert_eq!(ns, NamespaceIdent::new("dev_raw".to_string()));
    }

    // ─── Error discriminator tests ────────────────────────────────
    //
    // The risk these tests guard against: a missing-namespace error
    // getting mis-classified as table-not-found and silently swallowed
    // by DROP TABLE IF EXISTS. That masked an upstream CREATE SCHEMA
    // failure in the Polaris bench on 2026-05-06 (clickbench load).
    //
    // We construct iceberg::Error via the public Error::new API. The
    // Polaris error path uses ErrorKind::Unexpected with a message
    // string, not the typed NamespaceNotFound kind, so the match has
    // to read the message.

    use iceberg::{Error as IcebergError, ErrorKind};

    fn unexpected(msg: &str) -> IcebergError {
        IcebergError::new(ErrorKind::Unexpected, msg)
    }

    #[test]
    fn namespace_not_found_recognises_polaris_create_table_phrasing() {
        // Exact wording observed from Polaris in the 2026-05-06 incident.
        let e = unexpected(
            "Failed to create table: Unexpected => \
             Tried to create a table under a namespace that does not exist",
        );
        assert!(is_namespace_not_found(&e));
    }

    #[test]
    fn namespace_not_found_recognises_common_phrasings() {
        for msg in [
            "namespace does not exist",
            "Namespace not found",
            "no such namespace: foo",
            "schema does not exist",
            "schema not found",
        ] {
            let e = unexpected(msg);
            assert!(is_namespace_not_found(&e), "should match: {msg}");
        }
    }

    #[test]
    fn table_not_found_does_not_swallow_namespace_errors() {
        // Critical invariant: if we cannot tell table-missing from
        // namespace-missing, DROP TABLE IF EXISTS will swallow a
        // missing-schema condition and the operator will only see the
        // failure on the next CTAS, far from the root cause.
        for msg in [
            "Tried to create a table under a namespace that does not exist",
            "namespace does not exist",
            "no such namespace: foo",
        ] {
            let e = unexpected(msg);
            assert!(
                !is_table_not_found(&e),
                "is_table_not_found must reject namespace-missing message: {msg}"
            );
        }
    }

    #[test]
    fn table_not_found_recognises_real_table_errors() {
        for msg in [
            "table not found: foo.bar",
            "No such table",
            "Table does not exist",
            "404 Not Found",
        ] {
            let e = unexpected(msg);
            assert!(is_table_not_found(&e), "should match: {msg}");
        }
    }

    #[test]
    fn namespace_already_exists_recognises_common_phrasings() {
        for msg in ["namespace already exists", "409 Conflict", "Conflict"] {
            let e = unexpected(msg);
            assert!(is_namespace_already_exists(&e), "should match: {msg}");
        }
    }
}
