//! The SHOW family: `SHOW CREATE TABLE / VIEW / SCHEMA`, `SHOW CATALOGS /
//! SCHEMAS / TABLES / VIEWS / COLUMNS / FUNCTIONS / ROLES`, and the column lookup
//! `DESCRIBE` shares with `SHOW COLUMNS`. The table-level statements refuse a
//! table the policy store hides from the caller before they read its metadata.

use super::*;

impl QueryHandler {
    /// Handle SHOW CREATE TABLE by querying information_schema and reconstructing DDL.
    pub(super) async fn handle_show_create_table(
        &self,
        session: &Session,
        stmt: &Statement,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use arrow_array::StringArray as ArrowStringArray;

        // Extract object name from the ShowCreate statement
        let Statement::ShowCreate { obj_type, obj_name } = stmt else {
            let msg = "Expected ShowCreate statement".to_string();
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, msg));
        };
        let table_name = obj_name.to_string();

        // An Iceberg view renders its stored body, as written. Looked up
        // before the column path below, which succeeds on a view too and
        // would print it as a table. The wrong kind fails the way Trino
        // fails it.
        let want_view = matches!(obj_type, sqlparser::ast::ShowCreateObject::View);
        let view_body = self
            .show_create_view_body(session, obj_name)
            .await
            .or_else(|e| if want_view { Err(e) } else { Ok(None) })?;
        match (view_body, want_view) {
            (Some(body), true) => {
                let ddl = format!("CREATE VIEW {table_name} AS\n{body}");
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "Create View",
                    DataType::Utf8,
                    false,
                )]));
                let result = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(ArrowStringArray::from(vec![ddl.as_str()])) as ArrayRef],
                )
                .map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!("Failed to build result batch: {e}"),
                    )
                })?;
                return Ok(vec![result]);
            }
            (Some(_), false) => {
                return Err(SqeError::with_code(
                    SqeErrorCode::SemanticError,
                    format!("Relation '{table_name}' is a view, not a table"),
                ));
            }
            (None, true) => {
                return Err(SqeError::with_code(
                    SqeErrorCode::SemanticError,
                    format!("Relation '{table_name}' is a table, not a view"),
                ));
            }
            (None, false) => {}
        }

        // The table's column definitions, from the table alone; see
        // `single_table_columns`. The refresh resolves a namespace created
        // since this provider listed (a CTAS just before, #850).
        let probe = format!("SELECT * FROM {table_name}");
        self.refresh_listing_snapshots(ctx, session_catalog, session, &probe)
            .await;
        let batches = single_table_columns(ctx, &table_name).await?;

        let mut columns = Vec::new();
        for batch in &batches {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<ArrowStringArray>()
                .ok_or_else(|| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        "Unexpected column_name type".to_string(),
                    )
                })?;
            let types = batch
                .column(1)
                .as_any()
                .downcast_ref::<ArrowStringArray>()
                .ok_or_else(|| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        "Unexpected data_type type".to_string(),
                    )
                })?;
            let nullables = batch
                .column(2)
                .as_any()
                .downcast_ref::<ArrowStringArray>()
                .ok_or_else(|| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        "Unexpected is_nullable type".to_string(),
                    )
                })?;

            for i in 0..batch.num_rows() {
                columns.push(sqe_catalog::show_create::DdlColumn {
                    name: names.value(i).to_string(),
                    // The Arrow display string, as the Trino type CREATE
                    // TABLE takes back: `integer`, `timestamp(9)`, `variant`
                    // (#849).
                    ddl_type: sqe_trino_compat::info_schema_compat::arrow_display_to_ddl_type(
                        types.value(i),
                    ),
                    nullable: nullables.value(i) != "NO",
                });
            }
        }
        // No columns: the table does not exist or is not visible. Say so,
        // as the Hive path does, rather than print an empty CREATE TABLE.
        if columns.is_empty() {
            return Err(SqeError::with_code(
                SqeErrorCode::TableNotFound,
                format!("table not found: {table_name}"),
            ));
        }

        // The partitioning, sort order and user-set properties come from the
        // Iceberg metadata.
        // Resolved as every other statement resolves a name: quotes stripped,
        // a bare name in the session schema. The table loads from the catalog
        // the name resolves in (its qualifier, else the session default):
        // `session_catalog` is the config default's, and read another
        // catalog's table of the same name, or none.
        let table_ident = crate::ident::resolve_table_ident(obj_name, session)?;
        let qualifier = crate::ident::catalog_qualifier(obj_name);
        // Best effort, like the load: an ATTACHed catalog has columns but no
        // `SessionCatalog`, and its DDL prints without properties.
        let props_table = match self.show_catalog(session, qualifier.as_deref()).await {
            Ok(props_catalog) => props_catalog.load_table(&table_ident).await.ok(),
            Err(_) => None,
        };
        let ddl = sqe_catalog::show_create::render_create_table(
            &split_qualified_identifier(&table_name),
            &columns,
            props_table.as_ref().map(|t| t.metadata()),
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "Create Table",
            DataType::Utf8,
            false,
        )]));
        let result = RecordBatch::try_new(
            schema,
            vec![Arc::new(ArrowStringArray::from(vec![ddl.as_str()])) as ArrayRef],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build result batch: {e}"),
            )
        })?;

        Ok(vec![result])
    }

    /// The stored SQL of the Iceberg view `obj_name` names, or `None` when
    /// it names no view. Resolved like the table path resolves it: the
    /// name's catalog qualifier, else the session default. A catalog with
    /// no `SessionCatalog` (an ATTACHed one) or no view endpoints holds no
    /// views SQE can show.
    async fn show_create_view_body(
        &self,
        session: &Session,
        obj_name: &sqlparser::ast::ObjectName,
    ) -> sqe_core::Result<Option<String>> {
        let ident = crate::ident::resolve_table_ident(obj_name, session)?;
        let qualifier = crate::ident::catalog_qualifier(obj_name);
        let Ok(catalog) = self.show_catalog(session, qualifier.as_deref()).await else {
            return Ok(None);
        };
        if !catalog.is_rest_backend() {
            return Ok(None);
        }
        catalog.load_view_sql(ident.namespace(), ident.name()).await
    }

    /// Handle SHOW CREATE SCHEMA <name> by reconstructing the namespace DDL
    /// from catalog metadata, mirroring `handle_show_create_table`'s output
    /// shape. Trino emits a single-column result named `Create Schema`; the
    /// value is a `CREATE SCHEMA <name>` string with an optional
    /// `WITH ( location = '...' )` clause for any namespace `location`
    /// property. sqlparser cannot parse the SHOW CREATE SCHEMA form, so this
    /// is reached via the `ShowCreateSchema(String)` classification rather than
    /// an AST match. (#351a)
    pub(super) async fn handle_show_create_schema(
        &self,
        schema_ref: &str,
        session_catalog: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use arrow_array::StringArray as ArrowStringArray;

        // The reference may be catalog-qualified (`iceberg.tpch_demo`) or a
        // bare schema (`tpch_demo`). The namespace ident is the last part; a
        // catalog prefix is only used for the displayed DDL name. Quoting is
        // stripped so the lookup uses the raw namespace name.
        let cleaned = schema_ref.trim().trim_end_matches(';').trim();
        let parts: Vec<String> = cleaned
            .split('.')
            .map(|p| p.trim().trim_matches('"').trim_matches('\'').to_string())
            .collect();
        let ns_name = parts
            .last()
            .filter(|p| !p.is_empty())
            .ok_or_else(|| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    "SHOW CREATE SCHEMA requires a schema name".to_string(),
                )
            })?
            .clone();

        let ns_ident = iceberg::NamespaceIdent::new(ns_name.clone());
        // Resolve the namespace so a missing schema errors like Trino, and so
        // any `location` property can be surfaced in the DDL. A schema that
        // does not exist (or a backend that rejects the lookup) surfaces a
        // clear error rather than fabricating DDL for a non-existent schema.
        let location = match session_catalog.get_namespace(&ns_ident).await {
            Ok(ns) => ns.properties().get("location").cloned(),
            Err(e) => {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to describe schema {ns_name}: {e}"),
                ));
            }
        };

        let mut ddl = format!("CREATE SCHEMA {cleaned}");
        if let Some(loc) = location.filter(|l| !l.is_empty()) {
            let loc_escaped = loc.replace('\'', "''");
            ddl.push_str(&format!("\nWITH (\n   location = '{loc_escaped}'\n)"));
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "Create Schema",
            DataType::Utf8,
            false,
        )]));
        let result = RecordBatch::try_new(
            schema,
            vec![Arc::new(ArrowStringArray::from(vec![ddl.as_str()])) as ArrayRef],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build result batch: {e}"),
            )
        })?;

        Ok(vec![result])
    }

    /// Handle SHOW CATALOGS by listing every catalog the session can reach:
    /// the configured catalogs plus the session's own (X-Trino-Catalog) Polaris
    /// warehouse, deduplicated. JDBC schema sync (DatabaseMetaData.getCatalogs)
    /// then sees workspace catalogs, not just the default warehouse. (#5)
    pub(super) async fn handle_show_catalogs(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Configured catalogs first (default warehouse leads), then the session
        // catalog if it is not already in the list.
        let mut candidates: Vec<String> = self
            .config
            .flattened_catalogs()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        if let Some(session_catalog) = session.default_catalog.as_deref() {
            candidates.push(session_catalog.to_string());
        }
        if candidates.is_empty() {
            candidates.push(self.config.catalog.warehouse.clone());
        }
        // Hive roots (`[[hive.roots]]`, `--glue-catalog`, `ATTACH ... TYPE
        // hive_external`) are catalogs a query can name, so they are listed
        // too; without them an MCP client's `list_catalogs` never learns the
        // Glue catalog exists.
        if let Ok(roots) = self.runtime_catalogs.hive_roots() {
            candidates.extend(roots.into_iter().map(|(name, _)| name));
        }
        let mut catalog_names: Vec<String> = Vec::new();
        for name in candidates {
            let name = if name.is_empty() {
                "default".to_string()
            } else {
                name
            };
            if !catalog_names.contains(&name) {
                catalog_names.push(name);
            }
        }
        // A catalog scope (#908) lists exactly its own catalogs: the hidden
        // ones drop out, and a scoped Polaris catalog not in config joins.
        if let Some(scope) = session.effective_scope()? {
            catalog_names.retain(|n| scope.access(n).is_some());
            for name in scope.catalog_names() {
                if !catalog_names.iter().any(|n| n.eq_ignore_ascii_case(name)) {
                    catalog_names.push(name.to_string());
                }
            }
        }

        // Trino's `SHOW CATALOGS` column is named `Catalog`.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Catalog",
            DataType::Utf8,
            false,
        )]));

        let mut builder = StringBuilder::new();
        for name in &catalog_names {
            builder.append_value(name);
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to create result batch: {e}"),
            )
        })?;

        Ok(vec![batch])
    }

    /// Handle SHOW SCHEMAS by listing namespaces from the Polaris catalog.
    ///
    /// Reuses the cached `Arc<SessionCatalog>` from `create_session_context`
    /// instead of rebuilding the wrapping on every call (issue #15).
    pub(super) async fn handle_show_schemas(
        &self,
        session: &Session,
        filter: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // `SHOW SCHEMAS FROM <catalog>` must list that catalog's namespaces, not
        // the default warehouse's. Resolve the named catalog (discovered under
        // polaris-auto) so reads line up with the write path. A trailing
        // `LIKE '<pattern>'` filters the names.
        let (filter, like) = ShowLike::split(filter);
        let keep = |name: &str| like.as_ref().is_none_or(|l| l.matches(name));
        let catalog = show_schemas_catalog(filter);

        // Trino's `SHOW SCHEMAS` column is named `Schema`.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Schema",
            DataType::Utf8,
            false,
        )]));
        let mut builder = StringBuilder::new();

        // `system` is SQE's virtual schemas plus, when one exists, a Polaris
        // catalog of that name (#901). List both; a Polaris namespace with a
        // reserved name is shadowed, so it is not listed.
        if catalog.as_deref() == Some(sqe_core::system_catalog::SYSTEM_CATALOG) {
            for name in self.system_schema_names(session).await? {
                if keep(&name) {
                    builder.append_value(name);
                }
            }
            let array: ArrayRef = Arc::new(builder.finish());
            let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to create result batch: {e}"),
                )
            })?;
            return Ok(vec![batch]);
        }

        // A Hive root is not an Iceberg catalog: `show_catalog` knows
        // `[catalogs.*]` and Polaris discovery only, so a root's name would
        // fail as "Unknown catalog" while `SHOW TABLES FROM <root>.<db>`
        // works. Answer from the root's metadata source (Glue's databases,
        // or the sidecar warehouse's directories) in the same shape.
        if let Some(root) = catalog.as_deref() {
            if self.runtime_catalogs.is_hive_root(root).unwrap_or(false) {
                for name in self.hive_ddl_handler.list_databases(root).await? {
                    if keep(&name) {
                        builder.append_value(name);
                    }
                }
                let array: ArrayRef = Arc::new(builder.finish());
                let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!("Failed to create result batch: {e}"),
                    )
                })?;
                return Ok(vec![batch]);
            }
        }

        let session_catalog = self.show_catalog(session, catalog.as_deref()).await?;

        // A principal that is not authorized to list this catalog's namespaces
        // gets an empty result, not a hard error -- so JDBC schema sync and
        // SHOW SCHEMAS skip catalogs the user can't see instead of aborting. (#5)
        let namespaces = namespaces_or_empty_on_denial(missing_catalog_as_not_found(
            catalog.as_deref(),
            session_catalog.list_namespaces().await,
        ))?;

        for ns in &namespaces {
            // Namespace is a list of parts, join with "."
            let name: Vec<&str> = ns.as_ref().iter().map(|s| s.as_str()).collect();
            let name = name.join(".");
            if keep(&name) {
                builder.append_value(name);
            }
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to create result batch: {e}"),
            )
        })?;

        Ok(vec![batch])
    }

    /// Handle `SHOW COLUMNS FROM ns.table` (Trino syntax) by translating
    /// into a query against `information_schema.columns`. Same pattern as
    /// `handle_show_create_table`'s column lookup.
    ///
    /// Trino's full `SHOW COLUMNS` output has four columns (Column, Type,
    /// Extra, Comment); SQE returns (column_name, data_type, is_nullable),
    /// which is the subset dbt and BI clients use for schema inspection.
    pub(super) async fn handle_show_columns(
        &self,
        session: &Session,
        show_options: &sqlparser::ast::ShowStatementOptions,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // sqlparser groups the table identifier under
        // `show_in.parent_name`. Both `show_in` and `parent_name` are
        // optional in the AST; missing either means the query did not
        // name a table to inspect.
        let table_name = show_options
            .show_in
            .as_ref()
            .and_then(|si| si.parent_name.as_ref())
            .map(|n| n.to_string())
            .ok_or_else(|| {
                SqeError::Execution(
                    "SHOW COLUMNS requires `FROM <table>` (or `IN <table>`)".to_string(),
                )
            })?;

        // Use only the bare table name for the WHERE filter; information_schema
        // queries collapse to the leaf table name.
        self.columns_for_table(session, &table_name).await
    }

    /// The schemas of the `system` catalog: the virtual ones (`jdbc`,
    /// `metadata`, `runtime`), then the namespaces of a Polaris catalog
    /// `system` the caller may list (#901). No Polaris catalog `system`, or
    /// one the caller may not list: the virtual schemas only.
    async fn system_schema_names(&self, session: &Session) -> sqe_core::Result<Vec<String>> {
        use sqe_core::system_catalog::{is_reserved_system_schema, SYSTEM_CATALOG};
        let mut names: Vec<String> = vec!["jdbc".into(), "metadata".into(), "runtime".into()];
        if self.config.query.catalog_discovery != sqe_core::config::CatalogDiscovery::PolarisAuto {
            return Ok(names);
        }
        let polaris = crate::session_context::discover_session_catalog(
            SYSTEM_CATALOG,
            &self.config,
            session,
            self.table_cache.as_ref(),
        )
        .await;
        let Some(polaris) = polaris else {
            return Ok(names);
        };
        let namespaces = match polaris.list_namespaces().await {
            Ok(ns) => ns,
            Err(e) if e.is_not_found() => Vec::new(),
            Err(e) => namespaces_or_empty_on_denial(Err(e))?,
        };
        names.extend(
            namespaces
                .iter()
                .map(|ns| ns.as_ref().join("."))
                .filter(|n| !is_reserved_system_schema(n)),
        );
        Ok(names)
    }

    /// Column metadata for a table via `information_schema.columns`, shared by
    /// `SHOW COLUMNS` and `DESCRIBE <table>`.
    pub(super) async fn columns_for_table(
        &self,
        session: &Session,
        table_name: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // A fully-qualified name under a Hive root is answered from the
        // table's manifest. The `information_schema.columns` route below
        // refreshes the listing snapshots of every catalog first, which
        // against a live Glue catalog walks every database and table for one
        // DESCRIBE (minutes, and a GetTables call per table).
        let parts = split_qualified_identifier(table_name);
        if let [root, database, table] = parts.as_slice() {
            if self.runtime_catalogs.is_hive_root(root).unwrap_or(false) {
                let columns = self
                    .hive_ddl_handler
                    .describe_table(&session.user, root, database, table)
                    .await?;
                return hive_columns_batch(&columns);
            }
        }

        self.deny_hidden_table(session, table_name).await?;

        let (ctx, session_catalog) = self.create_session_context(session).await?;
        // `system.<schema>.t` outside the virtual schemas is a table of a
        // Polaris catalog `system`, attached to the composite on demand (#901).
        if let [catalog, schema, _] = parts.as_slice() {
            if catalog == sqe_core::system_catalog::SYSTEM_CATALOG
                && !sqe_core::system_catalog::is_reserved_system_schema(schema)
            {
                self.attach_polaris_system(&ctx, session).await;
            }
        }
        // The schema lookups serve snapshots (review 806); fill the ones
        // this name touches first.
        let probe = format!("SELECT * FROM {table_name}");
        self.refresh_listing_snapshots(&ctx, &session_catalog, session, &probe)
            .await;
        let batches = single_table_columns(&ctx, table_name).await?;
        // Iceberg keeps a per-column `doc`; `information_schema.columns`
        // does not carry it, so fetch the (cached) table metadata for the
        // comments. Best effort: a table that is not an Iceberg table, or
        // one this catalog cannot load, simply has no comments.
        let comments = match parts.as_slice() {
            [.., schema, table] => {
                self.iceberg_column_comments(session, &parts, schema, table)
                    .await
            }
            _ => std::collections::HashMap::new(),
        };
        with_column_comments(batches, &comments)
    }

    /// Refuse `SHOW CREATE TABLE`, `DESCRIBE` / `SHOW COLUMNS` and `CALL
    /// system.table_health` on an Iceberg table the policy store denies the
    /// caller all access to, as "table not found" (the metadata TVF gate's
    /// wording). Hive tables are gated by `HiveDdlHandler` on their own path.
    /// A bare name is checked under both the session schema and `default`,
    /// since these handlers resolve it against either.
    pub(super) async fn deny_hidden_table(
        &self,
        session: &Session,
        table_name: &str,
    ) -> sqe_core::Result<()> {
        let parts = split_qualified_identifier(table_name);
        let targets: Vec<(String, String)> = match parts.as_slice() {
            [.., ns, table] => vec![(ns.clone(), table.clone())],
            [table] => {
                let mut namespaces = vec!["default".to_string()];
                namespaces.extend(session.default_schema.clone());
                namespaces
                    .into_iter()
                    .map(|ns| (ns, table.clone()))
                    .collect()
            }
            [] => Vec::new(),
        };
        let catalog = match parts.as_slice() {
            [catalog, _, _] => catalog.clone(),
            _ => self.session_catalog_name(session),
        };
        deny_hidden_metadata_tvf_targets(
            self.policy_store.as_deref(),
            &session.user,
            self.config.policy.mask_precedence,
            &catalog,
            &targets,
        )
        .await
    }

    /// `doc` per column of an Iceberg table, keyed by column name. Empty when
    /// the table cannot be loaded as Iceberg; `SHOW COLUMNS` never fails on
    /// a missing comment.
    async fn iceberg_column_comments(
        &self,
        session: &Session,
        parts: &[String],
        schema: &str,
        table: &str,
    ) -> std::collections::HashMap<String, String> {
        use iceberg::{NamespaceIdent, TableIdent};
        let catalog = match parts.len() {
            3 => Some(parts[0].as_str()),
            _ => None,
        };
        let Ok(session_catalog) = self.show_catalog(session, catalog).await else {
            return std::collections::HashMap::new();
        };
        let ident = TableIdent::new(NamespaceIdent::new(schema.to_string()), table.to_string());
        match session_catalog.load_table(&ident).await {
            Ok(loaded) => {
                let metadata = loaded.metadata();
                // The schema's own `doc` first; then SQE's `COMMENT ON COLUMN`,
                // which `handle_comment_on` stores as the table property
                // `comment.<column>` because the REST catalog has no
                // per-field doc update.
                let mut comments: std::collections::HashMap<String, String> = metadata
                    .current_schema()
                    .as_struct()
                    .fields()
                    .iter()
                    .filter_map(|f| {
                        f.doc
                            .as_deref()
                            .map(str::trim)
                            .filter(|d| !d.is_empty())
                            .map(|d| (f.name.clone(), d.to_string()))
                    })
                    .collect();
                for (key, value) in metadata.properties() {
                    if let Some(column) = key.strip_prefix("comment.") {
                        let value = value.trim();
                        if !value.is_empty() {
                            comments
                                .entry(column.to_string())
                                .or_insert_with(|| value.to_string());
                        }
                    }
                }
                comments
            }
            Err(error) => {
                tracing::debug!(%error, table = %ident, "no Iceberg column comments for SHOW COLUMNS");
                std::collections::HashMap::new()
            }
        }
    }

    /// Handle SHOW TABLES by listing tables in a namespace from the Polaris catalog.
    ///
    /// Reuses the cached `Arc<SessionCatalog>` from `create_session_context`
    /// instead of re-running `SessionCatalog::for_session` (which re-hashes
    /// the bearer and rebuilds the props map on every call — issue #15).
    pub(super) async fn handle_show_tables(
        &self,
        session: &Session,
        filter: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // `SHOW TABLES FROM <catalog>.<schema>` must list tables from that
        // catalog, not the default warehouse. Resolve the leading catalog
        // qualifier (discovered under polaris-auto); the schema part is parsed
        // separately by parse_show_tables_namespace. A trailing
        // `LIKE '<pattern>'` filters the names.
        let (filter, like) = ShowLike::split(filter);
        let keep = |name: &str| like.as_ref().is_none_or(|l| l.matches(name));
        let catalog = show_tables_catalog(filter);
        // A virtual `system` schema is listed from SQE's own provider, never
        // from a Polaris catalog `system`, whose namespace of the same name
        // the composite shadows (#901).
        if catalog.as_deref() == Some(sqe_core::system_catalog::SYSTEM_CATALOG) {
            if let Some(ns) = parse_show_tables_namespace(filter)
                .filter(|ns| sqe_core::system_catalog::is_reserved_system_schema(ns))
            {
                let (ctx, _) = self.create_session_context(session).await?;
                let mut names: Vec<String> = ctx
                    .catalog(sqe_core::system_catalog::SYSTEM_CATALOG)
                    .and_then(|c| c.schema(&ns.to_ascii_lowercase()))
                    .map(|s| s.table_names())
                    .unwrap_or_default();
                names.retain(|n| keep(n));
                names.sort();
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "Table",
                    DataType::Utf8,
                    false,
                )]));
                let mut table_builder = StringBuilder::new();
                for name in &names {
                    table_builder.append_value(name);
                }
                let array: ArrayRef = Arc::new(table_builder.finish());
                let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!("Failed to create result batch: {e}"),
                    )
                })?;
                return Ok(vec![batch]);
            }
        }
        // A Hive root is not an Iceberg catalog: `show_catalog` only knows
        // `[catalogs.*]` config and Polaris discovery, so a Hive root's name
        // would otherwise fail as "Unknown catalog". Answer directly from
        // the manifest store instead, in the same Trino single-`Table`-column
        // shape the Iceberg path below produces.
        if let Some(root) = catalog.as_deref() {
            if self.runtime_catalogs.is_hive_root(root).unwrap_or(false) {
                let database = parse_show_tables_namespace(filter)
                    .or_else(|| session.default_schema.clone())
                    .ok_or_else(|| {
                        SqeError::Execution(
                            "SHOW TABLES against a Hive root needs a database (e.g. \
                             `SHOW TABLES FROM lake.sales`)"
                                .to_string(),
                        )
                    })?;
                let mut names = self
                    .hive_ddl_handler
                    .list_tables(&session.user, root, &database)
                    .await?;
                names.retain(|n| keep(n));
                names.sort();
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "Table",
                    DataType::Utf8,
                    false,
                )]));
                let mut table_builder = StringBuilder::new();
                for name in &names {
                    table_builder.append_value(name);
                }
                let table_array: ArrayRef = Arc::new(table_builder.finish());
                let batch = RecordBatch::try_new(schema, vec![table_array]).map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!("Failed to create result batch: {e}"),
                    )
                })?;
                return Ok(vec![batch]);
            }
        }
        let session_catalog = self.show_catalog(session, catalog.as_deref()).await?;

        // Explicit `FROM <schema>` wins; otherwise fall back to the session
        // schema (X-Trino-Schema) so bare `SHOW TABLES` lists that schema's
        // tables (Trino behavior). Only when neither is set list all
        // namespaces. (#7)
        let ns_name =
            parse_show_tables_namespace(filter).or_else(|| session.default_schema.clone());
        let namespaces = match ns_name {
            // Same information-hiding contract as SHOW SCHEMAS: a principal who
            // may not list this catalog's namespaces gets an empty result, not
            // an error. Propagating the denial here leaked the raw Polaris
            // message, which names both the operation and the principal
            // ("Principal 'dave' is not authorized for op 'LIST_NAMESPACES'"),
            // while the same user running SHOW SCHEMAS got a silent 0 rows. The
            // pair disagreeing is what made it a leak: the erroring one told an
            // unprivileged caller that the catalog exists and that they are
            // known to it.
            //
            // The per-namespace list_tables below already degrades this way.
            None => namespaces_or_empty_on_denial(missing_catalog_as_not_found(
                catalog.as_deref(),
                session_catalog.list_namespaces().await,
            ))?,
            Some(name) => vec![iceberg::NamespaceIdent::new(name)],
        };

        // Trino's `SHOW TABLES` returns a SINGLE column named `Table` holding
        // bare table names (the schema is the FROM / session-schema context).
        // SQE previously returned two columns [namespace, table_name]; the
        // Trino / Starburst JDBC driver reads column 0 as the table name, so
        // the leading namespace column made every row collapse to the namespace
        // (e.g. Metabase schema sync saw one malformed `gold.` entry instead of
        // the real tables). Match Trino exactly: one `Table` column.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "Table",
            DataType::Utf8,
            false,
        )]));

        let mut names: Vec<String> = Vec::new();
        for ns in &namespaces {
            match session_catalog.list_tables(ns).await {
                Ok(tables) => {
                    for table in &tables {
                        names.push(table.name().to_string());
                    }
                }
                Err(e) => {
                    warn!(
                        namespace = ?ns,
                        error = %e,
                        "Failed to list tables in namespace, skipping"
                    );
                }
            }
            // Views too, as Trino's SHOW TABLES does: a Polaris view is
            // queried like a table, so a listing that omits it sends the
            // MCP crawler and every JDBC tool looking for something they can
            // never find. REST-only; other backends answer with an error
            // that is swallowed here, as `list_metadata_tables` does.
            if let Ok(views) = session_catalog.list_views(ns).await {
                names.extend(views);
            }
        }
        // Trino lists table names sorted.
        names.retain(|n| keep(n));
        names.sort();
        names.dedup();

        let mut table_builder = StringBuilder::new();
        for name in &names {
            table_builder.append_value(name);
        }
        let table_array: ArrayRef = Arc::new(table_builder.finish());

        let batch = RecordBatch::try_new(schema, vec![table_array]).map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to create result batch: {e}"),
            )
        })?;

        Ok(vec![batch])
    }

    /// `SHOW VIEWS FROM <catalog>.<schema>`: the views among that schema's
    /// tables, one `View` column, sorted. `SHOW TABLES` lists views alongside
    /// tables (as Trino does) and gives no way to tell them apart; the
    /// alternative, `information_schema.tables`, enumerates every schema of
    /// the catalog to answer for one, which against a Glue catalog of a
    /// hundred databases is a `GetTables` per database (12 s cold for one
    /// `describe_schema`). This is one listing of one schema.
    ///
    /// A Hive root answers from its manifests (`TableType = VIRTUAL_VIEW`),
    /// policy-gated like `SHOW TABLES`. An Iceberg catalog answers from the
    /// REST `list_views`; a backend without views is an empty result, not an
    /// error, matching how `SHOW TABLES` treats it.
    pub(super) async fn handle_show_views(
        &self,
        session: &Session,
        filter: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let catalog = show_tables_catalog(filter);
        let schema_name =
            parse_show_tables_namespace(filter).or_else(|| session.default_schema.clone());
        let mut names: Vec<String> = Vec::new();
        match catalog.as_deref() {
            Some(root) if self.runtime_catalogs.is_hive_root(root).unwrap_or(false) => {
                let database = schema_name.ok_or_else(|| {
                    SqeError::Execution(
                        "SHOW VIEWS against a Hive root needs a database (e.g. \
                         `SHOW VIEWS FROM lake.sales`)"
                            .to_string(),
                    )
                })?;
                names = self
                    .hive_ddl_handler
                    .list_views(&session.user, root, &database)
                    .await?;
            }
            _ => {
                let session_catalog = self.show_catalog(session, catalog.as_deref()).await?;
                let namespaces = match schema_name {
                    None => namespaces_or_empty_on_denial(session_catalog.list_namespaces().await)?,
                    Some(name) => vec![iceberg::NamespaceIdent::new(name)],
                };
                for ns in &namespaces {
                    if let Ok(views) = session_catalog.list_views(ns).await {
                        names.extend(views);
                    }
                }
            }
        }
        names.sort();
        names.dedup();

        let schema = Arc::new(Schema::new(vec![Field::new("View", DataType::Utf8, false)]));
        let mut builder = StringBuilder::new();
        for name in &names {
            builder.append_value(name);
        }
        let column: ArrayRef = Arc::new(builder.finish());
        RecordBatch::try_new(schema, vec![column])
            .map(|batch| vec![batch])
            .map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to create result batch: {e}"),
                )
            })
    }

    /// Handle `SHOW FUNCTIONS` by listing the registered scalar, aggregate, and
    /// window functions (#344).
    ///
    /// Trino's `SHOW FUNCTIONS` returns six columns (Function Name, Return Type,
    /// Argument Types, Function Type, Deterministic, Description). SQE emits the
    /// same column shape so BI tools that introspect the function catalog get a
    /// well-formed result. The row *content* is engine-specific and never
    /// matches Trino: DataFusion's UDFs carry variadic/overloaded signatures
    /// that don't decompose into Trino's one-row-per-signature type strings, so
    /// the type columns are left empty and every function is reported
    /// deterministic. Function Type is `scalar` / `aggregate` / `window`.
    pub(super) async fn handle_show_functions(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (ctx, _) = self.create_session_context(session).await?;
        let state = ctx.state();

        // Collect (name, kind) across the three registries, then sort so the
        // output is stable regardless of HashMap iteration order.
        let mut rows: Vec<(String, &'static str)> = Vec::new();
        for name in state.scalar_functions().keys() {
            rows.push((name.clone(), "scalar"));
        }
        for name in state.aggregate_functions().keys() {
            rows.push((name.clone(), "aggregate"));
        }
        for name in state.window_functions().keys() {
            rows.push((name.clone(), "window"));
        }
        rows.sort();

        let schema = Arc::new(Schema::new(vec![
            Field::new("Function Name", DataType::Utf8, false),
            Field::new("Return Type", DataType::Utf8, false),
            Field::new("Argument Types", DataType::Utf8, false),
            Field::new("Function Type", DataType::Utf8, false),
            Field::new("Deterministic", DataType::Boolean, false),
            Field::new("Description", DataType::Utf8, false),
        ]));

        let mut name_b = StringBuilder::new();
        let mut ret_b = StringBuilder::new();
        let mut arg_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();
        let mut desc_b = StringBuilder::new();
        for (name, kind) in &rows {
            name_b.append_value(name);
            ret_b.append_value("");
            arg_b.append_value("");
            type_b.append_value(kind);
            desc_b.append_value("");
        }
        let det = BooleanArray::from(vec![true; rows.len()]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name_b.finish()) as ArrayRef,
                Arc::new(ret_b.finish()) as ArrayRef,
                Arc::new(arg_b.finish()) as ArrayRef,
                Arc::new(type_b.finish()) as ArrayRef,
                Arc::new(det) as ArrayRef,
                Arc::new(desc_b.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build SHOW FUNCTIONS result: {e}"),
            )
        })?;

        Ok(vec![batch])
    }

    /// SHOW ROLES / SHOW CURRENT ROLES / SHOW ROLE GRANTS -> the caller's roles.
    ///
    /// SQE derives roles from the OIDC token, not a global role catalog, so all
    /// three return the caller's own roles under a single `Role` column (Trino's
    /// column name). This lets JDBC clients that probe roles on connect proceed
    /// (#350).
    pub(super) fn handle_show_roles(
        &self,
        session: &Session,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![Field::new("Role", DataType::Utf8, false)]));
        let mut role_b = StringBuilder::new();
        for role in &session.user.roles {
            role_b.append_value(role);
        }
        let batch = RecordBatch::try_new(schema, vec![Arc::new(role_b.finish()) as ArrayRef])
            .map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to build SHOW ROLES result: {e}"),
                )
            })?;
        Ok(vec![batch])
    }
}
