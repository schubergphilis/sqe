//! Catalog and secret statements: `SHOW STATS FOR` reads the current snapshot
//! summary, `ANALYZE` is accepted as a no-op after resolving the table,
//! `ATTACH` / `DETACH` register and drop external catalogs, and the
//! `CREATE SECRET` / `DROP SECRET` / `SHOW SECRETS` handlers manage the secret store.

use super::*;
use sqe_catalog::schema_tags::SchemaTagState;

impl QueryHandler {
    /// Handle `SHOW STATS FOR <table>` by reading the current snapshot summary.
    ///
    /// Returns a single-row RecordBatch with columns:
    /// - `column_name`   — `"<all columns>"` (aggregate row)
    /// - `row_count`     — total-records from snapshot summary
    /// - `data_file_count` — total-data-files from snapshot summary
    /// - `total_size`    — total-files-size from snapshot summary (bytes)
    pub(super) async fn handle_show_stats(
        &self,
        session: &Session,
        table_name: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use iceberg::{NamespaceIdent, TableIdent};

        // Parse "table" / "schema.table" / "catalog.schema.table". Resolve the
        // catalog via show_catalog (explicit qualifier wins, else the session
        // catalog, with polaris-auto discovery) so SHOW STATS finds tables in
        // the caller's catalog instead of failing against the default
        // warehouse ("Failed to load table ... does not exist"). (#6)
        //
        // Quote-aware: the MCP tools and Trino clients send
        // `"cat"."schema"."table"`, and a quoted part is a name, not a name
        // with quotes in it.
        let parts = split_qualified_identifier(table_name);
        let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
        let (catalog, namespace, bare_table) = match parts.as_slice() {
            [t] => (None, "default", *t),
            [s, t] => (None, *s, *t),
            [c, s, t, ..] => (Some(*c), *s, *t),
            [] => (None, "default", table_name),
        };

        // A Hive root answers from its manifest: the metastore's own
        // `numRows` / `totalSize`, never a scan (the MCP `describe_schema`
        // tool asks this for every table in a Glue database).
        if let Some(root) = catalog {
            if self.runtime_catalogs.is_hive_root(root).unwrap_or(false) {
                let stats = self
                    .hive_ddl_handler
                    .show_stats(&session.user, root, namespace, bare_table)
                    .await?;
                return build_show_stats_batch(&stats.columns, stats.row_count, stats.data_size);
            }
        }
        let session_catalog = self.show_catalog(session, catalog).await?;

        let ns_ident = NamespaceIdent::new(namespace.to_string());
        let table_ident = TableIdent::new(ns_ident, bare_table.to_string());
        let table = session_catalog.load_table(&table_ident).await?;
        let metadata = table.metadata();

        // Table-level row count from the current snapshot summary (None when the
        // table has never been written). Per-column stats (size, distinct, null
        // fraction, min/max) are not cheaply available from iceberg metadata, so
        // they are reported NULL -- the result SHAPE matches Trino so clients
        // and the CBO do not error.
        // A table the engine identity was not allowed to read carries no keys
        // and no counts: the snapshot summary is the person's metadata, but
        // the gate denied the read, so the count is not theirs to see (#934).
        let read_denied = sqe_catalog::engine_identity::is_read_denied(&table);
        let row_count: Option<f64> = metadata
            .current_snapshot()
            .filter(|_| !read_denied)
            .and_then(|s| {
                s.summary()
                    .additional_properties
                    .get("total-records")
                    .and_then(|v| v.parse::<f64>().ok())
            });

        let column_names: Vec<String> = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();

        // Architecture review 205 (#558): the same policy the plan rewriter
        // applies to a scan decides what this statement may say: restricted
        // columns are not listed here, a row filter makes the snapshot's total
        // row count someone else's
        // number so it is reported NULL, and a deny-all policy answers with
        // no rows at all. When per-column statistics land here (Puffin NDV,
        // min/max), they must come from the enforced plan or stay NULL for
        // masked and restricted columns; a raw min/max of a masked column is
        // the value leak this gate exists to prevent. The MCP
        // `get_table_stats` tool is a thin wrapper over this statement.
        let (column_names, row_count) = match self.policy_store.as_deref() {
            None => (column_names, row_count),
            Some(store) => {
                // Malformed tags are UNKNOWN, not empty: the rewriter fails
                // closed on them and so does this. Table tags count, as
                // the rewriter inherits them (#924).
                let schema_tags = match session_catalog
                    .schema_tag_state(&table_ident.namespace)
                    .await
                {
                    SchemaTagState::NotInherited => Vec::new(),
                    SchemaTagState::Known(tags) => tags,
                    SchemaTagState::Unknown => {
                        return Err(SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!(
                                "SHOW STATS: schema tags of {table_name} could not be read; \
                                 tag state unknown"
                            ),
                        ));
                    }
                };
                let col_tags = crate::tag_source_impl::effective_column_tags(
                    metadata.properties(),
                    metadata.current_schema(),
                    &schema_tags,
                )
                .map_err(|e| {
                    SqeError::with_code(
                        SqeErrorCode::ExecutionFailed,
                        format!("SHOW STATS: malformed column or table tags on {table_name}: {e}"),
                    )
                })?;
                let default_hive = sqe_policy::plan_rewriter::HiveCatalogs::new();
                let hive_catalogs = self.hive_catalogs.as_deref().unwrap_or(&default_hive);
                // Keyed by the catalog NAME the planner uses (explicit
                // qualifier, else the session's), not the warehouse, which can
                // differ and is what a catalog-scoped Ranger key must carry.
                let catalog_name = catalog
                    .map(str::to_string)
                    .unwrap_or_else(|| self.session_catalog_name(session));
                let Some((policy_ns, policy_table)) =
                    sqe_policy::plan_rewriter::resolve_policy_key_for_ident_scoped(
                        Some(&catalog_name),
                        table_ident.namespace().as_ref(),
                        table_ident.name(),
                        hive_catalogs,
                        store.catalog_scoped(),
                    )
                else {
                    return Err(SqeError::with_code(
                        SqeErrorCode::TableNotFound,
                        format!("table not found: {table_name}"),
                    ));
                };
                let policy = sqe_policy::plan_rewriter::resolve_effective_policy(
                    store,
                    &session.user,
                    Some(&catalog_name),
                    &policy_table,
                    &policy_ns,
                    &col_tags,
                    self.config.policy.mask_precedence,
                )
                .await;
                governed_show_stats(&policy, column_names, row_count)
            }
        };
        build_show_stats_batch(&column_names, row_count, None)
    }

    pub(super) async fn handle_analyze_dispatch(
        &self,
        session: &Session,
        table_name: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let table_ref = TableReference::from(table_name);
        if matches!(
            self.hive_ddl_handler
                .resolve_table_kind_for_session(session.default_catalog.as_deref(), &table_ref),
            crate::hive_ddl_handler::TableKind::Hive { .. }
        ) {
            return self
                .hive_ddl_handler
                .analyze_table(session, &table_ref)
                .await
                .map(|_| Vec::new());
        }
        self.handle_analyze(session, table_name).await
    }

    /// ANALYZE [catalog.][schema.]table [WITH (...)] — Trino's table-statistics
    /// command. SQE resolves and loads the table (so a missing table errors the
    /// same way a SELECT would) and returns a successful empty result. Computing
    /// or persisting Iceberg statistics is not yet implemented; the statement
    /// succeeds as a no-op so tooling that issues ANALYZE after a load (and the
    /// cost-based optimizer's stats refresh) does not fail (#329).
    async fn handle_analyze(
        &self,
        session: &Session,
        table_name: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use iceberg::{NamespaceIdent, TableIdent};

        // Resolve the same way SHOW STATS does: explicit catalog qualifier wins,
        // else the session catalog (with polaris-auto discovery). load_table
        // returns an error for a missing table, so ANALYZE on a non-existent
        // table fails instead of silently succeeding.
        let parts: Vec<&str> = table_name.splitn(3, '.').collect();
        let (catalog, namespace, bare_table) = match parts.len() {
            1 => (None, "default", parts[0]),
            2 => (None, parts[0], parts[1]),
            _ => (Some(parts[0]), parts[1], parts[2]),
        };
        let session_catalog = self.show_catalog(session, catalog).await?;
        let ns_ident = NamespaceIdent::new(namespace.to_string());
        let table_ident = TableIdent::new(ns_ident, bare_table.to_string());
        let _table = session_catalog.load_table(&table_ident).await?;

        // TODO(#329): compute and persist Iceberg table statistics (NDV/min/max
        // via Puffin sketches) so the cost-based optimizer benefits. For now the
        // statement is a no-op once the table is confirmed to exist.
        debug!(
            table = %table_name,
            "ANALYZE: table statistics collection is a no-op; statement accepted"
        );
        Ok(vec![])
    }

    // -----------------------------------------------------------------------
    // ATTACH / DETACH / CREATE SECRET / DROP SECRET / SHOW SECRETS handlers
    // -----------------------------------------------------------------------

    pub(super) async fn handle_attach(
        &self,
        session: &Session,
        stmt: &sqe_sql::AttachStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // `TYPE hive_external` is not an Iceberg catalog (see `build_catalog`'s
        // own comment), so it does not go through `runtime_catalogs.attach()`:
        // that builds an `iceberg::Catalog` and would fail with "dispatched
        // separately" for this kind. `hive_catalogs.insert` is the security
        // requirement, not bookkeeping: `resolve_policy_key` reads this set
        // live to decide whether the root's tables key under its own name or
        // the Iceberg namespace convention (see the type's own doc comment).
        if stmt.kind == sqe_sql::CatalogKind::HiveExternal {
            // REFUSE before touching `runtime_catalogs` at all: attaching the
            // root and only then discovering there is nowhere to register its
            // name would make the root readable while leaving
            // `resolve_policy_key` blind to it -- the exact escalation this
            // guard exists to prevent, just reached one step later. See the
            // `hive_catalogs` field's own doc comment for the full case.
            let Some(hive_catalogs) = self.hive_catalogs.as_ref() else {
                return Err(SqeError::Execution(
                    "hive_external: this coordinator was started without the shared Hive \
                     registries wired (QueryHandler::with_hive_registries was not called); \
                     that is a deployment wiring bug, not a mistake in this SQL, and ATTACH \
                     ... TYPE hive_external is refused rather than attaching a root the \
                     policy layer would not see"
                        .to_string(),
                ));
            };
            // Refuse a name collision with the session default catalog. See
            // `attach_configured_hive_roots`'s doc comment (`runtime_catalog.rs`)
            // for the full case: `resolve_policy_key` and
            // `CatalogRoutedTagSource` both dispatch on `table_ref.catalog()`
            // alone, which is `None` for a two-part reference, so a Hive root
            // sharing the default catalog's name would let a two-part
            // reference reach it while both the policy key and the tag route
            // take the Iceberg branch regardless.
            let default_catalog = self.config.resolve_default_catalog();
            if stmt.name == default_catalog {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!(
                        "hive_external: '{}' collides with the session default catalog '{default_catalog}': \
                     a two-part table reference under this default resolves to a Hive table \
                     without either the policy key or the tag route ever seeing its catalog \
                     name, so a root cannot share a name with the default catalog. ATTACH it \
                     under a different name, or change query.default_catalog",
                        stmt.name
                    ),
                ));
            }
            // This ATTACHING session's caller is baked into `build_hive_root`'s
            // `HiveManifestStore` below, and every LATER session that reads
            // through this root shares that same store (and thus this same
            // caller) for `check_path` on every manifest load -- see
            // `runtime_catalog.rs`'s "Hive lane" comment for the full case.
            // Fail-closed: it can only cause a false denial for a more-
            // privileged later reader, never let a less-privileged one read
            // more than the attacher could, since the gate never loosens
            // for anyone.
            let caller = sqe_core::config::TvfCaller::for_user(
                session.user.username.clone(),
                session.user.roles.clone(),
            );
            let limits = sqe_catalog::hive::manifest::ManifestLimits {
                max_bytes: self.config.hive.manifest_max_bytes,
                max_columns: self.config.hive.manifest_max_columns,
                max_partition_keys: self.config.hive.manifest_max_partition_keys,
                max_partition_index_entries: self.config.hive.manifest_max_partition_index_entries,
            };
            let store = sqe_catalog::mount::build_hive_root(
                &stmt.location,
                &stmt.options,
                &self.config.storage,
                // An ATTACHed root has no `data_prefixes` of its own: the
                // global `[hive] allowed_data_prefixes` applies (#886).
                sqe_core::config::HiveDataPolicy::from_config(
                    &self.config.hive,
                    &self.config.storage.tvf,
                ),
                limits,
                caller,
            )
            .await
            .map_err(SqeError::Execution)?;
            self.runtime_catalogs
                .attach_hive_root(
                    &stmt.name,
                    sqe_catalog::hive::source::HiveRoot::from_store(store),
                )
                .map_err(SqeError::Execution)?;
            hive_catalogs.insert(&stmt.name);
            crate::session_context::invalidate_all_session_caches().await;
            info!(catalog = %stmt.name, kind = "hive_external", "ATTACH complete");
            return Ok(vec![]);
        }

        self.runtime_catalogs
            .attach(stmt, &self.secrets, self.config.policy.inherit_schema_tags)
            .await
            .map_err(SqeError::Execution)?;
        crate::session_context::invalidate_all_session_caches().await;
        info!(catalog = %stmt.name, kind = %stmt.kind.name(), "ATTACH complete");
        Ok(vec![])
    }

    pub(super) async fn handle_detach(
        &self,
        stmt: &sqe_sql::DetachStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Mirror `handle_attach`'s branch: a name attached as a Hive root
        // lives in the registry's Hive lane, not the Iceberg one, and must
        // also leave `hive_catalogs` so a same-named Iceberg namespace
        // cannot inherit its policy key once the root is gone.
        if self
            .runtime_catalogs
            .is_hive_root(&stmt.name)
            .map_err(SqeError::Execution)?
        {
            self.runtime_catalogs
                .detach_hive_root(&stmt.name)
                .map_err(SqeError::Execution)?;
            // `handle_attach`'s guard means this root could only have been
            // attached while `hive_catalogs` was `Some`, so this is normally
            // populated. Not blocking on `None` regardless: DETACH only
            // removes access, never grants it, so there is no escalation to
            // guard against here the way there is on the attach side -- but
            // warn, since a `None` here on a root that exists at all means
            // the deployment was misconfigured for the whole time this root
            // was attached (see the field's doc comment).
            match self.hive_catalogs.as_ref() {
                Some(hive_catalogs) => hive_catalogs.remove(&stmt.name),
                None => warn!(
                    catalog = %stmt.name,
                    "hive_external: DETACH removed a root from the catalog registry, but \
                     this QueryHandler has no shared hive_catalogs to remove it from; the \
                     name may still be misclassified under the Iceberg policy-key convention \
                     for any table that shares its namespace -- this coordinator was started \
                     without with_hive_registries"
                ),
            }
            crate::session_context::invalidate_all_session_caches().await;
            info!(catalog = %stmt.name, "DETACH complete");
            return Ok(vec![]);
        }

        self.runtime_catalogs
            .detach(&stmt.name)
            .map_err(SqeError::Execution)?;
        crate::session_context::invalidate_all_session_caches().await;
        info!(catalog = %stmt.name, "DETACH complete");
        Ok(vec![])
    }

    pub(super) fn handle_create_secret(
        &self,
        stmt: &sqe_sql::CreateSecretStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let secret = sqe_sql::build_secret_from_stmt(stmt)?;
        self.secrets.create(&stmt.name, secret)?;
        info!(name = %stmt.name, kind = %stmt.kind.name(), "CREATE SECRET complete");
        Ok(vec![])
    }

    pub(super) fn handle_drop_secret(
        &self,
        stmt: &sqe_sql::DropSecretStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let in_use = self
            .runtime_catalogs
            .referenced_secrets(&stmt.name)
            .map_err(SqeError::Catalog)?;
        self.secrets.drop_secret(&stmt.name, &in_use)?;
        info!(name = %stmt.name, "DROP SECRET complete");
        Ok(vec![])
    }

    pub(super) fn handle_show_secrets(&self) -> sqe_core::Result<Vec<RecordBatch>> {
        let listed = self.secrets.list()?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("type", DataType::Utf8, false),
        ]));

        let mut name_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();
        for (name, type_name) in &listed {
            name_b.append_value(name);
            type_b.append_value(type_name);
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name_b.finish()) as ArrayRef,
                Arc::new(type_b.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build SHOW SECRETS result: {e}"),
            )
        })?;

        Ok(vec![batch])
    }
}
