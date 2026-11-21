//! Time travel and incremental reads: the `FOR SYSTEM_TIME AS OF`,
//! `FOR VERSION AS OF` and incremental table clauses are stripped from the
//! SQL, the referenced snapshots are pinned as aliased table providers, and
//! a [`TimeTravelCleanup`] guard deregisters them when the query finishes.

use super::*;

impl QueryHandler {
    /// Pre-process SQL for time travel: detect `FOR SYSTEM_TIME AS OF`, resolve
    /// timestamps to snapshot IDs, register snapshot-specific table providers,
    /// and return the rewritten SQL with the temporal clause stripped.
    ///
    /// Also handles `FOR VERSION AS OF <snapshot_id_or_ref>` via a pre-scan
    /// because sqlparser-rs 0.53 does not model that variant.
    ///
    /// Returns the rewritten SQL plus a [`TimeTravelCleanup`] guard whose
    /// Drop deregisters every alias that was registered for this query. The
    /// caller must hold the guard across query execution so the pinned
    /// provider doesn't leak into subsequent SQL in the same session (#44).
    ///
    /// When no time travel is found the original SQL is returned unchanged
    /// and the guard is empty.
    /// Resolve which catalog a (possibly catalog-qualified) time-travel table
    /// reference loads against, mirroring the SELECT/SHOW resolution (#2/#6):
    /// an explicit `catalog.schema.table` uses that catalog; otherwise the
    /// session's bound default catalog (discovered under polaris-auto). The
    /// time-travel path previously loaded against the primary/config catalog,
    /// so a polaris-auto session querying a discovered catalog failed with
    /// "table does not exist" (#317). Falls back to `fallback` only when the
    /// reference names no catalog and the session has no default.
    async fn resolve_reference_catalog(
        &self,
        session: &Session,
        table: &str,
        fallback: &Arc<SessionCatalog>,
    ) -> sqe_core::Result<Arc<SessionCatalog>> {
        let explicit = explicit_catalog_component(table);
        if explicit.is_none() && session.default_catalog.is_none() {
            return Ok(Arc::clone(fallback));
        }
        self.show_catalog(session, explicit).await
    }

    pub(super) async fn handle_time_travel(
        &self,
        sql: &str,
        ctx: &SessionContext,
        session: &Session,
        session_catalog: &Arc<SessionCatalog>,
        mut identities: Vec<(String, TableReference)>,
    ) -> sqe_core::Result<(String, TimeTravelCleanup)> {
        use sqlparser::ast::SetExpr;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let mut cleanup_aliases: Vec<String> = Vec::new();
        let prefetch_concurrency = self.config.storage.prefetch_concurrency;

        // First, pre-scan and resolve FOR VERSION AS OF. This path hides the
        // clause from sqlparser and registers snapshot-pinned providers
        // under aliases in the writable `datafusion.public` schema. The
        // SQL is rewritten so the original table reference (which lives
        // in the read-only Iceberg schema provider) is replaced with the
        // alias.
        let (mut rewritten_for_version, version_specs) = sqe_sql::extract_time_travel_spec(sql)?;
        let mut version_resolved = !version_specs.is_empty();
        if version_resolved {
            for spec in &version_specs {
                // Resolve the catalog the table lives in, like the SELECT/SHOW
                // paths do (#2/#6). A time-travel reference loads against the
                // session's bound catalog -- or, for a catalog-qualified name,
                // that catalog -- not the primary/config catalog. Using the
                // primary made polaris-auto sessions fail with "table does not
                // exist" (#317).
                let resolved_catalog = self
                    .resolve_reference_catalog(session, &spec.table, session_catalog)
                    .await?;
                let alias =
                    Self::apply_version_spec(spec, ctx, &resolved_catalog, prefetch_concurrency)
                        .await?;
                cleanup_aliases.push(alias.clone());
                identities.push((alias.clone(), TableReference::from(spec.table.as_str())));
                rewritten_for_version =
                    replace_table_reference(&rewritten_for_version, &spec.table, &alias);
            }
        }
        let sql_for_ast = if version_resolved {
            rewritten_for_version
        } else {
            sql.to_string()
        };

        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, &sql_for_ast).map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Parse error in time travel detection: {e}"),
            )
        })?;

        if statements.is_empty() {
            return Ok((
                sql_for_ast,
                TimeTravelCleanup::new(ctx, cleanup_aliases).with_identities(identities),
            ));
        }

        let stmt = &mut statements[0];
        let mut found_time_travel = false;
        let cleanup = &mut cleanup_aliases;

        if let sqlparser::ast::Statement::Query(ref mut query) = stmt {
            if let SetExpr::Select(ref mut select) = *query.body {
                for twj in &mut select.from {
                    if self
                        .process_time_travel_table_factor(
                            &mut twj.relation,
                            ctx,
                            session,
                            session_catalog,
                            cleanup,
                            &mut identities,
                            prefetch_concurrency,
                        )
                        .await?
                    {
                        found_time_travel = true;
                    }
                    for join in &mut twj.joins {
                        if self
                            .process_time_travel_table_factor(
                                &mut join.relation,
                                ctx,
                                session,
                                session_catalog,
                                cleanup,
                                &mut identities,
                                prefetch_concurrency,
                            )
                            .await?
                        {
                            found_time_travel = true;
                        }
                    }
                }
            }
        }

        let guard = TimeTravelCleanup::new(ctx, cleanup_aliases).with_identities(identities);
        if found_time_travel {
            Ok((statements[0].to_string(), guard))
        } else if version_resolved {
            Ok((sql_for_ast, guard))
        } else {
            version_resolved = false;
            let _ = version_resolved; // silence unused warning if flow changes
            Ok((sql.to_string(), guard))
        }
    }

    /// Resolve a parsed `TimeTravelSpec` (from `FOR VERSION AS OF`) against
    /// table metadata and register a snapshot-pinned provider.
    ///
    /// - `VersionRef::SnapshotId(id)` pins to that specific snapshot.
    /// - `VersionRef::Named(name)` looks up `name` in the table's refs. If both
    ///   a tag and a branch exist with the same name, the tag wins and a
    ///   warning is logged.
    async fn apply_version_spec(
        spec: &sqe_sql::TimeTravelSpec,
        ctx: &SessionContext,
        session_catalog: &Arc<SessionCatalog>,
        prefetch_concurrency: usize,
    ) -> sqe_core::Result<String> {
        let parts: Vec<&str> = spec.table.split('.').collect();
        let (namespace, bare_table) = match parts.len() {
            1 => ("default", parts[0]),
            2 => (parts[0], parts[1]),
            3 => (parts[1], parts[2]),
            _ => {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!(
                        "Unsupported table name format for time travel: {}",
                        spec.table
                    ),
                ));
            }
        };

        let ns_ident = iceberg::NamespaceIdent::new(namespace.to_string());
        let table_ident = iceberg::TableIdent::new(ns_ident, bare_table.to_string());
        let iceberg_table = session_catalog.load_table(&table_ident).await?;
        let snapshot_id = sqe_catalog::time_travel::resolve_version_snapshot(
            iceberg_table.metadata(),
            &spec.version,
            &spec.table,
        )?;

        tracing::info!(
            table = %spec.table,
            snapshot_id,
            "Time travel (FOR VERSION AS OF): pinned snapshot"
        );

        let provider = sqe_catalog::table_provider::SqeTableProvider::try_new(iceberg_table)
            .await?
            .with_snapshot_id(snapshot_id)
            .with_prefetch_concurrency(prefetch_concurrency);

        let alias = format!("__sqe_ver_{}_{}", bare_table, uuid::Uuid::now_v7().simple());
        let qualified = format!("datafusion.public.{alias}");
        ctx.register_table(qualified.as_str(), Arc::new(provider))
            .map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to register time-travel provider for {bare_table}: {e}"),
                )
            })?;

        Ok(qualified)
    }

    /// Process a single `TableFactor` for `FOR SYSTEM_TIME AS OF`.
    ///
    /// When a time travel clause is found:
    /// 1. Resolves the timestamp to a snapshot ID
    /// 2. Loads the Iceberg table and creates a snapshot-pinned provider
    /// 3. Registers it under a unique alias `datafusion.public.__sqe_tt_<table>_<uuid>`
    /// 4. Rewrites the `TableFactor` name to point at the alias and strips
    ///    the `version` field so DataFusion doesn't see it
    ///
    /// Returning the qualified alias under `datafusion.public` (#44):
    /// - Earlier versions registered under the bare table name, leaking the
    ///   pinned provider into the session for subsequent queries (silent
    ///   stale reads).
    /// - Qualified names also avoid collisions between two namespaces that
    ///   happen to share a bare table name (`ns_a.t` vs `ns_b.t`).
    ///
    /// The alias is pushed into `cleanup` so the caller can deregister it
    /// once query execution completes.
    #[allow(clippy::too_many_arguments)]
    async fn process_time_travel_table_factor(
        &self,
        relation: &mut TableFactor,
        ctx: &SessionContext,
        session: &Session,
        session_catalog: &Arc<SessionCatalog>,
        cleanup: &mut Vec<String>,
        identities: &mut Vec<(String, TableReference)>,
        prefetch_concurrency: usize,
    ) -> sqe_core::Result<bool> {
        use sqlparser::ast::TableVersion;

        if let TableFactor::Table {
            ref mut name,
            ref mut version,
            ..
        } = relation
        {
            if let Some(TableVersion::ForSystemTimeAsOf(ref expr)) = version {
                let table_name = name.to_string();
                let timestamp_ms = resolve_timestamp_expr(expr)?;

                // Resolve the catalog the same way the SELECT/SHOW paths do, so
                // a polaris-auto session loads from its bound catalog rather
                // than the primary/config one (#317).
                let session_catalog = &self
                    .resolve_reference_catalog(session, &table_name, session_catalog)
                    .await?;

                // Extract namespace and table from the (possibly qualified) name
                let parts: Vec<&str> = table_name.split('.').collect();
                let (namespace, bare_table) = match parts.len() {
                    1 => ("default", parts[0]),
                    2 => (parts[0], parts[1]),
                    3 => (parts[1], parts[2]), // catalog.schema.table — skip catalog
                    _ => {
                        return Err(SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!("Unsupported table name format for time travel: {table_name}"),
                        ));
                    }
                };

                let ns_ident = iceberg::NamespaceIdent::new(namespace.to_string());
                let table_ident = iceberg::TableIdent::new(ns_ident, bare_table.to_string());
                let iceberg_table = session_catalog.load_table(&table_ident).await?;

                let snapshot_id =
                    find_snapshot_at_timestamp(iceberg_table.metadata(), timestamp_ms)?;

                tracing::info!(
                    table = %table_name,
                    timestamp_ms,
                    snapshot_id,
                    "Time travel: resolved timestamp to snapshot"
                );

                // Build a snapshot-pinned SqeTableProvider and register it
                // under a unique alias under datafusion.public.
                let provider =
                    sqe_catalog::table_provider::SqeTableProvider::try_new(iceberg_table)
                        .await?
                        .with_snapshot_id(snapshot_id)
                        .with_prefetch_concurrency(prefetch_concurrency);

                let alias = format!("__sqe_tt_{}_{}", bare_table, uuid::Uuid::now_v7().simple());
                let qualified = format!("datafusion.public.{alias}");
                ctx.register_table(qualified.as_str(), Arc::new(provider))
                    .map_err(|e| {
                        SqeError::with_code(
                            SqeErrorCode::ExecutionFailed,
                            format!(
                                "Failed to register time-travel provider for {bare_table}: {e}"
                            ),
                        )
                    })?;
                cleanup.push(qualified.clone());
                identities.push((qualified.clone(), TableReference::from(table_name.as_str())));

                // Rewrite the AST to point at the qualified alias so the
                // planner resolves it under the writable `datafusion.public`
                // schema. Splitting on `.` reconstructs the ObjectName as
                // three Idents (catalog / schema / table).
                *name = sqlparser::ast::ObjectName::from(
                    qualified
                        .split('.')
                        .map(|part| sqlparser::ast::Ident::new(part.to_string()))
                        .collect::<Vec<_>>(),
                );

                // Strip the temporal clause so DataFusion doesn't reject it
                *version = None;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Pre-process SQL for CDC incremental scans. Detect
    /// `FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y`, resolve the range
    /// against table metadata, plan the file list with delete reconciliation,
    /// and register a transient `IncrementalTableProvider` for each target
    /// table. Returns the SQL with the clause stripped and each target table
    /// reference rewritten to a qualified `datafusion.public.<alias>` name so
    /// the planner resolves to the freshly registered provider.
    ///
    /// When no incremental clause is found, the original SQL is returned
    /// unchanged.
    pub(super) async fn handle_incremental(
        &self,
        sql: &str,
        ctx: &SessionContext,
    ) -> sqe_core::Result<(String, Vec<(String, TableReference)>)> {
        let (mut rewritten, specs) = sqe_sql::extract_incremental_spec(sql)?;
        if specs.is_empty() {
            return Ok((sql.to_string(), Vec::new()));
        }
        let mut identities = Vec::with_capacity(specs.len());
        for spec in &specs {
            let alias = Self::apply_incremental_spec(spec, ctx).await?;
            // The alias hides the table from the policy rewriter; remember
            // the real reference so the plan can be given it back (400).
            identities.push((alias.clone(), TableReference::from(spec.table.as_str())));
            // Rewrite the table reference (e.g. `ns.events`) to the qualified
            // MemoryCatalog path (`datafusion.public.<alias>`). The replacement
            // is whole-token so `events.id` inside a SELECT list is preserved
            // because it doesn't match the qualified form.
            rewritten = replace_table_reference(&rewritten, &spec.table, &alias);
        }
        tracing::info!(
            specs = specs.len(),
            "Handled FOR INCREMENTAL BETWEEN SNAPSHOT clauses"
        );
        Ok((rewritten, identities))
    }

    /// Resolve one `IncrementalSpec` against the catalog, register an
    /// `IncrementalTableProvider` in the in-memory `datafusion.public` schema,
    /// and return the fully qualified name the caller should substitute into
    /// the SQL.
    async fn apply_incremental_spec(
        spec: &sqe_sql::IncrementalSpec,
        ctx: &SessionContext,
    ) -> sqe_core::Result<String> {
        use iceberg::arrow::schema_to_arrow_schema;
        use std::collections::HashMap;

        // Resolve the table the way a plain read of the same name does, so
        // a catalog-qualified name reads that catalog's table (#845).
        let reference = TableReference::from(spec.table.as_str());
        let bare_table = reference.table().to_string();
        let planned = ctx.table_provider(reference).await?;
        let iceberg_table = (planned.as_ref() as &dyn std::any::Any)
            .downcast_ref::<sqe_catalog::table_provider::SqeTableProvider>()
            .ok_or_else(|| {
                SqeError::with_code(
                    SqeErrorCode::NotSupported,
                    format!(
                        "FOR INCREMENTAL BETWEEN SNAPSHOT reads Iceberg tables only; {} is not one",
                        spec.table
                    ),
                )
            })?
            .table()
            .clone();

        // Plan the range. `plan_incremental` also calls `resolve_range`
        // internally, so range validation happens once.
        let mut plan =
            sqe_catalog::incremental_scan::plan_incremental(&iceberg_table, spec.start, spec.end)
                .await?;

        // Reconcile in-range deletes. Without a referenced-data-file map
        // (position deletes currently do not expose one through
        // `plan_incremental`), we default every delete file to "no reference"
        // which keeps equality deletes intact. This is the same defensive
        // default the helper uses in its unit tests.
        let empty_refs: HashMap<String, Option<String>> = std::collections::HashMap::new();
        let kept_deletes = sqe_catalog::incremental_scan::reconcile_in_range_deletes(
            &plan.data_files,
            std::mem::take(&mut plan.delete_files),
            &empty_refs,
        );
        plan.delete_files = kept_deletes;

        tracing::info!(
            table = %spec.table,
            start = spec.start,
            end = spec.end,
            snapshots = plan.snapshots_in_range.len(),
            data_files = plan.data_files.len(),
            delete_files = plan.delete_files.len(),
            "Incremental scan: planned range"
        );

        let base_schema = schema_to_arrow_schema(iceberg_table.metadata().current_schema())
            .map_err(|e| {
                SqeError::Catalog(format!(
                    "Failed to convert Iceberg schema to Arrow for {}: {e}",
                    spec.table
                ))
            })?;

        let file_io = iceberg_table.file_io().clone();
        let provider = sqe_catalog::incremental_provider::IncrementalTableProvider::new(
            Arc::new(base_schema),
            plan,
            Some(file_io),
        )
        // The policy rewriter reads `sqe.column-tags` off the scan; without
        // these it fell back to the metadata cache and refused the read on
        // a miss (#844).
        .with_table_properties(
            iceberg_table.metadata().properties().clone(),
            iceberg_table.metadata().current_schema().clone(),
        );

        // Register under a unique alias in the `datafusion.public` schema,
        // which is a MemoryCatalogProvider and supports dynamic registration.
        // The alias embeds a short uuid to avoid collisions when the same
        // table is referenced multiple times in one statement.
        let alias = format!(
            "__sqe_incr_{}_{}",
            bare_table,
            uuid::Uuid::now_v7().simple()
        );
        let qualified = format!("datafusion.public.{alias}");
        ctx.register_table(qualified.as_str(), Arc::new(provider))
            .map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to register incremental provider for {bare_table}: {e}"),
                )
            })?;

        Ok(qualified)
    }
}

/// RAII guard that deregisters time-travel pinned providers when the query
/// completes (#44). The pinned provider must not survive the query that
/// asked for it; otherwise later SQL in the same session that references
/// the bare table name resolves to the pinned snapshot instead of HEAD,
/// silently serving stale data.
///
/// Each entry is the fully-qualified `datafusion.public.<alias>` path used
/// at registration, matching what `ctx.deregister_table` expects.
pub struct TimeTravelCleanup {
    ctx: SessionContext,
    qualified_aliases: Vec<String>,
    /// `qualified alias -> the table reference the user wrote`, for every
    /// pinned provider registered under `datafusion.public`. Consumed by
    /// [`Self::restore_identities`].
    identities: Vec<(String, TableReference)>,
}

impl TimeTravelCleanup {
    pub(super) fn new(ctx: &SessionContext, qualified_aliases: Vec<String>) -> Self {
        Self {
            ctx: ctx.clone(),
            qualified_aliases,
            identities: Vec::new(),
        }
    }

    pub(super) fn with_identities(mut self, identities: Vec<(String, TableReference)>) -> Self {
        self.identities = identities;
        self
    }

    /// Give every pinned scan its real table identity back before policy
    /// enforcement (architecture review 400).
    ///
    /// Time-travel, version and incremental reads register their pinned
    /// provider under `datafusion.public.__sqe_*` and rewrite the SQL to that
    /// alias, so the planner's `TableScan` carried the alias. The policy
    /// rewriter keys row filters, masks and tag lookups on the scan's
    /// `TableReference`, found no policy for the alias, and treated the
    /// `datafusion` catalog as reserved (known-no-tags): every fine-grained
    /// control was inert on these paths, and the audit named the alias.
    ///
    /// The scan is renamed to the reference the user wrote and wrapped in a
    /// `SubqueryAlias` carrying the old alias, so column qualifiers above it
    /// stay valid while the rewriter, audit and lineage see the real table.
    pub(crate) fn restore_identities(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
    ) -> sqe_core::Result<datafusion::logical_expr::LogicalPlan> {
        use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
        use datafusion::logical_expr::{LogicalPlan, SubqueryAlias, TableScanBuilder};

        if self.identities.is_empty() {
            return Ok(plan);
        }
        let identities: std::collections::HashMap<&str, &TableReference> = self
            .identities
            .iter()
            .map(|(alias, real)| (alias.as_str(), real))
            .collect();
        plan.transform_down_with_subqueries(|node| {
            let LogicalPlan::TableScan(scan) = &node else {
                return Ok(Transformed::no(node));
            };
            let alias = scan.table_name.to_string();
            let Some(real) = identities.get(alias.as_str()) else {
                return Ok(Transformed::no(node));
            };
            let renamed = TableScanBuilder::new((*real).clone(), Arc::clone(&scan.source))
                .with_projection(scan.projection.clone())
                .with_filters(scan.filters.clone())
                .with_fetch(scan.fetch)
                .build()?;
            let aliased = SubqueryAlias::try_new(
                Arc::new(LogicalPlan::TableScan(renamed)),
                scan.table_name.clone(),
            )?;
            Ok(Transformed::new(
                LogicalPlan::SubqueryAlias(aliased),
                true,
                TreeNodeRecursion::Jump,
            ))
        })
        .map(|t| t.data)
        .map_err(|e| {
            SqeError::Execution(format!("restoring time-travel table identity failed: {e}"))
        })
    }
}

impl Drop for TimeTravelCleanup {
    fn drop(&mut self) {
        for name in &self.qualified_aliases {
            if let Err(e) = self.ctx.deregister_table(name.as_str()) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "time-travel pinned provider deregister failed"
                );
            }
        }
    }
}
