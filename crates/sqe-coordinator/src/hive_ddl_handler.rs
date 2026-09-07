//! Server-mode Hive DDL: `CREATE EXTERNAL TABLE`, `DROP TABLE`, and
//! `SHOW CREATE TABLE` against an attached Hive root, plus the single
//! branch point (`resolve_table_kind`) that decides whether an arbitrary
//! table reference names a Hive table or an Iceberg one.
//!
//! Mirrors `crates/sqe-cli/src/embedded.rs`'s `embedded_handle_create_external_table`
//! / `embedded_handle_drop_hive_table` / `embedded_handle_show_create_hive_table`,
//! but resolves against the shared, process-wide `RuntimeCatalogRegistry`
//! (session-independent `HiveManifestStore` per root) instead of a
//! single-process `HashMap`, and authenticates with the caller's real
//! session identity instead of `TvfCaller::trusted_local()`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::TableReference;
use sqe_catalog::hive::manifest::HiveManifest;
use sqe_catalog::hive::partition::parse_projection;
use sqe_catalog::hive::provider::HiveMetadataCache;
use sqe_core::{SecretStore, Session, SessionUser, SqeError, SqeErrorCode};
use sqe_metrics::audit::{
    Actor, AuditEvent, AuditKind, AuditLogger, ObjectType, Outcome, Resource,
};
use sqe_policy::plan_rewriter::{policy_denies_all, resolve_effective_policy, HiveCatalogs};
use sqe_policy::PolicyStore;
use sqe_sql::hive_ddl::HiveTableSpec;

use crate::runtime_catalog::RuntimeCatalogRegistry;

/// Which backend a resolved table reference names. `resolve_table_kind` is
/// the only place this is decided; every dispatch site that needs to tell
/// a Hive table apart from an Iceberg one calls it rather than growing its
/// own detection (a second detector is how the two backends' table-name
/// conventions drift apart, which is exactly the escalation Task 7 closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKind {
    Iceberg,
    Hive {
        root: String,
        database: String,
        table: String,
    },
}

/// Server-mode Hive DDL handler. Holds a live clone of the process-wide
/// `RuntimeCatalogRegistry` (not a snapshot: `Arc<RwLock<..>>` underneath,
/// so an `ATTACH` issued mid-session is visible on this handler's very next
/// call) plus the two things every write path needs to check before it
/// touches a manifest: the security registry `resolve_policy_key` reads,
/// and a secret store to validate `TBLPROPERTIES ('sqe.secret' = ...)`
/// references at DDL time instead of failing later at first scan.
#[derive(Clone)]
pub struct HiveDdlHandler {
    runtime_catalogs: RuntimeCatalogRegistry,
    /// `None` means this `QueryHandler` was never wired via
    /// `with_hive_registries` (a deployment bug, not a SQL error -- see
    /// `QueryHandler::hive_catalogs`'s own doc comment). Every write path
    /// here (`create_external_table`, `drop_table`) refuses on `None`
    /// rather than proceeding: proceeding would write or delete a manifest
    /// in a root the policy layer cannot see, letting a same-named Iceberg
    /// grant (wrongly) govern it, or letting a drop go unaudited by the
    /// convention `resolve_policy_key` actually uses.
    hive_catalogs: Option<Arc<HiveCatalogs>>,
    secrets: SecretStore,
    audit: Option<Arc<AuditLogger>>,
    /// `None` means policy enforcement is disabled cluster-wide (no `[policy]`
    /// backend configured), the same condition under which `QueryHandler`
    /// runs `PassthroughEnforcer` on the Iceberg read path. Mirrors that:
    /// `None` here means "no gate", not "deny everything" -- `resolve_policy`
    /// returns the default (unrestricted) `ResolvedPolicy` rather than
    /// refusing, or a cluster with policy off would lose Hive metadata
    /// listing while Iceberg metadata listing kept working.
    policy_store: Option<Arc<dyn PolicyStore>>,
    mask_precedence: sqe_core::config::MaskPrecedence,
    manifest_cache: Option<Arc<HiveMetadataCache>>,
}

/// The wiring-gap refusal shared by every write path below. Named the same
/// way `handle_attach`'s does, so grepping for "with_hive_registries" finds
/// every place a deployment can hit this.
fn hive_registries_not_wired() -> SqeError {
    SqeError::Execution(
        "hive_external: this coordinator was started without the shared Hive registries \
         wired (QueryHandler::with_hive_registries was not called); that is a deployment \
         wiring bug, not a mistake in this SQL"
            .to_string(),
    )
}

/// Whether `err` is `drop_table`'s "table not found" error, for the caller's
/// `DROP TABLE IF EXISTS` handling. A named, grep-able equivalent of
/// `catalog_ops::is_table_not_found`'s role on the Iceberg drop path, not a
/// fix for the string-substring coupling either one has against its own
/// backend's error text: `drop_table` returns a plain `SqeError`, not a
/// typed `HiveError`, by the time it reaches the caller (`store.delete`'s
/// `Err` is mapped to `SqeError::Execution` inside `drop_table` itself), so
/// there is nothing more structured than the message to match on without a
/// wider change to how `drop_table` reports failure.
pub(crate) fn hive_drop_target_missing(err: &SqeError) -> bool {
    err.to_string().contains("table not found")
}

impl HiveDdlHandler {
    pub fn new(runtime_catalogs: RuntimeCatalogRegistry, secrets: SecretStore) -> Self {
        Self {
            runtime_catalogs,
            hive_catalogs: None,
            secrets,
            audit: None,
            policy_store: None,
            mask_precedence: sqe_core::config::MaskPrecedence::default(),
            manifest_cache: None,
        }
    }

    /// Wire the SAME `PolicyStore` and `mask_precedence` `QueryHandler` backs
    /// its Iceberg read path with, so `SHOW TABLES`/`SHOW CREATE TABLE`
    /// against a Hive root are gated by the identical policy, not a second,
    /// independently-derived one. Called from `QueryHandler::new`.
    #[must_use = "with_policy consumes self; bind the returned HiveDdlHandler"]
    pub fn with_policy(
        mut self,
        policy_store: Option<Arc<dyn PolicyStore>>,
        mask_precedence: sqe_core::config::MaskPrecedence,
    ) -> Self {
        self.policy_store = policy_store;
        self.mask_precedence = mask_precedence;
        self
    }

    /// Wire the process-wide `hive_catalogs` registry and audit sink. Called
    /// from `QueryHandler::with_hive_registries` with the same `Arc`s the
    /// policy layer was built from.
    #[must_use = "with_hive_registries consumes self; bind the returned HiveDdlHandler"]
    pub fn with_hive_registries(mut self, hive_catalogs: Arc<HiveCatalogs>) -> Self {
        self.hive_catalogs = Some(hive_catalogs);
        self
    }

    #[must_use = "with_manifest_cache consumes self; bind the returned HiveDdlHandler"]
    pub fn with_manifest_cache(mut self, cache: Arc<HiveMetadataCache>) -> Self {
        self.manifest_cache = Some(cache);
        self
    }

    fn invalidate_manifest(&self, database: &str, table: &str) {
        if let Some(cache) = &self.manifest_cache {
            cache.invalidate(database, table);
        }
    }

    #[must_use = "with_audit consumes self; bind the returned HiveDdlHandler"]
    pub fn with_audit(mut self, audit: Arc<AuditLogger>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// The single branch point: does `table_ref` name a table in an
    /// attached Hive root? Only a fully-qualified (`catalog.schema.table`)
    /// reference can, since a Hive table is always addressed by its
    /// attached root's name; a bare or partial reference (no catalog part)
    /// falls to `TableKind::Iceberg` here.
    ///
    /// This only looks at `table_ref` itself. A bare or partial reference
    /// under a session whose DEFAULT catalog is a Hive root still needs a
    /// Hive answer once that default is filled in -- `resolve_table_kind_for_session`
    /// below does that promotion first and is what every dispatch-site
    /// rejection check (`reject_if_hive_table`) actually calls; this method
    /// is for the one caller that already has a `Full` reference in hand
    /// (`CREATE EXTERNAL TABLE`/`DROP TABLE`/`SHOW CREATE TABLE`/`SHOW
    /// TABLES`, all of which require one).
    pub fn resolve_table_kind(&self, table_ref: &TableReference) -> TableKind {
        if let TableReference::Full {
            catalog,
            schema,
            table,
        } = table_ref
        {
            if matches!(self.runtime_catalogs.is_hive_root(catalog), Ok(true)) {
                return TableKind::Hive {
                    root: catalog.to_string(),
                    database: schema.to_string(),
                    table: table.to_string(),
                };
            }
        }
        TableKind::Iceberg
    }

    /// Same branch point as `resolve_table_kind`, but promotes a `Bare`/
    /// `Partial` reference to `Full` using the session's default catalog
    /// first, so a Hive root set as the session default is not missed.
    ///
    /// `resolve_table_kind` alone erred safely only by accident: a write
    /// statement (`DELETE`/`UPDATE`/`ANALYZE`/...) against an unqualified
    /// reference under a Hive-root default catalog does not always fail
    /// downstream. Under `polaris-auto` catalog discovery it does (the
    /// Hive root name does not resolve as an Iceberg catalog, so the
    /// session falls back and the Iceberg write path errors as
    /// "unknown catalog"), but under `static` discovery
    /// (`session_context.rs`'s `create_session_context`) the session
    /// silently falls back to `config.resolve_default_catalog()` instead,
    /// which could be a real, different Iceberg catalog -- letting a
    /// `DELETE FROM sales.orders` meant for a Hive table silently target a
    /// same-named table in the wrong catalog instead of being rejected.
    /// This closes that gap by resolving the SAME way before the
    /// Hive-or-Iceberg question is even asked.
    ///
    /// Only the catalog component matters for the answer this returns
    /// (`is_hive_root` inspects nothing else), so a `Bare` reference is
    /// promoted with a placeholder `"default"` schema -- consistent with
    /// `ident::parse_table_ref`'s own fallback for a one-part name --
    /// rather than threading the session's default schema through for a
    /// value this method never uses.
    pub fn resolve_table_kind_for_session(
        &self,
        default_catalog: Option<&str>,
        table_ref: &TableReference,
    ) -> TableKind {
        if matches!(table_ref, TableReference::Full { .. }) {
            return self.resolve_table_kind(table_ref);
        }
        let Some(catalog) = default_catalog else {
            return TableKind::Iceberg;
        };
        let (schema, table) = match table_ref {
            TableReference::Partial { schema, table } => (schema.to_string(), table.to_string()),
            TableReference::Bare { table } => ("default".to_string(), table.to_string()),
            TableReference::Full { .. } => unreachable!("handled above"),
        };
        self.resolve_table_kind(&TableReference::full(catalog.to_string(), schema, table))
    }

    /// Build the standard rejection for a statement this release does not
    /// support against a Hive external table. `cycle` is the release that
    /// adds support; `0` is the sentinel for "no planned cycle" and omits
    /// the clause naming one, rather than rendering the nonsensical "cycle
    /// 0 adds it". `statement` is a short label (e.g. `"ANALYZE"`), never
    /// the raw SQL text: interpolating user SQL into an error message is
    /// how an unrelated failure (e.g. a `HiveError` that echoes its input)
    /// ends up looking like this rejection by accident.
    pub fn reject_unsupported(&self, statement: &str, cycle: u8) -> SqeError {
        if cycle == 0 {
            SqeError::NotImplemented(format!(
                "hive_external: {statement} is not supported for Hive external tables"
            ))
        } else {
            SqeError::NotImplemented(format!(
                "hive_external: {statement} is not supported for Hive external tables in this \
                 release; cycle {cycle} adds it"
            ))
        }
    }

    /// `CREATE EXTERNAL TABLE`: write `spec`'s sidecar manifest into the
    /// resolved Hive root's warehouse. Mirrors
    /// `embedded_handle_create_external_table`, but resolves the root
    /// through `runtime_catalogs.hive_roots()` and authenticates with the
    /// caller's real session identity.
    pub async fn create_external_table(
        &self,
        session: &Session,
        spec: &HiveTableSpec,
    ) -> Result<(), SqeError> {
        self.hive_catalogs
            .as_ref()
            .ok_or_else(hive_registries_not_wired)?;

        let catalog_name = spec.catalog.as_deref().ok_or_else(|| {
            SqeError::Execution(
                "CREATE EXTERNAL TABLE needs a catalog-qualified name (e.g. \
                 `lake.sales.orders`); there is no \"current\" Hive root to fall back to"
                    .to_string(),
            )
        })?;
        let store = self.store_for(catalog_name)?;

        // "default", matching the same fallback `embedded_handle_create_external_table`
        // uses when the statement did not give a 3-part name.
        let database = spec
            .database
            .clone()
            .unwrap_or_else(|| "default".to_string());

        // Fail fast on a typo'd `TBLPROPERTIES ('sqe.secret' = ...)` reference
        // at DDL time, rather than letting it surface later as an opaque scan
        // failure the first time someone SELECTs from the table.
        if let Some(name) = sqe_sql::hive_ddl::secret_name(spec) {
            self.secrets.get(name).map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "hive_external: TBLPROPERTIES 'sqe.secret' names '{name}', which is not a \
                     known secret: {e}"
                ))
            })?;
        }

        let created_at = chrono::Utc::now().to_rfc3339();
        let manifest =
            HiveManifest::from_spec(spec, &database, &session.user.username, &created_at);
        parse_projection(&manifest.parameters, &manifest.partition_keys)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        store
            .put(&manifest, spec.if_not_exists)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        self.invalidate_manifest(&database, &spec.name);
        self.emit_audit_event(
            session,
            catalog_name,
            &database,
            &spec.name,
            "CREATE EXTERNAL TABLE",
        );
        Ok(())
    }

    /// `DROP TABLE` against a Hive root: delete the manifest, never the
    /// data. Callers decide whether `table_ref` even names a Hive table
    /// (via `resolve_table_kind`) before calling this; `if_exists` handling
    /// is the caller's job too, by matching the returned error via
    /// `hive_drop_target_missing` below, the same way the Iceberg drop path
    /// matches `catalog_ops::is_table_not_found`.
    pub async fn drop_table(
        &self,
        session: &Session,
        table_ref: &TableReference,
        purge: bool,
    ) -> Result<(), SqeError> {
        self.hive_catalogs
            .as_ref()
            .ok_or_else(hive_registries_not_wired)?;

        let TableReference::Full {
            catalog,
            schema,
            table,
        } = table_ref
        else {
            return Err(SqeError::Execution(
                "hive_external: DROP TABLE needs a catalog-qualified name (e.g. \
                 `lake.sales.orders`)"
                    .to_string(),
            ));
        };

        if purge {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: DROP TABLE ... PURGE is not supported for Hive external tables \
                 ({schema}.{table}): PURGE deletes the underlying data, which an external \
                 table's DROP never does"
            )));
        }

        let store = self.store_for(catalog)?;
        store
            .delete(schema, table)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        self.invalidate_manifest(schema, table);
        self.emit_audit_event(session, catalog, schema, table, "DROP TABLE");
        Ok(())
    }

    /// `SHOW CREATE TABLE` against a Hive root: render Athena/Glue-style
    /// DDL from the manifest, through the same shared renderer the
    /// embedded client uses (`sqe_catalog::hive::render_hive_create_table_ddl`).
    ///
    /// Gated: a caller with no privilege on the table gets the SAME "table
    /// not found" wording an absent table would, so a denial cannot be told
    /// apart from a table that never existed (the project's no-information-
    /// leakage convention; see `resolve_policy`'s doc comment). A caller with
    /// partial access sees the DDL with restricted columns silently dropped
    /// from the column list, mirroring how `information_schema.columns`
    /// already treats a restricted Iceberg column as invisible rather than
    /// erroring. Column masks are NOT reflected here (masking is a data-time
    /// rewrite, not a schema change; the Iceberg `SHOW CREATE TABLE` path
    /// does not filter for masks either, only `information_schema.columns`
    /// itself would, and that is a value question, not a name/type one).
    pub async fn show_create_table(
        &self,
        user: &SessionUser,
        table_ref: &TableReference,
    ) -> Result<String, SqeError> {
        let TableReference::Full {
            catalog,
            schema,
            table,
        } = table_ref
        else {
            return Err(SqeError::Execution(
                "hive_external: SHOW CREATE TABLE needs a catalog-qualified name (e.g. \
                 `lake.sales.orders`)"
                    .to_string(),
            ));
        };
        let store = self.source_for(catalog)?;
        let manifest = store
            .get(schema, table)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        let policy = self.resolve_policy(user, schema, table, &manifest).await;
        if policy_denies_all(&policy) {
            // Identical wording to `HiveManifestStore::get`'s own not-found
            // error (see `HiveError::NotFound`'s `Display`): a caller with
            // zero privilege must not be able to distinguish "denied" from
            // "never existed" by the error text alone.
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: table not found: {schema}.{table}"
            )));
        }

        let mut spec = sqe_catalog::hive::hive_spec_from_manifest(catalog, &manifest);
        spec.columns
            .retain(|c| !policy.restricted_columns.contains(&c.name));
        spec.partition_keys
            .retain(|c| !policy.restricted_columns.contains(&c.name));
        Ok(sqe_catalog::hive::render_hive_create_table_ddl(&spec))
    }

    /// `SHOW TABLES FROM <root>.<database>`: list tables via the manifest
    /// store, in the same Trino-compatible bare-name shape
    /// `handle_show_tables` already uses for Iceberg, filtered to the tables
    /// `user` has any privilege on at all (a fully-denied table is omitted,
    /// never erred on -- the same "invisible, not an error" convention
    /// `show_create_table` above and `information_schema.columns` both
    /// follow for restricted columns, applied here at table granularity).
    ///
    /// Unlike the Iceberg `SHOW TABLES` path (which is filtered for free by
    /// Polaris authorizing `list_tables` against the caller's own OIDC
    /// token), a Hive root has no external metastore to delegate to: the
    /// manifest store is read with the coordinator's own object-store
    /// credentials, the same ones for every caller. This gate is the only
    /// thing standing between "engine can read it" and "user can see it",
    /// so it fetches and resolves a policy for every candidate table rather
    /// than trusting the listing to already be scoped. That is an N+1
    /// manifest read per `SHOW TABLES` call; acceptable for a first cut,
    /// worth caching if a Hive root's table count grows large enough to
    /// notice.
    pub async fn list_tables(
        &self,
        user: &SessionUser,
        root: &str,
        database: &str,
    ) -> Result<Vec<String>, SqeError> {
        let store = self.source_for(root)?;
        let names = store
            .list_tables(database)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        let mut visible = Vec::with_capacity(names.len());
        for name in names {
            let manifest = match store.get(database, &name).await {
                Ok(m) => m,
                // A table that vanished between `list_tables` and `get`
                // (dropped concurrently) is omitted rather than erroring the
                // whole listing.
                Err(_) => continue,
            };
            let policy = self.resolve_policy(user, database, &name, &manifest).await;
            if !policy_denies_all(&policy) {
                visible.push(name);
            }
        }
        Ok(visible)
    }

    /// `ALTER TABLE ... ADD PARTITION` against a Hive root: append one
    /// index entry and rewrite the manifest. Data files are never touched.
    pub async fn add_partition(
        &self,
        session: &Session,
        table_ref: &TableReference,
        spec: sqe_catalog::hive::partition::PartitionSpec,
        if_not_exists: bool,
    ) -> Result<(), SqeError> {
        let (root, database, table) = self.full_hive_name(table_ref)?;
        let store = self.store_for(&root)?;
        let mut manifest = store
            .get(&database, &table)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;
        self.refuse_if_denied(
            &session.user,
            &database,
            &table,
            &manifest,
            "ALTER TABLE ADD PARTITION",
        )
        .await?;

        sqe_catalog::hive::partition::add_partition(&store, &mut manifest, &spec, if_not_exists)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        self.invalidate_manifest(&database, &table);
        self.emit_audit_event(
            session,
            &root,
            &database,
            &table,
            "ALTER TABLE ADD PARTITION",
        );
        Ok(())
    }

    /// `ALTER TABLE ... DROP PARTITION` against a Hive root: remove the
    /// index entry, never the data. `purge` is rejected here (cycle 4 owns
    /// data deletion), matching `drop_table`'s PURGE wording.
    pub async fn drop_partition(
        &self,
        session: &Session,
        table_ref: &TableReference,
        spec: sqe_catalog::hive::partition::PartitionSpec,
        if_exists: bool,
        purge: bool,
    ) -> Result<(), SqeError> {
        let (root, database, table) = self.full_hive_name(table_ref)?;
        if purge {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: ALTER TABLE ... DROP PARTITION ... PURGE is not supported for \
                 Hive external tables ({database}.{table}): PURGE deletes the underlying data, \
                 which cycle 4 does not implement yet"
            )));
        }
        let store = self.store_for(&root)?;
        let mut manifest = store
            .get(&database, &table)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;
        self.refuse_if_denied(
            &session.user,
            &database,
            &table,
            &manifest,
            "ALTER TABLE DROP PARTITION",
        )
        .await?;

        sqe_catalog::hive::partition::drop_partition(&store, &mut manifest, &spec, if_exists)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        self.invalidate_manifest(&database, &table);
        self.emit_audit_event(
            session,
            &root,
            &database,
            &table,
            "ALTER TABLE DROP PARTITION",
        );
        Ok(())
    }

    /// `MSCK REPAIR TABLE` against a Hive root: rebuild the whole partition
    /// index from storage. Returns the number of partitions found. Junk
    /// paths (non-`k=v` dirs, untypeable values, files at the table root)
    /// are skipped. Projection-enabled tables are a successful no-op.
    pub async fn msck_repair(
        &self,
        session: &Session,
        table_ref: &TableReference,
    ) -> Result<usize, SqeError> {
        self.repair_index(session, table_ref, "MSCK REPAIR TABLE")
            .await
    }

    /// Rebuild `sqe.partition_index` after a write, so a new partition
    /// directory is visible to the next query.
    ///
    /// Cycle 2 made reads plan from the index instead of listing the table
    /// prefix, which means a write into a partition the index does not mention
    /// is INVISIBLE. Measured, not assumed: an insert into an unindexed
    /// partition lands its file on disk and the next `SELECT` returns the old
    /// five rows. Without this call the strongest read optimization in the Hive
    /// path is also the thing that makes the first write look like a lost write.
    ///
    /// Deliberately the same discovery `MSCK REPAIR TABLE` runs, rather than an
    /// incremental update computed from the written files. Two reasons: a second
    /// implementation would drift from the one `MSCK REPAIR` uses, and drift
    /// here means the recovery path disagrees with the write path about what
    /// the table contains. The cost is a full prefix LIST per write, which is
    /// exactly the work the partition index exists to avoid on READS -- an
    /// incremental update is the right optimization later, and it needs the
    /// written partition values threaded out of the sink to be worth doing.
    ///
    /// Audited as the write rather than as a repair: an operator reading the
    /// audit log should not see a `MSCK REPAIR TABLE` nobody issued.
    pub async fn refresh_index_after_write(
        &self,
        session: &Session,
        table_ref: &TableReference,
    ) -> Result<usize, SqeError> {
        self.repair_index(session, table_ref, "INSERT").await
    }

    /// Refuse a write BEFORE any data file lands. The post-write index
    /// refresh runs the same check, but by then the objects exist in the
    /// bucket and a later `MSCK REPAIR TABLE` by anyone makes them visible
    /// (architecture review 503). Both checks stay: this one is the gate,
    /// the later one is belt and braces.
    pub async fn refuse_write_if_denied(
        &self,
        session: &Session,
        table_ref: &TableReference,
        statement: &str,
    ) -> Result<(), SqeError> {
        let (root, database, table) = self.full_hive_name(table_ref)?;
        let store = self.store_for(&root)?;
        let manifest = match store.get(&database, &table).await {
            Ok(m) => m,
            // No manifest means no table and therefore nothing to authorize:
            // the write path resolves its provider from this same manifest,
            // so without one no object can land. Let it report the lookup
            // failure itself, so the error names the path the statement
            // actually took rather than the gate in front of it.
            Err(sqe_catalog::hive::HiveError::NotFound(_)) => return Ok(()),
            Err(e) => return Err(SqeError::Execution(e.to_string())),
        };
        self.refuse_if_denied(&session.user, &database, &table, &manifest, statement)
            .await
    }

    async fn repair_index(
        &self,
        session: &Session,
        table_ref: &TableReference,
        audit_label: &str,
    ) -> Result<usize, SqeError> {
        let (root, database, table) = self.full_hive_name(table_ref)?;
        let store = self.store_for(&root)?;
        let mut manifest = store
            .get(&database, &table)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;
        self.refuse_if_denied(&session.user, &database, &table, &manifest, audit_label)
            .await?;

        let count = sqe_catalog::hive::partition::msck_repair(
            &store,
            &mut manifest,
            store.limits().max_partition_index_entries,
        )
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        // Invalidate BEFORE returning. `HiveMetadataCache` has a 60s TTL, so a
        // correct index update is still invisible for up to a minute without
        // this -- which looks exactly like the index update having failed.
        self.invalidate_manifest(&database, &table);
        self.emit_audit_event(session, &root, &database, &table, audit_label);
        Ok(count)
    }

    /// `SHOW PARTITIONS <t>` against a Hive root: from the index when
    /// present, from a listing when not. Gated like `show_create_table`: a
    /// fully-denied caller gets the same "table not found" wording an
    /// absent table gets.
    pub async fn show_partitions(
        &self,
        user: &SessionUser,
        table_ref: &TableReference,
    ) -> Result<Vec<String>, SqeError> {
        let (root, database, table) = self.full_hive_name(table_ref)?;
        let store = self.store_for(&root)?;
        let manifest = store
            .get(&database, &table)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        let policy = self
            .resolve_policy(user, &database, &table, &manifest)
            .await;
        if policy_denies_all(&policy) {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: table not found: {database}.{table}"
            )));
        }

        Ok(
            sqe_catalog::hive::partition::list_partitions(&store, &manifest)
                .await
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?
                .into_iter()
                .map(|p| p.spec)
                .collect(),
        )
    }

    /// `ANALYZE TABLE` against a Hive root: persist row counts into
    /// `sqe.statistics`. Gated like a read.
    pub async fn analyze_table(
        &self,
        session: &Session,
        table_ref: &TableReference,
    ) -> Result<sqe_catalog::hive::manifest::HiveTableStatistics, SqeError> {
        let (root, database, table) = self.full_hive_name(table_ref)?;
        let store = self.store_for(&root)?;
        let mut manifest = store
            .get(&database, &table)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;

        let policy = self
            .resolve_policy(&session.user, &database, &table, &manifest)
            .await;
        if policy_denies_all(&policy) {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: ANALYZE TABLE denied on {database}.{table}"
            )));
        }

        let stats = sqe_catalog::hive::partition::analyze_table(&store, &mut manifest)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, e.to_string()))?;
        self.invalidate_manifest(&database, &table);
        self.emit_audit_event(session, &root, &database, &table, "ANALYZE TABLE");
        Ok(stats)
    }

    /// Decompose a catalog-qualified reference into `(root, database,
    /// table)` or refuse. Every cycle-2 statement addresses a Hive table by
    /// its attached root's name, same rule as `DROP TABLE`.
    fn full_hive_name(
        &self,
        table_ref: &TableReference,
    ) -> Result<(String, String, String), SqeError> {
        match self.resolve_table_kind(table_ref) {
            crate::hive_ddl_handler::TableKind::Hive {
                root,
                database,
                table,
            } => Ok((root, database, table)),
            crate::hive_ddl_handler::TableKind::Iceberg => Err(SqeError::Execution(
                "hive_external: this statement needs a catalog-qualified Hive table name \
                 (e.g. `lake.sales.orders`)"
                    .to_string(),
            )),
        }
    }

    /// Resolve `user`'s effective policy against a Hive table, using the
    /// SAME resolution the plan rewriter runs for a real Iceberg scan
    /// (`resolve_effective_policy`: resource policy, then tag-derived masks
    /// and filters, with the configured mask/tag precedence) -- not a
    /// second, hand-rolled authorization check. `policy_store: None` means
    /// policy enforcement is off cluster-wide, in which case this returns
    /// the default (unrestricted) policy, matching `PassthroughEnforcer`'s
    /// effect on the Iceberg read path.
    ///
    /// Column tags come straight from the manifest already in hand
    /// (`manifest.sqe.column_tags`), not `HiveMetadataCache`/`HiveTagSource`:
    /// that cache is populated as a side effect of `HiveSchemaProvider::table()`
    /// during planning, which neither `show_create_table` nor `list_tables`
    /// goes through (there is no LogicalPlan here), so it may not be warm.
    /// Reading the manifest's own field is both simpler and never stale.
    async fn resolve_policy(
        &self,
        user: &SessionUser,
        database: &str,
        table: &str,
        manifest: &HiveManifest,
    ) -> sqe_policy::ResolvedPolicy {
        let Some(store) = self.policy_store.as_deref() else {
            return sqe_policy::ResolvedPolicy::default();
        };
        let col_tags: HashMap<String, Vec<String>> = manifest
            .sqe
            .column_tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        resolve_effective_policy(
            store,
            user,
            table,
            database,
            &col_tags,
            self.mask_precedence,
        )
        .await
    }

    async fn refuse_if_denied(
        &self,
        user: &SessionUser,
        database: &str,
        table: &str,
        manifest: &HiveManifest,
        statement: &str,
    ) -> Result<(), SqeError> {
        let policy = self.resolve_policy(user, database, table, manifest).await;
        if policy_denies_all(&policy) {
            return Err(SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "hive_external: {statement} denied on {database}.{table}"
            )));
        }
        Ok(())
    }

    /// The metadata source for a root, whatever backs it. Everything that
    /// only reads should use this: a Glue-backed root answers reads fine and
    /// has no sidecar at all.
    fn source_for(
        &self,
        catalog_name: &str,
    ) -> Result<Arc<dyn sqe_catalog::hive::source::HiveMetadataSource>, SqeError> {
        let roots = self
            .runtime_catalogs
            .hive_roots()
            .map_err(SqeError::Catalog)?;
        roots
            .into_iter()
            .find(|(name, _)| name == catalog_name)
            .map(|(_, root)| root.source())
            .ok_or_else(|| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "hive root '{catalog_name}' is not attached; attach it with ATTACH \
                     ... TYPE hive_external (or configure it under [[hive.roots]])"
                ))
            })
    }

    fn store_for(
        &self,
        catalog_name: &str,
    ) -> Result<Arc<sqe_catalog::hive::store::HiveManifestStore>, SqeError> {
        let roots = self
            .runtime_catalogs
            .hive_roots()
            .map_err(SqeError::Catalog)?;
        let root = roots
            .into_iter()
            .find(|(name, _)| name == catalog_name)
            .map(|(_, root)| root)
            .ok_or_else(|| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                    "hive root '{catalog_name}' is not attached; attach it with ATTACH \
                     ... TYPE hive_external (or configure it under [[hive.roots]]) before \
                     creating tables in it"
                ))
            })?;
        // A root over a metastore SQE does not own has nothing to write a
        // manifest to. Say so rather than failing further in on a
        // half-applied change.
        root.sidecar().ok_or_else(|| {
            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                "catalog '{catalog_name}' reads an external metastore that SQE does not own, so \
                 this statement is not available on it; make the change where the metastore owns \
                 the definition"
            ))
        })
    }

    /// Emit the handler's own audit event. `audit_resources` (in
    /// `query_handler.rs`) only sees DataFusion `CreateExternalTable`
    /// plans, and the server-mode Hive DDL path produces none (there is no
    /// LogicalPlan: the write is a manifest, not a table scan), so the
    /// resource that ties this event to the table it touched has to come
    /// from here. Uses the same three-part name `resolve_policy_key` keys
    /// grants under, so audit and policy agree on identity.
    fn emit_audit_event(
        &self,
        session: &Session,
        root: &str,
        database: &str,
        table: &str,
        statement_type: &str,
    ) {
        let Some(audit) = self.audit.as_ref() else {
            return;
        };
        let actor = Actor::from_parts(
            session.user.username.clone(),
            session.user.subject.clone(),
            session.user.email.clone(),
            session.user.roles.clone(),
            session.user.groups.clone(),
        );
        let resource = Resource {
            catalog: Some(root.to_string()),
            namespace: vec![database.to_string()],
            name: table.to_string(),
            object_type: ObjectType::Table,
        };
        audit.log_event(AuditEvent {
            time: chrono::Utc::now(),
            kind: AuditKind::AdminDdl,
            actor,
            outcome: Outcome::Success,
            resources: vec![resource],
            policy: None,
            timing: None,
            stats: None,
            query: Some(sqe_metrics::audit::QueryInfo {
                text: None,
                query_hash: sqe_metrics::audit::query_hash(statement_type),
                statement_type: statement_type.to_string(),
            }),
            session_id: Some(session.id.clone()),
            client_ip: None,
            trace_id: None,
            query_id: None,
            integrity: sqe_metrics::audit::Integrity::default(),
        });
    }
}
