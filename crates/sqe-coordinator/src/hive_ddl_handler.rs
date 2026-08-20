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

use std::sync::Arc;

use datafusion::common::TableReference;
use sqe_catalog::hive::manifest::HiveManifest;
use sqe_core::{SecretStore, Session, SqeError};
use sqe_metrics::audit::{Actor, AuditEvent, AuditKind, AuditLogger, ObjectType, Outcome, Resource};
use sqe_policy::plan_rewriter::HiveCatalogs;
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

impl HiveDdlHandler {
    pub fn new(runtime_catalogs: RuntimeCatalogRegistry, secrets: SecretStore) -> Self {
        Self {
            runtime_catalogs,
            hive_catalogs: None,
            secrets,
            audit: None,
        }
    }

    /// Wire the process-wide `hive_catalogs` registry and audit sink. Called
    /// from `QueryHandler::with_hive_registries` with the same `Arc`s the
    /// policy layer was built from.
    #[must_use = "with_hive_registries consumes self; bind the returned HiveDdlHandler"]
    pub fn with_hive_registries(mut self, hive_catalogs: Arc<HiveCatalogs>) -> Self {
        self.hive_catalogs = Some(hive_catalogs);
        self
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
    /// falls to `TableKind::Iceberg` here, the same outcome an unqualified
    /// name resolving against a non-Hive default catalog would produce. A
    /// caller whose session default catalog happens to be a Hive root and
    /// who addresses a table by its bare name is not covered by this
    /// resolver; today that surfaces as a table-not-found on the Iceberg
    /// path rather than as a Hive-aware answer.
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
                SqeError::Execution(format!(
                    "hive_external: TBLPROPERTIES 'sqe.secret' names '{name}', which is not a \
                     known secret: {e}"
                ))
            })?;
        }

        let created_at = chrono::Utc::now().to_rfc3339();
        let manifest =
            HiveManifest::from_spec(spec, &database, &session.user.username, &created_at);

        store
            .put(&manifest, spec.if_not_exists)
            .await
            .map_err(|e| SqeError::Execution(e.to_string()))?;

        self.emit_audit_event(session, catalog_name, &database, &spec.name, "CREATE EXTERNAL TABLE");
        Ok(())
    }

    /// `DROP TABLE` against a Hive root: delete the manifest, never the
    /// data. Callers decide whether `table_ref` even names a Hive table
    /// (via `resolve_table_kind`) before calling this; `if_exists` handling
    /// is the caller's job too, by matching the returned error the same
    /// way the Iceberg drop path matches `is_table_not_found`.
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
            return Err(SqeError::Execution(format!(
                "hive_external: DROP TABLE ... PURGE is not supported for Hive external tables \
                 ({schema}.{table}): PURGE deletes the underlying data, which an external \
                 table's DROP never does"
            )));
        }

        let store = self.store_for(catalog)?;
        store
            .delete(schema, table)
            .await
            .map_err(|e| SqeError::Execution(e.to_string()))?;

        self.emit_audit_event(session, catalog, schema, table, "DROP TABLE");
        Ok(())
    }

    /// `SHOW CREATE TABLE` against a Hive root: render Athena/Glue-style
    /// DDL from the manifest, through the same shared renderer the
    /// embedded client uses (`sqe_catalog::hive::render_hive_create_table_ddl`).
    pub async fn show_create_table(&self, table_ref: &TableReference) -> Result<String, SqeError> {
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
        let store = self.store_for(catalog)?;
        let manifest = store
            .get(schema, table)
            .await
            .map_err(|e| SqeError::Execution(e.to_string()))?;
        let spec = sqe_catalog::hive::hive_spec_from_manifest(catalog, &manifest);
        Ok(sqe_catalog::hive::render_hive_create_table_ddl(&spec))
    }

    /// `SHOW TABLES FROM <root>.<database>`: list tables via the manifest
    /// store, in the same Trino-compatible bare-name shape
    /// `handle_show_tables` already uses for Iceberg.
    pub async fn list_tables(&self, root: &str, database: &str) -> Result<Vec<String>, SqeError> {
        let store = self.store_for(root)?;
        store
            .list_tables(database)
            .await
            .map_err(|e| SqeError::Execution(e.to_string()))
    }

    fn store_for(
        &self,
        catalog_name: &str,
    ) -> Result<Arc<sqe_catalog::hive::store::HiveManifestStore>, SqeError> {
        let roots = self
            .runtime_catalogs
            .hive_roots()
            .map_err(SqeError::Catalog)?;
        roots
            .into_iter()
            .find(|(name, _)| name == catalog_name)
            .map(|(_, store)| store)
            .ok_or_else(|| {
                SqeError::Execution(format!(
                    "hive root '{catalog_name}' is not attached; attach it with ATTACH \
                     ... TYPE hive_external (or configure it under [[hive.roots]]) before \
                     creating tables in it"
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
