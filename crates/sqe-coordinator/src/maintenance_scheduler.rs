//! Advisory auto-compaction scheduler loop (Phase 4a, Task 5).
//!
//! `MaintenanceScheduler` ties together the earlier Phase 4a pieces into a
//! background tick loop:
//!
//! - Task 1 (`sqe_core::config::MaintenanceConfig`) supplies the gate
//!   (`mode`), the tick cadence and per-table jitter (`scheduler`), and the
//!   compaction-debt sizing knobs (`compaction`).
//! - Task 2 (`crate::maintenance_principal::MaintenancePrincipal`) mints an
//!   ephemeral, job-scoped `Session` for each tick. That session is never
//!   registered with a `SessionManager` and never touches the interactive
//!   auth chain.
//! - Task 3 (`crate::table_health::analyze_table_health`) is the pure
//!   analysis; this module only wires its inputs (via
//!   `crate::maintenance::collect_health_inputs`) and its outputs (metrics,
//!   audit, log).
//! - Task 4 (`crate::maintenance_log::{advisory_row, append_row}`) is the
//!   one write this subsystem performs: a best-effort append to the ledger
//!   table. It is not a mutation of any *user* table.
//!
//! # Mutates nothing
//!
//! `advisory_tick` never rewrites, deletes, or otherwise commits against a
//! discovered user table. It loads each table read-only (`load_table`,
//! `collect_health_inputs`, `analyze_table_health`), then only *reports*:
//! Prometheus gauges, an `AuditKind::Maintenance` event, and a best-effort
//! `maintenance_log` row. The `[maintenance]` `Active` mode (real rewrites)
//! is a later phase.
//!
//! # Off mode is total absence, not a runtime no-op
//!
//! `MaintenanceScheduler` deliberately has no path that constructs a
//! `MaintenancePrincipal` on its own -- the caller (coordinator bootstrap in
//! `sqe_server.rs`) is the only place that decides whether the principal
//! and this scheduler exist at all. When `[maintenance] mode = "off"`
//! (the default), bootstrap constructs neither: there is no
//! `MaintenanceScheduler` value anywhere in the process, not merely one
//! whose loop declines to run. That is the actual isolation guarantee;
//! see the wiring in `sqe_server.rs`'s `run_coordinator`.
//!
//! # `catalog_factory`: the test seam
//!
//! Building an `Arc<dyn Catalog>` for a minted session in production goes
//! through `SessionCatalog::for_session`, which only speaks to the REST
//! (Polaris) backend meaningfully in this codebase's test environment (the
//! sqlite-backed test harness used by `maintenance_log_test.rs` bypasses
//! `SessionCatalog` entirely via `sqe_catalog::mount::build_catalog`).
//! `catalog_factory` decouples "how do I turn a minted session into a
//! catalog handle" from `advisory_tick`'s discovery/analysis logic, so the
//! `#[cfg(feature = "test-sqlite")]` end-to-end test in
//! `tests/maintenance_scheduler_test.rs` can inject a closure that ignores
//! the session and hands back a pre-built sqlite catalog, while still
//! exercising the real `MaintenancePrincipal::mint_session` call (against a
//! wiremock IdP) for everything upstream of catalog construction.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use iceberg::{Catalog, TableIdent};
use sqe_core::config::{MaintenanceCompactionConfig, MaintenanceConfig, MaintenanceMode};
use sqe_core::{Session, SqeError};
use sqe_sql::TableRef;
use tracing::{info, warn};

use crate::maintenance_log;
use crate::maintenance_principal::MaintenancePrincipal;
/// Table property that opts a table into this scheduler. Reuses
/// `table_health`'s constant so the `CALL system.table_health` report and
/// the scheduler can never disagree about which property gates advisory
/// analysis.
use crate::table_health::MAINTENANCE_ENABLED_PROPERTY;

/// Builds an `Arc<dyn Catalog>` for a minted maintenance `Session`.
///
/// See the module docs' "`catalog_factory`: the test seam" section. Boxed
/// (not generic) so `MaintenanceScheduler` stays an ordinary `Send`able
/// struct instead of infecting every call site with a type parameter.
pub type CatalogFactory = Arc<
    dyn Fn(&Session) -> Pin<Box<dyn Future<Output = sqe_core::Result<Arc<dyn Catalog>>> + Send>>
        + Send
        + Sync,
>;

/// Build the production `CatalogFactory`: one `SessionCatalog` per tick,
/// scoped to the bearer token the minted maintenance session carries.
/// Mirrors `MaintenanceHandler::create_catalog_bridge`'s default-warehouse
/// branch; Phase 4a is single-catalog, so the Polaris-auto multi-catalog
/// resolution branch that method also has is intentionally not reproduced
/// here.
pub fn default_catalog_factory(
    config: sqe_core::SqeConfig,
    table_cache: Option<sqe_catalog::TableMetadataCache>,
) -> CatalogFactory {
    Arc::new(move |session: &Session| {
        let config = config.clone();
        let table_cache = table_cache.clone();
        let token = session.access_token().expose().to_string();
        Box::pin(async move {
            let session_catalog =
                Arc::new(sqe_catalog::SessionCatalog::for_session(&config, table_cache, &token).await?);
            Ok(session_catalog.as_catalog() as Arc<dyn Catalog>)
        })
    })
}

/// The advisory and active auto-compaction scheduler.
///
/// Holds everything one tick needs: the config gate/knobs, the dedicated
/// service principal, a metrics handle, an optional audit sink, the
/// catalog-construction seam described in the module docs, and (Phase 4b)
/// a `MaintenanceHandler` so the active arm can reuse the EXACT same
/// `rewrite_data_files` code path `CALL system.rewrite_data_files` uses.
///
/// `handler` is a dedicated `MaintenanceHandler` instance for the scheduler,
/// not the one the interactive query path's `QueryHandler` owns: it needs
/// its own DataFusion runtime (`with_runtime`, for sort/z-order compaction)
/// and this scheduler never resolves a catalog through it (see
/// `active_one_table`, which calls `handler.rewrite_data_files` with a
/// catalog it already built via `catalog_factory`, bypassing
/// `MaintenanceHandler::create_catalog_bridge` entirely). Building a second
/// runtime does mean a second `FairSpillPool` instance competing for the
/// same host memory as the query engine's; see the module docs for the
/// tradeoff this accepts in Phase 4b.
pub struct MaintenanceScheduler {
    cfg: MaintenanceConfig,
    principal: Arc<MaintenancePrincipal>,
    metrics: Arc<sqe_metrics::MetricsRegistry>,
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    catalog_factory: CatalogFactory,
    handler: Arc<crate::maintenance::MaintenanceHandler>,
}

impl MaintenanceScheduler {
    pub fn new(
        cfg: MaintenanceConfig,
        principal: Arc<MaintenancePrincipal>,
        metrics: Arc<sqe_metrics::MetricsRegistry>,
        audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
        catalog_factory: CatalogFactory,
        handler: Arc<crate::maintenance::MaintenanceHandler>,
    ) -> Self {
        Self {
            cfg,
            principal,
            metrics,
            audit,
            catalog_factory,
            handler,
        }
    }

    /// One advisory pass: mint a session, discover tables under the default
    /// warehouse (single-catalog in 4a), filter to those opted in via
    /// `sqe.maintenance.enabled = 'true'`, analyze each due table, and emit
    /// metrics/audit/log rows. Never mutates a user table.
    ///
    /// A failure discovering or loading one table (or a lower-level catalog
    /// hiccup while listing one namespace) is logged and skipped rather
    /// than aborting the whole tick: one bad table must not blind the
    /// scheduler to every other table's compaction debt.
    ///
    /// Deliberately does not call `MaintenancePrincipal::refresh`. Task 2's
    /// carried-forward warning is that a maintenance session's
    /// `token_expiry()` is fabricated (`now + 1h`) and must never be
    /// trusted for a long-running job that COMMITS. `advisory_tick` mints a
    /// brand-new session at the top of every tick and only performs
    /// read-only catalog calls plus one best-effort log append; a tick that
    /// somehow runs past real token expiry degrades to a logged per-table
    /// warning (or a warned, swallowed `maintenance_log` append failure),
    /// not a silent wrong-privilege commit. The Active-mode rewrite path
    /// (a later phase) is the one that must refresh unconditionally before
    /// its commit.
    pub async fn advisory_tick(&self) -> sqe_core::Result<()> {
        let job_id = uuid::Uuid::now_v7().to_string();
        let session = self.principal.mint_session(&job_id).await?;
        let catalog = (self.catalog_factory)(&session).await?;

        let namespaces = catalog.list_namespaces(None).await.map_err(|e| {
            SqeError::Catalog(format!("advisory_tick: failed to list namespaces: {e}"))
        })?;

        let mut props_by_table: Vec<(String, HashMap<String, String>)> = Vec::new();
        let mut idents_by_table: HashMap<String, TableIdent> = HashMap::new();

        for ns in &namespaces {
            let tables = match catalog.list_tables(ns).await {
                Ok(t) => t,
                Err(e) => {
                    warn!(
                        namespace = %ns,
                        error = %e,
                        "advisory_tick: failed to list tables; skipping namespace"
                    );
                    continue;
                }
            };
            for ident in tables {
                match crate::maintenance::load_table(&catalog, &ident).await {
                    Ok(table) => {
                        let name = ident.to_string();
                        props_by_table.push((name.clone(), table.metadata().properties().clone()));
                        idents_by_table.insert(name, ident);
                    }
                    Err(e) => {
                        warn!(
                            table = %ident,
                            error = %e,
                            "advisory_tick: failed to load table; skipping"
                        );
                    }
                }
            }
        }

        let enabled = select_enabled(&props_by_table);
        let now_ms = chrono::Utc::now().timestamp_millis();

        for name in enabled {
            if !table_due(
                &name,
                &self.cfg.scheduler.schedule,
                self.cfg.scheduler.jitter_secs,
                self.cfg.scheduler.tick_secs,
                now_ms,
            ) {
                continue;
            }
            let Some(ident) = idents_by_table.get(&name) else {
                continue;
            };
            // Active mode compacts (Phase 4b); every other mode (today only
            // `Advisory` reaches this loop -- `Off` never constructs a
            // scheduler at all) keeps the pre-4b read-only report. This is
            // the entire "advisory/off unchanged" boundary: `active_one_table`
            // is the ONLY place in this file that ever commits to a user
            // table.
            match self.cfg.mode {
                MaintenanceMode::Active => {
                    self.active_one_table(&catalog, ident, now_ms).await;
                }
                _ => {
                    if let Err(e) = self.analyze_one_table(&catalog, ident, &job_id, now_ms).await {
                        warn!(
                            table = %name,
                            error = %e,
                            "advisory_tick: per-table analysis failed; continuing with other tables"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Analyze one table's compaction debt and report it. Read-only: the
    /// only write is the best-effort `maintenance_log` append, and even
    /// that failure is swallowed (logged, not propagated) so it never
    /// blocks the next table in the same tick.
    async fn analyze_one_table(
        &self,
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
        job_id: &str,
        now_ms: i64,
    ) -> sqe_core::Result<()> {
        let table = crate::maintenance::load_table(catalog, ident).await?;
        let (data_files, delete_files, tasks_by_path) =
            crate::maintenance::collect_health_inputs(&table).await?;

        let health = crate::table_health::analyze_table_health(
            &data_files,
            &delete_files,
            &tasks_by_path,
            &self.cfg.compaction,
            table.metadata().properties(),
        );

        let name = ident.to_string();
        self.metrics
            .table_small_files
            .with_label_values(&[&name])
            .set(health.small_files as f64);
        self.metrics
            .table_delete_files
            .with_label_values(&[&name])
            .set(health.delete_files as f64);
        self.metrics
            .maintenance_est_rewrite_bytes
            .with_label_values(&[&name])
            .set(health.est_rewrite_bytes as f64);

        if let Some(audit) = &self.audit {
            audit.log_event(build_maintenance_audit_event(
                ident,
                &self.principal.user_id,
                job_id,
                &health,
            ));
        }

        let row = crate::maintenance_log::advisory_row(&name, &self.principal.user_id, &health, now_ms);
        if let Err(e) =
            crate::maintenance_log::append_row(catalog, &self.cfg.scheduler.state_table, &row).await
        {
            warn!(
                table = %name,
                error = %e,
                "advisory_tick: maintenance_log append failed (best-effort, not fatal)"
            );
        }

        info!(
            table = %name,
            small_files = health.small_files,
            delete_files = health.delete_files,
            eligible_groups = health.eligible_groups,
            est_rewrite_bytes = health.est_rewrite_bytes,
            "advisory_tick: recorded table health"
        );

        Ok(())
    }

    /// Compact one due, opted-in table under `active` mode (Phase 4b).
    ///
    /// Always computes and emits the same health gauges [`analyze_one_table`]
    /// does (an operator watching Grafana sees the same signal regardless of
    /// mode), then either skips (no eligible debt) or runs a real
    /// `rewrite_data_files` commit through `self.handler` -- the SAME method
    /// `CALL system.rewrite_data_files` uses -- under a freshly minted
    /// maintenance session.
    ///
    /// Infallible by design: every failure path (load, mint, refresh,
    /// catalog build, or the rewrite itself) is caught here, turned into a
    /// `failed` `maintenance_log` row plus a `sqe_maintenance_job_total`
    /// sample, and swallowed. One table's compaction failing must never
    /// abort the tick or block any other opted-in table from being
    /// considered.
    async fn active_one_table(&self, catalog: &Arc<dyn Catalog>, ident: &TableIdent, now_ms: i64) {
        let name = ident.to_string();

        let table = match crate::maintenance::load_table(catalog, ident).await {
            Ok(t) => t,
            Err(e) => {
                warn!(table = %name, error = %e, "active_tick: failed to load table; skipping");
                return;
            }
        };
        let (data_files, delete_files, tasks_by_path) =
            match crate::maintenance::collect_health_inputs(&table).await {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        table = %name,
                        error = %e,
                        "active_tick: failed to collect health inputs; skipping"
                    );
                    return;
                }
            };

        // Resolve per-table overrides BEFORE computing health: three of the
        // four overridable knobs (target_file_size_bytes, min_input_files,
        // delete_file_threshold) directly determine eligibility. Computing
        // health from the GLOBAL config while the rewrite below uses the
        // per-table-resolved `params` would let a table that LOOSENS an
        // override (e.g. a lower `min-input-files`) get skipped by a gate
        // that never saw its own override -- the opposite of "per-table
        // overrides win over global config".
        let params = resolve_compaction_params(&self.cfg.compaction, table.metadata().properties());
        let effective_cfg = MaintenanceCompactionConfig {
            target_file_size_bytes: params.target_file_size_bytes,
            min_input_files: params.min_input_files,
            delete_file_threshold: params.delete_file_threshold,
            strategy: params.strategy.clone(),
        };

        let health = crate::table_health::analyze_table_health(
            &data_files,
            &delete_files,
            &tasks_by_path,
            &effective_cfg,
            table.metadata().properties(),
        );

        // Same observability as advisory mode, regardless of whether this
        // tick goes on to actually compact. Gauges reflect the EFFECTIVE
        // (per-table-resolved) knobs too, so they agree with the gate and
        // the rewrite below, not with a global config a table has opted
        // out of via its own override.
        self.metrics
            .table_small_files
            .with_label_values(&[&name])
            .set(health.small_files as f64);
        self.metrics
            .table_delete_files
            .with_label_values(&[&name])
            .set(health.delete_files as f64);
        self.metrics
            .maintenance_est_rewrite_bytes
            .with_label_values(&[&name])
            .set(health.est_rewrite_bytes as f64);

        let job_id = uuid::Uuid::now_v7().to_string();
        let started_at_ms = now_ms;

        // Eligibility: small-file debt (eligible_groups) OR ANY delete-heavy
        // file. `>= delete_file_threshold` (a per-file delete-COUNT
        // threshold) does not apply here -- `delete_heavy_files` already
        // is a file COUNT that met that threshold; requiring it to also
        // clear the threshold would skip a table with exactly one
        // delete-heavy file under the (very common) default threshold of 2.
        let has_eligible_work = health.eligible_groups > 0 || health.delete_heavy_files > 0;
        if !has_eligible_work {
            info!(table = %name, "active_tick: skipped, no eligible compaction debt");
            let row = maintenance_log::skipped_row(
                &job_id,
                &name,
                &self.principal.user_id,
                started_at_ms,
                "no eligible compaction debt",
            );
            self.append_job_row(catalog, &row).await;
            self.metrics
                .maintenance_job_total
                .with_label_values(&["skipped"])
                .inc();
            return;
        }

        let mut session = match self.principal.mint_session(&job_id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(table = %name, error = %e, "active_tick: failed to mint maintenance session");
                self.record_failed_job(catalog, &job_id, &name, started_at_ms, &e.to_string())
                    .await;
                return;
            }
        };

        // Defensive: `MaintenancePrincipal::session_from_identity` always
        // sets `has_maintenance_authority(true)`, but a compaction commit
        // is exactly the kind of consequential action that must not
        // silently proceed if that ever regresses.
        if !crate::maintenance::session_has_write_privilege(&session) {
            warn!(
                table = %name,
                "active_tick: minted maintenance session unexpectedly lacks write privilege; refusing to compact"
            );
            self.record_failed_job(
                catalog,
                &job_id,
                &name,
                started_at_ms,
                "minted maintenance session lacks write privilege",
            )
            .await;
            return;
        }

        // Refresh the token right before the commit: `Session::token_expiry()`
        // on a maintenance session is fabricated (see
        // `MaintenancePrincipal::refresh`'s doc comment) and must never be
        // trusted. Best-effort -- a refresh failure is logged and the
        // mint-time token is used as-is (still valid for a short job); the
        // eventual commit fails cleanly on a genuinely dead token rather
        // than silently using the wrong privilege.
        if let Err(e) = self.principal.refresh(&mut session).await {
            warn!(table = %name, error = %e, "active_tick: token refresh failed; proceeding with the mint-time token");
        }

        // Build the COMMIT catalog from the (possibly refreshed) session.
        // The tick's own `catalog` parameter was built from the tick-level
        // discovery session, not this table's job session; rebuilding here
        // is what makes the refresh above actually reach the commit instead
        // of being a no-op.
        let commit_catalog = match (self.catalog_factory)(&session).await {
            Ok(c) => c,
            Err(e) => {
                warn!(table = %name, error = %e, "active_tick: failed to build commit catalog");
                self.record_failed_job(catalog, &job_id, &name, started_at_ms, &e.to_string())
                    .await;
                return;
            }
        };

        let table_ref = match table_ref_from_ident(ident) {
            Ok(t) => t,
            Err(e) => {
                warn!(table = %name, error = %e, "active_tick: failed to build table reference");
                self.record_failed_job(catalog, &job_id, &name, started_at_ms, &e.to_string())
                    .await;
                return;
            }
        };

        // `params` was already resolved above (before the health/eligibility
        // check); reused here so the gate and the rewrite can never disagree
        // about the effective per-table knobs.
        // "binpack" (the default) means no explicit strategy: `None` lets
        // `rewrite_data_files_once` take its own bin-pack default without
        // requiring the shared DataFusion runtime a sort/z-order strategy
        // needs.
        let strategy = if params.strategy.eq_ignore_ascii_case("binpack") {
            None
        } else {
            Some(params.strategy.clone())
        };
        let snapshot_properties = HashMap::from([
            ("sqe.maintenance.job-id".to_string(), job_id.clone()),
            ("sqe.maintenance.principal".to_string(), self.principal.user_id.clone()),
            ("sqe.maintenance.trigger".to_string(), "scheduled".to_string()),
        ]);

        let result = self
            .handler
            .rewrite_data_files(
                &commit_catalog,
                &table_ref,
                Some(params.target_file_size_bytes),
                Some(params.min_input_files),
                None, // max_concurrent_file_group_rewrites: handler default; not part of the Phase 4b per-table override surface.
                strategy,
                None, // sort_order: not part of the Phase 4b per-table override surface.
                Some(params.delete_file_threshold),
                Some(snapshot_properties),
            )
            .await;

        let finished_at_ms = chrono::Utc::now().timestamp_millis();
        match result {
            Ok(outcome) if outcome.skipped_reason.is_some() => {
                let reason = outcome.skipped_reason.unwrap_or_default();
                info!(table = %name, reason = %reason, "active_tick: rewrite_data_files skipped");
                let row = maintenance_log::skipped_row(
                    &job_id,
                    &name,
                    &self.principal.user_id,
                    started_at_ms,
                    &reason,
                );
                self.append_job_row(catalog, &row).await;
                self.metrics
                    .maintenance_job_total
                    .with_label_values(&["skipped"])
                    .inc();
            }
            Ok(outcome) => {
                info!(
                    table = %name,
                    files_in = outcome.files_in,
                    files_out = outcome.files_out,
                    bytes_out = outcome.bytes_out,
                    rows_removed = outcome.rows_removed,
                    snapshot_id = ?outcome.snapshot_id,
                    "active_tick: compaction committed"
                );
                if let Some(audit) = &self.audit {
                    audit.log_event(build_active_audit_event(
                        ident,
                        &self.principal.user_id,
                        &job_id,
                        &outcome,
                    ));
                }
                let row = maintenance_log::success_row(
                    &job_id,
                    &name,
                    &self.principal.user_id,
                    started_at_ms,
                    finished_at_ms,
                    outcome.files_in,
                    outcome.files_out,
                    outcome.bytes_in,
                    outcome.bytes_out,
                    outcome.rows_removed,
                    outcome.snapshot_id,
                );
                self.append_job_row(catalog, &row).await;
                self.metrics
                    .maintenance_job_total
                    .with_label_values(&["success"])
                    .inc();
                self.metrics
                    .maintenance_bytes_rewritten_total
                    .inc_by(outcome.bytes_out.max(0) as u64);
            }
            Err(e) => {
                warn!(table = %name, error = %e, "active_tick: compaction failed");
                self.record_failed_job(catalog, &job_id, &name, started_at_ms, &e.to_string())
                    .await;
            }
        }
    }

    /// Build and append a `failed` `maintenance_log` row plus its metric
    /// sample. Shared by every `active_one_table` error path so the
    /// job-total counter and the ledger can never disagree about whether a
    /// job counted as failed.
    async fn record_failed_job(
        &self,
        catalog: &Arc<dyn Catalog>,
        job_id: &str,
        table: &str,
        started_at_ms: i64,
        error: &str,
    ) {
        let row = maintenance_log::failed_row(
            job_id,
            table,
            &self.principal.user_id,
            started_at_ms,
            chrono::Utc::now().timestamp_millis(),
            error,
        );
        self.append_job_row(catalog, &row).await;
        self.metrics
            .maintenance_job_total
            .with_label_values(&["failed"])
            .inc();
    }

    /// Best-effort `maintenance_log` append, shared by every active-mode
    /// terminal row. Mirrors `analyze_one_table`'s advisory-row append: a
    /// failure to write the ledger is logged, never propagated, and never
    /// blocks the next table.
    async fn append_job_row(&self, catalog: &Arc<dyn Catalog>, row: &maintenance_log::MaintenanceLogRow) {
        if let Err(e) =
            maintenance_log::append_row(catalog, &self.cfg.scheduler.state_table, row).await
        {
            warn!(
                table = %row.table,
                status = %row.status,
                error = %e,
                "active_tick: maintenance_log append failed (best-effort, not fatal)"
            );
        }
    }

    /// Spawn the supervised tick loop.
    ///
    /// Callers must only call this when `cfg.mode != Off` -- in `Off` mode
    /// bootstrap never constructs a `MaintenanceScheduler` at all (see the
    /// module docs). Inside the loop, a tick only runs when the mode is
    /// `Advisory` or `Active`; that check is redundant with the
    /// never-construct-in-Off-mode invariant today, but keeps the loop body
    /// correct on its own terms if that invariant is ever loosened.
    pub fn spawn(self) -> sqe_core::TaskGuard {
        sqe_core::spawn_supervised("maintenance-scheduler", move |token| async move {
            let tick_secs = self.cfg.scheduler.tick_secs.max(1);
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(tick_secs));
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = ticker.tick() => {
                        if matches!(self.cfg.mode, MaintenanceMode::Advisory | MaintenanceMode::Active) {
                            if let Err(e) = self.advisory_tick().await {
                                warn!(error = %e, "advisory tick failed");
                                self.metrics.maintenance_tick_errors.inc();
                            }
                        }
                    }
                }
            }
        })
    }
}

/// Build the `AuditKind::Maintenance` event for one analyzed table.
///
/// `actor` is the maintenance principal (never the interactive caller: this
/// runs on a background loop, not in response to a user request).
/// `session_id` carries the tick's job ID so every audit line, metric
/// sample, and `maintenance_log` row from the same tick share a
/// correlatable identifier.
fn build_maintenance_audit_event(
    ident: &TableIdent,
    principal_user: &str,
    job_id: &str,
    health: &crate::table_health::TableHealth,
) -> sqe_metrics::audit::AuditEvent {
    sqe_metrics::audit::AuditEvent {
        time: chrono::Utc::now(),
        kind: sqe_metrics::audit::AuditKind::Maintenance,
        actor: sqe_metrics::audit::Actor::from_parts(
            principal_user.to_string(),
            None,
            None,
            vec!["maintenance".to_string()],
            vec![],
        ),
        outcome: sqe_metrics::audit::Outcome::Success,
        resources: vec![sqe_metrics::audit::Resource {
            catalog: None,
            namespace: ident.namespace().to_vec(),
            name: ident.name().to_string(),
            object_type: sqe_metrics::audit::ObjectType::Table,
        }],
        policy: None,
        timing: None,
        stats: None,
        query: Some(sqe_metrics::audit::QueryInfo {
            text: Some(format!(
                "advisory_tick: table_health small_files={} delete_files={} \
                 eligible_groups={} est_rewrite_bytes={}",
                health.small_files, health.delete_files, health.eligible_groups, health.est_rewrite_bytes
            )),
            query_hash: sqe_metrics::audit::query_hash(&format!(
                "maintenance-advisory:{}",
                ident
            )),
            statement_type: "maintenance_advisory".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    }
}

/// Build the `AuditKind::Maintenance` event for one table this tick actually
/// compacted (Phase 4b active mode). Distinct from
/// [`build_maintenance_audit_event`] (which reports health without acting):
/// this one's `query.text` and `stats` describe what was committed, not what
/// was merely observed.
fn build_active_audit_event(
    ident: &TableIdent,
    principal_user: &str,
    job_id: &str,
    outcome: &crate::maintenance::RewriteOutcome,
) -> sqe_metrics::audit::AuditEvent {
    sqe_metrics::audit::AuditEvent {
        time: chrono::Utc::now(),
        kind: sqe_metrics::audit::AuditKind::Maintenance,
        actor: sqe_metrics::audit::Actor::from_parts(
            principal_user.to_string(),
            None,
            None,
            vec!["maintenance".to_string()],
            vec![],
        ),
        outcome: sqe_metrics::audit::Outcome::Success,
        resources: vec![sqe_metrics::audit::Resource {
            catalog: None,
            namespace: ident.namespace().to_vec(),
            name: ident.name().to_string(),
            object_type: sqe_metrics::audit::ObjectType::Table,
        }],
        policy: None,
        timing: None,
        stats: None,
        query: Some(sqe_metrics::audit::QueryInfo {
            text: Some(format!(
                "active_tick: rewrite_data_files committed files_in={} files_out={} \
                 bytes_out={} rows_removed={} snapshot_id={:?}",
                outcome.files_in, outcome.files_out, outcome.bytes_out, outcome.rows_removed, outcome.snapshot_id
            )),
            query_hash: sqe_metrics::audit::query_hash(&format!("maintenance-active:{}", ident)),
            statement_type: "maintenance_active".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    }
}

/// Build a `sqe_sql::TableRef` from a discovered `TableIdent`, the reverse
/// of `crate::maintenance::to_table_ident` (which this crate keeps private
/// to that module). Round-trips correctly for the single-segment namespaces
/// every table constructed in this codebase uses (`NamespaceIdent::new`);
/// see `TableIdent`'s `Display` impl (`{namespace}.{name}`, and
/// `NamespaceIdent`'s `Display` joins its segments with `.` too), which is
/// exactly the 2-part form `TableRef::parse` accepts.
fn table_ref_from_ident(ident: &TableIdent) -> sqe_core::Result<TableRef> {
    TableRef::parse(&ident.to_string())
}

/// Per-table compaction knobs after resolving `[maintenance.compaction]`
/// overrides. Mirrors `MaintenanceCompactionConfig`'s shape (this is the
/// per-table-resolved form of that global config), not `RewriteOutcome`
/// (which is `rewrite_data_files`'s per-run RESULT, not its input params).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionParams {
    pub target_file_size_bytes: u64,
    pub min_input_files: usize,
    pub delete_file_threshold: usize,
    pub strategy: String,
}

/// Table property that overrides `[maintenance.compaction].target_file_size_bytes`.
pub const COMPACTION_TARGET_FILE_SIZE_BYTES_PROPERTY: &str =
    "sqe.maintenance.compaction.target-file-size-bytes";
/// Table property that overrides `[maintenance.compaction].min_input_files`.
pub const COMPACTION_MIN_INPUT_FILES_PROPERTY: &str = "sqe.maintenance.compaction.min-input-files";
/// Table property that overrides `[maintenance.compaction].delete_file_threshold`.
pub const COMPACTION_DELETE_FILE_THRESHOLD_PROPERTY: &str =
    "sqe.maintenance.compaction.delete-file-threshold";
/// Table property that overrides `[maintenance.compaction].strategy`.
pub const COMPACTION_STRATEGY_PROPERTY: &str = "sqe.maintenance.compaction.strategy";

/// Resolve per-table compaction params: a `sqe.maintenance.compaction.*`
/// table property overrides the matching `[maintenance.compaction]` global
/// config field; an absent or malformed override falls back to the global
/// value. Pure: takes the already-collected global config and table
/// properties, touches no catalog.
///
/// "Malformed" (fails to parse as the target numeric type) is treated
/// exactly like "absent": fall back to the global default and log a
/// warning identifying which property and value were rejected, rather than
/// erroring the whole tick over one bad property string on one table.
pub fn resolve_compaction_params(
    cfg: &MaintenanceCompactionConfig,
    table_props: &HashMap<String, String>,
) -> CompactionParams {
    CompactionParams {
        target_file_size_bytes: resolve_u64_override(
            table_props,
            COMPACTION_TARGET_FILE_SIZE_BYTES_PROPERTY,
            cfg.target_file_size_bytes,
        ),
        min_input_files: resolve_usize_override(
            table_props,
            COMPACTION_MIN_INPUT_FILES_PROPERTY,
            cfg.min_input_files,
        ),
        delete_file_threshold: resolve_usize_override(
            table_props,
            COMPACTION_DELETE_FILE_THRESHOLD_PROPERTY,
            cfg.delete_file_threshold,
        ),
        strategy: table_props
            .get(COMPACTION_STRATEGY_PROPERTY)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
            .unwrap_or_else(|| cfg.strategy.clone()),
    }
}

fn resolve_u64_override(table_props: &HashMap<String, String>, key: &str, default: u64) -> u64 {
    match table_props.get(key) {
        None => default,
        Some(raw) => match raw.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                warn!(
                    property = key,
                    value = %raw,
                    "resolve_compaction_params: malformed override, falling back to global config"
                );
                default
            }
        },
    }
}

fn resolve_usize_override(table_props: &HashMap<String, String>, key: &str, default: usize) -> usize {
    match table_props.get(key) {
        None => default,
        Some(raw) => match raw.parse::<usize>() {
            Ok(v) => v,
            Err(_) => {
                warn!(
                    property = key,
                    value = %raw,
                    "resolve_compaction_params: malformed override, falling back to global config"
                );
                default
            }
        },
    }
}

/// Select the tables opted into the scheduler: `props.get(MAINTENANCE_ENABLED_PROPERTY) == Some("true")`.
///
/// Pure and unit-testable: takes already-collected `(table, props)` pairs
/// rather than touching a catalog itself.
pub fn select_enabled(tables: &[(String, HashMap<String, String>)]) -> Vec<String> {
    tables
        .iter()
        .filter(|(_, props)| {
            props
                .get(MAINTENANCE_ENABLED_PROPERTY)
                .map(|v| v == "true")
                .unwrap_or(false)
        })
        .map(|(name, _)| name.clone())
        .collect()
}

/// Deterministic per-table jitter offset in `[0, jitter_secs)`.
///
/// Manual FNV-1a rather than `std::collections::hash_map::DefaultHasher`:
/// the standard library explicitly does not guarantee `DefaultHasher`'s
/// algorithm is stable across releases, which would make `table_due`'s
/// behavior (and this function's own unit tests) depend on the exact
/// toolchain a build happens to use. FNV-1a is a fixed, simple algorithm
/// with no such caveat.
fn jitter_offset_secs(ident: &str, jitter_secs: u64) -> u64 {
    if jitter_secs == 0 {
        return 0;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in ident.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash % jitter_secs
}

/// True when table `ident` is due for an advisory pass at `now_ms`.
///
/// `schedule` (a cron string, e.g. `MaintenanceSchedulerConfig::schedule`)
/// is accepted but not yet parsed: full cron scheduling is deferred past
/// Phase 4a. In its place, a table is due once per `jitter_secs`-second
/// window, at a deterministic per-table second offset within that window
/// (`jitter_offset_secs`), so many tables opted in at once do not all fire
/// on the same tick. `jitter_secs == 0` disables windowing (every tick is
/// due for every table); the scheduler's config default is nonzero, but a
/// caller that wants unconditional "due if enabled" behavior (as the design
/// brief allows for 4a) can set it to zero.
///
/// The due predicate is a half-open window `[offset, offset + tick_secs)`
/// (mod `jitter_secs`), not a single second. The scheduler loop only
/// samples `now` once per `tick_secs` (see `spawn`); a single-second-wide
/// window can fall entirely between two samples whenever `tick_secs` does
/// not evenly divide into `jitter_secs`'s residues the loop actually visits
/// (e.g. the default `tick_secs = 60` / `jitter_secs = 900` only ever lands
/// on 15 of the 900 possible residues from a given process start, starving
/// the other ~98% of per-table offsets forever). Widening the window to
/// `tick_secs` wide guarantees every offset is covered by some sampled tick
/// once per `jitter_secs` period, at the cost of a table occasionally
/// firing on two adjacent ticks at a window boundary -- acceptable since
/// this scheduler is advisory-only (read-only, idempotent).
pub fn table_due(ident: &str, _schedule: &str, jitter_secs: u64, tick_secs: u64, now_ms: i64) -> bool {
    if jitter_secs == 0 {
        return true;
    }
    let now_secs = now_ms.max(0) as u64 / 1000;
    let r = now_secs % jitter_secs;
    let offset = jitter_offset_secs(ident, jitter_secs);
    let end = offset + tick_secs;
    if end <= jitter_secs {
        r >= offset && r < end
    } else {
        // Window wraps past the end of the jitter period; split into the
        // tail segment [offset, jitter_secs) and the wrapped head segment
        // [0, end - jitter_secs).
        r >= offset || r < end - jitter_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_compaction_cfg() -> MaintenanceCompactionConfig {
        MaintenanceCompactionConfig {
            target_file_size_bytes: 512 * 1024 * 1024,
            min_input_files: 5,
            delete_file_threshold: 2,
            strategy: "binpack".to_string(),
        }
    }

    #[test]
    fn resolve_compaction_params_falls_back_to_global_when_no_props() {
        let cfg = default_compaction_cfg();
        let params = resolve_compaction_params(&cfg, &HashMap::new());
        assert_eq!(params.target_file_size_bytes, cfg.target_file_size_bytes);
        assert_eq!(params.min_input_files, cfg.min_input_files);
        assert_eq!(params.delete_file_threshold, cfg.delete_file_threshold);
        assert_eq!(params.strategy, cfg.strategy);
    }

    #[test]
    fn resolve_compaction_params_per_table_override_wins_target_file_size_bytes() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(
            COMPACTION_TARGET_FILE_SIZE_BYTES_PROPERTY.to_string(),
            "1000".to_string(),
        );
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.target_file_size_bytes, 1000);
        // Every other field still falls back to global.
        assert_eq!(params.min_input_files, cfg.min_input_files);
    }

    #[test]
    fn resolve_compaction_params_per_table_override_wins_min_input_files() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(COMPACTION_MIN_INPUT_FILES_PROPERTY.to_string(), "3".to_string());
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.min_input_files, 3);
    }

    #[test]
    fn resolve_compaction_params_per_table_override_wins_delete_file_threshold() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(
            COMPACTION_DELETE_FILE_THRESHOLD_PROPERTY.to_string(),
            "1".to_string(),
        );
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.delete_file_threshold, 1);
    }

    #[test]
    fn resolve_compaction_params_per_table_override_wins_strategy() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(COMPACTION_STRATEGY_PROPERTY.to_string(), "sort".to_string());
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.strategy, "sort");
    }

    #[test]
    fn resolve_compaction_params_all_overrides_apply_together() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(
            COMPACTION_TARGET_FILE_SIZE_BYTES_PROPERTY.to_string(),
            "2048".to_string(),
        );
        props.insert(COMPACTION_MIN_INPUT_FILES_PROPERTY.to_string(), "7".to_string());
        props.insert(
            COMPACTION_DELETE_FILE_THRESHOLD_PROPERTY.to_string(),
            "4".to_string(),
        );
        props.insert(COMPACTION_STRATEGY_PROPERTY.to_string(), "zorder".to_string());
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(
            params,
            CompactionParams {
                target_file_size_bytes: 2048,
                min_input_files: 7,
                delete_file_threshold: 4,
                strategy: "zorder".to_string(),
            }
        );
    }

    #[test]
    fn resolve_compaction_params_malformed_u64_falls_back_to_global() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(
            COMPACTION_TARGET_FILE_SIZE_BYTES_PROPERTY.to_string(),
            "not-a-number".to_string(),
        );
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.target_file_size_bytes, cfg.target_file_size_bytes);
    }

    #[test]
    fn resolve_compaction_params_malformed_usize_falls_back_to_global() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(COMPACTION_MIN_INPUT_FILES_PROPERTY.to_string(), "-3".to_string());
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.min_input_files, cfg.min_input_files);
    }

    #[test]
    fn resolve_compaction_params_negative_delete_file_threshold_falls_back_to_global() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(
            COMPACTION_DELETE_FILE_THRESHOLD_PROPERTY.to_string(),
            "abc".to_string(),
        );
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.delete_file_threshold, cfg.delete_file_threshold);
    }

    #[test]
    fn resolve_compaction_params_empty_strategy_override_falls_back_to_global() {
        // An empty string override must not silently become the active
        // strategy (which would then fail schema validation deeper in the
        // rewrite path); it should behave exactly like an absent property.
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(COMPACTION_STRATEGY_PROPERTY.to_string(), "".to_string());
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.strategy, cfg.strategy);
    }

    #[test]
    fn resolve_compaction_params_whitespace_only_strategy_override_falls_back_to_global() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(COMPACTION_STRATEGY_PROPERTY.to_string(), "   ".to_string());
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(params.strategy, cfg.strategy);
    }

    #[test]
    fn table_ref_from_ident_round_trips_ns_dot_table() {
        let ident = TableIdent::new(iceberg::NamespaceIdent::new("ns".to_string()), "t".to_string());
        let table_ref = table_ref_from_ident(&ident).expect("parses");
        assert_eq!(table_ref.namespace, "ns");
        assert_eq!(table_ref.name, "t");
        assert_eq!(table_ref.catalog, None);
    }

    #[test]
    fn table_due_is_deterministic_for_same_inputs() {
        let a = table_due("ns.t1", "0 2 * * *", 900, 60, 1_700_000_000_000);
        let b = table_due("ns.t1", "0 2 * * *", 900, 60, 1_700_000_000_000);
        assert_eq!(a, b, "same ident/schedule/jitter/tick/now must always agree");
    }

    #[test]
    fn table_due_true_at_its_jitter_offset_second() {
        let jitter_secs = 900;
        let tick_secs = 60;
        let offset = jitter_offset_secs("ns.t1", jitter_secs);
        let now_ms = (offset as i64) * 1000;
        assert!(
            table_due("ns.t1", "0 2 * * *", jitter_secs, tick_secs, now_ms),
            "table must be due at its own jitter offset second (window's inclusive start)"
        );
    }

    #[test]
    fn table_due_false_outside_window() {
        let jitter_secs = 900;
        let tick_secs = 60;
        let offset = jitter_offset_secs("ns.t1", jitter_secs);
        // One second before the window's start (mod jitter_secs) is outside
        // the half-open [offset, offset + tick_secs) window regardless of
        // tick_secs, unlike "offset + 1" which now falls inside a
        // tick_secs-wide window.
        let before_ms = (((offset + jitter_secs - 1) % jitter_secs) as i64) * 1000;
        assert!(
            !table_due("ns.t1", "0 2 * * *", jitter_secs, tick_secs, before_ms),
            "table must not be due one second before its own window opens"
        );
    }

    #[test]
    fn table_due_offsets_differ_across_distinct_idents() {
        // Not a hard requirement of the hash (collisions are legal), but
        // this specific pair must not collide or the fixtures above would
        // not actually be exercising distinct windows.
        let a = jitter_offset_secs("ns.a", 900);
        let b = jitter_offset_secs("ns.completely_different_table_name", 900);
        assert_ne!(a, b, "fixture idents collided; pick different fixtures");
    }

    #[test]
    fn table_due_always_true_when_jitter_disabled() {
        assert!(table_due("ns.t1", "0 2 * * *", 0, 60, 0));
        assert!(table_due("ns.t1", "0 2 * * *", 0, 60, 123_456_789));
    }

    /// Reproduces the tick-cadence aliasing bug directly: with the
    /// production defaults (`jitter_secs = 900`, `tick_secs = 60`), the
    /// scheduler loop only ever samples `now_secs` at multiples of
    /// `tick_secs` from some fixed process-start offset. This test walks
    /// exactly that lattice (`now_secs = k * tick_secs` for one full
    /// `jitter_secs` period) and asserts a table is due on at least one --
    /// and, since a `tick_secs`-wide window over a `tick_secs`-spaced
    /// lattice contains exactly one lattice point, exactly one -- of those
    /// ticks. Against the old 1-second-window logic this fails whenever the
    /// table's jitter offset is not itself a multiple of `tick_secs`, which
    /// is the ~14/15 common case; the fixture ident below is chosen to land
    /// in that case so the test actually discriminates old vs. new
    /// behavior rather than passing on both by luck.
    #[test]
    fn table_due_not_starved_across_full_tick_grid() {
        let jitter_secs = 900;
        let tick_secs = 60;
        let ident = "ns.grid_fixture_table";

        let offset = jitter_offset_secs(ident, jitter_secs);
        assert_ne!(
            offset % tick_secs,
            0,
            "fixture ident's offset must NOT be tick-aligned, or this test can't tell old code from new"
        );

        let ticks = jitter_secs / tick_secs;
        let mut due_count = 0;
        for k in 0..ticks {
            let now_secs = k * tick_secs;
            let now_ms = (now_secs as i64) * 1000;
            if table_due(ident, "0 2 * * *", jitter_secs, tick_secs, now_ms) {
                due_count += 1;
            }
        }

        assert!(
            due_count >= 1,
            "table must be due on at least one sampled tick per jitter_secs period, got 0 (starved)"
        );
        assert_eq!(
            due_count, 1,
            "a tick_secs-wide window over a tick_secs-spaced grid must be hit exactly once, got {due_count}"
        );
    }

    /// Companion to `table_due_not_starved_across_full_tick_grid`: two
    /// distinct idents opted into the scheduler must not always fire on the
    /// same sampled tick, or the staggering `jitter_offset_secs` exists to
    /// provide is defeated. Picks idents whose offsets fall in different
    /// `tick_secs`-wide buckets (not merely "different offsets", which
    /// could still collide in the same bucket) so the assertion is
    /// meaningful.
    #[test]
    fn table_due_staggers_across_distinct_idents() {
        let jitter_secs = 900;
        let tick_secs = 60;
        let a = "ns.grid_fixture_table";
        let b = "ns.completely_different_table_name";

        let offset_a = jitter_offset_secs(a, jitter_secs);
        let offset_b = jitter_offset_secs(b, jitter_secs);
        assert_ne!(
            offset_a / tick_secs,
            offset_b / tick_secs,
            "fixture idents must land in different tick_secs buckets, or this test can't show staggering"
        );

        let ticks = jitter_secs / tick_secs;
        let mut both_due_on_same_tick = false;
        for k in 0..ticks {
            let now_ms = ((k * tick_secs) as i64) * 1000;
            let due_a = table_due(a, "0 2 * * *", jitter_secs, tick_secs, now_ms);
            let due_b = table_due(b, "0 2 * * *", jitter_secs, tick_secs, now_ms);
            if due_a && due_b {
                both_due_on_same_tick = true;
            }
        }

        assert!(
            !both_due_on_same_tick,
            "distinct idents in different offset buckets must not fire on the same sampled tick"
        );
    }

    #[test]
    fn select_enabled_filters_by_property() {
        let tables = vec![
            (
                "ns.on".to_string(),
                HashMap::from([(MAINTENANCE_ENABLED_PROPERTY.to_string(), "true".to_string())]),
            ),
            (
                "ns.off".to_string(),
                HashMap::from([(MAINTENANCE_ENABLED_PROPERTY.to_string(), "false".to_string())]),
            ),
            ("ns.missing".to_string(), HashMap::new()),
        ];
        let enabled = select_enabled(&tables);
        assert_eq!(enabled, vec!["ns.on".to_string()]);
    }

    #[test]
    fn select_enabled_empty_input_returns_empty() {
        assert!(select_enabled(&[]).is_empty());
    }
}
