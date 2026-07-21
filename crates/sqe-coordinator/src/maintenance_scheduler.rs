//! Advisory and active auto-compaction scheduler loop (Phase 4a Task 5,
//! extended by Phase 4b's active-mode arm).
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
//! # Advisory mutates nothing; Active commits rewrites
//!
//! In `Advisory` mode, `advisory_tick` never rewrites, deletes, or otherwise
//! commits against a discovered user table. It loads each table read-only
//! (`load_table`, `collect_health_inputs`, `analyze_table_health`), then only
//! *reports*: Prometheus gauges, an `AuditKind::Maintenance` event, and a
//! best-effort `maintenance_log` row.
//!
//! In `Active` mode (Phase 4b), `advisory_tick` delegates each due,
//! opted-in table to `active_one_table` instead, which refreshes the
//! maintenance session's token and commits a `rewrite_data_files` rewrite
//! for that table through the scheduler's own `MaintenanceHandler`.
//! `active_one_table` is the only place in this file that ever commits to
//! a user table.
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
use std::str::FromStr;
use std::sync::Arc;

use croner::Cron;
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
/// not the one the interactive query path's `QueryHandler` owns, and this
/// scheduler never resolves a catalog through it (see `active_one_table`,
/// which calls `handler.rewrite_data_files` with a catalog it already built
/// via `catalog_factory`, bypassing `MaintenanceHandler::create_catalog_bridge`
/// entirely). In `mode == Active` it also carries its own DataFusion runtime
/// (`with_runtime`, for sort/z-order compaction); that is a second
/// `FairSpillPool` instance competing for the same host memory as the query
/// engine's, so the caller wires it in only when Active mode is configured
/// (see `sqe_server.rs`). In `Advisory` mode `handler` is built without a
/// runtime: the advisory arm never calls `handler.rewrite_data_files`.
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

    /// One tick: mint a session, discover tables under the default
    /// warehouse (single-catalog in 4a), filter to those opted in via
    /// `sqe.maintenance.enabled = 'true'`, and process each due table.
    /// In `Advisory` mode this only reads and reports: it analyzes each due
    /// table and emits metrics/audit/log rows, mutating nothing. In `Active`
    /// mode each due table is instead delegated to `active_one_table`,
    /// which refreshes the session's token and commits a rewrite for that
    /// table.
    ///
    /// A failure discovering or loading one table (or a lower-level catalog
    /// hiccup while listing one namespace) is logged and skipped rather
    /// than aborting the whole tick: one bad table must not blind the
    /// scheduler to every other table's compaction debt.
    ///
    /// `advisory_tick` itself deliberately does not call
    /// `MaintenancePrincipal::refresh`. Task 2's carried-forward warning is
    /// that a maintenance session's `token_expiry()` is fabricated
    /// (`now + 1h`) and must never be trusted for a long-running job that
    /// COMMITS. This function mints a brand-new session at the top of every
    /// tick; in `Advisory` mode that session is only used for read-only
    /// catalog calls plus one best-effort log append, so a tick that somehow
    /// runs past real token expiry degrades to a logged per-table warning
    /// (or a warned, swallowed `maintenance_log` append failure), not a
    /// silent wrong-privilege commit. In `Active` mode, `active_one_table`
    /// is the one that refreshes the token unconditionally before its
    /// commit.
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
        // Lookup for `resolve_schedule`'s per-table `sqe.maintenance.compaction.schedule`
        // override; built once per tick from the same discovery pass `select_enabled`
        // just filtered, so the gate and the override resolution can never see
        // different properties for the same table.
        let props_by_name: HashMap<&str, &HashMap<String, String>> = props_by_table
            .iter()
            .map(|(name, props)| (name.as_str(), props))
            .collect();

        for name in enabled {
            let empty_props = HashMap::new();
            let table_props = props_by_name.get(name.as_str()).copied().unwrap_or(&empty_props);
            let schedule = resolve_schedule(&self.cfg.scheduler.schedule, table_props);
            if !table_due(
                &name,
                &schedule,
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
/// Table property that overrides `[maintenance.scheduler].schedule` (the
/// global cron expression). See [`resolve_schedule`] and [`table_due`].
pub const COMPACTION_SCHEDULE_PROPERTY: &str = "sqe.maintenance.compaction.schedule";

/// Resolve the effective cron `schedule` for one table: a
/// `sqe.maintenance.compaction.schedule` table property overrides
/// `[maintenance.scheduler].schedule`; an absent or blank override falls
/// back to the global value. Pure, and deliberately does not validate the
/// cron syntax itself -- [`table_due`] is the single place that parses (and
/// reports on) an invalid cron string, so the two can never disagree about
/// what "invalid" means.
pub fn resolve_schedule(global_schedule: &str, table_props: &HashMap<String, String>) -> String {
    table_props
        .get(COMPACTION_SCHEDULE_PROPERTY)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| global_schedule.to_string())
}

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

/// Process-wide dedup for the "invalid cron schedule" warning: keyed on
/// `ident` + the exact (rejected) schedule string, so a table logs at most
/// once per distinct bad value rather than once per tick forever, but still
/// re-warns if the operator edits the property to a DIFFERENT (still
/// invalid) string. Mutating this on every `table_due` call for an invalid
/// schedule does not make `table_due` impure in the sense that matters here:
/// it never reads wall-clock or any other ambient state, and it never
/// changes the function's *return value* for given inputs (always `false`
/// on a parse error) -- only whether a side-effecting log line fires.
///
/// Intentionally unbounded: it never evicts entries, but in practice it is
/// bounded by the number of distinct (table, invalid-schedule-string) pairs
/// ever seen, which tracks table/config count, not tick count.
fn warned_invalid_schedules() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static WARNED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    WARNED.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

fn warn_invalid_schedule_once(ident: &str, schedule: &str, error: &croner::errors::CronError) {
    let key = format!("{ident}\u{1}{schedule}");
    let mut warned = warned_invalid_schedules()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if warned.insert(key) {
        warn!(
            table = %ident,
            schedule = %schedule,
            error = %error,
            "table_due: invalid cron schedule; skipping this table until the schedule is fixed"
        );
    }
}

/// True when table `ident` is due for an advisory (or active) pass at
/// `now_ms`.
///
/// `schedule` is a standard 5-field cron expression (minute hour dom month
/// dow), e.g. `MaintenanceSchedulerConfig::schedule`'s daily-02:00 default
/// `"0 2 * * *"`, evaluated via `croner` in UTC (see `MaintenanceSchedulerConfig::schedule`'s
/// doc comment). A fleet-wide schedule would still fire every opted-in
/// table on the exact same tick, so `jitter_offset_secs` adds a
/// deterministic per-table delay (in `[0, jitter_secs)`) on top of the
/// cron's own fire time before the tick-window check below: the EFFECTIVE
/// fire instant this function checks against `now_ms` is `cron_fire_time +
/// jitter_offset_secs(ident, jitter_secs)`, not the raw cron fire time
/// itself.
///
/// `jitter_secs == 0` means zero jitter delay (the effective fire instant
/// equals the raw cron fire time) -- it does NOT bypass the schedule check.
/// `schedule` is always parsed and validated, and the normal tick-window
/// check below always applies; an invalid cron with `jitter_secs == 0` is
/// therefore never always-due, matching every other `jitter_secs` value.
/// Tests that want deterministic single-tick behavior regardless of
/// wall-clock time should pair `jitter_secs = 0` with a permissive
/// every-tick schedule (e.g. `"* * * * *"`), not rely on a bypass (see
/// `tests/maintenance_scheduler_test.rs`).
///
/// Due predicate: the table is due iff the effective fire instant falls in
/// the half-open window `(now_ms - tick_secs, now_ms]`. This is the same
/// tick-window shape the pre-cron implementation used (a `tick_secs`-wide
/// window, not a single second) and for the same reason: the scheduler loop
/// only samples `now` once per `tick_secs` (see `spawn`), and a
/// `tick_secs`-wide window is exactly what makes consecutive sampled ticks
/// tile the timeline with no gaps, so no effective fire instant is ever
/// starved between two samples. Unlike the pre-cron version, there is no
/// modulo/aliasing concern here: the old bug came from comparing a
/// coarse-grid `now_secs % jitter_secs` against a single-second window at an
/// arbitrary (non-tick-aligned) offset; here the window is a plain absolute
/// half-open interval, so it is covered by the sampled-tick grid regardless
/// of that grid's phase.
///
/// Implementation: shift `now_ms` backward by the jitter offset to get
/// `adjusted_now`, then ask `croner` for the cron's most recent fire at or
/// before `adjusted_now` (`find_previous_occurrence(.., inclusive = true)`);
/// the table is due iff that fire is strictly after `adjusted_now -
/// tick_secs` seconds, which is algebraically the same window shifted back
/// by the offset, since `effective_fire = fire + offset` and
/// `now - tick_secs < effective_fire <= now` iff
/// `adjusted_now - tick_secs < fire <= adjusted_now` where
/// `adjusted_now = now - offset`.
///
/// An invalid cron `schedule` (fails to parse) or a `croner` search error
/// (e.g. an unsatisfiable pattern) is logged once per `(ident, schedule)`
/// pair and treated as "never due" -- this function never panics and never
/// falls back to "always due" on a bad schedule.
pub fn table_due(ident: &str, schedule: &str, jitter_secs: u64, tick_secs: u64, now_ms: i64) -> bool {
    let cron = match Cron::from_str(schedule) {
        Ok(c) => c,
        Err(e) => {
            warn_invalid_schedule_once(ident, schedule, &e);
            return false;
        }
    };

    // `jitter_offset_secs` returns 0 when `jitter_secs == 0` (its own guard,
    // not this call site's), so this is safe against divide-by-zero and
    // yields zero jitter delay rather than bypassing the schedule check.
    let offset_secs = jitter_offset_secs(ident, jitter_secs);
    let adjusted_now_ms = now_ms - (offset_secs as i64) * 1000;
    let Some(adjusted_now) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(adjusted_now_ms) else {
        return false;
    };

    let prev_fire = match cron.find_previous_occurrence(&adjusted_now, true) {
        Ok(t) => t,
        Err(_) => return false,
    };

    let window_start_ms = adjusted_now_ms - (tick_secs as i64) * 1000;
    prev_fire.timestamp_millis() > window_start_ms
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
    fn table_due_offsets_differ_across_distinct_idents() {
        // Not a hard requirement of the hash (collisions are legal), but
        // this specific pair must not collide or the fixtures below would
        // not actually be exercising distinct windows.
        let a = jitter_offset_secs("ns.a", 900);
        let b = jitter_offset_secs("ns.completely_different_table_name", 900);
        assert_ne!(a, b, "fixture idents collided; pick different fixtures");
    }

    #[test]
    fn table_due_jitter_disabled_invalid_cron_is_never_always_due() {
        // `jitter_secs == 0` must NOT resurrect the removed pre-cron bypass:
        // an invalid cron combined with zero jitter must still be treated as
        // "never due", not "always due". This is the exact regression this
        // fix closes (Task 4 review).
        assert!(!table_due("ns.t1", "not a cron expression", 0, 60, 0));
        assert!(!table_due("ns.t1", "not a cron expression", 0, 60, 123_456_789));
    }

    #[test]
    fn table_due_jitter_disabled_still_honors_schedule_window() {
        // `jitter_secs == 0` means zero jitter *delay* (the effective fire
        // instant equals the raw cron fire time exactly, since
        // `jitter_offset_secs` returns 0 for `jitter_secs == 0`), not a
        // schedule bypass: the table must be due only inside the daily
        // 02:00 window, not at an arbitrary tick.
        let ident = "ns.t1";
        let schedule = "0 2 * * *";
        let tick_secs = 60;

        let fire_ms = utc_ms(2026, 3, 5, 2, 0, 0);
        assert!(
            table_due(ident, schedule, 0, tick_secs, fire_ms),
            "must be due at the exact (unjittered) cron fire instant"
        );

        let unrelated_ms = utc_ms(2026, 3, 5, 14, 0, 0);
        assert!(
            !table_due(ident, schedule, 0, tick_secs, unrelated_ms),
            "must not be due at an arbitrary tick far from the schedule's fire instant"
        );
    }

    /// Fixed UTC instant helper for cron fixtures: avoids depending on the
    /// local timezone `chrono::Local` would pull in, and panics (rather than
    /// silently picking an ambiguous instant) on a malformed fixture -- there
    /// is no DST in UTC, so `.single()` always succeeds for a valid
    /// (y, mo, d, h, mi, s) tuple.
    fn utc_ms(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
        use chrono::TimeZone;
        chrono::Utc
            .with_ymd_and_hms(y, mo, d, h, mi, s)
            .single()
            .expect("valid fixture datetime")
            .timestamp_millis()
    }

    #[test]
    fn table_due_true_in_tick_window_covering_daily_0200_fire() {
        // Daily "0 2 * * *" is due within the tick window covering its own
        // (jitter-delayed) fire instant.
        let ident = "ns.cron_fixture_a";
        let schedule = "0 2 * * *";
        let jitter_secs = 900;
        let tick_secs = 60;

        let fire_ms = utc_ms(2026, 3, 5, 2, 0, 0);
        let offset_secs = jitter_offset_secs(ident, jitter_secs);
        let effective_fire_ms = fire_ms + (offset_secs as i64) * 1000;

        assert!(
            table_due(ident, schedule, jitter_secs, tick_secs, effective_fire_ms),
            "table must be due at its own effective (schedule + jitter) fire instant"
        );
    }

    #[test]
    fn table_due_false_at_1400_for_daily_0200_schedule() {
        // 14:00 UTC the same day is nowhere near the daily 02:00 fire (even
        // with up to `jitter_secs` of delay), so the table must not be due.
        let ident = "ns.cron_fixture_a";
        let schedule = "0 2 * * *";
        let jitter_secs = 900;
        let tick_secs = 60;

        let now_ms = utc_ms(2026, 3, 5, 14, 0, 0);
        assert!(!table_due(ident, schedule, jitter_secs, tick_secs, now_ms));
    }

    #[test]
    fn table_due_false_one_tick_before_effective_fire() {
        let ident = "ns.cron_fixture_a";
        let schedule = "0 2 * * *";
        let jitter_secs = 900;
        let tick_secs = 60;

        let fire_ms = utc_ms(2026, 3, 5, 2, 0, 0);
        let offset_secs = jitter_offset_secs(ident, jitter_secs);
        let effective_fire_ms = fire_ms + (offset_secs as i64) * 1000;

        assert!(
            !table_due(ident, schedule, jitter_secs, tick_secs, effective_fire_ms - 1_000),
            "table must not be due one second before its tick window opens"
        );
    }

    #[test]
    fn table_due_true_throughout_tick_window_then_false_after() {
        // The due window is `tick_secs` wide (half-open, ending at the next
        // tick boundary), the same anti-aliasing shape the pre-cron
        // implementation used: the scheduler loop only samples `now` once
        // per `tick_secs`, so a single-instant-wide window could fall
        // entirely between two samples. Widening to `tick_secs` guarantees
        // the sampled-tick grid always covers the effective fire instant
        // exactly once, regardless of the grid's phase.
        let ident = "ns.cron_fixture_a";
        let schedule = "0 2 * * *";
        let jitter_secs = 900;
        let tick_secs = 60;

        let fire_ms = utc_ms(2026, 3, 5, 2, 0, 0);
        let offset_secs = jitter_offset_secs(ident, jitter_secs);
        let effective_fire_ms = fire_ms + (offset_secs as i64) * 1000;

        assert!(table_due(
            ident,
            schedule,
            jitter_secs,
            tick_secs,
            effective_fire_ms + (tick_secs as i64 - 1) * 1000
        ));
        assert!(!table_due(
            ident,
            schedule,
            jitter_secs,
            tick_secs,
            effective_fire_ms + (tick_secs as i64) * 1000
        ));
    }

    #[test]
    fn table_due_staggers_same_schedule_across_distinct_idents() {
        // Two tables sharing the exact same fleet-wide cron schedule must
        // not both become due on the other's tick: the jitter delay exists
        // precisely so a shared "0 2 * * *" does not fire every opted-in
        // table on the same tick.
        let schedule = "0 2 * * *";
        let jitter_secs = 900;
        let tick_secs = 60;
        let a = "ns.cron_fixture_a";
        let b = "ns.cron_fixture_b";

        let offset_a = jitter_offset_secs(a, jitter_secs);
        let offset_b = jitter_offset_secs(b, jitter_secs);
        assert_ne!(
            offset_a / tick_secs,
            offset_b / tick_secs,
            "fixture idents must land in different tick_secs buckets, or this test can't show staggering"
        );

        let fire_ms = utc_ms(2026, 3, 5, 2, 0, 0);
        let now_ms_a = fire_ms + (offset_a as i64) * 1000;
        let now_ms_b = fire_ms + (offset_b as i64) * 1000;

        assert!(table_due(a, schedule, jitter_secs, tick_secs, now_ms_a));
        assert!(
            !table_due(b, schedule, jitter_secs, tick_secs, now_ms_a),
            "b must not be due on a's tick"
        );
        assert!(table_due(b, schedule, jitter_secs, tick_secs, now_ms_b));
        assert!(
            !table_due(a, schedule, jitter_secs, tick_secs, now_ms_b),
            "a must not be due on b's tick"
        );
    }

    #[test]
    fn table_due_invalid_cron_is_skipped_not_panicked() {
        let now_ms = utc_ms(2026, 3, 5, 2, 0, 0);
        assert!(!table_due("ns.bad_schedule", "not a cron expression", 900, 60, now_ms));
        // Calling it again (exercising the warn-once dedup path) must still
        // just return false, never panic.
        assert!(!table_due("ns.bad_schedule", "not a cron expression", 900, 60, now_ms));
    }

    #[test]
    fn resolve_schedule_falls_back_to_global_when_no_override() {
        assert_eq!(resolve_schedule("0 2 * * *", &HashMap::new()), "0 2 * * *");
    }

    #[test]
    fn resolve_schedule_per_table_override_wins() {
        let mut props = HashMap::new();
        props.insert(COMPACTION_SCHEDULE_PROPERTY.to_string(), "0 3 * * *".to_string());
        assert_eq!(resolve_schedule("0 2 * * *", &props), "0 3 * * *");
    }

    #[test]
    fn resolve_schedule_blank_override_falls_back_to_global() {
        let mut props = HashMap::new();
        props.insert(COMPACTION_SCHEDULE_PROPERTY.to_string(), "   ".to_string());
        assert_eq!(resolve_schedule("0 2 * * *", &props), "0 2 * * *");
    }

    #[test]
    fn table_due_per_table_override_schedule_wins_over_global() {
        // The global schedule fires at 02:00 and the per-table override at
        // 03:00; at 03:00 the table must be due under the resolved
        // (per-table) schedule even though the global schedule alone would
        // not be due then.
        let ident = "ns.cron_override_fixture";
        let jitter_secs = 900;
        let tick_secs = 60;
        let mut props = HashMap::new();
        props.insert(COMPACTION_SCHEDULE_PROPERTY.to_string(), "0 3 * * *".to_string());

        let global_schedule = "0 2 * * *".to_string();
        let resolved = resolve_schedule(&global_schedule, &props);
        assert_eq!(resolved, "0 3 * * *");

        let offset_secs = jitter_offset_secs(ident, jitter_secs);
        let override_fire_ms = utc_ms(2026, 3, 5, 3, 0, 0) + (offset_secs as i64) * 1000;

        assert!(
            table_due(ident, &resolved, jitter_secs, tick_secs, override_fire_ms),
            "resolved (per-table override) schedule must be due at its own 03:00 fire"
        );
        assert!(
            !table_due(ident, &global_schedule, jitter_secs, tick_secs, override_fire_ms),
            "the unresolved global 02:00 schedule must not be due at the override's 03:00 fire"
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
