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
//! The daily ledger-retention pass (finding 511, below) also commits in
//! `Advisory` mode -- but only against `sqe_system.maintenance_log`, this
//! subsystem's own state table, which advisory mode already appends a row
//! to on every tick. "Advisory mutates nothing" has always meant no USER
//! table, and it still does.
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
//! # A tick discovers, decides, and hands off -- it never does the work
//!
//! (Review finding 511.) `advisory_tick` is a *scheduling* pass, not an
//! execution pass. It costs one `list_namespaces` + one `list_tables` per
//! namespace, plus a `load_table` only for tables it has never seen or
//! whose cached properties have aged past
//! `scheduler.discovery_cache_ticks` ticks (default 5); the listing itself
//! is the change signal (a table that stops being listed is evicted). Every
//! due table is then handed to a bounded [`JoinSet`] -- concurrency
//! `scheduler.max_concurrent_jobs`, default 1, i.e. the same
//! one-job-at-a-time behaviour as before -- and the tick returns
//! immediately. A table whose previous job is still in flight is skipped
//! (not queued twice) on the following tick.
//!
//! Three consequences worth stating out loud:
//!
//! - A 40-minute rewrite no longer delays health reporting for any other
//!   table: it delays only the *permit*, and only when
//!   `max_concurrent_jobs` is exhausted.
//! - Both interval loops (`spawn` and `run_with_lease_renewal`) use
//!   `MissedTickBehavior::Skip`, so a long job is followed by ONE tick, not
//!   by a burst of every tick that was missed while it ran.
//! - Tests (and shutdown) need an explicit join point, because "the tick
//!   returned" no longer means "the work finished": see
//!   [`MaintenanceScheduler::drain_inflight_jobs`].
//!
//! The scheduler also runs [`crate::maintenance_log::compact_history`] on a
//! daily cadence (first tick after process start, then once per UTC day) so
//! the one-Parquet-file-per-row ledger does not grow without bound.
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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use croner::Cron;
use iceberg::{Catalog, TableIdent};
use sqe_core::config::{
    LeaseMode, MaintenanceCompactionConfig, MaintenanceConfig, MaintenanceMode,
};
use sqe_core::{Session, SqeError};
use sqe_sql::TableRef;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::maintenance_lease::{self, LeaseHandle};
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
            let session_catalog = Arc::new(
                sqe_catalog::SessionCatalog::for_session(&config, table_cache, &token).await?,
            );
            Ok(session_catalog.as_catalog() as Arc<dyn Catalog>)
        })
    })
}

/// Build a stable `holder_id` for this coordinator process's catalog HA
/// lease (Phase 4d Task 3): `"<hostname>-<pid>"` when the environment
/// exposes a hostname (checked via the `HOSTNAME` then `POD_NAME` env vars,
/// the same precedence `sqe-worker::advertise` uses for pod identity), a
/// fresh `crate::maintenance_lease::generate_holder_id` (v4 UUID) otherwise.
/// `std::process::id()` never fails, so the UUID fallback only triggers on
/// the "no hostname available" half of the pair, not the pid half.
///
/// Called exactly once, at [`MaintenanceScheduler::new`] -- see that
/// struct's doc comment for why this must stay stable for the whole process
/// lifetime rather than being recomputed per tick.
pub fn stable_holder_id() -> String {
    let hostname = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.trim().is_empty())
        .or_else(|| {
            std::env::var("POD_NAME")
                .ok()
                .filter(|h| !h.trim().is_empty())
        });
    match hostname {
        Some(h) => format!("{}-{}", h.trim(), std::process::id()),
        None => crate::maintenance_lease::generate_holder_id(),
    }
}

/// Lock a `std::sync::Mutex` guarding scheduler bookkeeping, recovering
/// from poisoning instead of propagating a panic.
///
/// Every mutex this is used on guards a plain cache/bookkeeping value whose
/// worst case after a panic is a stale entry (one extra `load_table`, one
/// extra ledger row, one extra job). Turning that into a panic in the tick
/// loop -- which the supervisor would then restart -- is strictly worse
/// than carrying on with possibly-stale bookkeeping. Same pattern as
/// `warned_invalid_schedules` below.
fn lock_or_recover<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One table's cached discovery result. See
/// [`MaintenanceScheduler::discover_tables`].
struct CachedTable {
    ident: TableIdent,
    props: HashMap<String, String>,
    loaded_at_tick: u64,
}

/// Cross-tick `(ident, properties)` cache keyed on `ident.to_string()`.
///
/// `tick` is a monotonically increasing counter of DISCOVERY passes, not
/// wall-clock: the TTL the operator configures is "N ticks", so counting
/// ticks is the honest unit, and it keeps the cache's behaviour identical
/// whether the scheduler is running on its real cadence or being driven a
/// tick at a time by a test.
#[derive(Default)]
struct DiscoveryCache {
    tick: u64,
    entries: HashMap<String, CachedTable>,
}

impl DiscoveryCache {
    /// Start a new discovery pass and return its tick number. Saturating,
    /// not wrapping: a wrapped counter would make every entry look
    /// arbitrarily fresh or stale exactly once every 2^64 ticks, and
    /// pinning the counter at `u64::MAX` (~584 billion years at a 1s tick)
    /// merely degrades to "always fresh".
    fn advance_tick(&mut self) -> u64 {
        self.tick = self.tick.saturating_add(1);
        self.tick
    }

    /// Evict every cached table that the catalog no longer lists. The
    /// listing is the change signal: a dropped (or renamed) table must not
    /// keep being scheduled off a cached property map.
    fn retain_listed(&mut self, listed: &[TableIdent]) {
        let live: HashSet<String> = listed.iter().map(|i| i.to_string()).collect();
        self.entries.retain(|name, _| live.contains(name));
    }

    /// True when `name`'s cached properties are younger than `ttl_ticks`
    /// discovery passes, i.e. may be reused without a `load_table`.
    fn is_fresh(&self, name: &str, tick: u64, ttl_ticks: u64) -> bool {
        self.entries
            .get(name)
            .is_some_and(|e| tick.saturating_sub(e.loaded_at_tick) < ttl_ticks)
    }

    fn insert(&mut self, ident: TableIdent, props: HashMap<String, String>) {
        let tick = self.tick;
        self.entries.insert(
            ident.to_string(),
            CachedTable {
                ident,
                props,
                loaded_at_tick: tick,
            },
        );
    }

    /// The two structures a tick works from, in the shape the pre-cache
    /// discovery loop produced them. Sorted by table name so a tick's
    /// evaluation order is deterministic rather than `HashMap`-iteration
    /// order (which decides who gets the first job permit).
    #[allow(clippy::type_complexity)]
    fn snapshot(
        &self,
    ) -> (
        Vec<(String, HashMap<String, String>)>,
        HashMap<String, TableIdent>,
    ) {
        let mut props_by_table: Vec<(String, HashMap<String, String>)> = self
            .entries
            .iter()
            .map(|(name, e)| (name.clone(), e.props.clone()))
            .collect();
        props_by_table.sort_by(|a, b| a.0.cmp(&b.0));
        let idents_by_table: HashMap<String, TableIdent> = self
            .entries
            .iter()
            .map(|(name, e)| (name.clone(), e.ident.clone()))
            .collect();
        (props_by_table, idents_by_table)
    }
}

/// The last `skipped` ledger row written per table, as
/// `(schedule_window_start_ms, reason)`.
///
/// A table with no compaction debt is skipped on EVERY tick it is due, and
/// each skip used to append one Parquet file to `maintenance_log` (finding
/// 511's fourth arm). One row per table per schedule window per reason
/// keeps the information -- "this table was considered and had nothing to
/// do this window" -- without the file-per-tick growth.
///
/// Bounded by table count, not tick count: one entry per table, overwritten
/// in place.
#[derive(Default)]
struct SkipLedger {
    last: HashMap<String, (i64, String)>,
}

impl SkipLedger {
    /// True when this skip is new information (a different table, a
    /// different schedule window, or a different reason within the same
    /// window) and should therefore be written to the ledger.
    fn should_record(&mut self, table: &str, window_start_ms: i64, reason: &str) -> bool {
        match self.last.get(table) {
            Some((window, prev_reason))
                if *window == window_start_ms && prev_reason.as_str() == reason =>
            {
                false
            }
            _ => {
                self.last
                    .insert(table.to_string(), (window_start_ms, reason.to_string()));
                true
            }
        }
    }
}

/// Identity of one scheduled per-table job, as the three recorders
/// (`record_failed_job`, `record_skipped_job`,
/// `record_skipped_insufficient_workers`) need it.
///
/// Grouped rather than passed as four loose arguments so a caller cannot
/// hand one recorder this job's `job_id` and another its neighbour's
/// `started_at_ms`, and so adding the schedule window (finding 511) did not
/// push those signatures past readability.
struct JobRecord<'a> {
    job_id: &'a str,
    ident: &'a TableIdent,
    /// When the job started, for the ledger row's `started_at_ms`.
    started_at_ms: i64,
    /// The schedule window the job was scheduled FOR: the `skipped`-row
    /// dedup key, never a scheduling input. See [`schedule_window_start_ms`].
    window_start_ms: i64,
}

/// RAII claim on a job key in the scheduler's in-flight set: released on
/// drop, which covers a normal return, a panic inside the job, and a
/// `JoinSet::abort_all` at shutdown alike.
struct InflightGuard {
    set: Arc<Mutex<HashSet<String>>>,
    key: String,
}

impl InflightGuard {
    /// `Some(guard)` when `key` was not already in flight, `None` when it
    /// was (the caller must then skip, not queue).
    fn try_claim(set: &Arc<Mutex<HashSet<String>>>, key: &str) -> Option<Self> {
        if !lock_or_recover(set).insert(key.to_string()) {
            return None;
        }
        Some(Self {
            set: Arc::clone(set),
            key: key.to_string(),
        })
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        lock_or_recover(&self.set).remove(&self.key);
    }
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
///
/// `holder_id` (Phase 4d Task 3) is this coordinator process's identity for
/// the catalog HA lease (`crate::maintenance_lease`): [`stable_holder_id`]
/// computes it ONCE, here in `new`, and it is never regenerated per tick --
/// a holder_id that changed tick-to-tick would make `try_acquire`'s
/// still-mine/held-by-other decision in `crate::maintenance_lease::lease_acquirable`
/// see a *different* holder on every renewal attempt, breaking the very
/// identity the lease is keyed on. Override via [`Self::with_holder_id`]
/// (tests only; production always uses the `new`-time default).
pub struct MaintenanceScheduler {
    cfg: MaintenanceConfig,
    principal: Arc<MaintenancePrincipal>,
    metrics: Arc<sqe_metrics::MetricsRegistry>,
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    catalog_factory: CatalogFactory,
    handler: Arc<crate::maintenance::MaintenanceHandler>,
    holder_id: String,
    /// Cross-tick discovery cache (finding 511): `(ident, properties)` per
    /// discovered table, refreshed at most once every
    /// `scheduler.discovery_cache_ticks` ticks.
    discovery: Arc<Mutex<DiscoveryCache>>,
    /// Job keys (table idents, plus [`RETENTION_JOB_KEY`]) whose job is
    /// spawned but not finished. A key in here is skipped, not queued
    /// again, by the next tick.
    inflight: Arc<Mutex<HashSet<String>>>,
    /// Last `skipped` ledger row written per table, so a table with no debt
    /// writes one row per schedule window instead of one per tick.
    skip_ledger: Arc<Mutex<SkipLedger>>,
    /// The bounded set every per-table job is spawned into. `tokio::sync::Mutex`
    /// (not `std`) because [`Self::drain_inflight_jobs`] holds it across an
    /// `await`.
    jobs: Arc<tokio::sync::Mutex<JoinSet<()>>>,
    /// `scheduler.max_concurrent_jobs` permits: what actually bounds the
    /// `JoinSet` above (a `JoinSet` on its own bounds nothing).
    job_permits: Arc<Semaphore>,
    /// UTC day number of the last `maintenance_log` retention pass, so it
    /// runs once a day rather than once a tick.
    last_retention_day: Arc<Mutex<Option<i64>>>,
}

/// In-flight job key for the daily `maintenance_log` retention pass. Not a
/// table ident (`ns.table`), and deliberately not parseable as one, so it
/// can never collide with a real table's key in `inflight`.
const RETENTION_JOB_KEY: &str = "<maintenance_log retention>";

/// In-flight key prefix of a stale transaction branch sweep; the table
/// name follows. Angle brackets keep it apart from table names.
const TXN_BRANCH_JOB_KEY: &str = "<txn branches>";

/// How much `maintenance_log` history the daily retention pass leaves
/// alone. Rows younger than this are never rewritten (they are the rows a
/// live lease CAS and a recent-history query care about); older rows are
/// rewritten into one file per UTC day. Not a config knob: the procedure
/// itself takes `retention_days` as a parameter, so an operator-driven
/// `CALL` can pass any value; this is only the scheduler's own cadence
/// default.
const HISTORY_RETENTION_DAYS: i64 = 7;

impl MaintenanceScheduler {
    pub fn new(
        cfg: MaintenanceConfig,
        principal: Arc<MaintenancePrincipal>,
        metrics: Arc<sqe_metrics::MetricsRegistry>,
        audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
        catalog_factory: CatalogFactory,
        handler: Arc<crate::maintenance::MaintenanceHandler>,
    ) -> Self {
        let max_concurrent_jobs = cfg.scheduler.max_concurrent_jobs.max(1);
        Self {
            cfg,
            principal,
            metrics,
            audit,
            catalog_factory,
            handler,
            holder_id: stable_holder_id(),
            discovery: Arc::new(Mutex::new(DiscoveryCache::default())),
            inflight: Arc::new(Mutex::new(HashSet::new())),
            skip_ledger: Arc::new(Mutex::new(SkipLedger::default())),
            jobs: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            job_permits: Arc::new(Semaphore::new(max_concurrent_jobs)),
            last_retention_day: Arc::new(Mutex::new(None)),
        }
    }

    /// A handle to the same scheduler for one spawned job: every field is
    /// an `Arc` or a cheap clone, and every piece of MUTABLE state
    /// (`discovery`, `inflight`, `skip_ledger`, `jobs`, `job_permits`,
    /// `last_retention_day`) is shared, not copied -- a job's clone and the
    /// tick loop's original are the same scheduler, seen from two tasks.
    ///
    /// Deliberately private and deliberately not a `Clone` impl: an
    /// external caller that cloned this expecting an INDEPENDENT scheduler
    /// would get one that silently shares the other's in-flight set and
    /// lease identity.
    fn clone_for_job(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            principal: Arc::clone(&self.principal),
            metrics: Arc::clone(&self.metrics),
            audit: self.audit.clone(),
            catalog_factory: Arc::clone(&self.catalog_factory),
            handler: Arc::clone(&self.handler),
            holder_id: self.holder_id.clone(),
            discovery: Arc::clone(&self.discovery),
            inflight: Arc::clone(&self.inflight),
            skip_ledger: Arc::clone(&self.skip_ledger),
            jobs: Arc::clone(&self.jobs),
            job_permits: Arc::clone(&self.job_permits),
            last_retention_day: Arc::clone(&self.last_retention_day),
        }
    }

    /// Override this scheduler's lease `holder_id`. Tests only (e.g.
    /// simulating two DIFFERENT coordinators sharing one state table by
    /// building two `MaintenanceScheduler`s with distinct holder ids);
    /// production code always keeps the stable per-process default `new`
    /// computes.
    #[cfg(any(test, feature = "test-sqlite"))]
    pub fn with_holder_id(mut self, holder_id: impl Into<String>) -> Self {
        self.holder_id = holder_id.into();
        self
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

        // Drop the bookkeeping for jobs that have already finished. Without
        // this the `JoinSet` keeps one completed entry per job forever
        // (nothing else ever joins them in production, where
        // `drain_inflight_jobs` only runs at shutdown).
        self.reap_finished_jobs().await;

        let now_ms = chrono::Utc::now().timestamp_millis();
        self.maybe_spawn_history_retention(&catalog, now_ms).await;

        let (props_by_table, idents_by_table, with_stale_txn_branches) =
            self.discover_tables(&catalog).await?;
        let enabled = select_enabled(&props_by_table);
        // Dropping a ref commits to a user table: Active mode and opted-in
        // tables only, like a rewrite. Other tables have
        // `CALL system.expire_snapshots`.
        if self.cfg.mode == MaintenanceMode::Active {
            let opted_in: HashSet<&str> = enabled.iter().map(String::as_str).collect();
            let sweep = with_stale_txn_branches
                .into_iter()
                .filter(|ident| opted_in.contains(ident.to_string().as_str()))
                .collect();
            self.spawn_txn_branch_sweeps(&catalog, sweep, now_ms).await;
        }
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
            let table_props = props_by_name
                .get(name.as_str())
                .copied()
                .unwrap_or(&empty_props);
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
            // The schedule window this table is due FOR (the cron's fire
            // instant, not "now"): the dedup key for `skipped` ledger rows,
            // so a table with no debt writes one row per window instead of
            // one per tick. `None` (invalid cron) cannot happen here --
            // `table_due` already returned false for that -- but falling
            // back to `now_ms` degrades to "never dedup", never to "dedup
            // two different windows together".
            let window_start_ms =
                schedule_window_start_ms(&name, &schedule, self.cfg.scheduler.jitter_secs, now_ms)
                    .unwrap_or(now_ms);

            // One job per table at a time. A table whose previous job is
            // still running is skipped, NOT queued a second time: queueing
            // would let a table that takes longer than its schedule period
            // build an unbounded backlog of jobs that each compact the same
            // debt the one before it already handled.
            let Some(inflight_guard) = InflightGuard::try_claim(&self.inflight, &name) else {
                debug!(
                    table = %name,
                    "advisory_tick: previous job for this table is still in flight; skipping this tick"
                );
                continue;
            };

            // Hand off and move on. The tick's job is to DECIDE; the
            // `JoinSet` (bounded by `job_permits`) does the work, so a slow
            // rewrite delays neither this tick's remaining tables nor the
            // next tick (finding 511).
            let job = self.clone_for_job();
            let job_catalog = Arc::clone(&catalog);
            let job_ident = ident.clone();
            let job_id = job_id.clone();
            let job_name = name.clone();
            let permits = Arc::clone(&self.job_permits);
            self.jobs.lock().await.spawn(async move {
                // Held for the whole job: dropping it (normal return, panic,
                // or abort) is what frees the table for a later tick.
                let _inflight_guard = inflight_guard;
                // Acquire AFTER spawning, not before: `max_concurrent_jobs`
                // bounds how many jobs RUN at once, and the tick must not
                // block waiting for a permit on a table's behalf.
                let Ok(_permit) = permits.acquire().await else {
                    // Semaphore closed: the scheduler is shutting down.
                    return;
                };
                // Active mode compacts (Phase 4b); every other mode (today
                // only `Advisory` reaches this loop -- `Off` never
                // constructs a scheduler at all) keeps the pre-4b read-only
                // report. This is the entire "advisory/off unchanged"
                // boundary: `active_one_table` is the ONLY place in this
                // file that ever commits to a user table.
                match job.cfg.mode {
                    MaintenanceMode::Active => {
                        job.active_one_table(&job_catalog, &job_ident, now_ms, window_start_ms)
                            .await;
                    }
                    _ => {
                        if let Err(e) = job
                            .analyze_one_table(&job_catalog, &job_ident, &job_id, now_ms)
                            .await
                        {
                            warn!(
                                table = %job_name,
                                error = %e,
                                "advisory_tick: per-table analysis failed; continuing with other tables"
                            );
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Discover `(name, properties)` and `name -> ident` for every table
    /// currently listed under the default warehouse, re-reading properties
    /// from the catalog only when they are missing or stale.
    ///
    /// The listing (`list_namespaces` + `list_tables` per namespace) runs
    /// every tick and is the change signal: a newly listed table is loaded
    /// on the spot, a table that stops being listed is evicted from the
    /// cache. What does NOT run every tick is `load_table`: a table whose
    /// cached entry is younger than `scheduler.discovery_cache_ticks` ticks
    /// reuses its cached properties. That is the difference between one
    /// metadata.json GET per table per tick and one per table per N ticks,
    /// per coordinator (finding 511).
    ///
    /// Staleness window, stated plainly: an operator who flips
    /// `sqe.maintenance.enabled` (or a per-table `sqe.maintenance.*`
    /// override) is picked up within `discovery_cache_ticks` ticks, not on
    /// the next tick. Every property this cache serves only ever decides
    /// WHETHER and WHEN to schedule a job; the job itself re-loads the
    /// table (`analyze_one_table`/`active_one_table` both call
    /// `load_table`) and re-resolves its overrides from that fresh copy, so
    /// no rewrite ever runs with stale parameters.
    ///
    /// A `load_table` failure keeps whatever entry is already cached (the
    /// table is still listed, so it still exists) and is otherwise the same
    /// warn-and-skip as before: a table that has never loaded successfully
    /// is simply absent from this tick's result.
    async fn discover_tables(
        &self,
        catalog: &Arc<dyn Catalog>,
    ) -> sqe_core::Result<(
        Vec<(String, HashMap<String, String>)>,
        HashMap<String, TableIdent>,
        Vec<TableIdent>,
    )> {
        let namespaces = catalog.list_namespaces(None).await.map_err(|e| {
            SqeError::Catalog(format!("advisory_tick: failed to list namespaces: {e}"))
        })?;

        let mut listed: Vec<TableIdent> = Vec::new();
        for ns in &namespaces {
            match catalog.list_tables(ns).await {
                Ok(tables) => listed.extend(tables),
                Err(e) => {
                    warn!(
                        namespace = %ns,
                        error = %e,
                        "advisory_tick: failed to list tables; skipping namespace"
                    );
                }
            }
        }

        let ttl_ticks = self.cfg.scheduler.discovery_cache_ticks.max(1);
        let stale: Vec<TableIdent> = {
            let mut cache = lock_or_recover(&self.discovery);
            let tick = cache.advance_tick();
            cache.retain_listed(&listed);
            listed
                .iter()
                .filter(|ident| !cache.is_fresh(&ident.to_string(), tick, ttl_ticks))
                .cloned()
                .collect()
        };

        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut loaded: Vec<(TableIdent, HashMap<String, String>)> = Vec::new();
        let mut with_stale_txn_branches = Vec::new();
        for ident in stale {
            match crate::maintenance::load_table(catalog, &ident).await {
                Ok(table) => {
                    if !sqe_catalog::txn::stale_txn_refs(table.metadata(), now_ms).is_empty() {
                        with_stale_txn_branches.push(ident.clone());
                    }
                    loaded.push((ident, table.metadata().properties().clone()))
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

        let mut cache = lock_or_recover(&self.discovery);
        for (ident, props) in loaded {
            cache.insert(ident, props);
        }
        let (props_by_table, idents_by_table) = cache.snapshot();
        Ok((props_by_table, idents_by_table, with_stale_txn_branches))
    }

    /// Drop the transaction branches a crashed coordinator left on
    /// `idents` ([`sqe_catalog::txn::stale_txn_refs`]), one background job
    /// per table. Like the retention pass it takes no `job_permits` permit:
    /// a ref drop is one metadata commit. The in-flight set keeps one job
    /// per table.
    async fn spawn_txn_branch_sweeps(
        &self,
        catalog: &Arc<dyn Catalog>,
        idents: Vec<TableIdent>,
        now_ms: i64,
    ) {
        for ident in idents {
            let key = format!("{TXN_BRANCH_JOB_KEY} {ident}");
            let Some(guard) = InflightGuard::try_claim(&self.inflight, &key) else {
                continue;
            };
            let catalog = Arc::clone(catalog);
            self.jobs.lock().await.spawn(async move {
                let _guard = guard;
                if let Err(e) =
                    sqe_catalog::txn::drop_stale_branches(catalog.as_ref(), &ident, now_ms).await
                {
                    warn!(
                        table = %ident,
                        error = %e,
                        "stale transaction branch sweep failed (best-effort, not fatal)"
                    );
                }
            });
        }
    }

    /// Join every job this scheduler has spawned and is still running.
    ///
    /// Since finding 511 a returned `advisory_tick` means "the work is
    /// scheduled", not "the work is done". Callers that need the stronger
    /// statement -- tests asserting on a compaction's effects, and a
    /// graceful shutdown that would rather let a rewrite finish than abort
    /// it mid-commit -- call this afterwards.
    pub async fn drain_inflight_jobs(&self) {
        let mut jobs = self.jobs.lock().await;
        while jobs.join_next().await.is_some() {}
    }

    /// Abort every in-flight job. Used on cancellation, where the previous
    /// (inline) implementation dropped a half-finished rewrite the same way
    /// when the tick future itself was cancelled.
    async fn abort_inflight_jobs(&self) {
        self.jobs.lock().await.abort_all();
    }

    /// Reap already-finished jobs without blocking on the running ones.
    async fn reap_finished_jobs(&self) {
        let mut jobs = self.jobs.lock().await;
        while jobs.try_join_next().is_some() {}
    }

    /// Spawn the `maintenance_log` retention pass at most once per UTC day
    /// (finding 511's ledger arm).
    ///
    /// Deliberately does NOT take a `job_permits` permit: with the default
    /// `max_concurrent_jobs = 1` that would make a control-plane
    /// housekeeping pass block the very compaction work the scheduler
    /// exists to run. It is guarded by the same in-flight set as a table
    /// job (under [`RETENTION_JOB_KEY`]), so a slow pass is never started
    /// twice.
    ///
    /// Multi-coordinator safe without a lease: the rewrite commit is a
    /// `check_file_existence` CAS, so a second coordinator running the same
    /// pass concurrently loses cleanly and no-ops (see
    /// [`crate::maintenance_log::compact_history`]).
    async fn maybe_spawn_history_retention(&self, catalog: &Arc<dyn Catalog>, now_ms: i64) {
        let today = now_ms.div_euclid(86_400_000);
        {
            let mut last = lock_or_recover(&self.last_retention_day);
            if *last == Some(today) {
                return;
            }
            *last = Some(today);
        }
        let Some(guard) = InflightGuard::try_claim(&self.inflight, RETENTION_JOB_KEY) else {
            return;
        };

        let catalog = Arc::clone(catalog);
        let state_table = self.cfg.scheduler.state_table.clone();
        self.jobs.lock().await.spawn(async move {
            let _guard = guard;
            match maintenance_log::compact_history(
                &catalog,
                &state_table,
                HISTORY_RETENTION_DAYS,
                now_ms,
            )
            .await
            {
                Ok(outcome) if outcome.is_noop() => {
                    debug!(
                        state_table = %state_table,
                        "maintenance_log retention: nothing to compact"
                    );
                }
                Ok(outcome) => {
                    info!(
                        state_table = %state_table,
                        files_in = outcome.files_in,
                        files_out = outcome.files_out,
                        rows = outcome.rows,
                        days = outcome.days,
                        "maintenance_log retention: rewrote aged ledger rows into one file per day"
                    );
                }
                Err(e) => {
                    warn!(
                        state_table = %state_table,
                        error = %e,
                        "maintenance_log retention failed (best-effort, not fatal)"
                    );
                }
            }
        });
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
        let last_compaction_ms = crate::maintenance::last_compaction_snapshot_ms(&table);

        let health = crate::table_health::analyze_table_health(
            &data_files,
            &delete_files,
            &tasks_by_path,
            &self.cfg.compaction,
            table.metadata().properties(),
            last_compaction_ms,
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

        let row =
            crate::maintenance_log::advisory_row(&name, &self.principal.user_id, &health, now_ms);
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
    /// mode), then either skips (no eligible debt, recorded via
    /// [`record_skipped_job`]: log row, metric, and audit event) or runs a
    /// real `rewrite_data_files` commit through `self.handler` -- the SAME
    /// method `CALL system.rewrite_data_files` uses -- under a freshly
    /// minted maintenance session.
    ///
    /// Infallible by design: every failure path from the eligibility check
    /// onward (mint, write-privilege check, refresh's own commit-catalog
    /// build, or the rewrite itself) is caught here, turned into a `failed`
    /// `maintenance_log` row, a `sqe_maintenance_job_total` sample, AND
    /// (Phase 4b review, Fix 1) an `AuditKind::Maintenance` audit event
    /// carrying the failure reason, via [`record_failed_job`], then
    /// swallowed. The two EARLIER failure points, `load_table` and
    /// `collect_health_inputs`, happen before `job_id`/`started_at_ms` even
    /// exist; those are logged and skipped (matching pre-4b behavior) with
    /// no log row, metric, or audit event, since there is no job to
    /// attribute one to. One table's compaction failing must never abort
    /// the tick or block any other opted-in table from being considered.
    ///
    /// `window_start_ms` is the schedule window this job was scheduled for
    /// (the cron fire instant, from [`schedule_window_start_ms`]); it is
    /// only ever used as the dedup key for `skipped` ledger rows, never to
    /// decide whether to compact.
    async fn active_one_table(
        &self,
        catalog: &Arc<dyn Catalog>,
        ident: &TableIdent,
        now_ms: i64,
        window_start_ms: i64,
    ) {
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

        // Resolve per-table overrides BEFORE computing health: four of the
        // five overridable knobs (target_file_size_bytes, min_input_files,
        // delete_file_threshold, delete_ratio_threshold) directly determine
        // eligibility. Computing
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
            delete_ratio_threshold: params.delete_ratio_threshold,
            strategy: params.strategy.clone(),
        };

        let last_compaction_ms = crate::maintenance::last_compaction_snapshot_ms(&table);
        let health = crate::table_health::analyze_table_health(
            &data_files,
            &delete_files,
            &tasks_by_path,
            &effective_cfg,
            table.metadata().properties(),
            last_compaction_ms,
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
        // Everything the ledger/metric/audit recorders below need to
        // identify this job, gathered once so the four of them cannot be
        // handed inconsistent pieces of it.
        let rec = JobRecord {
            job_id: &job_id,
            ident,
            started_at_ms,
            window_start_ms,
        };

        // Eligibility: small-file debt (eligible_groups) OR ANY delete-heavy
        // file. `>= delete_file_threshold` (a per-file delete-COUNT
        // threshold) does not apply here -- `delete_heavy_files` already
        // is a file COUNT that met that threshold; requiring it to also
        // clear the threshold would skip a table with exactly one
        // delete-heavy file under the (very common) default threshold of 2.
        let has_eligible_work = health.eligible_groups > 0 || health.delete_heavy_files > 0;
        if !has_eligible_work {
            info!(table = %name, "active_tick: skipped, no eligible compaction debt");
            self.record_skipped_job(catalog, &rec, "no eligible compaction debt")
                .await;
            return;
        }

        let mut session = match self.principal.mint_session(&job_id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(table = %name, error = %e, "active_tick: failed to mint maintenance session");
                self.record_failed_job(catalog, &rec, &e.to_string()).await;
                return;
            }
        };

        // Defensive: `MaintenancePrincipal::session_from_identity` always
        // sets `has_maintenance_authority(true)`, but a compaction commit
        // is exactly the kind of consequential action that must not
        // silently proceed if that ever regresses.
        if !session.has_maintenance_authority() {
            warn!(
                table = %name,
                "active_tick: minted maintenance session unexpectedly lacks write privilege; refusing to compact"
            );
            self.record_failed_job(
                catalog,
                &rec,
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
                self.record_failed_job(catalog, &rec, &e.to_string()).await;
                return;
            }
        };

        let table_ref = match table_ref_from_ident(ident) {
            Ok(t) => t,
            Err(e) => {
                warn!(table = %name, error = %e, "active_tick: failed to build table reference");
                self.record_failed_job(catalog, &rec, &e.to_string()).await;
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
            (
                "sqe.maintenance.principal".to_string(),
                self.principal.user_id.clone(),
            ),
            (
                "sqe.maintenance.trigger".to_string(),
                "scheduled".to_string(),
            ),
        ]);

        // Phase 4c Task 5: `distribution.mode` routing. `resolve_execution`
        // is the SAME pure function `handle()`'s manual `CALL` arm uses, fed
        // by the SAME `healthy_worker_count()` accessor on `self.handler`
        // (the only place a worker registry is attached), so a scheduled
        // tick and an interactive `CALL` can never disagree about whether
        // the fleet clears the floor right now.
        //
        // `require` below the floor is a LOUD skip here, never a silent
        // fallback to `Local`: a `skipped` maintenance_log row, the existing
        // `AuditKind::Maintenance` skip event (via `record_skipped_job`), and
        // a dedicated `sqe_maintenance_skipped_total{reason="insufficient_workers"}`
        // sample an operator can alert on independently of the generic
        // `sqe_maintenance_job_total{status="skipped"}` counter (which also
        // fires for "no eligible debt").
        let dist = &self.cfg.distribution;
        let healthy = self.handler.healthy_worker_count().await;
        let plan = crate::maintenance::resolve_execution(dist.mode, healthy, dist.min_workers);
        if matches!(
            plan,
            crate::maintenance::ExecutionPlan::SkipInsufficientWorkers
        ) {
            warn!(
                table = %name,
                healthy_workers = healthy,
                min_workers = dist.min_workers,
                "active_tick: skipped, distribution.mode=require but the fleet is below min_workers"
            );
            self.record_skipped_insufficient_workers(catalog, &rec, healthy, dist.min_workers)
                .await;
            return;
        }

        // Phase 4d Task 3: acquire the HA lease immediately before the ONE
        // expensive step this function performs (the rewrite dispatch right
        // below), not any earlier -- the eligibility check, mint, refresh,
        // and commit-catalog build above are all cheap or already needed
        // regardless of which coordinator ends up doing the work. `job_key`
        // is `name` (the table ident string), matching the brief and
        // `crate::maintenance_lease`'s module docs.
        //
        // Correctness never depends on this lease: Iceberg's optimistic
        // commit (`Transaction::commit`) already prevents two coordinators
        // from double-committing a rewrite for the same table -- see
        // `crate::maintenance_lease`'s module docs. This step exists only to
        // avoid a second coordinator also paying for the (expensive)
        // scan+rewrite+encode work Iceberg would throw away anyway.
        let lease_handle = match self.cfg.scheduler.lease {
            LeaseMode::None => None,
            // `Kubernetes` is rejected at config-validation startup
            // (`SqeConfig::validate`) when the scheduler is enabled -- Phase
            // 4d only implements the catalog-native backend. Treat it the
            // same as `Catalog` here defensively (validate() should already
            // have refused to start), rather than silently running unleased.
            LeaseMode::Catalog | LeaseMode::Kubernetes => {
                let acquire_now_ms = chrono::Utc::now().timestamp_millis();
                match maintenance_lease::try_acquire(
                    catalog,
                    &self.cfg.scheduler.state_table,
                    &name,
                    &self.holder_id,
                    self.cfg.scheduler.lease_ttl_secs,
                    acquire_now_ms,
                )
                .await
                {
                    Ok(Some(handle)) => {
                        if let Some(audit) = &self.audit {
                            // A steal (this acquire won by claiming a
                            // DIFFERENT holder's expired lease) is audited as
                            // its own distinct action, not folded into a
                            // plain "acquired" event -- "acquire / steal /
                            // release" are three things the brief asks to be
                            // able to tell apart in the audit trail, and
                            // `LeaseHandle::stolen_from` (Task 2's
                            // `try_acquire`, surfaced for exactly this) is
                            // what makes that possible without re-deriving
                            // it here.
                            let action = if handle.stolen_from.is_some() {
                                "stolen"
                            } else {
                                "acquired"
                            };
                            audit.log_event(build_lease_audit_event(
                                ident,
                                &self.principal.user_id,
                                &job_id,
                                action,
                                &handle,
                            ));
                        }
                        Some(handle)
                    }
                    Ok(None) => {
                        // Routine under multi-coordinator deployments: NOT a
                        // failure, NOT a `maintenance_log` job row (no job
                        // was even attempted, so there is nothing job-shaped
                        // to log) -- just a debug log and a dedicated
                        // counter an operator can watch to confirm the lease
                        // is doing its job.
                        debug!(
                            table = %name,
                            holder_id = %self.holder_id,
                            "active_tick: another coordinator holds the catalog lease for this table; skipping this tick"
                        );
                        self.metrics.maintenance_lease_skipped_total.inc();
                        return;
                    }
                    Err(e) => {
                        // A lease *operational* failure (state table
                        // missing/unreadable, catalog hiccup) must never
                        // block an otherwise-eligible compaction -- the
                        // lease is advisory, not a correctness or
                        // availability gate. Proceed unleased, best-effort,
                        // exactly as `lease = "none"` would.
                        warn!(
                            table = %name,
                            error = %e,
                            "active_tick: lease acquisition failed; proceeding without it \
                             (best-effort, the lease is never a correctness gate)"
                        );
                        None
                    }
                }
            }
        };

        let rewrite_future: Pin<
            Box<
                dyn Future<Output = sqe_core::Result<crate::maintenance::RewriteOutcome>>
                    + Send
                    + '_,
            >,
        > = match plan {
            crate::maintenance::ExecutionPlan::SkipInsufficientWorkers => {
                unreachable!("handled above, before the lease was acquired")
            }
            crate::maintenance::ExecutionPlan::Local => Box::pin(self.handler.rewrite_data_files(
                &commit_catalog,
                &table_ref,
                Some(params.target_file_size_bytes),
                Some(params.min_input_files),
                None, // max_concurrent_file_group_rewrites: handler default; not part of the Phase 4b per-table override surface.
                strategy,
                None, // sort_order: not part of the Phase 4b per-table override surface.
                Some(params.delete_file_threshold),
                Some(params.delete_ratio_threshold),
                false, // rewrite_all: not part of the Phase 4b per-table override surface; the scheduler never forces a full-table rewrite.
                Some(snapshot_properties),
            )),
            crate::maintenance::ExecutionPlan::Distributed => {
                Box::pin(self.handler.rewrite_data_files_distributed_with_defaults(
                    &commit_catalog,
                    &table_ref,
                    &job_id,
                    Some(params.target_file_size_bytes),
                    Some(params.min_input_files),
                    strategy,
                    None, // sort_order: not part of the Phase 4b per-table override surface.
                    Some(params.delete_file_threshold),
                    Some(params.delete_ratio_threshold),
                    false, // rewrite_all: not part of the Phase 4b per-table override surface; the scheduler never forces a full-table rewrite.
                    Some(snapshot_properties),
                ))
            }
        };

        let (result, released_handle) = match lease_handle {
            Some(handle) => {
                let (result, handle) = run_with_lease_renewal(
                    rewrite_future,
                    handle,
                    Arc::clone(&commit_catalog),
                    self.cfg.scheduler.state_table.clone(),
                    self.cfg.scheduler.lease_ttl_secs,
                )
                .await;
                (result, Some(handle))
            }
            None => (rewrite_future.await, None),
        };

        // Always release -- success or failure -- so the lease never
        // lingers to TTL once this coordinator is done with the table.
        // Best-effort: a release failure is logged and swallowed, same as
        // every other lease operation (see the module docs).
        if let Some(handle) = released_handle {
            let release_now_ms = chrono::Utc::now().timestamp_millis();
            let handle_for_audit = handle.clone();
            // Use `commit_catalog` (the refreshed per-job catalog), not the
            // tick-level discovery `catalog`: `renew` (in
            // `run_with_lease_renewal`) already uses `commit_catalog`, and a
            // long job's discovery-time token can be stale by the time it
            // finishes, same rationale as the commit-catalog rebuild above.
            match maintenance_lease::release(
                handle,
                &commit_catalog,
                &self.cfg.scheduler.state_table,
                release_now_ms,
            )
            .await
            {
                Ok(()) => {
                    if let Some(audit) = &self.audit {
                        audit.log_event(build_lease_audit_event(
                            ident,
                            &self.principal.user_id,
                            &job_id,
                            "released",
                            &handle_for_audit,
                        ));
                    }
                }
                Err(e) => {
                    warn!(
                        table = %name,
                        error = %e,
                        "active_tick: lease release failed (best-effort; the claim will expire via ttl)"
                    );
                }
            }
        }

        let finished_at_ms = chrono::Utc::now().timestamp_millis();
        match result {
            Ok(outcome) if outcome.skipped_reason.is_some() => {
                let reason = outcome.skipped_reason.unwrap_or_default();
                info!(table = %name, reason = %reason, "active_tick: rewrite_data_files skipped");
                self.record_skipped_job(catalog, &rec, &reason).await;
            }
            Ok(outcome) if outcome.partial => {
                // Phase 4d Task 3: `distribution.partial_progress` opted in
                // and a terminal group failure hit after some batches had
                // already committed. This is neither a clean success nor a
                // job that committed nothing -- record it distinctly so an
                // operator scanning `maintenance_log` can tell "fully
                // compacted" from "partially compacted, needs a re-run" at
                // a glance, and so `success`'s job-total counter never
                // silently absorbs a job that did not finish.
                let reason = outcome.partial_error.clone().unwrap_or_default();
                warn!(
                    table = %name,
                    files_in = outcome.files_in,
                    files_out = outcome.files_out,
                    bytes_out = outcome.bytes_out,
                    rows_removed = outcome.rows_removed,
                    snapshot_id = ?outcome.snapshot_id,
                    reason = %reason,
                    "active_tick: distributed rewrite committed partial progress before a \
                     terminal group failure"
                );
                if let Some(audit) = &self.audit {
                    audit.log_event(build_partial_audit_event(
                        ident,
                        &self.principal.user_id,
                        &job_id,
                        &outcome,
                        &reason,
                    ));
                }
                let row = maintenance_log::partial_row(
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
                    &reason,
                );
                self.append_job_row(catalog, &row).await;
                self.metrics
                    .maintenance_job_total
                    .with_label_values(&["partial"])
                    .inc();
                self.metrics
                    .maintenance_bytes_rewritten_total
                    .inc_by(outcome.bytes_out.max(0) as u64);
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
                self.record_failed_job(catalog, &rec, &e.to_string()).await;
            }
        }
    }

    /// Build and append a `failed` `maintenance_log` row plus its metric
    /// sample, and (Design doc-6 review) emit the matching
    /// `AuditKind::Maintenance` audit event. Shared by every
    /// `active_one_table` error path so the job-total counter, the ledger,
    /// and the audit trail can never disagree about whether a job counted
    /// as failed.
    async fn record_failed_job(
        &self,
        catalog: &Arc<dyn Catalog>,
        rec: &JobRecord<'_>,
        error: &str,
    ) {
        let name = rec.ident.to_string();
        let row = maintenance_log::failed_row(
            rec.job_id,
            &name,
            &self.principal.user_id,
            rec.started_at_ms,
            chrono::Utc::now().timestamp_millis(),
            error,
        );
        self.append_job_row(catalog, &row).await;
        self.metrics
            .maintenance_job_total
            .with_label_values(&["failed"])
            .inc();
        if let Some(audit) = &self.audit {
            audit.log_event(build_failed_audit_event(
                rec.ident,
                &self.principal.user_id,
                rec.job_id,
                error,
            ));
        }
    }

    /// Build and append a `skipped` `maintenance_log` row plus its metric
    /// sample and matching `AuditKind::Maintenance` audit event. Shared by
    /// both `active_one_table` skip paths (no eligible debt, and
    /// `rewrite_data_files` itself reporting `skipped_reason`) so the
    /// job-total counter, the ledger, and the audit trail can never
    /// disagree about whether a job counted as skipped.
    ///
    /// # One ledger row per table per schedule window (finding 511)
    ///
    /// A table with no compaction debt is skipped on every tick it is due.
    /// The ledger append is one Parquet FILE per row, so at `tick_secs = 60`
    /// that was 1,440 files per idle table per day, forever, in the same
    /// table the HA lease reads on the critical path of every compaction.
    /// The row is therefore written only when this skip is new information:
    /// a different `window_start_ms` (the schedule window the job was
    /// scheduled for) or a different `reason` than the last row this
    /// process wrote for this table.
    ///
    /// The metric and the audit event are deliberately NOT deduped. Those
    /// two are bounded-cost, aggregate-shaped signals ("how often did we
    /// decide to skip", "what did the maintenance principal do") that an
    /// operator reads as rates; the ledger row is the unbounded, one-file-
    /// per-write one. The counter therefore counts skip DECISIONS while the
    /// ledger records skip WINDOWS -- they can differ by design, and that
    /// difference is the fix, not a disagreement.
    ///
    /// The dedup state is per process: a coordinator restart re-writes one
    /// row per table per window, which is the correct trade (bounded by
    /// restarts, and a restart is exactly when an operator wants to see the
    /// scheduler is alive).
    async fn record_skipped_job(
        &self,
        catalog: &Arc<dyn Catalog>,
        rec: &JobRecord<'_>,
        reason: &str,
    ) {
        let name = rec.ident.to_string();
        let record_row =
            lock_or_recover(&self.skip_ledger).should_record(&name, rec.window_start_ms, reason);
        if record_row {
            let row = maintenance_log::skipped_row(
                rec.job_id,
                &name,
                &self.principal.user_id,
                rec.started_at_ms,
                reason,
            );
            self.append_job_row(catalog, &row).await;
        } else {
            debug!(
                table = %name,
                reason = %reason,
                "active_tick: same skip already recorded for this schedule window; not appending a duplicate ledger row"
            );
        }
        self.metrics
            .maintenance_job_total
            .with_label_values(&["skipped"])
            .inc();
        if let Some(audit) = &self.audit {
            audit.log_event(build_skipped_audit_event(
                rec.ident,
                &self.principal.user_id,
                rec.job_id,
                reason,
            ));
        }
    }

    /// Record a `require`-mode skip caused by the fleet being below
    /// `min_workers` healthy (Phase 4c Task 5). Delegates the log row, the
    /// generic `sqe_maintenance_job_total{status="skipped"}` sample, and the
    /// audit event to [`Self::record_skipped_job`] (so a "skip" always looks
    /// the same shape regardless of cause), then ALSO increments the
    /// dedicated `sqe_maintenance_skipped_total{reason="insufficient_workers"}`
    /// counter -- a signal an operator can alert on specifically ("my fleet
    /// fell below the floor I required") without conflating it with the
    /// unrelated "table had no eligible compaction debt" skip.
    async fn record_skipped_insufficient_workers(
        &self,
        catalog: &Arc<dyn Catalog>,
        rec: &JobRecord<'_>,
        healthy_workers: usize,
        min_workers: usize,
    ) {
        let reason = format!(
            "insufficient_workers: distribution.mode=require needs >= {min_workers} \
             healthy workers, only {healthy_workers} are healthy"
        );
        self.record_skipped_job(catalog, rec, &reason).await;
        self.metrics
            .maintenance_skipped_total
            .with_label_values(&["insufficient_workers"])
            .inc();
    }

    /// Best-effort `maintenance_log` append, shared by every active-mode
    /// terminal row. Mirrors `analyze_one_table`'s advisory-row append: a
    /// failure to write the ledger is logged, never propagated, and never
    /// blocks the next table.
    async fn append_job_row(
        &self,
        catalog: &Arc<dyn Catalog>,
        row: &maintenance_log::MaintenanceLogRow,
    ) {
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
            // `Skip`, not the default `Burst` (finding 511): if the loop is
            // ever held up past a tick boundary, resume on the next
            // scheduled tick instead of firing once per missed tick back to
            // back. A discovery pass is not free (a `list_tables` per
            // namespace, plus a `load_table` per newly seen or aged-out
            // table), and replaying it N times the instant a stall clears
            // makes this loop the noisiest tenant on the catalog exactly
            // when the catalog is recovering.
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
            // Jobs now outlive the tick that scheduled them, so cancelling
            // the loop no longer cancels them implicitly. Abort explicitly,
            // matching the pre-511 behaviour where cancelling the loop
            // dropped the in-progress tick (and with it its rewrite) --
            // Iceberg's optimistic commit means an aborted rewrite loses
            // work, never corrupts a table.
            self.abort_inflight_jobs().await;
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
                health.small_files,
                health.delete_files,
                health.eligible_groups,
                health.est_rewrite_bytes
            )),
            query_hash: sqe_metrics::audit::query_hash(&format!("maintenance-advisory:{}", ident)),
            statement_type: "maintenance_advisory".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        transaction: None,
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
                outcome.files_in,
                outcome.files_out,
                outcome.bytes_out,
                outcome.rows_removed,
                outcome.snapshot_id
            )),
            query_hash: sqe_metrics::audit::query_hash(&format!("maintenance-active:{}", ident)),
            statement_type: "maintenance_active".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        transaction: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    }
}

/// Build the `AuditKind::Maintenance` event for one table's active-mode
/// distributed rewrite that committed PARTIAL progress (Phase 4d Task 3:
/// `distribution.partial_progress` opted in, a terminal group failure hit
/// after one or more batches had already committed). `outcome` stays
/// `Outcome::Success` -- the job did commit real, correct work, just not
/// all of it -- but `query.text` carries both the committed counts and the
/// terminal failure's message, so this event is never confused with a
/// clean `build_active_audit_event` success by a reader scanning `text`
/// alone.
fn build_partial_audit_event(
    ident: &TableIdent,
    principal_user: &str,
    job_id: &str,
    outcome: &crate::maintenance::RewriteOutcome,
    reason: &str,
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
                "active_tick: rewrite_data_files partial: committed files_in={} files_out={} \
                 bytes_out={} rows_removed={} snapshot_id={:?}, then stopped: {reason}",
                outcome.files_in,
                outcome.files_out,
                outcome.bytes_out,
                outcome.rows_removed,
                outcome.snapshot_id
            )),
            query_hash: sqe_metrics::audit::query_hash(&format!(
                "maintenance-active-partial:{}",
                ident
            )),
            statement_type: "maintenance_active".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        transaction: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    }
}

/// Build the `AuditKind::Maintenance` event for one table's active-mode
/// compaction job that FAILED (Phase 4b review, design doc section 6:
/// failures and denials must be audited, not just metered and logged).
/// `Outcome::Failure` mirrors the shape `authorize_or_deny`'s denial event
/// uses (`crate::maintenance::authorize_or_deny`): an `error_type` tag plus
/// the human-readable `message`, so every failure this scheduler ever
/// records, an authorization refusal or a mint/refresh/commit error, looks
/// the same to a downstream SIEM regardless of which check tripped it.
fn build_failed_audit_event(
    ident: &TableIdent,
    principal_user: &str,
    job_id: &str,
    error: &str,
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
        outcome: sqe_metrics::audit::Outcome::Failure {
            error_type: Some("MaintenanceJobFailed".to_string()),
            error_code: None,
            message: Some(error.to_string()),
        },
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
            text: Some(format!("active_tick: rewrite_data_files failed: {error}")),
            query_hash: sqe_metrics::audit::query_hash(&format!(
                "maintenance-active-failed:{}",
                ident
            )),
            statement_type: "maintenance_active".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        transaction: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    }
}

/// Build the `AuditKind::Maintenance` event for one table's active-mode
/// compaction job that was SKIPPED (no eligible compaction debt, or
/// `rewrite_data_files` itself reported a `skipped_reason`). A skip is a
/// correct no-op decision, not a failure, so `outcome` stays
/// `Outcome::Success` (there is no `message` field on that variant); the
/// skip reason travels in `query.text` instead, the same place
/// [`build_active_audit_event`] puts its committed-outcome description.
fn build_skipped_audit_event(
    ident: &TableIdent,
    principal_user: &str,
    job_id: &str,
    reason: &str,
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
            text: Some(format!("active_tick: rewrite_data_files skipped: {reason}")),
            query_hash: sqe_metrics::audit::query_hash(&format!(
                "maintenance-active-skipped:{}",
                ident
            )),
            statement_type: "maintenance_active".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        transaction: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    }
}

/// Build the `AuditKind::Maintenance` event for a catalog HA lease acquire,
/// steal, or release (Phase 4d Task 3). Distinct from the job-outcome events
/// above: this fires around the lease bookkeeping itself (`action` is
/// `"acquired"`, `"stolen"`, or `"released"` -- the caller picks `"stolen"`
/// over `"acquired"` when `handle.stolen_from.is_some()`, and this function
/// always appends `stolen_from=<prev holder>` to the text when it is set,
/// regardless of `action`, so a steal is identifiable even on its later
/// `"released"` event), independent of whether the rewrite it protects goes
/// on to succeed, fail, or (in the `Ok(None)` "held by another coordinator"
/// case, which never reaches this function at all -- see `active_one_table`)
/// isn't attempted this tick.
fn build_lease_audit_event(
    ident: &TableIdent,
    principal_user: &str,
    job_id: &str,
    action: &str,
    handle: &LeaseHandle,
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
            text: Some(match &handle.stolen_from {
                Some(prev) => format!(
                    "active_tick: catalog lease {action}: holder={} expires_at_ms={} stolen_from={prev}",
                    handle.holder_id, handle.expires_at_ms
                ),
                None => format!(
                    "active_tick: catalog lease {action}: holder={} expires_at_ms={}",
                    handle.holder_id, handle.expires_at_ms
                ),
            }),
            query_hash: sqe_metrics::audit::query_hash(&format!("maintenance-lease-{action}:{}", ident)),
            statement_type: "maintenance_lease".to_string(),
        }),
        session_id: Some(job_id.to_string()),
        client_ip: None,
        trace_id: None,
        query_id: None,
        transaction: None,
        integrity: sqe_metrics::audit::Integrity::default(),
    }
}

/// Run `fut` to completion, renewing `handle`'s lease at roughly half its
/// TTL for as long as `fut` is still running (Phase 4d Task 3: "renew before
/// the commit if the job is long"). A long-running `rewrite_data_files`/
/// `rewrite_data_files_distributed_with_defaults` call never yields a
/// natural "about to commit" checkpoint back to this module, so this races
/// the compaction future against a renewal ticker instead of hooking a
/// specific pre-commit point -- functionally equivalent (the lease never
/// goes more than ~ttl/2 without a fresh renewal while the job runs) without
/// requiring a callback into `MaintenanceHandler`.
///
/// Best-effort, like every other lease operation: a renewal failure (lost
/// the race to another coordinator's steal, transient catalog conflict) is
/// logged and swallowed, NEVER propagated into `fut`'s result or used to
/// cancel it -- the lease is advisory (see `crate::maintenance_lease`'s
/// module docs), so losing it mid-job must not abort an already-in-flight
/// rewrite that Iceberg's own optimistic commit will arbitrate correctly
/// regardless.
async fn run_with_lease_renewal<'a, T>(
    fut: Pin<Box<dyn Future<Output = sqe_core::Result<T>> + Send + 'a>>,
    mut handle: LeaseHandle,
    catalog: Arc<dyn Catalog>,
    state_table: String,
    ttl_secs: u64,
) -> (sqe_core::Result<T>, LeaseHandle) {
    let mut fut = fut;
    let renew_every = std::time::Duration::from_secs((ttl_secs / 2).max(1));
    let mut ticker = tokio::time::interval(renew_every);
    // `Skip`, not the default `Burst` (finding 511): a renewal that runs
    // long (a slow catalog commit) must not be followed by a burst of
    // back-to-back renewals, each of which is a real CAS commit against the
    // shared state table. One renewal per half-TTL period is the whole
    // point; catching up on missed ones renews nothing extra.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await; // the first tick fires immediately; consume it so renewal starts one full half-TTL period in, not instantly.
    loop {
        tokio::select! {
            res = &mut fut => return (res, handle),
            _ = ticker.tick() => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                if let Err(e) = maintenance_lease::renew(&mut handle, &catalog, &state_table, ttl_secs, now_ms).await {
                    warn!(
                        job_key = %handle.job_key,
                        error = %e,
                        "active_tick: lease renew failed mid-job (best-effort, continuing the job anyway)"
                    );
                }
            }
        }
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
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionParams {
    pub target_file_size_bytes: u64,
    pub min_input_files: usize,
    pub delete_file_threshold: usize,
    pub delete_ratio_threshold: f64,
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
/// Table property that overrides `[maintenance.compaction].delete_ratio_threshold`.
pub const COMPACTION_DELETE_RATIO_THRESHOLD_PROPERTY: &str =
    "sqe.maintenance.compaction.delete-ratio-threshold";
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
        delete_ratio_threshold: resolve_ratio_override(
            table_props,
            COMPACTION_DELETE_RATIO_THRESHOLD_PROPERTY,
            cfg.delete_ratio_threshold,
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

fn resolve_usize_override(
    table_props: &HashMap<String, String>,
    key: &str,
    default: usize,
) -> usize {
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

/// A share override in `[0, 1]`; `0` turns the trigger off for the table. A
/// value that does not parse, or falls outside that range, is malformed and
/// falls back to the global value like the integer overrides.
fn resolve_ratio_override(table_props: &HashMap<String, String>, key: &str, default: f64) -> f64 {
    match table_props.get(key) {
        None => default,
        Some(raw) => match raw.trim().parse::<f64>() {
            Ok(v) if (0.0..=1.0).contains(&v) => v,
            _ => {
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
pub fn table_due(
    ident: &str,
    schedule: &str,
    jitter_secs: u64,
    tick_secs: u64,
    now_ms: i64,
) -> bool {
    let Some((prev_fire_ms, adjusted_now_ms)) =
        previous_fire_ms(ident, schedule, jitter_secs, now_ms)
    else {
        return false;
    };
    prev_fire_ms > adjusted_now_ms - (tick_secs as i64) * 1000
}

/// The schedule window `ident` is currently in: the cron's most recent fire
/// instant at or before `now_ms` (jitter-adjusted, so two tables sharing a
/// schedule but not a jitter offset are in different windows), in epoch ms.
/// `None` for an invalid or unsatisfiable cron -- the same inputs for which
/// [`table_due`] returns false.
///
/// Used as the dedup key for `skipped` ledger rows (finding 511): "already
/// recorded for this window" has to mean the SCHEDULE window, not "within
/// the last N seconds", or a table whose schedule fires more often than the
/// dedup interval would silently stop recording skips.
pub fn schedule_window_start_ms(
    ident: &str,
    schedule: &str,
    jitter_secs: u64,
    now_ms: i64,
) -> Option<i64> {
    previous_fire_ms(ident, schedule, jitter_secs, now_ms).map(|(fire_ms, _)| fire_ms)
}

/// Shared core of [`table_due`] and [`schedule_window_start_ms`]: the cron's
/// most recent fire at or before the jitter-adjusted `now`, together with
/// that adjusted `now`. `None` on an invalid cron (warned once per
/// `(ident, schedule)` pair) or a `croner` search error.
fn previous_fire_ms(
    ident: &str,
    schedule: &str,
    jitter_secs: u64,
    now_ms: i64,
) -> Option<(i64, i64)> {
    let cron = match Cron::from_str(schedule) {
        Ok(c) => c,
        Err(e) => {
            warn_invalid_schedule_once(ident, schedule, &e);
            return None;
        }
    };

    // `jitter_offset_secs` returns 0 when `jitter_secs == 0` (its own guard,
    // not this call site's), so this is safe against divide-by-zero and
    // yields zero jitter delay rather than bypassing the schedule check.
    let offset_secs = jitter_offset_secs(ident, jitter_secs);
    let adjusted_now_ms = now_ms - (offset_secs as i64) * 1000;
    let adjusted_now = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(adjusted_now_ms)?;
    let prev_fire = cron.find_previous_occurrence(&adjusted_now, true).ok()?;
    Some((prev_fire.timestamp_millis(), adjusted_now_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_compaction_cfg() -> MaintenanceCompactionConfig {
        MaintenanceCompactionConfig {
            target_file_size_bytes: 512 * 1024 * 1024,
            min_input_files: 5,
            delete_file_threshold: 2,
            delete_ratio_threshold: 0.0,
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
        props.insert(
            COMPACTION_MIN_INPUT_FILES_PROPERTY.to_string(),
            "3".to_string(),
        );
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
    fn resolve_compaction_params_per_table_override_wins_delete_ratio_threshold() {
        let cfg = default_compaction_cfg();
        let mut props = HashMap::new();
        props.insert(
            COMPACTION_DELETE_RATIO_THRESHOLD_PROPERTY.to_string(),
            "0.3".to_string(),
        );
        assert_eq!(
            resolve_compaction_params(&cfg, &props).delete_ratio_threshold,
            0.3
        );
        // Out of range or unparsable falls back to the global value.
        for bad in ["1.5", "-0.1", "NaN", "lots"] {
            props.insert(
                COMPACTION_DELETE_RATIO_THRESHOLD_PROPERTY.to_string(),
                bad.to_string(),
            );
            assert_eq!(
                resolve_compaction_params(&cfg, &props).delete_ratio_threshold,
                cfg.delete_ratio_threshold,
                "{bad}"
            );
        }
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
        props.insert(
            COMPACTION_MIN_INPUT_FILES_PROPERTY.to_string(),
            "7".to_string(),
        );
        props.insert(
            COMPACTION_DELETE_FILE_THRESHOLD_PROPERTY.to_string(),
            "4".to_string(),
        );
        props.insert(
            COMPACTION_STRATEGY_PROPERTY.to_string(),
            "zorder".to_string(),
        );
        let params = resolve_compaction_params(&cfg, &props);
        assert_eq!(
            params,
            CompactionParams {
                target_file_size_bytes: 2048,
                min_input_files: 7,
                delete_file_threshold: 4,
                delete_ratio_threshold: 0.0,
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
        props.insert(
            COMPACTION_MIN_INPUT_FILES_PROPERTY.to_string(),
            "-3".to_string(),
        );
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
        let ident = TableIdent::new(
            iceberg::NamespaceIdent::new("ns".to_string()),
            "t".to_string(),
        );
        let table_ref = table_ref_from_ident(&ident).expect("parses");
        assert_eq!(table_ref.namespace, "ns");
        assert_eq!(table_ref.name, "t");
        assert_eq!(table_ref.catalog, None);
    }

    #[test]
    fn table_due_is_deterministic_for_same_inputs() {
        let a = table_due("ns.t1", "0 2 * * *", 900, 60, 1_700_000_000_000);
        let b = table_due("ns.t1", "0 2 * * *", 900, 60, 1_700_000_000_000);
        assert_eq!(
            a, b,
            "same ident/schedule/jitter/tick/now must always agree"
        );
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
        assert!(!table_due(
            "ns.t1",
            "not a cron expression",
            0,
            60,
            123_456_789
        ));
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
            !table_due(
                ident,
                schedule,
                jitter_secs,
                tick_secs,
                effective_fire_ms - 1_000
            ),
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
        assert!(!table_due(
            "ns.bad_schedule",
            "not a cron expression",
            900,
            60,
            now_ms
        ));
        // Calling it again (exercising the warn-once dedup path) must still
        // just return false, never panic.
        assert!(!table_due(
            "ns.bad_schedule",
            "not a cron expression",
            900,
            60,
            now_ms
        ));
    }

    #[test]
    fn resolve_schedule_falls_back_to_global_when_no_override() {
        assert_eq!(resolve_schedule("0 2 * * *", &HashMap::new()), "0 2 * * *");
    }

    #[test]
    fn resolve_schedule_per_table_override_wins() {
        let mut props = HashMap::new();
        props.insert(
            COMPACTION_SCHEDULE_PROPERTY.to_string(),
            "0 3 * * *".to_string(),
        );
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
        props.insert(
            COMPACTION_SCHEDULE_PROPERTY.to_string(),
            "0 3 * * *".to_string(),
        );

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
            !table_due(
                ident,
                &global_schedule,
                jitter_secs,
                tick_secs,
                override_fire_ms
            ),
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
                HashMap::from([(
                    MAINTENANCE_ENABLED_PROPERTY.to_string(),
                    "false".to_string(),
                )]),
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

    // --- Discovery cache (finding 511, arm 1) ---

    fn ident(ns: &str, name: &str) -> TableIdent {
        TableIdent::new(
            iceberg::NamespaceIdent::new(ns.to_string()),
            name.to_string(),
        )
    }

    #[test]
    fn discovery_cache_entry_is_fresh_until_the_ttl_elapses() {
        let mut cache = DiscoveryCache::default();
        assert_eq!(cache.advance_tick(), 1);
        cache.insert(ident("ns", "t"), HashMap::new());
        // Loaded at tick 1 with a 5-tick TTL: reused on ticks 2..=5, and
        // reloaded on tick 6 (age 5 is no longer "younger than 5").
        for expected_tick in 2..=5 {
            let tick = cache.advance_tick();
            assert_eq!(tick, expected_tick);
            assert!(
                cache.is_fresh("ns.t", tick, 5),
                "tick {tick}: a {}-tick-old entry must still be reused",
                tick - 1
            );
        }
        let tick = cache.advance_tick();
        assert_eq!(tick, 6);
        assert!(
            !cache.is_fresh("ns.t", tick, 5),
            "a 5-tick-old entry must be re-loaded under a 5-tick TTL"
        );
        assert_eq!(tick - 1, 5);
    }

    #[test]
    fn discovery_cache_ttl_of_one_reloads_every_tick() {
        let mut cache = DiscoveryCache::default();
        cache.advance_tick();
        cache.insert(ident("ns", "t"), HashMap::new());
        let tick = cache.advance_tick();
        assert!(
            !cache.is_fresh("ns.t", tick, 1),
            "ttl_ticks = 1 is the pre-cache behaviour: load on every tick"
        );
    }

    #[test]
    fn discovery_cache_never_serves_a_table_it_has_not_loaded() {
        let mut cache = DiscoveryCache::default();
        let tick = cache.advance_tick();
        assert!(!cache.is_fresh("ns.never-seen", tick, 5));
    }

    #[test]
    fn discovery_cache_evicts_tables_the_catalog_stopped_listing() {
        let mut cache = DiscoveryCache::default();
        cache.advance_tick();
        cache.insert(ident("ns", "kept"), HashMap::new());
        cache.insert(ident("ns", "dropped"), HashMap::new());

        cache.retain_listed(&[ident("ns", "kept")]);

        let (props, idents) = cache.snapshot();
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].0, "ns.kept");
        assert!(idents.contains_key("ns.kept"));
        assert!(
            !idents.contains_key("ns.dropped"),
            "a table the catalog no longer lists must not keep being scheduled"
        );
    }

    #[test]
    fn discovery_cache_snapshot_is_sorted_by_table_name() {
        let mut cache = DiscoveryCache::default();
        cache.advance_tick();
        for name in ["c", "a", "b"] {
            cache.insert(ident("ns", name), HashMap::new());
        }
        let (props, _) = cache.snapshot();
        let names: Vec<&str> = props.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["ns.a", "ns.b", "ns.c"]);
    }

    #[test]
    fn discovery_cache_insert_refreshes_the_load_tick() {
        let mut cache = DiscoveryCache::default();
        cache.advance_tick();
        cache.insert(ident("ns", "t"), HashMap::new());
        let tick = cache.advance_tick();
        assert!(!cache.is_fresh("ns.t", tick, 1));
        // Re-loading at the current tick makes it fresh again.
        cache.insert(ident("ns", "t"), HashMap::new());
        assert!(cache.is_fresh("ns.t", tick, 1));
    }

    #[test]
    fn discovery_cache_snapshot_returns_the_cached_properties() {
        let mut cache = DiscoveryCache::default();
        cache.advance_tick();
        cache.insert(
            ident("ns", "t"),
            HashMap::from([(MAINTENANCE_ENABLED_PROPERTY.to_string(), "true".to_string())]),
        );
        let (props, _) = cache.snapshot();
        assert_eq!(select_enabled(&props), vec!["ns.t".to_string()]);
    }

    // --- Skipped-row dedup (finding 511, arm 4) ---

    #[test]
    fn skip_ledger_writes_one_row_per_table_per_window() {
        let mut ledger = SkipLedger::default();
        let window = 1_700_000_000_000;
        let recorded = (0..100)
            .filter(|_| ledger.should_record("ns.idle", window, "no eligible compaction debt"))
            .count();
        assert_eq!(
            recorded, 1,
            "100 ticks with no debt must write exactly one skipped row for the table"
        );
    }

    #[test]
    fn skip_ledger_records_again_in_the_next_window() {
        let mut ledger = SkipLedger::default();
        let reason = "no eligible compaction debt";
        assert!(ledger.should_record("ns.idle", 1_000, reason));
        assert!(!ledger.should_record("ns.idle", 1_000, reason));
        assert!(
            ledger.should_record("ns.idle", 2_000, reason),
            "a new schedule window is new information and must be recorded"
        );
    }

    #[test]
    fn skip_ledger_records_a_different_reason_in_the_same_window() {
        let mut ledger = SkipLedger::default();
        assert!(ledger.should_record("ns.idle", 1_000, "no eligible compaction debt"));
        assert!(
            ledger.should_record("ns.idle", 1_000, "insufficient_workers: ..."),
            "a table that starts being skipped for a DIFFERENT reason must say so"
        );
        assert!(!ledger.should_record("ns.idle", 1_000, "insufficient_workers: ..."));
    }

    #[test]
    fn skip_ledger_tracks_tables_independently() {
        let mut ledger = SkipLedger::default();
        let reason = "no eligible compaction debt";
        assert!(ledger.should_record("ns.a", 1_000, reason));
        assert!(
            ledger.should_record("ns.b", 1_000, reason),
            "one table's skip must never suppress another's"
        );
        assert!(!ledger.should_record("ns.a", 1_000, reason));
        assert!(!ledger.should_record("ns.b", 1_000, reason));
    }

    // --- In-flight guard (finding 511, arm 2) ---

    #[test]
    fn inflight_guard_refuses_a_second_claim_and_frees_the_key_on_drop() {
        let set = Arc::new(Mutex::new(HashSet::new()));
        let first = InflightGuard::try_claim(&set, "ns.t").expect("first claim wins");
        assert!(
            InflightGuard::try_claim(&set, "ns.t").is_none(),
            "a table whose job is still in flight must be skipped, not queued twice"
        );
        assert!(
            InflightGuard::try_claim(&set, "ns.other").is_some(),
            "a different table is unaffected"
        );
        drop(first);
        assert!(
            InflightGuard::try_claim(&set, "ns.t").is_some(),
            "dropping the guard (return, panic, or abort) frees the table"
        );
    }

    // --- Schedule window key ---

    #[test]
    fn schedule_window_start_ms_is_constant_across_a_window_and_moves_with_it() {
        // Every minute, no jitter: two instants inside the same minute share
        // a window; the next minute does not.
        let base = utc_ms(2026, 9, 5, 12, 30, 5);
        let same_window = utc_ms(2026, 9, 5, 12, 30, 55);
        let next_window = utc_ms(2026, 9, 5, 12, 31, 5);

        let w1 = schedule_window_start_ms("ns.t", "* * * * *", 0, base).expect("valid cron");
        let w2 = schedule_window_start_ms("ns.t", "* * * * *", 0, same_window).expect("valid cron");
        let w3 = schedule_window_start_ms("ns.t", "* * * * *", 0, next_window).expect("valid cron");

        assert_eq!(w1, w2);
        assert_ne!(w2, w3);
        assert_eq!(w1, utc_ms(2026, 9, 5, 12, 30, 0));
    }

    #[test]
    fn schedule_window_start_ms_is_none_for_an_invalid_cron() {
        assert!(schedule_window_start_ms("ns.t", "not a cron", 0, 1_700_000_000_000).is_none());
    }

    #[test]
    fn schedule_window_start_ms_agrees_with_table_due_on_the_daily_schedule() {
        // 02:00:30 on the daily-02:00 schedule: due, and the window is the
        // 02:00:00 fire it is due for.
        let now = utc_ms(2026, 9, 5, 2, 0, 30);
        assert!(table_due("ns.t", "0 2 * * *", 0, 60, now));
        assert_eq!(
            schedule_window_start_ms("ns.t", "0 2 * * *", 0, now),
            Some(utc_ms(2026, 9, 5, 2, 0, 0))
        );
    }

    /// End-to-end proof of the discovery cache against a REAL tick, with a
    /// `catalog_factory` handing the scheduler a catalog that records every
    /// `load_table` call (finding 511's first acceptance criterion).
    ///
    /// Uses the same SQLite-backed Iceberg harness as
    /// `tests/maintenance_scheduler_test.rs`, but lives here rather than
    /// there because an integration test cannot implement
    /// `#[async_trait] iceberg::Catalog`: `async-trait` is a dependency of
    /// this crate, not a dev-dependency, so the counting wrapper only
    /// compiles inside the lib.
    #[cfg(feature = "test-sqlite")]
    mod discovery_cache_e2e {
        use super::*;
        use std::collections::BTreeMap;

        use sqe_core::config::{
            MaintenanceDistributionConfig, MaintenancePrincipalConfig, MaintenanceSchedulerConfig,
        };
        use sqe_core::{SecretStore, SecretString};
        use tempfile::TempDir;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// Delegating `iceberg::Catalog` that counts `load_table` per table
        /// ident. Everything else is pass-through: the point is to measure
        /// the scheduler's catalog traffic, not to fake catalog behaviour.
        #[derive(Debug)]
        struct CountingCatalog {
            inner: Arc<dyn Catalog>,
            loads: Mutex<HashMap<String, usize>>,
        }

        impl CountingCatalog {
            fn new(inner: Arc<dyn Catalog>) -> Self {
                Self {
                    inner,
                    loads: Mutex::new(HashMap::new()),
                }
            }

            fn loads_for(&self, table: &str) -> usize {
                lock_or_recover(&self.loads)
                    .get(table)
                    .copied()
                    .unwrap_or(0)
            }
        }

        #[async_trait::async_trait]
        impl Catalog for CountingCatalog {
            async fn list_namespaces(
                &self,
                parent: Option<&iceberg::NamespaceIdent>,
            ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
                self.inner.list_namespaces(parent).await
            }
            async fn create_namespace(
                &self,
                namespace: &iceberg::NamespaceIdent,
                properties: HashMap<String, String>,
            ) -> iceberg::Result<iceberg::Namespace> {
                self.inner.create_namespace(namespace, properties).await
            }
            async fn get_namespace(
                &self,
                namespace: &iceberg::NamespaceIdent,
            ) -> iceberg::Result<iceberg::Namespace> {
                self.inner.get_namespace(namespace).await
            }
            async fn namespace_exists(
                &self,
                namespace: &iceberg::NamespaceIdent,
            ) -> iceberg::Result<bool> {
                self.inner.namespace_exists(namespace).await
            }
            async fn update_namespace(
                &self,
                namespace: &iceberg::NamespaceIdent,
                properties: HashMap<String, String>,
            ) -> iceberg::Result<()> {
                self.inner.update_namespace(namespace, properties).await
            }
            async fn drop_namespace(
                &self,
                namespace: &iceberg::NamespaceIdent,
            ) -> iceberg::Result<()> {
                self.inner.drop_namespace(namespace).await
            }
            async fn list_tables(
                &self,
                namespace: &iceberg::NamespaceIdent,
            ) -> iceberg::Result<Vec<TableIdent>> {
                self.inner.list_tables(namespace).await
            }
            async fn create_table(
                &self,
                namespace: &iceberg::NamespaceIdent,
                creation: iceberg::TableCreation,
            ) -> iceberg::Result<iceberg::table::Table> {
                self.inner.create_table(namespace, creation).await
            }
            async fn load_table(
                &self,
                table: &TableIdent,
            ) -> iceberg::Result<iceberg::table::Table> {
                *lock_or_recover(&self.loads)
                    .entry(table.to_string())
                    .or_insert(0) += 1;
                self.inner.load_table(table).await
            }
            async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
                self.inner.drop_table(table).await
            }
            async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
                self.inner.purge_table(table).await
            }
            async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
                self.inner.table_exists(table).await
            }
            async fn rename_table(
                &self,
                src: &TableIdent,
                dest: &TableIdent,
            ) -> iceberg::Result<()> {
                self.inner.rename_table(src, dest).await
            }
            async fn register_table(
                &self,
                table: &TableIdent,
                metadata_location: String,
            ) -> iceberg::Result<iceberg::table::Table> {
                self.inner.register_table(table, metadata_location).await
            }
            async fn update_table(
                &self,
                commit: iceberg::TableCommit,
            ) -> iceberg::Result<iceberg::table::Table> {
                self.inner.update_table(commit).await
            }
        }

        async fn sqlite_catalog(dir: &TempDir) -> Arc<dyn Catalog> {
            sqe_catalog::mount::build_catalog(
                dir.path().to_str().expect("tempdir path is UTF-8"),
                sqe_sql::CatalogKind::Sqlite,
                &BTreeMap::new(),
                &SecretStore::new(),
            )
            .await
            .expect("sqlite catalog builds")
        }

        async fn create_table(catalog: &Arc<dyn Catalog>, ns: &str, name: &str) {
            let ns_ident = iceberg::NamespaceIdent::new(ns.to_string());
            if !catalog
                .namespace_exists(&ns_ident)
                .await
                .expect("namespace_exists")
            {
                catalog
                    .create_namespace(&ns_ident, HashMap::new())
                    .await
                    .expect("create namespace");
            }
            let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow_schema)
                .expect("arrow schema converts");
            let creation = iceberg::TableCreation::builder()
                .name(name.to_string())
                .schema(schema)
                .properties(HashMap::new())
                .build();
            catalog
                .create_table(&ns_ident, creation)
                .await
                .expect("create table");
        }

        async fn mock_idp() -> MockServer {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/token"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "fake-maintenance-catalog-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })))
                .mount(&server)
                .await;
            server
        }

        fn build_scheduler(
            idp: &MockServer,
            catalog: Arc<CountingCatalog>,
            discovery_cache_ticks: u64,
        ) -> MaintenanceScheduler {
            let cfg = MaintenanceConfig {
                mode: MaintenanceMode::Advisory,
                principal: Some(MaintenancePrincipalConfig {
                    token_endpoint: format!("{}/token", idp.uri()),
                    client_id: "sqe-maintenance-test".to_string(),
                    client_secret: SecretString::new("test-secret".to_string()),
                    scope: None,
                    user_id: "svc-sqe-maintenance".to_string(),
                    roles: vec!["maintenance".to_string()],
                    refresh_skew_secs: 60,
                }),
                scheduler: MaintenanceSchedulerConfig {
                    jitter_secs: 0,
                    schedule: "* * * * *".to_string(),
                    discovery_cache_ticks,
                    ..Default::default()
                },
                compaction: MaintenanceCompactionConfig::default(),
                distribution: MaintenanceDistributionConfig::default(),
            };
            let principal = Arc::new(
                MaintenancePrincipal::from_config(cfg.principal.as_ref().expect("principal set"))
                    .expect("build principal"),
            );
            let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().expect("metrics registry"));
            let factory: CatalogFactory = {
                Arc::new(move |_session: &Session| {
                    let catalog: Arc<dyn Catalog> = Arc::clone(&catalog) as Arc<dyn Catalog>;
                    Box::pin(async move { Ok(catalog) })
                })
            };
            let handler = Arc::new(crate::maintenance::MaintenanceHandler::new(
                toml::from_str(
                    r#"
[coordinator]
[auth]
[catalog]
catalog_url = ""
"#,
                )
                .expect("minimal SqeConfig parses"),
            ));
            MaintenanceScheduler::new(cfg, principal, metrics, None, factory, handler)
        }

        /// The acceptance criterion, literally: two consecutive ticks with
        /// no property change call `load_table` at most once per table.
        ///
        /// The tables are deliberately NOT opted in, so the ONLY thing that
        /// would load them is discovery -- an opted-in table's job loads it
        /// again by design (the job never trusts cached properties).
        #[tokio::test]
        async fn two_consecutive_ticks_load_each_table_once() {
            let dir = tempfile::tempdir().expect("tempdir");
            let inner = sqlite_catalog(&dir).await;
            for name in ["a", "b", "c"] {
                create_table(&inner, "ns", name).await;
            }
            let counting = Arc::new(CountingCatalog::new(inner));
            let idp = mock_idp().await;
            let scheduler = build_scheduler(&idp, Arc::clone(&counting), 5);

            scheduler.advisory_tick().await.expect("tick 1");
            scheduler.drain_inflight_jobs().await;
            scheduler.advisory_tick().await.expect("tick 2");
            scheduler.drain_inflight_jobs().await;

            for table in ["ns.a", "ns.b", "ns.c"] {
                assert_eq!(
                    counting.loads_for(table),
                    1,
                    "{table} must be loaded once across two ticks, not once per tick"
                );
            }
        }

        /// The cache is the DEFAULT, not the only behaviour:
        /// `discovery_cache_ticks = 1` restores the pre-511
        /// load-every-table-every-tick pass, which is what makes the knob
        /// meaningful (and proves the two ticks above really did both run).
        #[tokio::test]
        async fn discovery_cache_ticks_of_one_loads_every_tick() {
            let dir = tempfile::tempdir().expect("tempdir");
            let inner = sqlite_catalog(&dir).await;
            create_table(&inner, "ns", "a").await;
            let counting = Arc::new(CountingCatalog::new(inner));
            let idp = mock_idp().await;
            let scheduler = build_scheduler(&idp, Arc::clone(&counting), 1);

            scheduler.advisory_tick().await.expect("tick 1");
            scheduler.drain_inflight_jobs().await;
            scheduler.advisory_tick().await.expect("tick 2");
            scheduler.drain_inflight_jobs().await;

            assert_eq!(counting.loads_for("ns.a"), 2);
        }

        /// A table created between two ticks is picked up on the next one:
        /// `list_tables` is the change signal, so a cache hit for the
        /// tables it already knows never hides a NEW table.
        #[tokio::test]
        async fn a_newly_listed_table_is_loaded_on_the_next_tick() {
            let dir = tempfile::tempdir().expect("tempdir");
            let inner = sqlite_catalog(&dir).await;
            create_table(&inner, "ns", "a").await;
            let counting = Arc::new(CountingCatalog::new(inner.clone()));
            let idp = mock_idp().await;
            let scheduler = build_scheduler(&idp, Arc::clone(&counting), 5);

            scheduler.advisory_tick().await.expect("tick 1");
            scheduler.drain_inflight_jobs().await;
            create_table(&inner, "ns", "b").await;
            scheduler.advisory_tick().await.expect("tick 2");
            scheduler.drain_inflight_jobs().await;

            assert_eq!(counting.loads_for("ns.a"), 1, "already cached");
            assert_eq!(
                counting.loads_for("ns.b"),
                1,
                "a newly listed table must be loaded immediately, not after the TTL"
            );
        }
    }
}
