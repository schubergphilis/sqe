use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use sqe_planner::{CoordinatorVersions, HelloCompat, WorkerCapacity, WorkerHello};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::channel_pool::ChannelPool;

/// Tracks in-flight work per worker URL across all concurrent queries: a
/// fragment count and the reserved *load* in the scheduler's cost unit
/// (megabytes of scan input, see `scheduler::estimate_cost`). The scheduler
/// reads the load back as its initial per-worker weight so two queries
/// planning at the same time do not both pick the same idle worker.
///
/// Finding 408: the tracker used to expose only the fragment count, which the
/// scheduler then added MB costs to. Reserve with the task's estimated cost
/// via [`WorkerLoadTracker::reserve_load`]; the count-only
/// [`WorkerLoadTracker::reserve`] remains for callers whose unit is a job
/// slot (compaction dispatch).
///
/// Release is automatic: [`ReservationGuard::drop`] subtracts both the count
/// and the reserved load when the fragment completes (success, error, or
/// cancel), so it is panic-safe.
#[derive(Debug, Default, Clone)]
pub struct WorkerLoadTracker {
    entries: Arc<DashMap<String, LoadEntry>>,
}

#[derive(Debug, Default)]
struct LoadEntry {
    count: AtomicU32,
    load: AtomicU64,
}

impl WorkerLoadTracker {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
        }
    }

    /// Return the current in-flight fragment count for `url`.
    pub fn in_flight(&self, url: &str) -> u32 {
        self.entries
            .get(url)
            .map(|e| e.count.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Return the load currently reserved on `url`, in `estimate_cost` units.
    pub fn in_flight_load(&self, url: &str) -> u64 {
        self.entries
            .get(url)
            .map(|e| e.load.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Snapshot `(url, count, load)` triples without holding the map locked.
    pub fn snapshot(&self) -> Vec<(String, u32, u64)> {
        self.entries
            .iter()
            .map(|kv| {
                (
                    kv.key().clone(),
                    kv.value().count.load(Ordering::Relaxed),
                    kv.value().load.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// Reserve one fragment slot on `url` with no load weight. Use
    /// [`Self::reserve_load`] for scan fragments so the scheduler sees the
    /// cost; this form is for slot-counted dispatch (compaction groups).
    pub fn reserve(&self, url: &str) -> ReservationGuard {
        self.reserve_load(url, 0)
    }

    /// Reserve one fragment slot on `url` carrying `load` units (the task's
    /// `estimate_cost`). The returned guard releases both on drop.
    pub fn reserve_load(&self, url: &str, load: u64) -> ReservationGuard {
        let entry = self.entries.entry(url.to_string()).or_default();
        entry.count.fetch_add(1, Ordering::Relaxed);
        entry.load.fetch_add(load, Ordering::Relaxed);
        ReservationGuard {
            tracker: self.entries.clone(),
            url: url.to_string(),
            load,
        }
    }
}

/// RAII guard returned by [`WorkerLoadTracker::reserve`] /
/// [`WorkerLoadTracker::reserve_load`]. Releases the fragment slot and the
/// reserved load for the worker URL when dropped.
pub struct ReservationGuard {
    tracker: Arc<DashMap<String, LoadEntry>>,
    url: String,
    load: u64,
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(e) = self.tracker.get(&self.url) {
            let prev = e.count.fetch_sub(1, Ordering::Relaxed);
            if prev == 0 {
                e.count.fetch_add(1, Ordering::Relaxed);
                debug!(worker = %self.url, "reservation underflow guarded");
            }
            // Saturate rather than wrap: a guard can only release what it
            // reserved, but keep the same defensive shape as the count.
            let _ = e
                .load
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(self.load))
                });
        }
    }
}

/// Tracks available workers and their health status.
///
/// Workers are discovered from config and health-checked periodically.
/// Unhealthy workers (3 consecutive failed health checks) are removed
/// from the active pool but retained in the registry for recovery.
#[derive(Debug, Clone)]
pub struct WorkerRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    channel_pool: Arc<ChannelPool>,
    max_workers: usize,
    max_consecutive_failures: u32,
    /// Finding 609: how long after the last eviction a heartbeat that still
    /// reports worker-attributable failures is refused re-admission.
    recovery_quiet_period: Duration,
    /// Finding 609: hello rejections already logged, by URL -> reason, so a
    /// mismatched worker heartbeating every few seconds produces one warning
    /// per (worker, reason) rather than one per interval.
    logged_hello_rejections: Arc<DashMap<String, String>>,
}

#[derive(Debug)]
struct RegistryInner {
    workers: HashMap<String, WorkerState>,
}

#[derive(Debug)]
struct WorkerState {
    healthy: bool,
    consecutive_failures: u32,
    last_healthy: Option<Instant>,
    /// Finding 609: when this worker was last removed from the healthy set by
    /// an execution failure or the health checker. Gates re-admission.
    last_unhealthy_at: Option<Instant>,
    /// Finding 609: process identity from the hello; `nil` until the worker
    /// sends one (legacy body).
    instance_id: Uuid,
    /// Build identifier from the hello, for diagnostics.
    build: String,
    /// Newest `ScanTask` version the worker said it implements; `0` unknown.
    scan_task_version: u32,
    /// Load vector from the most recent hello.
    capacity: WorkerCapacity,
}

impl WorkerState {
    fn unknown() -> Self {
        Self {
            healthy: false,
            consecutive_failures: 0,
            last_healthy: None,
            last_unhealthy_at: None,
            instance_id: Uuid::nil(),
            build: String::new(),
            scan_task_version: 0,
            capacity: WorkerCapacity::default(),
        }
    }
}

/// What [`WorkerRegistry::register_hello`] observed (finding 609). The
/// heartbeat handler uses it to decide what to audit and what to purge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HelloOutcome {
    /// The URL was not in the registry before this hello.
    pub newly_discovered: bool,
    /// A known URL reported a different `instance_id`: the worker process
    /// restarted and every fragment-scoped thing the coordinator remembered
    /// about it (credential channels, in-flight scans) is gone.
    pub instance_changed: bool,
    /// The instance id the registry held before, when `instance_changed`.
    pub previous_instance: Option<Uuid>,
    /// The worker was unhealthy and this hello returned it to the pool.
    pub readmitted: bool,
    /// The worker was unhealthy and this hello did NOT return it: it still
    /// reports failures and the quiet period has not elapsed.
    pub held_unhealthy: bool,
}

const DEFAULT_MAX_CONSECUTIVE_FAILURES: u32 = 3;
const DEFAULT_MAX_WORKERS: usize = 1024;
/// Finding 609: a worker that keeps failing scans while heartbeating fine
/// stays out of the pool for at least this long after its last eviction.
const DEFAULT_RECOVERY_QUIET_PERIOD: Duration = Duration::from_secs(30);

/// Reason a [`WorkerRegistry::register_heartbeat`] call was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationError {
    /// The registry already tracks `max_workers` URLs.
    CapacityExceeded { cap: usize },
    /// The advertised worker URL is empty, unparseable, or points at an
    /// unspecified address (`0.0.0.0` / `::`). Registering it would let
    /// every misconfigured worker collide on one bogus loopback entry that
    /// the scheduler then targets (issue #220).
    InvalidAdvertiseUrl { url: String },
    /// Finding 408: the worker speaks a different `sqe.proto.version` or was
    /// built against a different DataFusion release than this coordinator.
    /// The `datafusion_proto` `Expr` bytes in a `ScanTask` are coupled to the
    /// exact DataFusion version, so such a worker would fail at decode time
    /// mid-query; refuse it at registration instead.
    ProtocolMismatch {
        url: String,
        worker_proto: u32,
        coordinator_proto: u32,
        worker_datafusion: String,
        coordinator_datafusion: String,
    },
    /// Finding 609: the hello's `check_compat` found a skew (`ScanTask`
    /// version, protocol, DataFusion). `reason` names each mismatch.
    HelloMismatch { url: String, reason: String },
}

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HelloMismatch { url, reason } => write!(
                f,
                "worker {url} version mismatch: {reason}. Deploy the same SQE build on both \
                 sides; the worker is not admitted to the pool."
            ),
            Self::CapacityExceeded { cap } => write!(
                f,
                "worker registry at max_workers cap ({cap}); refusing to track additional workers"
            ),
            Self::InvalidAdvertiseUrl { url } => write!(
                f,
                "worker advertised an unroutable URL {url:?} (empty, unparseable, or \
                 an unspecified 0.0.0.0 / :: address); the worker must advertise a \
                 routable address. Set worker.advertise_url or expose POD_IP."
            ),
            Self::ProtocolMismatch {
                url,
                worker_proto,
                coordinator_proto,
                worker_datafusion,
                coordinator_datafusion,
            } => write!(
                f,
                "worker {url} protocol mismatch: worker sqe.proto.version={worker_proto} \
                 datafusion={worker_datafusion:?}, coordinator sqe.proto.version=\
                 {coordinator_proto} datafusion={coordinator_datafusion:?} (0 / \"\" means \
                 the worker predates the handshake). Deploy the same SQE build on both \
                 sides; ScanTask predicate bytes are not portable across DataFusion releases."
            ),
        }
    }
}

impl std::error::Error for RegistrationError {}

/// Finding 408: decide whether a heartbeating worker is protocol-compatible
/// with this coordinator.
///
/// - Versions present and equal: accepted.
/// - Versions present and different: refused, always. A DataFusion or
///   `sqe.proto.version` skew would otherwise fail at `Expr` decode inside the
///   worker with a generic error.
/// - Versions absent (legacy bare-URL heartbeat, `proto_version == 0`):
///   refused when `strict` (`coordinator.production_mode`), otherwise accepted
///   with a warning so a mixed rolling upgrade is explicit rather than silent.
pub fn check_worker_protocol(
    payload: &sqe_planner::HeartbeatPayload,
    strict: bool,
) -> Result<(), RegistrationError> {
    let coordinator_proto = sqe_planner::PROTO_VERSION;
    let coordinator_datafusion = datafusion::DATAFUSION_VERSION;
    let mismatch = || RegistrationError::ProtocolMismatch {
        url: payload.worker_url.clone(),
        worker_proto: payload.proto_version,
        coordinator_proto,
        worker_datafusion: payload.datafusion_version.clone(),
        coordinator_datafusion: coordinator_datafusion.to_string(),
    };
    if payload.is_legacy() {
        if strict {
            warn!(
                worker = %payload.worker_url,
                "Rejected heartbeat: worker sent no protocol version and production_mode is on"
            );
            return Err(mismatch());
        }
        warn!(
            worker = %payload.worker_url,
            coordinator_proto,
            coordinator_datafusion,
            "Worker heartbeat carries no protocol version (pre-handshake build); \
             accepting because production_mode is off. Upgrade the worker: a \
             DataFusion skew will fail ScanTask predicate decode mid-query."
        );
        return Ok(());
    }
    if payload.proto_version != coordinator_proto
        || payload.datafusion_version != coordinator_datafusion
    {
        let err = mismatch();
        warn!(worker = %payload.worker_url, error = %err, "Rejected heartbeat: protocol mismatch");
        return Err(err);
    }
    Ok(())
}

/// Returns `true` when an advertised worker URL is safe to register: it has a
/// non-empty host that is not the unspecified address (`0.0.0.0` / `::`).
///
/// The host is extracted without pulling in the `url` crate: strip any
/// `scheme://`, take everything before the first `/`, then strip a trailing
/// `:port`. IPv6 literals are bracketed (`[::1]:50052`) and handled. A bare
/// IP that parses as unspecified is rejected; hostnames and routable IPs pass.
fn advertise_url_is_routable(url: &str) -> bool {
    let url = url.trim();
    if url.is_empty() {
        return false;
    }
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    // Drop userinfo if present, then path/query.
    let authority = after_scheme
        .split('/')
        .next()
        .unwrap_or(after_scheme)
        .rsplit_once('@')
        .map_or(
            after_scheme.split('/').next().unwrap_or(after_scheme),
            |(_, host)| host,
        );

    // Extract host, stripping a trailing :port. Bracketed IPv6 first.
    let host = if let Some(rest) = authority.strip_prefix('[') {
        match rest.split_once(']') {
            Some((h, _)) => h,
            None => return false,
        }
    } else {
        match authority.rsplit_once(':') {
            // Only treat the trailing colon as a port separator when the host
            // part has no further colons (i.e. not an unbracketed IPv6).
            Some((h, _)) if !h.contains(':') => h,
            _ => authority,
        }
    };

    if host.is_empty() {
        return false;
    }
    // A host that parses as an unspecified IP is the poisoning case.
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return !ip.is_unspecified();
    }
    // Non-IP hostnames are accepted (resolved by the coordinator's client).
    true
}

impl WorkerRegistry {
    pub fn new(worker_urls: Vec<String>) -> Self {
        Self::with_channel_pool(worker_urls, ChannelPool::shared())
    }

    pub fn with_channel_pool(worker_urls: Vec<String>, channel_pool: Arc<ChannelPool>) -> Self {
        Self::with_options(worker_urls, channel_pool, DEFAULT_MAX_WORKERS)
    }

    pub fn with_options(
        worker_urls: Vec<String>,
        channel_pool: Arc<ChannelPool>,
        max_workers: usize,
    ) -> Self {
        Self::with_options_and_failures(
            worker_urls,
            channel_pool,
            max_workers,
            DEFAULT_MAX_CONSECUTIVE_FAILURES,
        )
    }

    pub fn with_options_and_failures(
        worker_urls: Vec<String>,
        channel_pool: Arc<ChannelPool>,
        max_workers: usize,
        max_consecutive_failures: u32,
    ) -> Self {
        let workers: HashMap<String, WorkerState> = worker_urls
            .into_iter()
            .map(|url| (url, WorkerState::unknown()))
            .collect();

        // Honour the configured cap but never refuse seed URLs already in config.
        let effective_cap = max_workers.max(workers.len());

        info!(
            worker_count = workers.len(),
            max_workers = effective_cap,
            "Initialized worker registry"
        );

        Self {
            inner: Arc::new(RwLock::new(RegistryInner { workers })),
            channel_pool,
            max_workers: effective_cap,
            max_consecutive_failures,
            recovery_quiet_period: DEFAULT_RECOVERY_QUIET_PERIOD,
            logged_hello_rejections: Arc::new(DashMap::new()),
        }
    }

    /// Finding 609: set how long an evicted worker that still reports
    /// failures stays out of the pool. `Duration::ZERO` restores the
    /// pre-609 "any heartbeat re-admits" behaviour.
    #[must_use = "with_recovery_quiet_period consumes self; bind the returned registry"]
    pub fn with_recovery_quiet_period(mut self, quiet_period: Duration) -> Self {
        self.recovery_quiet_period = quiet_period;
        self
    }

    pub fn channel_pool(&self) -> Arc<ChannelPool> {
        self.channel_pool.clone()
    }

    /// Finding 609: the load vector `url` last reported, if it is known.
    pub async fn worker_capacity(&self, url: &str) -> Option<WorkerCapacity> {
        self.inner.read().await.workers.get(url).map(|w| w.capacity)
    }

    /// Finding 609: the process instance id `url` last reported. `Some(nil)`
    /// for a known worker that has only ever sent legacy bodies.
    pub async fn worker_instance(&self, url: &str) -> Option<Uuid> {
        self.inner
            .read()
            .await
            .workers
            .get(url)
            .map(|w| w.instance_id)
    }

    /// Finding 609: `(build, scan_task_version)` `url` last reported.
    pub async fn worker_build(&self, url: &str) -> Option<(String, u32)> {
        self.inner
            .read()
            .await
            .workers
            .get(url)
            .map(|w| (w.build.clone(), w.scan_task_version))
    }

    /// Finding 609: log-once bookkeeping for hello rejections. Returns `true`
    /// the first time `reason` is seen for `url` (the caller should log at
    /// `warn!`), `false` on every repeat (the caller should stay quiet or log
    /// at `debug!`). A later successful hello clears the entry so a worker
    /// that is fixed and then breaks again is reported again.
    pub fn note_hello_rejected(&self, url: &str, reason: &str) -> bool {
        // Drop the read guard before inserting: a DashMap `get` guard held
        // across an `insert` on the same shard deadlocks.
        let already_logged = self
            .logged_hello_rejections
            .get(url)
            .is_some_and(|prev| prev.value() == reason);
        if already_logged {
            return false;
        }
        self.logged_hello_rejections
            .insert(url.to_string(), reason.to_string());
        true
    }

    pub async fn healthy_workers(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        inner
            .workers
            .iter()
            .filter(|(_, w)| w.healthy)
            .map(|(url, _)| url.clone())
            .collect()
    }

    pub async fn total_workers(&self) -> usize {
        let inner = self.inner.read().await;
        inner.workers.len()
    }

    pub async fn mark_healthy(&self, url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(state) = inner.workers.get_mut(url) {
            if !state.healthy {
                info!(worker = url, "Worker became healthy");
            }
            state.healthy = true;
            state.consecutive_failures = 0;
            state.last_healthy = Some(Instant::now());
        }
    }

    /// Register a worker (if not already known) and mark it healthy.
    ///
    /// Legacy entry point: a bare URL with no hello fields. Equivalent to
    /// [`Self::register_hello`] with a pre-609 body, so it always re-admits
    /// (a legacy worker reports `failures_since_last == 0`).
    pub async fn register_heartbeat(&self, url: &str) -> Result<(), RegistrationError> {
        let hello = WorkerHello {
            url: url.to_string(),
            ..WorkerHello::default()
        };
        self.register_hello(&hello).await.map(|_| ())
    }

    /// Register a worker from its heartbeat hello (finding 609).
    ///
    /// Workers that were not in the initial config list are dynamically added,
    /// bounded by `max_workers`. Heartbeats from previously-unknown URLs are
    /// rejected with `Err` once the cap is reached so a buggy or malicious
    /// worker reporting rotating URLs cannot grow the registry without limit.
    ///
    /// Beyond the URL, the hello drives three things:
    ///
    /// - **version skew** (`check_compat`): a mismatch is refused with
    ///   [`RegistrationError::HelloMismatch`] and, if the URL was known, the
    ///   worker is evicted so an upgraded-but-mismatched process does not keep
    ///   receiving tickets on the strength of its old registration;
    /// - **instance change**: a known URL with a new `instance_id` is reported
    ///   in [`HelloOutcome::instance_changed`] so the caller can drop
    ///   fragment-scoped bookkeeping;
    /// - **re-admission**: an unhealthy worker returns to the pool only when
    ///   it reports `failures_since_last == 0` or the recovery quiet period
    ///   has elapsed since its eviction. A worker that fails every scan but
    ///   heartbeats fine no longer flaps back every interval.
    ///
    /// Legacy bodies (bare URL, `HeartbeatPayload`) carry no versions and no
    /// failure count, so they are compat-neutral and always re-admit; the
    /// strict/lenient legacy policy lives in [`check_worker_protocol`].
    pub async fn register_hello(
        &self,
        hello: &WorkerHello,
    ) -> Result<HelloOutcome, RegistrationError> {
        let url = hello.url.as_str();
        // Reject unroutable advertise URLs before touching the registry.
        // A worker that advertises 0.0.0.0 (the old bug) would otherwise
        // make every worker collide on one bogus loopback entry that the
        // scheduler targets, causing flapping (issue #220).
        if !advertise_url_is_routable(url) {
            warn!(worker = url, "Rejected heartbeat: unroutable advertise URL");
            return Err(RegistrationError::InvalidAdvertiseUrl {
                url: url.to_string(),
            });
        }
        if let HelloCompat::Mismatch(reason) = hello.check_compat(&CoordinatorVersions::current())
        {
            // A worker that was in the pool before it was upgraded must not
            // stay there on the strength of its old registration.
            let mut inner = self.inner.write().await;
            if let Some(state) = inner.workers.get_mut(url) {
                if state.healthy {
                    warn!(
                        worker = url,
                        reason = %reason,
                        "Evicting known worker: its hello no longer matches this coordinator"
                    );
                }
                state.healthy = false;
                state.consecutive_failures = self.max_consecutive_failures;
                state.last_unhealthy_at = Some(Instant::now());
                state.build = hello.build.clone();
                state.scan_task_version = hello.scan_task_version;
            }
            return Err(RegistrationError::HelloMismatch {
                url: url.to_string(),
                reason,
            });
        }
        let mut inner = self.inner.write().await;
        if !inner.workers.contains_key(url) && inner.workers.len() >= self.max_workers {
            warn!(
                worker = url,
                registered = inner.workers.len(),
                cap = self.max_workers,
                "Rejected heartbeat: registry at max_workers cap"
            );
            return Err(RegistrationError::CapacityExceeded {
                cap: self.max_workers,
            });
        }
        let mut outcome = HelloOutcome::default();
        let state = inner.workers.entry(url.to_string()).or_insert_with(|| {
            info!(
                worker = url,
                instance_id = %hello.instance_id,
                build = %hello.build,
                "Discovered new worker via heartbeat"
            );
            outcome.newly_discovered = true;
            WorkerState::unknown()
        });
        let now = Instant::now();

        // Instance change: same URL, new process.
        if !hello.instance_id.is_nil() {
            if !state.instance_id.is_nil() && state.instance_id != hello.instance_id {
                warn!(
                    worker = url,
                    previous_instance = %state.instance_id,
                    instance_id = %hello.instance_id,
                    build = %hello.build,
                    "Worker restarted: same URL, new instance id; fragment-scoped state is gone"
                );
                outcome.instance_changed = true;
                outcome.previous_instance = Some(state.instance_id);
            }
            state.instance_id = hello.instance_id;
        }
        if !hello.is_legacy() {
            state.build = hello.build.clone();
            state.scan_task_version = hello.scan_task_version;
        }
        state.capacity = hello.capacity;

        if state.healthy {
            state.last_healthy = Some(now);
        } else {
            let quiet_elapsed = state
                .last_unhealthy_at
                .is_none_or(|t| now.duration_since(t) >= self.recovery_quiet_period);
            if hello.failures_since_last == 0 || quiet_elapsed {
                info!(
                    worker = url,
                    failures_since_last = hello.failures_since_last,
                    "Worker became healthy"
                );
                state.healthy = true;
                state.consecutive_failures = 0;
                state.last_healthy = Some(now);
                outcome.readmitted = true;
            } else {
                debug!(
                    worker = url,
                    failures_since_last = hello.failures_since_last,
                    quiet_period_secs = self.recovery_quiet_period.as_secs(),
                    "Worker heartbeat received but it still reports scan failures; \
                     holding it out of the pool until the quiet period elapses"
                );
                outcome.held_unhealthy = true;
            }
        }
        drop(inner);
        // A hello that got this far is compatible; a later mismatch is news.
        self.logged_hello_rejections.remove(url);
        Ok(outcome)
    }

    /// Immediately mark a worker as unhealthy.
    ///
    /// Unlike [`mark_failed`](Self::mark_failed), this does not use a
    /// consecutive-failure threshold — the worker is removed from the active
    /// pool right away.  Used when a worker fails during query execution
    /// (connection error, timeout, etc.) which is a stronger signal than a
    /// missed health check.
    pub async fn mark_unhealthy(&self, url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(state) = inner.workers.get_mut(url) {
            if state.healthy {
                warn!(
                    worker = url,
                    "Worker marked unhealthy immediately (execution failure)"
                );
            }
            state.healthy = false;
            state.consecutive_failures = self.max_consecutive_failures;
            state.last_unhealthy_at = Some(Instant::now());
        }
    }

    pub async fn mark_failed(&self, url: &str) {
        let mut inner = self.inner.write().await;
        let threshold = self.max_consecutive_failures;
        if let Some(state) = inner.workers.get_mut(url) {
            state.consecutive_failures += 1;
            if state.consecutive_failures >= threshold {
                if state.healthy {
                    warn!(
                        worker = url,
                        failures = state.consecutive_failures,
                        "Worker marked unhealthy after {} consecutive failures",
                        threshold
                    );
                }
                state.healthy = false;
                state.last_unhealthy_at = Some(Instant::now());
            } else {
                debug!(
                    worker = url,
                    failures = state.consecutive_failures,
                    "Worker health check failed ({}/{})",
                    state.consecutive_failures,
                    threshold
                );
            }
        }
    }

    pub fn start_health_check_task(self: &Arc<Self>, interval: Duration) -> sqe_core::TaskGuard {
        let registry = self.clone();
        sqe_core::spawn_supervised("worker-health-check", move |token| async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = ticker.tick() => registry.check_all_workers().await,
                }
            }
        })
    }

    async fn check_all_workers(&self) {
        let urls: Vec<String> = {
            let inner = self.inner.read().await;
            inner.workers.keys().cloned().collect()
        };

        // COORD-05: run per-worker checks concurrently. Previously this was a
        // sequential loop; a single kernel-paused worker that accepted the TCP
        // connection but stalled the `do_action` reply blocked the whole loop
        // for up to the pool's 30s request-timeout before the next worker was
        // checked, serializing detection latency across N stalled workers and
        // delaying failover.
        let checks = urls.into_iter().map(|url| async move {
            let result = self.health_check_worker(&url).await;
            (url, result)
        });
        let results = futures::future::join_all(checks).await;

        for (url, result) in results {
            match result {
                Ok(()) => self.mark_healthy(&url).await,
                Err(e) => {
                    debug!(worker = %url, error = %e, "Health check failed");
                    self.channel_pool.invalidate(&url);
                    self.mark_failed(&url).await;
                }
            }
        }
    }

    async fn health_check_worker(
        &self,
        url: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::Action;

        // COORD-05: bound the whole check with a short, dedicated timeout
        // (independent of the pool's 30s data-RPC budget) so a stalled worker
        // is marked failed within a few seconds, not after 30s.
        let check = async {
            let channel = self.channel_pool.get(url).await?;
            let mut client = FlightServiceClient::new(channel);
            let action = Action {
                r#type: "health_check".to_string(),
                body: bytes::Bytes::new(),
            };
            let _response = client.do_action(tonic::Request::new(action)).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };

        match tokio::time::timeout(HEALTH_CHECK_TIMEOUT, check).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "health check for worker {url} exceeded {}s",
                HEALTH_CHECK_TIMEOUT.as_secs()
            )
            .into()),
        }
    }
}

/// COORD-05: per-worker health-check timeout. Deliberately short and
/// independent of the channel pool's 30s data-RPC request-timeout so a
/// stalled worker is detected and marked failed quickly, keeping failover
/// responsive.
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_empty_registry() {
        let registry = WorkerRegistry::new(vec![]);
        assert_eq!(registry.total_workers().await, 0);
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn test_workers_start_unhealthy() {
        let registry = WorkerRegistry::new(vec![
            "http://worker1:50052".to_string(),
            "http://worker2:50052".to_string(),
        ]);
        assert_eq!(registry.total_workers().await, 2);
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn test_mark_healthy() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);

        registry.mark_healthy("http://worker1:50052").await;
        let healthy = registry.healthy_workers().await;
        assert_eq!(healthy, vec!["http://worker1:50052"]);
    }

    #[tokio::test]
    async fn test_mark_failed_threshold() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;

        // First two failures: still healthy
        registry.mark_failed("http://worker1:50052").await;
        registry.mark_failed("http://worker1:50052").await;
        assert_eq!(registry.healthy_workers().await.len(), 1);

        // Third failure: marked unhealthy
        registry.mark_failed("http://worker1:50052").await;
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn test_recovery_after_failure() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;

        for _ in 0..3 {
            registry.mark_failed("http://worker1:50052").await;
        }
        assert!(registry.healthy_workers().await.is_empty());

        registry.mark_healthy("http://worker1:50052").await;
        assert_eq!(registry.healthy_workers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_register_heartbeat_new_worker() {
        let registry = WorkerRegistry::new(vec![]);
        assert_eq!(registry.total_workers().await, 0);

        registry
            .register_heartbeat("http://worker1:50052")
            .await
            .expect("first heartbeat fits under cap");

        assert_eq!(registry.total_workers().await, 1);
        assert_eq!(
            registry.healthy_workers().await,
            vec!["http://worker1:50052"]
        );
    }

    #[tokio::test]
    async fn test_register_heartbeat_existing_worker() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        // Worker starts unhealthy
        assert!(registry.healthy_workers().await.is_empty());

        // Heartbeat marks it healthy
        registry
            .register_heartbeat("http://worker1:50052")
            .await
            .expect("known worker heartbeat accepted");
        assert_eq!(registry.healthy_workers().await.len(), 1);
        // Total count unchanged (was already registered)
        assert_eq!(registry.total_workers().await, 1);
    }

    #[tokio::test]
    async fn test_register_heartbeat_recovers_failed_worker() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;

        // Fail the worker
        for _ in 0..3 {
            registry.mark_failed("http://worker1:50052").await;
        }
        assert!(registry.healthy_workers().await.is_empty());

        // Heartbeat recovers it
        registry
            .register_heartbeat("http://worker1:50052")
            .await
            .expect("known worker heartbeat accepted");
        assert_eq!(registry.healthy_workers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_mark_unhealthy_immediate() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;
        assert_eq!(registry.healthy_workers().await.len(), 1);

        // A single mark_unhealthy call should immediately remove the worker
        registry.mark_unhealthy("http://worker1:50052").await;
        assert!(registry.healthy_workers().await.is_empty());
    }

    #[tokio::test]
    async fn test_mark_unhealthy_recovers_with_heartbeat() {
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;
        registry.mark_unhealthy("http://worker1:50052").await;
        assert!(registry.healthy_workers().await.is_empty());

        // Heartbeat should still recover after immediate unhealthy
        registry
            .register_heartbeat("http://worker1:50052")
            .await
            .expect("known worker heartbeat accepted");
        assert_eq!(registry.healthy_workers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_concurrent_health_updates() {
        // 10 tokio tasks marking the same worker healthy/unhealthy simultaneously.
        // The final state is non-deterministic, but no panics should occur.
        let registry = Arc::new(WorkerRegistry::new(
            vec!["http://worker1:50052".to_string()],
        ));
        registry.mark_healthy("http://worker1:50052").await;

        let mut handles = tokio::task::JoinSet::new();
        for i in 0..10 {
            let reg = registry.clone();
            handles.spawn(async move {
                if i % 2 == 0 {
                    reg.mark_healthy("http://worker1:50052").await;
                } else {
                    reg.mark_unhealthy("http://worker1:50052").await;
                }
            });
        }

        // Wait for all tasks to complete — no panics expected
        while let Some(result) = handles.join_next().await {
            result.expect("task should not panic");
        }

        // The worker should exist regardless of the final health state
        assert_eq!(registry.total_workers().await, 1);
        // Health state is non-deterministic, but the count must be 0 or 1
        let healthy_count = registry.healthy_workers().await.len();
        assert!(
            healthy_count <= 1,
            "healthy count should be 0 or 1, got {healthy_count}"
        );
    }

    #[tokio::test]
    async fn test_heartbeat_recovery_after_mark_failed() {
        // After mark_failed reaches the threshold, a heartbeat immediately
        // recovers the worker.
        let registry = WorkerRegistry::new(vec!["http://worker1:50052".to_string()]);
        registry.mark_healthy("http://worker1:50052").await;

        // Fail to the threshold (3 consecutive failures)
        for _ in 0..DEFAULT_MAX_CONSECUTIVE_FAILURES {
            registry.mark_failed("http://worker1:50052").await;
        }
        assert!(
            registry.healthy_workers().await.is_empty(),
            "worker should be unhealthy after reaching failure threshold"
        );

        // A single heartbeat should immediately recover it
        registry
            .register_heartbeat("http://worker1:50052")
            .await
            .expect("known worker heartbeat accepted");
        let healthy = registry.healthy_workers().await;
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0], "http://worker1:50052");
    }

    #[tokio::test]
    async fn test_many_workers() {
        // Registry with 50 workers; mark a subset healthy and verify
        // healthy_workers() returns exactly that subset.
        let urls: Vec<String> = (0..50).map(|i| format!("http://worker{i}:50052")).collect();
        let registry = WorkerRegistry::new(urls.clone());

        assert_eq!(registry.total_workers().await, 50);
        assert!(
            registry.healthy_workers().await.is_empty(),
            "all workers should start unhealthy"
        );

        // Mark even-indexed workers healthy
        let expected_healthy: Vec<String> = (0..50)
            .filter(|i| i % 2 == 0)
            .map(|i| format!("http://worker{i}:50052"))
            .collect();
        for url in &expected_healthy {
            registry.mark_healthy(url).await;
        }

        let mut healthy = registry.healthy_workers().await;
        healthy.sort();
        let mut expected_sorted = expected_healthy.clone();
        expected_sorted.sort();

        assert_eq!(healthy.len(), 25);
        assert_eq!(healthy, expected_sorted);

        // Mark some of them failed past the threshold
        for url in expected_healthy.iter().take(5) {
            for _ in 0..DEFAULT_MAX_CONSECUTIVE_FAILURES {
                registry.mark_failed(url).await;
            }
        }

        let healthy_after = registry.healthy_workers().await;
        assert_eq!(
            healthy_after.len(),
            20,
            "5 workers should have been removed from healthy pool"
        );
    }

    #[tokio::test]
    async fn test_register_heartbeat_rejects_after_cap() {
        // A worker with a rotating URL must not be able to grow the registry past
        // the configured cap.
        let registry = WorkerRegistry::with_options(vec![], ChannelPool::shared(), 2);

        // Cap=2: two unique URLs accepted, the third is rejected.
        registry
            .register_heartbeat("http://w1:50052")
            .await
            .expect("first heartbeat fits under cap");
        registry
            .register_heartbeat("http://w2:50052")
            .await
            .expect("second heartbeat fits under cap");
        let err = registry
            .register_heartbeat("http://w3:50052")
            .await
            .expect_err("third heartbeat must be refused");
        assert!(matches!(
            err,
            RegistrationError::CapacityExceeded { cap: 2 }
        ));
        assert_eq!(registry.total_workers().await, 2);

        // Heartbeats for already-known URLs still work, even at the cap.
        registry
            .register_heartbeat("http://w1:50052")
            .await
            .expect("known worker heartbeat accepted at cap");
    }

    #[tokio::test]
    async fn test_seed_urls_above_cap_are_preserved() {
        // If the seed list exceeds max_workers, all seeds are kept (the cap only
        // restricts dynamically-discovered workers).
        let registry = WorkerRegistry::with_options(
            vec!["http://w1:50052".to_string(), "http://w2:50052".to_string()],
            ChannelPool::shared(),
            1,
        );
        assert_eq!(registry.total_workers().await, 2);

        // New dynamic discovery is still rejected: the effective cap is the seed
        // size, not max_workers.
        let err = registry
            .register_heartbeat("http://w3:50052")
            .await
            .expect_err("dynamic discovery refused above effective cap");
        assert!(matches!(err, RegistrationError::CapacityExceeded { .. }));
    }

    #[tokio::test]
    async fn test_register_heartbeat_rejects_unspecified_url() {
        // A worker that advertises 0.0.0.0 (the issue #220 bug) must be
        // rejected so it cannot poison the registry. The error is the
        // dedicated InvalidAdvertiseUrl variant, and nothing is registered.
        let registry = WorkerRegistry::with_options(vec![], ChannelPool::shared(), 16);

        for bad in [
            "http://0.0.0.0:50052",
            "0.0.0.0:50052",
            "http://[::]:50052",
            "",
        ] {
            let err = registry
                .register_heartbeat(bad)
                .await
                .expect_err(&format!("expected rejection for {bad:?}"));
            assert!(
                matches!(err, RegistrationError::InvalidAdvertiseUrl { .. }),
                "wrong error for {bad:?}: {err:?}"
            );
        }
        assert_eq!(registry.total_workers().await, 0, "nothing should register");

        // A routable URL still registers.
        registry
            .register_heartbeat("http://10.1.2.3:50052")
            .await
            .expect("routable URL accepted");
        assert_eq!(registry.total_workers().await, 1);
    }

    /// Finding 408: a worker on another `sqe.proto.version` or DataFusion
    /// release is refused with the dedicated `ProtocolMismatch` variant.
    #[test]
    fn test_check_worker_protocol_refuses_mismatch() {
        use sqe_planner::{HeartbeatPayload, PROTO_VERSION};

        let current = HeartbeatPayload::current("http://w1:50052", datafusion::DATAFUSION_VERSION);
        check_worker_protocol(&current, true).expect("same build accepted in strict mode");
        check_worker_protocol(&current, false).expect("same build accepted");

        let mut other_df = current.clone();
        other_df.datafusion_version = "1.0.0".to_string();
        for strict in [true, false] {
            let err = check_worker_protocol(&other_df, strict)
                .expect_err("DataFusion skew must be refused regardless of strictness");
            assert!(matches!(
                err,
                RegistrationError::ProtocolMismatch {
                    coordinator_proto,
                    ..
                } if coordinator_proto == PROTO_VERSION
            ));
            let msg = err.to_string();
            assert!(
                msg.contains("1.0.0") && msg.contains("protocol mismatch"),
                "{msg}"
            );
        }

        let mut other_proto = current.clone();
        other_proto.proto_version = PROTO_VERSION + 1;
        assert!(matches!(
            check_worker_protocol(&other_proto, false),
            Err(RegistrationError::ProtocolMismatch { .. })
        ));
    }

    /// Finding 408: a legacy bare-URL heartbeat (version 0) is refused only
    /// under production_mode; otherwise it is accepted with a warning so a
    /// rolling upgrade keeps working.
    #[test]
    fn test_check_worker_protocol_legacy_gated_by_strict() {
        let legacy = sqe_planner::HeartbeatPayload::parse(b"http://w1:50052").unwrap();
        assert!(legacy.is_legacy());
        check_worker_protocol(&legacy, false).expect("legacy accepted when not strict");
        let err = check_worker_protocol(&legacy, true).expect_err("legacy refused when strict");
        assert!(matches!(
            err,
            RegistrationError::ProtocolMismatch {
                worker_proto: 0,
                ..
            }
        ));
    }

    /// Finding 408: the load tracker carries reserved load in `estimate_cost`
    /// units alongside the fragment count, and the guard releases both.
    #[test]
    fn test_load_tracker_reserves_and_releases_load() {
        let tracker = WorkerLoadTracker::new();
        assert_eq!(tracker.in_flight_load("http://w1:50052"), 0);

        let g1 = tracker.reserve_load("http://w1:50052", 500);
        let g2 = tracker.reserve_load("http://w1:50052", 250);
        let slot = tracker.reserve("http://w1:50052");
        assert_eq!(tracker.in_flight("http://w1:50052"), 3);
        assert_eq!(tracker.in_flight_load("http://w1:50052"), 750);

        drop(g1);
        assert_eq!(tracker.in_flight("http://w1:50052"), 2);
        assert_eq!(tracker.in_flight_load("http://w1:50052"), 250);

        drop(slot);
        drop(g2);
        assert_eq!(tracker.in_flight("http://w1:50052"), 0);
        assert_eq!(tracker.in_flight_load("http://w1:50052"), 0);
        assert_eq!(
            tracker.snapshot(),
            vec![("http://w1:50052".to_string(), 0, 0)]
        );
    }

    #[test]
    fn test_advertise_url_is_routable_classifies() {
        assert!(advertise_url_is_routable("http://10.1.2.3:50052"));
        assert!(advertise_url_is_routable("http://worker-1.svc:50052"));
        assert!(advertise_url_is_routable("https://[2001:db8::1]:50052"));
        assert!(advertise_url_is_routable("worker-1:50052"));
        assert!(!advertise_url_is_routable(""));
        assert!(!advertise_url_is_routable("   "));
        assert!(!advertise_url_is_routable("http://0.0.0.0:50052"));
        assert!(!advertise_url_is_routable("0.0.0.0:50052"));
        assert!(!advertise_url_is_routable("http://[::]:50052"));
    }

    // ---- Finding 609: hello-driven registration ----

    fn current_hello(url: &str, instance: Uuid) -> WorkerHello {
        WorkerHello::current(url, instance, "test-build", datafusion::DATAFUSION_VERSION)
    }

    /// 609 acceptance: a worker whose hello reports a `ScanTask` version the
    /// coordinator does not plan for is refused with `HelloMismatch`, is not
    /// registered, and so can never be scheduled a ticket.
    #[tokio::test]
    async fn hello_scan_task_version_mismatch_is_rejected_and_never_registered() {
        let registry = WorkerRegistry::new(vec![]);
        let mut hello = current_hello("http://w1:50052", Uuid::new_v4());
        // Older than the coordinator: it would accept only part of what the
        // coordinator sends (finding 109's rolling-upgrade hazard).
        hello.scan_task_version = sqe_planner::SCAN_TASK_VERSION_CURRENT - 1;

        let err = registry
            .register_hello(&hello)
            .await
            .expect_err("version skew must be refused");
        assert!(
            matches!(err, RegistrationError::HelloMismatch { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("ScanTask version"), "{err}");
        assert_eq!(registry.total_workers().await, 0);
        assert!(registry.healthy_workers().await.is_empty());

        // Log-once bookkeeping: first sighting is loud, repeats are quiet, a
        // different reason is loud again.
        assert!(registry.note_hello_rejected("http://w1:50052", "a"));
        assert!(!registry.note_hello_rejected("http://w1:50052", "a"));
        assert!(registry.note_hello_rejected("http://w1:50052", "b"));
    }

    /// A worker that was healthy and then comes back with a mismatched hello
    /// (upgraded on one side only) is evicted, not left in the pool.
    #[tokio::test]
    async fn hello_mismatch_evicts_a_known_healthy_worker() {
        let registry = WorkerRegistry::new(vec![]);
        let instance = Uuid::new_v4();
        registry
            .register_hello(&current_hello("http://w1:50052", instance))
            .await
            .unwrap();
        assert_eq!(registry.healthy_workers().await.len(), 1);

        let mut skewed = current_hello("http://w1:50052", instance);
        skewed.datafusion_version = "0.0.1".to_string();
        assert!(registry.register_hello(&skewed).await.is_err());
        assert!(
            registry.healthy_workers().await.is_empty(),
            "mismatched worker must leave the pool"
        );
        assert_eq!(registry.total_workers().await, 1, "still known, for diagnostics");
    }

    /// 609 acceptance: the same URL with a new `instance_id` is reported as an
    /// instance change, with the previous id, so the caller can purge
    /// fragment-scoped state. Capacity and build are stored.
    #[tokio::test]
    async fn hello_new_instance_id_is_reported_and_capacity_stored() {
        let registry = WorkerRegistry::new(vec![]);
        let first = Uuid::new_v4();
        let outcome = registry
            .register_hello(&current_hello("http://w1:50052", first))
            .await
            .unwrap();
        assert!(outcome.newly_discovered);
        assert!(!outcome.instance_changed);
        assert_eq!(registry.worker_instance("http://w1:50052").await, Some(first));

        let second = Uuid::new_v4();
        let hello = current_hello("http://w1:50052", second).with_capacity(WorkerCapacity {
            memory_limit_bytes: 8 << 30,
            scan_budget_free_bytes: 1 << 30,
            spill_free_bytes: 100 << 30,
            inflight_scans: 3,
        });
        let outcome = registry.register_hello(&hello).await.unwrap();
        assert!(!outcome.newly_discovered);
        assert!(outcome.instance_changed);
        assert_eq!(outcome.previous_instance, Some(first));
        assert_eq!(registry.worker_instance("http://w1:50052").await, Some(second));
        assert_eq!(
            registry.worker_capacity("http://w1:50052").await,
            Some(hello.capacity)
        );
        assert_eq!(
            registry.worker_build("http://w1:50052").await,
            Some(("test-build".to_string(), sqe_planner::SCAN_TASK_VERSION_CURRENT))
        );

        // Same instance again: no change reported.
        let outcome = registry
            .register_hello(&current_hello("http://w1:50052", second))
            .await
            .unwrap();
        assert!(!outcome.instance_changed);
    }

    /// A legacy body (nil instance id) never counts as an instance change, so a
    /// mixed fleet does not purge state on every heartbeat.
    #[tokio::test]
    async fn legacy_hello_never_reports_instance_change() {
        let registry = WorkerRegistry::new(vec![]);
        registry
            .register_hello(&current_hello("http://w1:50052", Uuid::new_v4()))
            .await
            .unwrap();
        let legacy = WorkerHello::parse(b"http://w1:50052").unwrap();
        let outcome = registry.register_hello(&legacy).await.unwrap();
        assert!(!outcome.instance_changed);
        assert!(!outcome.newly_discovered);
    }

    /// 609: an evicted worker whose hello still reports worker-attributable
    /// failures is held out of the pool inside the quiet period, and re-admitted
    /// once it reports a clean interval.
    #[tokio::test]
    async fn hello_with_failures_does_not_readmit_inside_quiet_period() {
        let registry = WorkerRegistry::new(vec![])
            .with_recovery_quiet_period(Duration::from_secs(3600));
        let instance = Uuid::new_v4();
        registry
            .register_hello(&current_hello("http://w1:50052", instance))
            .await
            .unwrap();
        registry.mark_unhealthy("http://w1:50052").await;
        assert!(registry.healthy_workers().await.is_empty());

        let still_failing =
            current_hello("http://w1:50052", instance).with_failures_since_last(2);
        let outcome = registry.register_hello(&still_failing).await.unwrap();
        assert!(outcome.held_unhealthy);
        assert!(!outcome.readmitted);
        assert!(
            registry.healthy_workers().await.is_empty(),
            "a worker still failing scans must not flap back on a heartbeat"
        );

        let clean = current_hello("http://w1:50052", instance);
        let outcome = registry.register_hello(&clean).await.unwrap();
        assert!(outcome.readmitted);
        assert_eq!(registry.healthy_workers().await.len(), 1);
    }

    /// 609: after the quiet period a heartbeat re-admits even if the worker
    /// still reports failures, so a bad stretch is not a permanent eviction.
    #[tokio::test]
    async fn hello_with_failures_readmits_after_quiet_period() {
        let registry = WorkerRegistry::new(vec![]).with_recovery_quiet_period(Duration::ZERO);
        let instance = Uuid::new_v4();
        registry
            .register_hello(&current_hello("http://w1:50052", instance))
            .await
            .unwrap();
        registry.mark_unhealthy("http://w1:50052").await;

        let still_failing =
            current_hello("http://w1:50052", instance).with_failures_since_last(5);
        let outcome = registry.register_hello(&still_failing).await.unwrap();
        assert!(outcome.readmitted);
        assert_eq!(registry.healthy_workers().await.len(), 1);
    }
}

/// Finding 601: check that the host a heartbeat advertises resolves to the
/// TCP peer that sent it. Literal IPs must equal the peer address; DNS names
/// must resolve to it. `peer == None` (transport did not expose an address)
/// cannot be verified and is accepted with a debug log.
pub async fn advertise_matches_peer(
    advertised_url: &str,
    peer: Option<std::net::SocketAddr>,
) -> Result<(), String> {
    let Some(peer) = peer else {
        debug!(
            worker = advertised_url,
            "heartbeat peer address unavailable; skipping advertise check"
        );
        return Ok(());
    };
    let url =
        url::Url::parse(advertised_url).map_err(|e| format!("unparsable advertise URL: {e}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| "advertise URL has no host".to_string())?
        .trim_matches(['[', ']'])
        .to_string();
    let peer_ip = canonical_ip(peer.ip());
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return if canonical_ip(ip) == peer_ip {
            Ok(())
        } else {
            Err(format!(
                "advertised {ip} but the heartbeat came from {peer_ip}"
            ))
        };
    }
    let port = url.port_or_known_default().unwrap_or(0);
    let resolved = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| format!("cannot resolve advertised host {host}: {e}"))?;
    let mut seen = Vec::new();
    for addr in resolved {
        let ip = canonical_ip(addr.ip());
        if ip == peer_ip {
            return Ok(());
        }
        seen.push(ip);
    }
    Err(format!(
        "advertised host {host} resolves to {seen:?}, but the heartbeat came from {peer_ip}"
    ))
}

fn canonical_ip(ip: std::net::IpAddr) -> std::net::IpAddr {
    match ip {
        std::net::IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(std::net::IpAddr::V4)
            .unwrap_or(std::net::IpAddr::V6(v6)),
        v4 => v4,
    }
}

#[cfg(test)]
mod advertise_peer_tests {
    use super::advertise_matches_peer;

    #[tokio::test]
    async fn literal_ip_must_equal_peer() {
        let peer = Some("127.0.0.1:41000".parse().unwrap());
        advertise_matches_peer("http://127.0.0.1:50052", peer)
            .await
            .unwrap();
        // 601 acceptance: valid secret from 127.0.0.1 advertising 10.9.9.9 is refused.
        let err = advertise_matches_peer("http://10.9.9.9:50052", peer)
            .await
            .unwrap_err();
        assert!(err.contains("10.9.9.9"), "{err}");
        // IPv4-mapped IPv6 peers are the same address.
        let mapped = Some("[::ffff:127.0.0.1]:41000".parse().unwrap());
        advertise_matches_peer("http://127.0.0.1:50052", mapped)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dns_name_must_resolve_to_peer() {
        let peer = Some("127.0.0.1:41000".parse().unwrap());
        advertise_matches_peer("http://localhost:50052", peer)
            .await
            .unwrap();
        let other = Some("10.9.9.9:41000".parse().unwrap());
        assert!(advertise_matches_peer("http://localhost:50052", other)
            .await
            .is_err());
        assert!(advertise_matches_peer("not a url", peer).await.is_err());
    }

    #[tokio::test]
    async fn unknown_peer_is_not_a_mismatch() {
        advertise_matches_peer("http://10.9.9.9:50052", None)
            .await
            .unwrap();
    }
}
