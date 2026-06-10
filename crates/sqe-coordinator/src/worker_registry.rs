use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::channel_pool::ChannelPool;

/// Tracks in-flight fragment counts per worker URL across all concurrent
/// queries. The scheduler consults this counter as the initial load so two
/// queries planning at the same time do not both pick the same idle worker.
///
/// Increment via [`WorkerLoadTracker::reserve`] right after assignment;
/// decrement via [`ReservationGuard::drop`] when the fragment completes
/// (success, error, or cancel). The guard makes the decrement automatic
/// and panic-safe.
#[derive(Debug, Default, Clone)]
pub struct WorkerLoadTracker {
    counts: Arc<DashMap<String, AtomicU32>>,
}

impl WorkerLoadTracker {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(DashMap::new()),
        }
    }

    /// Return the current in-flight fragment count for `url`.
    pub fn in_flight(&self, url: &str) -> u32 {
        self.counts
            .get(url)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Snapshot `(url, count)` pairs without holding the map locked.
    pub fn snapshot(&self) -> Vec<(String, u32)> {
        self.counts
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().load(Ordering::Relaxed)))
            .collect()
    }

    /// Reserve a fragment slot on `url`. The returned guard decrements the
    /// counter on drop.
    pub fn reserve(&self, url: &str) -> ReservationGuard {
        self.counts
            .entry(url.to_string())
            .or_insert_with(|| AtomicU32::new(0))
            .fetch_add(1, Ordering::Relaxed);
        ReservationGuard {
            tracker: self.counts.clone(),
            url: url.to_string(),
        }
    }
}

/// RAII guard returned by [`WorkerLoadTracker::reserve`]. Decrements the
/// in-flight count for the reserved worker URL when dropped.
pub struct ReservationGuard {
    tracker: Arc<DashMap<String, AtomicU32>>,
    url: String,
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(c) = self.tracker.get(&self.url) {
            let prev = c.fetch_sub(1, Ordering::Relaxed);
            if prev == 0 {
                c.fetch_add(1, Ordering::Relaxed);
                debug!(worker = %self.url, "reservation underflow guarded");
            }
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
}

const DEFAULT_MAX_CONSECUTIVE_FAILURES: u32 = 3;
const DEFAULT_MAX_WORKERS: usize = 1024;

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
}

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
        }
    }
}

impl std::error::Error for RegistrationError {}

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
        .map_or(after_scheme.split('/').next().unwrap_or(after_scheme), |(_, host)| host);

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
            .map(|url| {
                let state = WorkerState {
                    healthy: false,
                    consecutive_failures: 0,
                    last_healthy: None,
                };
                (url, state)
            })
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
        }
    }

    pub fn channel_pool(&self) -> Arc<ChannelPool> {
        self.channel_pool.clone()
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
    /// Called when the coordinator receives a heartbeat from a worker.
    /// Workers that were not in the initial config list are dynamically added,
    /// bounded by `max_workers`. Heartbeats from previously-unknown URLs are
    /// rejected with `Err` once the cap is reached so a buggy or malicious
    /// worker reporting rotating URLs cannot grow the registry without limit.
    pub async fn register_heartbeat(&self, url: &str) -> Result<(), RegistrationError> {
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
        let state = inner.workers.entry(url.to_string()).or_insert_with(|| {
            info!(worker = url, "Discovered new worker via heartbeat");
            WorkerState {
                healthy: false,
                consecutive_failures: 0,
                last_healthy: None,
            }
        });
        if !state.healthy {
            info!(worker = url, "Worker became healthy");
        }
        state.healthy = true;
        state.consecutive_failures = 0;
        state.last_healthy = Some(Instant::now());
        Ok(())
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

    pub fn start_health_check_task(
        self: &Arc<Self>,
        interval: Duration,
    ) -> sqe_core::TaskGuard {
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
        let registry = Arc::new(WorkerRegistry::new(vec![
            "http://worker1:50052".to_string(),
        ]));
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
        let urls: Vec<String> = (0..50)
            .map(|i| format!("http://worker{i}:50052"))
            .collect();
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
}
