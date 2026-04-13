use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Tracks available workers and their health status.
///
/// Workers are discovered from config and health-checked periodically.
/// Unhealthy workers (3 consecutive failed health checks) are removed
/// from the active pool but retained in the registry for recovery.
#[derive(Debug, Clone)]
pub struct WorkerRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

#[derive(Debug)]
struct RegistryInner {
    workers: HashMap<String, WorkerState>,
}

#[derive(Debug)]
struct WorkerState {
    url: String,
    healthy: bool,
    consecutive_failures: u32,
    last_healthy: Option<Instant>,
}

const MAX_CONSECUTIVE_FAILURES: u32 = 3;

impl WorkerRegistry {
    pub fn new(worker_urls: Vec<String>) -> Self {
        let workers: HashMap<String, WorkerState> = worker_urls
            .into_iter()
            .map(|url| {
                let state = WorkerState {
                    url: url.clone(),
                    healthy: false,
                    consecutive_failures: 0,
                    last_healthy: None,
                };
                (url, state)
            })
            .collect();

        info!(worker_count = workers.len(), "Initialized worker registry");

        Self {
            inner: Arc::new(RwLock::new(RegistryInner { workers })),
        }
    }

    pub async fn healthy_workers(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        inner
            .workers
            .values()
            .filter(|w| w.healthy)
            .map(|w| w.url.clone())
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
    /// Workers that were not in the initial config list are dynamically added.
    pub async fn register_heartbeat(&self, url: &str) {
        let mut inner = self.inner.write().await;
        let state = inner.workers.entry(url.to_string()).or_insert_with(|| {
            info!(worker = url, "Discovered new worker via heartbeat");
            WorkerState {
                url: url.to_string(),
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
            state.consecutive_failures = MAX_CONSECUTIVE_FAILURES;
        }
    }

    pub async fn mark_failed(&self, url: &str) {
        let mut inner = self.inner.write().await;
        if let Some(state) = inner.workers.get_mut(url) {
            state.consecutive_failures += 1;
            if state.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                if state.healthy {
                    warn!(
                        worker = url,
                        failures = state.consecutive_failures,
                        "Worker marked unhealthy after {} consecutive failures",
                        MAX_CONSECUTIVE_FAILURES
                    );
                }
                state.healthy = false;
            } else {
                debug!(
                    worker = url,
                    failures = state.consecutive_failures,
                    "Worker health check failed ({}/{})",
                    state.consecutive_failures,
                    MAX_CONSECUTIVE_FAILURES
                );
            }
        }
    }

    pub fn start_health_check_task(self: &Arc<Self>, interval: Duration) {
        let registry = self.clone();
        // TODO(security-hardening): store JoinHandle and add CancellationToken
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                registry.check_all_workers().await;
            }
        });
    }

    async fn check_all_workers(&self) {
        let urls: Vec<String> = {
            let inner = self.inner.read().await;
            inner.workers.keys().cloned().collect()
        };

        for url in urls {
            let result = Self::health_check_worker(&url).await;
            match result {
                Ok(()) => self.mark_healthy(&url).await,
                Err(e) => {
                    debug!(worker = %url, error = %e, "Health check failed");
                    self.mark_failed(&url).await;
                }
            }
        }
    }

    async fn health_check_worker(url: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::Action;
        use tonic::transport::Endpoint;

        let channel = Endpoint::new(url.to_string())?.connect().await?;
        let mut client = FlightServiceClient::new(channel);
        let action = Action {
            r#type: "health_check".to_string(),
            body: bytes::Bytes::new(),
        };
        let _response = client.do_action(tonic::Request::new(action)).await?;
        Ok(())
    }
}

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
            .await;

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
            .await;
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
            .await;
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
            .await;
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
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            registry.mark_failed("http://worker1:50052").await;
        }
        assert!(
            registry.healthy_workers().await.is_empty(),
            "worker should be unhealthy after reaching failure threshold"
        );

        // A single heartbeat should immediately recover it
        registry
            .register_heartbeat("http://worker1:50052")
            .await;
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
            for _ in 0..MAX_CONSECUTIVE_FAILURES {
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
}
