//! Shared `tonic::transport::Channel` pool keyed by URL.
//!
//! `Channel` is cheap to clone and multiplexes RPCs over a single HTTP/2
//! connection. Building a new `Endpoint::connect()` per RPC throws away the
//! handshake (TCP + TLS) every time and burns ~5-50 ms on the LAN.
//!
//! The pool caches one `Channel` per URL. Concurrent first-time connects
//! to the same URL race through a per-URL `tokio::sync::Mutex` so only one
//! handshake runs. Callers can `invalidate(url)` on connection-level errors;
//! the next call rebuilds the entry.
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, Clone, Copy)]
pub struct ChannelPoolConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub keep_alive: Option<Duration>,
}

impl Default for ChannelPoolConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            keep_alive: Some(Duration::from_secs(30)),
        }
    }
}

#[derive(Debug)]
pub struct ChannelPool {
    config: ChannelPoolConfig,
    entries: DashMap<String, Arc<ChannelEntry>>,
}

#[derive(Debug)]
struct ChannelEntry {
    channel: Mutex<Option<Channel>>,
}

impl ChannelPool {
    pub fn new(config: ChannelPoolConfig) -> Self {
        Self {
            config,
            entries: DashMap::new(),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new(ChannelPoolConfig::default()))
    }

    /// Build a shared pool whose Endpoint-level `connect_timeout` and
    /// `request_timeout` come from the deployment's configured worker
    /// timeouts rather than the hardcoded defaults.
    ///
    /// `request_timeout` matches the `worker_rpc_timeout` that
    /// `distributed_scan.rs` wraps every `do_get` with, so pooled and
    /// freshly-connected channels share one budget. Without this, a pooled
    /// channel would be killed at the 30s default even on a deployment that
    /// configured a longer rpc timeout (COORD-05 / #237).
    pub fn shared_with_timeouts(
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self::new(ChannelPoolConfig {
            connect_timeout,
            request_timeout,
            ..ChannelPoolConfig::default()
        }))
    }

    /// Return a clone of the cached channel for `url`, connecting on miss.
    pub async fn get(&self, url: &str) -> Result<Channel, tonic::transport::Error> {
        let entry = self
            .entries
            .entry(url.to_string())
            .or_insert_with(|| {
                Arc::new(ChannelEntry {
                    channel: Mutex::new(None),
                })
            })
            .clone();

        let mut guard = entry.channel.lock().await;
        if let Some(ch) = guard.as_ref() {
            return Ok(ch.clone());
        }

        let mut endpoint = Endpoint::new(url.to_string())?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);
        if let Some(ka) = self.config.keep_alive {
            endpoint = endpoint.keep_alive_timeout(ka).http2_keep_alive_interval(ka);
        }
        let channel = endpoint.connect().await?;
        *guard = Some(channel.clone());
        Ok(channel)
    }

    pub fn invalidate(&self, url: &str) {
        self.entries.remove(url);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalidate_drops_entry() {
        let pool = ChannelPool::new(ChannelPoolConfig::default());
        // Insert a fake entry by exercising the dashmap directly via get on an
        // unreachable address. The connect itself will fail, so we test the
        // entries map shape instead.
        assert!(pool.is_empty());
        let _ = pool.get("http://127.0.0.1:1").await;
        // Connect attempts populate the entries map regardless of outcome
        // since we record the slot before the handshake.
        assert_eq!(pool.len(), 1);
        pool.invalidate("http://127.0.0.1:1");
        assert!(pool.is_empty());
    }

    #[test]
    fn shared_with_timeouts_carries_configured_request_timeout() {
        // A deployment configures a 630s rpc timeout; the pooled-channel
        // request_timeout must follow it, not fall back to the 30s default.
        let pool = ChannelPool::shared_with_timeouts(
            Duration::from_secs(7),
            Duration::from_secs(630),
        );
        assert_eq!(pool.config.request_timeout, Duration::from_secs(630));
        assert_eq!(pool.config.connect_timeout, Duration::from_secs(7));
        assert_ne!(
            pool.config.request_timeout,
            ChannelPoolConfig::default().request_timeout,
            "configured rpc timeout must override the hardcoded pool default"
        );
    }

    #[tokio::test]
    async fn second_get_to_unreachable_does_not_reuse_failed_connect() {
        // A failed connect leaves the slot empty (None inside the Mutex),
        // so the next call retries rather than returning a stale clone.
        let pool = ChannelPool::new(ChannelPoolConfig::default());
        let first = pool.get("http://127.0.0.1:1").await;
        assert!(first.is_err());
        let second = pool.get("http://127.0.0.1:1").await;
        assert!(second.is_err());
        // Slot is still tracked; invalidation is the explicit cleanup.
        assert_eq!(pool.len(), 1);
    }
}
