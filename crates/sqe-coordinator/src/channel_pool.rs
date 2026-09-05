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
//!
//! Finding 603: this pool used to build every channel with
//! `Endpoint::new(url)` and never call `tls_config`, so an `https://` worker
//! URL was rejected by tonic before a single byte left the coordinator
//! ("Connecting to HTTPS without TLS enabled"). Turning `[coordinator.tls]` on
//! therefore made distributed execution impossible, and leaving it off sent
//! every user's S3 credentials over the wire in cleartext. The pool now
//! carries a client TLS config built once from the deployment's `TlsConfig`
//! and attaches it to any `https://` target, with the URL host as the SNI
//! domain.
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use sqe_core::config::{GrpcTransportConfig, TlsConfig};
use tokio::sync::Mutex;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::transport::apply_grpc_transport_endpoint;

#[derive(Debug, Clone)]
pub struct ChannelPoolConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub keep_alive: Option<Duration>,
    /// HTTP/2 receive windows for the pooled channel. tonic leaves both at
    /// h2's 65,535-byte default, which caps each worker's scan stream at
    /// `window / RTT`. See `transport::apply_grpc_transport_endpoint`.
    pub transport: GrpcTransportConfig,
    /// Client TLS for `https://` targets, built once from `[coordinator.tls]`
    /// (trust root from `ca_file`, else the platform store; our own cert
    /// attached so an mTLS worker accepts us). Built at pool construction
    /// rather than per connect so `get()` never does file I/O and stays
    /// infallible with respect to a missing CA.
    pub client_tls: Option<ClientTlsConfig>,
}

impl Default for ChannelPoolConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            keep_alive: Some(Duration::from_secs(30)),
            transport: GrpcTransportConfig::default(),
            // No configured TLS block: an https target still gets a client
            // config (platform trust store) rather than tonic's hard refusal.
            client_tls: sqe_core::tls::tonic_client(&TlsConfig::default(), None).ok(),
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
        transport: &GrpcTransportConfig,
    ) -> Arc<Self> {
        Arc::new(Self::new(ChannelPoolConfig {
            connect_timeout,
            request_timeout,
            transport: *transport,
            ..ChannelPoolConfig::default()
        }))
    }

    /// [`Self::shared_with_timeouts`] plus the deployment's TLS block, which
    /// becomes the client config for every `https://` worker URL.
    ///
    /// This is the constructor the coordinator binaries use. Without it the
    /// pool falls back to the platform trust store, which will not contain a
    /// private cluster CA.
    ///
    /// # Errors
    /// Returns [`sqe_core::SqeError::Config`] when `ca_file`, `cert_file` or
    /// `key_file` cannot be read, so a bad TLS block fails at coordinator boot
    /// rather than at the first dispatch.
    pub fn shared_with_timeouts_and_tls(
        connect_timeout: Duration,
        request_timeout: Duration,
        transport: &GrpcTransportConfig,
        tls: &TlsConfig,
    ) -> sqe_core::Result<Arc<Self>> {
        Ok(Arc::new(Self::new(ChannelPoolConfig {
            connect_timeout,
            request_timeout,
            transport: *transport,
            client_tls: Some(sqe_core::tls::tonic_client(tls, None)?),
            ..ChannelPoolConfig::default()
        })))
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

        let mut endpoint =
            apply_grpc_transport_endpoint(Endpoint::new(url.to_string())?, &self.config.transport)
                .connect_timeout(self.config.connect_timeout)
                .timeout(self.config.request_timeout);
        if let Some(ka) = self.config.keep_alive {
            endpoint = endpoint
                .keep_alive_timeout(ka)
                .http2_keep_alive_interval(ka);
        }
        // An https target without a ClientTlsConfig is refused by tonic before
        // the connect even starts, which is finding 603 in one line.
        if sqe_core::tls::is_https_url(url) {
            let mut client_tls = self.config.client_tls.clone().unwrap_or_default();
            if let Some(domain) = sqe_core::tls::sni_domain(url) {
                client_tls = client_tls.domain_name(domain);
            }
            endpoint = endpoint.tls_config(client_tls)?;
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
    fn pool_windows_are_never_tonic_defaults() {
        // h2 falls back to 65,535 bytes when tonic is handed `None`, which
        // caps a worker's scan stream at window/RTT. Both the shipped default
        // and an explicitly configured transport must clear that by orders of
        // magnitude; a regression here is invisible on loopback.
        const H2_DEFAULT_WINDOW: u32 = 65_535;

        let shipped = ChannelPoolConfig::default().transport;
        assert!(
            shipped.initial_stream_window_size > H2_DEFAULT_WINDOW * 16,
            "stream window {} is at or near h2's default",
            shipped.initial_stream_window_size
        );
        assert!(
            shipped.initial_connection_window_size >= shipped.initial_stream_window_size,
            "connection window must not be tighter than one stream's"
        );

        let configured = GrpcTransportConfig {
            initial_stream_window_size: 4 * 1024 * 1024,
            initial_connection_window_size: 12 * 1024 * 1024,
            ..GrpcTransportConfig::default()
        };
        let pool = ChannelPool::shared_with_timeouts(
            Duration::from_secs(7),
            Duration::from_secs(630),
            &configured,
        );
        assert_eq!(
            pool.config.transport.initial_stream_window_size,
            4 * 1024 * 1024
        );
        assert_eq!(
            pool.config.transport.initial_connection_window_size,
            12 * 1024 * 1024
        );
    }

    #[test]
    fn shared_with_timeouts_carries_configured_request_timeout() {
        // A deployment configures a 630s rpc timeout; the pooled-channel
        // request_timeout must follow it, not fall back to the 30s default.
        let pool = ChannelPool::shared_with_timeouts(
            Duration::from_secs(7),
            Duration::from_secs(630),
            &GrpcTransportConfig::default(),
        );
        assert_eq!(pool.config.request_timeout, Duration::from_secs(630));
        assert_eq!(pool.config.connect_timeout, Duration::from_secs(7));
        assert_ne!(
            pool.config.request_timeout,
            ChannelPoolConfig::default().request_timeout,
            "configured rpc timeout must override the hardcoded pool default"
        );
    }

    #[test]
    fn channel_pool_carries_client_tls_from_the_config_block() {
        let tls = TlsConfig {
            ca_file: format!(
                "{}/../sqe-core/tests/fixtures/tls/ca.crt",
                env!("CARGO_MANIFEST_DIR")
            ),
            ..TlsConfig::default()
        };
        let pool = ChannelPool::shared_with_timeouts_and_tls(
            Duration::from_secs(5),
            Duration::from_secs(30),
            &GrpcTransportConfig::default(),
            &tls,
        )
        .unwrap();
        let rendered = format!("{:?}", pool.config.client_tls.as_ref().unwrap());
        assert!(
            rendered.contains("certs: [Certificate"),
            "coordinator.tls.ca_file must become the pool's trust root: {rendered}"
        );
    }

    #[test]
    fn channel_pool_surfaces_a_bad_ca_at_construction() {
        // Fail at coordinator boot, not at the first dispatch.
        let tls = TlsConfig {
            ca_file: "/nonexistent/ca.pem".to_string(),
            ..TlsConfig::default()
        };
        let err = ChannelPool::shared_with_timeouts_and_tls(
            Duration::from_secs(5),
            Duration::from_secs(30),
            &GrpcTransportConfig::default(),
            &tls,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("Failed to read TLS CA"), "got: {err}");
    }

    #[tokio::test]
    async fn channel_pool_https_target_is_not_refused_for_missing_tls() {
        // Regression for finding 603. Before the fix tonic rejected the URI
        // outright with "Connecting to HTTPS without TLS enabled"; the connect
        // must now fail on the network (nothing listens on port 1), which is a
        // completely different error.
        let pool = ChannelPool::new(ChannelPoolConfig::default());
        let err = pool
            .get("https://127.0.0.1:1")
            .await
            .expect_err("nothing listens on port 1");

        let mut chain = err.to_string();
        let mut source: Option<&(dyn std::error::Error + 'static)> =
            std::error::Error::source(&err);
        while let Some(cause) = source {
            chain.push_str(&format!(" | {cause}"));
            source = cause.source();
        }
        assert!(
            !chain.to_lowercase().contains("without tls"),
            "https target still refused for want of a ClientTlsConfig: {chain}"
        );
    }

    #[test]
    fn channel_pool_default_still_has_a_client_tls_for_https() {
        // `ChannelPool::shared()` is used by tests and by the worker registry
        // default path; an https worker must not become unreachable there.
        assert!(ChannelPoolConfig::default().client_tls.is_some());
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
