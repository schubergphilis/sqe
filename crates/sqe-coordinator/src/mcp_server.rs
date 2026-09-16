//! Embedded MCP endpoint wiring for the coordinator binary.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use sqe_auth::AuthProvider;
use sqe_core::{
    config::{McpConfig, TlsConfig},
    Session, SqeError, SqeErrorCode,
};

use crate::{rate_limiter::QueryRateLimiter, QueryHandler};

struct CoordinatorMcpExecutor {
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
}

/// The `[[hive.roots]]` catalog names, which the MCP change tool treats as
/// engine-credential targets (see `McpConfig::write_catalogs`). Session
/// `ATTACH`ed Hive catalogs cannot exist over MCP: a session there lives for
/// one request.
pub fn hive_root_names(config: &sqe_core::config::SqeConfig) -> Vec<String> {
    config.hive.roots.iter().map(|root| root.name.clone()).collect()
}

/// `auth` is the coordinator's provider chain, already wrapped in the
/// pre-auth rate limiter (#562); MCP sends the peer address in the
/// credentials and maps the denial to 429.
pub fn mcp_router(
    auth: Arc<dyn AuthProvider>,
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
    config: McpConfig,
    hive_roots: Vec<String>,
) -> axum::Router {
    let executor: Arc<dyn sqe_mcp::QueryExecutor> = Arc::new(CoordinatorMcpExecutor {
        handler,
        rate_limiter,
    });
    sqe_mcp::router(sqe_mcp::McpServerState::new(auth, executor, config).with_hive_roots(hive_roots))
}

#[async_trait]
impl sqe_mcp::QueryExecutor for CoordinatorMcpExecutor {
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
        self.rate_limiter
            .check(&session.user.username)
            .map_err(|error| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("rate limit: {error}")))?;
        self.handler.execute(session, sql, None).await
    }
}

/// Start MCP inside the coordinator process, sharing auth, query admission,
/// policy enforcement, and source credentials with Flight SQL and Trino.
///
/// Returns the serve task and the address it is bound to (`config.port`
/// may be 0 for an ephemeral port, which tests use). `alive` is cleared the
/// moment the serve loop ends for any reason, including a panic, so the
/// ops port can report a dead `/mcp` on a live PID; the caller decides what
/// to do about it (`sqe-server` exits, see `watch_mcp_listener` there).
pub async fn start_mcp_server(
    auth: Arc<dyn AuthProvider>,
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
    config: McpConfig,
    hive_roots: Vec<String>,
    tls: &TlsConfig,
    alive: Arc<AtomicBool>,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let app = mcp_router(auth, handler, rate_limiter, config, hive_roots);
    // Bind synchronously up front so both branches can report the port they
    // actually got, and so a bind failure is this call's error, not a log
    // line from a task nobody joins.
    let std_listener = std::net::TcpListener::bind(addr)?;
    std_listener.set_nonblocking(true)?;
    let bound = std_listener.local_addr()?;

    if tls.is_enabled() {
        // Checked, not discarded: the old `let _ = ...install_default()`
        // silently no-opped when another component had installed a provider
        // first, so the MCP port could end up on a different crypto backend
        // than the Flight port with no trace of it (finding 210).
        crate::tls::ensure_crypto_provider();

        // One builder for every listener. This is what makes `ca_file` apply
        // to MCP: the old path called `RustlsConfig::from_pem_file` directly
        // and ignored the CA entirely, so an mTLS deployment had an mTLS
        // Flight port and a server-only MCP port on the same certificate.
        let material = sqe_core::ServerTlsMaterial::load(tls)?;
        if material.requires_client_cert() {
            tracing::info!(
                ca_file = %tls.ca_file,
                "MCP HTTPS listener requires a client certificate (mTLS)"
            );
        }
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_config(material.rustls_server_config());

        // Rotation without a restart. The Flight listener cannot do this; see
        // the module docs on `crate::tls`.
        let _reload_task = crate::tls::spawn_cert_reload_watch(
            tls.clone(),
            rustls_config.clone(),
            crate::tls::DEFAULT_CERT_RELOAD_INTERVAL,
        );

        let task = supervise_listener("MCP HTTPS", alive, async move {
            axum_server::from_tcp_rustls(std_listener, rustls_config)
                .map_err(anyhow::Error::from)?
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .map_err(anyhow::Error::from)
        });
        Ok((task, bound))
    } else {
        let listener = tokio::net::TcpListener::from_std(std_listener)?;
        let task = supervise_listener("MCP HTTP", alive, async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .map_err(anyhow::Error::from)
        });
        Ok((task, bound))
    }
}

/// Run a serve loop as a task that cannot end silently. `axum::serve` only
/// returns on an accept error it does not retry, and a panic inside the
/// accept loop would otherwise unwind into tokio's default hook and a
/// dropped `JoinHandle`: the process stays up, `pgrep` says healthy, and
/// `/mcp` refuses connections until someone restarts it (seen twice on
/// 2026-09-16 after 30 to 60 minutes of idle time). Every exit path here
/// logs at ERROR and clears `alive`, which `/healthz` and `/readyz` read.
fn supervise_listener<F>(
    what: &'static str,
    alive: Arc<AtomicBool>,
    serve: F,
) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    use futures::FutureExt;
    /// Clears the flag however the task ends: a return, a panic, or an
    /// `abort` (which drops the future without running anything after the
    /// await). A liveness flag that survives its task would be a lie.
    struct AliveGuard(Arc<AtomicBool>);
    impl Drop for AliveGuard {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    tokio::spawn(async move {
        let _guard = AliveGuard(alive);
        match std::panic::AssertUnwindSafe(serve).catch_unwind().await {
            Ok(Ok(())) => {
                tracing::error!("{what} server exited without an error; the listener is closed")
            }
            Ok(Err(error)) => tracing::error!(%error, "{what} server exited"),
            Err(payload) => {
                let panic = payload
                    .downcast_ref::<&'static str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                tracing::error!(%panic, "{what} server panicked");
            }
        }
    })
}
