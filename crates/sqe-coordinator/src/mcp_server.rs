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
    /// `mcp.request_timeout_secs`; zero disables the deadline.
    request_timeout: std::time::Duration,
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
    storage: &sqe_core::config::StorageConfig,
) -> axum::Router {
    let executor: Arc<dyn sqe_mcp::QueryExecutor> = Arc::new(CoordinatorMcpExecutor {
        handler,
        rate_limiter,
        request_timeout: std::time::Duration::from_secs(config.request_timeout_secs),
    });
    sqe_mcp::router(
        sqe_mcp::McpServerState::new(auth, executor, config)
            .with_hive_roots(hive_roots)
            .with_storage_probe(storage_probe(storage)),
    )
}

/// The AWS SDK default-chain probe. The SDK config module is compiled only
/// with an AWS catalog feature (`glue` / `s3tables`); a build without one
/// cannot resolve the chain and says so instead of failing to compile.
#[cfg(feature = "glue")]
async fn sdk_chain_report() -> sqe_mcp::StorageCredentialReport {
    let r = sqe_catalog::aws_config::probe_storage_credentials().await;
    sqe_mcp::StorageCredentialReport {
        status: r.status,
        provider: r.provider,
        expires_at: r.expires_at,
        detail: r.detail,
    }
}

#[cfg(not(feature = "glue"))]
async fn sdk_chain_report() -> sqe_mcp::StorageCredentialReport {
    sqe_mcp::StorageCredentialReport {
        status: "unknown",
        provider: "aws-sdk-default-chain",
        expires_at: None,
        detail: Some(
            "built without the glue feature; the AWS SDK credential chain is not probed"
                .to_string(),
        ),
    }
}

/// The engine's object-store credential probe for `list_catalogs`
/// diagnostics, from the effective `[storage]` block.
///
/// With no static key the S3 stores read through the AWS SDK default chain
/// (`aws_config::sdk_object_store_credentials`), so the probe resolves that
/// chain once and reports `ok` with the expiry the SDK knows, or
/// `auth_error` with the reason (an SSO session that ended, a chain with no
/// provider). With a static key nothing here can see whether S3 still
/// accepts it, so the probe says `unknown` and names the kind: a session
/// token makes them temporary keys that will expire with the session that
/// issued them, which is the shape that produced "every scan fails after a
/// few hours while SELECT 1 works".
pub fn storage_probe(storage: &sqe_core::config::StorageConfig) -> sqe_mcp::StorageProbe {
    if storage.s3_access_key.is_empty() {
        return Arc::new(|| Box::pin(sdk_chain_report()));
    }
    let temporary = !storage.s3_session_token.expose().is_empty();
    Arc::new(move || {
        Box::pin(async move {
            sqe_mcp::StorageCredentialReport {
                status: "unknown",
                provider: if temporary {
                    "static-temporary-keys"
                } else {
                    "static-keys"
                },
                expires_at: None,
                detail: temporary.then(|| {
                    "static keys with a session token expire with the session that issued \
                     them and cannot be refreshed in-process; run with AWS_PROFILE for \
                     credentials the SDK renews"
                        .to_string()
                }),
            }
        })
    })
}

#[async_trait]
impl sqe_mcp::QueryExecutor for CoordinatorMcpExecutor {
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
        self.rate_limiter
            .check(&session.user.username)
            .map_err(|error| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("rate limit: {error}")))?;
        let run = self.handler.execute(session, sql, None);
        if self.request_timeout.is_zero() {
            return run.await;
        }
        // Past the deadline the future is dropped, which stops the
        // DataFusion streams and object-store calls under it, and the
        // handler's guard marks the query abandoned. Before this, an MCP
        // caller that gave up after ninety seconds and retried left the
        // first attempt running to the five-minute query timeout beside the
        // second: four such attempts on two snapshot tables held the
        // coordinator for half an hour and 1.8 GB.
        match tokio::time::timeout(self.request_timeout, run).await {
            Ok(result) => result,
            Err(_) => Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!(
                    "the statement did not finish within the MCP deadline of {}s and was \
                     cancelled (mcp.request_timeout_secs); narrow it with a partition \
                     predicate or a LIMIT, or raise the deadline",
                    self.request_timeout.as_secs()
                ),
            )),
        }
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
// One call site (sqe-server main) and one test; the arguments are the
// coordinator's shared wiring, not a configuration surface to bundle.
#[allow(clippy::too_many_arguments)]
pub async fn start_mcp_server(
    auth: Arc<dyn AuthProvider>,
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
    config: McpConfig,
    hive_roots: Vec<String>,
    tls: &TlsConfig,
    alive: Arc<AtomicBool>,
    storage: &sqe_core::config::StorageConfig,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
    // Loopback by default in anonymous mode, every interface otherwise;
    // `validate` already rejected an address that does not parse (#671).
    let ip = config.bind_ip().map_err(anyhow::Error::msg)?;
    let addr = SocketAddr::new(ip, config.port);
    let app = mcp_router(auth, handler, rate_limiter, config, hive_roots, storage);
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
