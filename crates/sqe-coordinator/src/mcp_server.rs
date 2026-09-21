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

use crate::{
    rate_limiter::{MetadataRateLimiter, QueryRateLimiter},
    QueryHandler,
};

struct CoordinatorMcpExecutor {
    handler: Arc<QueryHandler>,
    /// Statements the caller asked for: `rate_limit.per_user_queries_per_minute`.
    rate_limiter: Arc<QueryRateLimiter>,
    /// `mcp.request_timeout_secs`; zero disables the deadline.
    request_timeout: std::time::Duration,
    /// One charge per metadata tool call: `rate_limit.metadata_per_user_per_minute`.
    /// The `SHOW`s inside the call are not charged (#699: `search_tables` over
    /// 135 schemas burned the query budget and dropped the tail).
    metadata_rate_limiter: Arc<MetadataRateLimiter>,
}

/// Catalog names the MCP change tool treats as engine-credential targets
/// (see `McpConfig::write_catalogs`): every `[[hive.roots]]` entry, plus any
/// configured catalog that does not carry the caller's own bearer to the
/// catalog, i.e. a non-REST backend (Glue, HMS, JDBC, S3 Tables) or a REST
/// catalog whose `auth` is static, client-credentials, anonymous or AWS
/// (#718). A write there runs as the coordinator, so it needs an explicit
/// opt-in. Session `ATTACH`ed catalogs cannot exist over MCP: a session there
/// lives for one request.
pub fn engine_credential_catalog_names(config: &sqe_core::config::SqeConfig) -> Vec<String> {
    use sqe_core::config::{CatalogAuthConfig, CatalogBackend};
    let mut names: Vec<String> = config.hive.roots.iter().map(|root| root.name.clone()).collect();
    for (name, catalog) in config.flattened_catalogs() {
        let per_user_rest = matches!(catalog.backend, CatalogBackend::Rest)
            && matches!(catalog.auth, None | Some(CatalogAuthConfig::SessionBearer));
        if !per_user_rest {
            names.push(name);
        }
    }
    names
}

/// `auth` is the coordinator's provider chain, already wrapped in the
/// pre-auth rate limiter (#562); MCP sends the peer address in the
/// credentials and maps the denial to 429.
#[allow(clippy::too_many_arguments)]
pub fn mcp_router(
    auth: Arc<dyn AuthProvider>,
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
    metadata_rate_limiter: Arc<MetadataRateLimiter>,
    config: McpConfig,
    hive_roots: Vec<String>,
    storage: &sqe_core::config::StorageConfig,
    rate_limit: &sqe_core::config::RateLimitConfig,
) -> axum::Router {
    let executor: Arc<dyn sqe_mcp::QueryExecutor> = Arc::new(CoordinatorMcpExecutor {
        handler,
        rate_limiter,
        request_timeout: std::time::Duration::from_secs(config.request_timeout_secs),
        metadata_rate_limiter,
    });
    // The whole request, not just the statement: auth (a JWKS fetch), the
    // metadata fan-out and the encoding run outside the executor's deadline,
    // and before this the only bound on a request was the engine's five
    // minute query timeout (#679). Sits above the statement deadline so a
    // statement times out with its own message first.
    let request_deadline = (config.request_timeout_secs > 0).then(|| {
        std::time::Duration::from_secs(config.request_timeout_secs + REQUEST_DEADLINE_MARGIN_SECS)
    });
    sqe_mcp::router(
        sqe_mcp::McpServerState::new(auth, executor, config)
            .with_hive_roots(hive_roots)
            .with_storage_probe(storage_probe(storage))
            .with_auth_rate(rate_limit.auth_attempts_per_minute),
    )
    .layer(axum::middleware::from_fn_with_state(
        request_deadline,
        request_deadline_layer,
    ))
}

/// Added to `mcp.request_timeout_secs` for the request-level deadline.
const REQUEST_DEADLINE_MARGIN_SECS: u64 = 15;

/// How long in-flight MCP requests get to finish after the shutdown signal.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// Signal the MCP listener to stop accepting and drain (#679). Re-exported
/// so the binary does not need the crate for one type.
pub use tokio_util::sync::CancellationToken;

async fn request_deadline_layer(
    axum::extract::State(deadline): axum::extract::State<Option<std::time::Duration>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(deadline) = deadline else {
        return next.run(request).await;
    };
    match tokio::time::timeout(deadline, next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            axum::Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {
                    "code": -32000,
                    "message": format!(
                        "the request did not finish within {}s (mcp.request_timeout_secs + {}) and was cancelled",
                        deadline.as_secs(), REQUEST_DEADLINE_MARGIN_SECS
                    )
                }
            })),
        )
            .into_response(),
    }
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
        // Keyed on `(peer, user)`, not the user alone (#680): in static and
        // anonymous mode every caller is one configured user, and a per-user
        // limit would be one shared global that any single client can spend.
        let key = match &session.peer_ip {
            Some(peer) => format!("{peer}|{}", session.user.username),
            None => session.user.username.clone(),
        };
        // The denial already carries the code, the knob and the wait.
        self.rate_limiter.check(&key)?;
        with_deadline(self.request_timeout, self.handler.execute(session, sql, None)).await
    }

    /// Same deadline as `execute` (#738), and at most
    /// [`METADATA_STATEMENTS_MAX`] in flight process-wide (#678): each
    /// `search_tables` / `describe_schema` call fans out eight statements,
    /// so concurrent calls multiplied planner work without a bound. Waiting
    /// for a permit counts against the deadline.
    async fn execute_metadata(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
        with_deadline(self.request_timeout, async {
            let _permit = METADATA_STATEMENTS
                .acquire()
                .await
                .expect("metadata statement semaphore is never closed");
            self.handler.execute(session, sql, None).await
        })
        .await
    }

    fn admit_metadata_call(&self, session: &Session) -> Result<(), SqeError> {
        self.metadata_rate_limiter.check(&session.user.username)
    }
}

/// Metadata statements (the `SHOW`s a tool issues) in flight at once, across
/// every MCP request in the process (#678).
// ponytail: fixed constant; make it `mcp.metadata_concurrency` if a deployment needs to tune it.
const METADATA_STATEMENTS_MAX: usize = 16;
static METADATA_STATEMENTS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(METADATA_STATEMENTS_MAX);

/// Run a statement under `mcp.request_timeout_secs`; zero disables it.
///
/// Past the deadline the future is dropped, which stops the DataFusion
/// streams and object-store calls under it, and the handler's guard marks
/// the query abandoned. Before this, an MCP caller that gave up after
/// ninety seconds and retried left the first attempt running to the
/// five-minute query timeout beside the second: four such attempts on two
/// snapshot tables held the coordinator for half an hour and 1.8 GB.
async fn with_deadline<F>(timeout: std::time::Duration, run: F) -> Result<Vec<RecordBatch>, SqeError>
where
    F: std::future::Future<Output = Result<Vec<RecordBatch>, SqeError>>,
{
    if timeout.is_zero() {
        return run.await;
    }
    match tokio::time::timeout(timeout, run).await {
        Ok(result) => result,
        Err(_) => Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "the statement did not finish within the MCP deadline of {}s and was \
                 cancelled (mcp.request_timeout_secs); narrow it with a partition \
                 predicate or a LIMIT, or raise the deadline",
                timeout.as_secs()
            ),
        )),
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
    metadata_rate_limiter: Arc<MetadataRateLimiter>,
    config: McpConfig,
    hive_roots: Vec<String>,
    tls: &TlsConfig,
    alive: Arc<AtomicBool>,
    storage: &sqe_core::config::StorageConfig,
    rate_limit: &sqe_core::config::RateLimitConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
    // Loopback by default in anonymous mode, every interface otherwise;
    // `validate` already rejected an address that does not parse (#671).
    let ip = config.bind_ip().map_err(anyhow::Error::msg)?;
    let addr = SocketAddr::new(ip, config.port);
    let app = mcp_router(auth, handler, rate_limiter, metadata_rate_limiter, config, hive_roots, storage, rate_limit);
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
        // the module docs on `crate::tls`. Supervised (#679): a reloader that
        // died would go on serving the certificate it last loaded, and an
        // expired certificate is the kind of failure nobody sees in a log
        // until a client refuses to connect.
        let reload_task = crate::tls::spawn_cert_reload_watch(
            tls.clone(),
            rustls_config.clone(),
            crate::tls::DEFAULT_CERT_RELOAD_INTERVAL,
        );
        let reload_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let outcome = reload_task.await;
            if reload_shutdown.is_cancelled() {
                return;
            }
            match outcome {
                Ok(()) => tracing::error!(
                    "MCP TLS certificate reload task exited; the certificate loaded at startup \
                     is served until a restart"
                ),
                Err(error) => tracing::error!(
                    %error,
                    "MCP TLS certificate reload task panicked; the certificate loaded at startup \
                     is served until a restart"
                ),
            }
        });

        let handle = axum_server::Handle::new();
        let stop = handle.clone();
        let drain = shutdown.clone();
        tokio::spawn(async move {
            drain.cancelled().await;
            stop.graceful_shutdown(Some(SHUTDOWN_GRACE));
        });
        let task = supervise_listener("MCP HTTPS", alive, shutdown, async move {
            axum_server::from_tcp_rustls(std_listener, rustls_config)
                .map_err(anyhow::Error::from)?
                .handle(handle)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .map_err(anyhow::Error::from)
        });
        Ok((task, bound))
    } else {
        let listener = tokio::net::TcpListener::from_std(std_listener)?;
        let drain = shutdown.clone();
        let task = supervise_listener("MCP HTTP", alive, shutdown, async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move { drain.cancelled().await })
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
    shutdown: CancellationToken,
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
            // The one expected exit: the shutdown signal drained it (#679).
            Ok(Ok(())) if shutdown.is_cancelled() => {
                tracing::info!("{what} server drained and closed on shutdown")
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::config::{CatalogAuthConfig, CatalogBackend, SqeConfig};

    #[test]
    fn engine_credential_catalogs_are_named_per_user_rest_is_not() {
        let mut config: SqeConfig = toml::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/sqe-test.toml"
        )))
        .expect("example config parses");
        config.hive.roots.clear();
        config.catalogs.clear();
        let mut polaris = config.catalog.clone();
        polaris.backend = CatalogBackend::Rest;
        polaris.auth = None;
        let mut static_rest = polaris.clone();
        static_rest.auth = Some(CatalogAuthConfig::Static {
            token: "t".to_string(),
        });
        let mut glue = polaris.clone();
        glue.backend = CatalogBackend::Glue {
            region: "eu-central-1".to_string(),
            warehouse: String::new(),
            endpoint: None,
        };
        config.catalogs.insert("polaris".to_string(), polaris);
        config.catalogs.insert("partner".to_string(), static_rest);
        config.catalogs.insert("glue".to_string(), glue);

        let mut names = engine_credential_catalog_names(&config);
        names.sort();
        assert_eq!(names, vec!["glue".to_string(), "partner".to_string()]);
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;

    /// #738: the metadata lane shares the statement deadline.
    #[tokio::test]
    async fn a_hanging_statement_is_cut_at_the_deadline() {
        let hang = std::future::pending::<Result<Vec<RecordBatch>, SqeError>>();
        let err = with_deadline(std::time::Duration::from_millis(20), hang)
            .await
            .expect_err("deadline");
        assert!(err.to_string().contains("MCP deadline"), "{err}");
        let quick = with_deadline(std::time::Duration::ZERO, async { Ok(Vec::new()) }).await;
        assert!(quick.is_ok());
    }
}
