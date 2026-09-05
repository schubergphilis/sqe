//! Embedded MCP endpoint wiring for the coordinator binary.

use std::{net::SocketAddr, sync::Arc};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use sqe_auth::AuthProvider;
use sqe_core::{
    config::{McpConfig, TlsConfig},
    Session, SqeError,
};

use crate::{
    rate_limiter::{AuthRateLimiter, QueryRateLimiter},
    QueryHandler,
};

impl sqe_mcp::AuthRateLimiter for AuthRateLimiter {
    fn allow(&self, peer_ip: &str) -> bool {
        self.check(peer_ip, "mcp-bearer").is_ok()
    }
}

struct CoordinatorMcpExecutor {
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
}

pub fn mcp_router(
    auth: Arc<dyn AuthProvider>,
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
    auth_rate_limiter: Arc<AuthRateLimiter>,
    config: McpConfig,
) -> axum::Router {
    let executor: Arc<dyn sqe_mcp::QueryExecutor> = Arc::new(CoordinatorMcpExecutor {
        handler,
        rate_limiter,
    });
    let auth_limiter: Arc<dyn sqe_mcp::AuthRateLimiter> = auth_rate_limiter;
    sqe_mcp::router(
        sqe_mcp::McpServerState::new(auth, executor, config).with_auth_rate_limiter(auth_limiter),
    )
}

#[async_trait]
impl sqe_mcp::QueryExecutor for CoordinatorMcpExecutor {
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
        self.rate_limiter
            .check(&session.user.username)
            .map_err(|error| SqeError::Execution(format!("rate limit: {error}")))?;
        self.handler.execute(session, sql, None).await
    }
}

/// Start MCP inside the coordinator process, sharing auth, query admission,
/// policy enforcement, and source credentials with Flight SQL and Trino.
pub async fn start_mcp_server(
    auth: Arc<dyn AuthProvider>,
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<QueryRateLimiter>,
    auth_rate_limiter: Arc<AuthRateLimiter>,
    config: McpConfig,
    tls: &TlsConfig,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let app = mcp_router(auth, handler, rate_limiter, auth_rate_limiter, config);

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

        Ok(tokio::spawn(async move {
            if let Err(error) = axum_server::bind_rustls(addr, rustls_config)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                tracing::error!(%error, "MCP HTTPS server exited");
            }
        }))
    } else {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(tokio::spawn(async move {
            if let Err(error) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            {
                tracing::error!(%error, "MCP HTTP server exited");
            }
        }))
    }
}
