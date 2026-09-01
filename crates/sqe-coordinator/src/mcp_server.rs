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
        let _ = rustls::crypto::ring::default_provider().install_default();
        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_file, &tls.key_file)
                .await?;
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
