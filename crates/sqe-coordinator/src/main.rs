use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use sqe_core::SqeConfig;

use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::QueryHandler;
use sqe_coordinator::SessionManager;

// Trino adapter types
use sqe_trino_compat::server::{NodeContext, TrinoAuthenticator, TrinoQueryExecutor};

struct AuthenticatorAdapter {
    authenticator: Arc<sqe_auth::Authenticator>,
    bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>>,
}

#[async_trait::async_trait]
impl TrinoAuthenticator for AuthenticatorAdapter {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<sqe_core::Session, String> {
        self.authenticator
            .authenticate(username, password)
            .await
            .map_err(|e| e.to_string())
    }

    async fn authenticate_bearer(&self, token: &str) -> Result<sqe_core::Session, String> {
        let provider = self
            .bearer_provider
            .as_ref()
            .ok_or_else(|| "Bearer token authentication is not configured".to_string())?;

        let credentials = sqe_auth::FlightCredentials {
            bearer_token: Some(token.to_string()),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&credentials)
            .await
            .map_err(|e| format!("Bearer token validation failed: {e}"))?;

        // Convert Identity to Session: use the JWT itself as the catalog token
        // (passthrough to Polaris), and the identity fields for user/roles.
        let token_expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        Ok(sqe_core::Session::new(
            identity.user_id,
            identity.catalog_token.unwrap_or_else(|| token.to_string()),
            None,
            token_expiry,
            identity.roles,
        ))
    }
}

struct QueryHandlerAdapter(Arc<QueryHandler>);

#[async_trait::async_trait]
impl TrinoQueryExecutor for QueryHandlerAdapter {
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, String> {
        self.0
            .execute(session, sql)
            .await
            .map_err(|e| e.to_string())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let started_at = Instant::now();
    let ready = Arc::new(AtomicBool::new(false));

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sqe.toml".to_string());
    let config = SqeConfig::load(&config_path)?;
    config.validate()?;

    // Security warnings for production readiness
    if !config.coordinator.tls.is_enabled() {
        tracing::warn!("WARNING: TLS is DISABLED -- Flight SQL and worker connections are unencrypted. Set [coordinator.tls] cert_file and key_file for production.");
    }
    if !config.rate_limit.enabled {
        tracing::warn!("WARNING: Rate limiting is DISABLED -- no protection against query flooding. Set [rate_limit] enabled = true for production.");
    }
    if !config.auth.ssl_verification {
        tracing::warn!("WARNING: SSL certificate verification is DISABLED for auth endpoints -- vulnerable to MITM. Set auth.ssl_verification = true for production.");
    }

    // Initialize tracing + OTel (traces, metrics, logs via OTLP when configured)
    let _otel_guard = sqe_metrics::otel::init_telemetry_with_sampling(
        "sqe-coordinator",
        &config.metrics.otlp_endpoint,
        config.metrics.trace_sample_rate,
    );

    tracing::info!(
        "Starting SQE coordinator on Flight SQL port {}",
        config.coordinator.flight_sql_port
    );

    // Initialize auth
    let authenticator = Arc::new(sqe_auth::Authenticator::new(&config.auth).await?);
    authenticator.start_refresh_task();

    // Initialize session manager
    let session_manager = Arc::new(SessionManager::new(authenticator.clone()));

    // Initialize policy (passthrough)
    let policy_enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
        Arc::new(sqe_policy::PassthroughEnforcer);

    // Initialize worker registry
    let worker_registry = Arc::new(
        sqe_coordinator::worker_registry::WorkerRegistry::new(
            config.coordinator.worker_urls.clone(),
        ),
    );

    // Start background health checks (every 5 seconds)
    if !config.coordinator.worker_urls.is_empty() {
        worker_registry.start_health_check_task(std::time::Duration::from_secs(5));
        tracing::info!(
            workers = ?config.coordinator.worker_urls,
            "Started worker health check task"
        );
    }

    // Initialize metrics
    let metrics = Arc::new(sqe_metrics::MetricsRegistry::new());
    let audit = Arc::new(
        sqe_metrics::audit::AuditLogger::new(&config.metrics.audit_log_path)
            .map_err(|e| anyhow::anyhow!(e))?,
    );

    // Start metrics server
    sqe_metrics::server::start_metrics_server(metrics.clone(), config.metrics.prometheus_port);
    tracing::info!(
        "Prometheus metrics on port {}",
        config.metrics.prometheus_port
    );

    // Initialize query tracker and result cache
    let query_tracker = Arc::new(
        sqe_coordinator::query_tracker::QueryTracker::new(&config.query_history),
    );
    let query_cache = if config.query_cache.enabled {
        Some(Arc::new(sqe_coordinator::query_cache::ResultCache::new(&config.query_cache, Some(metrics.clone()))))
    } else {
        None
    };

    // Initialize query handler
    let query_handler = Arc::new(QueryHandler::new(
        policy_enforcer,
        None, // policy_store — wired when policy engine is enabled
        config.clone(),
        if config.coordinator.worker_urls.is_empty() {
            None
        } else {
            Some(worker_registry.clone())
        },
        None, // credential tracker — wired via sqe_server binary
        Some(metrics.clone()),
        Some(audit.clone()),
        query_tracker,
        query_cache,
    ));

    // Build bearer token auth chain for Trino-compat HTTP bearer token validation.
    // This uses the same provider chain configured in [auth.providers], which may
    // include a BearerTokenProvider with JWKS validation.
    let bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>> =
        match sqe_auth::build_auth_chain(&config.auth).await {
            Ok(chain) => {
                tracing::info!("Bearer token auth chain built for Trino-compat endpoint");
                Some(Arc::new(chain))
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to build bearer auth chain; bearer token auth will be disabled for Trino-compat"
                );
                None
            }
        };

    // Start Trino-compat HTTP server
    if config.coordinator.trino_http_port > 0 {
        let auth_adapter = Arc::new(AuthenticatorAdapter {
            authenticator: authenticator.clone(),
            bearer_provider: bearer_provider.clone(),
        });
        let handler_adapter = Arc::new(QueryHandlerAdapter(query_handler.clone()));
        sqe_trino_compat::server::start_trino_server(
            auth_adapter,
            handler_adapter,
            config.coordinator.trino_http_port,
            NodeContext {
                version: sqe_core::VERSION.to_string(),
                ready: ready.clone(),
                started_at,
            },
            None, // OAuth2 external auth — wired when [auth.external] is configured
        );
        tracing::info!(
            "Trino-compat HTTP server on port {}",
            config.coordinator.trino_http_port
        );
    }

    // Start Flight SQL server
    let mut flight_service =
        SqeFlightSqlService::new(session_manager, query_handler, config.clone());
    if !config.coordinator.worker_urls.is_empty() {
        flight_service = flight_service.with_worker_registry(worker_registry.clone());
    }
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    // Optional TLS
    let tls_config = sqe_coordinator::tls::build_server_tls_config(&config.coordinator.tls)?;

    let mut server_builder = tonic::transport::Server::builder();
    if let Some(tls) = tls_config {
        server_builder = server_builder.tls_config(tls)?;
        tracing::info!("SQE coordinator listening on {} (TLS)", addr);
    } else {
        tracing::info!("SQE coordinator listening on {} (plaintext)", addr);
    }

    server_builder
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(
            flight_service,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
