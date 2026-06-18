use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use sqe_core::SqeConfig;

use sqe_catalog::grant_chameleon::ChameleonGrantBackend;
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::QueryHandler;
use sqe_coordinator::SessionManager;
use sqe_policy::grants::{polaris::PolarisGrantBackend, ranger::RangerGrantBackend, GrantBackend};

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
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        self.authenticator
            .authenticate(username, password)
            .await
            .map_err(|e| sqe_core::SqeError::Auth(e.to_string()))
    }

    async fn authenticate_bearer(
        &self,
        token: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        let provider = self.bearer_provider.as_ref().ok_or_else(|| {
            sqe_core::SqeError::Auth(
                "Bearer token authentication is not configured".to_string(),
            )
        })?;

        let credentials = sqe_auth::FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token.to_string())),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&credentials)
            .await
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("Bearer token validation failed: {e}"))
            })?;

        // Convert Identity to Session: use the JWT itself as the catalog token
        // (passthrough to Polaris), and the identity fields for user/roles.
        let token_expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        Ok(sqe_core::Session::new(
            identity.user_id,
            identity
                .catalog_token
                .unwrap_or_else(|| sqe_core::SecretString::new(token.to_string())),
            None,
            token_expiry,
            identity.roles,
        ))
    }
}

struct QueryHandlerAdapter {
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<sqe_coordinator::rate_limiter::QueryRateLimiter>,
}

#[async_trait::async_trait]
impl TrinoQueryExecutor for QueryHandlerAdapter {
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
        self.rate_limiter
            .check(&session.user.username)
            .map_err(|e| sqe_core::SqeError::Execution(format!("rate limit: {e}")))?;
        self.handler.execute(session, sql).await
    }
}

// We build the tokio runtime manually instead of using `#[tokio::main]` so we
// can set a larger thread stack. See the matching note in `bin/sqe_server.rs`.
// The 2 MiB default is too small for DataFusion's AST walkers on deep WHERE
// trees produced by CoW DML rewrites; 8 MiB gives ~4x headroom at no runtime
// cost. Keep this binary and `sqe_server` in sync.
fn main() -> anyhow::Result<()> {
    const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(WORKER_STACK_BYTES)
        .thread_name("sqe-coordinator")
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build tokio runtime: {e}"))?;

    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
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
    if config.auth.should_skip_tls_verify()
        || config
            .auth
            .external
            .as_ref()
            .is_some_and(|e| e.accept_invalid_certs)
    {
        // AUTH-04: the external (interactive) auth path has its own
        // accept_invalid_certs flag that was not covered by this warning.
        tracing::warn!("WARNING: TLS certificate verification is DISABLED for auth endpoints (auth.tls_skip_verify / auth.ssl_verification / auth.external.accept_invalid_certs) -- vulnerable to MITM. Disable these for production.");
    }
    // Fire whenever the unauthenticated-discovery path is actually live: the
    // worker registry is attached (worker_urls non-empty), there is no secret
    // to check (worker_secret empty), and the operator has waived the
    // empty-secret refusal. A configured secret is enforced on the heartbeat
    // path even with the waiver set, so gating on the empty secret avoids a
    // false positive on authenticated-but-waived deployments. Shared with
    // bin/sqe_server.rs so the two coordinator binaries cannot drift.
    if sqe_coordinator::mode::warns_unauthenticated_workers(&config.coordinator) {
        tracing::warn!("WARNING: coordinator.allow_unauthenticated_workers = true with an empty coordinator.worker_secret -- any client reachable on the cluster network can register as a worker and receive user bearer tokens. Set worker_secret for production.");
    }
    // AUTH-01: policy enforcement is hardcoded to passthrough below. If the
    // operator configured a real engine (OPA/Cedar/InMemory) it is SILENTLY
    // IGNORED -- every row filter and column mask is unenforced. Fail loud so
    // an operator is never lulled into believing SQE-side policy is active.
    if config.policy.engine != sqe_core::config::PolicyEngine::Passthrough {
        tracing::error!(
            configured_engine = ?config.policy.engine,
            "CRITICAL: policy.engine is set but policy enforcement is NOT wired -- \
             the engine runs PASSTHROUGH and NO row filters or column masks are \
             applied. Do not rely on SQE-side policy until this is implemented."
        );
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

    // Track supervised background tasks for the lifetime of the binary.
    let mut _task_guards: Vec<sqe_core::TaskGuard> = Vec::new();

    // Initialize auth
    let authenticator = Arc::new(sqe_auth::Authenticator::new(&config.auth).await?);
    _task_guards.push(authenticator.start_refresh_task());

    // Build the auth provider chain from `[[auth.providers]]`. The chain
    // dispatches to `oidc_password`, `bearer_token`, `client_credentials`,
    // `mtls`, etc. based on the credential shape. Without this wiring the
    // Flight SQL handshake path saw only the legacy Authenticator and
    // rejected every bearer-only request as `NotMyCredentials` — even
    // when the same JWT was accepted by the Trino-compat HTTP path
    // (which has always had its own chain). When `[[auth.providers]]`
    // is empty, build_auth_chain wraps the legacy Authenticator in a
    // single-provider chain so behaviour is unchanged for legacy
    // configs.
    let auth_chain: Arc<dyn sqe_auth::AuthProvider> =
        Arc::new(sqe_auth::build_auth_chain(&config.auth).await?);

    // Initialize session manager. The chain authenticates new requests;
    // the legacy Authenticator stays attached so the background refresh
    // task it owns continues to keep username/password tokens current.
    let session_manager = Arc::new(SessionManager::with_provider_and_legacy(
        Arc::clone(&auth_chain),
        authenticator.clone(),
    ));

    // Initialize policy (passthrough)
    // TODO(AUTH-01): wire PolicyPlanRewriter from config.policy.engine (Opa ->
    // OpaStore, InMemory -> InMemoryPolicyStore) instead of hardcoding
    // PassthroughEnforcer. Until then a non-passthrough engine is ignored and
    // the startup error! above fires.
    let policy_enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
        Arc::new(sqe_policy::PassthroughEnforcer);

    // Initialize worker registry
    let worker_registry = Arc::new(
        sqe_coordinator::worker_registry::WorkerRegistry::with_options(
            config.coordinator.worker_urls.clone(),
            sqe_coordinator::channel_pool::ChannelPool::shared_with_timeouts(
                std::time::Duration::from_secs(config.coordinator.worker_connect_timeout_secs),
                std::time::Duration::from_secs(config.coordinator.worker_rpc_timeout_secs),
            ),
            config.coordinator.max_workers,
        ),
    );

    // Start background health checks (every 5 seconds). Keep the TaskGuard
    // for the lifetime of the binary so cancellation fires at shutdown.
    if !config.coordinator.worker_urls.is_empty() {
        _task_guards.push(
            worker_registry.start_health_check_task(std::time::Duration::from_secs(5)),
        );
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

    // Manifest-list and manifest caching is delegated to iceberg-rust's
    // per-`Table` `ObjectCache`. Because `TableMetadataCache` (built below)
    // caches `Table` instances globally, the per-table object cache persists
    // across queries and sessions.

    // Build the global table metadata cache (shared across all sessions and queries).
    // Table metadata is user-independent. Schema, partitions, and snapshots are the
    // same regardless of who queries. The cache is invalidated on DDL/DML operations.
    let table_cache = sqe_catalog::TableMetadataCache::new(config.catalog.metadata_cache_ttl_secs)
        .with_metrics(Arc::clone(&metrics));
    tracing::info!(
        metadata_cache_ttl_secs = config.catalog.metadata_cache_ttl_secs,
        "Initialized global table metadata cache (shared across all sessions)"
    );

    let grant_backend: Option<Arc<dyn GrantBackend>> = build_grant_backend(&config)?;

    // Initialize query handler
    let query_handler = Arc::new(
        QueryHandler::new(
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
            grant_backend,
            None, // lineage observer — wired in a later phase
            sqe_coordinator::RuntimeCatalogRegistry::default(),
            sqe_core::SecretStore::default(),
        )?
        .with_table_cache(table_cache)
        .with_session_manager(session_manager.clone()),
    );

    // Spawn background memory metrics reporter (updates gauges every 1s for Grafana)
    sqe_coordinator::memory::spawn_metrics_reporter(
        query_handler.runtime().clone(),
        metrics.clone(),
    );

    // Bearer auth chain for the Trino-compat HTTP path. Reuses the same
    // chain instance the Flight SQL path uses (built above via
    // `build_auth_chain` and stored in `auth_chain`) so both endpoints
    // see identical provider behaviour.
    let bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>> =
        Some(Arc::clone(&auth_chain));

    // Construct OAuth2 external auth state from [auth.external] config (if present).
    let oauth2_state: Option<Arc<sqe_trino_compat::oauth2::OAuth2State>> =
        if let Some(ref ext) = config.auth.external {
            match build_oauth2_state(ext, &config) {
                Ok(state) => {
                    tracing::info!(
                        issuer = %ext.issuer,
                        "External auth (OAuth2) enabled for Trino SSO"
                    );
                    Some(Arc::new(state))
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to initialize external auth; Trino SSO will be disabled"
                    );
                    None
                }
            }
        } else {
            None
        };

    // Rate limiters — shared between Flight SQL and Trino paths
    let rate_limiter = Arc::new(sqe_coordinator::rate_limiter::QueryRateLimiter::new(
        &config.rate_limit,
    ));
    let auth_rate_limiter = Arc::new(sqe_coordinator::rate_limiter::AuthRateLimiter::new(
        &config.rate_limit,
    ));
    let metadata_rate_limiter = Arc::new(sqe_coordinator::rate_limiter::MetadataRateLimiter::new(
        &config.rate_limit,
    ));

    // Start Trino-compat HTTP server
    if config.coordinator.trino_http_port > 0 {
        let auth_adapter = Arc::new(AuthenticatorAdapter {
            authenticator: authenticator.clone(),
            bearer_provider: bearer_provider.clone(),
        });
        let handler_adapter = Arc::new(QueryHandlerAdapter {
            handler: query_handler.clone(),
            rate_limiter: Arc::clone(&rate_limiter),
        });
        let trino_auth_limiter: Arc<dyn sqe_trino_compat::server::TrinoAuthRateLimiter> =
            Arc::clone(&auth_rate_limiter) as _;
        let trino_opts = sqe_trino_compat::server::TrinoServerOptions {
            security: config.security.clone(),
            auth_rate_limiter: Some(trino_auth_limiter),
            expose_version: false,
        };
        sqe_trino_compat::server::start_trino_server_with_options(
            auth_adapter,
            handler_adapter,
            config.coordinator.trino_http_port,
            NodeContext {
                version: sqe_core::VERSION.to_string(),
                ready: ready.clone(),
                started_at,
            },
            oauth2_state,
            trino_opts,
        );
        tracing::info!(
            "Trino-compat HTTP server on port {}",
            config.coordinator.trino_http_port
        );
    }

    // Start Flight SQL server
    let mut flight_service =
        SqeFlightSqlService::new(session_manager, query_handler, config.clone())
            .with_rate_limiter(Arc::clone(&rate_limiter))
            .with_auth_rate_limiter(Arc::clone(&auth_rate_limiter))
            .with_metadata_rate_limiter(Arc::clone(&metadata_rate_limiter));
    if !config.coordinator.worker_urls.is_empty() {
        flight_service = flight_service.with_worker_registry(worker_registry.clone());
    }
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    // Optional TLS
    let tls_config = sqe_coordinator::tls::build_server_tls_config(&config.coordinator.tls)?;

    let mut server_builder = sqe_coordinator::transport::apply_grpc_transport(
        tonic::transport::Server::builder(),
        &config.coordinator.transport,
    );
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
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("SQE coordinator shut down");
    Ok(())
}

/// Resolve once a SIGINT or SIGTERM arrives. Drives `serve_with_shutdown` so
/// tonic stops accepting new connections and lets in-flight RPCs finish at the
/// graceful boundary instead of being hard-killed on signal. Mirrors the
/// `shutdown_signal` helper in `bin/sqe_server.rs` (#225). This compose-path
/// binary has no health server, so there is no readiness flag to flip; the
/// readiness-flip drain lives in `sqe_server.rs` (#250).
async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, shutting down"),
        _ = terminate => tracing::info!("Received SIGTERM, shutting down"),
    }
}

// ── External auth (OAuth2) construction ───────────────────────

/// Build the [`OAuth2State`] from the `[auth.external]` config section.
fn build_oauth2_state(
    ext: &sqe_core::config::ExternalAuthConfig,
    config: &SqeConfig,
) -> anyhow::Result<sqe_trino_compat::oauth2::OAuth2State> {
    let discovery_config = sqe_auth::OidcDiscoveryConfig {
        issuer: ext.issuer.clone(),
        authorization_endpoint_override: ext.authorization_endpoint.clone(),
        token_endpoint_override: ext.token_endpoint.clone(),
        device_authorization_endpoint_override: ext.device_authorization_endpoint.clone(),
        accept_invalid_certs: ext.accept_invalid_certs,
    };

    let discovery = Arc::new(
        sqe_auth::OidcDiscovery::new(discovery_config)
            .map_err(|e| anyhow::anyhow!("OIDC discovery init failed: {e}"))?,
    );

    let auth_code_service = Arc::new(sqe_auth::AuthCodeService::new(
        discovery.clone(),
        ext.client_id.clone(),
        ext.client_secret.clone(),
        ext.redirect_uri.clone(),
        ext.scopes.clone(),
    ));

    let pending_store = Arc::new(sqe_auth::PendingAuthStore::new(
        std::time::Duration::from_secs(ext.challenge_timeout_secs),
    ));

    let base_url = if ext.redirect_uri.contains("://") {
        let uri = ext.redirect_uri.trim_end_matches('/');
        match uri.find("://") {
            Some(idx) => {
                let after_scheme = &uri[idx + 3..];
                let host_end = after_scheme.find('/').unwrap_or(after_scheme.len());
                format!("{}://{}", &uri[..idx], &after_scheme[..host_end])
            }
            None => format!("http://localhost:{}", config.coordinator.trino_http_port),
        }
    } else {
        format!("http://localhost:{}", config.coordinator.trino_http_port)
    };

    Ok(sqe_trino_compat::oauth2::OAuth2State {
        auth_code_service,
        pending_store,
        base_url,
    })
}

fn build_grant_backend(
    config: &SqeConfig,
) -> anyhow::Result<Option<Arc<dyn GrantBackend>>> {
    use sqe_core::config::AccessControlBackend;
    match config.access_control.backend {
        AccessControlBackend::Chameleon if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "chameleon",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            let client = Arc::new(sqe_catalog::AccessControlClient::new(
                &config.access_control.url,
            )?);
            Ok(Some(Arc::new(ChameleonGrantBackend::new(client))))
        }
        AccessControlBackend::Polaris if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "polaris",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            Ok(Some(Arc::new(PolarisGrantBackend::new(
                &config.access_control.url,
                config.access_control.client_id.clone(),
                config.access_control.client_secret.clone(),
            )?)))
        }
        AccessControlBackend::Ranger if !config.access_control.url.is_empty() => {
            let r = &config.access_control.ranger;
            tracing::info!(
                backend = "ranger",
                url = %config.access_control.url,
                service = %r.service_name,
                "Access control backend configured"
            );
            Ok(Some(Arc::new(RangerGrantBackend::new(
                &config.access_control.url,
                &r.service_name,
                &r.admin_user,
                &r.admin_password,
                &r.realm,
                r.timeout_secs,
                r.accept_invalid_certs,
            )?)))
        }
        AccessControlBackend::None
        | AccessControlBackend::Chameleon
        | AccessControlBackend::Polaris
        | AccessControlBackend::Ranger => Ok(None),
    }
}
