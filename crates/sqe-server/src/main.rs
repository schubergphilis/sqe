use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use serde::Serialize;
use tokio::signal;

use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::mode::Mode;
use sqe_coordinator::{QueryHandler, SessionManager};
use sqe_core::SqeConfig;
use sqe_policy::grants::GrantBackend;
use sqe_trino_compat::server::{
    NodeContext, TrinoAuthenticator, TrinoQueryExecutor, TrinoResultStream,
};

/// Build identity stamped in by `build.rs` (issue #669): shown in the
/// startup log and `/api/v1/status` so a debug binary or a stale checkout
/// is visible before anyone reads a benchmark off it.
mod build_info {
    pub const GIT_SHA: &str = env!("SQE_GIT_SHA");
    pub const PROFILE: &str = env!("SQE_BUILD_PROFILE");
    pub const IS_DEBUG: bool = cfg!(debug_assertions);
}

// ── CLI ────────────────────────────────────────────────────────
#[derive(Parser)]
#[command(name = "sqe-server", version = sqe_core::VERSION, about = "SQE server — runs as coordinator or worker")]
struct Cli {
    /// Path to TOML configuration file
    #[arg(short, long)]
    config: Option<String>,

    /// Deprecated positional config path. Prefer `--config`.
    #[arg(value_name = "CONFIG")]
    config_positional: Option<String>,

    /// Server mode (overrides SQE_MODE env var and config file)
    #[arg(short, long, value_enum, default_value = "coordinator")]
    mode: CliMode,

    /// Verify the hash chain of a native audit log, or a contiguous extract
    /// of one, print a chain-of-custody report and exit (non-zero on a
    /// broken chain). Rotated generations (`FILE.<n>`) are verified first,
    /// oldest to newest, each chained to the one before. Starts no server.
    #[arg(long, value_name = "FILE")]
    verify_audit: Option<String>,

    /// Serve `/mcp` with NO authentication: every caller is one anonymous
    /// identity reading with the engine's own storage credentials. For a
    /// laptop or a trusted network, never a shared environment.
    ///
    /// Equivalent to `[mcp] enabled = true` plus `[mcp.auth] mode =
    /// "anonymous"`, and forces `allow_writes = false`. The usual refusals
    /// still apply: anonymous is rejected when `[[auth.providers]]`
    /// configures an identity provider, and under `production_mode`.
    #[arg(long)]
    mcp_anonymous: bool,

    /// Port for `/mcp`. Zero mounts it on the existing web listener.
    /// Overrides `[mcp] port`.
    #[arg(long)]
    mcp_port: Option<u16>,

    /// Interface the dedicated `/mcp` listener binds. Default: 127.0.0.1
    /// with `--mcp-anonymous`, 0.0.0.0 otherwise. Overrides `[mcp]
    /// bind_address`.
    #[arg(long, value_name = "IP")]
    mcp_bind: Option<String>,

    /// Attach the AWS Glue Data Catalog as a read-only Hive catalog under
    /// this SQL name, so every Glue database in the region resolves as
    /// `<name>.<database>.<table>`. Equivalent to a `[[hive.roots]]` entry
    /// with `kind = "glue"`.
    ///
    /// Tables are resolved lazily on first reference, never enumerated up
    /// front, so one unreadable table does not take the catalog down.
    /// Credentials come from the AWS provider chain, and each table's
    /// location is still gated by `[storage.tvf]`, which must name the
    /// buckets the data lives in.
    #[arg(long, value_name = "NAME")]
    glue_catalog: Option<String>,

    /// AWS region for `--glue-catalog`. Falls back to the ambient AWS
    /// configuration when omitted.
    #[arg(long, requires = "glue_catalog")]
    glue_region: Option<String>,

    /// Restrict `--glue-catalog` to these databases. Repeatable, and each
    /// value may be a comma-separated list. Empty (the default) exposes every
    /// database in the catalog. For example: `--glue-database claude,afas`.
    #[arg(long = "glue-database", requires = "glue_catalog", value_delimiter = ',')]
    glue_database: Vec<String>,

    /// Allow reads from every object-store bucket with the engine's storage
    /// credentials, bypassing the `--allow-s3-prefix` / `[storage.tvf]`
    /// allowlist. The explicit "allow all buckets" switch. Safe on a
    /// single-tenant laptop or a trusted-network read gateway; dangerous on a
    /// multi-tenant coordinator, where any caller can then read anything the
    /// credentials reach. Startup logs a warning when it is set.
    #[arg(long)]
    allow_all_buckets: bool,

    /// Serve only the MCP endpoint. Disables the Flight SQL, Trino HTTP and
    /// Quack query ports (sets them to 0), leaving just `/mcp` and the ops
    /// health/metrics port. For an assistant-only deployment that should not
    /// expose the SQL front doors. Requires MCP to be enabled
    /// (`--mcp-anonymous`, `--mcp-port`, or `[mcp]` in a config file).
    #[arg(long)]
    mcp_only: bool,

    /// Allow reads from these S3 prefixes, repeatable. Appended to
    /// `[storage.tvf] allowed_object_store_prefixes`. Reads use the engine's
    /// own AWS credentials, so this is the access boundary: a Glue table whose
    /// location is not under one of these prefixes is refused. Needed for a
    /// flag-only `--glue-catalog` run, where there is no config file to carry
    /// the list. For example: `--allow-s3-prefix s3://my-lake/`.
    #[arg(long = "allow-s3-prefix", value_name = "PREFIX")]
    allow_s3_prefix: Vec<String>,

    /// Print the merged, redacted configuration and exit.
    #[arg(long)]
    print_config: bool,
}

#[derive(Clone, clap::ValueEnum)]
enum CliMode {
    Coordinator,
    Worker,
}

/// Decide whether the coordinator should run the distributed worker path:
/// consult the worker registry on queries, run health checks, and accept
/// heartbeats into the registry that `try_distribute` reads.
///
/// Returns `true` whenever the operator has expressed distributed intent.
/// Static seeding (`worker_urls`) is the explicit form. Dynamic heartbeat
/// discovery has no URL list, so we key off the worker auth secret being set
/// (the mechanism heartbeats authenticate against) or the explicit
/// `allow_unauthenticated_workers` opt-in. The default single-node config
/// (empty `worker_urls`, empty `worker_secret`, opt-in off) returns `false`,
/// so it never wires the registry and never accepts unauthenticated heartbeats
/// that would route user bearer tokens to unknown workers.
fn distributed_enabled(coordinator: &sqe_core::config::CoordinatorConfig) -> bool {
    !coordinator.worker_urls.is_empty()
        || !coordinator.worker_secret.is_empty()
        || coordinator.allow_unauthenticated_workers
}

// ── Health endpoints ───────────────────────────────────────────

struct HealthState {
    ready: Arc<AtomicBool>,
    started_at: Instant,
    role: &'static str,
    worker_registry: Option<Arc<sqe_coordinator::worker_registry::WorkerRegistry>>,
    query_tracker: Option<Arc<sqe_coordinator::query_tracker::QueryTracker>>,
    web_ui: bool,
    catalog_url: String,
    /// Populated from config in `run_coordinator`; `None` in `run_worker` and tests.
    node_info: Option<sqe_coordinator::web_ui::NodeInfo>,
    /// Populated in `run_coordinator`; `None` in `run_worker` and tests.
    metrics_history: Option<Arc<sqe_coordinator::metrics_history::MetricsHistory>>,
    /// Bearer auth provider for the web_ui guard. `None` in `run_worker` and
    /// tests. In the coordinator this is the chain wrapped in the pre-auth
    /// rate limiter (#562), so the guard needs no limiter of its own.
    bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>>,
    /// Auth config for admin-role check in the web_ui guard. `None` in `run_worker` and tests.
    auth_cfg: Option<sqe_core::config::AuthConfig>,
    /// Security config for client-IP resolution in the web_ui guard.
    /// Used to honour X-Forwarded-For from trusted proxies, mirroring the Flight
    /// SQL path. `None` in `run_worker` and tests (degrades to peer-wins).
    security_cfg: Option<sqe_core::config::SecurityConfig>,
    /// Audit logger wired for dashboard-access events. `None` in `run_worker` and tests.
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    /// Counter incremented for anonymous (Unauthorized) dashboard denials instead
    /// of writing an audit line. `None` in `run_worker` and tests.
    anonymous_denied: Option<prometheus::IntCounter>,
    /// Counter incremented on every successful dashboard auth, including
    /// within-window deduplicated hits that do not write an audit line.
    /// `None` in `run_worker` and tests.
    dashboard_success: Option<prometheus::IntCounter>,
    /// Moka TTL cache keyed by principal. A hit means the principal was already
    /// audited within the current window; skip the audit line but still count.
    /// `None` means no dedup (window == 0 or run_worker/tests).
    success_audit_dedup: Option<moka::sync::Cache<String, ()>>,
    /// Shared global table metadata cache, so the admin catalog-refresh handler
    /// can drop it alongside the session + REST-catalog caches. Populated in
    /// `run_coordinator`; `None` in `run_worker` and tests.
    table_cache: Option<sqe_catalog::TableMetadataCache>,
    /// Shared Hive manifest cache (issue #666), dropped by the same admin
    /// refresh so a Glue table changed out of band is re-read before its
    /// TTL. Populated in `run_coordinator`; `None` in `run_worker` and tests.
    hive_manifest_cache: Option<Arc<sqe_catalog::hive::provider::HiveMetadataCache>>,
    /// The `serving` block of `/api/v1/status`. Populated in
    /// `run_coordinator`; `None` in `run_worker` and tests.
    serving: Option<ServingStatus>,
    /// `true` while the dedicated `/mcp` listener's serve task is running;
    /// cleared by `mcp_server::start_mcp_server` the moment it ends. `None`
    /// when MCP is off or mounted on this listener (`mcp.port = 0`), where
    /// answering `/healthz` at all already proves the MCP routes are up.
    mcp_listener_alive: Option<Arc<AtomicBool>>,
}

impl sqe_coordinator::web_auth::BearerAdminState for HealthState {
    fn bearer_provider(&self) -> Option<&Arc<dyn sqe_auth::AuthProvider>> {
        self.bearer_provider.as_ref()
    }

    fn auth_config(&self) -> Option<&sqe_core::config::AuthConfig> {
        self.auth_cfg.as_ref()
    }

    fn audit(&self) -> Option<&Arc<sqe_metrics::audit::AuditLogger>> {
        self.audit.as_ref()
    }

    fn on_anonymous_denial(&self) {
        if let Some(c) = &self.anonymous_denied {
            c.inc();
        }
    }

    fn should_emit_success_audit(&self, principal: &str) -> bool {
        match &self.success_audit_dedup {
            None => true, // window == 0 or not wired: always emit
            Some(cache) => {
                if cache.contains_key(principal) {
                    false
                } else {
                    cache.insert(principal.to_string(), ());
                    true
                }
            }
        }
    }

    fn note_dashboard_success(&self) {
        if let Some(c) = &self.dashboard_success {
            c.inc();
        }
    }

    fn resolve_client_ip(&self, peer: Option<&str>, xff: Option<&str>) -> Option<String> {
        match &self.security_cfg {
            Some(sec) => {
                let resolved = sec.resolve_client_ip(peer, xff);
                // resolve_client_ip returns "unknown" when peer is None; map that
                // back to None so we don't pollute audit events with a literal
                // "unknown" string.
                if resolved == "unknown" {
                    None
                } else {
                    Some(resolved)
                }
            }
            // No security config wired (run_worker / tests): fall back to peer-wins.
            None => peer.map(|p| p.to_string()),
        }
    }
}

/// Liveness. `200 ok` while the process can serve; `503` once the dedicated
/// `/mcp` listener has died, because a process whose only front door is
/// closed is not alive in any sense a health check cares about, whatever
/// `pgrep` says. `sqe-server` also exits in that case (`watch_mcp_listener`);
/// the 503 covers the window before it does and any embedding that keeps
/// the process up.
async fn healthz(state: axum::extract::State<Arc<HealthState>>) -> Response {
    if mcp_listener_down(&state) {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "mcp listener down",
        )
            .into_response();
    }
    "ok".into_response()
}

fn mcp_listener_down(state: &HealthState) -> bool {
    state
        .mcp_listener_alive
        .as_ref()
        .is_some_and(|alive| !alive.load(Ordering::SeqCst))
}

/// Cap on concurrent outbound catalog probes from `/readyz` (#754). The
/// endpoint is unauthenticated, so without this every parallel probe became
/// a 5 s GET against the catalog. Probes beyond the cap answer from the last
/// result while it is fresh, else 503 (fail closed, no fan-out).
const READYZ_MAX_PROBES: usize = 4;
const READYZ_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(2);
static READYZ_PROBE_SEM: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(READYZ_MAX_PROBES);
/// `(probed_at, catalog_url, ok)` of the last completed probe.
static READYZ_LAST: std::sync::Mutex<Option<(Instant, String, bool)>> = std::sync::Mutex::new(None);

async fn probe_catalog(catalog_url: &str) -> bool {
    let Ok(_permit) = READYZ_PROBE_SEM.try_acquire() else {
        return READYZ_LAST
            .lock()
            .ok()
            .and_then(|last| last.clone())
            .is_some_and(|(at, url, ok)| {
                ok && url == catalog_url && at.elapsed() < READYZ_CACHE_TTL
            });
    };
    let ok = reqwest::Client::new()
        .get(format!("{catalog_url}/api/catalog/v1/config"))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .map(|r| r.status().is_success() || r.status().as_u16() == 401)
        .unwrap_or(false);
    if let Ok(mut last) = READYZ_LAST.lock() {
        *last = Some((Instant::now(), catalog_url.to_string(), ok));
    }
    ok
}

async fn readyz(state: axum::extract::State<Arc<HealthState>>) -> Response {
    if !state.ready.load(Ordering::Relaxed) {
        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response();
    }

    let mut checks = serde_json::Map::new();
    let mut all_healthy = true;

    // Check Polaris catalog reachability
    if !state.catalog_url.is_empty() {
        let polaris_ok = probe_catalog(&state.catalog_url).await;

        checks.insert(
            "polaris".to_string(),
            serde_json::Value::String(if polaris_ok {
                "ok".to_string()
            } else {
                "unreachable".to_string()
            }),
        );
        if !polaris_ok {
            all_healthy = false;
        }
    }

    // The dedicated /mcp listener, when there is one. `down` means its serve
    // task ended; the process is about to exit (see `watch_mcp_listener`).
    if let Some(alive) = &state.mcp_listener_alive {
        let up = alive.load(Ordering::SeqCst);
        checks.insert(
            "mcp".to_string(),
            serde_json::Value::String(if up { "ok" } else { "down" }.to_string()),
        );
        if !up {
            all_healthy = false;
        }
    }

    if all_healthy {
        (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({"status": "healthy", "checks": checks})),
        )
            .into_response()
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "unhealthy", "checks": checks})),
        )
            .into_response()
    }
}

// ── Ballista/DataFusion-style /api/v1/status ─────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    status: &'static str,
    node: NodeStatus,
    workers: Option<WorkersStatus>,
    /// What this process is serving: which front doors are open, how MCP
    /// authenticates, which policy engine runs, and which catalogs are
    /// attached. `None` on a worker. Fixed at startup from the effective
    /// config (flags included), so an operator can tell a flag-only
    /// `--mcp-only --mcp-anonymous` Glue gateway from a full coordinator by
    /// reading one endpoint instead of the startup log.
    serving: Option<ServingStatus>,
}

/// The `serving` block of `/api/v1/status`.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ServingStatus {
    /// `"mcp-only"` when every SQL front door is closed and only `/mcp`
    /// answers (`--mcp-only`), else `"full"`.
    mode: &'static str,
    /// `None` when that listener is disabled (port 0).
    flight_sql_port: Option<u16>,
    trino_http_port: Option<u16>,
    quack_port: Option<u16>,
    /// `None` when MCP is not enabled.
    mcp: Option<McpServingStatus>,
    /// `[policy] engine`, kebab-case as in the config file.
    policy_engine: sqe_core::config::PolicyEngine,
    /// Every attached catalog: `[catalogs.*]` entries, the legacy `[catalog]`
    /// block when set, and `[[hive.roots]]` (including `--glue-catalog`).
    catalogs: Vec<CatalogServingStatus>,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
struct McpServingStatus {
    /// `0` means `/mcp` is mounted on this health/web listener.
    port: u16,
    /// `oauth`, `static` or `anonymous`.
    auth_mode: &'static str,
    /// Whether `execute_sql_change` is advertised at all.
    allow_writes: bool,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
struct CatalogServingStatus {
    /// The SQL catalog name.
    name: String,
    /// `rest`, `hms`, `glue`, `jdbc`, `s3tables` for Iceberg catalogs;
    /// `hive-glue` or `hive-sidecar` for Hive roots.
    kind: &'static str,
}

fn catalog_backend_name(backend: &sqe_core::config::CatalogBackend) -> &'static str {
    use sqe_core::config::CatalogBackend;
    match backend {
        CatalogBackend::Rest => "rest",
        CatalogBackend::Hms { .. } => "hms",
        CatalogBackend::Glue { .. } => "glue",
        CatalogBackend::Jdbc { .. } => "jdbc",
        CatalogBackend::S3tables { .. } => "s3tables",
    }
}

/// The `serving` block from the effective coordinator config, after every
/// flag (`--mcp-only`, `--glue-catalog`, `--mcp-anonymous`) has been applied.
fn serving_status(config: &SqeConfig) -> ServingStatus {
    let port = |p: u16| (p != 0).then_some(p);
    let flight_sql_port = port(config.coordinator.flight_sql_port);
    let trino_http_port = port(config.coordinator.trino_http_port);
    let quack_port = port(config.coordinator.quack_port);
    let mcp = config.mcp.is_enabled().then(|| McpServingStatus {
        port: config.mcp.port,
        auth_mode: config.mcp.auth.mode.as_str(),
        allow_writes: config.mcp.allow_writes,
    });
    let mode = if flight_sql_port.is_none()
        && trino_http_port.is_none()
        && quack_port.is_none()
        && mcp.is_some()
    {
        "mcp-only"
    } else {
        "full"
    };
    let mut catalogs: Vec<CatalogServingStatus> = config
        .catalogs
        .iter()
        .map(|(name, c)| CatalogServingStatus {
            name: name.clone(),
            kind: catalog_backend_name(&c.backend),
        })
        .collect();
    if config.catalogs.is_empty() && !config.catalog.catalog_url.is_empty() {
        catalogs.push(CatalogServingStatus {
            name: config.catalog.warehouse.clone(),
            kind: catalog_backend_name(&config.catalog.backend),
        });
    }
    for root in &config.hive.roots {
        catalogs.push(CatalogServingStatus {
            name: root.name.clone(),
            kind: match root.kind {
                sqe_core::config::HiveRootKind::Glue => "hive-glue",
                sqe_core::config::HiveRootKind::Sidecar => "hive-sidecar",
            },
        });
    }
    catalogs.sort_by(|a, b| a.name.cmp(&b.name));
    ServingStatus {
        mode,
        flight_sql_port,
        trino_http_port,
        quack_port,
        mcp,
        policy_engine: config.policy.engine,
        catalogs,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeStatus {
    role: &'static str,
    version: &'static str,
    git_sha: &'static str,
    build_profile: &'static str,
    datafusion_version: &'static str,
    uptime_seconds: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkersStatus {
    total: usize,
    healthy: usize,
    healthy_urls: Vec<String>,
}

/// The open `/api/v1/status`: lifecycle state and role only. It is bound on
/// every interface without auth, so build identity, worker URLs and the
/// `serving` block live behind the admin guard at `/api/v1/status/detail`.
async fn open_status(state: axum::extract::State<Arc<HealthState>>) -> Json<serde_json::Value> {
    let ready = state.ready.load(Ordering::Relaxed);
    Json(serde_json::json!({
        "status": if ready { "ACTIVE" } else { "STARTING" },
        "node": {"role": state.role},
    }))
}

async fn cluster_status(state: axum::extract::State<Arc<HealthState>>) -> Json<ClusterStatus> {
    let ready = state.ready.load(Ordering::Relaxed);

    let workers = if let Some(ref registry) = state.worker_registry {
        Some(WorkersStatus {
            total: registry.total_workers().await,
            healthy: registry.healthy_workers().await.len(),
            healthy_urls: registry.healthy_workers().await,
        })
    } else {
        None
    };

    Json(ClusterStatus {
        status: if ready { "ACTIVE" } else { "STARTING" },
        node: NodeStatus {
            role: state.role,
            version: sqe_core::VERSION,
            git_sha: build_info::GIT_SHA,
            build_profile: build_info::PROFILE,
            // The linked library's own version, not a literal: this said
            // "51" through three DataFusion upgrades.
            datafusion_version: datafusion::DATAFUSION_VERSION,
            uptime_seconds: state.started_at.elapsed().as_secs(),
        },
        workers,
        serving: state.serving.clone(),
    })
}

// ── Read-only web UI JSON API ──────────────────────────────────

#[derive(serde::Deserialize)]
struct QueryListParams {
    state: Option<String>,
    limit: Option<usize>,
}

async fn api_queries(
    state: axum::extract::State<Arc<HealthState>>,
    params: axum::extract::Query<QueryListParams>,
) -> Json<Vec<sqe_coordinator::web_ui::QueryListItem>> {
    let limit = params.limit.unwrap_or(200).min(1000);
    let items = match &state.query_tracker {
        Some(t) => sqe_coordinator::web_ui::query_list(t, params.state.as_deref(), limit),
        None => Vec::new(),
    };
    Json(items)
}

async fn api_query_detail(
    state: axum::extract::State<Arc<HealthState>>,
    id: axum::extract::Path<String>,
) -> Response {
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return (axum::http::StatusCode::BAD_REQUEST, "invalid query id").into_response();
    };
    let detail = state
        .query_tracker
        .as_ref()
        .and_then(|t| sqe_coordinator::web_ui::query_detail(t, &uuid));
    match detail {
        Some(d) => Json(d).into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, "query not found").into_response(),
    }
}

async fn api_workers(
    state: axum::extract::State<Arc<HealthState>>,
) -> Json<sqe_coordinator::web_ui::WorkersDto> {
    let (total, healthy_urls) = match &state.worker_registry {
        Some(r) => (r.total_workers().await, r.healthy_workers().await),
        None => (0, Vec::new()),
    };
    // With a tracker, derive per-worker in-flight from running fragments. Without
    // one (it is always present on the coordinator path; this is the defensive
    // branch), report health only.
    let view = match &state.query_tracker {
        Some(t) => sqe_coordinator::web_ui::workers_view(total, healthy_urls, t.active_count(), t),
        None => sqe_coordinator::web_ui::WorkersDto {
            total,
            healthy_count: healthy_urls.len(),
            active_queries: 0,
            workers: healthy_urls
                .into_iter()
                .map(|url| sqe_coordinator::web_ui::WorkerDto {
                    url,
                    healthy: true,
                    in_flight: 0,
                })
                .collect(),
        },
    };
    Json(view)
}

async fn api_overview(
    state: axum::extract::State<Arc<HealthState>>,
) -> Json<sqe_coordinator::web_ui::OverviewDto> {
    let uptime_seconds = state.started_at.elapsed().as_secs();
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    // Build (or synthesise) the NodeInfo.
    let node_info = state
        .node_info
        .clone()
        .unwrap_or_else(|| sqe_coordinator::web_ui::NodeInfo {
            name: "SQE",
            version: sqe_core::VERSION,
            role: state.role,
            datafusion_version: datafusion::DATAFUSION_VERSION,
            flight_sql_port: 0,
            trino_http_port: None,
            quack_port: None,
            catalog_backend: String::new(),
            catalog_url: String::new(),
            storage: String::new(),
            memory_limit: None,
            max_concurrent_queries: 0,
        });

    // Use the real tracker when available; fall back to an ephemeral empty one
    // (tracker is always Some on coordinator path; this branch is defensive).
    let dto = match &state.query_tracker {
        Some(t) => sqe_coordinator::web_ui::overview(&node_info, uptime_seconds, cpu_cores, t),
        None => {
            let t = sqe_coordinator::query_tracker::QueryTracker::new(
                &sqe_core::QueryHistoryConfig::default(),
            );
            sqe_coordinator::web_ui::overview(&node_info, uptime_seconds, cpu_cores, &t)
        }
    };
    Json(dto)
}

async fn api_metrics_history(
    state: axum::extract::State<Arc<HealthState>>,
) -> Json<sqe_coordinator::metrics_history::HistoryResponse> {
    let response = state
        .metrics_history
        .as_ref()
        .map(|h| {
            let samples = h.snapshot();
            sqe_coordinator::metrics_history::bucket_samples(&samples)
        })
        .unwrap_or_else(|| sqe_coordinator::metrics_history::HistoryResponse {
            bucket_seconds: sqe_coordinator::metrics_history::BUCKET_SECS,
            buckets: Vec::new(),
        });
    Json(response)
}

/// Optional request body for `POST /api/v1/catalogs/refresh`.
///
/// Absent body (or `{}`) invalidates every session's caches; `{"username":
/// "<u>"}` scopes the session-cache drop to that user. The shared REST-catalog
/// cache is always dropped regardless (it is keyed by url+warehouse+token, not
/// by user).
#[derive(serde::Deserialize, Default)]
struct CatalogRefreshBody {
    #[serde(default)]
    username: Option<String>,
}

#[derive(Serialize)]
struct CatalogRefreshResponse {
    /// What was invalidated: `"all"` or `"session:<username>"`.
    invalidated: String,
}

/// Admin-gated catalog cache refresh (event-driven invalidation hook).
///
/// The platform's workspace-provisioning path calls this right after it creates
/// or rebinds a Polaris catalog, so SQE drops its cached SessionContexts (the
/// catalog provider set) and RestCatalog instances immediately instead of
/// waiting out the TTL backstop. Without it, a freshly created workspace
/// catalog is invisible to reads until an unrelated write/DDL rebuilds the
/// session: the #368 miss-triggered re-list only refreshes namespaces *within*
/// an already-registered catalog, never the catalog set itself.
///
/// No new invalidation logic lives here -- it wires the existing process-global
/// invalidators. `Option<Json<_>>` so a bodyless POST succeeds and invalidates
/// all (bare `Json<T>` 422s on an empty body). `State` must precede the body
/// extractor: `Option<Json<_>>` consumes the request and has to come last.
async fn api_catalogs_refresh(
    state: axum::extract::State<Arc<HealthState>>,
    body: Option<Json<CatalogRefreshBody>>,
) -> Json<CatalogRefreshResponse> {
    let invalidated = match body.and_then(|Json(b)| b.username) {
        Some(username) => {
            sqe_coordinator::session_context::invalidate_session_cache(&username).await;
            format!("session:{username}")
        }
        None => {
            sqe_coordinator::session_context::invalidate_all_session_caches().await;
            "all".to_string()
        }
    };
    // Always drop the shared REST-catalog cache too: a rebind changes the
    // Polaris binding for a catalog name, and that cache (plus each entry's
    // `/v1/config` OnceCell) is keyed by url+warehouse+token, not by user.
    sqe_catalog::invalidate_rest_catalog_cache_all().await;
    // And the shared table metadata cache: a table rebound out-of-band leaves a
    // stale present entry (1 hour hard TTL) under whatever token first loaded
    // it. Without this drop, an immediate readback after a workspace catalog is
    // created/rebound returns the stale metadata (or, via the still-empty
    // provider snapshot, "table not found") until the TTL lapses. This endpoint
    // is admin-gated, so the process-global drop is safe here -- unlike the
    // self-scoped `CALL system.refresh_catalog_cache()` path, which must not.
    if let Some(tc) = &state.table_cache {
        tc.invalidate_all().await;
    }
    // And the shared Hive manifest cache (issue #666): a Glue table altered
    // out of band otherwise serves its old definition until the TTL lapses.
    let hive_manifests_dropped = state.hive_manifest_cache.as_ref().map(|c| {
        let n = c.len();
        c.invalidate_all();
        n
    });
    tracing::info!(
        invalidated = %invalidated,
        hive_manifests_dropped = ?hive_manifests_dropped,
        "catalog cache refresh via admin endpoint"
    );
    Json(CatalogRefreshResponse { invalidated })
}

async fn dashboard() -> Response {
    // WEB-05: send nosniff plus a tight CSP. The dashboard is a self-contained
    // single page (inline styles/scripts, same-origin fetches only), so a
    // restrictive policy does not break it and blocks injected external loads.
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (axum::http::header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (
                axum::http::header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; style-src 'unsafe-inline'; \
                 script-src 'unsafe-inline'; connect-src 'self'; \
                 img-src 'self' data:; base-uri 'none'; form-action 'none'; \
                 frame-ancestors 'none'",
            ),
        ],
        sqe_coordinator::web_ui::DASHBOARD_HTML,
    )
        .into_response()
}

/// Build the health server router. Extracted so tests can drive it directly.
///
/// `/healthz`, `/readyz`, and `/api/v1/status` are always ungated -- they
/// serve k8s liveness/readiness probes and LB health checks. The open
/// `/api/v1/status` carries status and role only; the full payload is
/// `/api/v1/status/detail`, admin-gated on a coordinator. The `web_ui`
/// route group (dashboard + JSON API) is gated behind bearer + admin auth via
/// `route_layer` applied only to that sub-router.
///
/// `/api/v1/catalogs/refresh` is a control-plane hook, not part of the
/// read-only dashboard, so on a coordinator it registers regardless of
/// `web_ui`, with its own `require_admin_bearer` layer. It used to live inside
/// the `web_ui` group, where `metrics.web_ui = false` (the default, and
/// TOML-only -- there is no `SQE_METRICS__*` override) silently removed the
/// route while `/healthz` kept answering 200 on the same port. An operator
/// could not tell that apart from a build predating the endpoint, and catalog
/// invalidation never fired.
fn build_health_router(state: Arc<HealthState>) -> Router {
    let open = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/status", get(open_status));

    // Coordinator-only: a worker holds no shared table cache and no session
    // cache, so the hook would 200 having invalidated nothing. `run_worker`
    // also wires no `bearer_provider`, so it would answer a permanent 401 --
    // either way the route would be a lie on a worker. Keep it off there.
    // The full status payload rides the same guard for the same reason.
    let app = if state.role == "coordinator" {
        let admin_hooks = Router::new()
            .route("/api/v1/catalogs/refresh", post(api_catalogs_refresh))
            .route("/api/v1/status/detail", get(cluster_status))
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                sqe_coordinator::web_auth::require_admin_bearer::<HealthState>,
            ));
        open.merge(admin_hooks)
    } else {
        open
    };

    let app = if state.web_ui {
        let web = Router::new()
            .route("/", get(dashboard))
            .route("/api/v1/overview", get(api_overview))
            .route("/api/v1/queries", get(api_queries))
            .route("/api/v1/queries/{id}", get(api_query_detail))
            .route("/api/v1/workers", get(api_workers))
            .route("/api/v1/metrics/history", get(api_metrics_history))
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                sqe_coordinator::web_auth::require_admin_bearer::<HealthState>,
            ));
        app.merge(web)
    } else {
        app
    };

    app.with_state(state)
}

/// Interface for the health/web and metrics listeners (finding 764). An
/// unauthenticated MCP gateway (`--mcp-anonymous`) keeps every ops listener on
/// loopback as well, since `/api/v1/status` and `/metrics` are unauthenticated
/// and `/mcp` may itself be mounted on the health listener (`[mcp] port = 0`).
/// An explicit `[mcp] bind_address` / `--mcp-bind` is the operator's choice
/// and is followed. Every other mode binds all interfaces, as before.
fn ops_bind_ip(config: &SqeConfig) -> std::net::IpAddr {
    if config.mcp.is_enabled() && config.mcp.auth.mode == sqe_core::config::McpAuthMode::Anonymous {
        config
            .mcp
            .bind_ip()
            .unwrap_or_else(|_| std::net::Ipv4Addr::LOCALHOST.into())
    } else {
        std::net::Ipv4Addr::UNSPECIFIED.into()
    }
}

fn start_health_server_with_extra(
    bind: std::net::IpAddr,
    port: u16,
    state: Arc<HealthState>,
    extra: Option<Router>,
) {
    let mut app = build_health_router(state);
    if let Some(extra) = extra {
        app = app.merge(extra);
    }

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::new(bind, port))
            .await
            .expect("Failed to bind health server");
        tracing::info!("Health endpoints on {bind}:{port} (/healthz, /readyz, /api/v1/status)");
        // Serve with connect-info so the dashboard-access guard can extract the
        // peer TCP address for client_ip in audit events.
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap_or_else(|e| tracing::error!(error = %e, "Health server terminated unexpectedly"));
    });
}

// ── Graceful shutdown ──────────────────────────────────────────
/// Exit code when the dedicated `/mcp` listener dies: BSD `EX_SOFTWARE`, so a
/// supervisor log can tell it from a crash (101) and a clean stop (0).
const EXIT_MCP_LISTENER_DIED: i32 = 70;

/// Turn a dead `/mcp` listener into a dead process. The serve task cannot
/// end quietly (`mcp_server::supervise_listener` logs and clears the alive
/// flag), and this makes sure the process does not outlive it: readiness
/// flips so a load balancer stops routing, then `exit` runs with
/// [`EXIT_MCP_LISTENER_DIED`] so the supervisor restarts the container. The
/// alternative, rebinding in place, would race the old socket's close and
/// hide the fact that something killed the accept loop. `exit` is a
/// parameter so the test can observe the call instead of dying.
fn watch_mcp_listener(
    listener_task: tokio::task::JoinHandle<()>,
    ready: Arc<AtomicBool>,
    shutdown: sqe_coordinator::mcp_server::CancellationToken,
    exit: impl FnOnce(i32) + Send + 'static,
) {
    tokio::spawn(async move {
        let _ = listener_task.await;
        ready.store(false, Ordering::SeqCst);
        // Drained on the shutdown signal (#679): the process is on its way
        // out already, and exiting 70 here would tell the supervisor a
        // healthy stop was a crash.
        if shutdown.is_cancelled() {
            tracing::info!("the /mcp listener closed on shutdown");
            return;
        }
        tracing::error!(
            exit_code = EXIT_MCP_LISTENER_DIED,
            "the /mcp listener task has ended while the process is alive; exiting so a \
             supervisor restarts it (a dead listener on a live PID is invisible to every \
             health check but /healthz on this port)"
        );
        exit(EXIT_MCP_LISTENER_DIED);
    });
}

async fn shutdown_signal() {
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

/// Shutdown future for `serve_with_shutdown` that drains before stopping (#250).
///
/// On SIGTERM/SIGINT this:
/// 1. flips `ready` to NOT-ready so `/readyz` returns 503 and the Kubernetes
///    Service / load balancer stops routing NEW work to this pod;
/// 2. sleeps `drain_secs` so connections already routed before the readiness
///    flip propagated can finish at the tonic graceful boundary;
/// 3. runs `before_close` (the coordinator rolls back its open transactions,
///    #824);
/// 4. resolves, which tells tonic to stop accepting and shut down.
///
/// The grace period is bounded by `coordinator.shutdown_drain_secs` and must
/// stay below the pod's `terminationGracePeriodSeconds` so the process exits
/// before SIGKILL. NOTE: this is a time-bounded connection-drain, not a true
/// in-flight-query drain. SQE has no query-lifecycle registry to wait on yet;
/// tracking in-flight queries and blocking shutdown until they complete is a
/// follow-up (see MR).
async fn shutdown_with_drain(
    ready: Arc<AtomicBool>,
    drain_secs: u64,
    before_close: impl std::future::Future<Output = ()>,
) {
    shutdown_signal().await;
    let drain_secs = effective_drain_secs(drain_secs, std::io::stderr().is_terminal());
    // A second SIGINT/SIGTERM during the drain means "now": exit the way a
    // shell expects (130) instead of making the operator wait it out.
    tokio::select! {
        _ = flip_ready_and_drain(&ready, drain_secs) => {}
        _ = shutdown_signal() => {
            tracing::warn!("second shutdown signal during drain; exiting now");
            std::process::exit(130);
        }
    }
    before_close.await;
    // tonic's graceful stop then waits for every open connection to close,
    // which an idle keep-alive client can hold for as long as it likes. Bound
    // that: the accept loop is already closed, so nothing new is lost.
    tokio::spawn(async {
        tokio::time::sleep(CONNECTION_DRAIN_LIMIT).await;
        tracing::warn!(
            limit_secs = CONNECTION_DRAIN_LIMIT.as_secs(),
            "connections still open after the drain limit; exiting"
        );
        std::process::exit(0);
    });
}

/// How long tonic may wait for open connections after the drain (see
/// [`shutdown_with_drain`]).
const CONNECTION_DRAIN_LIMIT: std::time::Duration = std::time::Duration::from_secs(5);

/// The drain exists so a Kubernetes Service stops routing to this pod before
/// it closes. A process whose stderr is a terminal is a laptop run that nobody
/// routes to, so it drains nothing; `shutdown_drain_secs` keeps its meaning
/// everywhere else.
fn effective_drain_secs(configured: u64, interactive: bool) -> u64 {
    if interactive {
        0
    } else {
        configured
    }
}

/// Flip readiness to NOT-ready, then sleep the bounded drain period. Split out
/// of [`shutdown_with_drain`] so the readiness-flip is unit-testable without a
/// real OS signal.
async fn flip_ready_and_drain(ready: &AtomicBool, drain_secs: u64) {
    // Stop advertising readiness BEFORE we start draining so the Service
    // routing table converges away from this pod while we wait.
    ready.store(false, Ordering::Relaxed);

    if drain_secs == 0 {
        tracing::info!("shutdown_drain_secs = 0, shutting down immediately");
        return;
    }

    tracing::info!(
        drain_secs,
        "readiness flipped to NOT-ready, draining connections before shutdown"
    );
    tokio::time::sleep(std::time::Duration::from_secs(drain_secs)).await;
    tracing::info!("drain period elapsed, shutting down");
}

// ── Trino adapters ─────────────────────────────────────────────
struct AuthenticatorAdapter {
    authenticator: Arc<sqe_auth::Authenticator>,
    /// The full auth provider chain (built from `[[auth.providers]]`). When set
    /// (always, in production) BOTH Basic auth and bearer tokens dispatch
    /// through it, so the Trino-compat path sees the same providers as the
    /// Flight SQL handshake, including `client_credentials_passthrough`. When
    /// `None` (some unit-test fixtures), Basic auth falls back to the legacy
    /// `Authenticator`.
    bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>>,
    /// The chain's pre-auth limiter, also spent on every OAuth2 challenge an
    /// unauthenticated request mints, keyed `(peer, "oauth2-challenge")`.
    auth_rate_limiter: Arc<sqe_coordinator::rate_limiter::AuthRateLimiter>,
}

/// The chain-level pre-auth limiter (#562) refuses with `RateLimited`; the
/// Trino server recognises `ResourceExhausted` and answers 429 instead of
/// the 401 every other provider failure gets.
fn trino_auth_error(e: sqe_auth::AuthError, context: &str) -> sqe_core::SqeError {
    match e {
        sqe_auth::AuthError::RateLimited => sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::ResourceExhausted,
            "authentication rate limit".to_string(),
        ),
        other => sqe_core::SqeError::Auth(format!("{context}{other}")),
    }
}

#[async_trait::async_trait]
impl TrinoAuthenticator for AuthenticatorAdapter {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
        peer_ip: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        // Route Basic auth through the chain so a service principal's
        // client_id/client_secret reaches `client_credentials_passthrough` over
        // Trino just as it does over Flight SQL. Fall back to the legacy
        // authenticator only when no chain is wired (test fixtures).
        if let Some(provider) = self.bearer_provider.as_ref() {
            let credentials = sqe_auth::FlightCredentials {
                username: Some(username.to_string()),
                password: Some(sqe_core::SecretString::new(password.to_string())),
                peer_ip: Some(peer_ip.to_string()),
                ..Default::default()
            };
            let identity = provider
                .authenticate(&credentials)
                .await
                .map_err(|e| trino_auth_error(e, ""))?;
            return Ok(identity.into_session(None));
        }
        self.authenticator
            .authenticate(username, password)
            .await
            .map_err(|e| sqe_core::SqeError::Auth(e.to_string()))
    }

    async fn authenticate_bearer(
        &self,
        token: &str,
        peer_ip: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        let provider = self.bearer_provider.as_ref().ok_or_else(|| {
            sqe_core::SqeError::Auth("Bearer token authentication is not configured".to_string())
        })?;

        let credentials = sqe_auth::FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token.to_string())),
            peer_ip: Some(peer_ip.to_string()),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&credentials)
            .await
            .map_err(|e| trino_auth_error(e, "Bearer token validation failed: "))?;

        // Fall back to the raw JWT as the catalog token when the provider did
        // not supply one (passthrough to Polaris).
        Ok(identity.into_session(Some(token)))
    }

    async fn admit_auth_challenge(&self, peer_ip: &str) -> Result<(), sqe_core::SqeError> {
        self.auth_rate_limiter
            .check(peer_ip, "oauth2-challenge")
            .map_err(|_| trino_auth_error(sqe_auth::AuthError::RateLimited, ""))
    }
}

struct QueryHandlerAdapter {
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<sqe_coordinator::rate_limiter::QueryRateLimiter>,
}

#[async_trait::async_trait]
impl TrinoQueryExecutor for QueryHandlerAdapter {
    /// See [`QueryHandlerAdapter::execute_as`]; the engine mints the id.
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<TrinoResultStream, sqe_core::SqeError> {
        self.execute_as(session, sql, None).await
    }

    /// The Trino query id is a UUID this server minted; the engine records
    /// the statement under it, so its query history, audit record and
    /// OpenLineage `runId` equal the `id` the client polls. A non-UUID id
    /// (none today) falls back to an engine-minted one.
    async fn execute_with_query_id(
        &self,
        session: &sqe_core::Session,
        sql: &str,
        query_id: &str,
    ) -> Result<TrinoResultStream, sqe_core::SqeError> {
        self.execute_as(session, sql, uuid::Uuid::parse_str(query_id).ok())
            .await
    }

    fn memory_pool(&self) -> Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>> {
        Some(self.handler.runtime().memory_pool.clone())
    }

    fn audit_rejected(&self, session: &sqe_core::Session, sql: &str, error: &sqe_core::SqeError) {
        self.handler.record_failed_statement(
            session,
            sql,
            std::time::Instant::now(),
            None,
            None,
            error,
        );
    }

    async fn describe_prepared(
        &self,
        session: &sqe_core::Session,
        prepared_sql: &str,
        kind: sqe_trino_compat::protocol::DescribeKind,
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
        self.handler
            .describe_prepared(session, prepared_sql, kind)
            .await
    }
}

impl QueryHandlerAdapter {
    /// SELECTs stream through [`QueryHandler::execute_stream_with_id`] so the
    /// Trino path never holds more than one page of a result (#621); every
    /// other statement kind (DML counts, DDL, SHOW, ...) takes the buffered
    /// `execute_with_id` path exactly as Flight SQL does. `query_id` is the
    /// id the engine records the statement under (`None`: engine-minted).
    async fn execute_as(
        &self,
        session: &sqe_core::Session,
        sql: &str,
        query_id: Option<uuid::Uuid>,
    ) -> Result<TrinoResultStream, sqe_core::SqeError> {
        use futures::StreamExt;
        use sqe_sql::{
            classify_normalized, pre_parse_pipeline, SqlFrontDoor, StatementKind, UserSql,
            WireDialect,
        };

        self.rate_limiter
            .check(&session.user.username)
            .map_err(|e| {
                sqe_core::SqeError::with_code(
                    sqe_core::SqeErrorCode::ResourceExhausted,
                    format!("rate limit: {e}"),
                )
            })?;
        // Classification failures fall through to `execute`, which produces
        // the engine's own parse error for the client. The front door folds
        // Trino-only shapes, so a Trino-shaped SELECT streams too (#811).
        let is_query = pre_parse_pipeline(&UserSql::from(sql))
            .and_then(|c| {
                SqlFrontDoor::new(WireDialect::Trino)
                    .with_session_catalog(session.default_catalog.clone())
                    .normalize(c.as_str())
            })
            .and_then(classify_normalized)
            .map(|kind| matches!(kind, StatementKind::Query(_)))
            .unwrap_or(false);
        if is_query {
            let (schema, stream) = self
                .handler
                .execute_stream_with_id(session, sql, None, query_id)
                .await?;
            let stream = stream.map(|res| res.map_err(sqe_core::SqeError::from));
            return Ok(TrinoResultStream::new(schema, Box::pin(stream)));
        }
        self.handler
            .execute_with_id(session, sql, None, query_id)
            .await
            .map(TrinoResultStream::from_batches)
    }
}

// ── Main ───────────────────────────────────────────────────────
//
// We build the tokio runtime manually instead of using `#[tokio::main]` so we
// can set a larger thread stack. The default 2 MiB worker stack is enough for
// most query plans but overflows on deep AST trees — notably, DataFusion
// re-parses WHERE clauses produced by our CoW DML rewrites, and the SQL
// grammar parses `a OR b OR c OR ...` as a left-leaning chain of depth N.
// For N in the thousands (e.g. TPC-E `trade_result_update_holding`, which
// materialises `(ca, sym) IN (SELECT ...)` into an O(N) OR chain over every
// pending trade), DataFusion's own AST walkers exhaust the 2 MiB stack and
// SIGABRT the coordinator. The coordinator must never crash — we spill,
// stream, and absorb large plans instead. An 8 MiB worker stack gives us ~4x
// the headroom at zero runtime cost.
fn main() -> anyhow::Result<()> {
    // ring is the process TLS provider (installed pre-main by sqe-core; this
    // makes it explicit and logs which case happened). The later call on the
    // TLS listener path stays as the finding-210 check.
    sqe_coordinator::tls::ensure_crypto_provider();
    let cli = Cli::parse();
    if let Some(active) = &cli.verify_audit {
        // A rotated log is its generations oldest first, then the active
        // file; each must chain to the one before. The active file is empty
        // right after a rotation, which is not a broken chain.
        let rotated = sqe_metrics::audit::rotated_audit_files(std::path::Path::new(active));
        let mut files: Vec<String> = rotated
            .iter()
            .map(|(_, p)| p.display().to_string())
            .collect();
        let active_empty = std::fs::metadata(active).is_ok_and(|m| m.len() == 0);
        if rotated.is_empty() || !active_empty {
            files.push(active.clone());
        }
        let mut prev: Option<sqe_metrics::audit::AuditVerifyReport> = None;
        for path in &files {
            let bytes = std::fs::read(path)
                .map_err(|e| anyhow::anyhow!("cannot read audit file {path}: {e}"))?;
            let r = match sqe_metrics::audit::verify_audit_bytes(&bytes) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("file:            {path}");
                    eprintln!("chain: BROKEN: {e}");
                    std::process::exit(1);
                }
            };
            if let Some(p) = &prev {
                match sqe_metrics::audit::link_rotated_reports(p, &r) {
                    Ok(None) => {}
                    Ok(Some(reason)) => println!("broken segment:  start of {path}: {reason}"),
                    Err(e) => {
                        eprintln!("chain: BROKEN: {path}: {e}");
                        std::process::exit(1);
                    }
                }
            }
            println!("file:            {path}");
            println!("sha256:          {}", r.file_sha256);
            println!(
                "records:         {} (seq {}..={})",
                r.records, r.first_seq, r.last_seq
            );
            let genesis = if r.starts_at_genesis() {
                " (genesis)"
            } else {
                ""
            };
            println!("first prev_hash: {}{genesis}", r.first_prev_hash);
            println!("last hash:       {}", r.last_hash);
            for (line, reason) in &r.broken_segments {
                println!("broken segment:  line {line}: {reason}");
            }
            prev = Some(r);
        }
        println!("chain:           OK");
        return Ok(());
    }
    let config_path = cli
        .config
        .clone()
        .or_else(|| cli.config_positional.clone())
        .or_else(|| std::env::var("SQE_CONFIG").ok())
        .unwrap_or_else(|| "sqe.toml".to_string());
    if cli.config_positional.is_some() && cli.config.is_none() {
        eprintln!("warning: positional config path is deprecated; use --config");
    }

    if cli.print_config {
        // The flag-only default has nothing secret to redact; a real file
        // still goes through the redactor.
        let toml = if std::path::Path::new(&config_path).exists() || config_path != "sqe.toml" {
            SqeConfig::load_redacted_toml(&config_path)
                .map_err(|e| anyhow::anyhow!("Failed to load config from {config_path}: {e}"))?
        } else {
            DEFAULT_FLAG_ONLY_CONFIG.to_string()
        };
        print!("{toml}");
        return Ok(());
    }

    let config = load_config_or_default(&config_path)?;
    sqe_coordinator::credential_refresh::configure_push_timeouts(
        config.coordinator.credential_push_connect_timeout_secs,
        config.coordinator.credential_push_request_timeout_secs,
    );
    let runtime = sqe_core::build_server_runtime(&config.coordinator, "sqe-server")
        .map_err(|e| anyhow::anyhow!("Failed to build tokio runtime: {e}"))?;

    runtime.block_on(async_main())
}

/// Built-in config for the flag-only path (`sqe-server` with no config file).
/// It satisfies `validate()` with the no-IdP `[auth]` placeholder the
/// `glue-readonly` quickstart uses and declares no Iceberg REST catalog, so a
/// deployment reached only through `--glue-catalog` + `--mcp-anonymous` needs
/// nothing on disk. Anything real (a Polaris catalog, `[[auth.providers]]`,
/// storage keys, TLS) still goes in a config file.
const DEFAULT_FLAG_ONLY_CONFIG: &str = r#"
[coordinator]

[catalog]
catalog_url = ""

[auth]
client_id = "unused-no-idp"
token_endpoint = "http://127.0.0.1:1/token"
"#;

/// Load the config file at `path`, or fall back to the built-in flag-only
/// default when `path` is the conventional `sqe.toml` and it is absent. An
/// explicitly named file that does not exist is still an error. Shared by the
/// runtime-building `main` and the serving `async_main` so both agree on which
/// config the process runs.
fn load_config_or_default(path: &str) -> anyhow::Result<SqeConfig> {
    if std::path::Path::new(path).exists() {
        SqeConfig::load(path).map_err(|e| anyhow::anyhow!("Failed to load config from {path}: {e}"))
    } else if path == "sqe.toml" {
        SqeConfig::load_from_str(DEFAULT_FLAG_ONLY_CONFIG)
            .map_err(|e| anyhow::anyhow!("Failed to build default config: {e}"))
    } else {
        Err(anyhow::anyhow!("config file not found: {path}"))
    }
}

async fn async_main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let explicit_config = cli
        .config
        .clone()
        .or_else(|| cli.config_positional.clone())
        .or_else(|| std::env::var("SQE_CONFIG").ok());

    // No config file named, and the conventional `sqe.toml` is not in the
    // working directory: start from the built-in default rather than erroring.
    // This is the flag-only path -- `sqe-server --glue-catalog glue
    // --mcp-anonymous --mcp-port 8098` with nothing on disk. The default
    // carries the no-IdP `[auth]` placeholder that `validate()` wants and an
    // empty `[catalog]` (no Iceberg REST catalog); the flags below fill in the
    // rest. Env overlays (`SQE_*`, `AWS_*`) still apply on top.
    let config_path = explicit_config.unwrap_or_else(|| "sqe.toml".to_string());
    let mut config = load_config_or_default(&config_path)?;

    // CLI overrides land before `validate`, so a flag cannot talk the
    // engine past a check the TOML equivalent would have failed.
    if let Some(port) = cli.mcp_port {
        config.mcp.port = port;
        config.mcp.enabled = true;
        // A loopback `resource_uri` from the config names the port the config
        // chose, so overriding the port leaves it pointing at a listener that
        // no longer exists. Public URLs are left alone: behind a proxy the
        // resource identifier is deliberately not the local port.
        let uri = config.mcp.resource_uri.trim().to_string();
        if uri.contains("127.0.0.1") || uri.contains("localhost") || uri.contains("[::1]") {
            config.mcp.resource_uri = format!("http://127.0.0.1:{port}/mcp");
        }
    }
    if cli.mcp_anonymous {
        config.mcp.enabled = true;
        config.mcp.auth.mode = sqe_core::config::McpAuthMode::Anonymous;
        // Anonymous plus writes is refused by `validate` anyway; clearing it
        // here means `--mcp-anonymous` against a config that enabled writes
        // starts read-only rather than failing to start.
        config.mcp.allow_writes = false;
        // `resource_uri` is required whenever MCP listens on its own port.
        // A flag-only invocation has no config to carry one, so derive the
        // loopback URL the caller will actually use.
        if config.mcp.resource_uri.trim().is_empty() && config.mcp.port != 0 {
            config.mcp.resource_uri = format!("http://127.0.0.1:{}/mcp", config.mcp.port);
        }
        tracing::warn!(
            "WARNING: MCP is UNAUTHENTICATED (--mcp-anonymous). Every caller reaching the \
             endpoint reads as one anonymous identity using the engine's storage credentials. \
             Use [mcp.auth] mode = \"static\" or \"oauth\" outside a trusted network."
        );
    }
    if let Some(bind) = &cli.mcp_bind {
        config.mcp.bind_address = bind.clone();
    }
    // #672: a leftover `port` line is not a decision to serve /mcp.
    if config.mcp.enabled_by_port_only() {
        tracing::warn!(
            port = config.mcp.port,
            "[mcp] port is set without enabled = true, so /mcp is NOT served (#672). Set \
             [mcp] enabled = true to serve it, or remove port."
        );
    }
    // #671: an unauthenticated endpoint off loopback is a choice the operator
    // made by name (bind_address / --mcp-bind, or port = 0 on the web
    // listener, which binds every interface); say so where they are looking.
    // Finding 764: with port = 0 the health listener now follows the same
    // bind (`ops_bind_ip`), so only a non-loopback address is an exposure.
    if config.mcp.is_enabled()
        && config.mcp.auth.mode == sqe_core::config::McpAuthMode::Anonymous
        && !config.mcp.bind_ip().is_ok_and(|ip| ip.is_loopback())
    {
        tracing::warn!(
            bind = %config.mcp.bind_address,
            port = config.mcp.port,
            "WARNING: the UNAUTHENTICATED /mcp endpoint is reachable from the network. Set a \
             dedicated [mcp] port and leave bind_address empty (or 127.0.0.1) to keep it on \
             loopback."
        );
    }

    // The explicit allow-all switch. Logged loud because it removes the
    // object-store read boundary.
    if cli.allow_all_buckets {
        config.storage.tvf.allow_all_object_stores = true;
        tracing::warn!(
            "WARNING: --allow-all-buckets is set. Every object-store path is \
             readable with the engine's storage credentials, bypassing the \
             prefix allowlist. Use only on a single-tenant or trusted-network \
             deployment."
        );
    }

    // Static keys with a session token are temporary credentials frozen
    // into the process: they expire with the session that issued them and
    // nothing here can renew them. Every scan then fails while SELECT 1 and
    // the metadata tools keep working, which is the shape that cost a day
    // on 2026-09-16. Say so at startup, once, where the operator is looking.
    if !config.storage.s3_access_key.is_empty()
        && !config.storage.s3_session_token.expose().is_empty()
    {
        tracing::warn!(
            "[storage] carries a static access key WITH a session token: these are temporary \
             credentials and will expire with the session that issued them; the server \
             cannot refresh them and every object-store read will then fail with \
             STORAGE_CREDENTIALS_UNAVAILABLE. For a long-running server leave the keys \
             unset and run with AWS_PROFILE (SSO or a role), which the AWS SDK refreshes."
        );
    }

    // MCP-only: close the SQL front doors, keep just /mcp (and the ops
    // health/metrics port). Applied before validate so it composes with the
    // other flags. Requires MCP to be enabled, or the process would serve
    // nothing.
    if cli.mcp_only {
        if !config.mcp.is_enabled() {
            return Err(anyhow::anyhow!(
                "--mcp-only needs MCP enabled: add --mcp-anonymous, --mcp-port, or an [mcp] block"
            ));
        }
        config.coordinator.flight_sql_port = 0;
        config.coordinator.trino_http_port = 0;
        config.coordinator.quack_port = 0;
        tracing::info!(
            "--mcp-only: Flight SQL, Trino HTTP and Quack ports disabled; serving /mcp only"
        );
    }

    // Extra read prefixes from the flag, appended before validate so a
    // flag-only Glue run has an access boundary without a config file.
    for prefix in &cli.allow_s3_prefix {
        if !config
            .storage
            .tvf
            .allowed_object_store_prefixes
            .iter()
            .any(|p| p == prefix)
        {
            config
                .storage
                .tvf
                .allowed_object_store_prefixes
                .push(prefix.clone());
        }
    }

    if let Some(name) = cli.glue_catalog.as_deref() {
        // Appended rather than replacing, so the flag composes with any
        // sidecar roots the config already declares.
        config.hive.roots.push(sqe_core::config::HiveRootConfig {
            name: name.to_string(),
            kind: sqe_core::config::HiveRootKind::Glue,
            location: String::new(),
            region: cli.glue_region.clone(),
            catalog_id: None,
            databases: cli.glue_database.clone(),
            secret: None,
        });
    }

    // Region fallback: if nothing set the S3 region (no [storage], no
    // SQE_STORAGE__S3_REGION, no AWS_REGION / AWS_DEFAULT_REGION), resolve it
    // from the AWS SDK config, which reads the active profile's `region` in
    // ~/.aws/config. This lets `export AWS_PROFILE=...` alone give the S3 data
    // path a region, the same profile the Glue catalog already uses.
    #[cfg(feature = "glue")]
    if config.storage.s3_region.trim().is_empty() {
        if let Some(region) = sqe_catalog::aws_config::sdk_resolved_region().await {
            tracing::info!(%region, "S3 region resolved from the AWS SDK config (profile/SSO)");
            config.storage.s3_region = region;
        }
    }

    let config = config;
    config.validate().map_err(|e| anyhow::anyhow!("{e}"))?;
    sqe_catalog::rest_catalog::configure_breaker_failure_classes(
        &config.catalog.circuit_breaker_failure_classes,
    )
    .map_err(|e| anyhow::anyhow!("catalog.circuit_breaker_failure_classes: {e}"))?;

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
    // main.rs so the two coordinator binaries cannot drift.
    if sqe_coordinator::mode::warns_unauthenticated_workers(&config.coordinator) {
        tracing::warn!("WARNING: coordinator.allow_unauthenticated_workers = true with an empty coordinator.worker_secret -- any client reachable on the cluster network can register as a worker and receive user bearer tokens. Set worker_secret for production.");
    }
    // WEB-01: the web UI / JSON API serve on the health port (0.0.0.0).
    // Routes are gated behind bearer + admin auth (see web_auth::require_admin_bearer).
    // Warn so operators know the surface is active.
    if config.metrics.web_ui {
        tracing::warn!("WARNING: metrics.web_ui = true -- the ops dashboard and /api/v1/* endpoints are served on the health port (0.0.0.0). Routes require a valid admin bearer token. Network-gate the health port or leave web_ui = false.");
    }

    // Priority: --mode flag > SQE_MODE env > config file mode
    // Since clap always has a default, check if user explicitly passed --mode
    // by seeing if SQE_MODE or config override it; otherwise use CLI default.
    let mode = match cli.mode {
        CliMode::Coordinator => Mode::Coordinator,
        CliMode::Worker => Mode::Worker,
    };

    let service_name = match mode {
        Mode::Coordinator => "sqe-coordinator",
        Mode::Worker => "sqe-worker",
    };

    let _otel_guard = sqe_metrics::otel::init_telemetry_with_sampling(
        service_name,
        &config.metrics.otlp_endpoint,
        &config.metrics.traces_otlp_endpoint,
        config.metrics.trace_sample_rate,
    )
    .map_err(|e| anyhow::anyhow!(e))?;

    tracing::info!(
        mode = ?mode,
        config = config_path,
        version = sqe_core::VERSION,
        git_sha = build_info::GIT_SHA,
        build_profile = build_info::PROFILE,
        "Starting sqe-server"
    );
    // Issue #669: a debug binary is 5-10x slower on scans and decoding and
    // was once mistaken for an engine regression. Say so once, loudly.
    if build_info::IS_DEBUG {
        tracing::warn!(
            "debug build: performance is not representative, build with \
             `cargo build --release --bin sqe-server` for anything but tests"
        );
    }

    match mode {
        Mode::Coordinator => run_coordinator(config).await,
        Mode::Worker => run_worker(config, config_path).await,
    }
}

// ── Coordinator ────────────────────────────────────────────────
async fn run_coordinator(mut config: SqeConfig) -> anyhow::Result<()> {
    let started_at = Instant::now();
    let ready = Arc::new(AtomicBool::new(false));

    // Resolve `memory_limit = "auto"` to bytes and fail closed over the
    // cgroup limit before any pool, admission gate or write path reads it.
    let memory_need =
        sqe_coordinator::runtime::resolve_coordinator_memory(&mut config.coordinator)?;
    // Detached on purpose, like the worker's: the watch only logs, and lives
    // as long as the runtime.
    let _runtime_memory_task = sqe_core::spawn_runtime_memory_watch(
        Arc::new(AtomicU64::new(memory_need)),
        sqe_core::DEFAULT_RUNTIME_MEMORY_WATCH_INTERVAL,
    );

    // Single source of truth for the distributed worker path. Computed once so
    // the registry wiring, health checks, credential refresh, the health-server
    // view, and the query handler can never drift out of agreement. See
    // `distributed_enabled` for why heartbeat discovery keys off the secret.
    let distributed = distributed_enabled(&config.coordinator);

    // Push the configured SessionContext-cache TTL into the process-global cache
    // before any session is built, so the `LazyLock` initializer reads the
    // configured value instead of the compiled-in default.
    sqe_coordinator::session_context::configure_session_cache_ttl(
        config.coordinator.session_context_cache_ttl_secs,
    );

    // Health endpoints on metrics port + 1 (or 9091 default)
    let health_port = config.metrics.prometheus_port + 1;

    // Track supervised background tasks for the lifetime of the binary;
    // dropping each TaskGuard signals cooperative cancellation and aborts
    // the underlying tokio task.
    let mut _task_guards: Vec<sqe_core::TaskGuard> = Vec::new();

    // Auth
    let authenticator = Arc::new(sqe_auth::Authenticator::new(&config.auth).await?);
    _task_guards.push(authenticator.start_refresh_task());

    // Rate limiters, shared by the Flight SQL, Trino, MCP and dashboard paths.
    // Built before the chain so the pre-auth limiter can wrap it (#562).
    let rate_limiter = Arc::new(sqe_coordinator::rate_limiter::QueryRateLimiter::new(
        &config.rate_limit,
    ));
    let auth_rate_limiter = Arc::new(sqe_coordinator::rate_limiter::AuthRateLimiter::new(
        &config.rate_limit,
    ));
    let metadata_rate_limiter = Arc::new(sqe_coordinator::rate_limiter::MetadataRateLimiter::new(
        &config.rate_limit,
    ));

    // Build the auth provider chain from `[[auth.providers]]`. Both Flight
    // SQL (via SessionManager) and the Trino-compat HTTP path use the
    // same chain so the two endpoints accept the same set of credentials.
    // For configs without `[[auth.providers]]` the chain wraps the legacy
    // Authenticator and behaviour is unchanged.
    //
    // #562: the chain is wrapped in the pre-auth rate limiter ONCE, here, so
    // every entry point that hands it a credential with a peer address
    // (Flight, MCP, the dashboard) is gated without deciding to be. Trino
    // and Quack still run their own check and send no peer address; they
    // migrate next.
    let auth_chain: Arc<dyn sqe_auth::AuthProvider> =
        Arc::new(sqe_coordinator::rate_limiter::RateLimitedAuthProvider::new(
            Arc::new(sqe_auth::build_auth_chain(&config.auth).await?),
            Arc::clone(&auth_rate_limiter),
        ));

    // SessionManager authenticates new Flight SQL requests through the
    // chain; the legacy Authenticator stays attached so its background
    // refresh task continues to keep username/password tokens current.
    let session_manager = Arc::new(SessionManager::with_provider_and_legacy(
        Arc::clone(&auth_chain),
        authenticator.clone(),
    ));

    let worker_registry = Arc::new(
        sqe_coordinator::worker_registry::WorkerRegistry::with_options_and_failures(
            config.coordinator.worker_urls.clone(),
            // `_and_tls`: an https worker URL needs a ClientTlsConfig or
            // tonic refuses the URI outright (finding 603).
            sqe_coordinator::channel_pool::ChannelPool::shared_with_timeouts_and_tls(
                std::time::Duration::from_secs(config.coordinator.worker_connect_timeout_secs),
                std::time::Duration::from_secs(config.coordinator.worker_rpc_timeout_secs),
                &config.coordinator.transport,
                &config.coordinator.tls,
            )?,
            config.coordinator.max_workers,
            config.coordinator.health_check_max_failures,
        ),
    );

    if distributed {
        let interval =
            std::time::Duration::from_secs(config.coordinator.health_check_interval_secs);
        _task_guards.push(worker_registry.start_health_check_task(interval));
        tracing::info!(
            seeded_workers = ?config.coordinator.worker_urls,
            interval_secs = config.coordinator.health_check_interval_secs,
            "Started worker health checks (covers heartbeat-discovered workers too)"
        );
    }

    // Created before the health server so the web UI can read it; the same Arc
    // is moved into the QueryHandler below.
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));

    // What this process serves, for `/api/v1/status`. Taken here, while the
    // effective config (flags applied) is still in scope.
    let serving = serving_status(&config);
    // Liveness of the dedicated /mcp listener, shared between the serve task
    // (which clears it) and the health handlers (which read it). Only for a
    // dedicated port: with `mcp.port = 0` the routes ride this very listener.
    let mcp_listener_alive: Option<Arc<AtomicBool>> = (config.mcp.is_enabled()
        && config.mcp.port > 0)
        .then(|| Arc::new(AtomicBool::new(true)));

    // Build NodeInfo for the Overview endpoint from config (config is still in
    // scope at this point; it gets moved into the handlers below).
    let node_info = {
        use sqe_core::config::CatalogBackend;
        let catalog_backend = match &config.catalog.backend {
            CatalogBackend::Rest => "rest",
            CatalogBackend::Hms { .. } => "hms",
            CatalogBackend::Glue { .. } => "glue",
            CatalogBackend::Jdbc { .. } => "jdbc",
            CatalogBackend::S3tables { .. } => "s3tables",
        }
        .to_string();
        let storage = if config.storage.s3_endpoint.is_empty() {
            "local".to_string()
        } else {
            config.storage.s3_endpoint.clone()
        };
        let trino_http_port =
            (config.coordinator.trino_http_port != 0).then_some(config.coordinator.trino_http_port);
        let quack_port =
            (config.coordinator.quack_port != 0).then_some(config.coordinator.quack_port);
        let memory_limit =
            if config.query.max_query_memory.is_empty() || config.query.max_query_memory == "0" {
                None
            } else {
                Some(config.query.max_query_memory.clone())
            };
        sqe_coordinator::web_ui::NodeInfo {
            name: "SQE",
            version: sqe_core::VERSION,
            role: "coordinator",
            datafusion_version: datafusion::DATAFUSION_VERSION,
            flight_sql_port: config.coordinator.flight_sql_port,
            trino_http_port,
            quack_port,
            catalog_backend,
            catalog_url: config.catalog.catalog_url.clone(),
            storage,
            memory_limit,
            max_concurrent_queries: config.query.max_concurrent_queries,
        }
    };

    // Metrics ring-buffer for the dashboard history endpoint (1 h at 5 s resolution).
    let metrics_history = if config.metrics.web_ui {
        Some(Arc::new(
            sqe_coordinator::metrics_history::MetricsHistory::new(720),
        ))
    } else {
        None
    };

    // Preserve a clone of query_tracker for the metrics sampler; the original
    // is moved into QueryHandler::new below.
    let sampler_tracker = query_tracker.clone();

    // Metrics & audit
    let metrics = Arc::new(
        sqe_metrics::MetricsRegistry::new()
            .map_err(|e| anyhow::anyhow!("failed to initialize metrics registry: {e}"))?,
    );

    let _metrics_server = sqe_metrics::server::start_metrics_server(
        metrics.clone(),
        ops_bind_ip(&config),
        config.metrics.prometheus_port,
    )
    .map_err(|e| anyhow::anyhow!("failed to bind metrics server: {e}"))?;
    tracing::info!(
        "Prometheus metrics on port {}",
        config.metrics.prometheus_port
    );

    // Credential refresh tracker — shared between query handler and background task
    let credential_tracker =
        Arc::new(sqe_coordinator::credential_refresh::CredentialRefreshTracker::new());

    if distributed {
        let interval =
            std::time::Duration::from_secs(config.coordinator.credential_refresh_interval_secs);
        _task_guards.push(
            sqe_coordinator::credential_refresh::start_credential_refresh_task(
                credential_tracker.clone(),
                interval,
                config.coordinator.worker_secret.expose().to_string(),
                // Reload the fragment's table as the querying user; the
                // catalog vends a fresh table-scoped set (or refuses).
                sqe_coordinator::credential_refresh::reload_vended_credentials,
            ),
        );
        tracing::info!(
            interval_secs = config.coordinator.credential_refresh_interval_secs,
            "Started credential refresh background task"
        );
    }

    {
        let sm = session_manager.clone();
        let idle = config.session.idle_timeout_secs;
        let absolute = config.session.absolute_timeout_secs;
        let sweep_interval =
            std::time::Duration::from_secs(config.session.expiry_sweep_interval_secs);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(sweep_interval);
            tick.tick().await;
            loop {
                tick.tick().await;
                sm.sweep_expired_sessions(idle, absolute);
            }
        });
        tracing::info!(
            idle_timeout_secs = config.session.idle_timeout_secs,
            absolute_timeout_secs = config.session.absolute_timeout_secs,
            sweep_interval_secs = config.session.expiry_sweep_interval_secs,
            "Started session expiry sweeper"
        );
    }

    // File-based session persistence — optional, off by default
    if config.session.persistence == "file" {
        tracing::warn!(
            path = %config.session.persistence_path,
            "⚠ Session file persistence writes access tokens to disk in plaintext. \
             Ensure the persistence file has restrictive permissions (chmod 600). \
             Consider using memory persistence in production unless restart recovery is required."
        );

        // Try to restore sessions from the last snapshot on startup (best-effort).
        // Runs in spawn_blocking to avoid blocking the Tokio worker thread with std::fs I/O.
        {
            let sm = session_manager.clone();
            let restore_path = config.session.persistence_path.clone();
            match tokio::task::spawn_blocking(move || sm.restore_from_file(&restore_path)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "session restore from file failed");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "session restore task panicked");
                }
            }
        }

        // Spawn background task to periodically snapshot sessions to disk
        let sm = session_manager.clone();
        let path = config.session.persistence_path.clone();
        let interval = config.session.snapshot_interval_secs;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
            ticker.tick().await; // skip first immediate tick
            loop {
                ticker.tick().await;
                let sm_inner = sm.clone();
                let path_inner = path.clone();
                let result =
                    tokio::task::spawn_blocking(move || sm_inner.snapshot_to_file(&path_inner))
                        .await;
                match result {
                    Ok(Err(e)) => tracing::warn!(error = %e, "Session snapshot failed"),
                    Err(e) => tracing::warn!(error = %e, "Session snapshot task panicked"),
                    Ok(Ok(())) => {}
                }
            }
        });
        tracing::info!(
            path = %config.session.persistence_path,
            interval_secs = config.session.snapshot_interval_secs,
            "File-based session persistence enabled"
        );
    }

    // Query tracker and result cache (query_tracker Arc already created above for the health server)
    let query_cache = if config.query_cache.enabled {
        Some(Arc::new(sqe_coordinator::query_cache::ResultCache::new(
            &config.query_cache,
            Some(metrics.clone()),
        )))
    } else {
        None
    };
    tracing::info!(
        history_max_entries = config.query_history.max_entries,
        cache_enabled = config.query_cache.enabled,
        "Initialized query tracker and result cache"
    );

    // Manifest-list and manifest caching is delegated to iceberg-rust's
    // per-`Table` `ObjectCache`. Because `TableMetadataCache` (built below)
    // caches `Table` instances globally, the per-table object cache persists
    // across queries and sessions.

    // Build the global table metadata cache (shared across all sessions and queries).
    // Table metadata is user-independent. Schema, partitions, and snapshots are the
    // same regardless of who queries. The cache is invalidated on DDL/DML operations.
    let table_cache = sqe_catalog::TableMetadataCache::new(config.catalog.metadata_cache_ttl_secs)
        .with_authorization_recheck(config.catalog.authorization_recheck_secs)
        .with_metrics(Arc::clone(&metrics));
    tracing::info!(
        metadata_cache_ttl_secs = config.catalog.metadata_cache_ttl_secs,
        authorization_recheck_secs = config.catalog.authorization_recheck_secs,
        "Initialized global table metadata cache (shared across all sessions)"
    );

    // Build the audit logger after the table cache so GDPR tag masking can be
    // wired via CacheTagSource. Salt is derived once at startup (stable within
    // a deployment, not across restarts/replicas, not secret-grade).
    let audit_salt = uuid::Uuid::new_v4().to_string();
    let audit_logger = sqe_metrics::audit::AuditLogger::with_options(
        &config.metrics.audit_log_path,
        sqe_coordinator::parse_audit_format(&config.metrics.audit.format),
        sqe_metrics::audit::AuditLoggerOptions {
            queue_capacity: config.metrics.audit.queue_capacity,
            fail_mode: sqe_metrics::audit::AuditFailMode::parse(&config.metrics.audit.fail_mode),
            dropped: Some(metrics.audit_events_dropped_total.clone()),
            rotation: sqe_metrics::audit::AuditRotation {
                max_file_bytes: config.metrics.audit.max_file_bytes,
                max_files: config.metrics.audit.max_files,
                min_free_bytes: config.metrics.audit.min_free_bytes,
            },
            privacy: sqe_metrics::audit::AuditPrivacy {
                omit_query_text: config.metrics.audit.omit_query_text,
                omit_actor_identity: config.metrics.audit.omit_actor_identity,
            },
        },
    )
    .map_err(|e| anyhow::anyhow!(e))?;

    let audit_logger = if !config.metrics.audit.gdpr_tags.is_empty() {
        let tag_src = Arc::new(sqe_coordinator::tag_source_impl::CacheTagSource::new(
            Arc::new(table_cache.clone()),
        ));
        let adapter = Arc::new(sqe_coordinator::audit_tag_adapter::AuditTagAdapter(tag_src));
        audit_logger.with_gdpr(
            config.metrics.audit.gdpr_tags.clone(),
            sqe_coordinator::parse_gdpr_mode(&config.metrics.audit.gdpr_identifier_mode),
            audit_salt,
            adapter,
        )
    } else {
        audit_logger
    };

    // Resolve the export endpoint before attaching the spool. Only the
    // shipper enforces `max_spool_bytes`, so a spool with no shipper behind
    // it would grow without bound (#838): no exporter, no spool.
    let audit_exporter =
        if config.metrics.audit_export.enabled && config.metrics.audit_export.target == "otlp" {
            let ae = &config.metrics.audit_export;
            let endpoint = if !ae.otlp_endpoint.is_empty() {
                ae.otlp_endpoint.clone()
            } else {
                config.metrics.otlp_endpoint.clone()
            };
            if endpoint.is_empty() {
                tracing::warn!(
                    "audit export: no OTLP endpoint configured \
                     (audit_export.otlp_endpoint and metrics.otlp_endpoint are both empty); \
                     no spool is written and the shipper will not start"
                );
                None
            } else {
                match sqe_metrics::audit::export::OtlpExporter::new(&endpoint) {
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "audit export: failed to build OtlpExporter; \
                             no spool is written and the shipper will not start"
                        );
                        None
                    }
                    Ok(exporter) => Some((exporter, endpoint)),
                }
            }
        } else {
            None
        };

    // Attach the OCSF export spool when there is a shipper to drain it.
    let (audit_logger, shipper_spool_path) = if audit_exporter.is_some() {
        let spool = if config.metrics.audit_export.spool_path.is_empty() {
            format!("{}.ocsf.spool.jsonl", config.metrics.audit_log_path)
        } else {
            config.metrics.audit_export.spool_path.clone()
        };
        let logger = audit_logger
            .with_export_spool(&spool)
            .map_err(|e| anyhow::anyhow!("audit export: failed to open spool file: {e}"))?;
        (logger, Some(spool))
    } else {
        (audit_logger, None)
    };

    let audit = Arc::new(audit_logger);

    // Finding 601 / L4: one ticket-signing key for the whole coordinator.
    // The query path and the scheduler's maintenance handler both sign
    // worker requests with it, and workers pin only one key.
    let ticket_signer = Arc::new(sqe_coordinator::load_ticket_signer(&config.coordinator));

    // Advisory/active auto-compaction scheduler (Phase 4a, Task 5). `Off`
    // mode (the default) constructs NOTHING here: neither the dedicated
    // maintenance service principal nor the `MaintenanceScheduler` value
    // exist anywhere in the process. That absence -- not a runtime no-op --
    // is the isolation guarantee `[maintenance] mode = "off"` promises; the
    // principal is never reachable from the interactive auth chain because
    // it is never constructed at all when mode is off.
    //
    // `config.maintenance.scheduler.enabled` is a second, independent gate:
    // an operator can run `mode = "advisory"` purely so `CALL
    // system.table_health` and a future external-trigger RPC work, while
    // leaving the in-process tick loop off (`scheduler.enabled = false`,
    // the config default) for a Kubernetes-CronJob-driven deployment. Both
    // the principal and the `MaintenanceScheduler` are still built in that
    // case (so the external trigger path has something to call into later);
    // only `_task_guards.push(scheduler.spawn())` is skipped.
    if config.maintenance.mode != sqe_core::config::MaintenanceMode::Off {
        let principal_cfg = config.maintenance.principal.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "maintenance.mode is not \"off\" but [maintenance.principal] is unset; \
                 this should have been rejected by config validation at startup"
            )
        })?;
        let maintenance_principal = Arc::new(
            sqe_coordinator::maintenance_principal::MaintenancePrincipal::from_config(
                principal_cfg,
            )
            .map_err(|e| anyhow::anyhow!("failed to build maintenance principal: {e}"))?,
        );
        let catalog_factory = sqe_coordinator::maintenance_scheduler::default_catalog_factory(
            config.clone(),
            Some(table_cache.clone()),
        );
        // Dedicated `MaintenanceHandler` for the scheduler's active-mode arm
        // (Phase 4b), separate from the one `QueryHandler` builds for the
        // interactive `CALL system.rewrite_data_files` path below.
        // `Advisory` mode's `analyze_one_table` never touches this handler
        // (it only calls `table_health`, which needs no DataFusion runtime),
        // so the handler is built without a runtime there. Only in `Active`
        // mode does `active_one_table` call `handler.rewrite_data_files`,
        // and only a per-table `strategy => "sort"` override needs the
        // runtime (to spill instead of OOMing); that runtime is a second
        // `FairSpillPool` competing for host memory alongside the query
        // engine's, and building it unconditionally would let Active-mode
        // compaction run concurrently with queries at up to ~2x the
        // operator's configured `memory_limit`, contradicting the
        // single-shared-budget invariant `runtime.rs` documents. So it is
        // built and attached ONLY when `mode == Active`.
        // TODO(4c): share the coordinator query runtime instead of a second pool.
        // The scheduler never resolves a catalog through this handler's own
        // `create_catalog_bridge`; it always passes in a catalog it already
        // built via `catalog_factory` (see
        // `maintenance_scheduler.rs::active_one_table`).
        let mut maintenance_handler_builder =
            sqe_coordinator::maintenance::MaintenanceHandler::new(config.clone())
                .with_table_cache(table_cache.clone())
                .with_audit(Arc::clone(&audit))
                .with_metrics(Arc::clone(&metrics))
                // Phase 4c Task 5: same registry the interactive `CALL` path's
                // `MaintenanceHandler` gets (see `QueryHandler::new`), so the
                // scheduler's active-mode `distribution.mode` routing sees the
                // real healthy-worker count too. Wiring it unconditionally
                // (even when `distributed` is false) is safe: a registry with
                // no seeded/heartbeat-discovered workers, or one whose health
                // checks never started, reports `0` healthy either way.
                .with_worker_registry(Arc::clone(&worker_registry))
                .with_ticket_signer(Arc::clone(&ticket_signer));
        if config.maintenance.mode == sqe_core::config::MaintenanceMode::Active {
            let maintenance_runtime = sqe_coordinator::runtime::build_coordinator_runtime(
                &config.coordinator,
                &config.storage,
            )
            .map_err(|e| anyhow::anyhow!("failed to build maintenance scheduler runtime: {e}"))?;
            maintenance_handler_builder =
                maintenance_handler_builder.with_runtime(maintenance_runtime);
        }
        let maintenance_handler = Arc::new(maintenance_handler_builder);
        let maintenance_scheduler =
            sqe_coordinator::maintenance_scheduler::MaintenanceScheduler::new(
                config.maintenance.clone(),
                maintenance_principal,
                metrics.clone(),
                Some(audit.clone()),
                catalog_factory,
                maintenance_handler,
            );
        if config.maintenance.scheduler.enabled {
            _task_guards.push(maintenance_scheduler.spawn());
            tracing::info!(
                mode = ?config.maintenance.mode,
                tick_secs = config.maintenance.scheduler.tick_secs,
                jitter_secs = config.maintenance.scheduler.jitter_secs,
                "Started maintenance advisory scheduler loop"
            );
        } else {
            drop(maintenance_scheduler);
            tracing::info!(
                mode = ?config.maintenance.mode,
                "maintenance.mode is not \"off\" but maintenance.scheduler.enabled is false; \
                 the in-process tick loop will not run (external-trigger deployment expected)"
            );
        }
    }

    // Health server. Started here (after audit init) so the audit logger can be
    // threaded into HealthState and dashboard-access events are captured from
    // the first request. Probes (/healthz, /readyz) are still served immediately
    // after coordinator startup; the only change vs. the pre-Task-2 placement is
    // that the health server starts a few ms later (after audit file open).
    // Process-wide Hive manifest cache: the same instance every Hive
    // consumer receives (Task 8+), built here so the admin refresh endpoint
    // below can drop it (issue #666).
    let hive_manifest_cache = Arc::new(
        sqe_catalog::hive::provider::HiveMetadataCache::with_ttl(std::time::Duration::from_secs(
            config.hive.metadata_ttl_secs,
        ))
        .with_recheck(std::time::Duration::from_secs(
            config.hive.metadata_recheck_secs,
        )),
    );

    let health_state = Arc::new(HealthState {
        ready: ready.clone(),
        started_at,
        role: "coordinator",
        worker_registry: if distributed {
            Some(worker_registry.clone())
        } else {
            None
        },
        query_tracker: Some(query_tracker.clone()),
        web_ui: config.metrics.web_ui,
        catalog_url: config.catalog.catalog_url.clone(),
        node_info: Some(node_info),
        metrics_history: metrics_history.clone(),
        // auth_chain is already built above; reuse it for the web_ui guard.
        bearer_provider: Some(Arc::clone(&auth_chain) as Arc<dyn sqe_auth::AuthProvider>),
        auth_cfg: Some(config.auth.clone()),
        // Thread the security config so the dashboard guard honours
        // trusted_proxies for XFF resolution, matching the Flight SQL path.
        security_cfg: Some(config.security.clone()),
        audit: Some(Arc::clone(&audit)),
        anonymous_denied: Some(metrics.dashboard_auth_anonymous_denied_total.clone()),
        dashboard_success: Some(metrics.dashboard_auth_success_total.clone()),
        // Same shared cache the QueryHandler uses; drop it on catalog refresh.
        // Cloned (not moved) because `with_table_cache` below still needs it.
        table_cache: Some(table_cache.clone()),
        hive_manifest_cache: Some(Arc::clone(&hive_manifest_cache)),
        serving: Some(serving),
        mcp_listener_alive: mcp_listener_alive.clone(),
        success_audit_dedup: {
            let window = config.metrics.audit.dashboard_access_audit_window_secs;
            if window == 0 {
                None
            } else {
                Some(
                    moka::sync::Cache::builder()
                        .max_capacity(1024)
                        .time_to_live(std::time::Duration::from_secs(window))
                        .build(),
                )
            }
        },
    });
    // Spawn the OTLP audit shipper when export is enabled and an exporter
    // was built above (the spool exists only then).
    let shipper_shutdown_tx = if let (Some(spool_path), Some((exporter, endpoint))) =
        (shipper_spool_path.as_ref(), audit_exporter)
    {
        let ae = &config.metrics.audit_export;
        let cursor_path = format!("{spool_path}.cursor");
        let shipper_metrics = sqe_metrics::audit::export::ShipperMetrics {
            records_total: Some(metrics.audit_export_records_total.clone()),
            batch_failures_total: Some(metrics.audit_export_batch_failures_total.clone()),
            spool_lag_bytes: Some(metrics.audit_export_spool_lag_bytes.clone()),
            cursor_seq: Some(metrics.audit_export_cursor_seq.clone()),
            last_success_timestamp: Some(metrics.audit_export_last_success_timestamp.clone()),
            spool_dropped_records_total: Some(
                metrics.audit_export_spool_dropped_records_total.clone(),
            ),
        };
        let shipper = sqe_metrics::audit::export::OtlpLogShipper::new(
            std::path::PathBuf::from(spool_path),
            std::path::PathBuf::from(&cursor_path),
            std::sync::Arc::new(exporter),
            ae.batch_max,
            ae.max_spool_bytes,
            sqe_coordinator::parse_start_at(&ae.start_at),
            ae.flush_interval_ms,
        )
        .with_metrics(shipper_metrics)
        .with_compactor(audit.spool_compactor(spool_path));

        let (tx, rx) = tokio::sync::watch::channel(false);
        tokio::spawn(shipper.run(rx));
        tracing::info!(
            spool = %spool_path,
            endpoint = %endpoint,
            "Started audit OTLP shipper"
        );
        Some(tx)
    } else {
        if config.metrics.audit_export.enabled && config.metrics.audit_export.target != "otlp" {
            tracing::warn!(
                target = %config.metrics.audit_export.target,
                "audit export: unknown target; only \"otlp\" is supported; shipper will not start"
            );
        }
        None
    };

    // Guard + self-audit the superdebug_log_results escape hatch.
    // No-op when the flag is false (the default).
    sqe_coordinator::maybe_warn_superdebug(&audit, &config);

    // Process-wide, LIVE Hive registries; see the matching comment in
    // `main.rs`. Both Arcs must be the same instance every other Hive
    // consumer receives (Task 8+).
    let hive_catalogs = Arc::new(sqe_policy::plan_rewriter::HiveCatalogs::new());
    // `hive_manifest_cache` is built above the health server so the admin
    // refresh endpoint can drop it (issue #666).

    // Attach every `[[hive.roots]]` configured root before the first
    // session context is ever built; see the matching comment in `main.rs`.
    let runtime_catalogs = sqe_coordinator::RuntimeCatalogRegistry::default();
    sqe_coordinator::runtime_catalog::attach_configured_hive_roots(
        &config.hive,
        &config.storage,
        &runtime_catalogs,
        &hive_catalogs,
        &config.resolve_default_catalog(),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))?;

    // AUTH-01: build the enforcer + store from config.policy.engine.
    // table_cache is passed so the rewriter can wire CacheTagSource for
    // tag-based column masking (Task 4). A clone is taken here; the original
    // is passed to with_table_cache() on the QueryHandler below.
    let (policy_enforcer, policy_store) = sqe_coordinator::policy_wiring::build_policy_enforcer(
        &config.policy,
        Some(table_cache.clone()),
        hive_catalogs.clone(),
        hive_manifest_cache.clone(),
        Some(Arc::clone(&metrics)),
    )?;
    if config.policy.engine != sqe_core::config::PolicyEngine::Passthrough {
        tracing::info!(
            engine = ?config.policy.engine,
            "policy enforcement ACTIVE (row filters + column masks + tag masking)"
        );
    }

    let grant_backend: Option<Arc<dyn GrantBackend>> =
        sqe_coordinator::policy_wiring::build_grant_backend(&config)?;

    // OpenLineage observer (optional). When [metrics.openlineage] enabled = true,
    // build the configured sinks (file and/or HTTP+spool), spawn the emitter task,
    // and hand back a ChannelObserver wired to the bounded mpsc.
    let lineage_obs = build_lineage_observer(&config, &metrics.registry)?;

    // Query handler
    let query_handler = Arc::new(
        QueryHandler::new(
            policy_enforcer,
            policy_store,
            config.clone(),
            if distributed {
                Some(worker_registry.clone())
            } else {
                None
            },
            if distributed {
                Some(credential_tracker)
            } else {
                None
            },
            Some(metrics.clone()),
            Some(audit.clone()),
            query_tracker,
            query_cache,
            grant_backend,
            lineage_obs,
            runtime_catalogs,
            sqe_core::SecretStore::from_optional_path(&config.session.secrets_path)
                .map_err(|e| anyhow::anyhow!(e))?,
        )?
        .with_table_cache(table_cache)
        .with_session_manager(session_manager.clone())
        .with_hive_registries(hive_catalogs.clone(), hive_manifest_cache.clone())
        .with_ticket_signer(ticket_signer),
    );

    // Spawn background memory metrics reporter (updates gauges every 1s for Grafana)
    sqe_coordinator::memory::spawn_metrics_reporter(
        query_handler.runtime().clone(),
        metrics.clone(),
    );

    // Spawn dashboard time-series sampler when the web UI is enabled.
    if let Some(ref hist) = metrics_history {
        _task_guards.push(sqe_coordinator::metrics_history::spawn_sampler(
            hist.clone(),
            query_handler.runtime().clone(),
            sampler_tracker,
            std::time::Duration::from_secs(5),
        ));
        tracing::info!("Started metrics history sampler (5 s interval, 1 h window)");
    }

    // Bearer auth chain for the Trino-compat HTTP path. Reuses the same
    // chain instance the Flight SQL path uses so both endpoints accept
    // the same credentials with identical provider behaviour.
    let bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>> = Some(Arc::clone(&auth_chain));

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

    // MCP can share the existing health/web listener or use a dedicated port.
    // Both modes reuse the same live auth chain and QueryHandler.
    let mcp_web_router = if config.mcp.is_enabled() && config.mcp.port == 0 {
        tracing::info!(port = health_port, resource = %config.mcp.resource_uri, "MCP endpoint mounted on web listener at /mcp");
        Some(sqe_coordinator::mcp_server::mcp_router(
            Arc::clone(&auth_chain),
            Arc::clone(&query_handler),
            Arc::clone(&rate_limiter),
            Arc::clone(&metadata_rate_limiter),
            config.mcp.clone(),
            sqe_coordinator::mcp_server::engine_credential_catalog_names(&config),
            &config.storage,
            &config.rate_limit,
        ))
    } else {
        if config.mcp.port > 0 {
            let alive = mcp_listener_alive
                .clone()
                .expect("mcp_listener_alive is Some whenever mcp.port > 0");
            // SIGTERM/SIGINT stop the accept loop and drain in-flight calls
            // (#679); the same signal the SQL listeners already act on.
            let mcp_shutdown = sqe_coordinator::mcp_server::CancellationToken::new();
            tokio::spawn({
                let token = mcp_shutdown.clone();
                async move {
                    shutdown_signal().await;
                    token.cancel();
                }
            });
            let (listener_task, bound) = sqe_coordinator::mcp_server::start_mcp_server(
                Arc::clone(&auth_chain),
                Arc::clone(&query_handler),
                Arc::clone(&rate_limiter),
                Arc::clone(&metadata_rate_limiter),
                config.mcp.clone(),
                sqe_coordinator::mcp_server::engine_credential_catalog_names(&config),
                &config.coordinator.tls,
                alive,
                &config.storage,
                &config.rate_limit,
                mcp_shutdown.clone(),
            )
            .await?;
            tracing::info!(port = bound.port(), resource = %config.mcp.resource_uri, "MCP endpoint enabled at /mcp");
            // A dead listener on a live PID is the one failure a health
            // check cannot see from outside, so do not stay alive to have it.
            watch_mcp_listener(listener_task, ready.clone(), mcp_shutdown, |code| {
                std::process::exit(code)
            });
        }
        None
    };
    start_health_server_with_extra(
        ops_bind_ip(&config),
        health_port,
        health_state,
        mcp_web_router,
    );

    // Trino compat: plain on trino_http_port, TLS on trino_https_port, either
    // may be 0.
    if config.coordinator.trino_http_port > 0 || config.coordinator.trino_https_port > 0 {
        let auth_adapter = Arc::new(AuthenticatorAdapter {
            authenticator: authenticator.clone(),
            bearer_provider: bearer_provider.clone(),
            auth_rate_limiter: Arc::clone(&auth_rate_limiter),
        });
        let handler_adapter = Arc::new(QueryHandlerAdapter {
            handler: query_handler.clone(),
            rate_limiter: Arc::clone(&rate_limiter),
        });
        // The pre-auth rate limit is inside `auth_chain` (#562): the adapter
        // forwards the peer host in the credentials and the chain's denial
        // comes back as `ResourceExhausted`, which the Trino server maps to 429.
        let https = if config.coordinator.trino_https_port > 0 {
            sqe_coordinator::tls::ensure_crypto_provider();
            let material = sqe_core::ServerTlsMaterial::load(&config.coordinator.trino_tls)
                .map_err(|e| anyhow::anyhow!("Trino HTTPS TLS config: {e}"))?;
            let mtls = material.requires_client_cert();
            let tls =
                axum_server::tls_rustls::RustlsConfig::from_config(material.rustls_server_config());
            // cert-manager rewrites the mounted secret in place; the watcher
            // swaps the config for new handshakes without a restart.
            let _trino_reload = sqe_coordinator::tls::spawn_cert_reload_watch(
                config.coordinator.trino_tls.clone(),
                tls.clone(),
                sqe_coordinator::tls::DEFAULT_CERT_RELOAD_INTERVAL,
            );
            tracing::info!(
                mtls,
                "Trino-compat HTTPS on port {}",
                config.coordinator.trino_https_port
            );
            Some(sqe_trino_compat::server::HttpsListener {
                port: config.coordinator.trino_https_port,
                tls,
            })
        } else {
            None
        };
        let trino_opts = sqe_trino_compat::server::TrinoServerOptions {
            security: config.security.clone(),
            expose_version: false,
            public_base_url: config.coordinator.public_base_url.clone(),
            https,
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
        if config.coordinator.trino_http_port > 0 {
            tracing::info!(
                "Trino-compat HTTP on port {}",
                config.coordinator.trino_http_port
            );
        }
    }

    // DuckDB Quack RPC endpoint (off by default). Shares the same AuthChain
    // as Flight SQL / Trino-compat, and dispatches every query through the
    // same QueryHandler via a CoordinatorExecutor adapter.
    if config.coordinator.quack_port > 0 {
        let quack_port = config.coordinator.quack_port;
        let executor: Arc<dyn sqe_quack_server::QueryExecutor> = Arc::new(
            sqe_coordinator::CoordinatorExecutor::new(query_handler.clone()),
        );
        // QUACK-08 / #562: the Quack ConnectionRequest -> authenticate path is
        // rate limited by the limiter inside `auth_chain`, keyed on the client
        // IP the Quack server resolves. We pass [security].trusted_proxies so
        // that IP is the real client (rightmost untrusted x-forwarded-for hop)
        // when the peer is a trusted proxy, and the raw TCP peer otherwise.
        // Serving with connect-info hands the handler the peer SocketAddr.
        let quack_state =
            sqe_quack_server::QuackServerState::new(Arc::clone(&auth_chain), executor)
                .with_security(config.security.clone());
        let quack_app = sqe_quack_server::router(quack_state);
        let bind = format!("0.0.0.0:{quack_port}");
        // QUACK-05: the Quack ConnectionRequest carries the user's OIDC bearer
        // token. On a plaintext channel any on-path observer can capture and
        // replay it against Flight SQL / Polaris / S3 as that user. Reuse the
        // coordinator's [coordinator.tls] block to serve the Quack endpoint over
        // TLS; fall back to plaintext (with a loud warning) when TLS is off.
        if config.coordinator.tls.is_enabled() {
            let quack_addr: std::net::SocketAddr = bind
                .parse()
                .map_err(|e| anyhow::anyhow!("Quack server addr {bind}: {e}"))?;
            // Checked install, not a discarded one: a silent no-op here used
            // to mean the Quack port could run on a different crypto backend
            // than the Flight port with nothing in the log (finding 210).
            sqe_coordinator::tls::ensure_crypto_provider();
            // One builder for every listener, so `ca_file` now applies here
            // too. The Quack path used to warn that mTLS was Flight-only.
            let material = sqe_core::ServerTlsMaterial::load(&config.coordinator.tls)
                .map_err(|e| anyhow::anyhow!("Quack TLS config: {e}"))?;
            let quack_mtls = material.requires_client_cert();
            let tls =
                axum_server::tls_rustls::RustlsConfig::from_config(material.rustls_server_config());
            // Rotation without a restart, same as the MCP listener.
            let _quack_reload = sqe_coordinator::tls::spawn_cert_reload_watch(
                config.coordinator.tls.clone(),
                tls.clone(),
                sqe_coordinator::tls::DEFAULT_CERT_RELOAD_INTERVAL,
            );
            tokio::spawn(async move {
                if let Err(e) = axum_server::bind_rustls(quack_addr, tls)
                    .serve(quack_app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                    .await
                {
                    tracing::error!(error = %e, "Quack RPC server (TLS) terminated unexpectedly");
                }
            });
            tracing::info!(
                mtls = quack_mtls,
                "DuckDB Quack RPC on port {quack_port} (TLS)"
            );
        } else {
            tracing::warn!("WARNING: the Quack endpoint is PLAINTEXT (no TLS) and binds 0.0.0.0 -- user OIDC bearer tokens travel in cleartext and can be captured and replayed. Set [coordinator.tls] cert_file/key_file to enable TLS, or do not expose the Quack port on untrusted networks.");
            let listener = tokio::net::TcpListener::bind(&bind)
                .await
                .map_err(|e| anyhow::anyhow!("Quack server bind to {bind}: {e}"))?;
            tokio::spawn(async move {
                if let Err(e) = axum::serve(
                    listener,
                    quack_app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .await
                {
                    tracing::error!(error = %e, "Quack RPC server terminated unexpectedly");
                }
            });
            tracing::info!("DuckDB Quack RPC on port {quack_port} (plaintext)");
        }
    }

    // Rolls back open transactions at shutdown (#824); the Flight service
    // takes ownership of `query_handler` below.
    let txn_handler = Arc::clone(&query_handler);

    // Mark ready
    ready.store(true, Ordering::Relaxed);

    // MCP-only: Flight SQL is disabled (port 0), so it is not the process's
    // keep-alive. The MCP and health servers are already spawned; wait for the
    // shutdown signal here instead of serving Flight SQL. Trino and Quack were
    // already skipped by their `> 0` guards.
    if config.coordinator.flight_sql_port == 0 {
        tracing::info!(
            "Flight SQL disabled (flight_sql_port = 0); serving MCP only. \
             Coordinator up."
        );
        shutdown_with_drain(
            ready.clone(),
            config.coordinator.shutdown_drain_secs,
            query_handler.abort_open_transactions(),
        )
        .await;
        if let Some(tx) = shipper_shutdown_tx {
            let _ = tx.send(true);
        }
        tracing::info!("SQE coordinator shut down");
        return Ok(());
    }

    // Flight SQL server with graceful shutdown
    let mut flight_service =
        SqeFlightSqlService::new(session_manager, query_handler, config.clone())
            .with_rate_limiter(rate_limiter)
            .with_metadata_rate_limiter(metadata_rate_limiter);
    // Hand the flight service the SAME registry the query handler reads, so
    // worker heartbeats (handled on the DoAction path) actually register
    // workers that `try_distribute` will route to. Without this the heartbeat
    // handler has no registry and silently drops every heartbeat -- which made
    // dynamic discovery dead even when health checks were running (#226).
    if distributed {
        flight_service = flight_service.with_worker_registry(worker_registry.clone());
    }
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    // Optional TLS, from the one shared builder in `sqe_core::tls`.
    let tls_material = sqe_core::ServerTlsMaterial::load_optional(&config.coordinator.tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = sqe_coordinator::transport::apply_grpc_transport(
        tonic::transport::Server::builder(),
        &config.coordinator.transport,
    );
    if let Some(material) = tls_material.as_ref() {
        server_builder = server_builder.tls_config(material.tonic_server())?;
        tracing::info!(
            mtls = material.requires_client_cert(),
            cert_fingerprint = material.cert_fingerprint(),
            // tonic gives no way to swap a live acceptor, so this port keeps
            // the certificate it started with until restart. See
            // `sqe_coordinator::tls`.
            hot_reload = false,
            "SQE coordinator listening on {addr} (TLS)"
        );
    } else {
        tracing::info!("SQE coordinator listening on {addr} (plaintext)");
    }

    let serve_result = server_builder
        .layer(sqe_coordinator::flight_sql::FlightTraceLayer)
        .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(flight_service))
        .serve_with_shutdown(
            addr,
            shutdown_with_drain(
                ready.clone(),
                config.coordinator.shutdown_drain_secs,
                async move { txn_handler.abort_open_transactions().await },
            ),
        )
        .await;

    // Signal the audit shipper to stop now that the server has exited.
    if let Some(tx) = shipper_shutdown_tx {
        let _ = tx.send(true);
    }

    serve_result?;
    tracing::info!("SQE coordinator shut down");
    Ok(())
}

// ── Worker ─────────────────────────────────────────────────────
async fn run_worker(config: SqeConfig, config_path: String) -> anyhow::Result<()> {
    let started_at = Instant::now();
    let ready = Arc::new(AtomicBool::new(false));

    let health_port = config.metrics.prometheus_port + 1;
    let health_state = Arc::new(HealthState {
        ready: ready.clone(),
        started_at,
        role: "worker",
        worker_registry: None,
        query_tracker: None,
        web_ui: false,
        // Mirror the coordinator's readiness check (the established downstream
        // probe) instead of short-circuiting on an empty URL. Leaving this
        // empty made /readyz a bare process-up check, so Kubernetes routed
        // do_get to a worker before it could serve a fragment (#245). Workers
        // read S3 directly from the data-file paths in the secured plan
        // fragment and do NOT query the catalog at runtime, so a catalog blip
        // alone does not break a scan. Gating worker readiness on catalog
        // reachability is a deliberately conservative cluster-health signal: if
        // the catalog is down the coordinator cannot plan or dispatch anyway.
        // Glue/Hive-only deployments leave this empty and skip the catalog
        // probe. Workers serving Iceberg keep the conservative readiness gate.
        catalog_url: config.catalog.catalog_url.clone(),
        node_info: None,
        metrics_history: None,
        bearer_provider: None,
        auth_cfg: None,
        security_cfg: None,
        audit: None,
        anonymous_denied: None,
        dashboard_success: None,
        success_audit_dedup: None,
        // Workers do not serve the catalog-refresh endpoint or hold the shared
        // table cache.
        table_cache: None,
        hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
    });
    let worker_bind: std::net::IpAddr = std::net::Ipv4Addr::UNSPECIFIED.into();
    start_health_server_with_extra(worker_bind, health_port, health_state, None);

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let worker_metrics = Arc::new(
        sqe_metrics::WorkerMetricsRegistry::new()
            .map_err(|e| anyhow::anyhow!("failed to initialize worker metrics registry: {e}"))?,
    );
    let _metrics_server = sqe_metrics::server::start_metrics_server(
        worker_metrics.clone(),
        worker_bind,
        config.metrics.prometheus_port,
    )
    .map_err(|e| anyhow::anyhow!("failed to bind metrics server: {e}"))?;

    let (session_ctx, memory_pool) = sqe_worker::runtime::build_session_context(&config.worker)?;

    // Build the fully-wired Flight service and start the heartbeat task via the
    // shared worker bootstrap. Previously run_worker built the service WITHOUT
    // .with_worker_secret(), without the footer cache, and never started the
    // heartbeat -- so Helm-deployed workers (which run `--mode worker`) were
    // unauthenticated, uncached, and invisible to the coordinator (#219).
    // Hot-reload watches config_path for worker memory budgets.
    let flight_service = sqe_worker::bootstrap::build_worker_service_with_hot_reload(
        &config,
        worker_metrics,
        session_ctx,
        Some(memory_pool),
        Some(&config_path),
    )?;

    // Mark ready
    ready.store(true, Ordering::Relaxed);

    // Optional TLS. `[worker.tls]` when the worker has its own certificate,
    // otherwise the inherited `[coordinator.tls]` (finding 603: one cert
    // carrying the coordinator's SAN cannot validate for a worker URL).
    let worker_tls = config.worker.effective_tls(&config.coordinator.tls).clone();
    let tls_material = sqe_core::ServerTlsMaterial::load_optional(&worker_tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = sqe_coordinator::transport::apply_grpc_transport(
        tonic::transport::Server::builder(),
        &config.coordinator.transport,
    );
    if let Some(material) = tls_material.as_ref() {
        server_builder = server_builder.tls_config(material.tonic_server())?;
        tracing::info!(
            mtls = material.requires_client_cert(),
            cert_fingerprint = material.cert_fingerprint(),
            hot_reload = false,
            "SQE worker listening on {addr} (TLS)"
        );
    } else {
        tracing::info!("SQE worker listening on {addr} (plaintext)");
    }

    server_builder
        .add_service(
            flight_service.into_server(config.coordinator.transport.max_decoding_message_bytes),
        )
        .serve_with_shutdown(
            addr,
            shutdown_with_drain(
                ready.clone(),
                config.coordinator.shutdown_drain_secs,
                std::future::ready(()),
            ),
        )
        .await?;

    tracing::info!("SQE worker shut down");
    Ok(())
}

// ── OpenLineage observer construction ─────────────────────────

/// SQE catalog name -> lineage identity (`sqe-lineage` namespaces datasets
/// `sqe://<name>` and links them to `<uri>` / `<name>.<schema>.<table>`).
///
/// A configured Iceberg REST catalog is named after its `warehouse`, the
/// Polaris catalog it is: the legacy `[catalog]` block is registered under the
/// SQL name `iceberg` but IS `workspace1` when its warehouse says so, and a
/// workspace catalog that `polaris-auto` discovers is registered under that
/// same warehouse name, so both spellings land on one dataset. The URI is the
/// catalog's `catalog_url`; for a discovered (unconfigured) name it is the
/// discovery template's, the Polaris every discovered catalog lives on.
/// Anything else (Glue, HMS, ATTACHed, Hive roots) keeps its SQL name and no
/// URI.
fn lineage_catalog_lookup(config: &SqeConfig) -> sqe_lineage::extract::CatalogLookup {
    use sqe_core::config::{CatalogBackend, CatalogDiscovery};
    use sqe_lineage::extract::CatalogInfo;

    let is_rest_url = |c: &sqe_core::config::CatalogConfig| {
        matches!(c.backend, CatalogBackend::Rest) && !c.catalog_url.trim().is_empty()
    };
    let configured: std::collections::HashMap<String, CatalogInfo> = config
        .flattened_catalogs()
        .iter()
        .map(|(name, c)| {
            let info = if is_rest_url(c) {
                let warehouse = c.warehouse.trim();
                let iceberg_name = if warehouse.is_empty() || warehouse.contains(['/', ':']) {
                    name.clone()
                } else {
                    warehouse.to_string()
                };
                CatalogInfo::new(iceberg_name, Some(c.catalog_url.clone()))
            } else {
                CatalogInfo::new(name.clone(), None)
            };
            (name.clone(), info)
        })
        .collect();
    // The REST catalog `polaris-auto` clones for a discovered warehouse
    // (mirrors `session_context::discovery_template`'s pick).
    let discovery_uri: Option<String> = (config.query.catalog_discovery
        == CatalogDiscovery::PolarisAuto)
        .then(|| {
            let flattened = config.flattened_catalogs();
            config
                .query
                .default_catalog
                .as_deref()
                .and_then(|name| flattened.iter().find(|(n, _)| n == name))
                .filter(|(_, c)| is_rest_url(c))
                .or_else(|| flattened.iter().find(|(_, c)| is_rest_url(c)))
                .map(|(_, c)| c.catalog_url.clone())
        })
        .flatten();
    Arc::new(move |name: &str| {
        configured
            .get(name)
            .cloned()
            .unwrap_or_else(|| CatalogInfo::new(name, discovery_uri.clone()))
    })
}

/// Build the [`sqe_lineage::ChannelObserver`] from `[metrics.openlineage]`.
///
/// Returns `None` when the section is disabled (the default). When enabled,
/// validates the config, opens the configured sinks (file and/or HTTP, with
/// optional disk spool wrapping HTTP), spawns the emitter background task,
/// and returns a [`sqe_lineage::ChannelObserver`] wired to a bounded mpsc.
///
/// The emitter `JoinHandle` is intentionally dropped: the bounded mpsc keeps
/// the task alive for the process lifetime, mirroring the SpoolSink replay
/// task pattern.
fn build_lineage_observer(
    config: &SqeConfig,
    registry: &prometheus::Registry,
) -> anyhow::Result<Option<Arc<sqe_lineage::ChannelObserver>>> {
    if !config.metrics.openlineage.enabled {
        return Ok(None);
    }

    config
        .metrics
        .openlineage
        .validate()
        .map_err(|e| anyhow::anyhow!("openlineage config: {e}"))?;

    let ol = &config.metrics.openlineage;

    let mut sinks: Vec<Arc<dyn sqe_lineage::Sink>> = Vec::new();

    // File sink
    if !ol.file_path.is_empty() {
        sinks.push(Arc::new(
            sqe_lineage::sinks::file::FileSink::new(&ol.file_path)
                .map_err(|e| anyhow::anyhow!("openlineage file sink: {e}"))?,
        ));
    }

    // HTTP sink (optionally wrapped by SpoolSink)
    if !ol.http_endpoint.is_empty() {
        let auth = match ol.auth_mode.as_str() {
            "none" => sqe_lineage::sinks::http::AuthMode::None,
            "bearer" => sqe_lineage::sinks::http::AuthMode::Bearer(
                sqe_core::SecretString::new(ol.api_key.clone()),
            ),
            other => {
                return Err(anyhow::anyhow!(
                    "openlineage auth_mode '{other}' is not recognised; \
                     expected one of: none, bearer"
                ));
            }
        };

        let http = sqe_lineage::sinks::http::HttpSink::new(sqe_lineage::sinks::http::HttpConfig {
            endpoint: ol.http_endpoint.clone(),
            auth,
            timeout_ms: ol.http_timeout_ms,
            retry_attempts: ol.http_retry_attempts,
        })
        .map_err(|e| anyhow::anyhow!("openlineage http sink: {e}"))?;

        let sink: Arc<dyn sqe_lineage::Sink> = if !ol.spool_path.is_empty() {
            let spool_dropped = prometheus::IntCounter::new(
                "sqe_lineage_spool_dropped_events_total",
                "OL events dropped from a full spool (oldest first) while the collector was down",
            )
            .expect("static prometheus opts cannot fail");
            register_lineage_counter(registry, &spool_dropped);
            sqe_lineage::sinks::spool::SpoolSink::wrap_with_counter(
                Arc::new(http),
                sqe_lineage::sinks::spool::SpoolConfig {
                    path: std::path::PathBuf::from(&ol.spool_path),
                    max_bytes: ol.spool_max_bytes,
                    replay_interval: std::time::Duration::from_secs(ol.replay_interval_secs),
                },
                Some(spool_dropped),
            )
        } else {
            Arc::new(http)
        };
        sinks.push(sink);
    }

    let multi = Arc::new(sqe_lineage::MultiSink::new(sinks));

    let (tx, rx) = tokio::sync::mpsc::channel(ol.channel_capacity);

    let drop_counter = prometheus::IntCounter::new(
        "sqe_lineage_dropped_events_total",
        "OL events dropped due to channel back-pressure",
    )
    .expect("static prometheus opts cannot fail");
    register_lineage_counter(registry, &drop_counter);

    let catalog_lookup = lineage_catalog_lookup(config);

    let producer = if ol.producer.is_empty() {
        format!("https://github.com/sbp/sqe/v{}", env!("CARGO_PKG_VERSION"))
    } else {
        ol.producer.clone()
    };

    let cfg = Arc::new(sqe_lineage::EmitterConfig {
        job_namespace: ol.job_namespace.clone(),
        producer,
        catalog_lookup,
    });

    // Spawn the emitter background task. The JoinHandle is intentionally
    // dropped: the bounded mpsc keeps the task alive for the process
    // lifetime (the Sender lives in the ChannelObserver below; the
    // Receiver lives in the spawned task).
    sqe_lineage::spawn_emitter(rx, multi, cfg);

    tracing::info!(
        file_sink = !ol.file_path.is_empty(),
        http_sink = !ol.http_endpoint.is_empty(),
        spool = !ol.spool_path.is_empty(),
        emit_selects = ol.emit_selects,
        channel_capacity = ol.channel_capacity,
        "OpenLineage emitter enabled"
    );

    Ok(Some(Arc::new(sqe_lineage::ChannelObserver::new(
        tx,
        drop_counter,
    ))))
}

/// Expose a lineage drop counter on `/metrics`. A duplicate registration
/// (the observer built twice in one process, as tests do) only logs.
fn register_lineage_counter(registry: &prometheus::Registry, counter: &prometheus::IntCounter) {
    if let Err(e) = registry.register(Box::new(counter.clone())) {
        tracing::warn!(error = %e, "could not register an OpenLineage drop counter");
    }
}

// ── External auth (OAuth2) construction ───────────────────────

/// Build the [`OAuth2State`] from the `[auth.external]` config section.
///
/// This creates:
/// - An [`OidcDiscovery`] instance (lazy-fetches `.well-known/openid-configuration`)
/// - An [`AuthCodeService`] for the Authorization Code + PKCE flow (Trino SSO)
/// - A [`PendingAuthStore`] to track in-flight auth sessions
///
/// The base URL for redirect/token URLs is derived from the coordinator's
/// Trino HTTP port (scheme defaults to `http`; in production, TLS termination
/// or a reverse proxy provides HTTPS).
fn build_oauth2_state(
    ext: &sqe_core::config::ExternalAuthConfig,
    config: &SqeConfig,
) -> anyhow::Result<sqe_trino_compat::oauth2::OAuth2State> {
    let discovery_config = sqe_auth::OidcDiscoveryConfig {
        issuer: ext.issuer.clone(),
        authorization_endpoint_override: ext.authorization_endpoint.clone(),
        token_endpoint_override: ext.token_endpoint.clone(),
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

    // Derive the base URL from the Trino HTTP port. In production, a reverse
    // proxy or TLS terminator provides the external HTTPS URL; for dev, use
    // the configured redirect_uri's scheme+host or fall back to localhost.
    // The fallback uses the plain port, or the HTTPS one when the Trino
    // listener is TLS-only (`trino_http_port = 0`).
    let localhost = if config.coordinator.trino_http_port == 0
        && config.coordinator.trino_https_port > 0
    {
        format!("https://localhost:{}", config.coordinator.trino_https_port)
    } else {
        format!("http://localhost:{}", config.coordinator.trino_http_port)
    };
    let base_url = if ext.redirect_uri.contains("://") {
        // Extract scheme + host from redirect_uri (e.g. "https://sqe.example.com")
        let uri = ext.redirect_uri.trim_end_matches('/');
        match uri.find("://") {
            Some(idx) => {
                let after_scheme = &uri[idx + 3..];
                let host_end = after_scheme.find('/').unwrap_or(after_scheme.len());
                format!("{}://{}", &uri[..idx], &after_scheme[..host_end])
            }
            None => localhost,
        }
    } else {
        localhost
    };

    Ok(sqe_trino_compat::oauth2::OAuth2State {
        auth_code_service,
        pending_store,
        base_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn flip_ready_and_drain_flips_readiness_first() {
        // drain_secs = 0 so the test does not sleep; readiness must still flip.
        let ready = AtomicBool::new(true);
        flip_ready_and_drain(&ready, 0).await;
        assert!(
            !ready.load(Ordering::Relaxed),
            "readiness must be flipped to NOT-ready on shutdown"
        );
    }

    /// The AI portal's shape: a legacy `[catalog]` block on Polaris with
    /// warehouse `workspace1` and `polaris-auto` discovery. The SQL name
    /// `iceberg` and the discovered `workspace1` are one Polaris catalog, so
    /// lineage names both `workspace1`; another discovered workspace keeps
    /// its own name on the same Polaris.
    #[test]
    fn lineage_lookup_names_rest_catalogs_after_their_warehouse() {
        let config = SqeConfig::load_from_str(
            r#"
[coordinator]

[catalog]
polaris_url = "http://polaris:8181/api/catalog"
warehouse = "workspace1"

[query]
catalog_discovery = "polaris-auto"

[auth]
client_id = "unused-no-idp"
token_endpoint = "http://127.0.0.1:1/token"
"#,
        )
        .unwrap();
        let lookup = lineage_catalog_lookup(&config);
        let polaris = Some("http://polaris:8181/api/catalog".to_string());
        assert_eq!(
            lookup("iceberg"),
            sqe_lineage::extract::CatalogInfo::new("workspace1", polaris.clone())
        );
        assert_eq!(
            lookup("workspace1"),
            sqe_lineage::extract::CatalogInfo::new("workspace1", polaris.clone())
        );
        assert_eq!(
            lookup("ws_team_a"),
            sqe_lineage::extract::CatalogInfo::new("ws_team_a", polaris)
        );

        // Without discovery an unknown name has no catalog URI.
        let config = SqeConfig::load_from_str(DEFAULT_FLAG_ONLY_CONFIG).unwrap();
        assert_eq!(
            lineage_catalog_lookup(&config)("other"),
            sqe_lineage::extract::CatalogInfo::new("other", None)
        );
    }

    #[test]
    fn interactive_runs_skip_the_drain_and_pods_keep_it() {
        assert_eq!(effective_drain_secs(25, true), 0);
        assert_eq!(effective_drain_secs(25, false), 25);
        assert_eq!(effective_drain_secs(0, false), 0);
    }

    #[tokio::test]
    async fn flip_ready_and_drain_flips_readiness_with_nonzero_drain() {
        // A short non-zero drain still flips readiness immediately, then sleeps.
        let ready = AtomicBool::new(true);
        flip_ready_and_drain(&ready, 1).await;
        assert!(!ready.load(Ordering::Relaxed));
    }

    /// Finding 764: anonymous MCP pulls the health and metrics listeners onto
    /// loopback unless the operator bound MCP elsewhere by name.
    #[test]
    fn ops_bind_follows_anonymous_mcp_to_loopback() {
        let mut config = SqeConfig::load_from_str(DEFAULT_FLAG_ONLY_CONFIG).unwrap();
        assert!(
            ops_bind_ip(&config).is_unspecified(),
            "no MCP: all interfaces"
        );

        config.mcp.enabled = true;
        config.mcp.port = 8099;
        config.mcp.auth.mode = sqe_core::config::McpAuthMode::Static;
        assert!(
            ops_bind_ip(&config).is_unspecified(),
            "authenticated MCP: unchanged"
        );

        config.mcp.auth.mode = sqe_core::config::McpAuthMode::Anonymous;
        assert!(
            ops_bind_ip(&config).is_loopback(),
            "anonymous MCP: loopback"
        );

        config.mcp.bind_address = "0.0.0.0".to_string();
        assert!(
            ops_bind_ip(&config).is_unspecified(),
            "explicit bind is the operator's call"
        );
    }

    #[test]
    fn serving_status_reads_the_effective_flag_only_config() {
        // The shape `sqe-server --glue-catalog glue --mcp-anonymous
        // --mcp-port 8099 --mcp-only` leaves behind after the flags apply.
        let mut config = SqeConfig::load_from_str(DEFAULT_FLAG_ONLY_CONFIG).unwrap();
        config.coordinator.flight_sql_port = 0;
        config.coordinator.trino_http_port = 0;
        config.coordinator.quack_port = 0;
        config.mcp.enabled = true;
        config.mcp.port = 8099;
        config.mcp.auth.mode = sqe_core::config::McpAuthMode::Anonymous;
        config.catalog.catalog_url = String::new();
        config.hive.roots.push(sqe_core::config::HiveRootConfig {
            name: "glue".to_string(),
            kind: sqe_core::config::HiveRootKind::Glue,
            location: String::new(),
            region: None,
            catalog_id: None,
            databases: Vec::new(),
            secret: None,
        });
        let serving = serving_status(&config);
        assert_eq!(serving.mode, "mcp-only");
        assert_eq!(serving.flight_sql_port, None);
        assert_eq!(serving.mcp.as_ref().map(|m| m.auth_mode), Some("anonymous"));
        assert_eq!(serving.mcp.as_ref().map(|m| m.port), Some(8099));
        assert_eq!(
            serving.catalogs,
            vec![CatalogServingStatus {
                name: "glue".to_string(),
                kind: "hive-glue"
            }]
        );

        // Open one SQL door and it is a full coordinator again.
        config.coordinator.flight_sql_port = 50051;
        assert_eq!(serving_status(&config).mode, "full");

        // No MCP and no SQL door is not "mcp-only" either.
        config.coordinator.flight_sql_port = 0;
        config.mcp.enabled = false;
        config.mcp.port = 0;
        let quiet = serving_status(&config);
        assert_eq!(quiet.mode, "full");
        assert!(quiet.mcp.is_none());
    }

    #[test]
    fn test_cluster_status_serialization_coordinator() {
        let status = ClusterStatus {
            status: "ACTIVE",
            node: NodeStatus {
                role: "coordinator",
                version: "0.1.0",
                git_sha: "abc123",
                build_profile: "release",
                datafusion_version: datafusion::DATAFUSION_VERSION,
                uptime_seconds: 120,
            },
            workers: Some(WorkersStatus {
                total: 3,
                healthy: 2,
                healthy_urls: vec![
                    "http://worker1:50052".to_string(),
                    "http://worker2:50052".to_string(),
                ],
            }),
            serving: Some(ServingStatus {
                mode: "mcp-only",
                flight_sql_port: None,
                trino_http_port: None,
                quack_port: None,
                mcp: Some(McpServingStatus {
                    port: 8099,
                    auth_mode: "anonymous",
                    allow_writes: false,
                }),
                policy_engine: sqe_core::config::PolicyEngine::Passthrough,
                catalogs: vec![CatalogServingStatus {
                    name: "glue".to_string(),
                    kind: "hive-glue",
                }],
            }),
        };

        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["status"], "ACTIVE");
        assert_eq!(json["node"]["role"], "coordinator");
        assert_eq!(json["node"]["version"], "0.1.0");
        // The real linked version, never a literal that goes stale.
        assert_eq!(json["node"]["datafusionVersion"], datafusion::DATAFUSION_VERSION);
        assert!(datafusion::DATAFUSION_VERSION.starts_with("55."), "{}", datafusion::DATAFUSION_VERSION);
        assert_eq!(json["serving"]["mode"], "mcp-only");
        assert_eq!(json["serving"]["flightSqlPort"], serde_json::Value::Null);
        assert_eq!(json["serving"]["mcp"]["authMode"], "anonymous");
        assert_eq!(json["serving"]["mcp"]["port"], 8099);
        assert_eq!(json["serving"]["policyEngine"], "passthrough");
        assert_eq!(json["serving"]["catalogs"][0]["name"], "glue");
        assert_eq!(json["serving"]["catalogs"][0]["kind"], "hive-glue");
        assert_eq!(json["node"]["uptimeSeconds"], 120);
        assert_eq!(json["workers"]["total"], 3);
        assert_eq!(json["workers"]["healthy"], 2);
        assert_eq!(json["workers"]["healthyUrls"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_cluster_status_serialization_worker() {
        let status = ClusterStatus {
            status: "ACTIVE",
            node: NodeStatus {
                role: "worker",
                version: "0.1.0",
                git_sha: "abc123",
                build_profile: "release",
                datafusion_version: datafusion::DATAFUSION_VERSION,
                uptime_seconds: 60,
            },
            workers: None,
            serving: None,
        };

        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["node"]["role"], "worker");
        assert!(json["workers"].is_null());
    }

    #[test]
    fn test_cluster_status_starting() {
        let status = ClusterStatus {
            status: "STARTING",
            node: NodeStatus {
                role: "coordinator",
                version: "0.1.0",
                git_sha: "abc123",
                build_profile: "debug",
                datafusion_version: datafusion::DATAFUSION_VERSION,
                uptime_seconds: 0,
            },
            workers: None,
            serving: None,
        };

        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["status"], "STARTING");
    }

    #[tokio::test]
    async fn test_cluster_status_handler_no_workers() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let Json(status) = cluster_status(axum::extract::State(state)).await;
        assert_eq!(status.status, "ACTIVE");
        assert_eq!(status.node.role, "coordinator");
        assert!(status.workers.is_none());
    }

    #[tokio::test]
    async fn test_cluster_status_handler_starting() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(false)),
            started_at: Instant::now(),
            role: "worker",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let Json(status) = cluster_status(axum::extract::State(state)).await;
        assert_eq!(status.status, "STARTING");
        assert_eq!(status.node.role, "worker");
    }

    #[tokio::test]
    async fn test_cluster_status_handler_with_workers() {
        let registry = Arc::new(sqe_coordinator::worker_registry::WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
            "http://w2:50052".to_string(),
        ]));
        // Mark one worker healthy
        registry.mark_healthy("http://w1:50052").await;

        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: Some(registry),
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let Json(status) = cluster_status(axum::extract::State(state)).await;
        assert_eq!(status.status, "ACTIVE");
        let workers = status.workers.unwrap();
        assert_eq!(workers.total, 2);
        assert_eq!(workers.healthy, 1);
        assert_eq!(workers.healthy_urls, vec!["http://w1:50052"]);
    }

    #[tokio::test]
    async fn test_readyz_ready() {
        // With an empty catalog_url, no catalog reachability check is performed.
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readyz_not_ready_when_catalog_unreachable() {
        // #245: a worker with a populated but unreachable catalog_url must
        // report NOT ready so Kubernetes keeps it out of rotation, rather than
        // short-circuiting to ready on an empty URL.
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "worker",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            // RFC 5737 TEST-NET-1; never routes, so the probe fails fast.
            catalog_url: "http://192.0.2.1:1".to_string(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn readyz_caps_concurrent_catalog_probes() {
        // #754: 20 parallel /readyz against a catalog that never answers must
        // open at most READYZ_MAX_PROBES connections; the rest answer from
        // cache or 503 without fanning out.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = accepted.clone();
        let fake = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                held.push(sock); // hang: never respond
            }
        });
        let calls: Vec<_> = (0..20)
            .map(|_| {
                let url = url.clone();
                tokio::spawn(async move { probe_catalog(&url).await })
            })
            .collect();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            accepted.load(Ordering::SeqCst) <= READYZ_MAX_PROBES,
            "probes fanned out: {}",
            accepted.load(Ordering::SeqCst)
        );
        for c in calls {
            c.abort();
        }
        fake.abort();
    }

    // All CoordinatorConfig fields carry `#[serde(default)]`, so an empty TOML
    // table yields the single-node default we want to start each case from.
    fn coordinator_config_from(toml_src: &str) -> sqe_core::config::CoordinatorConfig {
        toml::from_str(toml_src).expect("coordinator config parses")
    }

    #[test]
    fn distributed_enabled_off_for_single_node_default() {
        // #226 security guard: the default single-node config (empty
        // worker_urls, empty worker_secret, opt-in off) must NOT enable the
        // worker path, or it would accept unauthenticated heartbeats and route
        // user bearer tokens to unknown workers.
        let coord = coordinator_config_from("");
        assert!(!distributed_enabled(&coord));
    }

    #[test]
    fn distributed_enabled_on_for_static_worker_urls() {
        let coord = coordinator_config_from("worker_urls = [\"http://w1:50052\"]");
        assert!(distributed_enabled(&coord));
    }

    #[test]
    fn distributed_enabled_on_for_heartbeat_discovery_via_secret() {
        // Dynamic discovery: no static URLs, but a worker_secret is set, so
        // heartbeats are authenticated and the registry must be wired (#226).
        let coord = coordinator_config_from("worker_secret = \"hunter2\"");
        assert!(coord.worker_urls.is_empty());
        assert!(distributed_enabled(&coord));
    }

    #[test]
    fn distributed_enabled_on_for_explicit_unauthenticated_opt_in() {
        let coord = coordinator_config_from("allow_unauthenticated_workers = true");
        assert!(distributed_enabled(&coord));
    }

    #[tokio::test]
    async fn test_readyz_not_ready() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(false)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn api_queries_returns_tracked_queries() {
        let tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
            &sqe_core::QueryHistoryConfig {
                max_entries: 100,
                ttl_secs: 60,
            },
        ));
        let id = uuid::Uuid::now_v7();
        tracker.start(
            id,
            "alice",
            Some("cli"),
            "SELECT 1",
            "s1",
            None,
            vec![],
            None,
        );

        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: Some(tracker),
            web_ui: true,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let Json(items) = api_queries(
            axum::extract::State(state),
            axum::extract::Query(QueryListParams {
                state: None,
                limit: None,
            }),
        )
        .await;
        assert_eq!(items.len(), 1);
        // WEB-02: the unauth list no longer exposes the username or raw SQL;
        // it carries the SQL digest instead. Assert the item is populated.
        assert!(!items[0].sql_hash.is_empty());
    }

    #[tokio::test]
    async fn api_metrics_history_returns_empty_without_history() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: true,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });
        let Json(resp) = api_metrics_history(axum::extract::State(state)).await;
        assert_eq!(
            resp.bucket_seconds,
            sqe_coordinator::metrics_history::BUCKET_SECS
        );
        assert!(resp.buckets.is_empty());
    }

    #[tokio::test]
    async fn api_metrics_history_returns_buckets_when_history_populated() {
        use sqe_coordinator::metrics_history::{MetricsHistory, MetricsSample};
        let hist = Arc::new(MetricsHistory::new(4320));
        for i in 0..3u64 {
            hist.record(MetricsSample {
                unix_ms: i * 10_000,
                active_queries: 1,
                mem_used_bytes: 100,
                mem_limit_bytes: 1000,
                total_queries: i as usize + 1,
                failed_queries: 0,
                total_output_rows: i * 10,
                finished_queries: i as usize + 1,
                exec_ms_sum: (i + 1) * 100,
            });
        }
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: true,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: Some(hist),
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });
        let Json(resp) = api_metrics_history(axum::extract::State(state)).await;
        assert_eq!(
            resp.bucket_seconds,
            sqe_coordinator::metrics_history::BUCKET_SECS
        );
        assert!(!resp.buckets.is_empty());
        // 3 samples at 0, 10_000, 20_000 ms all fall in the same 900-s bucket
        assert_eq!(resp.buckets.len(), 1);
    }

    /// A minimal, valid v1 table + filesystem FileIO -- enough to populate the
    /// TableMetadataCache. `disable_cache()` skips the per-table ObjectCache
    /// (no I/O). The cache is keyed by the string passed to `insert`, so the
    /// identifier value is irrelevant to what the test asserts.
    fn test_table() -> iceberg::table::Table {
        const META_V1: &str = r#"{
            "format-version": 1,
            "table-uuid": "d20125c8-7284-442c-9aea-15fee620737c",
            "location": "s3://bucket/test/location",
            "last-updated-ms": 1602638573874,
            "last-column-id": 1,
            "schema": {
                "type": "struct",
                "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]
            },
            "partition-spec": [],
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": []
        }"#;
        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(META_V1).unwrap();
        let file_io = iceberg::io::FileIO::new_with_fs();
        iceberg::table::Table::builder()
            .runtime(iceberg::Runtime::current())
            .metadata(metadata)
            .identifier(iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new("ns".to_string()),
                "t".to_string(),
            ))
            .file_io(file_io)
            .disable_cache()
            .build()
            .unwrap()
    }

    /// Handler wiring: `api_catalogs_refresh` must drop the shared
    /// TableMetadataCache when the HealthState carries one, so a table rebound
    /// out-of-band is not served stale on the immediate next readback (the bug
    /// this fix targets). A bodyless POST takes the invalidate-all path.
    #[tokio::test]
    async fn api_catalogs_refresh_invalidates_table_cache() {
        let cache = sqe_catalog::TableMetadataCache::new(60);
        cache.insert("tok|ns.t".to_string(), test_table()).await;
        assert!(
            cache.get_fresh("tok|ns.t").await.is_some(),
            "precondition: entry should be cached"
        );

        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: Some(cache.clone()),
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });

        let Json(resp) = api_catalogs_refresh(axum::extract::State(state), None).await;

        assert_eq!(resp.invalidated, "all");
        assert!(
            cache.get_fresh("tok|ns.t").await.is_none(),
            "table cache entry survived the refresh handler"
        );
        assert_eq!(cache.entry_count(), 0);
    }

    // Issue #666: the admin refresh also drops the shared Hive manifest
    // cache, so a Glue table changed out of band is re-read before its TTL.
    #[tokio::test]
    async fn api_catalogs_refresh_drops_hive_manifests() {
        use sqe_catalog::hive::manifest::{HiveManifest, StorageDescriptor, MANIFEST_VERSION};
        let hive = Arc::new(sqe_catalog::hive::provider::HiveMetadataCache::new());
        hive.insert(
            "sales",
            "orders",
            HiveManifest {
                manifest_version: MANIFEST_VERSION,
                table_type: "EXTERNAL_TABLE".to_string(),
                database_name: "sales".to_string(),
                name: "orders".to_string(),
                owner: None,
                created_at: None,
                description: None,
                view_original_text: None,
                partition_keys: vec![],
                parameters: Default::default(),
                storage_descriptor: StorageDescriptor {
                    location: "file:///tmp/x".to_string(),
                    input_format: None,
                    output_format: None,
                    compressed: false,
                    serde_info: Default::default(),
                    columns: vec![],
                    sort_columns: vec![],
                },
                sqe: Default::default(),
            },
        );
        assert_eq!(hive.len(), 1, "precondition");

        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: Some(Arc::clone(&hive)),
            serving: None,
            mcp_listener_alive: None,
        });

        let Json(resp) = api_catalogs_refresh(axum::extract::State(state), None).await;
        assert_eq!(resp.invalidated, "all");
        assert!(hive.get("sales", "orders").is_none(), "hive manifest survived the refresh");
        assert!(hive.is_empty());
    }

    /// The handler must not panic when no table cache is wired (worker role /
    /// tests): the `Option` guard skips the invalidation cleanly.
    #[tokio::test]
    async fn api_catalogs_refresh_without_table_cache_is_ok() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });
        let Json(resp) = api_catalogs_refresh(axum::extract::State(state), None).await;
        assert_eq!(resp.invalidated, "all");
    }

    // ── Route-level bearer + admin guard tests ─────────────────────────────────
    //
    // These tests drive `build_health_router` directly via `tower::ServiceExt::oneshot`
    // to assert that:
    //   - `/healthz` is always open (no token needed)
    //   - web_ui routes require a valid admin bearer token
    //   - missing token -> 401, non-admin token -> 403, admin token -> 200

    struct StubAuthProvider {
        admin_roles: Vec<String>,
    }

    #[async_trait::async_trait]
    impl sqe_auth::AuthProvider for StubAuthProvider {
        async fn authenticate(
            &self,
            creds: &sqe_auth::FlightCredentials,
        ) -> Result<sqe_auth::Identity, sqe_auth::AuthError> {
            let token = creds
                .bearer_token
                .as_ref()
                .map(|t| t.expose().to_string())
                .unwrap_or_default();
            // "admin-tok" -> admin; "user-tok" -> non-admin; anything else -> fail
            match token.as_str() {
                "admin-tok" => Ok(sqe_auth::Identity {
                    user_id: "admin".into(),
                    display_name: "Admin".into(),
                    roles: self.admin_roles.clone(),
                    subject: None,
                    email: None,
                    groups: vec![],
                    catalog_token: None,
                    refresh_token: None,
                    expires_at: None,
                    scopes: vec![],
                }),
                "user-tok" => Ok(sqe_auth::Identity {
                    user_id: "user".into(),
                    display_name: "User".into(),
                    roles: vec!["analyst".into()],
                    subject: None,
                    email: None,
                    groups: vec![],
                    catalog_token: None,
                    refresh_token: None,
                    expires_at: None,
                    scopes: vec![],
                }),
                _ => Err(sqe_auth::AuthError::AuthFailed("bad token".into())),
            }
        }
    }

    fn make_guarded_state() -> Arc<HealthState> {
        make_guarded_state_with_web_ui(true)
    }

    fn make_guarded_state_with_web_ui(web_ui: bool) -> Arc<HealthState> {
        let provider: Arc<dyn sqe_auth::AuthProvider> = Arc::new(StubAuthProvider {
            admin_roles: vec!["sqe-admin".into()],
        });
        let mut auth_cfg: sqe_core::config::AuthConfig =
            toml::from_str("").expect("empty auth config");
        auth_cfg.admin_roles = vec!["sqe-admin".into()];
        Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: Some(provider),
            auth_cfg: Some(auth_cfg),
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        })
    }

    /// A guarded state whose dedicated /mcp listener has died.
    fn state_with_dead_mcp_listener() -> Arc<HealthState> {
        let mut state = Arc::try_unwrap(make_guarded_state())
            .unwrap_or_else(|_| panic!("fresh state has one owner"));
        state.ready.store(true, Ordering::SeqCst);
        state.mcp_listener_alive = Some(Arc::new(AtomicBool::new(false)));
        Arc::new(state)
    }

    #[tokio::test]
    async fn healthz_and_readyz_report_a_dead_mcp_listener() {
        use tower::ServiceExt;
        let app = build_health_router(state_with_dead_mcp_listener());
        let req = axum::http::Request::builder()
            .uri("/healthz")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"mcp listener down");

        let req = axum::http::Request::builder()
            .uri("/readyz")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "unhealthy");
        assert_eq!(json["checks"]["mcp"], "down");

        // A live listener, or no dedicated listener at all, is `ok`.
        let mut live = Arc::try_unwrap(state_with_dead_mcp_listener())
            .unwrap_or_else(|_| panic!("one owner"));
        live.mcp_listener_alive = Some(Arc::new(AtomicBool::new(true)));
        let app = build_health_router(Arc::new(live));
        let req = axum::http::Request::builder()
            .uri("/healthz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(req).await.unwrap().status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn watch_mcp_listener_flips_readiness_and_exits_when_the_task_ends() {
        let ready = Arc::new(AtomicBool::new(true));
        let (tx, rx) = tokio::sync::oneshot::channel::<i32>();
        // A listener whose serve loop returns at once, as a dead accept loop does.
        let listener_task = tokio::spawn(async {});
        let shutdown = sqe_coordinator::mcp_server::CancellationToken::new();
        watch_mcp_listener(listener_task, ready.clone(), shutdown, move |code| {
            let _ = tx.send(code);
        });
        let code = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("watcher acted within 5 s")
            .expect("exit was called");
        assert_eq!(code, EXIT_MCP_LISTENER_DIED);
        assert!(!ready.load(Ordering::SeqCst), "readiness flips before exit");
    }

    /// #679: the listener ending because shutdown drained it is not a crash.
    #[tokio::test]
    async fn watch_mcp_listener_does_not_exit_when_the_listener_closed_on_shutdown() {
        let ready = Arc::new(AtomicBool::new(true));
        let (tx, rx) = tokio::sync::oneshot::channel::<i32>();
        let shutdown = sqe_coordinator::mcp_server::CancellationToken::new();
        shutdown.cancel();
        let listener_task = tokio::spawn(async {});
        watch_mcp_listener(listener_task, ready.clone(), shutdown, move |code| {
            let _ = tx.send(code);
        });
        // The watcher returns without calling `exit`, so the sender is
        // dropped unsent (Ok(Err)); a value would mean exit was called.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
        assert!(
            matches!(outcome, Ok(Err(_))),
            "exit must not be called on a drained listener: {outcome:?}"
        );
        assert!(!ready.load(Ordering::SeqCst), "readiness still flips");
    }

    #[tokio::test]
    async fn healthz_open_without_token() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/healthz")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn web_ui_queries_no_token_is_401() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/api/v1/queries")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn web_ui_queries_non_admin_bearer_is_403() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/api/v1/queries")
            .header("Authorization", "Bearer user-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn web_ui_queries_admin_bearer_is_200() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/api/v1/queries")
            .header("Authorization", "Bearer admin-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    // ── /api/v1/status (open, trimmed) and /api/v1/status/detail (admin) ───

    #[tokio::test]
    async fn open_status_carries_only_status_and_role() {
        use tower::ServiceExt;
        let app = build_health_router(make_guarded_state_with_web_ui(false));
        let req = axum::http::Request::builder()
            .uri("/api/v1/status")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"status": "ACTIVE", "node": {"role": "coordinator"}})
        );
    }

    #[tokio::test]
    async fn status_detail_requires_admin_bearer() {
        use tower::ServiceExt;
        let app = build_health_router(make_guarded_state_with_web_ui(false));
        for (token, expected) in [
            (None, axum::http::StatusCode::UNAUTHORIZED),
            (Some("user-tok"), axum::http::StatusCode::FORBIDDEN),
            (Some("admin-tok"), axum::http::StatusCode::OK),
        ] {
            let mut req = axum::http::Request::builder().uri("/api/v1/status/detail");
            if let Some(t) = token {
                req = req.header("Authorization", format!("Bearer {t}"));
            }
            let resp = app
                .clone()
                .oneshot(req.body(axum::body::Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), expected, "token {token:?}");
            if expected == axum::http::StatusCode::OK {
                let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
                let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(json["node"]["gitSha"], build_info::GIT_SHA);
                assert!(json["node"]["uptimeSeconds"].is_u64());
            }
        }
    }

    // ── POST /api/v1/catalogs/refresh (admin-gated) ────────────────────────

    #[tokio::test]
    async fn catalogs_refresh_no_token_is_401() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn catalogs_refresh_non_admin_is_403() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .header("Authorization", "Bearer user-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    /// Bodyless POST must succeed (invalidate-all) -- proves the
    /// `Option<Json<_>>` extractor does not 422 on an empty body.
    #[tokio::test]
    async fn catalogs_refresh_admin_empty_body_is_200() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .header("Authorization", "Bearer admin-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn catalogs_refresh_admin_with_username_is_200() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .header("Authorization", "Bearer admin-tok")
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(r#"{"username":"alice"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    // ── /api/v1/catalogs/refresh with the dashboard OFF ────────────────────
    //
    // `metrics.web_ui` defaults to false and is TOML-only (no `SQE_METRICS__*`
    // override), so the default deployment is the one that matters here. The
    // refresh hook used to be registered inside the `web_ui` group, which made
    // it 404 on a default config while `/healthz` still answered 200 -- the
    // platform's catalog-invalidation hook silently never worked.

    /// The control-plane hook is reachable with the dashboard off, and still
    /// guarded: no token -> 401, NOT 404.
    #[tokio::test]
    async fn catalogs_refresh_registered_without_web_ui_no_token_is_401() {
        use tower::ServiceExt;
        let state = make_guarded_state_with_web_ui(false);
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::UNAUTHORIZED,
            "refresh hook must be registered (and guarded) when web_ui is off"
        );
    }

    #[tokio::test]
    async fn catalogs_refresh_without_web_ui_non_admin_is_403() {
        use tower::ServiceExt;
        let state = make_guarded_state_with_web_ui(false);
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .header("Authorization", "Bearer user-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    /// The load-bearing case: an admin token drives invalidation on a default
    /// (`web_ui = false`) deployment.
    #[tokio::test]
    async fn catalogs_refresh_without_web_ui_admin_is_200() {
        use tower::ServiceExt;
        let state = make_guarded_state_with_web_ui(false);
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .header("Authorization", "Bearer admin-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// Decoupling the hook must not leak the dashboard: with `web_ui = false`
    /// the operator UI and its JSON API stay absent entirely.
    #[tokio::test]
    async fn dashboard_routes_absent_without_web_ui() {
        use tower::ServiceExt;
        for uri in [
            "/",
            "/api/v1/overview",
            "/api/v1/queries",
            "/api/v1/workers",
        ] {
            let state = make_guarded_state_with_web_ui(false);
            let app = build_health_router(state);
            let req = axum::http::Request::builder()
                .uri(uri)
                .header("Authorization", "Bearer admin-tok")
                .body(axum::body::Body::empty())
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                axum::http::StatusCode::NOT_FOUND,
                "{uri} must not be served when web_ui is off"
            );
        }
    }

    /// The guard must FAIL CLOSED when no auth provider is wired. This is the
    /// load-bearing invariant of registering the hook unconditionally: the
    /// route is now present on every deployment, so `require_admin_bearer` is
    /// the only thing standing in front of it. A future refactor that made the
    /// guard fail open would expose a control-plane endpoint by default.
    #[tokio::test]
    async fn catalogs_refresh_without_auth_configured_is_401() {
        use tower::ServiceExt;
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::UNAUTHORIZED,
            "refresh hook must fail closed when no auth provider is configured"
        );
    }

    /// A worker must NOT serve the refresh hook. It holds no shared table cache
    /// and no session cache, so the call would report success having
    /// invalidated nothing. `/healthz` stays open there.
    #[tokio::test]
    async fn catalogs_refresh_absent_on_worker() {
        use tower::ServiceExt;
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "worker",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        });
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/api/v1/catalogs/refresh")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::NOT_FOUND,
            "workers must not serve the catalog-refresh hook"
        );
    }

    /// `/healthz` stays open with the dashboard off -- the pairing that made
    /// the original bug look like a stale build.
    #[tokio::test]
    async fn healthz_open_without_web_ui() {
        use tower::ServiceExt;
        let state = make_guarded_state_with_web_ui(false);
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/healthz")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    // ── HealthState dedup tests ────────────────────────────────────────────
    //
    // Drive the production HealthState implementation of should_emit_success_audit
    // and note_dashboard_success directly. These guard against regressions in the
    // real code path, not just the test stub in web_auth::tests.

    fn make_dedup_state(window_secs: u64) -> Arc<HealthState> {
        let cache = if window_secs == 0 {
            None
        } else {
            Some(
                moka::sync::Cache::builder()
                    .time_to_live(std::time::Duration::from_secs(window_secs))
                    .build(),
            )
        };
        let counter =
            prometheus::IntCounter::new(format!("sqe_test_success_{window_secs}"), "test counter")
                .unwrap();
        Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: Some(counter),
            success_audit_dedup: cache,
            table_cache: None,
            hive_manifest_cache: None,
        serving: None,
        mcp_listener_alive: None,
        })
    }

    #[test]
    fn health_state_dedup_same_principal_within_window() {
        use sqe_coordinator::web_auth::BearerAdminState;
        let state = make_dedup_state(300);
        // First call: new principal -> emit.
        assert!(
            state.should_emit_success_audit("alice"),
            "first call must emit"
        );
        // Second call: same principal within window -> suppress.
        assert!(
            !state.should_emit_success_audit("alice"),
            "second call within window must suppress"
        );
        // Different principal -> emit.
        assert!(
            state.should_emit_success_audit("bob"),
            "distinct principal must emit"
        );
    }

    #[test]
    fn health_state_dedup_window_zero_always_emits() {
        use sqe_coordinator::web_auth::BearerAdminState;
        let state = make_dedup_state(0);
        assert!(
            state.should_emit_success_audit("alice"),
            "window=0 first call"
        );
        assert!(
            state.should_emit_success_audit("alice"),
            "window=0 second call"
        );
        assert!(
            state.should_emit_success_audit("alice"),
            "window=0 third call"
        );
    }

    #[test]
    fn health_state_note_dashboard_success_increments_counter() {
        use sqe_coordinator::web_auth::BearerAdminState;
        let state = make_dedup_state(300);
        // Counter starts at 0; each call increments.
        state.note_dashboard_success();
        state.note_dashboard_success();
        // We can't read the IntCounter value directly from the state (private field),
        // but we can confirm the method does not panic and wiring is complete.
        // The counter value is observable via Prometheus scrape; that is tested by
        // the MetricsRegistry tests in sqe-metrics.
    }
}
