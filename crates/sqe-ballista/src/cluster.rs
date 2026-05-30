//! Multi-process ballista cluster: a coordinator-embedded scheduler plus
//! standalone executor processes, replacing the Phase 2 standalone-per-query
//! facade.
//!
//! ## Topology
//!
//! - The coordinator embeds a ballista **scheduler** ([`start_scheduler`])
//!   and submits each query to it via [`submit_remote`].
//! - Each `sqe-worker` process is a ballista **executor** ([`run_executor`]).
//!
//! ## Auth scope (Phase 3 fallback + Phase 4b per-user)
//!
//! The scheduler and every executor build a single-tenant [`SessionCatalog`] /
//! `SqeCatalogProvider` from their **own config** (catalog url + warehouse +
//! static S3 creds via a client_credentials service token), and the SQE codecs
//! on the cluster side hold that config-built catalog. This is the no-bearer
//! fallback (matches the legacy distributed path's static `[storage]` creds).
//!
//! Phase 4b adds per-user passthrough: the authenticated user bearer is
//! threaded through the plan (client logical codec stamps it; the scheduler
//! attaches it to the rehydrated provider; it rides the physical
//! `EncodedSqeScan` to the executor, which mints a per-(user,table) `FileIO`
//! from it). Only the bearer travels, never S3 secrets. See the cutover design
//! doc (ledger D8) for the full path and why it bypasses ballista's
//! `ConfigExtension` propagation.
//!
//! ## Codec placement
//!
//! Ballista requires the logical + physical codecs to match across all three
//! sites: the client `SessionConfig` ([`submit_remote`]), the
//! `SchedulerConfig` ([`start_scheduler`]), and the `ExecutorProcessConfig`
//! ([`run_executor`]). Each site builds its own catalog and its own codecs
//! over that catalog.

use std::sync::Arc;

use anyhow::{Context, Result};
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use ballista_scheduler::scheduler_server::SessionBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::CatalogProvider;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use ballista_core::ConfigProducer;
use sqe_catalog::{SessionCatalog, SqeCatalogProvider, TableMetadataCache};
use sqe_core::config::{CatalogAuthConfig, CatalogConfig, SqeConfig, StorageConfig};

use crate::auth_ext::SqeAuthOptions;
use crate::sqe_codec::{SqeLogicalCodec, SqePhysicalCodec};

/// A ballista config producer that registers the [`SqeAuthOptions`] extension
/// so the per-query `sqe_auth.bearer` key round-trips (set/get succeed) on the
/// scheduler and executor. Without registration ballista silently drops the
/// unknown key during `update_from_key_value_pair`.
fn sqe_config_producer() -> ConfigProducer {
    Arc::new(|| {
        let config = SessionConfig::new_with_ballista();
        let mut config = config;
        config
            .options_mut()
            .extensions
            .insert(SqeAuthOptions::default());
        config
    })
}

/// A single-tenant catalog built from config, plus the codecs over it.
///
/// Shared by the scheduler and executor bootstraps and by the coordinator's
/// remote-submit path so all three sites use matching codecs.
pub struct ClusterCatalog {
    pub catalog_name: String,
    pub provider: Arc<dyn CatalogProvider>,
    pub session_catalog: Arc<SessionCatalog>,
    /// Catalog + storage config retained so the physical codec can mint
    /// per-user `SessionCatalog`s (Phase 4 bearer passthrough).
    cat_cfg: CatalogConfig,
    storage: StorageConfig,
    /// Shared table-metadata cache so executor-side `load_table` calls don't
    /// re-fetch metadata from the catalog on every scan task (avoids a
    /// metadata refetch storm on shuffle-heavy multi-stage queries).
    table_cache: TableMetadataCache,
}

impl ClusterCatalog {
    fn logical_codec(&self) -> Arc<SqeLogicalCodec> {
        Arc::new(SqeLogicalCodec::new(self.provider.clone()))
    }

    /// Bearer-aware variant of [`logical_codec`]. Used by [`submit_remote`] to
    /// stamp the authenticated user's bearer onto the encoded plan so executors
    /// can mint a per-user FileIO without relying on ballista session-config
    /// propagation (which silently drops `ConfigExtension` keys).
    fn logical_codec_with_bearer(&self, bearer: Option<Arc<str>>) -> Arc<SqeLogicalCodec> {
        Arc::new(SqeLogicalCodec::new_with_bearer(self.provider.clone(), bearer))
    }

    fn physical_codec(&self) -> Arc<SqePhysicalCodec> {
        Arc::new(SqePhysicalCodec::new(
            self.session_catalog.clone(),
            self.cat_cfg.clone(),
            self.storage.clone(),
            self.table_cache.clone(),
        ))
    }
}

/// Build the single-tenant cluster catalog from SQE config.
///
/// Mints a service bearer via the `[auth]` client_credentials grant (the
/// executor/scheduler have no user handshake), loads the catalog, and wraps
/// it as a `SqeCatalogProvider`. Uses the primary/legacy catalog block.
pub async fn build_cluster_catalog(config: &SqeConfig) -> Result<ClusterCatalog> {
    let catalog_name = config.resolve_default_catalog();

    // Resolve the primary CatalogConfig (legacy [catalog] or the named entry).
    let flattened = config.flattened_catalogs();
    let cat_cfg = flattened
        .iter()
        .find(|(name, _)| name == &catalog_name)
        .map(|(_, cfg)| *cfg)
        .unwrap_or(&config.catalog);

    let storage = cat_cfg.storage.clone().unwrap_or_else(|| config.storage.clone());

    // Shared table-metadata cache (mirrors the coordinator's). Without it the
    // executor re-fetches table metadata from Polaris on every scan task.
    let table_cache = TableMetadataCache::new(cat_cfg.metadata_cache_ttl_secs);

    // Service token via client_credentials from the top-level [auth] block.
    let auth = CatalogAuthConfig::ClientCredentials {
        token_endpoint: config.auth.token_endpoint.clone(),
        client_id: config.auth.client_id.clone(),
        client_secret: config.auth.client_secret.expose().to_string(),
        scope: None, // resolve_bearer defaults to PRINCIPAL_ROLE:ALL
    };
    let bearer = sqe_auth::per_catalog::resolve_bearer(&auth, "")
        .await
        .map_err(|e| anyhow::anyhow!("minting cluster service token: {e}"))?;

    let session_catalog = Arc::new(
        SessionCatalog::for_session_with(cat_cfg, &storage, Some(table_cache.clone()), &bearer)
            .await
            .map_err(|e| anyhow::anyhow!("building cluster SessionCatalog: {e}"))?,
    );

    // The cluster catalog is built once and reused for the cluster's lifetime,
    // so it must resolve namespaces created after construction (e.g. a CTAS
    // load that runs after the coordinator starts). Live schema resolution
    // bypasses the construction-time namespace snapshot for point lookups
    // (cutover ledger D12); table existence is still decided live on scan.
    let provider = SqeCatalogProvider::try_new(
        session_catalog.clone(),
        storage.clone(),
        cat_cfg.warehouse.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("building cluster SqeCatalogProvider: {e}"))?
    .with_live_schema_resolution();

    Ok(ClusterCatalog {
        catalog_name,
        provider: Arc::new(provider),
        session_catalog,
        cat_cfg: cat_cfg.clone(),
        storage,
        table_cache,
    })
}

/// Network/config knobs for an executor process.
pub struct ExecutorOptions {
    pub bind_host: String,
    pub external_host: Option<String>,
    pub flight_port: u16,
    pub grpc_port: u16,
    pub scheduler_host: String,
    pub scheduler_port: u16,
    pub concurrent_tasks: usize,
    /// Hard memory ceiling for this executor's DataFusion pool, in bytes.
    /// `None` = ballista default (effectively unbounded), which on a shared
    /// box lets co-located executors over-allocate and get OOM-killed under
    /// sustained shuffle-heavy workloads. Always set this in real deployments.
    pub memory_pool_bytes: Option<usize>,
}

/// Run a ballista executor process to completion (blocks until shutdown).
///
/// Builds the cluster catalog + SQE codecs from `config` and registers them
/// on the `ExecutorProcessConfig` so the executor can deserialize SQE scan
/// nodes and reload tables from the catalog.
pub async fn run_executor(config: &SqeConfig, opts: ExecutorOptions) -> Result<()> {
    let cluster = build_cluster_catalog(config)
        .await
        .context("building executor cluster catalog")?;

    let exec_config = ExecutorProcessConfig {
        bind_host: opts.bind_host,
        external_host: opts.external_host,
        port: opts.flight_port,
        grpc_port: opts.grpc_port,
        scheduler_host: opts.scheduler_host,
        scheduler_port: opts.scheduler_port,
        concurrent_tasks: opts.concurrent_tasks,
        memory_pool_size: opts.memory_pool_bytes.map(|b| b as u64),
        override_config_producer: Some(sqe_config_producer()),
        override_logical_codec: Some(cluster.logical_codec()),
        override_physical_codec: Some(cluster.physical_codec()),
        ..Default::default()
    };

    start_executor_process(Arc::new(exec_config))
        .await
        .map_err(|e| anyhow::anyhow!("ballista executor process: {e}"))
}

/// Start a ballista scheduler in the background and return its client URL
/// (`http://external_host:bind_port`).
///
/// The scheduler's session builder registers the cluster catalog so it can
/// resolve tables during physical planning, and the SQE codecs are installed
/// so it round-trips SQE scan nodes to executors.
pub async fn start_scheduler(
    config: &SqeConfig,
    external_host: &str,
    bind_host: &str,
    bind_port: u16,
) -> Result<String> {
    let cluster_catalog = build_cluster_catalog(config)
        .await
        .context("building scheduler cluster catalog")?;

    let provider = cluster_catalog.provider.clone();
    let catalog_name = cluster_catalog.catalog_name.clone();

    // Session builder: register the catalog on every planning session so the
    // scheduler can resolve tables when it physical-plans the submitted
    // logical plan.
    let session_builder: SessionBuilder = Arc::new(move |session_config: SessionConfig| {
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_catalog(catalog_name.clone(), provider.clone());
        // Register SQE's Trino-compat functions so the scheduler can plan
        // queries that use them (the coordinator registers the same set on
        // its planning context). NOTE: executors also need these to *run*
        // UDF-bearing physical tasks (override_function_registry) — follow-up.
        sqe_trino_functions::register_trino_functions(&ctx);
        sqe_trino_functions::register_extended_trino_functions(&ctx);
        Ok(ctx.state())
    });

    let mut scheduler_config = SchedulerConfig::default()
        .with_hostname(external_host)
        .with_port(bind_port);
    scheduler_config.bind_host = bind_host.to_string();
    scheduler_config.override_session_builder = Some(session_builder);
    // Register the SqeAuthOptions extension so the per-query bearer survives
    // the scheduler's config merge and is re-emitted into each task's props.
    scheduler_config.override_config_producer = Some(sqe_config_producer());
    scheduler_config.override_logical_codec = Some(cluster_catalog.logical_codec());
    scheduler_config.override_physical_codec = Some(cluster_catalog.physical_codec());

    let addr = format!("{bind_host}:{bind_port}")
        .parse()
        .with_context(|| format!("parsing scheduler bind address {bind_host}:{bind_port}"))?;

    let ballista_cluster = BallistaCluster::new_from_config(&scheduler_config)
        .await
        .map_err(|e| anyhow::anyhow!("building ballista cluster: {e}"))?;

    let scheduler_config = Arc::new(scheduler_config);
    tokio::spawn(async move {
        if let Err(e) = start_server(ballista_cluster, addr, scheduler_config).await {
            tracing::error!(error = %e, "ballista scheduler server exited with error");
        }
    });

    Ok(format!("http://{external_host}:{bind_port}"))
}

/// A started scheduler plus the client-side catalog/codecs to talk to it.
/// Initialized once per coordinator process.
pub struct BallistaRuntime {
    pub scheduler_url: String,
    pub cluster: ClusterCatalog,
}

static RUNTIME: tokio::sync::OnceCell<BallistaRuntime> = tokio::sync::OnceCell::const_new();

/// Get (or lazily start) the process-global ballista runtime: an embedded
/// scheduler plus the client catalog/codecs used to submit to it.
///
/// The scheduler endpoint advertised to executors is controlled by env:
/// `SQE_BALLISTA_SCHEDULER_HOST` (default `localhost`) and
/// `SQE_BALLISTA_SCHEDULER_PORT` (default `50050`); it always binds
/// `0.0.0.0`. In a container deployment set the host to the coordinator's
/// service name so executors can reach it.
pub async fn get_or_init_runtime(config: &SqeConfig) -> Result<&'static BallistaRuntime> {
    RUNTIME
        .get_or_try_init(|| async {
            let external_host = std::env::var("SQE_BALLISTA_SCHEDULER_HOST")
                .unwrap_or_else(|_| "localhost".to_string());
            let port: u16 = std::env::var("SQE_BALLISTA_SCHEDULER_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(50050);

            let scheduler_url = start_scheduler(config, &external_host, "0.0.0.0", port)
                .await
                .context("starting embedded ballista scheduler")?;
            let cluster = build_cluster_catalog(config)
                .await
                .context("building coordinator client catalog")?;
            tracing::info!(scheduler_url, "embedded ballista scheduler started");
            Ok(BallistaRuntime {
                scheduler_url,
                cluster,
            })
        })
        .await
}

/// Submit a policy-rewritten logical plan to a remote ballista scheduler and
/// open the result stream. Replaces the Phase 2 standalone-per-query path.
///
/// `cluster` supplies the codecs (which must match the scheduler/executors)
/// and the catalog to register on the client context.
pub async fn submit_remote(
    scheduler_url: &str,
    plan: LogicalPlan,
    cluster: &ClusterCatalog,
    target_partitions: usize,
    user_bearer: &str,
) -> Result<(SchemaRef, SendableRecordBatchStream)> {
    // The bearer is threaded through the plan: the logical codec stamps it onto
    // the encoded SqeTableProvider node; the scheduler decodes it and attaches
    // the bearer to the rehydrated provider; from there it flows to
    // IcebergScanExec and the physical EncodedSqeScan, which uses it to mint a
    // per-(user,table) FileIO. This is the primary bearer-passthrough path.
    //
    // The SqeAuthOptions session-config insert below is retained as harmless
    // redundancy. Ballista currently drops ConfigExtension keys during
    // `update_from_key_value_pair` (they are emitted unprefixed so the
    // receiving `set()` cannot route them back), so the insert is a no-op at
    // runtime. It is kept so the wiring is already in place should a future
    // ballista version round-trip ConfigExtension keys correctly.
    //
    // Security: the bearer is a live OIDC token; do not log it at trace level.
    let mut config = SessionConfig::new_with_ballista()
        .with_target_partitions(target_partitions)
        .with_ballista_logical_extension_codec(cluster.logical_codec_with_bearer(
            (!user_bearer.is_empty()).then(|| Arc::from(user_bearer)),
        ))
        .with_ballista_physical_extension_codec(cluster.physical_codec());

    config.options_mut().extensions.insert(SqeAuthOptions {
        bearer: user_bearer.to_string(),
    });

    let state: SessionState = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state(scheduler_url, state)
        .await
        .with_context(|| format!("connecting to ballista scheduler at {scheduler_url}"))?;

    ctx.register_catalog(cluster.catalog_name.clone(), cluster.provider.clone());

    let df = ctx
        .execute_logical_plan(plan)
        .await
        .context("submitting logical plan to ballista scheduler")?;

    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let stream = df
        .execute_stream()
        .await
        .context("opening ballista result stream")?;

    Ok((schema, stream))
}
