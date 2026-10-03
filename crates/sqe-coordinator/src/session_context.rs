use std::sync::{Arc, LazyLock, OnceLock};

use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use moka::future::Cache;
use sha2::{Digest, Sha256};
use tracing::debug;

use sqe_catalog::{SessionCatalog, SqeCatalogProvider, TableMetadataCache};
use sqe_core::{Session, SqeConfig, SqeError};
use sqe_policy::PolicyStore;

use crate::query_tracker::QueryTracker;
use crate::runtime_catalog::RuntimeCatalogRegistry;

/// Per-user SessionContext cache keyed by token fingerprint.
///
/// The cache holds `(SessionContext, Arc<SessionCatalog>)` pairs so that warm
/// queries skip the ~50 ms registration overhead (UDFs, TVFs, catalog setup).
/// Entries expire after `SESSION_CONTEXT_CACHE_TTL` and the cache holds at most
/// `SESSION_CONTEXT_CACHE_MAX_CAPACITY` entries to bound memory usage.
///
/// The TTL doubles as the *passive backstop* for catalog-set discovery: a newly
/// created/rebound Polaris catalog (e.g. a second `ws_<slug>` workspace) is only
/// seen once a session rebuild re-enumerates catalogs, and the #368
/// miss-triggered re-list only refreshes namespaces *within* an already
/// registered catalog, never the catalog set. The
/// `POST /api/v1/catalogs/refresh` admin endpoint is the instant path (the
/// platform calls it on catalog create/bind); this TTL bounds worst-case
/// staleness if that call never arrives. Kept short (60s) so passive recovery
/// is fast; the endpoint means the common case never waits on it.
///
/// DataFusion's `SessionContext` is `Clone` and wraps an `Arc<SessionState>`
/// internally, so a clone is O(1): only the Arc ref-count changes.
///
/// Issue #240: at the old cap of 100 the 101st concurrent user-token pair
/// evicted someone, and each rebuild re-pays catalog construction plus a
/// `list_namespaces`. Raised to 2000 so a few thousand concurrent users stop
/// LRU-thrashing; the 300s TTL still expires idle entries to bound memory.
const SESSION_CONTEXT_CACHE_MAX_CAPACITY: u64 = 2000;

/// Default TTL (seconds) for `SESSION_CONTEXT_CACHE` when the coordinator has
/// not configured one. Kept short so a new/rebound Polaris catalog is picked
/// up quickly by the passive backstop; the `POST /api/v1/catalogs/refresh`
/// endpoint is the instant path for the common case.
const DEFAULT_SESSION_CONTEXT_CACHE_TTL_SECS: u64 = 60;

/// Coordinator-configured TTL for `SESSION_CONTEXT_CACHE`, wired from
/// `coordinator.session_context_cache_ttl_secs` via
/// [`configure_session_cache_ttl`] at startup. Read once by the cache's
/// `LazyLock` initializer, so it must be set before the first cache access;
/// unset (worker path, tests) falls back to
/// `DEFAULT_SESSION_CONTEXT_CACHE_TTL_SECS`.
static SESSION_CONTEXT_CACHE_TTL_SECS: OnceLock<u64> = OnceLock::new();

/// Set the `SESSION_CONTEXT_CACHE` TTL from coordinator config. Idempotent-ish:
/// only the first call (which must precede the first cache access) wins; later
/// calls are ignored. Call once, early in coordinator startup.
pub fn configure_session_cache_ttl(secs: u64) {
    let _ = SESSION_CONTEXT_CACHE_TTL_SECS.set(secs);
}

/// A cached session behind a trait object: a compile firewall, keep it.
///
/// Stored concretely, the value type rides in every moka future
/// (`try_get_with`, `remove`, `run_pending_tasks`), and each `Send` future
/// awaiting one re-proved `Send`/`Sync` over DataFusion's `SessionContext`
/// graph (6.2 s of the library's compile, charged to `do_get_tables`,
/// `-Zself-profile` 2026-09-27). `dyn SessionValue` stops the walk at its
/// declared bounds. Same device as `StoredPlan` and sqe-policy's
/// `CachedPolicy`.
#[derive(Clone)]
struct CachedSession(Arc<dyn SessionValue>);

trait SessionValue: Send + Sync + 'static {
    fn session(&self) -> (SessionContext, Arc<SessionCatalog>);
}

impl SessionValue for (SessionContext, Arc<SessionCatalog>) {
    fn session(&self) -> (SessionContext, Arc<SessionCatalog>) {
        // O(1): SessionContext wraps an Arc<SessionState>.
        self.clone()
    }
}

static SESSION_CONTEXT_CACHE: LazyLock<Cache<String, CachedSession>> =
    LazyLock::new(|| {
        let ttl_secs =
            *SESSION_CONTEXT_CACHE_TTL_SECS.get_or_init(|| DEFAULT_SESSION_CONTEXT_CACHE_TTL_SECS);
        Cache::builder()
            .max_capacity(SESSION_CONTEXT_CACHE_MAX_CAPACITY)
            .time_to_live(std::time::Duration::from_secs(ttl_secs))
            .build()
    });

/// Build one [`SqeCatalogProvider`] + [`Arc<SessionCatalog>`] for a single
/// catalog entry.
///
/// Extracted from the `create_session_context` loop so that Task 4 (dynamic
/// Polaris catalog discovery) can reuse identical construction logic without
/// duplicating it. This is the only place that calls
/// `SessionCatalog::for_session_with` and `SqeCatalogProvider::try_new_with_options`.
///
/// Returns `(catalog_provider, session_catalog)` so the caller can register
/// the provider under `cat_name` and optionally promote it to the primary
/// `SessionCatalog`.
///
/// `degrade_unresolvable` controls what happens when the catalog's namespace
/// listing fails because the warehouse is absent/unresolvable. The static
/// `[catalogs.*]` loop passes `true` so a missing anchor (e.g. `main_warehouse`
/// not seeded) registers empty instead of aborting the whole session and
/// blocking unrelated catalogs (#501). Dynamic discovery passes `false` to keep
/// its Err -> None -> config-default fallback.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_catalog_provider(
    cat_cfg: &sqe_core::config::CatalogConfig,
    session: &Session,
    global_storage: &sqe_core::config::StorageConfig,
    prefetch_concurrency: usize,
    table_cache: Option<&TableMetadataCache>,
    prom_metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    degrade_unresolvable: bool,
) -> Result<(SqeCatalogProvider, Arc<SessionCatalog>), Arc<SqeError>> {
    let (session_catalog, storage) =
        build_session_catalog(cat_cfg, session, global_storage, table_cache).await?;

    let mut catalog_provider = SqeCatalogProvider::try_new_with_options(
        session_catalog.clone(),
        storage.clone(),
        cat_cfg.warehouse.clone(),
        cat_cfg.namespace_visibility_filter,
        degrade_unresolvable,
    )
    .await
    .map_err(Arc::new)?;
    if let Some(m) = prom_metrics {
        catalog_provider = catalog_provider.with_metrics(Arc::clone(m));
    }
    // The nine scan knobs (#132 clustering gate, #369 bloom probing, the
    // fetch pipeline depth, ...) travel as one struct from here to
    // `IcebergScanExec` (#575); `ScanTuning::from_config` is the only copy
    // out of the operator's config.
    catalog_provider = catalog_provider.with_tuning(
        sqe_catalog::iceberg_scan::ScanTuning::from_config(cat_cfg, prefetch_concurrency),
    );

    Ok((catalog_provider, session_catalog))
}

/// Resolve the per-catalog bearer + storage and build the iceberg
/// [`SessionCatalog`] for `cat_cfg`. Shared by [`build_catalog_provider`] (read
/// path) and [`discover_session_catalog`] (write path) so both resolve auth and
/// storage identically — keep them on one code path to avoid silent divergence.
async fn build_session_catalog(
    cat_cfg: &sqe_core::config::CatalogConfig,
    session: &Session,
    global_storage: &sqe_core::config::StorageConfig,
    table_cache: Option<&TableMetadataCache>,
) -> Result<(Arc<SessionCatalog>, sqe_core::config::StorageConfig), Arc<SqeError>> {
    let auth = cat_cfg.auth.clone().unwrap_or_default();
    let bearer = sqe_auth::per_catalog::resolve_bearer(&auth, session.access_token().expose())
        .await
        .map_err(Arc::new)?;
    let storage = cat_cfg
        .storage
        .clone()
        .unwrap_or_else(|| global_storage.clone());

    let session_catalog = Arc::new(
        SessionCatalog::for_session_with(cat_cfg, &storage, table_cache.cloned(), &bearer)
            .await
            .map_err(Arc::new)?,
    );

    Ok((session_catalog, storage))
}

/// Build a DataFusion [`SessionContext`] for the given session.
///
/// The context is wired up with:
/// - The user's Polaris catalog (via their bearer token)
/// - The `system` catalog for Trino JDBC metadata + runtime query tables
/// - The `sha256()` UDF for column masking
/// - Trino-compatible function aliases (`year()`, `month()`, ...)
/// - The `read_parquet()` table-valued function
///
/// When a shared `runtime` is provided (built once at coordinator startup via
/// [`crate::runtime::build_coordinator_runtime`]), it is used for all sessions
/// so the shared memory pool limit is enforced globally. When `None`, a
/// per-query runtime is created using the legacy `max_query_memory` setting.
///
/// Results are cached per token fingerprint (5-minute TTL, max 100 entries).
/// On a cache hit the `SessionContext` and `SessionCatalog` are cloned in O(1)
/// and returned immediately, skipping all registration work.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip(config, session, policy_store, query_tracker, runtime, prom_metrics, table_cache, runtime_catalogs, hive_manifest_cache, secrets), fields(username = %session.user.username))]
pub async fn create_session_context(
    config: &SqeConfig,
    session: &Session,
    // Gates the per-session Hive providers' table visibility (#716). The
    // Iceberg providers do not read it (#665).
    policy_store: Option<&Arc<dyn PolicyStore>>,
    query_tracker: &Arc<QueryTracker>,
    runtime: Option<&Arc<RuntimeEnv>>,
    prom_metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    table_cache: Option<&TableMetadataCache>,
    runtime_catalogs: &RuntimeCatalogRegistry,
    hive_manifest_cache: &Arc<sqe_catalog::hive::provider::HiveMetadataCache>,
    secrets: &sqe_core::SecretStore,
) -> sqe_core::Result<(SessionContext, Arc<SessionCatalog>)> {
    // --- Cache key: username + token fingerprint + session catalog/schema ---
    // Different tokens from the same user must not share a stale SessionCatalog.
    // We key by username + first 16 hex chars of the SHA-256 of the access token.
    // The session catalog/schema (X-Trino-Catalog / X-Trino-Schema) are part of
    // the key: the same user+token opening two connections against different
    // catalogs/schemas builds distinct DataFusion default-catalog contexts and
    // must not collide in the cache.
    let token_hash = hex::encode(Sha256::digest(session.access_token().expose_bytes()));
    let cache_key = format!(
        "{}:{}:{}:{}",
        session.user.username,
        &token_hash[..16],
        session.default_catalog.as_deref().unwrap_or(""),
        session.default_schema.as_deref().unwrap_or(""),
    );

    // --- Atomic cache lookup / build via try_get_with ---
    // Eliminates the TOCTOU race where two concurrent requests for the same key
    // both miss the cache and build redundant SessionContexts. moka coalesces
    // concurrent callers into a single init future.
    let username = session.user.username.clone();
    let attached_providers = runtime_catalogs
        .providers()
        .map_err(sqe_core::SqeError::Catalog)?;
    // Snapshot taken at the same moment as `attached_providers` above, for
    // the same reason: it reflects the registry state at the moment of this
    // cache miss, not whatever it grows to while the closure below awaits.
    let hive_roots = runtime_catalogs
        .hive_roots()
        .map_err(sqe_core::SqeError::Catalog)?;
    let result = SESSION_CONTEXT_CACHE
        .try_get_with(cache_key.clone(), async {
            debug!(
                username = %username,
                "SessionContext cache miss — building new context"
            );

            // Multi-catalog: build the named list from
            // `flattened_catalogs()` (legacy `[catalog]` block joins
            // under `iceberg` when no `[catalogs.*]` are set; otherwise
            // the named map drives, alphabetically sorted, with the
            // legacy block folded in only when `default_catalog` names
            // it explicitly). The "default" catalog DataFusion uses
            // for unqualified names is `resolve_default_catalog()`.
            let flattened: Vec<(String, sqe_core::config::CatalogConfig)> = config
                .flattened_catalogs()
                .into_iter()
                .map(|(n, c)| (n, c.clone()))
                .collect();
            // Do not implicitly promote a Hive root to the config default.
            // A session may name one (`USE <root>.<db>`, handled below), and
            // policy then qualifies its references from the session scope;
            // the config default stays Iceberg (see
            // `attach_configured_hive_roots`).
            let catalog_name = if flattened.is_empty() {
                config
                    .query
                    .default_catalog
                    .clone()
                    .filter(|name| !name.is_empty())
                    .unwrap_or_else(|| "datafusion".to_string())
            } else {
                config.resolve_default_catalog()
            };

            // --- Session catalog/schema resolution (Trino X-Trino-Catalog /
            //     X-Trino-Schema) ---
            // Honor the session catalog and schema for ALL table resolution,
            // not just 3-part identifiers: an unqualified `t` resolves to
            // `<session-catalog>.<session-schema>.t` and a 2-part `s.t` to
            // `<session-catalog>.s.t`. The session catalog may be a Polaris
            // warehouse that is not in static config (the same path 3-part
            // names trigger), so discover and register it here so DataFusion
            // can resolve unqualified/2-part names against it. Falls back to
            // the config default catalog when the session names none, or names
            // one that does not resolve for this principal -- so unqualified
            // queries never break with "unknown catalog".
            let config_catalog_names: std::collections::HashSet<&str> =
                flattened.iter().map(|(n, _)| n.as_str()).collect();
            let session_schema = session
                .default_schema
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("default")
                .to_string();
            let mut discovered_session_catalog: Option<(
                String,
                SqeCatalogProvider,
                Arc<SessionCatalog>,
            )> = None;
            let default_catalog = match session
                .default_catalog
                .as_deref()
                .filter(|c| !c.is_empty())
            {
                // Already a static-config catalog, an ATTACHed catalog or a
                // Hive root: registered below, so just point DataFusion's
                // default at it. Without the last two, `USE <hive-root>.db`
                // fell through to the config default and unqualified names
                // silently read the same-named Iceberg table instead.
                Some(c)
                    if config_catalog_names.contains(c)
                        || attached_providers.iter().any(|(n, _)| n == c)
                        || hive_roots.iter().any(|(n, _)| n == c) =>
                {
                    c.to_string()
                }
                // Non-config catalog: try Polaris discovery with the caller's
                // bearer. Register it only if Polaris resolves it for this
                // principal; otherwise keep the config default.
                Some(c) => {
                    match discover_catalog_provider(c, config, session, table_cache, prom_metrics)
                    .await
                    {
                        Some((provider, session_catalog)) => {
                            discovered_session_catalog =
                                Some((c.to_string(), provider, session_catalog));
                            c.to_string()
                        }
                        None => catalog_name.clone(),
                    }
                }
                None => catalog_name.clone(),
            };
            drop(config_catalog_names);

            let session_config = sqe_catalog::engine_session_config()
                .with_information_schema(true)
                .with_default_catalog_and_schema(&default_catalog, &session_schema);

            // Dynamic-filter membership pushdown sizing. DataFusion only
            // materializes the build-side key set as an IN-list (extractable,
            // pushable into Iceberg scans and worker tickets) below these
            // thresholds; above them it falls back to an opaque hash-table
            // probe that no scan can use. The 150-value default is far below
            // star-schema dimension filters (SSB carries 160-6500 keys), so
            // every such query shipped the full fact table. 0 keeps
            // DataFusion's default.
            let session_config = {
                let mut sc = session_config;
                if config.query.runtime_filter_inlist_max_values > 0 {
                    sc = sc.set_usize(
                        "datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values",
                        config.query.runtime_filter_inlist_max_values,
                    );
                }
                let max_size = sqe_core::parse_memory_limit(&config.query.runtime_filter_inlist_max_size)
                    .unwrap_or(0);
                if max_size > 0 {
                    sc = sc.set_usize(
                        "datafusion.optimizer.hash_join_inlist_pushdown_max_size",
                        max_size,
                    );
                }
                // Registry and NDV requests are separate experiments: the
                // registry changes how every join is costed, NDV requests
                // only add distinct counts to Iceberg scan statistics.
                if config.query.statistics_registry {
                    sc = sc.set_bool(
                        "datafusion.optimizer.use_statistics_registry",
                        true,
                    );
                }
                sc
            };

            let ctx = if let Some(rt) = runtime {
                // Use the shared coordinator runtime (memory pool per
                // `coordinator.memory_pool`, spill-to-disk per config)
                SessionContext::new_with_config_rt(session_config, Arc::clone(rt))
            } else {
                // Fallback path (tests, one-shot helpers): greedy pool with
                // spill disabled. Greedy matches the shared runtime default;
                // FairSpillPool's static pool/N split across registered
                // consumers starves wide plans (see runtime::build_memory_pool).
                let max_memory = sqe_core::parse_memory_limit(&config.query.max_query_memory)
                    .unwrap_or(256 * 1024 * 1024);
                if max_memory > 0 {
                    let pool = Arc::new(
                        datafusion::execution::memory_pool::TrackConsumersPool::new(
                            datafusion::execution::memory_pool::GreedyMemoryPool::new(max_memory),
                            std::num::NonZeroUsize::new(5).expect("non-zero const"),
                        ),
                    );
                    // V10 httpfs: lazy http(s) + s3 ObjectStoreRegistry mirrors
                    // the primary coordinator runtime so the fallback path
                    // (tests, one-shot helpers) also accepts URL-shaped file
                    // paths and ad-hoc `s3://` buckets in read_* TVFs.
                    let registry = Arc::new(
                        sqe_catalog::lazy_object_store::LazyHttpObjectStoreRegistry::with_s3_fallback(
                            datafusion::execution::object_store::DefaultObjectStoreRegistry::new(),
                            config.storage.clone(),
                        ),
                    );
                    let rt = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                        .with_memory_pool(pool)
                        .with_object_store_registry(registry)
                        // #363: mirror the shared runtime — no infinite-TTL
                        // list_files_cache, so directory reads of mutable
                        // external buckets list fresh.
                        .with_cache_manager(
                            sqe_catalog::lazy_object_store::external_store_cache_config(),
                        )
                        .build_arc()
                        .map_err(|e| {
                            Arc::new(SqeError::Config(format!("Failed to create runtime env: {e}")))
                        })?;
                    SessionContext::new_with_config_rt(session_config, rt)
                } else {
                    // Unlimited-memory fallback (max_query_memory = 0; test /
                    // one-shot only). Build an explicit runtime so this path
                    // also gets the #363 cache config — a bare
                    // `new_with_config` would use DataFusion's default runtime
                    // with the infinite-TTL list_files_cache re-enabled.
                    let rt = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
                        .with_cache_manager(
                            sqe_catalog::lazy_object_store::external_store_cache_config(),
                        )
                        .build_arc()
                        .map_err(|e| {
                            Arc::new(SqeError::Config(format!("Failed to create runtime env: {e}")))
                        })?;
                    SessionContext::new_with_config_rt(session_config, rt)
                }
            };

            // CAT-01: `enable_url_table()` is deliberately NOT called on the
            // coordinator.
            //
            // DataFusion's `enable_url_table()` wraps the catalog list in a
            // `DynamicFileCatalog` that resolves *any* quoted-string table name
            // (`SELECT * FROM '<string>'`) against `ListingTableFactory` ->
            // `ObjectStoreRegistry`. That parallel resolution path never touches
            // `TvfPolicy::check`, so on the privileged coordinator pod it let an
            // authenticated user read `SELECT * FROM '/etc/shadow'`,
            // `'file:///var/run/secrets/.../token'`, or pivot via
            // `SELECT * FROM 'https://internal-svc/...'` — bypassing the
            // fail-closed `[storage.tvf]` guard entirely (the local-file vector
            // resolves through the inner registry's pre-registered `file` store
            // before any http allowlist check could fire).
            //
            // The guarded `read_parquet` / `read_csv` / `read_json` TVFs are
            // registered independently via `register_udtf` below and each call
            // `config.storage.tvf.check_path(...)`, so removing `enable_url_table()`
            // costs the coordinator only the DuckDB-style bare-string shorthand,
            // not the safe file-reader functions. The embedded CLI keeps
            // `enable_url_table()` (single-tenant, `allow_local_paths = true`).
            let mut ctx = ctx;

            // #366: two-phase rewrite for COUNT(DISTINCT x) with count()
            // companions. DataFusion's built-in single-distinct rule rejects
            // the shape when COUNT(*) / COUNT(col) rides along, dropping the
            // whole aggregation onto per-group HashSet distinct state (8-12GB
            // and a degenerate spill for bank q03 at SF10). The rule is
            // complementary: it only fires on plans the built-in rule skips.
            ctx.add_optimizer_rule(Arc::new(
                sqe_planner::SingleDistinctCountCompanionRule::new(),
            ));
            if config.query.puffin_statistics {
                ctx.add_optimizer_rule(Arc::new(
                    sqe_planner::PuffinStatisticsRequestRule::new(),
                ));
            }

            // Register DataFusion's built-in in-memory catalog so DML helpers can register
            // temporary MemTables under `datafusion.public.<name>` without hitting the
            // Iceberg catalog which does not support dynamic table registration.
            let df_catalog = Arc::new(MemoryCatalogProvider::new());
            let df_schema = Arc::new(MemorySchemaProvider::new());
            df_catalog
                .register_schema("public", df_schema)
                .expect("MemoryCatalogProvider always accepts schema registration");
            ctx.register_catalog("datafusion", df_catalog);

            // Build one SessionCatalog + SqeCatalogProvider per
            // entry from `flattened`. Each catalog gets its own
            // per-session connection. Two per-catalog overrides
            // resolved here (V7):
            //
            //   * `cat_cfg.auth` — when present, replaces the
            //     session bearer for this catalog only. Variants:
            //     SessionBearer (default), Static, Anonymous,
            //     ClientCredentials, Aws. Federated deployments
            //     where one catalog speaks Polaris (session token)
            //     and another speaks a partner Iceberg REST endpoint
            //     behind its own OAuth client now configure both in
            //     one TOML.
            //
            //   * `cat_cfg.storage` — when present, overrides the
            //     coordinator-wide `[storage]` block for this
            //     catalog. The override flows into `for_session_with`
            //     and into `SqeCatalogProvider`, so scan / write
            //     paths for this catalog hit the right S3 endpoint
            //     and region. Iceberg credential vending from REST
            //     catalogs still wins per-table over both.
            //
            // 3-part SQL identifiers work without session-state setup
            // because every catalog registers under its declared SQL
            // name. Cross-catalog joins like `polaris.sales.orders
            // LEFT JOIN nessie.archive.orders` flow through the
            // standard DataFusion path.
            //
            // The first entry in `flattened` is treated as the
            // "primary" — its `SessionCatalog` is what
            // `system.runtime.*` introspection and the legacy
            // `session_catalog_for_return` path use. Operators
            // running mixed Polaris+Glue (or whatever) deployments
            // pick the primary by name via `query.default_catalog`.
            let global_storage = config.storage.clone();
            let mut primary_session_catalog: Option<Arc<SessionCatalog>> = None;
            // Every reachable catalog paired with its SessionCatalog, so the
            // system.jdbc.* / system.metadata.* providers enumerate all of them
            // (config catalogs + the session's own discovered catalog), not just
            // the default. (#5)
            let mut system_catalog_entries: Vec<sqe_catalog::SystemCatalogEntry> = Vec::new();

            for (cat_name, cat_cfg) in &flattened {
                let (catalog_provider, session_catalog) = build_catalog_provider(
                    cat_cfg,
                    session,
                    &global_storage,
                    config.storage.prefetch_concurrency,
                    table_cache,
                    prom_metrics,
                    // Static anchor: never let an unresolvable warehouse abort
                    // the whole session build and take down unrelated catalogs
                    // (#501). It registers empty and self-heals on re-list.
                    true,
                )
                .await?;
                ctx.register_catalog(cat_name, Arc::new(catalog_provider));
                system_catalog_entries.push(sqe_catalog::SystemCatalogEntry {
                    name: cat_name.clone(),
                    catalog: session_catalog.clone(),
                });

                if primary_session_catalog.is_none() {
                    primary_session_catalog = Some(session_catalog);
                }
            }

            // Register the session's discovered Polaris catalog (from
            // X-Trino-Catalog) when it is not a static-config catalog, so
            // 2-part and bare table names resolve against it -- and add it to the
            // system metadata enumeration so JDBC clients see it. Skipped when
            // the session names no catalog, names a config catalog (already
            // registered above), or Polaris did not resolve it (default fell
            // back to the config catalog).
            if let Some((cat_name, provider, session_catalog)) = discovered_session_catalog {
                ctx.register_catalog(cat_name.clone(), Arc::new(provider));
                system_catalog_entries.push(sqe_catalog::SystemCatalogEntry {
                    name: cat_name,
                    catalog: session_catalog,
                });
            }

            // Hold onto the primary for downstream consumers that
            // expect a single SessionCatalog (system.runtime.*,
            // SessionCatalog return value). Multi-catalog access
            // for queries flows through DataFusion's CatalogProvider
            // registration above; this is purely the legacy
            // bookkeeping handle.
            let session_catalog = primary_session_catalog.unwrap_or_else(|| {
                Arc::new(SessionCatalog::unconfigured(
                    &global_storage,
                    table_cache.cloned(),
                ))
            });
            let session_catalog_for_return = session_catalog.clone();

            // The SessionCatalog bound to the session's *effective* default
            // catalog (the one a normal `SELECT ns.t` resolves against -- a
            // discovered Polaris warehouse from X-Trino-Catalog, or a named
            // config catalog). The iceberg metadata TVFs (table_snapshots,
            // $history, $partitions, ...) use this so `"ns.t$snapshots"` loads
            // the table from the caller's catalog instead of the primary/config
            // catalog (which failed with "table_snapshots: failed to load" when
            // the session catalog differed). Falls back to the primary when the
            // name isn't among the built catalogs. (#4)
            let effective_session_catalog = system_catalog_entries
                .iter()
                .find(|e| e.name == default_catalog)
                .map(|e| Arc::clone(&e.catalog))
                .unwrap_or_else(|| session_catalog_for_return.clone());

            // Register the system catalog for Trino JDBC metadata browsing
            // (system.jdbc.types, system.jdbc.catalogs, system.jdbc.schemas, etc.)
            // and the system.runtime.* virtual tables for query/node/task info.
            let tracker = Arc::clone(query_tracker);
            let records_fn: crate::system_runtime::QueryRecordsFn =
                Arc::new(move || tracker.records());
            let coordinator_uri = format!(
                "http://localhost:{}",
                config.coordinator.flight_sql_port
            );
            let runtime_schema = Arc::new(crate::system_runtime::RuntimeSchemaProvider::new(
                records_fn,
                config.catalog.warehouse.clone(),
                coordinator_uri,
                config.coordinator.worker_urls.clone(),
            ));
            // Enumerate every reachable catalog (config catalogs + the session's
            // own discovered catalog) so `system.jdbc.*` / `system.metadata.*`
            // see all of them, not just the default. (#5)
            let system_catalog =
                sqe_catalog::SystemCatalogProvider::new(system_catalog_entries)
                    .with_runtime(runtime_schema);
            ctx.register_catalog("system", Arc::new(system_catalog));

            // Register any catalogs attached at runtime via ATTACH.
            // The providers snapshot was taken before the cache future was
            // entered, so it reflects the registry state at the moment of
            // the cache miss.
            for (name, provider) in &attached_providers {
                ctx.register_catalog(name.clone(), Arc::clone(provider));
            }

            // Register every Hive-style external-table warehouse root
            // (`[[hive.roots]]` at startup, or `ATTACH ... TYPE
            // hive_external` since) with a freshly built, SESSION-SCOPED
            // `HiveCatalogProvider`. Unlike the Iceberg providers just
            // above -- built once and cloned unchanged into every session
            // -- a Hive provider cannot be reused across sessions. NOT
            // because of `ctx.runtime_env()`: this path shares one
            // `RuntimeEnv`/`ObjectStoreRegistry` across every session
            // (`Arc::clone(rt)` a few hundred lines up), so a cached
            // provider would not be looking anything up in a stale
            // runtime. The real reason is `hive_caller` below: it is built
            // fresh per session from `session.user`, and
            // `TvfPolicy::check_path` gates each table load on it,
            // including the `{user}`-substituted `allowed_object_store_prefixes`
            // check. A provider built once and reused would carry the
            // FIRST session's caller into every later session's table
            // loads, silently applying one user's object-store allowlist
            // to another user's query -- see `mount.rs`'s module doc
            // comment above `build_hive_root` for the full case. Nothing
            // here may be cached across sessions except `hive_roots` (the
            // shared, session-independent `HiveManifestStore` per root --
            // session-independent because it is built once at `ATTACH`
            // time, NOT because it carries no caller: it does, baked in
            // from whichever session ran the `ATTACH` (or
            // `trusted_startup_config()` for a `[[hive.roots]]` entry), and
            // every later session's manifest reads are gated against that
            // one caller, not their own -- see `runtime_catalog.rs`'s "Hive
            // lane" comment for the full case) and `hive_manifest_cache`
            // (the one process-wide manifest cache every session's provider
            // reads through).
            //
            // `cache_ttl` (`[hive] metadata_ttl_secs`) is how long the
            // database and table-name snapshots are served before the next
            // pre-plan refresh (`QueryHandler::refresh_listing_snapshots`,
            // review 806) re-lists them. The snapshots live in
            // `hive_manifest_cache` per catalog name, not in the provider:
            // an MCP tool call is a session of its own, and a per-provider
            // snapshot meant one `GetTables` per database per call. The
            // synchronous `schema_names()` / `table_names()` hooks only ever
            // serve those snapshots. `HiveDdlHandler` calls
            // `invalidate_table_names` after `CREATE EXTERNAL TABLE` or
            // `DROP TABLE`, which every session sees because the snapshots
            // are shared; another process's DDL is visible after the TTL.
            for (name, root) in &hive_roots {
                let hive_caller = sqe_core::config::TvfCaller::for_user(
                    session.user.username.clone(),
                    session.user.roles.clone(),
                );
                let hive_provider = sqe_catalog::mount::session_hive_catalog_from_source(
                    name,
                    root.source(),
                    &global_storage,
                    secrets,
                    hive_caller,
                    ctx.runtime_env(),
                    Arc::clone(hive_manifest_cache),
                    std::time::Duration::from_secs(config.hive.metadata_ttl_secs),
                );
                // #716: DataFusion's information_schema lists Hive tables
                // through the process-wide, principal-agnostic listings.
                // Gate them per session with the SAME resolution
                // `HiveDdlHandler::list_tables` uses for SHOW TABLES, keyed
                // `{root}.{database}` (#715). No policy store = passthrough.
                if let Some(store) = policy_store {
                    let store = Arc::clone(store);
                    let user = session.user.clone();
                    let root = name.clone();
                    let precedence = config.policy.mask_precedence;
                    hive_provider.set_visibility(Arc::new(move |database, table| {
                        let (store, user, root) = (Arc::clone(&store), user.clone(), root.clone());
                        Box::pin(async move {
                            // ponytail: no manifest column tags here; tags
                            // yield masks, never the lit(false) deny-all.
                            let policy = sqe_policy::plan_rewriter::resolve_effective_policy(
                                store.as_ref(),
                                &user,
                                &table,
                                &format!("{root}.{database}"),
                                &std::collections::HashMap::new(),
                                precedence,
                            )
                            .await;
                            !sqe_policy::plan_rewriter::policy_denies_all(&policy)
                        })
                    }));
                }
                ctx.register_catalog(name.clone(), hive_provider);
            }

            // Register the sha256() scalar function for column masking.
            // DataFusion does not ship a built-in sha256, we provide one via sqe-policy.
            // When `coordinator.policy.mask_key` is set the UDF runs as
            // HMAC-SHA256 with that key, blocking offline rainbow-table
            // attacks against low-entropy masked columns (issue #37).
            let mask_key = if config.policy.mask_key.is_empty() {
                None
            } else {
                Some(std::sync::Arc::new(config.policy.mask_key.as_bytes().to_vec()))
            };
            ctx.register_udf(sqe_policy::sha256_udf::sha256_udf(mask_key));

            // Register session-context UDFs: current_user(), is_role_in_session(),
            // current_available_roles(), current_database(), current_schema().
            //
            // Each UDF bakes in the authenticated user's identity at construction
            // time (Volatility::Immutable), so DataFusion const-folds them during
            // logical optimization on the coordinator -- they never travel to workers
            // as live function invocations.
            //
            // database: wired to config.catalog.warehouse (the Polaris warehouse name).
            // schema: None -- no per-session default namespace is set at this point (MVP).
            // current_catalog() / current_database() return the session's
            // effective catalog (X-Trino-Catalog, else the config default) and
            // current_schema() returns the session schema (X-Trino-Schema, else
            // "default"), so Trino clients see the catalog/schema they set. (#1)
            let session_identity = std::sync::Arc::new(sqe_policy::session_udf::SessionIdentity {
                username: session.user.username.clone(),
                roles: session.user.roles.clone(),
                database: Some(default_catalog.clone()),
                schema: Some(session_schema.clone()),
            });
            for udf in sqe_policy::session_udf::session_udfs(session_identity) {
                ctx.register_udf(udf);
            }

            // Register Trino-compatible function aliases (year(), month(), day_of_week(), etc.)
            // so Trino SQL and dbt models work without modification.
            sqe_trino_functions::register_trino_functions(&ctx);

            // Register extended Trino-compatible functions (soundex, regexp_extract, word_stem, etc.)
            sqe_trino_functions::register_extended_trino_functions(&ctx);

            // Register JSON functions from datafusion-functions-json crate.
            // Provides: json_get, json_get_str, json_get_int, json_get_float, json_get_bool,
            //           json_get_json, json_get_array, json_contains, json_as_text, json_length
            datafusion_functions_json::register_all(&mut ctx)
                .map_err(|e| Arc::new(SqeError::Config(format!("Failed to register JSON functions: {e}"))))?;

            // Register the read_parquet() table-valued function so users can
            // query external Parquet files directly from SQL:
            //   SELECT * FROM read_parquet('s3://bucket/path/*.parquet', ...)
            //
            // E2E-identity item 1: the TVFs are bound to the session's
            // authenticated principal. Object-store paths (`s3://`, ...)
            // read with the ENGINE's static storage key are denied unless
            // they fall under `[storage.tvf] allowed_object_store_prefixes`
            // (with `{user}` substitution) or the caller holds a role in
            // `object_store_admin_roles`. Calls carrying complete inline
            // credentials bypass the gate (the engine key is not used).
            // Scoped to the session's resolved catalog, so that catalog's
            // `tvf_prefixes` apply and every other catalog's do not.
            let tvf_caller = tvf_caller_for(session, &flattened, &default_catalog);
            // Registered through `FileTableFunctions::register`, not
            // `ctx.register_udtf`: the file readers build their provider on
            // the async path (`prepare_statement`, called by every planner
            // entry point before `ctx.sql`) and their synchronous `call`
            // only looks it up (architecture review 806). The registry it
            // keeps in the session config is how `prepare_statement` finds
            // them.
            use sqe_catalog::file_tvf_prepare::FileTableFunctions;
            FileTableFunctions::register(
                &ctx,
                "read_parquet",
                Arc::new(
                    sqe_catalog::read_parquet::ReadParquetFunction::with_caller(
                        config.storage.clone(),
                        tvf_caller.clone(),
                    )
                    .with_runtime_env(ctx.runtime_env()),
                ),
            );

            // V8: read_csv() and read_json() TVFs alongside read_parquet().
            // Same calling convention (positional path + named kw args);
            // CSV-specific args: delimiter, has_header, quote, escape,
            // comment, null_regex, file_extension. JSON-specific args:
            // newline_delimited, file_extension. S3 credentials + endpoint
            // overrides flow through the shared file_tvf_common helpers.
            FileTableFunctions::register(
                &ctx,
                "read_csv",
                Arc::new(
                    sqe_catalog::read_csv::ReadCsvFunction::with_caller(
                        config.storage.clone(),
                        tvf_caller.clone(),
                    )
                    .with_runtime_env(ctx.runtime_env()),
                ),
            );
            FileTableFunctions::register(
                &ctx,
                "read_json",
                Arc::new(
                    sqe_catalog::read_json::ReadJsonFunction::with_caller(
                        config.storage.clone(),
                        tvf_caller,
                    )
                    .with_runtime_env(ctx.runtime_env()),
                ),
            );

            // Register Iceberg metadata TVFs:
            //   SELECT * FROM table_snapshots('schema', 'table')
            //   SELECT * FROM table_manifests('schema', 'table')
            //   SELECT * FROM table_history('schema', 'table')
            //   SELECT * FROM table_files('schema', 'table')
            //   SELECT * FROM table_partitions('schema', 'table')
            //   SELECT * FROM table_refs('schema', 'table')
            ctx.register_udtf(
                "table_snapshots",
                Arc::new(sqe_catalog::iceberg_metadata_tvf::TableSnapshotsFunction::new(
                    Arc::clone(&effective_session_catalog),
                )),
            );
            ctx.register_udtf(
                "table_manifests",
                Arc::new(sqe_catalog::iceberg_metadata_tvf::TableManifestsFunction::new(
                    Arc::clone(&effective_session_catalog),
                )),
            );
            ctx.register_udtf(
                "table_history",
                Arc::new(sqe_catalog::iceberg_metadata_tvf::TableHistoryFunction::new(
                    Arc::clone(&effective_session_catalog),
                )),
            );
            ctx.register_udtf(
                "table_files",
                Arc::new(sqe_catalog::iceberg_metadata_tvf::TableFilesFunction::new(
                    Arc::clone(&effective_session_catalog),
                )),
            );
            ctx.register_udtf(
                "table_partitions",
                Arc::new(sqe_catalog::iceberg_metadata_tvf::TablePartitionsFunction::new(
                    Arc::clone(&effective_session_catalog),
                )),
            );
            ctx.register_udtf(
                "table_refs",
                Arc::new(sqe_catalog::iceberg_metadata_tvf::TableRefsFunction::new(
                    Arc::clone(&effective_session_catalog),
                )),
            );

            // Symmetric to DuckDB's quack_query(): pull rows from a remote
            // Quack endpoint inline.
            //   SELECT * FROM quack_query('quack:host:9495', 'SELECT 42');
            // Outbound HTTP from the coordinator is an SSRF primitive
            // (finding 604), so the function exists only when
            // `[quack_client] enabled = true` AND the session holds one of
            // `allowed_roles`; destinations are limited to `allowed_hosts`,
            // the session's own bearer is forwarded (no token literal in
            // SQL), and the decoded result is capped by `max_result_bytes`.
            if config.quack_client.permits(&session.user.roles) {
                let policy = sqe_quack_client::QuackTargetPolicy::new(
                    config.quack_client.allowed_hosts.clone(),
                    config.quack_client.max_result_bytes,
                );
                let token = session.access_token().expose().to_string();
                let token = (!token.is_empty()).then_some(token);
                ctx.register_udtf(
                    "quack_query",
                    Arc::new(sqe_quack_client::QuackQueryTvf::with_policy(policy, token)),
                );
            }

            debug!(
                catalog_name = %catalog_name,
                "Registered session catalog in DataFusion context"
            );

            Ok(CachedSession(Arc::new((ctx, session_catalog_for_return))))
                as Result<_, Arc<SqeError>>
        })
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to build session context: {e}")))?;

    Ok(result.0.session())
}

/// Pick the `CatalogConfig` to clone as the template for a discovered
/// warehouse: the configured default catalog (`query.default_catalog`) if it
/// names a REST catalog, else the first flattened REST catalog. Returns
/// `None` when no REST catalog is configured (discovery only targets Polaris/
/// REST). Only `warehouse` differs on the clone; `catalog_url` + `auth` +
/// backend are inherited.
pub(crate) fn discovery_template(config: &SqeConfig) -> Option<sqe_core::config::CatalogConfig> {
    let flattened = config.flattened_catalogs();
    let pick = config
        .query
        .default_catalog
        .as_deref()
        .and_then(|name| flattened.iter().find(|(n, _)| n == name))
        .or_else(|| {
            flattened
                .iter()
                .find(|(_, c)| matches!(c.backend, sqe_core::config::CatalogBackend::Rest))
        })
        .or_else(|| flattened.first());
    pick.map(|(_, c)| (*c).clone())
        .filter(|c| matches!(c.backend, sqe_core::config::CatalogBackend::Rest))
        // Finding 752: the clone carries the template's `auth` onto a
        // warehouse the caller named. Only per-user auth may travel; a
        // Static / ClientCredentials / Aws template would hand the caller
        // the engine's identity on any warehouse they can spell.
        .filter(|c| {
            let per_user = matches!(
                c.auth,
                None | Some(sqe_core::config::CatalogAuthConfig::SessionBearer)
            );
            if !per_user {
                debug!(
                    catalog_url = %c.catalog_url,
                    "catalog discovery disabled: template catalog auth is not per-user"
                );
            }
            per_user
        })
        // The template's TVF prefixes belong to the template catalog, not to
        // a warehouse the caller named.
        .map(|mut c| {
            c.tvf_prefixes.clear();
            c
        })
}

/// The file-TVF caller for `session`, scoped to `default_catalog` (the
/// catalog the session resolved to). That catalog's `[catalogs.<name>]
/// tvf_prefixes` join the global list; a catalog not in config (a
/// discovered warehouse) adds none.
fn tvf_caller_for(
    session: &Session,
    flattened: &[(String, sqe_core::config::CatalogConfig)],
    default_catalog: &str,
) -> sqe_core::config::TvfCaller {
    let prefixes = flattened
        .iter()
        .find(|(name, _)| name == default_catalog)
        .map(|(_, c)| c.tvf_prefixes.as_slice())
        .unwrap_or_default();
    sqe_core::config::TvfCaller::for_user(
        session.user.username.clone(),
        session.user.roles.clone(),
    )
    .with_catalog_scope(default_catalog, prefixes)
}

/// Attempt to resolve `warehouse` as a Polaris catalog and build its
/// `SqeCatalogProvider` using the discovery template + the caller's bearer.
/// Returns `Ok(None)` (not an error) when discovery is off, no template
/// exists, or Polaris rejects the warehouse (unauthorized / nonexistent) --
/// the caller turns `None` into the existing "unknown catalog" error so an
/// unauthorized warehouse is indistinguishable from a missing one.
pub(crate) async fn discover_catalog_provider(
    warehouse: &str,
    config: &SqeConfig,
    session: &Session,
    table_cache: Option<&TableMetadataCache>,
    prom_metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
) -> Option<(SqeCatalogProvider, Arc<SessionCatalog>)> {
    if config.query.catalog_discovery != sqe_core::config::CatalogDiscovery::PolarisAuto {
        return None;
    }
    let mut cfg = discovery_template(config)?;
    cfg.warehouse = warehouse.to_string();

    match build_catalog_provider(
        &cfg,
        session,
        &config.storage,
        config.storage.prefetch_concurrency,
        table_cache,
        prom_metrics,
        // Discovery keeps its Err -> None -> config-default fallback: an
        // unresolvable discovered warehouse must not register as an empty
        // catalog and hijack the session default.
        false,
    )
    .await
    {
        Ok((provider, session_catalog)) => Some((provider, session_catalog)),
        Err(e) => {
            tracing::info!(
                warehouse,
                error = %e,
                "catalog discovery: Polaris did not resolve warehouse (treated as unknown catalog)"
            );
            None
        }
    }
}

/// Write-path counterpart of [`discover_catalog_provider`]: resolve `warehouse`
/// as a Polaris catalog and return its [`SessionCatalog`] (the iceberg bridge
/// used for `load_table` + commit) rather than a `SqeCatalogProvider`. Skips
/// building the provider so no extra namespace enumeration happens on the write
/// path. Returns `None` (not an error) when discovery is off, no REST template
/// exists, or Polaris rejects the warehouse (unauthorized / nonexistent) — same
/// semantics as [`discover_catalog_provider`], so the caller turns `None` into
/// the existing "unknown catalog" error.
/// `SessionCatalog` for a catalog DECLARED in configuration, built from its own
/// `CatalogConfig`.
///
/// Matched by config key or by `warehouse`, because a legacy single-catalog
/// deployment is keyed `iceberg` while operators name it by its warehouse.
///
/// Distinct from `discover_session_catalog` on purpose. Discovery clones a
/// template catalog's config and overrides only `warehouse`, which is right for a
/// warehouse that exists in Polaris but not in SQE's config, and wrong for one
/// that IS configured: it would silently read that catalog with another's
/// `polaris_url`, auth, backend and cache TTL. Configuration wins where it exists.
pub(crate) async fn configured_session_catalog(
    name: &str,
    config: &SqeConfig,
    session: &Session,
    table_cache: Option<&TableMetadataCache>,
) -> Option<Arc<SessionCatalog>> {
    let flattened = config.flattened_catalogs();
    let (_, cfg) = flattened
        .iter()
        .find(|(n, _)| n == name)
        .or_else(|| flattened.iter().find(|(_, c)| c.warehouse == name))?;
    let cfg = (*cfg).clone();
    match build_session_catalog(&cfg, session, &config.storage, table_cache).await {
        Ok((session_catalog, _storage)) => Some(session_catalog),
        Err(e) => {
            tracing::info!(
                catalog = name,
                error = %e,
                "configured catalog did not build for this session"
            );
            None
        }
    }
}

/// The coordinator's implementation of the write path's
/// [`CatalogDiscovery`](crate::write_handler::CatalogDiscovery) hooks: Polaris
/// discovery through this module's per-catalog bearer resolution, and
/// invalidation of the moka session-context cache after a commit.
#[derive(Debug, Default, Clone, Copy)]
pub struct PolarisCatalogDiscovery;

#[async_trait::async_trait]
impl crate::write_handler::CatalogDiscovery for PolarisCatalogDiscovery {
    async fn discover_session_catalog(
        &self,
        warehouse: &str,
        config: &SqeConfig,
        session: &Session,
        table_cache: Option<&TableMetadataCache>,
    ) -> Option<Arc<SessionCatalog>> {
        discover_session_catalog(warehouse, config, session, table_cache).await
    }

    async fn invalidate_session_cache(&self, username: &str) {
        invalidate_session_cache(username).await
    }
}

pub(crate) async fn discover_session_catalog(
    warehouse: &str,
    config: &SqeConfig,
    session: &Session,
    table_cache: Option<&TableMetadataCache>,
) -> Option<Arc<SessionCatalog>> {
    if config.query.catalog_discovery != sqe_core::config::CatalogDiscovery::PolarisAuto {
        return None;
    }
    let mut cfg = discovery_template(config)?;
    cfg.warehouse = warehouse.to_string();

    match build_session_catalog(&cfg, session, &config.storage, table_cache).await {
        Ok((session_catalog, _storage)) => Some(session_catalog),
        Err(e) => {
            tracing::info!(
                warehouse,
                error = %e,
                "write-target catalog discovery: Polaris did not resolve warehouse (treated as unknown catalog)"
            );
            None
        }
    }
}

/// Invalidate the cached SessionContext for a specific user.
///
/// Because cache keys are now `username:token_hash`, we iterate and remove all
/// entries whose key starts with the given username prefix.
///
/// Must be called after DDL/DML operations (CTAS, DROP TABLE, INSERT, etc.)
/// that modify the catalog so subsequent queries see the new schema state.
/// Also called after CALL system.<maintenance procedure>(...) so the
/// cached DataFusion TVF MemTables (table_files, table_snapshots, etc.)
/// rebuild from the post-rewrite Polaris snapshot.
///
/// Boxed for compile time: the moka iteration and removal futures carry the
/// cache's value type (a DataFusion `SessionContext` and friends), and every
/// `Send` future that awaited this re-proved `Send` over that whole type
/// graph (3.7 s per call site, `-Zself-profile` 2026-09-07).
pub fn invalidate_session_cache(username: &str) -> futures::future::BoxFuture<'_, ()> {
    Box::pin(invalidate_session_cache_inner(username))
}

async fn invalidate_session_cache_inner(username: &str) {
    let prefix = format!("{username}:");
    let keys_to_remove: Vec<String> = SESSION_CONTEXT_CACHE
        .iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .map(|(k, _)| k.to_string())
        .collect();
    for key in &keys_to_remove {
        SESSION_CONTEXT_CACHE.remove(key.as_str()).await;
    }
    // Moka's remove is queued as a pending task; flush so the cache sees
    // the removal on the very next try_get_with in the same tokio tick.
    // Without this, a CALL system.<proc> followed by SELECT in the same
    // session within one test tick still hits the stale cached entry.
    SESSION_CONTEXT_CACHE.run_pending_tasks().await;
    debug!(
        username = %username,
        count = keys_to_remove.len(),
        "SessionContext cache invalidated for user after schema change"
    );
}

/// Invalidate all cached SessionContexts.
///
/// Used when a DDL/DML operation modifies the catalog, ensuring all users
/// (not just the current user) see the updated schema state.
///
/// The flush after `invalidate_all` is required for the same reason the
/// single-user version flushes: moka's eviction is queued as a pending task
/// and the very next `try_get_with` in the same tokio tick would otherwise
/// still hit the stale entry. Without this flush, `ATTACH`, `DETACH`,
/// `RENAME TABLE`, `CREATE SCHEMA`, and `DROP SCHEMA` would have a race window
/// where the issuing client's next query runs against the pre-mutation
/// SessionContext. Issue #25.
pub async fn invalidate_all_session_caches() {
    SESSION_CONTEXT_CACHE.invalidate_all();
    SESSION_CONTEXT_CACHE.run_pending_tasks().await;
    debug!("All SessionContext caches invalidated");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config_rest() -> SqeConfig {
        let toml_text = r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"

[catalog]
catalog_url = "https://polaris.example.com"
warehouse = "original_wh"

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true
"#;
        toml::from_str::<SqeConfig>(toml_text).expect("test config parses")
    }

    #[test]
    fn discovery_template_returns_rest_catalog_and_warehouse_override_works() {
        let config = test_config_rest();

        let tmpl = discovery_template(&config);
        assert!(tmpl.is_some(), "expected Some for a REST catalog config");

        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.catalog_url, "https://polaris.example.com");
        assert_eq!(tmpl.warehouse, "original_wh");

        // Callers override only `warehouse`; everything else is inherited.
        let mut cloned = tmpl.clone();
        cloned.warehouse = "discovered_wh".to_string();
        assert_eq!(cloned.warehouse, "discovered_wh");
        assert_eq!(cloned.catalog_url, "https://polaris.example.com");
        assert!(matches!(
            cloned.backend,
            sqe_core::config::CatalogBackend::Rest
        ));
    }

    /// Finding 752: a template with engine-held auth must not be cloned onto
    /// caller-named warehouses; discovery fails closed instead.
    #[test]
    fn discovery_template_refuses_non_per_user_auth() {
        let mut config = test_config_rest();
        assert!(discovery_template(&config).is_some());
        config.catalog.auth = Some(sqe_core::config::CatalogAuthConfig::SessionBearer);
        assert!(discovery_template(&config).is_some());
        config.catalog.auth = Some(sqe_core::config::CatalogAuthConfig::Static {
            token: "engine-token".to_string(),
        });
        assert!(discovery_template(&config).is_none());
    }

    fn two_workspace_config() -> SqeConfig {
        let toml_text = r#"
[coordinator]
flight_sql_port = 0
trino_http_port = 0

[auth]
token_endpoint = "http://127.0.0.1:9/unused"
client_id = "test_client"

[catalog]

[catalogs.ws_a]
catalog_url = "https://polaris.example.com"
warehouse = "ws_a"
tvf_prefixes = ["s3://ws-a-landing/"]

[catalogs.ws_b]
catalog_url = "https://polaris.example.com"
warehouse = "ws_b"
tvf_prefixes = ["s3://ws-b-landing/"]

[storage]
s3_endpoint = "http://127.0.0.1:9"
s3_access_key = "_"
s3_secret_key = "_"
s3_region = "us-east-1"
s3_path_style = true

[storage.tvf]
allowed_object_store_prefixes = ["s3://shared-landing/"]
"#;
        toml::from_str::<SqeConfig>(toml_text).expect("test config parses")
    }

    fn alice() -> Session {
        Session::new(
            "alice".to_string(),
            sqe_core::SecretString::new("token".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        )
    }

    /// The caller is scoped to the session's resolved catalog: catalog A's
    /// `tvf_prefixes` hold in A, not in B; the global list holds in both; a
    /// catalog not in config (a discovered warehouse) adds nothing.
    #[test]
    fn tvf_caller_carries_only_the_session_catalogs_prefixes() {
        let config = two_workspace_config();
        let flattened: Vec<(String, sqe_core::config::CatalogConfig)> = config
            .flattened_catalogs()
            .into_iter()
            .map(|(n, c)| (n, c.clone()))
            .collect();
        let policy = &config.storage.tvf;
        let a_file = "s3://ws-a-landing/2026/x.parquet";

        let in_a = tvf_caller_for(&alice(), &flattened, "ws_a");
        policy.check_path(a_file, &in_a, false).expect("A's prefix in A");

        let in_b = tvf_caller_for(&alice(), &flattened, "ws_b");
        let err = policy.check_path(a_file, &in_b, false).unwrap_err();
        assert!(err.contains("scoped to catalog 'ws_b'"), "{err}");
        policy
            .check_path("s3://ws-b-landing/x.parquet", &in_b, false)
            .expect("B's prefix in B");

        let discovered = tvf_caller_for(&alice(), &flattened, "polaris_wh");
        assert!(discovered.catalog_prefixes.is_empty());
        assert!(policy.check_path(a_file, &discovered, false).is_err());
        for c in [&in_a, &in_b, &discovered] {
            policy
                .check_path("s3://shared-landing/x.csv", c, false)
                .expect("global prefix holds everywhere");
        }
    }

    /// End to end through `read_parquet`: a query in a session scoped to
    /// catalog B is refused catalog A's bucket at planning, before any I/O.
    #[tokio::test]
    async fn read_parquet_in_catalog_b_rejects_catalog_a_prefix() {
        use sqe_catalog::file_tvf_prepare::FileTableFunctions;

        let config = two_workspace_config();
        let flattened: Vec<(String, sqe_core::config::CatalogConfig)> = config
            .flattened_catalogs()
            .into_iter()
            .map(|(n, c)| (n, c.clone()))
            .collect();
        let ctx = SessionContext::new();
        FileTableFunctions::register(
            &ctx,
            "read_parquet",
            Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::with_caller(
                config.storage.clone(),
                tvf_caller_for(&alice(), &flattened, "ws_b"),
            )),
        );
        let err = sqe_catalog::file_tvf_prepare::prepare_statement(
            &ctx,
            "SELECT * FROM read_parquet('s3://ws-a-landing/2026/x.parquet')",
        )
        .await
        .expect_err("catalog A's prefix must not open in catalog B");
        let msg = err.to_string();
        assert!(
            msg.contains("denied") && msg.contains("scoped to catalog 'ws_b'"),
            "{msg}"
        );
    }

    /// The discovery template must not hand its catalog's TVF prefixes to a
    /// warehouse the caller named.
    #[test]
    fn discovery_template_drops_tvf_prefixes() {
        let mut config = test_config_rest();
        config.catalog.tvf_prefixes = vec!["s3://template-landing/".to_string()];
        let tmpl = discovery_template(&config).expect("REST template");
        assert!(tmpl.tvf_prefixes.is_empty());
    }

    /// Issue #240: the session-context cache must hold well beyond the old cap
    /// of 100 so the 101st concurrent user-token pair does not evict someone.
    /// We build a cache with the same capacity constant and prove it retains
    /// far more than 100 keys.
    #[tokio::test]
    async fn session_context_cache_holds_well_beyond_100_keys() {
        let cache: Cache<String, u64> = Cache::builder()
            .max_capacity(SESSION_CONTEXT_CACHE_MAX_CAPACITY)
            .time_to_live(std::time::Duration::from_secs(300))
            .build();

        for i in 0..500u64 {
            cache.insert(format!("token-{i}"), i).await;
        }
        cache.run_pending_tasks().await;

        assert!(
            cache.entry_count() > 100,
            "cache should retain far more than the old cap of 100, got {}",
            cache.entry_count()
        );
    }

    /// Issue #240: idle entries must expire on the TTL rather than relying on
    /// LRU eviction under concurrency. A zero TTL makes entries expire
    /// immediately, so the cache reports no live entries after pending tasks.
    #[tokio::test]
    async fn session_context_cache_expires_on_ttl() {
        let cache: Cache<String, u64> = Cache::builder()
            .max_capacity(SESSION_CONTEXT_CACHE_MAX_CAPACITY)
            .time_to_live(std::time::Duration::from_millis(0))
            .build();

        cache.insert("token-a".to_string(), 1).await;
        cache.run_pending_tasks().await;

        assert!(
            cache.get("token-a").await.is_none(),
            "entry should have expired immediately under a zero TTL"
        );
    }

    // -----------------------------------------------------------------------
    // Session-context UDF registration smoke tests (Task 4, Phase 2B)
    // -----------------------------------------------------------------------

    /// Verify that the five session-context UDFs can be registered on a bare
    /// DataFusion SessionContext and that `is_role_in_session` and
    /// `current_available_roles` execute correctly via `ctx.sql`.
    ///
    /// `current_user()` is skipped here: sqlparser treats `current_user`
    /// (without parens) as a reserved keyword and normalises it to the built-in
    /// form; `current_user()` WITH parens is rejected by DataFusion's SQL
    /// parser (PlannerError). Use `is_role_in_session` or
    /// `current_available_roles` for round-trip tests.
    #[tokio::test]
    async fn session_udfs_register_and_execute_via_ctx_sql() {
        use datafusion::prelude::SessionContext;

        let ctx = SessionContext::new();
        let identity = std::sync::Arc::new(sqe_policy::session_udf::SessionIdentity {
            username: "alice".into(),
            roles: vec!["analyst".into(), "admin".into()],
            database: Some("test_wh".into()),
            schema: None,
        });
        for udf in sqe_policy::session_udf::session_udfs(identity) {
            ctx.register_udf(udf);
        }

        // is_role_in_session('analyst') -> true
        let df = ctx
            .sql("SELECT is_role_in_session('analyst') AS r")
            .await
            .expect("plan is_role_in_session");
        let batches = df.collect().await.expect("execute is_role_in_session");
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("boolean array");
        assert!(col.value(0), "is_role_in_session('analyst') should be true");

        // is_role_in_session('engineer') -> false
        let df2 = ctx
            .sql("SELECT is_role_in_session('engineer') AS r")
            .await
            .expect("plan is_role_in_session false");
        let batches2 = df2
            .collect()
            .await
            .expect("execute is_role_in_session false");
        let col2 = batches2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("boolean array");
        assert!(
            !col2.value(0),
            "is_role_in_session('engineer') should be false"
        );

        // current_available_roles() -> JSON array of roles (sorted)
        let df3 = ctx
            .sql("SELECT current_available_roles() AS r")
            .await
            .expect("plan current_available_roles");
        let batches3 = df3
            .collect()
            .await
            .expect("execute current_available_roles");
        let col3 = batches3[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(
            col3.value(0),
            r#"["admin","analyst"]"#,
            "current_available_roles() should return sorted JSON"
        );
    }

    /// Cross-family check that every custom UDF source the coordinator registers
    /// on a session still plans under the DuckDB SQL dialect (the dialect the
    /// session now uses so `x -> expr` lambdas parse; see #354). Guards against a
    /// dialect regression: a reserved-word collision or changed call syntax that
    /// stops a custom function resolving. The `sqe-trino-functions` suite covers
    /// its own functions exhaustively under DuckDB; this pins the policy,
    /// session-identity, JSON, and lambda families together in one place.
    #[tokio::test]
    async fn custom_udfs_plan_under_duckdb_dialect() {
        use datafusion::prelude::{SessionConfig, SessionContext};

        let cfg = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "DuckDB");
        let mut ctx = SessionContext::new_with_config(cfg);

        // Mirror the coordinator's registration set (see the builder above).
        ctx.register_udf(sqe_policy::sha256_udf::sha256_udf(None));
        let identity = std::sync::Arc::new(sqe_policy::session_udf::SessionIdentity {
            username: "alice".into(),
            roles: vec!["analyst".into()],
            database: Some("wh".into()),
            schema: Some("public".into()),
        });
        for udf in sqe_policy::session_udf::session_udfs(identity) {
            ctx.register_udf(udf);
        }
        sqe_trino_functions::register_trino_functions(&ctx);
        sqe_trino_functions::register_extended_trino_functions(&ctx);
        datafusion_functions_json::register_all(&mut ctx).expect("register json udfs");

        // One representative call per custom-UDF family. Each must parse and
        // build a logical plan under DuckDB; per-function logic is covered by the
        // owning crate's own tests.
        let cases = [
            ("policy sha256", "SELECT sha256('secret')"),
            ("session identity", "SELECT is_role_in_session('analyst')"),
            ("trino scalar", "SELECT typeof(1)"),
            (
                "trino coverage",
                "SELECT count_if(x > 1) FROM (VALUES (1),(2)) t(x)",
            ),
            ("trino extended", "SELECT soundex('robert')"),
            ("json", "SELECT json_get('{\"a\":1}', 'a')"),
            (
                "lambda filter",
                "SELECT filter(make_array(1,2,3), x -> x > 1)",
            ),
            (
                "lambda transform",
                "SELECT transform(make_array(1,2,3), x -> x * 2)",
            ),
            (
                "lambda all_match",
                "SELECT all_match(make_array(2,4), x -> x % 2 = 0)",
            ),
            (
                "lambda none_match",
                "SELECT none_match(make_array(1,3), x -> x % 2 = 0)",
            ),
            (
                "lambda reduce",
                "SELECT reduce(make_array(1,2,3), CAST(0 AS bigint), (s, x) -> s + x, s -> s)",
            ),
        ];
        for (family, sql) in cases {
            let plan = ctx.sql(sql).await;
            assert!(
                plan.is_ok(),
                "{family}: `{sql}` failed to plan under DuckDB: {:?}",
                plan.err()
            );
        }
    }
}
