use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use moka::future::Cache as MokaCache;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use sqe_core::config::{SqeConfig, StorageConfig};
use sqe_core::SqeError;
use sqe_metrics::propagation::trace_context_http_headers;

use crate::circuit_breaker::CircuitBreaker;

/// Process-wide reqwest client reused across every SessionCatalog so
/// non-REST backends stop opening a fresh connection pool and a fresh
/// TLS slow-start per authenticated session. The REST path already
/// honoured the shared-client contract; this lets the non-REST path
/// share the same default when callers pass `None`.
static SHARED_HTTP_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(reqwest::Client::new);

/// Per-user circuit breakers keyed by token fingerprint.
///
/// A single global breaker blast-radiused one user's bad token (rapid
/// retries on an expired bearer count as failures) into "no user can
/// talk to Polaris until the recovery timeout". Keying the breaker by
/// token fingerprint isolates each user's failure budget without
/// changing the rest of the wiring.
static USER_CIRCUIT_BREAKERS: std::sync::LazyLock<DashMap<String, Arc<CircuitBreaker>>> =
    std::sync::LazyLock::new(DashMap::new);

fn user_circuit_breaker(token_fingerprint: &str) -> Arc<CircuitBreaker> {
    if let Some(existing) = USER_CIRCUIT_BREAKERS.get(token_fingerprint) {
        return Arc::clone(existing.value());
    }
    let cb = Arc::new(CircuitBreaker::new(
        format!("polaris-user-{token_fingerprint}"),
        5,
        std::time::Duration::from_secs(30),
    ));
    USER_CIRCUIT_BREAKERS
        .entry(token_fingerprint.to_string())
        .or_insert_with(|| Arc::clone(&cb))
        .clone()
}

/// A cached table entry holding metadata, an optional ETag, and the time it was
/// last validated against Polaris.
#[derive(Clone)]
struct CachedTableEntry {
    table: Table,
    etag: Option<String>,
    /// When this entry was last confirmed fresh (inserted or revalidated via 304).
    validated_at: Instant,
}

/// Global table metadata cache shared across all sessions.
///
/// Cache keys are scoped per user via the session's token fingerprint
/// (`"{token_fingerprint}|{namespace}.{table}"`). The cached `Table` carries a
/// `FileIO` configured with vended S3 credentials returned by Polaris in the
/// `LoadTableResponse.config` block; those credentials are per-user STS, so
/// sharing a cache slot across users would silently hand User A's STS creds to
/// User B on every cache hit. Issue #49.
///
/// When a cached entry's soft TTL expires, the cache performs an ETag-based
/// conditional request (`If-None-Match`) to Polaris. If Polaris returns
/// `304 Not Modified`, the cached metadata is reused without re-downloading.
/// This avoids the full metadata fetch (~50-200 KB JSON) when only a freshness
/// check is needed.
///
/// The cache is created once at coordinator startup and passed into every
/// `SessionCatalog` via [`SessionCatalog::new`]. Each session falls through to
/// Polaris on a miss and populates the shared cache on success.
///
/// Use [`TableMetadataCache::invalidate`] after any DDL/DML that changes table
/// structure (DROP TABLE, ALTER TABLE, INSERT, MERGE, DELETE).
///
/// Process-global cache of `RestCatalog` instances, keyed by
/// `format!("{catalog_url}-{token_fingerprint}")`. Each entry holds an
/// `Arc<RestCatalog>` that bakes in the bearer token at construction
/// time, so when Polaris-side token expiry crosses the 5-minute TTL boundary
/// the cached entry returns 401 on every subsequent call. Issue #20 covers
/// the symptom (dbt models 401'ing partway through a run) and the matching
/// `invalidate_rest_catalog_cache_all` below is the error-driven escape
/// hatch called from the query handler whenever a catalog op surfaces an
/// `AuthenticationFailed`.
///
/// The cached value used to be `Arc<RwLock<RestCatalog>>` but every caller
/// only ever took the lock for read — `RestCatalog` is `Send + Sync` and
/// stateless per call. The lock plus its yield point were pure overhead
/// (~100-500 ns per dispatch, compounding on metadata fan-out). Issue #18.
pub(crate) static REST_CATALOG_CACHE: std::sync::LazyLock<
    moka::future::Cache<String, Arc<RestCatalog>>,
> = std::sync::LazyLock::new(|| {
    moka::future::Cache::builder()
        .max_capacity(100)
        .time_to_live(std::time::Duration::from_secs(300))
        .build()
});

/// Drop every cached `RestCatalog` entry. Called from the query handler when a
/// catalog operation surfaces 401/403, so the next query rebuilds the catalog
/// with whatever bearer the session has at that point (either a refreshed
/// token from the background refresher, or a fresh OIDC exchange after the
/// client re-authenticates).
///
/// Heavy hammer rather than per-entry invalidation: SQE does not maintain a
/// `username -> token_fingerprint` reverse index, so we cannot scope the
/// eviction to one user without a side-band map. Auth failures are rare; the
/// rebuild cost is amortised across however many entries were cached (max 100,
/// ~250 ms each, but lazily on next access — not all at once).
pub async fn invalidate_rest_catalog_cache_all() {
    REST_CATALOG_CACHE.invalidate_all();
    REST_CATALOG_CACHE.run_pending_tasks().await;
    debug!("REST_CATALOG_CACHE invalidated (auth failure recovery)");
}

#[derive(Clone)]
pub struct TableMetadataCache {
    /// Long-lived cache (1 hour hard TTL) holding table metadata + ETag.
    /// Soft freshness is checked via `CachedTableEntry::validated_at`.
    inner: MokaCache<String, CachedTableEntry>,
    /// Soft TTL: entries older than this are revalidated via conditional GET.
    soft_ttl: std::time::Duration,
    /// Optional Prometheus metrics for catalog roundtrip + circuit
    /// breaker state. Threaded through the cache so every
    /// `SessionCatalog` clone shares the same handle without changing
    /// the constructor signature.
    metrics: Option<std::sync::Arc<sqe_metrics::MetricsRegistry>>,
}

impl TableMetadataCache {
    /// Create a global table metadata cache with the given TTL.
    ///
    /// `ttl_secs` is the *soft* TTL — after this period, cached entries are
    /// revalidated via `If-None-Match`. The *hard* TTL (moka eviction) is set
    /// to 1 hour to keep stale entries available for conditional revalidation.
    ///
    /// Pass `ttl_secs = 0` to disable the cache (entries are never stored).
    pub fn new(ttl_secs: u64) -> Self {
        let (inner, soft_ttl) = if ttl_secs == 0 {
            (
                MokaCache::builder().max_capacity(0).build(),
                std::time::Duration::ZERO,
            )
        } else {
            (
                MokaCache::builder()
                    .max_capacity(1000)
                    // Hard TTL: keep entries for 1 hour so ETag revalidation can work
                    // even if the soft TTL is much shorter (e.g. 30s).
                    .time_to_live(std::time::Duration::from_secs(3600))
                    .build(),
                std::time::Duration::from_secs(ttl_secs),
            )
        };
        Self {
            inner,
            soft_ttl,
            metrics: None,
        }
    }

    /// Attach a metrics registry. Every `SessionCatalog` clone of this
    /// cache will see the same handle and report catalog roundtrip
    /// latency + circuit breaker state into it.
    #[must_use = "with_metrics consumes self; bind the returned cache"]
    pub fn with_metrics(mut self, metrics: std::sync::Arc<sqe_metrics::MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns the attached metrics registry, if any.
    pub fn metrics(&self) -> Option<&std::sync::Arc<sqe_metrics::MetricsRegistry>> {
        self.metrics.as_ref()
    }

    /// Get a cached entry if it exists and is still fresh (within soft TTL).
    /// Returns `None` if no entry exists or if the entry has expired.
    pub async fn get_fresh(&self, key: &str) -> Option<Table> {
        let entry = self.inner.get(key).await?;
        if entry.validated_at.elapsed() < self.soft_ttl {
            Some(entry.table)
        } else {
            None
        }
    }

    /// Get a stale cached entry for conditional revalidation.
    /// Returns the table and its ETag if the entry exists (regardless of soft TTL).
    pub async fn get_stale(&self, key: &str) -> Option<(Table, Option<String>)> {
        let entry = self.inner.get(key).await?;
        Some((entry.table, entry.etag))
    }

    /// Refresh the soft TTL on an existing entry (called after a 304 revalidation).
    pub async fn revalidate(&self, key: &str) {
        if let Some(mut entry) = self.inner.get(key).await {
            entry.validated_at = Instant::now();
            self.inner.insert(key.to_string(), entry).await;
        }
    }

    /// Backwards-compatible `get` — returns fresh entries only.
    pub async fn get(&self, key: &str) -> Option<Table> {
        self.get_fresh(key).await
    }

    pub async fn insert(&self, key: String, table: Table) {
        self.insert_with_etag(key, table, None).await;
    }

    /// Insert a table with an optional ETag from the response headers.
    pub async fn insert_with_etag(&self, key: String, table: Table, etag: Option<String>) {
        self.inner
            .insert(
                key,
                CachedTableEntry {
                    table,
                    etag,
                    validated_at: Instant::now(),
                },
            )
            .await;
    }

    /// Attach an ETag to an existing cache entry without changing the cached
    /// table or refreshing the soft-TTL clock. Used by the background HEAD
    /// path so the ETag becomes available for future conditional revalidation
    /// without blocking the cold load_table().
    pub async fn update_etag(&self, key: &str, etag: Option<String>) {
        if let Some(mut entry) = self.inner.get(key).await {
            entry.etag = etag;
            self.inner.insert(key.to_string(), entry).await;
        }
    }

    pub async fn invalidate(&self, key: &str) {
        self.inner.invalidate(key).await;
    }

    /// Number of entries currently held in the cache.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

/// Backend handle inside `SessionCatalog`.
///
/// REST keeps an `Arc<RestCatalog>` because the per-session
/// `REST_CATALOG_CACHE` (keyed by URL + token fingerprint) hands out
/// the same Arc to every session that authenticates with the same
/// token. The earlier `Arc<RwLock<RestCatalog>>` shape had zero
/// `.write()` callers anywhere in the codebase — every dispatch
/// took `read().await`, which is purely a futex acquisition plus an
/// extra scheduler yield point. `RestCatalog` is `Send + Sync` and
/// stateless per call, so the lock was overhead.
///
/// Non-REST backends construct their iceberg::Catalog implementation
/// once during `for_session` and store it directly as a trait
/// object; there is no equivalent shared cache today (HMS / Glue /
/// JDBC catalog construction is cheap and idempotent).
pub(crate) enum CatalogHandle {
    Rest(Arc<RestCatalog>),
    Other(Arc<dyn iceberg::Catalog>),
}

impl std::fmt::Debug for CatalogHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rest(_) => f.debug_tuple("Rest").field(&"<RestCatalog>").finish(),
            Self::Other(c) => f.debug_tuple("Other").field(c).finish(),
        }
    }
}

/// Match on a `CatalogHandle` and run the same iceberg::Catalog
/// trait method against either variant. Macro because each method
/// signature is different and async closures aren't stable.
///
/// Usage:
/// ```ignore
/// dispatch_catalog!(self.inner, list_namespaces(parent))
/// ```
/// Expands to a match that acquires the REST read lock when needed
/// and returns the awaited iceberg::Catalog method result. Used by
/// every `SessionCatalog` and `SessionCatalogBridge` method that
/// only needs the standard trait surface.
macro_rules! dispatch_catalog {
    ($handle:expr, $method:ident($($args:expr),* $(,)?)) => {
        match &$handle {
            $crate::rest_catalog::CatalogHandle::Rest(rest) => {
                // No `.read().await` here: `RestCatalog` is `Send + Sync`
                // and stateless per call, so the previous `RwLock` was
                // pure overhead. Issue #18.
                rest.$method($($args),*).await
            }
            $crate::rest_catalog::CatalogHandle::Other(catalog) => {
                catalog.$method($($args),*).await
            }
        }
    };
}
// Re-export the macro for use within this module. `pub(crate) use`
// is the documented idiom even though the import looks self-referential.
#[allow(unused_imports)]
pub(crate) use dispatch_catalog;

/// Per-session Iceberg REST catalog wrapper.
///
/// Each authenticated user session gets its own `SessionCatalog` instance
/// configured with the user's bearer token. The token is passed directly to
/// the Polaris REST catalog so that table-level authorization is enforced by
/// the catalog server.
///
/// A single `reqwest::Client` and `CircuitBreaker` are shared across all
/// sessions (passed in at construction time) to ensure:
/// * **Connection reuse**: one hyper connection pool, no per-session teardown.
/// * **Fault isolation**: when Polaris is unavailable the circuit opens and
///   subsequent requests fail fast without wasting threads / connections.
pub struct SessionCatalog {
    /// Backend handle. The REST variant holds an `Arc<RestCatalog>`
    /// (previously `Arc<RwLock<RestCatalog>>`; the lock was removed
    /// in issue #18 because every dispatch only ever did a read).
    /// Non-REST variants hold the iceberg trait object directly.
    /// REST-specific methods on `SessionCatalog` (view DDL, raw
    /// `commit_schema_update`, ETag revalidation in `load_table`)
    /// match on this and error out when the backend isn't REST.
    inner: CatalogHandle,
    catalog_url: String,
    warehouse: String,
    bearer_token: String,
    token_fingerprint: String,
    storage_config: StorageConfig,
    http_client: reqwest::Client,
    /// Shared circuit breaker for Polaris REST calls.
    circuit_breaker: Arc<CircuitBreaker>,
    /// Shared table metadata cache.
    ///
    /// When a global `TableMetadataCache` is provided at construction time it is
    /// used directly (shared across all sessions). Otherwise a private per-session
    /// cache is created as a fallback (used in tests / when the caller passes `None`).
    ///
    /// Cache is keyed by `"{token_fingerprint}|{namespace}.{table_name}"` so vended
    /// per-user S3 credentials baked into the cached `Table` never cross sessions
    /// (issue #49). TTL and capacity are configured when the global cache is created
    /// (see `TableMetadataCache::new`).
    table_cache: TableMetadataCache,
}

impl std::fmt::Debug for SessionCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCatalog")
            .field("catalog_url", &self.catalog_url)
            .field("warehouse", &self.warehouse)
            .field("token_fingerprint", &self.token_fingerprint)
            .field("circuit_breaker", &self.circuit_breaker.state_label())
            .field("table_cache_entries", &self.table_cache.entry_count())
            .finish()
    }
}

impl SessionCatalog {
    /// Create a new per-session catalog configured with the user's bearer token.
    ///
    /// The `bearer_token` is set as the `token` property in the REST catalog config,
    /// which causes iceberg-rust to send it as a Bearer token in the Authorization header.
    ///
    /// A token fingerprint (last 8 chars) is included in the session identifier to
    /// ensure that token refreshes invalidate the iceberg-rust internal REST session cache.
    ///
    /// `http_client` and `circuit_breaker` should be shared across all sessions (created
    /// once at startup) so that TCP connections and failure state are pooled globally.
    /// Pass `None` for either to fall back to per-instance defaults (suitable for tests).
    ///
    /// `table_cache` is the shared global `TableMetadataCache` created once at coordinator
    /// startup. When `Some`, all sessions share the same cache so cache misses are amortised
    /// across users. When `None` a private cache is built locally (used in tests).
    /// Build a `SessionCatalog` from the coordinator's `SqeConfig` plus
    /// the user's bearer token. Optional shared table cache is forwarded
    /// to `Self::new`.
    ///
    /// This is the helper every `*_handler.rs` should use; it keeps the
    /// 8-arg `new()` constructor available for the rare callers (tests)
    /// that need to wire a custom HTTP client or circuit breaker. The
    /// 13 coordinator call sites that all pass the same `(config,
    /// session.access_token, table_cache, None, None)` tuple should go
    /// through this single entry point so changes to the catalog
    /// construction path don't have to touch each handler.
    ///
    /// Phase O+ step 2: dispatches on `config.catalog.backend`. The
    /// REST variant (default) goes through `Self::new` and gives back
    /// the legacy `CatalogHandle::Rest` shape so view DDL and
    /// `commit_schema_update` keep working. HMS / Glue / JDBC build
    /// the matching iceberg::Catalog through the per-backend
    /// constructor in `crates/sqe-catalog/src/backends/` and store
    /// it as `CatalogHandle::Other`. REST-only methods on
    /// SessionCatalog return an error under non-REST handles.
    pub async fn for_session(
        config: &SqeConfig,
        table_cache: Option<TableMetadataCache>,
        bearer_token: &str,
    ) -> sqe_core::Result<Self> {
        Self::for_session_with(&config.catalog, &config.storage, table_cache, bearer_token).await
    }

    /// Per-catalog variant of `for_session`. Takes a single
    /// `CatalogConfig` rather than reaching into `SqeConfig.catalog`,
    /// so the multi-catalog cluster path can build one
    /// `SessionCatalog` per entry in `[catalogs.*]`. The `storage`
    /// argument is shared across all catalogs because S3 credentials
    /// today are a coordinator-wide concern (per-catalog credential
    /// scoping is a future change).
    pub async fn for_session_with(
        catalog: &sqe_core::config::CatalogConfig,
        storage: &sqe_core::config::StorageConfig,
        table_cache: Option<TableMetadataCache>,
        bearer_token: &str,
    ) -> sqe_core::Result<Self> {
        match &catalog.backend {
            sqe_core::config::CatalogBackend::Rest => {
                Self::new(
                    &catalog.catalog_url,
                    &catalog.warehouse,
                    bearer_token,
                    storage,
                    table_cache,
                    None,
                    None,
                )
                .await
            }
            other => Self::for_session_other_backend_with(
                catalog,
                storage,
                bearer_token,
                table_cache.unwrap_or_else(|| TableMetadataCache::new(0)),
                other,
            )
            .await,
        }
    }

    /// Per-catalog variant. Same logic as `for_session_other_backend`
    /// but scoped to a single `CatalogConfig` so the multi-catalog
    /// path can iterate over `[catalogs.*]` entries.
    async fn for_session_other_backend_with(
        catalog: &sqe_core::config::CatalogConfig,
        storage: &sqe_core::config::StorageConfig,
        bearer_token: &str,
        table_cache: TableMetadataCache,
        backend: &sqe_core::config::CatalogBackend,
    ) -> sqe_core::Result<Self> {
        use sqe_core::config::CatalogBackend;

        // Each non-REST backend translates its typed config into the
        // upstream loader's `(catalog_type, props_map)` shape. The
        // loader picks the right `CatalogBuilder`, applies the
        // shared `OpenDalStorageFactory::Fs` we want for SQE, and
        // returns an `Arc<dyn iceberg::Catalog>`. All of this used
        // to live in per-backend wrappers under
        // `crates/sqe-catalog/src/backends/`; the wrappers are gone
        // because the loader's uniform shape made them redundant.
        // See `vendor/iceberg-rust/README.md` and
        // `docs/catalogs.md` for the supported prop keys per backend.
        let (catalog_type, name, props): (&str, &str, HashMap<String, String>) = match backend {
            CatalogBackend::Rest => unreachable!("Rest handled in for_session"),

            #[cfg(feature = "hms")]
            CatalogBackend::Hms { uri, warehouse } => {
                use iceberg_catalog_hms::{HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE};
                let mut p = HashMap::new();
                p.insert(HMS_CATALOG_PROP_URI.to_string(), uri.clone());
                p.insert(HMS_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                ("hms", "sqe-hms-session", p)
            }
            #[cfg(not(feature = "hms"))]
            CatalogBackend::Hms { .. } => {
                return Err(SqeError::Catalog(
                    "HMS backend requires the `hms` cargo feature on sqe-catalog".into(),
                ));
            }

            #[cfg(feature = "glue")]
            CatalogBackend::Glue { region, warehouse, endpoint } => {
                use iceberg_catalog_glue::GLUE_CATALOG_PROP_WAREHOUSE;
                let mut p = HashMap::new();
                p.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                // The Glue builder reads `region` and `endpoint` via
                // the AWS SDK shared config layer; pass them through
                // as standard AWS keys so LocalStack and custom
                // endpoints work.
                p.insert("aws.region".to_string(), region.clone());
                if let Some(ep) = endpoint {
                    p.insert("aws.endpoint_url".to_string(), ep.clone());
                }
                ("glue", "sqe-glue-session", p)
            }
            #[cfg(not(feature = "glue"))]
            CatalogBackend::Glue { .. } => {
                return Err(SqeError::Catalog(
                    "Glue backend requires the `glue` cargo feature on sqe-catalog".into(),
                ));
            }

            #[cfg(feature = "sql-postgres")]
            CatalogBackend::Jdbc { url, warehouse } => {
                use iceberg_catalog_sql::{SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE};
                let mut p = HashMap::new();
                p.insert(SQL_CATALOG_PROP_URI.to_string(), url.clone());
                p.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                ("sql", "sqe-jdbc-session", p)
            }
            #[cfg(not(feature = "sql-postgres"))]
            CatalogBackend::Jdbc { .. } => {
                return Err(SqeError::Catalog(
                    "JDBC backend requires the `sql-postgres` cargo feature on sqe-catalog".into(),
                ));
            }

            #[cfg(feature = "s3tables")]
            CatalogBackend::S3tables { table_bucket_arn, endpoint_url } => {
                use iceberg_catalog_s3tables::{
                    S3TABLES_CATALOG_PROP_ENDPOINT_URL, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN,
                };
                let mut p = HashMap::new();
                p.insert(
                    S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
                    table_bucket_arn.clone(),
                );
                if let Some(ep) = endpoint_url {
                    p.insert(S3TABLES_CATALOG_PROP_ENDPOINT_URL.to_string(), ep.clone());
                }
                ("s3tables", "sqe-s3tables-session", p)
            }
            #[cfg(not(feature = "s3tables"))]
            CatalogBackend::S3tables { .. } => {
                return Err(SqeError::Catalog(
                    "S3 Tables backend requires the `s3tables` cargo feature on sqe-catalog".into(),
                ));
            }
        };

        let inner: Arc<dyn iceberg::Catalog> = iceberg_catalog_loader::load(catalog_type)
            .map_err(|e| SqeError::Catalog(format!(
                "Catalog loader rejected type `{catalog_type}`: {e}"
            )))?
            .load(name.to_string(), props)
            .await
            .map_err(|e| SqeError::Catalog(format!(
                "Catalog `{catalog_type}` build failed: {e}"
            )))?;

        let token_fingerprint = {
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(bearer_token.as_bytes());
            format!("{:x}", hash)[..16].to_string()
        };

        info!(
            backend = ?backend,
            token_fingerprint = %token_fingerprint,
            "Creating SessionCatalog over non-REST backend"
        );

        let circuit_breaker = user_circuit_breaker(&token_fingerprint);
        Ok(Self {
            inner: CatalogHandle::Other(inner),
            catalog_url: String::new(),
            warehouse: catalog.warehouse.clone(),
            bearer_token: bearer_token.to_string(),
            token_fingerprint,
            storage_config: storage.clone(),
            http_client: SHARED_HTTP_CLIENT.clone(),
            circuit_breaker,
            table_cache,
        })
    }

    pub async fn new(
        catalog_url: &str,
        warehouse: &str,
        bearer_token: &str,
        storage_config: &StorageConfig,
        table_cache: Option<TableMetadataCache>,
        http_client: Option<reqwest::Client>,
        circuit_breaker: Option<Arc<CircuitBreaker>>,
    ) -> sqe_core::Result<Self> {
        let token_fingerprint = {
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(bearer_token.as_bytes());
            format!("{:x}", hash)[..16].to_string()
        };

        info!(
            catalog_url = catalog_url,
            warehouse = warehouse,
            token_fingerprint = %token_fingerprint,
            "Creating per-session REST catalog"
        );

        let mut props = HashMap::new();
        // Set the bearer token; iceberg-rust's RestCatalog reads the "token" prop
        // and uses it in the Authorization: Bearer header.
        //
        // Per `crates/sqe-auth/src/per_catalog.rs`, an empty bearer is the
        // documented signal for "no Authorization header" (Anonymous / Aws
        // catalogs, sessions before OIDC has issued a token). Inserting
        // "token" -> "" makes iceberg-rust treat the catalog as authenticated
        // with an empty bearer, which the recent defensive guard in
        // `HttpClient::authenticate` correctly rejects, but only at request
        // time. We refuse it here at construction time so the misconfiguration
        // surfaces with a clearer call stack and never reaches the wire.
        if !bearer_token.is_empty() {
            props.insert("token".to_string(), bearer_token.to_string());
        }

        // Set the REST catalog URI and warehouse
        props.insert("uri".to_string(), catalog_url.to_string());
        props.insert("warehouse".to_string(), warehouse.to_string());

        // Inject S3 storage config as properties so that FileIO can be configured
        // when loading tables (fallback when credential vending is not available).
        if !storage_config.s3_endpoint.is_empty() {
            props.insert(
                "s3.endpoint".to_string(),
                storage_config.s3_endpoint.clone(),
            );
        }
        if !storage_config.s3_region.is_empty() {
            props.insert("s3.region".to_string(), storage_config.s3_region.clone());
        }
        if !storage_config.s3_access_key.is_empty() {
            props.insert(
                "s3.access-key-id".to_string(),
                storage_config.s3_access_key.clone(),
            );
        }
        if !storage_config.s3_secret_key.is_empty() {
            props.insert(
                "s3.secret-access-key".to_string(),
                storage_config.s3_secret_key.expose().to_string(),
            );
        }
        if storage_config.s3_path_style {
            props.insert("s3.path-style-access".to_string(), "true".to_string());
        }

        // RisingWave fork uses CatalogBuilder::load(name, props) pattern.
        // Storage factory (OpenDAL S3) is configured automatically from the s3.*
        // properties in the props HashMap — no explicit with_storage_factory() needed.
        //
        let catalog_key = format!("{}-{}", catalog_url, token_fingerprint);
        let inner = if let Some(cached) = REST_CATALOG_CACHE.get(&catalog_key).await {
            debug!(token_fingerprint = %token_fingerprint, "REST catalog cache hit");
            cached
        } else {
            debug!(token_fingerprint = %token_fingerprint, "REST catalog cache miss, creating");
            let catalog = RestCatalogBuilder::default()
                .load(
                    format!("sqe-session-{}", &token_fingerprint),
                    props,
                )
                .await
                .map_err(|e| SqeError::Catalog(format!("Failed to create REST catalog: {e}")))?;
            let arc_catalog = Arc::new(catalog);
            REST_CATALOG_CACHE.insert(catalog_key, arc_catalog.clone()).await;
            arc_catalog
        };

        let http_client = http_client.unwrap_or_else(|| SHARED_HTTP_CLIENT.clone());
        let circuit_breaker = circuit_breaker.unwrap_or_else(|| user_circuit_breaker(&token_fingerprint));

        // Use the shared global cache when provided; fall back to a private
        // per-session cache (disabled — max_capacity 0) so that call sites that
        // pass `None` (tests, one-shot DDL helpers) don't pollute a global state.
        let table_cache = table_cache.unwrap_or_else(|| TableMetadataCache::new(0));

        Ok(Self {
            inner: CatalogHandle::Rest(inner),
            catalog_url: catalog_url.to_string(),
            warehouse: warehouse.to_string(),
            bearer_token: bearer_token.to_string(),
            token_fingerprint,
            storage_config: storage_config.clone(),
            http_client,
            circuit_breaker,
            table_cache,
        })
    }

    /// Returns the token fingerprint for this session (last 8 chars of the bearer token).
    pub fn token_fingerprint(&self) -> &str {
        &self.token_fingerprint
    }

    /// Returns the storage config used for fallback S3 credentials.
    pub fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    /// Returns the REST catalog URL.
    pub fn catalog_url(&self) -> &str {
        &self.catalog_url
    }

    /// Returns the warehouse name.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// List all namespaces in the catalog.
    #[instrument(skip(self), fields(warehouse = %self.warehouse))]
    pub async fn list_namespaces(&self) -> sqe_core::Result<Vec<NamespaceIdent>> {
        debug!(token_fingerprint = %self.token_fingerprint, "Listing namespaces");
        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;
        let started = Instant::now();
        let result = dispatch_catalog!(self.inner, list_namespaces(None))
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to list namespaces: {e}")));
        match &result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(_) => self.circuit_breaker.record_failure(),
        }
        self.record_catalog_call("list_namespaces", started, result.is_ok());
        result
    }

    /// Get a namespace by its identifier.
    ///
    /// Returns the `Namespace` object which includes namespace properties.
    pub async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> sqe_core::Result<iceberg::Namespace> {
        debug!(
            token_fingerprint = %self.token_fingerprint,
            namespace = ?namespace,
            "Getting namespace"
        );
        dispatch_catalog!(self.inner, get_namespace(namespace))
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to get namespace: {e}")))
    }

    /// List all tables in the given namespace.
    #[instrument(skip(self), fields(namespace = ?namespace, warehouse = %self.warehouse))]
    pub async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> sqe_core::Result<Vec<TableIdent>> {
        debug!(
            token_fingerprint = %self.token_fingerprint,
            namespace = ?namespace,
            "Listing tables"
        );
        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;
        let started = Instant::now();
        let result = dispatch_catalog!(self.inner, list_tables(namespace))
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to list tables: {e}")));
        match &result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(_) => self.circuit_breaker.record_failure(),
        }
        self.record_catalog_call("list_tables", started, result.is_ok());
        result
    }

    /// Cache key for the table metadata cache.
    ///
    /// Scoped to the session's token fingerprint so vended S3 credentials baked
    /// into the cached `Table` (per-user STS creds returned by Polaris) never
    /// leak across users. Issue #49.
    fn table_cache_key(&self, table_ident: &TableIdent) -> String {
        format!(
            "{}|{}.{}",
            self.token_fingerprint,
            table_ident.namespace(),
            table_ident.name()
        )
    }

    /// Load a table by its identifier.
    ///
    /// The returned `Table` includes metadata and a FileIO configured with
    /// vended credentials (if Polaris provides them) or static S3 config.
    ///
    /// Results are cached in the shared global `TableMetadataCache` (passed at construction
    /// time). When a cached entry's soft TTL expires, the cache sends a conditional
    /// `If-None-Match` request using the stored ETag. If Polaris returns 304, the
    /// cached metadata is reused without re-downloading.
    ///
    /// Use [`invalidate_table`] to evict an entry after DDL/DML.
    #[instrument(skip(self), fields(table = %table_ident, warehouse = %self.warehouse))]
    pub async fn load_table(&self, table_ident: &TableIdent) -> sqe_core::Result<Table> {
        let cache_key = self.table_cache_key(table_ident);

        // Fast path: return cached table that is still within the soft TTL.
        if let Some(cached) = self.table_cache.get_fresh(&cache_key).await {
            debug!(
                token_fingerprint = %self.token_fingerprint,
                table = ?table_ident,
                "Table cache hit (fresh)"
            );
            return Ok(cached);
        }

        // Check for a stale entry with an ETag for conditional revalidation.
        if let Some((stale_table, Some(etag))) = self.table_cache.get_stale(&cache_key).await {
            debug!(
                token_fingerprint = %self.token_fingerprint,
                table = ?table_ident,
                etag = %etag,
                "Table cache stale, attempting ETag revalidation"
            );

            self.circuit_breaker
                .check()
                .map_err(sqe_core::SqeError::Catalog)?;

            // Build the REST URL for the loadTable endpoint.
            let ns_str = table_ident
                .namespace()
                .as_ref()
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join("\u{1F}");
            let url = format!(
                "{}/namespaces/{}/tables/{}",
                self.rest_prefix(),
                ns_str,
                table_ident.name()
            );

            let mut req = self
                .http_client
                .get(&url)
                .bearer_auth(&self.bearer_token)
                .header("If-None-Match", &etag)
                .header("X-Request-ID", Uuid::new_v4().to_string());
            for (k, v) in trace_context_http_headers() {
                req = req.header(k, v);
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status == reqwest::StatusCode::NOT_MODIFIED {
                        // 304: metadata unchanged, refresh the soft TTL.
                        debug!(
                            table = ?table_ident,
                            "ETag revalidation: 304 Not Modified, reusing cached metadata"
                        );
                        self.circuit_breaker.record_success();
                        self.table_cache.revalidate(&cache_key).await;
                        return Ok(stale_table);
                    }
                    // Non-304: fall through to the full load path below.
                    // The response body from this GET could in theory be parsed,
                    // but the Polaris loadTable response is complex (includes
                    // credential vending, FileIO config, etc.) and is best handled
                    // by iceberg-rust's RestCatalog. So we discard this response
                    // and let the normal path handle it.
                    debug!(
                        table = ?table_ident,
                        status = %status,
                        "ETag revalidation: metadata changed, performing full reload"
                    );
                    self.circuit_breaker.record_success();
                }
                Err(e) => {
                    // Network error during revalidation — fall through to normal load.
                    warn!(
                        table = ?table_ident,
                        error = %e,
                        "ETag revalidation request failed, falling back to full load"
                    );
                    self.circuit_breaker.record_failure();
                }
            }
        } else {
            debug!(
                token_fingerprint = %self.token_fingerprint,
                table = ?table_ident,
                "Loading table (cache miss)"
            );
        }

        // Full load via the configured catalog backend (REST or
        // any iceberg::Catalog). Non-REST backends skip the ETag
        // capture below since they do not expose REST headers.
        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;
        let started = Instant::now();
        let result = dispatch_catalog!(self.inner, load_table(table_ident))
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to load table: {e}")));
        match &result {
            Ok(table) => {
                self.circuit_breaker.record_success();
                self.table_cache
                    .insert_with_etag(cache_key.clone(), table.clone(), None)
                    .await;

                let http_client = self.http_client.clone();
                let bearer_token = self.bearer_token.clone();
                let url = self.table_url(table_ident);
                let table_cache = self.table_cache.clone();
                let table_ident_log = table_ident.clone();
                tokio::spawn(async move {
                    let etag = fetch_table_etag_inner(&http_client, &bearer_token, &url).await;
                    if let Some(e) = etag.as_deref() {
                        debug!(table = %table_ident_log, etag = %e, "Captured ETag for table");
                    }
                    table_cache.update_etag(&cache_key, etag).await;
                });
            }
            Err(_) => self.circuit_breaker.record_failure(),
        }
        self.record_catalog_call("load_table", started, result.is_ok());
        result
    }

    /// Record catalog roundtrip latency + circuit breaker state into
    /// the optional MetricsRegistry attached to the table cache. The
    /// helper is a no-op when no metrics handle is attached, so
    /// test-only SessionCatalogs pay nothing.
    fn record_catalog_call(&self, op: &'static str, started: Instant, ok: bool) {
        if let Some(metrics) = self.table_cache.metrics() {
            let status = if ok { "ok" } else { "err" };
            metrics
                .catalog_request_duration_seconds
                .with_label_values(&[op, status])
                .observe(started.elapsed().as_secs_f64());
            metrics
                .catalog_circuit_breaker_state
                .with_label_values(&[self.circuit_breaker.name()])
                .set(self.circuit_breaker.state_code() as f64);
        }
    }

    fn table_url(&self, table_ident: &TableIdent) -> String {
        let ns_str = table_ident
            .namespace()
            .as_ref()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\u{1F}");
        format!(
            "{}/namespaces/{}/tables/{}",
            self.rest_prefix(),
            ns_str,
            table_ident.name()
        )
    }

}

/// HEAD-based ETag fetch usable from a background `tokio::spawn`.
async fn fetch_table_etag_inner(
    http_client: &reqwest::Client,
    bearer_token: &str,
    url: &str,
) -> Option<String> {
    let mut req = http_client
        .head(url)
        .bearer_auth(bearer_token)
        .header("X-Request-ID", Uuid::new_v4().to_string());
    for (k, v) in trace_context_http_headers() {
        req = req.header(k, v);
    }
    match req.send().await {
        Ok(resp) => resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        Err(e) => {
            debug!(url = %url, error = %e, "Failed to fetch ETag (non-fatal)");
            None
        }
    }
}

impl SessionCatalog {

    /// Evict a table from the metadata cache.
    ///
    /// Call this after any DDL/DML operation that changes the table's metadata
    /// (DROP TABLE, CREATE TABLE, ALTER TABLE, INSERT, MERGE, DELETE) so that
    /// the next `load_table()` fetches fresh metadata from Polaris.
    pub async fn invalidate_table(&self, table_ident: &TableIdent) {
        let key = self.table_cache_key(table_ident);
        self.table_cache.invalidate(&key).await;
        debug!(table = %table_ident, "Table cache invalidated");
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, table_ident: &TableIdent) -> sqe_core::Result<bool> {
        dispatch_catalog!(self.inner, table_exists(table_ident)).map_err(|e| {
            sqe_core::SqeError::Catalog(format!("Failed to check table existence: {e}"))
        })
    }

    /// Build the Polaris REST URL prefix for this warehouse.
    fn rest_prefix(&self) -> String {
        let base = self.catalog_url.trim_end_matches('/');
        format!("{base}/v1/{}", self.warehouse)
    }

    /// Guard for methods that talk to the REST catalog directly
    /// (view DDL, raw `commit_schema_update`). These bypass the
    /// iceberg::Catalog trait, so they only function under
    /// `CatalogHandle::Rest`. Returns an error pointing at the
    /// backend mismatch rather than letting a downstream HTTP call
    /// fail with an opaque "connection refused" against an empty
    /// catalog_url.
    fn require_rest_backend(&self, op: &str) -> sqe_core::Result<()> {
        if matches!(self.inner, CatalogHandle::Rest(_)) {
            Ok(())
        } else {
            Err(SqeError::Catalog(format!(
                "{op} requires the REST catalog backend; the configured \
                 backend ({:?}) does not expose this operation through the \
                 iceberg::Catalog trait. Switch [catalog].backend to `rest` \
                 or use a tool that talks directly to the configured backend.",
                self.inner
            )))
        }
    }

    /// Create a view via the Polaris REST API.
    ///
    /// Calls `POST /v1/{prefix}/namespaces/{namespace}/views` with the
    /// Iceberg view creation payload.
    #[instrument(skip(self, sql, schema), fields(namespace = ?namespace, view = %name, warehouse = %self.warehouse))]
    pub async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
        sql: &str,
        schema: &serde_json::Value,
    ) -> sqe_core::Result<()> {
        self.require_rest_backend("create_view")?;
        let ns_str = namespace
            .as_ref()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\u{1F}"); // Iceberg REST uses unit separator for multi-level namespaces
        let url = format!(
            "{}/namespaces/{}/views",
            self.rest_prefix(),
            ns_str
        );

        let now_ms = chrono::Utc::now().timestamp_millis();

        let body = serde_json::json!({
            "name": name,
            "schema": schema,
            "view-version": {
                "version-id": 1,
                "schema-id": 0,
                "timestamp-ms": now_ms,
                "summary": { "engine-name": "sqe" },
                "representations": [{
                    "type": "sql",
                    "sql": sql,
                    "dialect": "sqe"
                }],
                "default-namespace": namespace.as_ref(),
            },
            "properties": {}
        });

        debug!(url = %url, view = name, "Creating view via Polaris REST");

        let mut req = self
            .http_client
            .post(&url)
            .bearer_auth(&self.bearer_token)
            .header("X-Request-ID", Uuid::new_v4().to_string())
            .json(&body);
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to create view: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(SqeError::Catalog(format!("Rate limited by Polaris catalog: {text}")));
            }
            if status == reqwest::StatusCode::CONFLICT {
                return Err(SqeError::Execution(format!("Catalog commit conflict: {text}")));
            }
            return Err(SqeError::Catalog(format!(
                "Failed to create view (HTTP {status}): {text}"
            )));
        }

        info!(view = name, namespace = ?namespace, "View created");
        Ok(())
    }

    /// List views in a namespace via the Polaris REST API.
    #[instrument(skip(self), fields(namespace = ?namespace, warehouse = %self.warehouse))]
    pub async fn list_views(
        &self,
        namespace: &NamespaceIdent,
    ) -> sqe_core::Result<Vec<String>> {
        self.require_rest_backend("list_views")?;
        let ns_str = namespace
            .as_ref()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\u{1F}");
        let url = format!("{}/namespaces/{}/views", self.rest_prefix(), ns_str);

        let mut req = self
            .http_client
            .get(&url)
            .bearer_auth(&self.bearer_token)
            .header("X-Request-ID", Uuid::new_v4().to_string());
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to list views: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(SqeError::Catalog(format!("Rate limited by Polaris catalog: {text}")));
            }
            if status == reqwest::StatusCode::CONFLICT {
                return Err(SqeError::Execution(format!("Catalog commit conflict: {text}")));
            }
            return Err(SqeError::Catalog(format!(
                "Failed to list views (HTTP {status}): {text}"
            )));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to parse views list: {e}")))?;

        let names = body["identifiers"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v["name"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Ok(names)
    }

    /// Load a view's SQL definition from the Polaris REST API.
    ///
    /// Returns `None` if the view does not exist (404), or the SQL string on success.
    #[instrument(skip(self), fields(namespace = ?namespace, view = %name, warehouse = %self.warehouse))]
    pub async fn load_view_sql(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
    ) -> sqe_core::Result<Option<String>> {
        self.require_rest_backend("load_view_sql")?;
        let ns_str = namespace
            .as_ref()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\u{1F}");
        let url = format!("{}/namespaces/{}/views/{}", self.rest_prefix(), ns_str, name);

        let mut req = self
            .http_client
            .get(&url)
            .bearer_auth(&self.bearer_token)
            .header("X-Request-ID", Uuid::new_v4().to_string());
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to load view: {e}")))?;

        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(SqeError::Catalog(format!("Rate limited by Polaris catalog: {text}")));
            }
            if status == reqwest::StatusCode::CONFLICT {
                return Err(SqeError::Execution(format!("Catalog commit conflict: {text}")));
            }
            return Err(SqeError::Catalog(format!(
                "Failed to load view '{name}' (HTTP {status}): {text}"
            )));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to parse view response: {e}")))?;

        // Iceberg REST view response: metadata.versions[last].representations[type=sql].sql
        let sql = body["metadata"]["versions"]
            .as_array()
            .and_then(|v| v.last())
            .and_then(|v| v["representations"].as_array())
            .and_then(|r| r.iter().find(|rep| rep["type"] == "sql"))
            .and_then(|rep| rep["sql"].as_str())
            .map(String::from);

        Ok(sql)
    }

    /// Drop a view via the Polaris REST API.
    ///
    /// Calls `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}`.
    #[instrument(skip(self), fields(namespace = ?namespace, view = %name, warehouse = %self.warehouse))]
    pub async fn drop_view(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
    ) -> sqe_core::Result<()> {
        self.require_rest_backend("drop_view")?;
        let ns_str = namespace
            .as_ref()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\u{1F}");
        let url = format!(
            "{}/namespaces/{}/views/{}",
            self.rest_prefix(),
            ns_str,
            name
        );

        debug!(url = %url, view = name, "Dropping view via Polaris REST");

        let mut req = self
            .http_client
            .delete(&url)
            .bearer_auth(&self.bearer_token)
            .header("X-Request-ID", Uuid::new_v4().to_string());
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to drop view: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(SqeError::Catalog(format!("Rate limited by Polaris catalog: {text}")));
            }
            if status == reqwest::StatusCode::CONFLICT {
                return Err(SqeError::Execution(format!("Catalog commit conflict: {text}")));
            }
            return Err(SqeError::Catalog(format!(
                "Failed to drop view (HTTP {status}): {text}"
            )));
        }

        info!(view = name, namespace = ?namespace, "View dropped");
        Ok(())
    }

    /// Commit a schema update to a table via the Polaris REST API.
    ///
    /// Sends `POST /v1/{warehouse}/namespaces/{namespace}/tables/{table}` with
    /// the provided `TableRequirement` list and `TableUpdate` list. This is the
    /// Iceberg REST Catalog table-commit endpoint used for schema evolution
    /// (ADD/DROP/RENAME/ALTER COLUMN).
    ///
    /// `TableUpdate` and `TableRequirement` are serialized directly; we build the
    /// JSON payload ourselves rather than going through `TableCommit::builder()`,
    /// whose `build()` method is crate-private in the upstream iceberg crate.
    #[instrument(skip(self, updates, requirements), fields(table = %table_ident, warehouse = %self.warehouse))]
    pub async fn commit_schema_update(
        &self,
        table_ident: &TableIdent,
        updates: Vec<TableUpdate>,
        requirements: Vec<TableRequirement>,
    ) -> sqe_core::Result<()> {
        self.require_rest_backend("commit_schema_update")?;
        let ns_str = table_ident
            .namespace()
            .as_ref()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\u{1F}");

        let url = format!(
            "{}/namespaces/{}/tables/{}",
            self.rest_prefix(),
            ns_str,
            table_ident.name()
        );

        let body = serde_json::json!({
            "identifier": {
                "namespace": table_ident.namespace().as_ref(),
                "name": table_ident.name(),
            },
            "requirements": serde_json::to_value(&requirements)
                .map_err(|e| SqeError::Execution(format!("Failed to serialize requirements: {e}")))?,
            "updates": serde_json::to_value(&updates)
                .map_err(|e| SqeError::Execution(format!("Failed to serialize updates: {e}")))?,
        });

        debug!(url = %url, table = %table_ident, "Committing schema update via Polaris REST");

        let mut req = self
            .http_client
            .post(&url)
            .bearer_auth(&self.bearer_token)
            .header("X-Request-ID", Uuid::new_v4().to_string())
            .json(&body);
        for (k, v) in trace_context_http_headers() {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SqeError::Catalog(format!("Failed to commit schema update: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(SqeError::Catalog(format!("Rate limited by Polaris catalog: {text}")));
            }
            if status == reqwest::StatusCode::CONFLICT {
                return Err(SqeError::Execution(format!("Catalog commit conflict: {text}")));
            }
            return Err(SqeError::Catalog(format!(
                "Failed to commit schema update for '{table_ident}' (HTTP {status}): {text}"
            )));
        }

        info!(table = %table_ident, "Schema update committed");
        // Invalidate cache so the next load_table() fetches the updated metadata.
        self.invalidate_table(table_ident).await;
        Ok(())
    }

    /// Return a bridge that implements iceberg's `Catalog` trait.
    /// This is useful for passing to iceberg-datafusion providers.
    pub fn as_catalog(self: &Arc<Self>) -> Arc<SessionCatalogBridge> {
        Arc::new(SessionCatalogBridge {
            session: self.clone(),
        })
    }
}

/// Bridge type that implements iceberg's `Catalog` trait by delegating
/// to our `SessionCatalog`. This is needed because `SessionCatalog` wraps
/// the inner `RestCatalog` in `CatalogHandle::Rest(Arc<RestCatalog>)`
/// (previously `Arc<RwLock<RestCatalog>>`, removed in issue #18) and we
/// need a plain `Catalog` trait object for the iceberg-datafusion
/// providers.
#[derive(Debug)]
pub struct SessionCatalogBridge {
    session: Arc<SessionCatalog>,
}

#[async_trait::async_trait]
impl Catalog for SessionCatalogBridge {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        dispatch_catalog!(self.session.inner, list_namespaces(parent))
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        dispatch_catalog!(self.session.inner, create_namespace(namespace, properties))
    }

    async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        dispatch_catalog!(self.session.inner, get_namespace(namespace))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        dispatch_catalog!(self.session.inner, namespace_exists(namespace))
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        dispatch_catalog!(self.session.inner, update_namespace(namespace, properties))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        dispatch_catalog!(self.session.inner, drop_namespace(namespace))
    }

    async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<Vec<TableIdent>> {
        dispatch_catalog!(self.session.inner, list_tables(namespace))
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: iceberg::TableCreation,
    ) -> iceberg::Result<Table> {
        let table_name = creation.name.clone();
        let result = dispatch_catalog!(self.session.inner, create_table(namespace, creation))?;
        // Invalidate any stale cache entry for this table name.
        let ident = TableIdent::new(namespace.clone(), table_name);
        self.session.table_cache.invalidate(&self.session.table_cache_key(&ident)).await;
        Ok(result)
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        // Delegate through SessionCatalog::load_table so the cache is used.
        self.session
            .load_table(table)
            .await
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let result = dispatch_catalog!(self.session.inner, drop_table(table));
        // Invalidate on success or failure: stale data is worse than a miss.
        self.session
            .table_cache
            .invalidate(&self.session.table_cache_key(table))
            .await;
        result
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        dispatch_catalog!(self.session.inner, table_exists(table))
    }

    async fn rename_table(
        &self,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> iceberg::Result<()> {
        dispatch_catalog!(self.session.inner, rename_table(src, dest))
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        dispatch_catalog!(self.session.inner, register_table(table, metadata_location))
    }

    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        let ident = commit.identifier().clone();
        let result = dispatch_catalog!(self.session.inner, update_table(commit));
        // Invalidate cache so any subsequent load_table sees updated metadata.
        self.session
            .table_cache
            .invalidate(&self.session.table_cache_key(&ident))
            .await;
        result
    }
}
