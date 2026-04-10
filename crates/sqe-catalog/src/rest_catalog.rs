use std::collections::HashMap;
use std::sync::Arc;

use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use moka::future::Cache as MokaCache;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};
use uuid::Uuid;

use sqe_core::config::StorageConfig;
use sqe_core::SqeError;
use sqe_metrics::propagation::trace_context_http_headers;

use crate::circuit_breaker::CircuitBreaker;

/// Global table metadata cache shared across all sessions.
///
/// Table metadata (schema, partitions, snapshots) is user-independent — the same
/// table has the same structure regardless of which user queries it. User-level
/// authorization is enforced by Polaris at load time; we cache the result here
/// so subsequent queries within the TTL window skip the REST round-trip entirely.
///
/// The cache is created once at coordinator startup and passed into every
/// `SessionCatalog` via [`SessionCatalog::new`]. Each session falls through to
/// Polaris on a miss and populates the shared cache on success.
///
/// Use [`TableMetadataCache::invalidate`] after any DDL/DML that changes table
/// structure (DROP TABLE, ALTER TABLE, INSERT, MERGE, DELETE).
#[derive(Clone)]
pub struct TableMetadataCache {
    inner: MokaCache<String, Table>,
}

impl TableMetadataCache {
    /// Create a global table metadata cache with the given TTL.
    ///
    /// Pass `ttl_secs = 0` to disable the cache (entries are never stored).
    pub fn new(ttl_secs: u64) -> Self {
        let inner = if ttl_secs == 0 {
            MokaCache::builder().max_capacity(0).build()
        } else {
            MokaCache::builder()
                .max_capacity(1000)
                .time_to_live(std::time::Duration::from_secs(ttl_secs))
                .build()
        };
        Self { inner }
    }

    pub async fn get(&self, key: &str) -> Option<Table> {
        self.inner.get(key).await
    }

    pub async fn insert(&self, key: String, table: Table) {
        self.inner.insert(key, table).await;
    }

    pub async fn invalidate(&self, key: &str) {
        self.inner.invalidate(key).await;
    }

    /// Number of entries currently held in the cache.
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

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
    inner: Arc<RwLock<RestCatalog>>,
    polaris_url: String,
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
    /// Cache is keyed by `"namespace.table_name"`. TTL and capacity are configured
    /// when the global cache is created (see `TableMetadataCache::new`).
    table_cache: TableMetadataCache,
}

impl std::fmt::Debug for SessionCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCatalog")
            .field("polaris_url", &self.polaris_url)
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
    pub async fn new(
        polaris_url: &str,
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
            polaris_url = polaris_url,
            warehouse = warehouse,
            token_fingerprint = %token_fingerprint,
            "Creating per-session REST catalog"
        );

        let mut props = HashMap::new();
        // Set the bearer token; iceberg-rust's RestCatalog reads the "token" prop
        // and uses it in the Authorization: Bearer header.
        props.insert("token".to_string(), bearer_token.to_string());

        // Set the REST catalog URI and warehouse
        props.insert("uri".to_string(), polaris_url.to_string());
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
                storage_config.s3_secret_key.clone(),
            );
        }
        if storage_config.s3_path_style {
            props.insert("s3.path-style-access".to_string(), "true".to_string());
        }

        // RisingWave fork uses CatalogBuilder::load(name, props) pattern.
        // Storage factory (OpenDAL S3) is configured automatically from the s3.*
        // properties in the props HashMap — no explicit with_storage_factory() needed.
        //
        // Cache RestCatalog instances by token fingerprint to avoid the expensive
        // (~250ms) per-query creation cost. The catalog is safe to reuse because
        // iceberg-rust's RestCatalog is stateless (each loadTable call goes to Polaris).
        static REST_CATALOG_CACHE: std::sync::LazyLock<
            moka::future::Cache<String, Arc<RwLock<RestCatalog>>>
        > = std::sync::LazyLock::new(|| {
            moka::future::Cache::builder()
                .max_capacity(100)
                .time_to_live(std::time::Duration::from_secs(300)) // 5 min
                .build()
        });

        let catalog_key = format!("{}-{}", polaris_url, token_fingerprint);
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
            let arc_catalog = Arc::new(RwLock::new(catalog));
            REST_CATALOG_CACHE.insert(catalog_key, arc_catalog.clone()).await;
            arc_catalog
        };

        let http_client = http_client.unwrap_or_default();
        let circuit_breaker = circuit_breaker.unwrap_or_else(|| {
            Arc::new(CircuitBreaker::new(
                "polaris-rest",
                5,
                std::time::Duration::from_secs(30),
            ))
        });

        // Use the shared global cache when provided; fall back to a private
        // per-session cache (disabled — max_capacity 0) so that call sites that
        // pass `None` (tests, one-shot DDL helpers) don't pollute a global state.
        let table_cache = table_cache.unwrap_or_else(|| TableMetadataCache::new(0));

        Ok(Self {
            inner,
            polaris_url: polaris_url.to_string(),
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

    /// Returns the Polaris URL.
    pub fn polaris_url(&self) -> &str {
        &self.polaris_url
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
        let catalog = self.inner.read().await;
        let result = catalog
            .list_namespaces(None)
            .await
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to list namespaces: {e}")));
        match &result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(_) => self.circuit_breaker.record_failure(),
        }
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
        let catalog = self.inner.read().await;
        catalog
            .get_namespace(namespace)
            .await
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
        let catalog = self.inner.read().await;
        let result = catalog
            .list_tables(namespace)
            .await
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to list tables: {e}")));
        match &result {
            Ok(_) => self.circuit_breaker.record_success(),
            Err(_) => self.circuit_breaker.record_failure(),
        }
        result
    }

    /// Load a table by its identifier.
    ///
    /// The returned `Table` includes metadata and a FileIO configured with
    /// vended credentials (if Polaris provides them) or static S3 config.
    ///
    /// Results are cached in the shared global `TableMetadataCache` (passed at construction
    /// time). Use [`invalidate_table`] to evict an entry after DDL/DML.
    #[instrument(skip(self), fields(table = %table_ident, warehouse = %self.warehouse))]
    pub async fn load_table(&self, table_ident: &TableIdent) -> sqe_core::Result<Table> {
        let cache_key = format!("{}.{}", table_ident.namespace(), table_ident.name());

        // Fast path: return cached table without touching Polaris.
        if let Some(cached) = self.table_cache.get(&cache_key).await {
            debug!(
                token_fingerprint = %self.token_fingerprint,
                table = ?table_ident,
                "Table cache hit"
            );
            return Ok(cached);
        }

        debug!(
            token_fingerprint = %self.token_fingerprint,
            table = ?table_ident,
            "Loading table (cache miss)"
        );
        self.circuit_breaker
            .check()
            .map_err(sqe_core::SqeError::Catalog)?;
        let catalog = self.inner.read().await;
        let result = catalog
            .load_table(table_ident)
            .await
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to load table: {e}")));
        match &result {
            Ok(table) => {
                self.circuit_breaker.record_success();
                self.table_cache.insert(cache_key, table.clone()).await;
            }
            Err(_) => self.circuit_breaker.record_failure(),
        }
        result
    }

    /// Evict a table from the metadata cache.
    ///
    /// Call this after any DDL/DML operation that changes the table's metadata
    /// (DROP TABLE, CREATE TABLE, ALTER TABLE, INSERT, MERGE, DELETE) so that
    /// the next `load_table()` fetches fresh metadata from Polaris.
    pub async fn invalidate_table(&self, table_ident: &TableIdent) {
        let key = format!("{}.{}", table_ident.namespace(), table_ident.name());
        self.table_cache.invalidate(&key).await;
        debug!(table = %table_ident, "Table cache invalidated");
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, table_ident: &TableIdent) -> sqe_core::Result<bool> {
        let catalog = self.inner.read().await;
        catalog
            .table_exists(table_ident)
            .await
            .map_err(|e| {
                sqe_core::SqeError::Catalog(format!("Failed to check table existence: {e}"))
            })
    }

    /// Build the Polaris REST URL prefix for this warehouse.
    fn rest_prefix(&self) -> String {
        let base = self.polaris_url.trim_end_matches('/');
        format!("{base}/v1/{}", self.warehouse)
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
/// `RestCatalog` behind an `RwLock` and we need the `Catalog` trait for
/// the iceberg-datafusion providers.
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
        let catalog = self.session.inner.read().await;
        catalog.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        let catalog = self.session.inner.read().await;
        catalog.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        let catalog = self.session.inner.read().await;
        catalog.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        let catalog = self.session.inner.read().await;
        catalog.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        let catalog = self.session.inner.read().await;
        catalog.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        let catalog = self.session.inner.read().await;
        catalog.drop_namespace(namespace).await
    }

    async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<Vec<TableIdent>> {
        let catalog = self.session.inner.read().await;
        catalog.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: iceberg::TableCreation,
    ) -> iceberg::Result<Table> {
        let table_name = creation.name.clone();
        let catalog = self.session.inner.read().await;
        let result = catalog.create_table(namespace, creation).await?;
        // Invalidate any stale cache entry for this table name.
        let ident = TableIdent::new(namespace.clone(), table_name);
        self.session.table_cache.invalidate(&format!("{}.{}", ident.namespace(), ident.name())).await;
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
        let catalog = self.session.inner.read().await;
        let result = catalog.drop_table(table).await;
        // Invalidate on success or failure — stale data is worse than a miss.
        drop(catalog);
        self.session.table_cache.invalidate(&format!("{}.{}", table.namespace(), table.name())).await;
        result
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        let catalog = self.session.inner.read().await;
        catalog.table_exists(table).await
    }

    async fn rename_table(
        &self,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> iceberg::Result<()> {
        let catalog = self.session.inner.read().await;
        catalog.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        let catalog = self.session.inner.read().await;
        catalog.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        let ident = commit.identifier().clone();
        let catalog = self.session.inner.read().await;
        let result = catalog.update_table(commit).await;
        drop(catalog);
        // Invalidate cache so any subsequent load_table sees updated metadata.
        self.session.table_cache.invalidate(&format!("{}.{}", ident.namespace(), ident.name())).await;
        result
    }
}
