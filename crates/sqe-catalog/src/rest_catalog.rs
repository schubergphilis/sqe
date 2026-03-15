use std::collections::HashMap;
use std::sync::Arc;

use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use tokio::sync::RwLock;
use tracing::{debug, info};

use sqe_core::config::StorageConfig;
use sqe_core::SqeError;

/// Per-session Iceberg REST catalog wrapper.
///
/// Each authenticated user session gets its own `SessionCatalog` instance
/// configured with the user's bearer token. The token is passed directly to
/// the Polaris REST catalog so that table-level authorization is enforced by
/// the catalog server.
pub struct SessionCatalog {
    inner: Arc<RwLock<RestCatalog>>,
    polaris_url: String,
    warehouse: String,
    bearer_token: String,
    token_fingerprint: String,
    storage_config: StorageConfig,
    http_client: reqwest::Client,
}

impl std::fmt::Debug for SessionCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCatalog")
            .field("polaris_url", &self.polaris_url)
            .field("warehouse", &self.warehouse)
            .field("token_fingerprint", &self.token_fingerprint)
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
    pub async fn new(
        polaris_url: &str,
        warehouse: &str,
        bearer_token: &str,
        storage_config: &StorageConfig,
    ) -> sqe_core::Result<Self> {
        let token_fingerprint = {
            let len = bearer_token.len();
            let tail = &bearer_token[len.saturating_sub(8)..];
            tail.to_string()
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

        let config = RestCatalogConfig::builder()
            .uri(polaris_url.to_string())
            .warehouse(warehouse.to_string())
            .props(props)
            .build();

        let catalog = RestCatalog::new(config);

        let http_client = reqwest::Client::new();

        Ok(Self {
            inner: Arc::new(RwLock::new(catalog)),
            polaris_url: polaris_url.to_string(),
            warehouse: warehouse.to_string(),
            bearer_token: bearer_token.to_string(),
            token_fingerprint,
            storage_config: storage_config.clone(),
            http_client,
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
    pub async fn list_namespaces(&self) -> sqe_core::Result<Vec<NamespaceIdent>> {
        debug!(token_fingerprint = %self.token_fingerprint, "Listing namespaces");
        let catalog = self.inner.read().await;
        catalog
            .list_namespaces(None)
            .await
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to list namespaces: {e}")))
    }

    /// List all tables in the given namespace.
    pub async fn list_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> sqe_core::Result<Vec<TableIdent>> {
        debug!(
            token_fingerprint = %self.token_fingerprint,
            namespace = ?namespace,
            "Listing tables"
        );
        let catalog = self.inner.read().await;
        catalog
            .list_tables(namespace)
            .await
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to list tables: {e}")))
    }

    /// Load a table by its identifier.
    ///
    /// The returned `Table` includes metadata and a FileIO configured with
    /// vended credentials (if Polaris provides them) or static S3 config.
    pub async fn load_table(&self, table_ident: &TableIdent) -> sqe_core::Result<Table> {
        debug!(
            token_fingerprint = %self.token_fingerprint,
            table = ?table_ident,
            "Loading table"
        );
        let catalog = self.inner.read().await;
        catalog
            .load_table(table_ident)
            .await
            .map_err(|e| sqe_core::SqeError::Catalog(format!("Failed to load table: {e}")))
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

        let resp = self
            .http_client
            .post(&url)
            .bearer_auth(&self.bearer_token)
            .json(&body)
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

    /// Drop a view via the Polaris REST API.
    ///
    /// Calls `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}`.
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

        let resp = self
            .http_client
            .delete(&url)
            .bearer_auth(&self.bearer_token)
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
        let catalog = self.session.inner.read().await;
        catalog.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let catalog = self.session.inner.read().await;
        catalog.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        let catalog = self.session.inner.read().await;
        catalog.drop_table(table).await
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

    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        let catalog = self.session.inner.read().await;
        catalog.update_table(commit).await
    }
}
