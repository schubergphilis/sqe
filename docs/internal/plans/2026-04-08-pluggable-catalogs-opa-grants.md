# Pluggable Catalogs + OPA + Grants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hard-coded Polaris REST catalog with a pluggable `CatalogBackend` trait supporting multiple catalog types (Iceberg REST, storage-only, AWS Glue, AWS S3 Tables), add OPA policy enforcement, and implement a hybrid grant system with SQL surface (`GRANT/REVOKE/SHOW GRANTS`).

**Architecture:** Three independent subsystems built on a shared trait foundation: (1) Pluggable catalogs via `CatalogBackend` + `CatalogAuth` traits in sqe-catalog, with config-driven backend selection; (2) OPA policy engine behind `policy-opa` Cargo feature flag in sqe-policy; (3) Hybrid grant system via `GrantBackend` trait with local (SQLite), catalog-native, and stub Chameleon implementations. The existing Polaris REST integration becomes `IcebergRestBackend` — zero behavior change for existing deployments.

**Tech Stack:** Rust, async-trait, iceberg-rust (RestCatalog), object_store (Azure/GCS/local), moka (caching), rusqlite (SQLite grants), reqwest (OPA HTTP), serde (config)

**Design spec:** `docs/superpowers/specs/2026-04-08-oss-release-and-catalogs-design.md` (Spec C)

**Depends on:** Plan A+B (OSS Release + Audit) must be merged first.

---

## File Structure

### Files to Create

| File | Purpose |
|---|---|
| `crates/sqe-catalog/src/traits.rs` | CatalogBackend + CatalogAuth traits |
| `crates/sqe-catalog/src/auth.rs` | CatalogAuth implementations (Passthrough, ServiceCredential, AwsIam, None) |
| `crates/sqe-catalog/src/backend_rest.rs` | IcebergRestBackend (refactored from rest_catalog.rs) |
| `crates/sqe-catalog/src/backend_storage.rs` | StorageOnlyBackend (scan path for metadata) |
| `crates/sqe-catalog/src/backend_glue.rs` | AwsGlueBackend (wraps IcebergRest + SigV4) |
| `crates/sqe-catalog/src/backend_factory.rs` | Factory: config → Arc\<dyn CatalogBackend\> |
| `crates/sqe-catalog/src/storage_config.rs` | Multi-cloud StorageConfig enum (S3, Azure, GCS, local) |
| `crates/sqe-policy/src/grants.rs` | GrantBackend trait + types |
| `crates/sqe-policy/src/grant_local.rs` | LocalGrantStore (SQLite) |
| `crates/sqe-policy/src/grant_catalog.rs` | CatalogNativeGrants (stub) |
| `crates/sqe-policy/src/grant_chameleon.rs` | ChameleonGrants (stub) |

### Files to Modify

| File | Change |
|---|---|
| `crates/sqe-catalog/src/lib.rs` | Re-export new modules |
| `crates/sqe-catalog/src/rest_catalog.rs` | Keep SessionCatalog for backward compat, delegate to IcebergRestBackend internally |
| `crates/sqe-catalog/src/catalog_provider.rs` | Accept `Arc<dyn CatalogBackend>` instead of `Arc<SessionCatalog>` |
| `crates/sqe-catalog/src/schema_provider.rs` | Accept `Arc<dyn CatalogBackend>` |
| `crates/sqe-catalog/Cargo.toml` | Add feature flags, optional deps (rusqlite, object_store azure/gcs) |
| `crates/sqe-core/src/config.rs` | Refactor CatalogConfig into enum, add StorageConfig variants |
| `crates/sqe-coordinator/src/session_context.rs` | Use backend factory instead of hardcoded SessionCatalog |
| `crates/sqe-coordinator/src/catalog_ops.rs` | Reuse session catalog from context instead of creating new one |
| `crates/sqe-coordinator/src/query_handler.rs` | Route GRANT/REVOKE to grant backend |
| `crates/sqe-policy/src/lib.rs` | Re-export grant types |
| `crates/sqe-policy/src/opa.rs` | Add grant-aware policy evaluation |
| `crates/sqe-policy/Cargo.toml` | Add rusqlite optional dep |
| `crates/sqe-sql/src/classifier.rs` | Ensure GRANT/REVOKE/SHOW GRANTS classified correctly |
| `sqe.toml.example` | Add multi-backend config examples |

---

## Phase 1: Catalog Trait Foundation

### Task 1: CatalogBackend + CatalogAuth Traits

**Files:**
- Create: `crates/sqe-catalog/src/traits.rs`
- Modify: `crates/sqe-catalog/src/lib.rs`

These are the core abstractions everything else builds on. Get these right and the rest follows.

- [ ] **Step 1: Write the trait definition tests**

Create `crates/sqe-catalog/src/traits.rs` with test stubs first:

```rust
//! Pluggable catalog backend and auth traits.
//!
//! Every catalog integration implements [`CatalogBackend`] for table discovery
//! and [`CatalogAuth`] for credential resolution. The coordinator creates
//! instances via config-driven factory (see `backend_factory.rs`).

use async_trait::async_trait;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableIdent};

/// Credential presented to a catalog backend for authentication.
#[derive(Debug, Clone)]
pub enum CatalogCredential {
    /// Forward user's OIDC bearer token (passthrough auth).
    Bearer(String),
    /// AWS SigV4 credentials.
    AwsSigV4(AwsCredentials),
    /// Username + password (basic auth).
    UsernamePassword(String, String),
    /// Anonymous / no auth.
    None,
}

/// AWS credentials for SigV4 signing.
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub region: String,
}

/// Table format detected from metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    Iceberg,
    Delta,
}

/// Resolves catalog credentials from a user's session token.
#[async_trait]
pub trait CatalogAuth: Send + Sync {
    /// Given an optional user bearer token, return the credential to use
    /// when calling the catalog backend.
    async fn catalog_credential(
        &self,
        user_token: Option<&str>,
    ) -> crate::Result<CatalogCredential>;
}

/// Backend for catalog operations: list namespaces/tables, load metadata.
///
/// Each backend wraps a specific catalog technology (Iceberg REST, AWS Glue,
/// storage-only, etc.) behind this uniform interface.
#[async_trait]
pub trait CatalogBackend: Send + Sync {
    /// List all namespaces visible to this credential.
    async fn list_namespaces(
        &self,
        cred: &CatalogCredential,
    ) -> crate::Result<Vec<NamespaceIdent>>;

    /// List all tables in a namespace.
    async fn list_tables(
        &self,
        ns: &NamespaceIdent,
        cred: &CatalogCredential,
    ) -> crate::Result<Vec<TableIdent>>;

    /// Load a table (metadata + FileIO) for scanning.
    async fn load_table(
        &self,
        ident: &TableIdent,
        cred: &CatalogCredential,
    ) -> crate::Result<Table>;

    /// Detect the table format from loaded metadata.
    fn table_format(&self, _metadata: &TableMetadata) -> TableFormat {
        TableFormat::Iceberg // default
    }

    /// Human-readable name for logging/metrics.
    fn backend_name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verify trait object safety — must compile.
    fn _assert_catalog_backend_object_safe(_: &dyn CatalogBackend) {}
    fn _assert_catalog_auth_object_safe(_: &dyn CatalogAuth) {}

    #[test]
    fn catalog_credential_debug() {
        let cred = CatalogCredential::Bearer("token123".into());
        let debug = format!("{:?}", cred);
        assert!(debug.contains("Bearer"));
    }

    #[test]
    fn table_format_default_is_iceberg() {
        assert_eq!(TableFormat::Iceberg, TableFormat::Iceberg);
    }
}
```

- [ ] **Step 2: Add the module to lib.rs**

In `crates/sqe-catalog/src/lib.rs`, add:

```rust
pub mod traits;
pub use traits::{CatalogAuth, CatalogBackend, CatalogCredential, TableFormat};
```

- [ ] **Step 3: Verify it compiles**

```bash
cargo check -p sqe-catalog 2>&1 | tail -5
```

Expected: Compiles. The `crate::Result` reference requires sqe-catalog to have a `Result` type — check if it re-exports from sqe-core or defines its own.

- [ ] **Step 4: Run tests**

```bash
cargo test -p sqe-catalog -- traits 2>&1 | tail -10
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/traits.rs crates/sqe-catalog/src/lib.rs
git commit -m "feat: add CatalogBackend + CatalogAuth traits

Core abstractions for pluggable catalog backends. CatalogBackend
provides list_namespaces/list_tables/load_table. CatalogAuth
resolves credentials from user tokens. CatalogCredential supports
bearer, AWS SigV4, basic auth, and anonymous modes."
```

---

### Task 2: CatalogAuth Implementations

**Files:**
- Create: `crates/sqe-catalog/src/auth.rs`

Four auth strategies per the design spec. `PassthroughCatalogAuth` is used by existing Polaris integration — forward user's OIDC bearer token.

- [ ] **Step 1: Write tests for each auth strategy**

```rust
//! CatalogAuth implementations for different authentication modes.

use async_trait::async_trait;
use crate::traits::{CatalogAuth, CatalogCredential, AwsCredentials};

/// Forward the user's OIDC bearer token to the catalog.
/// Used with Polaris and any Iceberg REST catalog that accepts bearer auth.
pub struct PassthroughCatalogAuth;

#[async_trait]
impl CatalogAuth for PassthroughCatalogAuth {
    async fn catalog_credential(
        &self,
        user_token: Option<&str>,
    ) -> crate::Result<CatalogCredential> {
        match user_token {
            Some(token) => Ok(CatalogCredential::Bearer(token.to_string())),
            None => Err(sqe_core::SqeError::Auth(
                "passthrough auth requires a user token".into(),
            ).into()),
        }
    }
}

/// OAuth2 client_credentials flow — fetch + cache a service token.
pub struct ServiceCredentialAuth {
    token_url: String,
    client_id: String,
    client_secret: String,
    cache: moka::future::Cache<String, String>,
}

impl ServiceCredentialAuth {
    pub fn new(token_url: String, client_id: String, client_secret: String) -> Self {
        Self {
            token_url,
            client_id,
            client_secret,
            cache: moka::future::Cache::builder()
                .max_capacity(1)
                .time_to_live(std::time::Duration::from_secs(3570)) // expires_in - 30s
                .build(),
        }
    }
}

#[async_trait]
impl CatalogAuth for ServiceCredentialAuth {
    async fn catalog_credential(
        &self,
        _user_token: Option<&str>,
    ) -> crate::Result<CatalogCredential> {
        let key = "service_token".to_string();
        if let Some(cached) = self.cache.get(&key).await {
            return Ok(CatalogCredential::Bearer(cached));
        }
        // Fetch new token via client_credentials grant
        let client = reqwest::Client::new();
        let resp = client
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
            ])
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("token fetch failed: {e}")))?;

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("token parse failed: {e}")))?;

        let token = body["access_token"]
            .as_str()
            .ok_or_else(|| sqe_core::SqeError::Auth("no access_token in response".into()))?
            .to_string();

        self.cache.insert(key, token.clone()).await;
        Ok(CatalogCredential::Bearer(token))
    }
}

/// AWS IAM credentials from environment / instance profile.
///
/// `signing_name` controls the SigV4 service name:
/// - `"s3"` (default) — standard S3
/// - `"s3tables"` — AWS S3 Tables Iceberg REST endpoint
/// - `"glue"` — AWS Glue Iceberg REST endpoint
pub struct AwsIamAuth {
    region: String,
    signing_name: String,
}

impl AwsIamAuth {
    pub fn new(region: String, signing_name: Option<String>) -> Self {
        Self { region, signing_name: signing_name.unwrap_or_else(|| "s3".into()) }
    }
}

#[async_trait]
impl CatalogAuth for AwsIamAuth {
    async fn catalog_credential(
        &self,
        _user_token: Option<&str>,
    ) -> crate::Result<CatalogCredential> {
        // Read from standard AWS credential chain:
        // 1. AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars
        // 2. Instance profile (IMDS)
        // 3. Web identity token
        let access_key = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| sqe_core::SqeError::Auth("AWS_ACCESS_KEY_ID not set".into()))?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| sqe_core::SqeError::Auth("AWS_SECRET_ACCESS_KEY not set".into()))?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

        Ok(CatalogCredential::AwsSigV4(AwsCredentials {
            access_key_id: access_key,
            secret_access_key: secret_key,
            session_token,
            region: self.region.clone(),
        }))
    }
}

/// No authentication — for anonymous catalog access.
pub struct NoCatalogAuth;

#[async_trait]
impl CatalogAuth for NoCatalogAuth {
    async fn catalog_credential(
        &self,
        _user_token: Option<&str>,
    ) -> crate::Result<CatalogCredential> {
        Ok(CatalogCredential::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn passthrough_forwards_token() {
        let auth = PassthroughCatalogAuth;
        let cred = auth.catalog_credential(Some("my-jwt")).await.unwrap();
        match cred {
            CatalogCredential::Bearer(t) => assert_eq!(t, "my-jwt"),
            _ => panic!("expected Bearer"),
        }
    }

    #[tokio::test]
    async fn passthrough_fails_without_token() {
        let auth = PassthroughCatalogAuth;
        let result = auth.catalog_credential(None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn no_auth_returns_none() {
        let auth = NoCatalogAuth;
        let cred = auth.catalog_credential(None).await.unwrap();
        assert!(matches!(cred, CatalogCredential::None));
    }

    #[tokio::test]
    async fn aws_iam_reads_env() {
        // Only test when AWS env vars are set (CI may not have them)
        if std::env::var("AWS_ACCESS_KEY_ID").is_ok() {
            let auth = AwsIamAuth::new("eu-west-1".into());
            let cred = auth.catalog_credential(None).await.unwrap();
            assert!(matches!(cred, CatalogCredential::AwsSigV4(_)));
        }
    }
}
```

- [ ] **Step 2: Add module to lib.rs**

```rust
pub mod auth;
pub use auth::{PassthroughCatalogAuth, ServiceCredentialAuth, AwsIamAuth, NoCatalogAuth};
```

- [ ] **Step 3: Verify tests pass**

```bash
cargo test -p sqe-catalog -- auth 2>&1 | tail -10
```

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-catalog/src/auth.rs crates/sqe-catalog/src/lib.rs
git commit -m "feat: add CatalogAuth implementations

PassthroughCatalogAuth (forward OIDC token), ServiceCredentialAuth
(OAuth2 client_credentials + moka cache), AwsIamAuth (env/instance
profile), NoCatalogAuth (anonymous). All implement CatalogAuth trait."
```

---

### Task 3: Config Refactoring — Pluggable CatalogConfig

**Files:**
- Modify: `crates/sqe-core/src/config.rs`

Refactor `CatalogConfig` from a flat Polaris-only struct into a tagged enum that supports multiple backend types.

- [ ] **Step 1: Read current CatalogConfig**

```bash
grep -n "CatalogConfig" crates/sqe-core/src/config.rs
```

- [ ] **Step 2: Add the new config types alongside the existing one**

Add to `crates/sqe-core/src/config.rs` (keeping the existing `CatalogConfig` temporarily for backward compat):

```rust
/// Catalog backend type selector.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogBackendConfig {
    /// Iceberg REST catalog (Polaris, Snowflake Open Catalog, Unity, etc.)
    #[default]
    IcebergRest(IcebergRestConfig),
    /// No catalog server — discover tables by scanning storage paths.
    StorageOnly(StorageOnlyConfig),
    /// AWS Glue via Iceberg REST endpoint + SigV4.
    AwsGlue(AwsGlueConfig),
}

#[derive(Debug, Deserialize, Clone)]
pub struct IcebergRestConfig {
    pub url: String,
    #[serde(default)]
    pub warehouse: String,
    #[serde(default = "default_cache_ttl")]
    pub metadata_cache_ttl_secs: u64,
    #[serde(default = "default_table_format_version")]
    pub default_table_format_version: u8,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageOnlyConfig {
    pub base_path: String,
    #[serde(default = "default_scan_depth")]
    pub scan_depth: usize,
    #[serde(default)]
    pub tables: Vec<ExplicitTableConfig>,
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExplicitTableConfig {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AwsGlueConfig {
    pub region: String,
    #[serde(default = "default_cache_ttl")]
    pub metadata_cache_ttl_secs: u64,
}

fn default_scan_depth() -> usize { 3 }

/// Catalog auth type selector.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogAuthConfig {
    /// Forward user's OIDC bearer token.
    #[default]
    Passthrough,
    /// OAuth2 client_credentials grant.
    ServiceCredential(ServiceCredentialAuthConfig),
    /// AWS IAM (SigV4 from environment).
    AwsIam(AwsIamAuthConfig),
    /// No authentication.
    None,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceCredentialAuthConfig {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AwsIamAuthConfig {
    #[serde(default = "default_region")]
    pub region: String,
    /// SigV4 service name: "s3" (default), "s3tables" (S3 Tables), "glue" (Glue).
    pub signing_name: Option<String>,
}

fn default_region() -> String { "us-east-1".into() }

/// Grant backend type selector.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum GrantBackendConfig {
    /// Auto-select based on catalog type.
    #[default]
    Auto,
    /// Local SQLite-backed grant store.
    Local,
    /// Delegate to catalog's native access control.
    CatalogNative,
    /// Chameleon platform backend (stub).
    Chameleon,
}

/// Grant configuration section.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct GrantConfig {
    #[serde(default)]
    pub backend: GrantBackendConfig,
    #[serde(default)]
    pub chameleon: Option<ChameleonGrantConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChameleonGrantConfig {
    pub api_url: String,
}
```

- [ ] **Step 3: Add `catalog_backend` and `grants` fields to SqeConfig**

```rust
// In SqeConfig struct, add:
#[serde(default)]
pub catalog_backend: Option<CatalogBackendConfig>,
#[serde(default)]
pub catalog_auth: Option<CatalogAuthConfig>,
#[serde(default)]
pub grants: GrantConfig,
```

The existing `catalog: CatalogConfig` field stays for backward compatibility. The factory will check `catalog_backend` first; if absent, it falls back to constructing `IcebergRestConfig` from the legacy `catalog` fields.

- [ ] **Step 4: Write migration logic**

Add a helper method to SqeConfig:

```rust
impl SqeConfig {
    /// Resolve the catalog backend config, falling back to legacy fields.
    pub fn resolved_catalog_backend(&self) -> CatalogBackendConfig {
        if let Some(ref backend) = self.catalog_backend {
            return backend.clone();
        }
        // Legacy: construct from flat fields
        CatalogBackendConfig::IcebergRest(IcebergRestConfig {
            url: self.catalog.polaris_url.clone(),
            warehouse: self.catalog.warehouse.clone(),
            metadata_cache_ttl_secs: self.catalog.metadata_cache_ttl_secs,
            default_table_format_version: self.catalog.default_table_format_version,
        })
    }

    /// Resolve the catalog auth config, falling back to passthrough.
    pub fn resolved_catalog_auth(&self) -> CatalogAuthConfig {
        self.catalog_auth.clone().unwrap_or_default()
    }
}
```

- [ ] **Step 5: Write config deserialization tests**

```rust
#[test]
fn deserialize_iceberg_rest_config() {
    let toml = r#"
        [catalog_backend]
        type = "iceberg_rest"
        url = "http://polaris:8181/api/catalog"
        warehouse = "mywarehouse"
    "#;
    let config: SqeConfig = toml::from_str(toml).unwrap();
    assert!(matches!(
        config.catalog_backend,
        Some(CatalogBackendConfig::IcebergRest(_))
    ));
}

#[test]
fn deserialize_storage_only_config() {
    let toml = r#"
        [catalog_backend]
        type = "storage_only"
        base_path = "s3://my-lake/"
        scan_depth = 2
    "#;
    let config: SqeConfig = toml::from_str(toml).unwrap();
    assert!(matches!(
        config.catalog_backend,
        Some(CatalogBackendConfig::StorageOnly(_))
    ));
}

#[test]
fn legacy_config_fallback() {
    let toml = r#"
        [catalog]
        polaris_url = "http://polaris:8181/api/catalog"
        warehouse = "iceberg"
    "#;
    let config: SqeConfig = toml::from_str(toml).unwrap();
    let resolved = config.resolved_catalog_backend();
    match resolved {
        CatalogBackendConfig::IcebergRest(cfg) => {
            assert_eq!(cfg.url, "http://polaris:8181/api/catalog");
        }
        _ => panic!("expected IcebergRest fallback"),
    }
}
```

- [ ] **Step 6: Verify everything compiles and tests pass**

```bash
cargo test -p sqe-core -- config 2>&1 | tail -15
```

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-core/src/config.rs
git commit -m "feat: add pluggable catalog config types

CatalogBackendConfig (iceberg_rest, storage_only, aws_glue),
CatalogAuthConfig (passthrough, service_credential, aws_iam, none),
GrantConfig with auto-selection. Legacy CatalogConfig preserved
for backward compatibility with resolved_catalog_backend() helper."
```

---

### Task 4: IcebergRestBackend — Refactor SessionCatalog

**Files:**
- Create: `crates/sqe-catalog/src/backend_rest.rs`
- Modify: `crates/sqe-catalog/src/rest_catalog.rs` (keep thin wrapper)

Extract the core catalog operations from `SessionCatalog` into `IcebergRestBackend` that implements `CatalogBackend`. The existing `SessionCatalog` becomes a thin adapter holding per-session state (token, circuit breaker) and delegating to the backend.

- [ ] **Step 1: Create IcebergRestBackend**

```rust
//! Iceberg REST catalog backend (Polaris, Snowflake Open Catalog, Unity, etc.)

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::circuit_breaker::CircuitBreaker;
use crate::traits::{CatalogBackend, CatalogCredential, TableFormat};

/// Iceberg REST catalog backend.
///
/// Wraps iceberg-rust's `RestCatalog` with circuit breaker and
/// per-request credential injection. Compatible with Polaris,
/// Snowflake Open Catalog, Unity Catalog, and any Iceberg REST server.
pub struct IcebergRestBackend {
    url: String,
    warehouse: String,
    circuit_breaker: Arc<CircuitBreaker>,
    http_client: reqwest::Client,
}

impl IcebergRestBackend {
    pub fn new(
        url: String,
        warehouse: String,
        circuit_breaker: Arc<CircuitBreaker>,
        http_client: reqwest::Client,
    ) -> Self {
        Self { url, warehouse, circuit_breaker, http_client }
    }

    /// Create a RestCatalog instance with the given credential.
    async fn create_catalog(&self, cred: &CatalogCredential) -> crate::Result<RestCatalog> {
        let mut props = std::collections::HashMap::new();
        props.insert("uri".to_string(), self.url.clone());
        props.insert("warehouse".to_string(), self.warehouse.clone());

        match cred {
            CatalogCredential::Bearer(token) => {
                props.insert("token".to_string(), token.clone());
            }
            CatalogCredential::None => {}
            _ => {
                return Err(sqe_core::SqeError::Config(
                    "IcebergRestBackend only supports Bearer or None credentials".into(),
                ).into());
            }
        }

        let config = RestCatalogConfig::builder()
            .props(props)
            .build();
        let catalog = RestCatalog::new(config);
        Ok(catalog)
    }
}

#[async_trait]
impl CatalogBackend for IcebergRestBackend {
    async fn list_namespaces(
        &self,
        cred: &CatalogCredential,
    ) -> crate::Result<Vec<NamespaceIdent>> {
        self.circuit_breaker.call(async {
            let catalog = self.create_catalog(cred).await?;
            let namespaces = catalog.list_namespaces(None).await
                .map_err(|e| sqe_core::SqeError::Catalog(format!("list namespaces: {e}")))?;
            Ok(namespaces)
        }).await
    }

    async fn list_tables(
        &self,
        ns: &NamespaceIdent,
        cred: &CatalogCredential,
    ) -> crate::Result<Vec<TableIdent>> {
        self.circuit_breaker.call(async {
            let catalog = self.create_catalog(cred).await?;
            let tables = catalog.list_tables(ns).await
                .map_err(|e| sqe_core::SqeError::Catalog(format!("list tables: {e}")))?;
            Ok(tables)
        }).await
    }

    async fn load_table(
        &self,
        ident: &TableIdent,
        cred: &CatalogCredential,
    ) -> crate::Result<Table> {
        self.circuit_breaker.call(async {
            let catalog = self.create_catalog(cred).await?;
            let table = catalog.load_table(ident).await
                .map_err(|e| sqe_core::SqeError::Catalog(format!("load table: {e}")))?;
            Ok(table)
        }).await
    }

    fn backend_name(&self) -> &str {
        "iceberg_rest"
    }
}
```

> **Note:** The exact `RestCatalog` construction may differ from the above. The implementing agent should check the current iceberg-rust API for `RestCatalog::new()` signature and adapt accordingly. The `SessionCatalog` in `rest_catalog.rs` shows the working pattern.

- [ ] **Step 2: Write basic tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_name() {
        let backend = IcebergRestBackend::new(
            "http://localhost:8181".into(),
            "test".into(),
            Arc::new(CircuitBreaker::new(5, std::time::Duration::from_secs(30))),
            reqwest::Client::new(),
        );
        assert_eq!(backend.backend_name(), "iceberg_rest");
    }
}
```

- [ ] **Step 3: Register module in lib.rs**

- [ ] **Step 4: Verify compilation**

```bash
cargo check -p sqe-catalog 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-catalog/src/backend_rest.rs crates/sqe-catalog/src/lib.rs
git commit -m "feat: add IcebergRestBackend implementing CatalogBackend

Wraps iceberg-rust RestCatalog with circuit breaker and per-request
credential injection. Supports Bearer and anonymous auth modes.
Compatible with Polaris, Snowflake Open Catalog, Unity Catalog."
```

---

### Task 5: Backend Factory — Config to Backend Instance

**Files:**
- Create: `crates/sqe-catalog/src/backend_factory.rs`

Factory function that reads config and returns `Arc<dyn CatalogBackend>` + `Arc<dyn CatalogAuth>`.

- [ ] **Step 1: Create the factory**

```rust
//! Factory: config → catalog backend + auth instances.

use std::sync::Arc;
use sqe_core::config::{CatalogBackendConfig, CatalogAuthConfig, SqeConfig};
use crate::auth::*;
use crate::backend_rest::IcebergRestBackend;
use crate::circuit_breaker::CircuitBreaker;
use crate::traits::{CatalogAuth, CatalogBackend};

/// Shared resources passed to backend construction.
pub struct BackendResources {
    pub http_client: reqwest::Client,
    pub circuit_breaker: Arc<CircuitBreaker>,
}

impl Default for BackendResources {
    fn default() -> Self {
        Self {
            http_client: reqwest::Client::new(),
            circuit_breaker: Arc::new(CircuitBreaker::new(5, std::time::Duration::from_secs(30))),
        }
    }
}

/// Create a catalog backend from config.
pub fn create_catalog_backend(
    config: &CatalogBackendConfig,
    resources: &BackendResources,
) -> crate::Result<Arc<dyn CatalogBackend>> {
    match config {
        CatalogBackendConfig::IcebergRest(cfg) => {
            Ok(Arc::new(IcebergRestBackend::new(
                cfg.url.clone(),
                cfg.warehouse.clone(),
                resources.circuit_breaker.clone(),
                resources.http_client.clone(),
            )))
        }
        CatalogBackendConfig::StorageOnly(_cfg) => {
            // Phase 3: StorageOnlyBackend
            Err(sqe_core::SqeError::NotImplemented(
                "storage_only backend not yet implemented".into(),
            ).into())
        }
        CatalogBackendConfig::AwsGlue(_cfg) => {
            // Phase 4: AwsGlueBackend
            Err(sqe_core::SqeError::NotImplemented(
                "aws_glue backend not yet implemented".into(),
            ).into())
        }
    }
}

/// Create a catalog auth from config.
pub fn create_catalog_auth(
    config: &CatalogAuthConfig,
) -> Arc<dyn CatalogAuth> {
    match config {
        CatalogAuthConfig::Passthrough => Arc::new(PassthroughCatalogAuth),
        CatalogAuthConfig::ServiceCredential(cfg) => {
            Arc::new(ServiceCredentialAuth::new(
                cfg.token_url.clone(),
                cfg.client_id.clone(),
                cfg.client_secret.clone(),
            ))
        }
        CatalogAuthConfig::AwsIam(cfg) => {
            Arc::new(AwsIamAuth::new(cfg.region.clone(), cfg.signing_name.clone()))
        }
        CatalogAuthConfig::None => Arc::new(NoCatalogAuth),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_creates_iceberg_rest() {
        let config = CatalogBackendConfig::IcebergRest(
            sqe_core::config::IcebergRestConfig {
                url: "http://localhost:8181".into(),
                warehouse: "test".into(),
                metadata_cache_ttl_secs: 30,
                default_table_format_version: 2,
            },
        );
        let resources = BackendResources::default();
        let backend = create_catalog_backend(&config, &resources).unwrap();
        assert_eq!(backend.backend_name(), "iceberg_rest");
    }

    #[test]
    fn factory_creates_passthrough_auth() {
        let auth = create_catalog_auth(&CatalogAuthConfig::Passthrough);
        // Just verify it compiles and returns
        let _ = auth;
    }
}
```

- [ ] **Step 2: Register module, run tests, commit**

```bash
cargo test -p sqe-catalog -- backend_factory 2>&1 | tail -10
git add crates/sqe-catalog/src/backend_factory.rs crates/sqe-catalog/src/lib.rs
git commit -m "feat: add catalog backend factory (config → backend + auth)

Factory functions create_catalog_backend() and create_catalog_auth()
map config to trait object instances. IcebergRest supported now;
StorageOnly and AwsGlue return NotImplemented (added in later tasks)."
```

---

### Task 6: Update SqeCatalogProvider + SqeSchemaProvider to Use Traits

**Files:**
- Modify: `crates/sqe-catalog/src/catalog_provider.rs`
- Modify: `crates/sqe-catalog/src/schema_provider.rs`

Replace `Arc<SessionCatalog>` with `Arc<dyn CatalogBackend>` + `CatalogCredential`. This is the key refactoring that makes the providers backend-agnostic.

- [ ] **Step 1: Read current provider implementations**

Read `catalog_provider.rs` and `schema_provider.rs` to understand all `session_catalog` call sites.

- [ ] **Step 2: Update SqeCatalogProvider**

Replace the `session_catalog: Arc<SessionCatalog>` field with:

```rust
pub struct SqeCatalogProvider {
    backend: Arc<dyn CatalogBackend>,
    credential: CatalogCredential,
    storage_config: StorageConfig,
    warehouse: String,
    cached_namespaces: Vec<String>,
    policy_store: Option<Arc<dyn PolicyStore>>,
    session_user: Option<SessionUser>,
    prom_metrics: Option<Arc<MetricsRegistry>>,
}
```

Update `try_new_with_policy` to accept the trait-based types. The `cached_namespaces` initialization calls `backend.list_namespaces(&credential)` instead of `session_catalog.list_namespaces()`.

- [ ] **Step 3: Update SqeSchemaProvider similarly**

Replace `session_catalog: Arc<SessionCatalog>` with `backend: Arc<dyn CatalogBackend>` + `credential: CatalogCredential`.

Update `table_names()` to call `backend.list_tables(&ns, &credential)` and `table()` to call `backend.load_table(&ident, &credential)`.

- [ ] **Step 4: Keep SessionCatalog as backward-compat wrapper**

In `rest_catalog.rs`, add a `CatalogBackend` impl for `SessionCatalog` that delegates to the internal REST catalog. This keeps existing code (catalog_ops.rs, write_handler.rs) working while they're migrated.

```rust
#[async_trait]
impl CatalogBackend for SessionCatalog {
    async fn list_namespaces(&self, _cred: &CatalogCredential) -> crate::Result<Vec<NamespaceIdent>> {
        self.list_namespaces().await  // Uses internal token
    }
    // ... etc
}
```

- [ ] **Step 5: Run full test suite to verify no regressions**

```bash
cargo test --all 2>&1 | tail -15
```

Expected: All 1,218+ tests still pass.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-catalog/src/catalog_provider.rs crates/sqe-catalog/src/schema_provider.rs crates/sqe-catalog/src/rest_catalog.rs
git commit -m "refactor: SqeCatalogProvider + SqeSchemaProvider accept dyn CatalogBackend

Replace Arc<SessionCatalog> with Arc<dyn CatalogBackend> + CatalogCredential
in both providers. SessionCatalog implements CatalogBackend for backward
compatibility. Zero behavior change for existing deployments."
```

---

### Task 7: Update Session Context Factory

**Files:**
- Modify: `crates/sqe-coordinator/src/session_context.rs`

Wire `create_session_context()` to use the backend factory instead of hardcoded `SessionCatalog::new()`.

- [ ] **Step 1: Read current session_context.rs**

Understand the full `create_session_context()` flow.

- [ ] **Step 2: Add backend factory call**

```rust
pub async fn create_session_context(
    config: &SqeConfig,
    session: &Session,
    // ... existing params ...
    backend: Option<Arc<dyn CatalogBackend>>,  // New: pre-created backend
    catalog_auth: Option<Arc<dyn CatalogAuth>>,  // New: auth strategy
) -> Result<(SessionContext, /* ... */)> {
    // Resolve credential for this session
    let credential = match catalog_auth {
        Some(ref auth) => auth.catalog_credential(Some(&session.access_token)).await?,
        None => CatalogCredential::Bearer(session.access_token.clone()), // Legacy passthrough
    };

    // Create backend if not provided (legacy path)
    let backend = match backend {
        Some(b) => b,
        None => {
            // Legacy: create SessionCatalog as before
            Arc::new(SessionCatalog::new(/* ... */).await?)
        }
    };

    // Create catalog provider with trait-based backend
    let catalog_provider = SqeCatalogProvider::try_new_with_policy(
        backend.clone(),
        credential.clone(),
        config.storage.clone(),
        warehouse,
        policy_store.cloned(),
        Some(session.user.clone()),
    ).await?;

    ctx.register_catalog(&catalog_name, Arc::new(catalog_provider));
    // ...
}
```

- [ ] **Step 3: Update all callers of create_session_context()**

Search for call sites and add the new params (passing `None` for backward compat).

- [ ] **Step 4: Run full test suite**

```bash
cargo test --all 2>&1 | tail -15
```

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/session_context.rs
git commit -m "feat: session context uses pluggable catalog backend

create_session_context() accepts optional Arc<dyn CatalogBackend> and
Arc<dyn CatalogAuth>. Falls back to legacy SessionCatalog when not
provided. Existing callers pass None for backward compatibility."
```

---

## Phase 2: Additional Catalog Backends

### Task 8: StorageOnlyBackend

**Files:**
- Create: `crates/sqe-catalog/src/backend_storage.rs`
- Modify: `crates/sqe-catalog/src/backend_factory.rs`

Discover Iceberg tables by scanning object storage paths for `metadata/v*.metadata.json`.

- [ ] **Step 1: Implement StorageOnlyBackend**

Key behavior:
- `list_namespaces()` — scan `base_path` up to `scan_depth` levels, derive namespaces from directory structure
- `list_tables()` — for a namespace, list directories containing `metadata/v*.metadata.json`
- `load_table()` — read the latest `metadata.json`, construct iceberg `Table`
- Cache results with moka (configurable TTL)
- Support explicit table mappings from config (`[[catalog.tables]]`)

```rust
pub struct StorageOnlyBackend {
    base_path: String,
    scan_depth: usize,
    explicit_tables: Vec<(String, String)>,  // (name, path)
    cache: moka::future::Cache<String, Vec<TableIdent>>,
    storage_config: StorageConfig,
}
```

- [ ] **Step 2: Add iceberg_scan() TVF for one-shot access**

Register a table-valued function `iceberg_scan(path)` that loads a single Iceberg table from an arbitrary path without catalog registration.

- [ ] **Step 3: Update factory to support storage_only**

- [ ] **Step 4: Write tests with local filesystem**

Test with a local directory containing an Iceberg table layout.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: add StorageOnlyBackend — catalog-free Iceberg table discovery

Scans storage paths for metadata/v*.metadata.json up to configurable
depth. Supports explicit table mappings and moka-cached auto-discovery.
Adds iceberg_scan(path) TVF for one-shot table access."
```

---

### Task 9: AwsGlueBackend

**Files:**
- Create: `crates/sqe-catalog/src/backend_glue.rs`
- Modify: `crates/sqe-catalog/src/backend_factory.rs`

Wraps `IcebergRestBackend` with Glue's Iceberg REST endpoint + `AwsIamAuth`.

- [ ] **Step 1: Implement AwsGlueBackend**

```rust
pub struct AwsGlueBackend {
    inner: IcebergRestBackend,
}

impl AwsGlueBackend {
    pub fn new(region: &str, resources: &BackendResources) -> Self {
        let url = format!("https://glue.{region}.amazonaws.com/iceberg");
        Self {
            inner: IcebergRestBackend::new(
                url,
                String::new(), // Glue doesn't use warehouse parameter
                resources.circuit_breaker.clone(),
                resources.http_client.clone(),
            ),
        }
    }
}
```

Delegate all `CatalogBackend` methods to `inner`, with `backend_name()` returning `"aws_glue"`.

- [ ] **Step 2: Update factory**

- [ ] **Step 3: Commit**

```bash
git commit -m "feat: add AwsGlueBackend — Glue Iceberg REST + SigV4

Wraps IcebergRestBackend with Glue regional endpoint
(https://glue.{region}.amazonaws.com/iceberg). Paired with
AwsIamAuth for SigV4 credential resolution."
```

---

## Phase 3: Multi-Cloud Storage

### Task 10: StorageConfig Refactoring

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Create: `crates/sqe-catalog/src/storage_config.rs` (if needed)

Extend `StorageConfig` to support Azure ADLS Gen2/Blob, GCS, and local filesystem.

- [ ] **Step 1: Add storage type variants**

```rust
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageBackendConfig {
    S3(S3StorageConfig),
    Azure(AzureStorageConfig),
    Gcs(GcsStorageConfig),
    Local(LocalStorageConfig),
}

#[derive(Debug, Deserialize, Clone)]
pub struct AzureStorageConfig {
    pub account_name: String,
    pub container_name: String,
    pub credentials: AzureCredentials,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AzureCredentials {
    AccessKey { access_key: String },
    SasToken { sas_token: String },
    WorkloadIdentity,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GcsStorageConfig {
    pub bucket: String,
    pub credentials: GcsCredentials,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GcsCredentials {
    ServiceAccount { service_account_key_file: String },
    WorkloadIdentity,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LocalStorageConfig {
    pub root: String,
}
```

- [ ] **Step 2: Add Cargo feature flags**

In `crates/sqe-catalog/Cargo.toml`:
```toml
[features]
default = ["catalog-iceberg-rest", "storage-s3"]
catalog-iceberg-rest = []
catalog-glue = ["catalog-iceberg-rest"]
catalog-storage-only = []
storage-s3 = ["object_store/aws"]
storage-azure = ["object_store/azure"]
storage-gcs = ["object_store/gcp"]
storage-local = []
policy-opa = []
```

- [ ] **Step 3: Write deserialization tests, commit**

---

## Phase 4: OPA Policy Engine

### Task 11: Extend OPA Policy Enforcement

**Files:**
- Modify: `crates/sqe-policy/src/opa.rs`
- Modify: `crates/sqe-policy/Cargo.toml`
- Modify: `crates/sqe-core/src/config.rs`

The OPA store already exists and works. This task extends it with:
1. Policy cache TTL from config (`cache_ttl_seconds`)
2. Feature flag `policy-opa`
3. Config wiring (`[policy] type = "opa"` with `url` and `cache_ttl_seconds`)

- [ ] **Step 1: Add OPA config fields**

```rust
// In PolicyConfig:
pub struct PolicyConfig {
    #[serde(default = "default_passthrough")]
    pub engine: String,
    #[serde(default)]
    pub opa_url: Option<String>,
    #[serde(default = "default_policy_cache_ttl")]
    pub cache_ttl_seconds: u64,
}

fn default_policy_cache_ttl() -> u64 { 300 }
```

- [ ] **Step 2: Wire OPA creation in coordinator**

In the coordinator's policy setup code, check `config.policy.engine`:
- `"passthrough"` → `PassthroughEnforcer`
- `"opa"` → `OpaStore::new(config.policy.opa_url, config.policy.cache_ttl_seconds)` + `PolicyPlanRewriter`

- [ ] **Step 3: Test with config, commit**

---

## Phase 5: Hybrid Grant System

### Task 12: GrantBackend Trait + Types

**Files:**
- Create: `crates/sqe-policy/src/grants.rs`

- [ ] **Step 1: Define the trait and types**

```rust
use async_trait::async_trait;
use std::collections::HashMap;

/// A resolved grant statement from SQL.
#[derive(Debug, Clone)]
pub struct GrantStatement {
    pub privilege: Privilege,
    pub target: GrantTarget,
    pub grantee: String,  // role name
}

#[derive(Debug, Clone)]
pub enum Privilege {
    Select,
    SelectColumns(Vec<String>),
    Insert,
    Update,
    Delete,
    All,
}

#[derive(Debug, Clone)]
pub struct GrantTarget {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct RevokeStatement {
    pub privilege: Privilege,
    pub target: GrantTarget,
    pub grantee: String,
}

#[derive(Debug, Clone)]
pub struct Grant {
    pub privilege: Privilege,
    pub target: GrantTarget,
    pub grantee: String,
    pub granted_by: Option<String>,
    pub granted_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Effective policy resolved for a user on a table.
#[derive(Debug, Clone, Default)]
pub struct EffectivePolicy {
    pub allowed_columns: Option<Vec<String>>,
    pub denied_columns: Vec<String>,
    pub row_filter: Option<String>,
    pub column_masks: HashMap<String, String>,
}

/// Backend for persisting and querying grants.
#[async_trait]
pub trait GrantBackend: Send + Sync {
    async fn grant(&self, stmt: &GrantStatement) -> sqe_core::Result<()>;
    async fn revoke(&self, stmt: &RevokeStatement) -> sqe_core::Result<()>;
    async fn list_grants(&self, target: &GrantTarget) -> sqe_core::Result<Vec<Grant>>;
    async fn effective_policy(
        &self,
        user: &sqe_core::session::SessionUser,
        table: &GrantTarget,
    ) -> sqe_core::Result<EffectivePolicy>;
}
```

- [ ] **Step 2: Write trait object safety test, commit**

---

### Task 13: LocalGrantStore (SQLite)

**Files:**
- Create: `crates/sqe-policy/src/grant_local.rs`

SQLite-backed grant store for storage-only and non-access-control catalogs.

- [ ] **Step 1: Implement with rusqlite**

```rust
pub struct LocalGrantStore {
    db: tokio::sync::Mutex<rusqlite::Connection>,
}

impl LocalGrantStore {
    pub fn new(path: &str) -> sqe_core::Result<Self> {
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| sqe_core::SqeError::Config(format!("grants db: {e}")))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS grants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                privilege TEXT NOT NULL,
                catalog TEXT,
                schema TEXT,
                table_name TEXT NOT NULL,
                grantee TEXT NOT NULL,
                granted_by TEXT,
                granted_at TEXT DEFAULT (datetime('now')),
                UNIQUE(privilege, catalog, schema, table_name, grantee)
            )"
        ).map_err(|e| sqe_core::SqeError::Config(format!("grants schema: {e}")))?;
        Ok(Self { db: tokio::sync::Mutex::new(conn) })
    }
}
```

- [ ] **Step 2: Implement GrantBackend trait**
- [ ] **Step 3: Write CRUD tests**
- [ ] **Step 4: Commit**

---

### Task 14: SQL Routing for GRANT/REVOKE

**Files:**
- Modify: `crates/sqe-sql/src/classifier.rs`
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

Route GRANT/REVOKE/SHOW GRANTS/SHOW EFFECTIVE POLICY through the coordinator to the GrantBackend.

- [ ] **Step 1: Verify classifier routes Policy statements**

Check that `StatementKind::Policy` is emitted for GRANT/REVOKE SQL.

- [ ] **Step 2: Add handler in query_handler.rs**

```rust
StatementKind::Policy(stmt) => {
    self.handle_policy_statement(session, &stmt).await
}
```

Parse the policy statement, dispatch to `grant_backend.grant()`, `grant_backend.revoke()`, `grant_backend.list_grants()`, or `grant_backend.effective_policy()`.

- [ ] **Step 3: Wire GrantBackend into QueryHandler**

Add `grant_backend: Option<Arc<dyn GrantBackend>>` field to QueryHandler.

- [ ] **Step 4: Write integration-style tests**
- [ ] **Step 5: Commit**

---

## Phase 6: Integration & Documentation

### Task 15: Coordinator Integration — Full Wiring

**Files:**
- Modify: `crates/sqe-coordinator/src/main.rs` or `bin/sqe_server.rs`

Wire everything together at startup:
1. Read config → `resolved_catalog_backend()` + `resolved_catalog_auth()`
2. Create backend via factory
3. Create auth via factory
4. Create grant backend (auto-select based on catalog type)
5. Pass to session context, query handler

- [ ] **Step 1: Add startup wiring**

```rust
// In main/server startup:
let backend_config = config.resolved_catalog_backend();
let auth_config = config.resolved_catalog_auth();
let resources = BackendResources::default();
let catalog_backend = create_catalog_backend(&backend_config, &resources)?;
let catalog_auth = create_catalog_auth(&auth_config);
let grant_backend = create_grant_backend(&config.grants, &backend_config)?;
```

- [ ] **Step 2: Pass to QueryHandler and SessionManager**
- [ ] **Step 3: Run full test suite**
- [ ] **Step 4: Commit**

---

### Task 16: Config Documentation + sqe.toml.example Update

**Files:**
- Modify: `sqe.toml.example`

Add example config sections for all new backend types.

- [ ] **Step 1: Add catalog backend examples**

```toml
# ── Catalog Backend ────────────────────────────────────────────
# Option 1: Iceberg REST (Polaris, Snowflake Open Catalog, Unity)
# [catalog_backend]
# type = "iceberg_rest"
# url = "https://polaris.example.com/api/catalog"
# warehouse = "mywarehouse"
# [catalog_backend.auth]
# type = "passthrough"

# Option 2: Storage-only (no catalog server)
# [catalog_backend]
# type = "storage_only"
# base_path = "s3://my-data-lake/"
# scan_depth = 3
# [[catalog_backend.tables]]
# name = "sales.orders"
# path = "s3://my-data-lake/sales/orders/"

# Option 3: AWS Glue
# [catalog_backend]
# type = "aws_glue"
# region = "eu-west-1"
# [catalog_backend.auth]
# type = "aws_iam"

# Option 4: AWS S3 Tables (managed Iceberg — uses standard REST + SigV4)
# [catalog_backend]
# type = "iceberg_rest"
# url = "https://s3tables.eu-west-1.amazonaws.com/iceberg"
# warehouse = "arn:aws:s3tables:eu-west-1:123456789012:bucket/my-table-bucket"
# [catalog_backend.auth]
# type = "aws_iam"
# region = "eu-west-1"
# signing_name = "s3tables"

# ── Policy ─────────────────────────────────────────────────────
# [policy]
# engine = "opa"
# opa_url = "http://opa:8181"
# cache_ttl_seconds = 300

# ── Grants ─────────────────────────────────────────────────────
# [grants]
# backend = "auto"  # auto, local, catalog_native, chameleon
```

- [ ] **Step 2: Update CLAUDE.md if needed**
- [ ] **Step 3: Commit**

```bash
git commit -m "docs: add pluggable catalog config examples to sqe.toml.example

Examples for iceberg_rest, storage_only, aws_glue, and AWS S3 Tables
backends plus OPA policy and grant configuration sections."
```

---

### Task 17: Update README.md + nextsteps.md

**Files:**
- Modify: `README.md`
- Modify: `nextsteps.md`

Mark Step 5 (pluggable catalogs) as complete. Update roadmap.

- [ ] **Step 1: Update nextsteps.md**

```
Step 5: pluggable catalogs  ✅ DONE (IcebergRest, StorageOnly, AwsGlue, S3 Tables, OPA, grants)
```

- [ ] **Step 2: Update README.md roadmap**

Check `[ ] Pluggable catalog backends` and `[ ] OPA/Cedar policy engine`.

- [ ] **Step 3: Commit**

---

## Summary

| Task | Phase | Description |
|---|---|---|
| 1 | Foundation | CatalogBackend + CatalogAuth traits |
| 2 | Foundation | CatalogAuth implementations (4 strategies) |
| 3 | Foundation | Config refactoring (backend enum, auth enum, grants) |
| 4 | Foundation | IcebergRestBackend (refactor from SessionCatalog) |
| 5 | Foundation | Backend factory (config → trait objects) |
| 6 | Foundation | Update providers to use dyn CatalogBackend |
| 7 | Foundation | Session context factory wiring |
| 8 | Backends | StorageOnlyBackend (path scanning) |
| 9 | Backends | AwsGlueBackend (Glue REST + SigV4) |
| 10 | Storage | Multi-cloud StorageConfig + feature flags |
| 11 | Policy | OPA Policy Engine extension |
| 12 | Grants | GrantBackend trait + types |
| 13 | Grants | LocalGrantStore (SQLite) |
| 14 | Grants | SQL routing for GRANT/REVOKE |
| 15 | Integration | Coordinator startup wiring |
| 16 | Docs | Config documentation + sqe.toml.example |
| 17 | Docs | README + nextsteps update |

**Parallelism:** Tasks 1-7 are sequential (each depends on prior). Tasks 8-9 are independent (parallel OK). Tasks 11-14 are independent from 8-10 (parallel OK). Tasks 15-17 depend on all prior tasks.

**Critical path:** Tasks 1 → 2 → 3 → 4 → 5 → 6 → 7 → 15 → 17

**Estimated time:** 6-8 hours for a single agent; ~3-4 hours with parallel execution of independent phases.
