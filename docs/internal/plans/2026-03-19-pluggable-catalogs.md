# Pluggable Catalogs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hardwired Polaris + S3 with pluggable `CatalogBackend`, `CatalogAuth`, and `StorageConfig` traits/enums covering: Iceberg REST (generalised), AWS Glue, Nessie, Hive Metastore, storage-only (path-based auto-discovery), Delta Lake (feature flag), and storage backends for Azure ADLS, GCS, S3-compatible (Ceph, R2, Garage), and local.

**Architecture:** Two trait abstractions (`CatalogAuth`, `CatalogBackend`) plus a `StorageConfig` enum wrapping the `object_store` crate. Each catalog type is a separate file in `sqe-catalog/src/backend/`. New storage types are in `sqe-catalog/src/storage/`. All gated behind Cargo feature flags; default is `catalog-iceberg-rest + storage-s3` (backwards compatible).

**Tech Stack:** Rust, iceberg-rust (REST + FileIO), delta-rs (Delta, feature flag), object_store (S3/Azure/GCS/local), aws-credential-types (SigV4), notify (file watch), axum (none), reqwest

**Spec:** `openspec/changes/pluggable-catalogs/`

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `crates/sqe-catalog/src/traits.rs` | create | `CatalogAuth`, `CatalogBackend`, `CatalogCredential`, `TableFormat` |
| `crates/sqe-catalog/src/auth/passthrough.rs` | create | forward user bearer token |
| `crates/sqe-catalog/src/auth/service_credential.rs` | create | OAuth2 client_credentials |
| `crates/sqe-catalog/src/auth/aws_iam.rs` | create | SigV4 signing |
| `crates/sqe-catalog/src/auth/none.rs` | create | anonymous |
| `crates/sqe-catalog/src/backend/iceberg_rest.rs` | refactor | generalise existing; implement trait |
| `crates/sqe-catalog/src/backend/glue.rs` | create | wraps iceberg_rest with AWS endpoint |
| `crates/sqe-catalog/src/backend/nessie.rs` | create | Nessie REST API |
| `crates/sqe-catalog/src/backend/hms.rs` | create | Hive Metastore Thrift (feature flag) |
| `crates/sqe-catalog/src/backend/storage_only.rs` | create | path scanning, auto-discovery |
| `crates/sqe-catalog/src/storage/s3.rs` | create | S3 + endpoint override |
| `crates/sqe-catalog/src/storage/azure.rs` | create | Azure ADLS/Blob |
| `crates/sqe-catalog/src/storage/gcs.rs` | create | Google Cloud Storage |
| `crates/sqe-catalog/src/storage/local.rs` | create | local filesystem |
| `crates/sqe-catalog/src/tvf.rs` | create | `iceberg_scan()` TVF |
| `crates/sqe-core/src/config.rs` | modify | `CatalogConfig` enum, `StorageConfig` enum |
| `crates/sqe-catalog/src/factory.rs` | create | build backend + auth + store from config |

---

### Task 1: CatalogAuth trait + implementations

**Files:**
- Create: `crates/sqe-catalog/src/traits.rs`
- Create: `crates/sqe-catalog/src/auth/passthrough.rs`
- Create: `crates/sqe-catalog/src/auth/none.rs`
- Test: `crates/sqe-catalog/tests/catalog_auth_test.rs`

- [ ] **Step 1: Write failing test**
```rust
// crates/sqe-catalog/tests/catalog_auth_test.rs
use sqe_catalog::auth::{PassthroughCatalogAuth, NoCatalogAuth, CatalogAuth};

#[tokio::test]
async fn passthrough_forwards_user_token() {
    let auth = PassthroughCatalogAuth;
    let cred = auth.catalog_credential(Some("user-bearer-token")).await.unwrap();
    assert!(matches!(cred, sqe_catalog::CatalogCredential::Bearer(t) if t == "user-bearer-token"));
}

#[tokio::test]
async fn no_auth_returns_none() {
    let auth = NoCatalogAuth;
    let cred = auth.catalog_credential(None).await.unwrap();
    assert!(matches!(cred, sqe_catalog::CatalogCredential::None));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-catalog catalog_auth_test 2>&1`
Expected: module not found

- [ ] **Step 3: Define traits.rs**
```rust
// crates/sqe-catalog/src/traits.rs
use async_trait::async_trait;

pub enum CatalogCredential {
    Bearer(String),
    AwsSigV4(AwsCredentials),
    UsernamePassword(String, String),
    None,
}

#[async_trait]
pub trait CatalogAuth: Send + Sync {
    async fn catalog_credential(&self, user_token: Option<&str>) -> Result<CatalogCredential>;
}

pub enum TableFormat { Iceberg, Delta }

#[async_trait]
pub trait CatalogBackend: Send + Sync {
    async fn list_namespaces(&self, cred: &CatalogCredential) -> Result<Vec<NamespaceIdent>>;
    async fn list_tables(&self, ns: &NamespaceIdent, cred: &CatalogCredential) -> Result<Vec<TableIdent>>;
    async fn load_table(&self, ident: &TableIdent, cred: &CatalogCredential) -> Result<TableMetadata>;
    fn table_format(&self, metadata: &TableMetadata) -> TableFormat;
}
```

- [ ] **Step 4: Implement Passthrough + NoAuth**

Simple one-liners; implement trait.

- [ ] **Step 5: Run tests**

Run: `cargo test -p sqe-catalog catalog_auth_test 2>&1`
Expected: pass

- [ ] **Step 6: Commit**
```bash
git add crates/sqe-catalog/src/traits.rs crates/sqe-catalog/src/auth/
git commit -m "feat(catalog): add CatalogAuth + CatalogBackend traits; passthrough and no-auth implementations"
```

---

### Task 2: ServiceCredentialAuth

**Files:**
- Create: `crates/sqe-catalog/src/auth/service_credential.rs`
- Test: `crates/sqe-catalog/tests/service_credential_test.rs`

- [ ] **Step 1: Write failing test**
```rust
#[tokio::test]
async fn service_credential_fetches_and_caches_token() {
    let mock = wiremock::MockServer::start().await;
    // First call returns token; second call should use cache (mock called once)
    wiremock::Mock::given(...)
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "svc-token-abc",
            "expires_in": 3600
        })))
        .expect(1)  // called exactly once due to caching
        .mount(&mock).await;
    let auth = ServiceCredentialAuth::new(mock.uri() + "/token", "sqe-svc", "secret");
    let c1 = auth.catalog_credential(None).await.unwrap();
    let c2 = auth.catalog_credential(None).await.unwrap();  // from cache
    assert!(matches!(c1, CatalogCredential::Bearer(t) if t == "svc-token-abc"));
    mock.verify().await;  // asserts called exactly once
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-catalog service_credential 2>&1`
Expected: module not found

- [ ] **Step 3: Implement ServiceCredentialAuth**

POST `grant_type=client_credentials` to `token_url`. Cache with moka, expire at `expires_in - 30s`.

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-catalog service_credential 2>&1`
Expected: pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-catalog/src/auth/service_credential.rs
git commit -m "feat(catalog): add ServiceCredentialAuth with token caching"
```

---

### Task 3: IcebergRestBackend refactor

**Files:**
- Modify: `crates/sqe-catalog/src/backend/iceberg_rest.rs`
- Test: `crates/sqe-catalog/tests/iceberg_rest_test.rs`

- [ ] **Step 1: Write test against existing behaviour**
```rust
// Verify existing Polaris integration still works after trait refactor
#[tokio::test]
async fn iceberg_rest_lists_namespaces() {
    // Uses existing quickstart test fixtures
    let backend = IcebergRestBackend::new("http://polaris:8181/api/catalog", "mywarehouse");
    let cred = CatalogCredential::Bearer("test-token".into());
    let ns = backend.list_namespaces(&cred).await.unwrap();
    assert!(!ns.is_empty());
}
```

- [ ] **Step 2: Refactor IcebergRestBackend to implement CatalogBackend trait**

Extract existing catalog calls into `list_namespaces`, `list_tables`, `load_table`. `table_format` checks for Delta marker in table properties; returns `TableFormat::Iceberg` by default.

- [ ] **Step 3: Run test**

Run: `cargo test -p sqe-catalog iceberg_rest 2>&1`
Expected: pass (integration test skipped in CI without quickstart)

- [ ] **Step 4: Commit**
```bash
git add crates/sqe-catalog/src/backend/iceberg_rest.rs
git commit -m "refactor(catalog): IcebergRestBackend implements CatalogBackend trait"
```

---

### Task 4: StorageOnlyBackend + iceberg_scan TVF

**Files:**
- Create: `crates/sqe-catalog/src/backend/storage_only.rs`
- Create: `crates/sqe-catalog/src/tvf.rs`
- Test: `crates/sqe-catalog/tests/storage_only_test.rs`

- [ ] **Step 1: Write failing test**
```rust
#[tokio::test]
async fn storage_only_discovers_tables_by_scanning() {
    // Set up a local temp dir with two Iceberg table paths
    let dir = tempdir().unwrap();
    create_fake_iceberg_metadata(&dir, "sales/orders").await;
    create_fake_iceberg_metadata(&dir, "sales/customers").await;

    let backend = StorageOnlyBackend::new(
        dir.path().to_str().unwrap(),
        3,   // scan_depth
        vec![], // no explicit registrations
    );
    let cred = CatalogCredential::None;
    let ns = NamespaceIdent::from_strs(["sales"]).unwrap();
    let tables = backend.list_tables(&ns, &cred).await.unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().any(|t| t.name() == "orders"));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p sqe-catalog storage_only 2>&1`
Expected: module not found

- [ ] **Step 3: Implement StorageOnlyBackend**

Algorithm:
1. Recursively list directories in `base_path` up to `scan_depth` levels using `object_store.list()`
2. For each path ending in `/metadata/`, check for `v*.metadata.json`; if found, record as table
3. Derive namespace = path components between `base_path` and table dir (up to second-to-last)
4. Derive table name = last path component before `/metadata/`
5. Cache discovery result with configurable TTL (moka, default 5 min)

Include explicit `[[catalog.tables]]` registrations (merged, overrides scan result).

- [ ] **Step 4: Implement iceberg_scan TVF**
```rust
// crates/sqe-catalog/src/tvf.rs
// DataFusion TableFunction that wraps StorageOnlyBackend for one arbitrary path
// SQL: SELECT * FROM iceberg_scan('s3://bucket/path/')
pub struct IcebergScanFunction;
impl TableFunctionImpl for IcebergScanFunction { ... }
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p sqe-catalog storage_only 2>&1`
Expected: pass

- [ ] **Step 6: Commit**
```bash
git add crates/sqe-catalog/src/backend/storage_only.rs crates/sqe-catalog/src/tvf.rs
git commit -m "feat(catalog): add StorageOnlyBackend with path auto-discovery and iceberg_scan TVF"
```

---

### Task 5: NessieBackend

**Files:**
- Create: `crates/sqe-catalog/src/backend/nessie.rs`
- Test: `crates/sqe-catalog/tests/nessie_test.rs`

- [ ] **Step 1: Write failing test (unit — mock HTTP)**
```rust
#[tokio::test]
async fn nessie_lists_tables_from_api() {
    let mock = wiremock::MockServer::start().await;
    // Mock GET /api/v2/trees/main/entries
    wiremock::Mock::given(wiremock::matchers::path("/api/v2/trees/main/entries"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(nessie_entries_fixture()))
        .mount(&mock).await;
    let backend = NessieBackend::new(mock.uri(), "main");
    let cred = CatalogCredential::None;
    let tables = backend.list_tables(&NamespaceIdent::from_strs(["db"]).unwrap(), &cred).await.unwrap();
    assert!(!tables.is_empty());
}
```

- [ ] **Step 2: Implement NessieBackend**

Use `reqwest` for Nessie REST API v2. Map Nessie `ContentType::ICEBERG_TABLE` entries to `TableIdent`. `load_table`: fetch content → get `metadata_location` → load via iceberg-rust `FileIO`.

- [ ] **Step 3: Run test**

Run: `cargo test -p sqe-catalog nessie 2>&1`
Expected: pass

- [ ] **Step 4: Commit**
```bash
git add crates/sqe-catalog/src/backend/nessie.rs
git commit -m "feat(catalog): add NessieBackend for Project Nessie catalog"
```

---

### Task 6: AwsGlueBackend + AwsIamAuth

**Files:**
- Create: `crates/sqe-catalog/src/auth/aws_iam.rs`
- Create: `crates/sqe-catalog/src/backend/glue.rs`
- Test: `crates/sqe-catalog/tests/glue_test.rs`

- [ ] **Step 1: Write failing test**
```rust
#[test]
fn glue_backend_builds_correct_endpoint_for_region() {
    let backend = AwsGlueBackend::new("eu-west-1");
    assert!(backend.endpoint_url().contains("eu-west-1"));
    assert!(backend.endpoint_url().contains("glue"));
}
```

- [ ] **Step 2: Implement AwsIamAuth**

Use `aws-credential-types` + `aws-sigv4` crates for SigV4 request signing. Support env vars, instance profile, and explicit key/secret.

- [ ] **Step 3: Implement AwsGlueBackend**

Thin wrapper: `AwsGlueBackend::new(region)` builds an `IcebergRestBackend` pointed at `https://glue.{region}.amazonaws.com/iceberg` with `AwsIamAuth`. All `CatalogBackend` methods delegate.

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-catalog glue 2>&1`
Expected: pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-catalog/src/auth/aws_iam.rs crates/sqe-catalog/src/backend/glue.rs
git commit -m "feat(catalog): add AwsGlueBackend and AwsIamAuth (SigV4)"
```

---

### Task 7: Azure storage backend

**Files:**
- Create: `crates/sqe-catalog/src/storage/azure.rs`
- Test: `crates/sqe-catalog/tests/azure_storage_test.rs`

- [ ] **Step 1: Write failing test (uses Azurite emulator in CI)**
```rust
#[tokio::test]
#[ignore = "requires Azurite"]
async fn azure_storage_reads_parquet() {
    let store = AzureStorageBuilder::new()
        .account_name("devstoreaccount1")
        .access_key(AZURITE_KEY)
        .endpoint("http://127.0.0.1:10000")
        .build().unwrap();
    let path = object_store::path::Path::from("test/file.parquet");
    let result = store.get(&path).await;
    assert!(result.is_ok());
}
```

- [ ] **Step 2: Implement AzureStorageConfig**
```rust
// sqe-catalog/src/storage/azure.rs
pub fn build_azure_store(cfg: &AzureStorageConfig) -> Result<Arc<dyn ObjectStore>> {
    let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
        .with_account(cfg.account_name.clone());
    match &cfg.credentials {
        AzureCredentials::AccessKey(key) => builder = builder.with_access_key(key),
        AzureCredentials::SasToken(sas) => builder = builder.with_sas_query_pairs(sas),
        AzureCredentials::WorkloadIdentity => builder = builder.with_use_azure_cli(true),
    }
    Ok(Arc::new(builder.build()?))
}
```

- [ ] **Step 3: Run test (skip without Azurite)**

Run: `cargo test -p sqe-catalog azure_storage 2>&1`
Expected: test ignored (no Azurite); compile passes

- [ ] **Step 4: Commit**
```bash
git add crates/sqe-catalog/src/storage/azure.rs
git commit -m "feat(storage): add Azure ADLS Gen2/Blob storage backend"
```

---

### Task 8: GCS storage backend

**Files:**
- Create: `crates/sqe-catalog/src/storage/gcs.rs`

- [ ] **Step 1: Implement GcsStorageConfig** (same pattern as Azure)
```rust
pub fn build_gcs_store(cfg: &GcsStorageConfig) -> Result<Arc<dyn ObjectStore>> {
    let mut builder = object_store::gcp::GoogleCloudStorageBuilder::new()
        .with_bucket_name(cfg.bucket.clone());
    match &cfg.credentials {
        GcsCredentials::ServiceAccountKeyFile(path) => builder = builder.with_service_account_path(path),
        GcsCredentials::WorkloadIdentity => builder = builder.with_application_credentials(),
    }
    Ok(Arc::new(builder.build()?))
}
```

- [ ] **Step 2: Test (uses fake-gcs-server in CI)**

```rust
#[tokio::test]
#[ignore = "requires fake-gcs-server"]
async fn gcs_storage_reads_object() { ... }
```

- [ ] **Step 3: Commit**
```bash
git add crates/sqe-catalog/src/storage/gcs.rs
git commit -m "feat(storage): add Google Cloud Storage backend"
```

---

### Task 9: Delta Lake support (feature flag)

**Files:**
- Modify: `crates/sqe-catalog/Cargo.toml` (add `delta` feature)
- Create: `crates/sqe-catalog/src/delta.rs`

- [ ] **Step 1: Add feature flag**
```toml
[features]
delta = ["deltalake"]
[dependencies]
deltalake = { version = "0.18", optional = true, features = ["datafusion"] }
```

- [ ] **Step 2: Write failing test**
```rust
#[cfg(feature = "delta")]
#[tokio::test]
async fn delta_table_can_be_opened() {
    let path = "tests/fixtures/delta_table/";
    let table = open_delta_table(path).await.unwrap();
    assert!(table.schema().is_some());
}
```

- [ ] **Step 3: Implement delta.rs**
```rust
// crates/sqe-catalog/src/delta.rs
#[cfg(feature = "delta")]
pub async fn open_delta_table(path: &str) -> Result<deltalake::DeltaTable> {
    deltalake::open_table(path).await.map_err(Into::into)
}

#[cfg(feature = "delta")]
pub fn register_delta_provider(ctx: &mut SessionContext, ident: &TableIdent, path: &str) -> Result<()> {
    // Register DeltaTableProvider in DataFusion SessionContext
    ...
}
```

In `CatalogBackend::table_format()` for `IcebergRestBackend`: check `table.properties().get("delta.minReaderVersion")` — if present, return `TableFormat::Delta`.

In coordinator's table resolution: if `TableFormat::Delta`, call `register_delta_provider` instead of iceberg-rust.

- [ ] **Step 4: Run test**

Run: `cargo test -p sqe-catalog --features delta delta_table 2>&1`
Expected: pass

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-catalog/src/delta.rs crates/sqe-catalog/Cargo.toml
git commit -m "feat(catalog): add Delta Lake read support via delta-rs (feature flag 'delta')"
```

---

### Task 10: Config + factory wiring

**Files:**
- Modify: `crates/sqe-core/src/config.rs`
- Create: `crates/sqe-catalog/src/factory.rs`
- Test: `crates/sqe-core/tests/catalog_config_test.rs`

- [ ] **Step 1: Write failing tests**
```rust
#[test]
fn iceberg_rest_config_deserialises() {
    let toml = r#"
[catalog]
type = "iceberg_rest"
url = "http://polaris:8181/api/catalog"
warehouse = "main"
[catalog.auth]
type = "passthrough"
"#;
    let config = Config::from_str(toml).unwrap();
    assert!(matches!(config.catalog, CatalogConfig::IcebergRest { .. }));
}

#[test]
fn storage_only_config_deserialises() {
    let toml = r#"
[catalog]
type = "storage_only"
base_path = "s3://my-lake/"
scan_depth = 3
"#;
    let config = Config::from_str(toml).unwrap();
    assert!(matches!(config.catalog, CatalogConfig::StorageOnly { .. }));
}
```

- [ ] **Step 2: Implement CatalogConfig enum**
```rust
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogConfig {
    IcebergRest { url: String, warehouse: String, auth: CatalogAuthConfig },
    AwsGlue { region: String, auth: CatalogAuthConfig },
    Nessie { url: String, ref_: String, auth: CatalogAuthConfig },
    HiveMetastore { thrift_url: String },
    StorageOnly { base_path: String, scan_depth: Option<u8>, tables: Vec<ExplicitTableConfig> },
}
```

- [ ] **Step 3: Implement factory**
```rust
// sqe-catalog/src/factory.rs
pub fn build_catalog(config: &CatalogConfig, storage: &StorageConfig) -> Result<(Arc<dyn CatalogBackend>, Arc<dyn CatalogAuth>)> {
    match config {
        CatalogConfig::IcebergRest { url, warehouse, auth } => {
            let backend = Arc::new(IcebergRestBackend::new(url, warehouse));
            let auth = build_catalog_auth(auth);
            Ok((backend, auth))
        }
        CatalogConfig::AwsGlue { region, auth } => { ... }
        CatalogConfig::StorageOnly { base_path, scan_depth, tables } => { ... }
        // etc.
    }
}
```

- [ ] **Step 4: Run all tests**

Run: `cargo test 2>&1`
Expected: all pass; integration tests skipped

- [ ] **Step 5: Commit**
```bash
git add crates/sqe-core/src/config.rs crates/sqe-catalog/src/factory.rs
git commit -m "feat(catalog): add pluggable CatalogConfig enum and factory; wire into coordinator"
```
