## Context

Replacing the hardwired Polaris REST catalog + S3 storage with a pluggable `CatalogBackend` + `CatalogAuth` + `StorageConfig` triple. Decisions from design exploration:
- Unity Catalog supports both **Iceberg and Delta** — both are first-class
- Storage-only catalog: **auto-discover tables by scanning a base path** (find `metadata/` dirs)
- API keys are **group-based** (inherited from pluggable-auth design)
- Catalog credentials for service-credential mode are **externally managed** (config file)

## Goals / Non-Goals

**Goals:**
- `CatalogBackend` trait for table discovery and metadata loading
- `CatalogAuth` trait for how SQE authenticates to the catalog service
- `StorageConfig` enum for multi-cloud object storage (S3, Azure ADLS, GCS, R2, Ceph, local)
- Support Iceberg and Delta Lake table formats (Iceberg v3 primary, Delta via feature flag)
- Storage-only auto-discovery: scan a root path, find all Iceberg tables without a catalog server

**Non-Goals:**
- Write path for Delta tables (read-only Delta via delta-rs in this change)
- Catalog federation / virtual catalog spanning multiple backends
- Automatic schema migration across catalog types

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     sqe-catalog crate                           │
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────────────────────┐    │
│  │   CatalogAuth   │───▶│         CatalogBackend          │    │
│  │ (how to auth    │    │  (table discovery + metadata)   │    │
│  │  to catalog)    │    └─────────────────────────────────┘    │
│  └─────────────────┘                    │                       │
│                                         │ TableMetadata          │
│                                         ▼                       │
│                          ┌─────────────────────────┐           │
│                          │  TableFormatProvider    │           │
│                          │  Iceberg │ Delta        │           │
│                          └──────────┬──────────────┘           │
│                                     │                           │
│                          ┌──────────▼──────────────┐           │
│                          │     StorageConfig       │           │
│                          │  S3 │ Azure │ GCS │ ... │           │
│                          └─────────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
```

### CatalogAuth Trait

```rust
#[async_trait]
pub trait CatalogAuth: Send + Sync {
    /// Produce credentials for a catalog HTTP/Thrift request.
    /// `user_token` is the current user's OIDC bearer token, if available.
    async fn catalog_credential(&self, user_token: Option<&str>) -> Result<CatalogCredential>;
}

pub enum CatalogCredential {
    Bearer(String),             // HTTP Authorization: Bearer
    AwsSigV4(AwsCredentials),   // SigV4 signing
    UsernamePassword(String, String), // HMS Thrift plain
    None,                       // anonymous
}
```

**Implementations:**

| Name | Behaviour |
|---|---|
| `PassthroughCatalogAuth` | Return `Bearer(user_token)` — forward user's OIDC token |
| `ServiceCredentialAuth` | OAuth2 client_credentials → fetch + cache catalog token |
| `AwsIamAuth` | SigV4 from env/instance/explicit key (aws-sdk-rust `aws-credential-types`) |
| `NoCatalogAuth` | Return `None` |

### CatalogBackend Trait

```rust
#[async_trait]
pub trait CatalogBackend: Send + Sync {
    async fn list_namespaces(&self, cred: &CatalogCredential) -> Result<Vec<NamespaceIdent>>;
    async fn list_tables(&self, ns: &NamespaceIdent, cred: &CatalogCredential) -> Result<Vec<TableIdent>>;
    async fn load_table(&self, ident: &TableIdent, cred: &CatalogCredential) -> Result<TableMetadata>;
    fn table_format(&self, metadata: &TableMetadata) -> TableFormat;
}

pub enum TableFormat { Iceberg, Delta }
```

**Implementations:**

### 1. IcebergRestBackend

Generalised Iceberg REST catalog (Polaris, Snowflake Open Catalog, Unity Catalog REST endpoint).

```toml
[catalog]
type     = "iceberg_rest"
url      = "https://polaris.example.com/api/catalog"
warehouse = "mywarehouse"

[catalog.auth]
type = "passthrough"   # or "service_credential"
```

Uses `iceberg-rust` `RestCatalog` internally. Zero behaviour change from current.

### 2. AwsGlueBackend

AWS Glue supports an [Iceberg REST endpoint](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-iceberg-rest-catalog.html) (`glue.{region}.amazonaws.com/iceberg`). Uses `AwsIamAuth` with SigV4.

```toml
[catalog]
type   = "aws_glue"
region = "eu-west-1"
# Optional: explicit credentials (else uses instance/env):
[catalog.auth]
type             = "aws_iam"
access_key_id    = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
```

Internally calls the Glue Iceberg REST endpoint via `IcebergRestBackend` with `AwsIamAuth`.

### 3. NessieBackend

[Project Nessie](https://projectnessie.org/) REST API. Supports namespaces, table listing, content resolution.

```toml
[catalog]
type     = "nessie"
url      = "http://nessie:19120/api/v2"
ref      = "main"   # branch/tag/commit

[catalog.auth]
type = "bearer_token"   # or "none" for dev
token = "${NESSIE_TOKEN}"
```

Nessie tables resolve to Iceberg metadata locations; `TableFormat::Iceberg` always.

### 4. HiveMetastoreBackend

Thrift HMS client for table discovery. Table files accessed directly via `StorageConfig`.

```toml
[catalog]
type    = "hive_metastore"
thrift_url = "thrift://hms:9083"
database   = "default"

[catalog.auth]
type = "none"   # or "kerberos" (future)
```

Only used for table discovery. Actual data files read by iceberg-rust `FileIO` directly from storage (no Hive SerDe). Tables must be Iceberg-format (property `table_type=ICEBERG`).

### 5. StorageOnlyBackend

No catalog server. Discovers Iceberg tables by scanning a root path on object storage.

```toml
[catalog]
type      = "storage_only"
base_path = "s3://my-data-lake/"
# Optional: restrict scan depth (default 3):
scan_depth = 3
# Optional: register named tables explicitly:
[[catalog.tables]]
name = "sales.orders"
path = "s3://my-data-lake/sales/orders/"
```

**Auto-discovery algorithm:**
1. List all objects under `base_path` up to `scan_depth` levels
2. Look for `metadata/` subdirectory containing `v*.metadata.json`
3. Each discovered path becomes a table; namespace is derived from directory structure:
   - `base_path/sales/orders/metadata/` → namespace `sales`, table `orders`
4. Cache discovery results with a configurable TTL (default 5 minutes)

Users can also access undiscovered tables via the `iceberg_scan()` table-valued function:
```sql
SELECT * FROM iceberg_scan('s3://my-bucket/path/to/table/');
```

### Unity Catalog: Iceberg + Delta

Unity Catalog exposes both an Iceberg REST endpoint and the Unity Catalog REST API.

```toml
[catalog]
type      = "iceberg_rest"          # for Iceberg tables
url       = "https://<workspace>.azuredatabricks.net/api/2.1/unity-catalog/iceberg"
warehouse = "main"

[catalog.auth]
type  = "bearer_token"
token = "${DATABRICKS_TOKEN}"   # PAT or Entra OIDC token
```

Delta tables in Unity Catalog: enabled via `features = ["delta"]` (Cargo feature flag). The `delta-rs` crate provides a `DeltaTableProvider` for DataFusion. When `table_format()` returns `TableFormat::Delta`, `sqe-catalog` wraps the delta-rs provider instead of iceberg-rust.

```toml
# In addition to catalog config:
[features]
delta = true
```

Delta tables are **read-only** in this change. Write path for Delta is deferred.

### Storage Backends

`StorageConfig` wraps the `object_store` crate:

```toml
[storage]
type   = "s3"
region = "eu-west-1"
# endpoint override for S3-compatible (Ceph, R2, SeaweedFS, Garage):
endpoint = "https://s3.my-ceph.example.com"
path_style = true   # required for most S3-compatible servers

[storage.credentials]
type = "static"   # or "env", "instance_profile", "web_identity"
access_key_id     = "${S3_ACCESS_KEY}"
secret_access_key = "${S3_SECRET_KEY}"
```

```toml
[storage]
type            = "azure"
account_name    = "mystorageaccount"
container_name  = "datalake"
# credentials: access_key, sas_token, or workload_identity
[storage.credentials]
type        = "access_key"
access_key  = "${AZURE_STORAGE_KEY}"
```

```toml
[storage]
type = "gcs"
bucket = "my-gcs-bucket"
[storage.credentials]
type                     = "service_account"
service_account_key_file = "/etc/sqe/gcs-sa.json"
# or: "workload_identity" for GKE
```

```toml
[storage]
type = "local"
root = "/data/iceberg"   # dev / CI only
```

**Cloudflare R2** uses the `s3` type with `endpoint = "https://<account>.r2.cloudflarestorage.com"` and `path_style = false`. No special case.

### Multi-Storage

SQE supports one primary storage config for credential vending. When catalogs vend their own storage credentials (Polaris, Glue), those override the global storage config per-table.

## Crate Feature Flags

```toml
# Cargo.toml (sqe-catalog)
[features]
default = ["catalog-iceberg-rest", "storage-s3"]

catalog-iceberg-rest = ["iceberg-rust/rest"]
catalog-glue         = ["catalog-iceberg-rest", "aws-credential-types"]
catalog-nessie       = []
catalog-hms          = ["hive-metastore-thrift"]
catalog-storage-only = []

storage-s3     = ["object_store/aws"]
storage-azure  = ["object_store/azure"]
storage-gcs    = ["object_store/gcp"]
storage-local  = []

delta          = ["deltalake"]
```

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Glue backend | wraps IcebergRestBackend | Glue exposes standard Iceberg REST; no custom code |
| Delta support | feature flag, read-only | Keeps default binary lean; write path is a separate change |
| Storage-only discovery | scan + explicit table list | Covers both migration (scan everything) and targeted (known paths) |
| Unity Catalog | iceberg_rest type + bearer token | UC's Iceberg REST endpoint is standard; no special backend needed |
| S3-compat (R2, Ceph) | endpoint override in s3 type | object_store handles it; no new type |
| MinIO | removed | BSL licence; users migrate to Ceph, Garage, or R2 |

## Risks

| Risk | Mitigation |
|---|---|
| Glue REST endpoint regional availability | Config requires explicit region; clear error on invalid region |
| delta-rs DataFusion provider API stability | Feature-flagged; can be disabled without breaking core |
| HMS Kerberos auth | Deferred (NoCatalogAuth only in this change) |
| Storage-only scan on large buckets | scan_depth limit + cache; warn in docs about large flat buckets |
