## Why

SQE is hardwired to Apache Polaris (Iceberg REST catalog) with a single storage backend (S3). As an Apache 2.0 open-source project it needs to run anywhere users have data:

- **AWS Glue**: dominant Iceberg catalog on AWS, uses IAM/SigV4 auth not OIDC bearer tokens
- **Project Nessie** (Dremio): popular open-source catalog with git-like versioning, own REST protocol
- **Databricks Unity Catalog**: exposes an Iceberg REST compatibility endpoint; auth via Databricks PAT or Entra
- **Apache Hive Metastore (HMS)**: legacy catalog still dominant in many Hadoop/Spark shops, Thrift protocol
- **Storage-only (no catalog)**: scan Iceberg metadata files directly from object storage by path; useful for ad-hoc access, migration, and local dev without a running catalog server
- **Delta Lake tables** (rs-delta): Unity Catalog serves both Iceberg and Delta; supporting Delta tables expands the audience significantly
- **Azure ADLS Gen2 / GCS / Cloudflare R2**: data lives beyond S3; object_store crate supports all of these but SQE has no wiring for them

MinIO is removed from the supported stack (BSL licence change).

## What Changes

Two new trait hierarchies in `sqe-catalog`:

```
CatalogAuth trait  →  how SQE authenticates to the catalog
CatalogBackend trait →  how SQE discovers and loads table metadata
```

Storage is handled by a `StorageConfig` enum that configures the `object_store` crate.

**CatalogAuth implementations:**
1. `PassthroughCatalogAuth` — forward the user's OIDC bearer token (current behaviour, Polaris + Unity Catalog)
2. `ServiceCredentialAuth` — fixed client_id/secret exchanged for a catalog token (OAuth2 client_credentials)
3. `AwsIamAuth` — SigV4 request signing from environment/instance credentials or explicit key/secret
4. `NoCatalogAuth` — anonymous (Nessie dev mode, local storage scan)

**CatalogBackend implementations:**
1. `IcebergRestBackend` — generalised Iceberg REST catalog (Polaris, Unity Catalog REST, Snowflake Open Catalog)
2. `AwsGlueBackend` — AWS Glue Data Catalog with Iceberg REST endpoint + SigV4
3. `NessieBackend` — Nessie REST API (table listing, namespace traversal, content resolution)
4. `HiveMetastoreBackend` — Thrift HMS client (table discovery only; execution via iceberg-rust file I/O)
5. `StorageOnlyBackend` — no catalog server; locate tables by `iceberg_scan('s3://...')` TVF or config-registered paths

**Storage backends (object_store):**
- AWS S3 (+ any S3-compatible endpoint: Ceph, SeaweedFS, Garage, Cloudflare R2)
- Azure Data Lake Storage Gen2 / Azure Blob Storage
- Google Cloud Storage
- Local filesystem (dev / CI)

**Table format support:**
- Apache Iceberg v3 (current)
- Delta Lake via `delta-rs` `DeltaTableProvider` for DataFusion (optional feature flag)

## Capabilities

### New Capabilities
- `catalog-aws-glue`: AWS Glue catalog backend with SigV4 auth
- `catalog-nessie`: Nessie catalog backend
- `catalog-hms`: Hive Metastore catalog backend (Thrift)
- `catalog-storage-only`: catalog-free table access via path scanning
- `catalog-auth-service-credential`: OAuth2 client_credentials for catalog auth
- `catalog-auth-iam`: AWS IAM SigV4 signing for catalog + storage
- `storage-azure`: Azure ADLS Gen2 / Blob storage backend
- `storage-gcs`: Google Cloud Storage backend
- `storage-s3-compat`: S3-compatible endpoint override (Ceph, R2, SeaweedFS, Garage)
- `storage-local`: local filesystem storage (dev/CI)
- `table-format-delta`: Delta Lake table support via delta-rs (feature flag `delta`)

### Modified Capabilities
- `catalog-integration`: now `IcebergRestBackend` — generalised, Polaris is one configuration

## Impact

- `sqe-catalog`: split into `backend/`, `auth/`, `storage/` submodules; public API unchanged for existing REST catalog users
- `sqe-core`: `CatalogConfig` becomes an enum; existing `[catalog]` TOML section remains valid
- New optional Cargo features: `catalog-hms`, `catalog-nessie`, `catalog-glue`, `storage-azure`, `storage-gcs`, `table-delta`
- Default feature set: `catalog-iceberg-rest`, `storage-s3` (backwards compatible)

## Rollback

Feature flags mean no forced adoption. Existing REST+S3 configs are unchanged.
