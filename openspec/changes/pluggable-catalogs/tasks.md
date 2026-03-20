## 1. CatalogAuth + CatalogBackend Traits

- [ ] 1.1 Define `CatalogAuth` trait, `CatalogCredential` enum, `CatalogBackend` trait, `TableFormat` enum in `sqe-catalog/src/traits.rs`
- [ ] 1.2 Implement `PassthroughCatalogAuth` (forward user bearer token)
- [ ] 1.3 Implement `NoCatalogAuth` (anonymous)
- [ ] 1.4 Refactor existing `IcebergRestCatalog` to implement `CatalogBackend` trait
- [ ] 1.5 Update coordinator to use `Arc<dyn CatalogBackend>` + `Arc<dyn CatalogAuth>`
- [ ] 1.6 Unit test: trait objects are dyn-safe; passthrough forwards token correctly

## 2. ServiceCredentialAuth

- [ ] 2.1 Implement `ServiceCredentialAuth` in `sqe-catalog/src/auth/service_credential.rs`
- [ ] 2.2 POST `grant_type=client_credentials` to configured token URL
- [ ] 2.3 Cache resulting token (moka, expire at `expires_in - 30s`)
- [ ] 2.4 Unit test: mock token endpoint; expired token triggers refresh

## 3. AwsIamAuth + Storage S3

- [ ] 3.1 Implement `AwsIamAuth` via `aws-credential-types`; support env, instance profile, explicit key
- [ ] 3.2 Implement `StorageConfig::S3` wiring in `sqe-catalog/src/storage/s3.rs`; support `endpoint` override and `path_style`
- [ ] 3.3 Wire `AwsIamAuth` credentials into `object_store` S3 builder
- [ ] 3.4 Integration test: list tables via Glue Iceberg REST endpoint with SigV4 (Localstack or real AWS)

## 4. AwsGlueBackend

- [ ] 4.1 Implement `AwsGlueBackend` in `sqe-catalog/src/backend/glue.rs` (wraps `IcebergRestBackend` with Glue endpoint + `AwsIamAuth`)
- [ ] 4.2 Config: `type = "aws_glue"`, `region`; auth section
- [ ] 4.3 Unit test: Glue backend constructs correct Iceberg REST URL for region

## 5. NessieBackend

- [ ] 5.1 Implement `NessieBackend` in `sqe-catalog/src/backend/nessie.rs`
- [ ] 5.2 `list_namespaces` → `GET /api/v2/trees/{ref}/namespaces`
- [ ] 5.3 `list_tables` → `GET /api/v2/trees/{ref}/entries?filter=type==ICEBERG_TABLE`
- [ ] 5.4 `load_table` → resolve content to metadata location → load via iceberg-rust `FileIO`
- [ ] 5.5 Integration test: list + read from local Nessie container

## 6. HiveMetastoreBackend

- [ ] 6.1 Add optional feature `catalog-hms`; gate behind `#[cfg(feature = "catalog-hms")]`
- [ ] 6.2 Implement HMS Thrift client in `sqe-catalog/src/backend/hms.rs` (using `hive_metastore` crate or raw Thrift)
- [ ] 6.3 `list_namespaces` / `list_tables` via HMS `GetAllDatabases` / `GetAllTables`
- [ ] 6.4 `load_table`: read `table_parameters['metadata_location']` → load iceberg metadata via `FileIO`
- [ ] 6.5 Unit test: mock HMS response → correct `TableMetadata` extracted

## 7. StorageOnlyBackend

- [ ] 7.1 Implement `StorageOnlyBackend` in `sqe-catalog/src/backend/storage_only.rs`
- [ ] 7.2 Auto-discovery: scan `base_path` up to `scan_depth` levels; detect `metadata/v*.metadata.json`
- [ ] 7.3 Derive namespace + table name from directory path relative to `base_path`
- [ ] 7.4 Explicit table registrations: load from `[[catalog.tables]]` config entries
- [ ] 7.5 Cache discovery results with configurable TTL (default 5 min, `SHOW TABLES` triggers refresh)
- [ ] 7.6 `iceberg_scan(path)` TVF: register a `TableValuedFunction` in DataFusion that wraps one-shot `StorageOnlyBackend` for arbitrary paths
- [ ] 7.7 Integration test: point at local filesystem with two Iceberg tables; `SHOW TABLES` discovers both; `SELECT` reads data

## 8. Delta Lake Support (feature flag)

- [ ] 8.1 Add `delta` Cargo feature; gate `deltalake` dependency behind it
- [ ] 8.2 Implement `DeltaTableProvider` wrapper: when `CatalogBackend::table_format()` returns `Delta`, use `deltalake::open_table()` instead of iceberg-rust
- [ ] 8.3 Register Delta provider in DataFusion `SessionContext` alongside Iceberg provider
- [ ] 8.4 Integration test: SELECT from a Delta table via Unity Catalog REST endpoint

## 9. Azure Storage Backend

- [ ] 9.1 Implement `StorageConfig::Azure` in `sqe-catalog/src/storage/azure.rs`
- [ ] 9.2 Support `access_key`, `sas_token`, and `workload_identity` credential types
- [ ] 9.3 Wire into `object_store` Azure builder
- [ ] 9.4 Integration test: read Iceberg table parquet files from Azure Blob / ADLS container (Azurite emulator)

## 10. GCS Storage Backend

- [ ] 10.1 Implement `StorageConfig::Gcs` in `sqe-catalog/src/storage/gcs.rs`
- [ ] 10.2 Support `service_account` (key file) and `workload_identity` credential types
- [ ] 10.3 Wire into `object_store` GCS builder
- [ ] 10.4 Integration test: read Iceberg table from GCS (fake-gcs-server emulator)

## 11. Config + Factory Wiring

- [ ] 11.1 Define `CatalogConfig` enum (one variant per backend) + `CatalogAuthConfig` + `StorageConfig` in `sqe-core`
- [ ] 11.2 Factory: `build_catalog(config) -> (Arc<dyn CatalogBackend>, Arc<dyn CatalogAuth>, ObjectStore)`
- [ ] 11.3 Default config (existing `[catalog] type="iceberg_rest"`) unchanged — backwards compat
- [ ] 11.4 Unit test: each catalog type deserialises from TOML example; factory returns correct backend type
