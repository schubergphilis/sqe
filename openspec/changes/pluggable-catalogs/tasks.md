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

- [x] 3.1 AWS credential resolution wired in via `aws-credential-types` + `aws_config::defaults` (Phase P, MR !113). Supports env vars, instance profile, `AWS_PROFILE`, SSO. Lives in `vendor/iceberg-rust/crates/catalog/rest/src/sigv4.rs` rather than a freestanding `AwsIamAuth` struct.
- [x] 3.2 Storage S3 wiring already shipped (Phase 1); `StorageConfig` in `sqe-core/src/config.rs` covers `endpoint`, `region`, `path_style`, `access_key`, `secret_key`. Live test exercises it end-to-end against AWS S3 and rustfs.
- [x] 3.3 AWS credentials flow through the SigV4 signer rather than `object_store`'s S3 builder. The `object_store` path keeps using static keys from `StorageConfig`; data-plane vending will land separately when Iceberg 1.12 ships remote signing.
- [x] 3.4 Live integration test: `crates/sqe-catalog/tests/backends_integration.rs::s3_tables::list_namespaces_via_glue_rest` lists namespaces and tables in a real AWS S3 Tables bucket through the federated Glue Iceberg REST endpoint with SigV4 (eu-central-1, Phase P).

## 4. AwsGlueBackend

- [x] 4.1 Implement `GlueBackend` in `sqe-catalog/src/backends/glue.rs`: vendored `iceberg-catalog-glue` from apache/iceberg-rust v0.9.0 with fork-API patches; `GlueBackend::build_catalog` delegates to the upstream crate when the `glue` cargo feature is enabled. Phase K MR !105.
- [x] 4.2 Live verification through `live_glue_namespace_round_trip` against a real AWS account in eu-central-1 (Phase O, MR !113). Engine session-manager dispatch is still REST-only; closing that gap is the deferred Phase O+ refactor (Section 11).
- [x] 4.3 Live test exceeds the unit-test ask: `live_glue_namespace_round_trip` exercises `create_namespace -> list_namespaces -> drop_namespace` against the real Glue API with creds from `.env` (template at `.env.example`, profile name not in source).

## 5. NessieBackend

- [x] 5.1 No dedicated NessieBackend module is needed: Nessie 0.107+ exposes a fully-working Iceberg REST adapter at `/iceberg/`, so SQE talks to it through the existing `iceberg-catalog-rest` client. The 0.76.x line shipped a partial adapter that 404'd on `/iceberg/v1/config`; we pin `ghcr.io/projectnessie/nessie:0.107.5` because that's the first tag where the REST surface is fully usable.
- [x] 5.2 `list_namespaces` covered by the standard Iceberg REST `/v1/{prefix}/namespaces` path through the shared client.
- [x] 5.3 `list_tables` covered by the standard `/v1/{prefix}/namespaces/{ns}/tables` path.
- [x] 5.4 `load_table` flows through the existing iceberg-rust `RestCatalog::load_table` implementation; format-version 3 forwarding via the reserved table property (Phase I) works against Nessie identically.
- [x] 5.5 Live integration test: `crates/sqe-catalog/tests/backends_integration.rs::nessie::nessie_namespace_round_trip` against `docker-compose.nessie.yml` (Phase O).

## 6. HiveMetastoreBackend

- [x] 6.1 Add optional cargo feature `hms` in `sqe-catalog/Cargo.toml`; gate the upstream crate behind it. Default REST-only build pulls zero new deps. Phase K MR !105.
- [x] 6.2 Vendor the upstream HMS Thrift client (`vendor/iceberg-rust/crates/catalog/hms/`) using `hive_metastore` + `volo-thrift` rather than rolling our own.
- [x] 6.3 `list_namespaces` / `list_tables` provided by the vendored `HmsCatalog` (delegates to HMS `GetAllDatabases` / `GetAllTables` via Thrift).
- [x] 6.4 `load_table`: vendored implementation reads `table_parameters['metadata_location']` and loads iceberg metadata via `FileIO`.
- [x] 6.5 Live integration test supersedes the mock: `crates/sqe-catalog/tests/backends_integration.rs::hms::hms_namespace_round_trip` runs against `apache/hive:standalone-metastore-4.1.0` from `docker-compose.hms.yml` (Phase O). Derby + local-fs warehouse keeps the stack self-contained; we use `127.0.0.1` instead of `localhost` to dodge macOS's IPv6-first resolution.

## 7. StorageOnlyBackend

- [x] 7.1 Shipped earlier as the `hadoop` cargo feature; lives at `crates/sqe-catalog/src/backends/hadoop.rs` and reuses the existing `object_store` integration so no extra deps are pulled in.
- [x] 7.2 Auto-discovery: scans `base_path` and walks down looking for `metadata/v*.metadata.json`. Test coverage in `tests/backends_integration.rs::hadoop::auto_discovery`.
- [x] 7.3 Namespace + table name derived from directory path relative to `base_path`.
- [x] 7.4 Explicit table registrations supported.
- [x] 7.5 Discovery cache with TTL configured at the backend level.
- [ ] 7.6 `iceberg_scan(path)` TVF: not yet wired. Tracked as a follow-up; the storage-only backend already covers the `[catalog]` config case.
- [x] 7.7 Integration test: `tests/backends_integration.rs::hadoop::auto_discovery` covers the end-to-end discovery path.

## 8. Delta Lake Support (deferred to a later change)

Out of scope for this change. The catalog work converged on Iceberg-only because that's the format every backend we exercised actually serves. Delta Lake support belongs in its own change once the engine wiring (Section 11) lands and `CatalogBackend::table_format()` exists as the natural extension point.

- [ ] 8.1 Add `delta` Cargo feature; gate `deltalake` dependency behind it
- [ ] 8.2 Implement `DeltaTableProvider` wrapper: when `CatalogBackend::table_format()` returns `Delta`, use `deltalake::open_table()` instead of iceberg-rust
- [ ] 8.3 Register Delta provider in DataFusion `SessionContext` alongside Iceberg provider
- [ ] 8.4 Integration test: SELECT from a Delta table via Unity Catalog REST endpoint

## 9. Azure Storage Backend (deferred to a later change)

Out of scope for this change. AWS S3 + S3-compatible (Ceph, R2, rustfs) plus local filesystem cover Phase O. Azure goes alongside GCS in a dedicated multi-cloud-storage change.

- [ ] 9.1 Implement `StorageConfig::Azure` in `sqe-catalog/src/storage/azure.rs`
- [ ] 9.2 Support `access_key`, `sas_token`, and `workload_identity` credential types
- [ ] 9.3 Wire into `object_store` Azure builder
- [ ] 9.4 Integration test: read Iceberg table parquet files from Azure Blob / ADLS container (Azurite emulator)

## 10. GCS Storage Backend (deferred to a later change)

Out of scope for this change. Bundled with Section 9 in the future multi-cloud-storage change.

- [ ] 10.1 Implement `StorageConfig::Gcs` in `sqe-catalog/src/storage/gcs.rs`
- [ ] 10.2 Support `service_account` (key file) and `workload_identity` credential types
- [ ] 10.3 Wire into `object_store` GCS builder
- [ ] 10.4 Integration test: read Iceberg table from GCS (fake-gcs-server emulator)

## 11. Config + Factory Wiring

The trait-based factory is intentionally deferred. Sections 4-7 ship live-tested catalog backends through the existing `SessionCatalog::new` path (REST only) plus per-backend `build_catalog` constructors gated by cargo features. Closing the engine wiring gap means refactoring 13 `SessionCatalog::new` call sites in `crates/sqe-coordinator/` to dispatch through a `CatalogBackend` trait. That's a separate phase (working name: Phase O+) tracked here. None of the Phase O / Phase P live tests need it because they hit the catalog libraries directly through `iceberg::Catalog`.

- [ ] 11.1 Define `CatalogConfig` enum (one variant per backend) + `CatalogAuthConfig` + `StorageConfig` in `sqe-core`
- [ ] 11.2 Factory: `build_catalog(config) -> (Arc<dyn CatalogBackend>, Arc<dyn CatalogAuth>, ObjectStore)`
- [ ] 11.3 Default config (existing `[catalog] type="iceberg_rest"`) unchanged — backwards compat
- [ ] 11.4 Unit test: each catalog type deserialises from TOML example; factory returns correct backend type
- [ ] 11.5 Refactor 13 `SessionCatalog::new` call sites in coordinator crates (catalog_ops.rs, query_handler.rs, write_handler.rs, maintenance.rs, session_context.rs) to take an `Arc<dyn CatalogBackend>` from the factory instead of constructing REST directly.

## 12. Phase O + Phase P deltas (2026-04 / 2026-04)

These items landed on `feat/matrix-phase-o-live-catalogs` (MR !113) and are checked off here for posterity. They cut across the sections above.

- [x] 12.1 `crates/sqe-catalog/Cargo.toml` default features expanded to `rest + sql-postgres + hms + glue + hadoop`. Default cargo build now ships every supported catalog backend compiled in. Picking a backend stays a runtime config concern, not a build-time one.
- [x] 12.2 Live test infrastructure: `docker-compose.hms.yml` (apache/hive:standalone-metastore-4.1.0 with bundled Derby + local-fs warehouse), `docker-compose.nessie.yml` (ghcr.io/projectnessie/nessie:0.107.5 with in-memory version store), `docker-compose.test.yml` postgres reused for JDBC. All three layer on top of the existing test stack.
- [x] 12.3 `.env` scaffolding for live AWS tests. `.env.example` is the committed template documenting `AWS_PROFILE`, `AWS_REGION`, `SQE_TEST_GLUE_WAREHOUSE`, `SQE_TEST_S3TABLES_WAREHOUSE`. `.env` itself is gitignored; `!.env.example` allowlist added so `cp .env.example .env` works.
- [x] 12.4 SigV4 auth in the vendored `iceberg-catalog-rest` (Phase P): new `aws-sigv4` cargo feature (default-on for SQE), new `sigv4.rs` module that reads creds from the standard AWS provider chain and signs each outgoing `reqwest::Request` inside the existing `HttpClient::authenticate` path. Engaged whenever `rest.sigv4-enabled=true` is on user props or surfaces in the server's `/v1/config` defaults.
- [x] 12.5 Five live tests in `crates/sqe-catalog/tests/backends_integration.rs`: `hms::hms_namespace_round_trip`, `nessie::nessie_namespace_round_trip`, `sql_postgres::jdbc_postgres_namespace_roundtrip`, `glue::live_glue_namespace_round_trip`, `s3_tables::list_namespaces_via_glue_rest`. All `#[ignore]` (need external services) and pass against the documented stacks.
- [x] 12.6 Matrix state JSON updated: `docs/iceberg-matrix-state.json` flips hive-metastore v2/v3, nessie v3, aws-glue-catalog v2/v3 from partial to full. Score 153/189 (81.0%) -> 158/189 (83.6%). Phase P enriched the rest-catalog and aws-glue-catalog cells with the SigV4 path evidence; the rubric counts cells not capabilities so the score is unchanged by Phase P alone.
