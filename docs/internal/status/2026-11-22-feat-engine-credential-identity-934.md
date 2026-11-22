**STATUS 2026-11-22: engine credential identity (`feat/engine-credential-identity-934`, #934).**

OpenSpec change: `openspec/changes/engine-credential-identity/`.

- Config: `CatalogConfig.credentials` (`sqe-core/src/config/engine_credentials.rs`); startup refuses it without `client_credentials`, `require_vended_credentials`, the Ranger backend and `catalog_scoped`.
- Catalog: `sqe-catalog/src/engine_identity.rs` (traits, markers) and `rest_catalog/engine_credentials.rs` (read load, gate, key fetch, write view).
- Coordinator: `engine_identity_wiring.rs` binds the policy gate, token and audit per session; store and audit are process-wide.
- Ranger: `engine_catalogs` default-deny `select` under `catalog_scoped`.
- Writes: every write path loads through the person's delegated client (`load_table_for_write`, `into_write_catalog`, the catalog bridge); the DML storage probe refuses an engine-keyed table.
- Gate: `make test-integration FILTER=engine_credential` (granted read on engine keys; INSERT/UPDATE/DELETE as the writer; denied read 0 rows with a fallible predicate; denied DELETE no-op; metadata-only person reads; metadata-only person and reader refused every DML kind, CTAS and compaction, no object written) and `FILTER=dml_write_precheck`.
- Limits: views over a sibling warehouse refused from an identity catalog; the KMS client is lost when the read client is rebuilt; denied tables return empty metadata TVFs. The Ranger stack does not vend, so the IT uses the in-memory store.
- Size budgets over on `ranger_store.rs`, sqe-coordinator and sqe-core; pushed with `SKIP=budgets`.

**NEXT:** operator step before enabling on a deployment: add Ranger `select` policies for the catalog's tables; portal wiring in ai-portal-data.
