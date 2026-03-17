## 1. Workspace Setup

- [x] 1.1 Initialize Cargo workspace with all 10 crates and two binary targets
- [x] 1.2 Set up shared dependencies (DataFusion, Arrow, tokio, serde) in workspace Cargo.toml
- [x] 1.3 Create sqe.toml.example with default config
- [x] 1.4 Create sqe-core: config parsing (sqe.toml), error types, shared types (Session, SessionUser, ObjectReference)

## 2. Authentication (sqe-auth)

- [x] 2.1 Implement Keycloak OIDC client: token acquisition via ROPC grant
- [x] 2.2 Implement token cache with expiry tracking (DashMap keyed by session_id)
- [x] 2.3 Implement background token refresh task (60s before expiry, refresh_token first, mark expired on failure)
- [x] 2.4 Implement Session struct carrying user identity + access_token + roles
- [ ] 2.5 Unit tests: token acquisition, refresh, expiry handling
- [ ] 2.6 Integration test: authenticate against quickstart Keycloak with test users

## 3. Catalog Integration (sqe-catalog)

- [x] 3.1 Wrap iceberg-rust REST catalog client with per-session bearer token injection
- [x] 3.2 Implement S3 credential vending: extract vended creds from Polaris loadTable response
- [x] 3.3 Implement credential caching per (session, table) with TTL from vended expiry
- [x] 3.4 Implement static S3 key fallback when Polaris doesn't vend credentials
- [x] 3.5 Implement token fingerprinting in catalog session IDs
- [x] 3.6 Implement DataFusion CatalogProvider trait backed by Polaris namespaces
- [x] 3.7 Implement DataFusion SchemaProvider trait backed by Polaris tables listing
- [x] 3.8 Implement DataFusion TableProvider using iceberg-rust IcebergTableProvider
- [x] 3.9 Implement metadata caching with 30s TTL
- [ ] 3.10 Integration test: list catalogs/schemas/tables via Polaris with user token
- [ ] 3.11 Integration test: SELECT from Iceberg table via Polaris with vended S3 creds

## 4. SQL Layer (sqe-sql)

- [x] 4.1 Implement statement classification: Query, DDL, DML, View, Catalog, InfoSchema, Policy, Utility
- [x] 4.2 Implement statement routing to appropriate handlers
- [x] 4.3 Parse and route write path SQL: CTAS, INSERT INTO, MERGE INTO, DELETE FROM, DROP TABLE, ALTER TABLE RENAME
- [x] 4.4 Parse and route view SQL: CREATE VIEW, DROP VIEW
- [x] 4.5 Parse and stub policy SQL: GRANT, REVOKE, SHOW GRANTS (return "not configured")
- [x] 4.6 Parse catalog SQL: SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES
- [x] 4.7 Unit tests: statement classification for all statement types

## 5. Policy Stub (sqe-policy)

- [x] 5.1 Define PolicyEnforcer trait with evaluate(user, plan) -> Result<LogicalPlan>
- [x] 5.2 Implement PassthroughEnforcer (returns plan unmodified)
- [x] 5.3 Wire PassthroughEnforcer into coordinator's query path

## 6. Query Engine & Planner (sqe-planner)

- [x] 6.1 Implement query planning: parse → LogicalPlan via DataFusion SQL planner
- [x] 6.2 Wire policy enforcement into plan pipeline (passthrough)
- [ ] 6.3 Implement DataFusion optimizer pass with Iceberg predicate pushdown
- [x] 6.4 Implement PhysicalPlan generation from optimized LogicalPlan
- [x] 6.5 Implement adaptive fragment splitting: extract Iceberg manifest groups from PhysicalPlan
- [ ] 6.6 Implement custom datafusion-proto codec extensions for iceberg-rust plan nodes
- [x] 6.7 Unit tests: fragment splitting for small (1 manifest) and large (100+ manifests) tables

## 7. Coordinator (sqe-coordinator)

- [x] 7.1 Implement Flight SQL server (arrow-flight crate) with auth handshake → sqe-auth
- [x] 7.2 Implement session management: create/track/expire sessions
- [x] 7.3 Implement statement routing: classify → dispatch to appropriate handler
- [x] 7.4 Implement local execution path (single-node mode via DataFusion SessionContext)
- [x] 7.5 Implement worker registry with heartbeat-based liveness tracking
- [ ] 7.6 Implement fragment scheduler: assign fragments to workers with load weighting
- [x] 7.7 Implement distributed dispatch: send fragments via Arrow Flight do_exchange with credentials in metadata
- [x] 7.8 Implement result collection: merge Arrow RecordBatch streams from workers
- [x] 7.9 Implement local/distributed decision logic (adaptive based on table size and worker availability)
- [ ] 7.10 Implement credential refresh push to workers for long-running queries
- [ ] 7.11 Implement failure handling: re-assign read fragments on worker death, local fallback
- [ ] 7.12 Integration test: single-node SELECT query end-to-end (Flight SQL → Polaris → S3 → results)
- [ ] 7.13 Integration test: authenticate as different users, verify different catalog visibility

## 8. Write Path (sqe-coordinator)

- [x] 8.1 Implement CTAS: execute SELECT → infer schema → create table in Polaris → write Parquet → commit snapshot
- [x] 8.2 Implement CREATE OR REPLACE TABLE: new snapshot replacement, old snapshots retained
- [x] 8.3 Implement INSERT INTO SELECT: execute SELECT → write new data files → append snapshot
- [ ] 8.4 Implement DELETE FROM: scan with predicate → write position delete files → commit
- [ ] 8.5 Implement MERGE INTO: scan target+source → join → classify → position deletes + new data → atomic commit
- [x] 8.6 Implement DROP TABLE / DROP TABLE IF EXISTS: Polaris REST delete
- [x] 8.7 Implement ALTER TABLE RENAME: Polaris REST rename
- [x] 8.8 Implement CREATE VIEW: serialize SQL to Polaris REST view API
- [x] 8.9 Implement DROP VIEW: Polaris REST view delete
- [x] 8.10 Implement view resolution on read: resolve view SQL → parse → inline into query plan
- [ ] 8.11 Integration test: CTAS → SELECT roundtrip
- [ ] 8.12 Integration test: INSERT INTO → verify appended data
- [ ] 8.13 Integration test: MERGE INTO → verify upserted data
- [ ] 8.14 Integration test: DELETE FROM → verify rows removed
- [ ] 8.15 Integration test: DROP TABLE → verify removed from Polaris
- [ ] 8.16 Integration test: CREATE VIEW → query view → verify results

## 9. Worker (sqe-worker)

- [x] 9.1 Implement Flight server: receive plan fragments + credentials from coordinator
- [x] 9.2 Implement fragment deserialization (JSON-based ScanTask, not proto codec)
- [x] 9.3 Implement local DataFusion execution with injected user credentials
- [x] 9.4 Implement RecordBatch streaming back to coordinator
- [ ] 9.5 Implement heartbeat to coordinator (5s interval)
- [ ] 9.6 Implement credential update channel: accept refreshed tokens from coordinator
- [ ] 9.7 Implement configurable memory limit and spill-to-disk
- [ ] 9.8 Integration test: coordinator + 2 workers → distributed SELECT → verify correct results

## 10. information_schema (sqe-catalog)

- [x] 10.1 Implement InfoSchemaTablesProvider: virtual TableProvider querying Polaris listTables
- [x] 10.2 Implement InfoSchemaColumnsProvider: Iceberg table schema → column metadata
- [x] 10.3 Implement InfoSchemaSchemataProvider: Polaris listNamespaces
- [x] 10.4 Register information_schema as virtual schema per session
- [ ] 10.5 Integration test: SELECT from information_schema.tables/columns/schemata

## 11. Trino Wire Compatibility (sqe-trino-compat)

- [x] 11.1 Implement axum HTTP server for Trino v1/statement endpoints
- [x] 11.2 Implement POST /v1/statement: auth + SQL submission + first result page
- [ ] 11.3 Implement GET /v1/statement/{id}/{token}: result pagination
- [x] 11.4 Implement DELETE /v1/statement/{id}: query cancellation
- [x] 11.5 Implement Arrow → Trino JSON column format type mapping
- [x] 11.6 Implement dual auth on /v1/statement: Bearer token (JWT passthrough from backend + X-Trino-User) and Basic auth (Keycloak ROPC), Bearer takes priority
- [ ] 11.7 Implement X-Trino-Catalog/Schema/User/Source header handling
- [x] 11.8 Implement Trino /v1/info endpoint: node version, environment, coordinator flag, starting state, uptime
- [x] 11.9 Implement Trino /v1/info/state endpoint: ACTIVE / STARTING state string
- [ ] 11.10 Integration test: connect via Trino JDBC driver → execute query → verify results

## 12. Observability (sqe-metrics)

- [x] 12.1 Implement Prometheus /metrics endpoint with core metrics
- [x] 12.2 Instrument coordinator: query counts, durations, active sessions, worker counts
- [ ] 12.3 Instrument workers: fragments executed, rows scanned, bytes read
- [x] 12.4 Implement structured JSON audit log writer
- [x] 12.5 Implement optional OpenTelemetry trace export via OTLP
- [ ] 12.6 Propagate trace context to workers via Flight metadata
- [x] 12.7 Implement /healthz liveness and /readyz readiness probes on health port
- [x] 12.8 Implement /api/v1/status Ballista/DataFusion-style cluster status endpoint (role, version, uptime, workers)

## 13. Docker & Integration

- [x] 13.1 Create unified Dockerfile (multi-stage Rust build, single sqe-server binary for coordinator + worker)
- [x] 13.2 Create docker-compose.yml connecting SQE to existing quickstart network
- [ ] 13.3 Register sqe-client in Keycloak realm config (same config as trino-client)
- [ ] 13.4 End-to-end test: docker-compose up → Flight SQL connect → SELECT → verify results
- [ ] 13.5 End-to-end test: docker-compose up → Trino JDBC connect → SELECT → verify results
