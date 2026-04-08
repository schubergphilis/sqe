## Why

The Chameleon platform currently runs a patched Trino fork (DCAF branch) to pass user Keycloak tokens through to Polaris (Iceberg REST catalog) and S3. Maintaining patches against Trino's Java monolith is expensive and fragile — every Trino upgrade risks breaking the auth passthrough. SQE replaces this with a purpose-built Rust engine where per-user auth is the core design, not a bolt-on.

## What Changes

- New distributed SQL query engine built on DataFusion with custom coordinator/worker architecture over Arrow Flight
- Keycloak OIDC authentication with token passthrough to Polaris REST catalog and S3 credential vending — no service accounts
- Full write path: CTAS, CREATE OR REPLACE, INSERT INTO, MERGE INTO, DELETE FROM, DROP TABLE, ALTER TABLE RENAME, CREATE/DROP VIEW
- Arrow Flight SQL as primary client protocol
- Trino v1/statement HTTP wire compatibility for existing dashboards (DBeaver, Tableau, Superset)
- Basic `information_schema` virtual schema for Trino compat and schema browsing
- PolicyEnforcer trait stub for future OPA/Cedar integration (passthrough only in this change)
- Connects to existing quickstart infrastructure (Polaris, Keycloak, MinIO)

## Capabilities

### New Capabilities
- `auth-passthrough`: Keycloak OIDC token acquisition (ROPC), session management, token refresh, bearer token propagation to Polaris and workers
- `catalog-integration`: Iceberg REST catalog via iceberg-rust with per-user bearer tokens, S3 credential vending, metadata caching
- `query-engine`: DataFusion-based SQL execution with statement classification, plan optimization, and streaming results
- `write-path`: CTAS, INSERT INTO, MERGE INTO, DELETE FROM, DROP TABLE, ALTER TABLE RENAME, CREATE/DROP VIEW via Iceberg commits
- `distributed-execution`: Custom coordinator/worker architecture with adaptive fragment splitting, Arrow Flight transport, credential propagation, and petabyte-scale streaming
- `flight-sql-server`: Arrow Flight SQL server for JDBC client connectivity
- `trino-compat`: Trino v1/statement HTTP wire protocol adapter for existing client migration
- `information-schema`: Virtual information_schema (tables, columns, schemata) backed by Polaris catalog metadata
- `observability`: Prometheus metrics, OpenTelemetry traces, structured query audit log

### Modified Capabilities

(none — this is a greenfield project)

## Impact

- **New Rust workspace**: 10 crates under `crates/`, two binaries (coordinator + worker)
- **New Keycloak client**: `sqe-client` registered in realm `iceberg` with same config as `trino-client`
- **Infrastructure**: Docker images for coordinator and worker, docker-compose for local dev connecting to existing quickstart network
- **Dependencies**: DataFusion, iceberg-rust, Arrow Flight, sqlparser-rs, axum, tokio, reqwest, moka, serde
- **Testing**: Integration tests run against existing `data-platform/quickstart/full/` stack
