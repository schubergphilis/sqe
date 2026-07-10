## ADDED Requirements

### Requirement: HiveMetastoreBackend adopts apache/iceberg-rust workspace crate

The system SHALL use the `iceberg-catalog-hms` crate from the apache/iceberg-rust workspace to connect to Apache Hive Metastore over Thrift. The backend SHALL support table listing, table read, table create, and table update (with locking per HMS protocol).

#### Scenario: List tables from HMS

- **GIVEN** `catalog.type = "hms"` with `uri = "thrift://hms.example.com:9083"`
- **WHEN** the user runs `SHOW TABLES`
- **THEN** Iceberg tables from the HMS namespaces are listed
- **AND** non-Iceberg Hive tables are filtered out

#### Scenario: Write via HMS with locking

- **WHEN** a user runs INSERT targeting an HMS-managed table
- **THEN** the backend acquires the HMS table-level lock before commit
- **AND** releases the lock after commit (success or failure)
- **AND** a concurrent writer is serialised behind the lock

#### Scenario: HMS unavailable falls fast

- **GIVEN** the HMS Thrift endpoint is unreachable
- **WHEN** the user runs any catalog operation
- **THEN** the error is surfaced within 5 seconds
- **AND** does NOT cascade silently

### Requirement: JdbcCatalogBackend via iceberg-catalog-sql

The system SHALL adopt `iceberg-catalog-sql` from the apache/iceberg-rust workspace to support JDBC-style catalogs backed by PostgreSQL, MySQL, or SQLite. The backend SHALL handle both `$N` (Postgres) and `?` (MySQL/SQLite) placeholder styles automatically.

#### Scenario: PostgreSQL catalog connection

- **GIVEN** `catalog.type = "jdbc"` with `url = "postgresql://catalog.example.com/iceberg"`
- **WHEN** the user runs `SHOW TABLES`
- **THEN** tables from the PostgreSQL catalog schema are listed

#### Scenario: SQLite for local dev

- **GIVEN** `catalog.type = "jdbc"` with `url = "sqlite:///tmp/catalog.db"`
- **WHEN** the user runs any catalog operation
- **THEN** the SQLite file is used as the catalog store
- **AND** local dev workflows work without a running catalog server

### Requirement: Unity Catalog OIDC machine-to-machine auth

The system SHALL support OIDC client-credentials (M2M) flow for Unity Catalog REST access in addition to existing PAT auth. A new auth provider `OidcM2mAuth` SHALL be available in the auth chain.

#### Scenario: M2M flow acquires catalog token

- **GIVEN** `catalog.auth.type = "oidc_m2m"` with `client_id` and `client_secret` configured
- **WHEN** a catalog call is made
- **THEN** the auth provider posts to the token endpoint with `grant_type=client_credentials`
- **AND** the returned access token is cached until expiry
- **AND** catalog requests include the token as a Bearer header

#### Scenario: Token refresh before expiry

- **GIVEN** a cached token with 60s remaining lifetime
- **WHEN** the next catalog call fires
- **THEN** the token is preemptively refreshed
- **AND** no call fails with 401 due to expiry

### Requirement: Hadoop storage-only backend for metadata/v*.metadata.json discovery

The system SHALL support a Hadoop-style catalog mode that lists tables by scanning a warehouse path for `metadata/v*.metadata.json` files, picking the highest version per table. This is additive to the existing `StorageOnlyBackend` which uses a single-path auto-discovery.

#### Scenario: Hadoop warehouse scan

- **GIVEN** `catalog.type = "hadoop"` with `warehouse = "s3://lake/warehouse"`
- **AND** the warehouse contains `s3://lake/warehouse/ns/t/metadata/v00001.metadata.json` and `v00002.metadata.json`
- **WHEN** the user runs `SHOW TABLES`
- **THEN** table `ns.t` is listed
- **AND** v00002 is chosen as the current metadata

### Requirement: Catalog backends gated by Cargo features

The system SHALL expose each additional catalog backend as a Cargo feature flag in the `sqe-catalog` crate. Default features include `rest` only. Optional features: `glue`, `hms`, `sql`, `hadoop`. Users building SQE SHALL pick the catalogs they need.

#### Scenario: Minimal build excludes Glue dependencies

- **WHEN** `sqe-catalog` is built with `--no-default-features --features rest`
- **THEN** the `aws-sdk-glue` dependency is NOT linked
- **AND** the binary size is smaller than a full-feature build

#### Scenario: Full build includes all backends

- **WHEN** `sqe-catalog` is built with `--features glue,hms,sql,hadoop`
- **THEN** all backends are available at runtime
- **AND** the `CatalogBackend` registry includes all five variants
