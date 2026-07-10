## ADDED Requirements

### Requirement: Per-session Iceberg REST catalog
The system SHALL create per-session catalog instances using the user's bearer token, ensuring catalog operations are scoped to the authenticated user.

#### Scenario: User-scoped table listing
- **GIVEN** an authenticated user with limited Polaris permissions
- **WHEN** the user lists tables
- **THEN** only tables accessible to that user are returned

### Requirement: S3 credential vending
The system SHALL use Polaris-vended S3 credentials (STS) for data file access instead of static S3 keys.

#### Scenario: Credential vending on table load
- **GIVEN** Polaris is configured with S3 credential vending
- **WHEN** a table is loaded via `GET /v1/namespaces/{ns}/tables/{table}`
- **THEN** the response includes vended S3 credentials (access_key, secret_key, session_token, expiry)
- **AND** these credentials are used for all S3 reads/writes for that table

#### Scenario: Credential vending fallback to static keys
- **GIVEN** Polaris does not vend S3 credentials (e.g., MinIO dev setup)
- **WHEN** a table is loaded
- **THEN** static S3 credentials from the `[storage]` config section are used

#### Scenario: Credential re-vending for long queries
- **GIVEN** a query outliving the vended credential TTL
- **WHEN** the coordinator detects credential expiry approaching
- **THEN** the coordinator re-vends by calling Polaris with the refreshed bearer token
- **AND** pushes updated S3 credentials to workers

### Requirement: DataFusion catalog/schema/table provider integration
The system SHALL implement DataFusion's CatalogProvider, SchemaProvider, and TableProvider traits backed by iceberg-rust's REST catalog.

#### Scenario: DataFusion resolves Iceberg table
- **GIVEN** a table `production.finance.transactions` exists in Polaris
- **WHEN** DataFusion plans a query referencing that table
- **THEN** the IcebergTableProvider is used for scan planning with manifest filtering and partition pruning

### Requirement: Token fingerprinting in session ID
The system SHALL include a token fingerprint in catalog session IDs to invalidate iceberg-rust's internal REST session cache on token refresh.

#### Scenario: Token refresh invalidates catalog cache
- **GIVEN** an active session whose token has been refreshed
- **WHEN** the next catalog operation occurs
- **THEN** a new iceberg-rust REST session is created (not reusing the stale cached one)
