## ADDED Requirements

### Requirement: Trino v1/statement HTTP protocol
The system SHALL implement the Trino REST protocol for query submission and result pagination.

#### Scenario: Submit query via Trino protocol
- **WHEN** a client POSTs SQL to `/v1/statement` with basic auth
- **THEN** the query is authenticated and executed
- **AND** the first page of results is returned in Trino JSON column format with a nextUri

#### Scenario: Paginate results
- **GIVEN** a query with results spanning multiple pages
- **WHEN** the client GETs the nextUri
- **THEN** the next page of results is returned in Trino JSON format

#### Scenario: Cancel query
- **WHEN** a client DELETEs `/v1/statement/{id}`
- **THEN** the query is cancelled and resources are freed

### Requirement: Trino /v1/info server information
The system SHALL implement the Trino `/v1/info` and `/v1/info/state` endpoints on the Trino HTTP port for compatibility with Trino clients and monitoring tools.

#### Scenario: Server info endpoint
- **WHEN** a client GETs `/v1/info`
- **THEN** a JSON response is returned matching Trino's format with:
  - `nodeVersion.version`: SQE version
  - `environment`: `"production"`
  - `coordinator`: `true`
  - `starting`: `false` when ready, `true` during startup
  - `uptime`: human-readable uptime string

#### Scenario: Server state endpoint
- **WHEN** a client GETs `/v1/info/state`
- **THEN** `"ACTIVE"` is returned when the server is ready
- **AND** `"STARTING"` is returned during initialization

### Requirement: Trino session properties
The system SHALL support basic Trino session properties for catalog and schema selection.

#### Scenario: Set catalog and schema via headers
- **WHEN** a client includes `X-Trino-Catalog` and `X-Trino-Schema` headers
- **THEN** the query executes in the specified catalog/schema context

### Requirement: Trino type mapping
The system SHALL map Arrow/Iceberg types to Trino JSON wire format types for result serialization.

#### Scenario: Arrow types serialized to Trino format
- **GIVEN** query results with Arrow types (Utf8, Int64, TimestampMicro, etc.)
- **WHEN** results are serialized for the Trino client
- **THEN** types are mapped to Trino's JSON column format (varchar, bigint, timestamp, etc.)
