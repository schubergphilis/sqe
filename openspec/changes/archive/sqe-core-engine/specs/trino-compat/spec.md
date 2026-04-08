## ADDED Requirements

### Requirement: Trino v1/statement HTTP protocol
The system SHALL implement the Trino REST protocol for query submission and result pagination.

#### Scenario: Submit query via basic auth
- **WHEN** a client POSTs SQL to `/v1/statement` with `Authorization: Basic <base64>`
- **THEN** the credentials are exchanged with Keycloak via ROPC grant
- **AND** the query is executed as the authenticated user
- **AND** the first page of results is returned in Trino JSON column format with a nextUri

#### Scenario: Submit query via bearer token
- **WHEN** a client POSTs SQL to `/v1/statement` with `Authorization: Bearer <jwt>` and `X-Trino-User` header
- **THEN** the JWT is used directly as the session access token (no Keycloak round-trip)
- **AND** the username is taken from the `X-Trino-User` header
- **AND** the query is executed as the identified user

### Requirement: Dual authentication on Trino HTTP endpoint
The system SHALL support both Bearer token and Basic auth on the Trino `/v1/statement` endpoint, with Bearer taking priority when both are present.

#### Scenario: Bearer token preferred over basic auth
- **GIVEN** a request with both `Authorization: Bearer <jwt>` and basic auth credentials
- **WHEN** the request is processed
- **THEN** the Bearer token is used (basic auth is ignored)

#### Scenario: Missing authorization
- **WHEN** a client POSTs SQL without an `Authorization` header
- **THEN** HTTP 401 is returned with a Trino error response

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
