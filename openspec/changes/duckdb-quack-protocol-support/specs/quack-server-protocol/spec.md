## ADDED Requirements

### Requirement: Quack RPC listener
The system SHALL accept incoming TCP connections on a configurable address and speak the Quack RPC protocol.

#### Scenario: DuckDB CLI attaches via quack URI
- **GIVEN** SQE is running with `[quack_server]` enabled on port 9000
- **AND** a user with a valid OIDC bearer token `<jwt>`
- **WHEN** the user runs `CREATE SECRET (TYPE quack, TOKEN '<jwt>'); ATTACH 'quack:localhost:9000' AS sqe;` from DuckDB CLI
- **THEN** the connection is established and `AttachOk` is returned with the catalog descriptor

#### Scenario: Protocol version mismatch
- **GIVEN** a DuckDB client sends a `Hello` frame with `proto_version = 99`
- **WHEN** SQE does not support version 99
- **THEN** the server returns `Error(UNSUPPORTED_VERSION)` and closes the connection

### Requirement: Token-based authentication
The system SHALL map the Quack `TOKEN '...'` value to an OIDC bearer and validate it via `sqe-auth`.

#### Scenario: Valid token attaches successfully
- **GIVEN** a user with a valid OIDC bearer token
- **WHEN** the client sends `Auth(token = "<jwt>")`
- **THEN** `sqe-auth::validate_bearer` is called with the token
- **AND** on success, `AuthOk` is returned and a `SqeSession` is bound to the connection

#### Scenario: Invalid token closes connection
- **GIVEN** an expired or malformed OIDC token
- **WHEN** the client sends `Auth(token = "<invalid>")`
- **THEN** the server returns `Error(AUTH_FAILED)` with the validation reason
- **AND** the connection is closed

### Requirement: Catalog browsing
The system SHALL expose the existing Iceberg catalog through Quack `Attach` and Quack catalog-introspection queries.

#### Scenario: SHOW TABLES lists Iceberg tables
- **GIVEN** an authenticated session
- **AND** the user has read access to namespaces `analytics` and `staging`
- **WHEN** the user runs `SHOW TABLES FROM sqe.analytics;`
- **THEN** Iceberg tables in `analytics` visible to the user are returned

#### Scenario: Hidden tables under policy
- **GIVEN** an authenticated session where the user lacks read on `analytics.salaries`
- **WHEN** the user runs `SHOW TABLES FROM sqe.analytics;`
- **THEN** the result does not include `salaries`
- **AND** no error is raised (per the PostgreSQL RLS model: denied = invisible)

### Requirement: Query execution
The system SHALL execute SQL sent over Quack and stream results as Arrow record batches.

#### Scenario: Simple SELECT against an Iceberg table
- **GIVEN** an authenticated session attached to namespace `sqe.analytics`
- **WHEN** the user runs `FROM analytics.orders LIMIT 100;`
- **THEN** the server streams `BatchResult` frames containing Arrow record batches matching the table schema
- **AND** the final frame is followed by end-of-stream

#### Scenario: Query cancellation
- **GIVEN** a long-running query in progress
- **WHEN** the client sends `Cancel(stmt_id)`
- **THEN** execution is aborted and `Error(CANCELLED)` is returned

### Requirement: Prepared statements
The system SHALL support prepared statements and bound parameter execution.

#### Scenario: Prepare and execute with parameters
- **GIVEN** an authenticated session
- **WHEN** the client sends `Prepare(1, "SELECT * FROM orders WHERE id = ?")`
- **AND** then `Execute(1, [42])`
- **THEN** the row with `id = 42` is returned

#### Scenario: Close prepared statement
- **GIVEN** a prepared statement with `stmt_id = 1`
- **WHEN** the client sends `Close(1)`
- **THEN** the statement is removed from the session
- **AND** subsequent `Execute(1, ...)` returns `Error(STATEMENT_NOT_FOUND)`
