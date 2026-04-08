## ADDED Requirements

### Requirement: Config rename — Keycloak → OIDC
The system SHALL accept `[auth.oidc]` as the canonical config section and treat `[keycloak]` as a deprecated alias.

#### Scenario: Deprecated config key
- **GIVEN** a `sqe.toml` with `[keycloak]` section
- **WHEN** the coordinator starts
- **THEN** config loads successfully
- **AND** a `WARN` log line is emitted: `config key 'keycloak.*' is deprecated, use 'auth.oidc.*'`

#### Scenario: New config key
- **GIVEN** a `sqe.toml` with `[auth.oidc]` section
- **WHEN** the coordinator starts
- **THEN** config loads successfully with no deprecation warning

### Requirement: Startup validation
The coordinator SHALL fail fast with a clear message when required configuration is missing or invalid.

#### Scenario: Missing required field
- **GIVEN** a `sqe.toml` missing `auth.oidc.token_url`
- **WHEN** the coordinator starts
- **THEN** it exits with a non-zero exit code
- **AND** the error message names the missing field: `required config field 'auth.oidc.token_url' is not set`

#### Scenario: TLS cert file missing
- **GIVEN** TLS is enabled and `server.tls.cert_file` path does not exist
- **WHEN** the coordinator starts
- **THEN** it exits with a non-zero exit code
- **AND** the error message names the missing file

### Requirement: Rate limiting
The system SHALL enforce per-user and global query rate limits.

#### Scenario: Per-user limit exceeded
- **GIVEN** `rate_limit.per_user_queries_per_minute = 5` is configured
- **WHEN** user `alice` submits 6 queries within one minute
- **THEN** the 6th query is rejected with a `RESOURCE_EXHAUSTED` Flight error
- **AND** alice's session remains open

#### Scenario: Global limit exceeded
- **GIVEN** `rate_limit.global_queries_per_minute = 10` is configured
- **WHEN** 11 concurrent queries are submitted by different users
- **THEN** at least one query is rejected with `RESOURCE_EXHAUSTED`

### Requirement: Query timeout
The system SHALL terminate queries that exceed the configured wall-clock timeout.

#### Scenario: Query exceeds timeout
- **GIVEN** `query.timeout_secs = 5`
- **WHEN** a query that runs for 10 seconds is submitted
- **THEN** the query is cancelled after 5 seconds
- **AND** the client receives a `DEADLINE_EXCEEDED` Flight error

#### Scenario: Role override
- **GIVEN** `query.role_overrides.admin = 3600` and the user has role `admin`
- **WHEN** the user submits a query that runs for 600 seconds
- **THEN** the query is NOT cancelled at 300 seconds

### Requirement: Session lifecycle
The system SHALL expire idle and long-running sessions.

#### Scenario: Idle session expiry
- **GIVEN** `session.idle_timeout_secs = 60`
- **WHEN** a session has had no query activity for 61 seconds
- **THEN** the session is marked expired
- **AND** any subsequent query returns `UNAUTHENTICATED`

### Requirement: Query cancellation
The system SHALL support client-initiated query cancellation.

#### Scenario: Client cancels query
- **GIVEN** a long-running query is executing on coordinator and workers
- **WHEN** the client sends a Flight cancel signal
- **THEN** the coordinator fires the cancellation token
- **AND** workers stop execution and release resources
- **AND** the coordinator returns an appropriate completion status

### Requirement: Audit log
The system SHALL emit a structured audit event for every query.

#### Scenario: Successful query
- **GIVEN** audit logging is enabled
- **WHEN** user `alice` runs `SELECT * FROM sales.orders`
- **THEN** a JSON audit event is emitted containing: `user=alice`, `outcome=success`, `tables=["sales.orders"]`, `rows_returned`, `duration_ms`, `query_hash`

#### Scenario: Query text not logged by default
- **GIVEN** `audit_log.log_query_text = false` (default)
- **WHEN** any query runs
- **THEN** the audit event contains `query_hash` but NOT the original SQL text

### Requirement: Error sanitisation
The system SHALL not expose internal details to clients in production mode.

#### Scenario: Internal error in production mode
- **GIVEN** `server.debug = false` (default)
- **WHEN** a catalog connection error occurs during query execution
- **THEN** the client receives: `"query execution failed"` with a request ID
- **AND** the full error (including catalog URL, stack trace) is logged on the coordinator

### Requirement: Health endpoints
The system SHALL expose liveness and readiness endpoints on the admin port.

#### Scenario: Liveness
- **GIVEN** the coordinator process has started
- **WHEN** `GET /healthz/live` is called
- **THEN** response is `200 OK`

#### Scenario: Readiness — catalog unreachable
- **GIVEN** the configured catalog is not reachable
- **WHEN** `GET /healthz/ready` is called
- **THEN** response is `503 Service Unavailable`
