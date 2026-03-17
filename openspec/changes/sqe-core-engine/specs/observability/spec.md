## ADDED Requirements

### Requirement: Prometheus metrics
The system SHALL expose Prometheus metrics on a /metrics endpoint for both coordinator and workers.

#### Scenario: Metrics endpoint accessible
- **WHEN** a Prometheus scraper hits `/metrics`
- **THEN** metrics including query counts, durations, rows scanned, active sessions, and worker counts are returned

### Requirement: OpenTelemetry tracing
The system SHALL optionally export distributed traces via OTLP with per-query span trees propagated to workers.

#### Scenario: Distributed query trace
- **GIVEN** OTLP endpoint is configured
- **WHEN** a distributed query executes across workers
- **THEN** a trace with spans for parse, auth, policy, optimize, schedule, execute is exported
- **AND** worker spans are children of the coordinator's execute span

### Requirement: Liveness and readiness probes
The system SHALL expose `/healthz` (liveness) and `/readyz` (readiness) HTTP endpoints on a dedicated health port for Kubernetes probes.

#### Scenario: Liveness check always succeeds
- **WHEN** a probe hits `GET /healthz`
- **THEN** HTTP 200 is returned with body `ok`

#### Scenario: Readiness reflects initialization state
- **GIVEN** the server is still initializing (auth, workers, metrics)
- **WHEN** a probe hits `GET /readyz`
- **THEN** HTTP 503 is returned
- **AND** after all initialization completes, HTTP 200 is returned

### Requirement: Cluster status endpoint (Ballista/DataFusion-style)
The system SHALL expose a `GET /api/v1/status` JSON endpoint on the health port reporting node role, version, uptime, DataFusion version, and worker cluster state.

#### Scenario: Coordinator status with workers
- **GIVEN** the coordinator is running with 2 configured workers, 1 healthy
- **WHEN** a client GETs `/api/v1/status`
- **THEN** a JSON response is returned with:
  - `status`: `"ACTIVE"`
  - `node.role`: `"coordinator"`
  - `node.version`: SQE version
  - `node.datafusionVersion`: DataFusion version
  - `node.uptimeSeconds`: seconds since startup
  - `workers.total`: 2
  - `workers.healthy`: 1
  - `workers.healthyUrls`: list of healthy worker URLs

#### Scenario: Worker status (no workers section)
- **GIVEN** a worker node is running
- **WHEN** a client GETs `/api/v1/status`
- **THEN** the response has `node.role` = `"worker"` and `workers` is null

#### Scenario: Status while starting
- **GIVEN** the server has not yet completed initialization
- **WHEN** a client GETs `/api/v1/status`
- **THEN** `status` is `"STARTING"`

### Requirement: Query audit log
The system SHALL write structured JSON audit log entries for every executed query.

#### Scenario: Audit log entry
- **WHEN** a query completes (success or failure)
- **THEN** a JSON line is written to the audit log with: timestamp, user, query_text, tables_accessed, statement_type, duration_ms, rows_returned, status
