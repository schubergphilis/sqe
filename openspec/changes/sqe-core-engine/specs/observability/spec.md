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

### Requirement: Query audit log
The system SHALL write structured JSON audit log entries for every executed query.

#### Scenario: Audit log entry
- **WHEN** a query completes (success or failure)
- **THEN** a JSON line is written to the audit log with: timestamp, user, query_text, tables_accessed, statement_type, duration_ms, rows_returned, status
