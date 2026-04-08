## ADDED Requirements

### Requirement: Mode selection via environment variable
The `sqe-server` binary SHALL select its operating mode (coordinator or worker) from the `SQE_MODE` environment variable. Valid values are `coordinator` and `worker`. The value SHALL be case-insensitive.

#### Scenario: Start as coordinator
- **WHEN** `SQE_MODE=coordinator` is set
- **THEN** `sqe-server` starts in coordinator mode (SQL parsing, planning, scheduling, Flight SQL endpoint)

#### Scenario: Start as worker
- **WHEN** `SQE_MODE=worker` is set
- **THEN** `sqe-server` starts in worker mode (fragment execution, DataFusion runtime, Flight data serving)

#### Scenario: Missing mode
- **WHEN** `SQE_MODE` is not set and no config file specifies the mode
- **THEN** `sqe-server` SHALL exit with a non-zero exit code and print an error message indicating that `SQE_MODE` must be set

#### Scenario: Invalid mode value
- **WHEN** `SQE_MODE` is set to an unrecognized value (e.g., `SQE_MODE=something`)
- **THEN** `sqe-server` SHALL exit with a non-zero exit code and print an error listing the valid modes

### Requirement: Config file override
The `sqe-server` binary SHALL accept an optional `--config <path>` argument pointing to a TOML configuration file. Settings in the config file SHALL override environment variables where both are provided.

#### Scenario: Config file specifies mode
- **WHEN** `--config sqe.toml` is passed and the file contains `mode = "coordinator"`
- **THEN** `sqe-server` starts in coordinator mode regardless of `SQE_MODE` env var

#### Scenario: Config file not found
- **WHEN** `--config missing.toml` is passed and the file does not exist
- **THEN** `sqe-server` SHALL exit with a non-zero exit code and print an error indicating the config file was not found

### Requirement: Graceful shutdown
The `sqe-server` binary SHALL handle SIGTERM and SIGINT signals by initiating graceful shutdown — completing in-flight queries before exiting.

#### Scenario: SIGTERM during query execution
- **WHEN** `sqe-server` receives SIGTERM while queries are in-flight
- **THEN** it SHALL stop accepting new queries, wait for in-flight queries to complete (up to a configurable timeout), and then exit with code 0

#### Scenario: Shutdown timeout exceeded
- **WHEN** in-flight queries do not complete within the shutdown timeout
- **THEN** `sqe-server` SHALL forcibly terminate remaining queries and exit with code 0

### Requirement: Health and readiness endpoints
The `sqe-server` binary SHALL expose HTTP health check endpoints for Kubernetes liveness and readiness probes.

#### Scenario: Liveness probe
- **WHEN** an HTTP GET request is made to `/healthz`
- **THEN** `sqe-server` SHALL respond with 200 OK if the process is alive

#### Scenario: Readiness probe
- **WHEN** an HTTP GET request is made to `/readyz`
- **THEN** `sqe-server` SHALL respond with 200 OK only when the server is ready to accept queries (catalog connected, auth configured)
