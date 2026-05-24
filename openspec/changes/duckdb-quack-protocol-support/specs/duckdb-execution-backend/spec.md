## ADDED Requirements

### Requirement: DuckDB worker registration
The system SHALL allow worker instances to register with the coordinator declaring `WorkerKind::Duckdb`.

#### Scenario: DuckDB worker comes online
- **GIVEN** a `sqe-worker-duckdb` binary started with a coordinator address
- **WHEN** the worker connects to the coordinator
- **THEN** the worker reports `kind = "duckdb"`, embedded DuckDB version, and Iceberg extension version
- **AND** the coordinator adds it to the worker registry

#### Scenario: Worker shows in cluster status
- **GIVEN** at least one DuckDB worker is registered
- **WHEN** an operator queries cluster status
- **THEN** the worker is listed with kind, version, health, and capacity

### Requirement: Per-session worker selection
The system SHALL allow a session to opt into the DuckDB worker via `SET worker_kind = 'duckdb'`.

#### Scenario: Default session uses DataFusion
- **GIVEN** an authenticated session with no `worker_kind` set
- **WHEN** a query is executed
- **THEN** the query is dispatched to a DataFusion worker
- **AND** the LogicalPlan policy rewrite path is used

#### Scenario: Opted-in session uses DuckDB
- **GIVEN** an authenticated session
- **WHEN** the user runs `SET worker_kind = 'duckdb';` and then a query
- **THEN** the query is dispatched to a DuckDB worker
- **AND** the SQL-text policy rewrite path is used

#### Scenario: Refusal when no DuckDB workers registered
- **GIVEN** a coordinator with zero DuckDB workers in the registry
- **WHEN** a session runs `SET worker_kind = 'duckdb';`
- **THEN** the SET succeeds but the next query returns `Error(NO_DUCKDB_WORKERS)` with a clear hint

### Requirement: DuckDB Iceberg integration
The system SHALL configure the embedded DuckDB instance to read Iceberg tables from SQE's REST catalog using the current user's bearer token.

#### Scenario: Token forwarded to DuckDB Iceberg extension
- **GIVEN** a DuckDB worker receiving a query for user U with bearer token T
- **WHEN** the worker prepares to execute
- **THEN** the DuckDB `iceberg` secret is updated with `TOKEN '<T>'`
- **AND** DuckDB reads metadata + data files with U's identity

#### Scenario: Token refresh between queries
- **GIVEN** a DuckDB worker handling sequential queries for U whose token rotated
- **WHEN** the next query arrives with a new token T2
- **THEN** the secret is updated to T2 before execution

### Requirement: Result format
The system SHALL return query results from DuckDB workers as Arrow record batches on the existing worker transport.

#### Scenario: DuckDB returns Arrow
- **GIVEN** a DuckDB worker executing a SELECT
- **WHEN** the query produces rows
- **THEN** the worker uses DuckDB's Arrow API (`query_arrow`) to stream batches
- **AND** the coordinator receives them with no row-by-row conversion

### Requirement: Result parity with DataFusion
The system SHALL produce equivalent results when the same query (with the same policy) runs on DataFusion and DuckDB workers.

#### Scenario: TPC-H SF1 parity
- **GIVEN** TPC-H SF1 data accessible through SQE
- **AND** an empty policy
- **WHEN** every TPC-H query is run against both worker kinds
- **THEN** every result is equal modulo row order

#### Scenario: Policy parity
- **GIVEN** a policy with row filters on `orders` and a mask on `customer.account_balance`
- **WHEN** a join query referencing both tables runs on both worker kinds
- **THEN** the projected rows and masked values are equal

### Requirement: Worker isolation
The system SHALL isolate the DuckDB worker process from the coordinator and other workers.

#### Scenario: DuckDB worker crash does not affect coordinator
- **GIVEN** a DuckDB worker running queries
- **WHEN** the worker process exits abnormally
- **THEN** the coordinator removes it from the registry
- **AND** in-flight queries on that worker return `Error(WORKER_LOST)`
- **AND** other workers continue serving traffic
