## ADDED Requirements

### Requirement: Per-session SQL dialect
The system SHALL select a SQL dialect at session creation time based on the connecting protocol.

#### Scenario: Flight SQL session uses DataFusion dialect
- **GIVEN** a client connects via Arrow Flight SQL
- **WHEN** the session is created
- **THEN** the dialect is set to DataFusion-native
- **AND** SQL is parsed with the existing sqlparser-rs generic dialect

#### Scenario: Quack session uses DuckDB dialect
- **GIVEN** a client connects via the Quack server
- **WHEN** the session is created
- **THEN** the dialect is set to DuckDB
- **AND** SQL is parsed with sqlparser-rs `DuckDbDialect`

### Requirement: DuckDB AST translation
The system SHALL translate the common subset of DuckDB-flavoured SQL AST nodes to DataFusion-compatible nodes when dispatching to a DataFusion worker.

#### Scenario: LIST_VALUE translates to array literal
- **GIVEN** an inbound query `SELECT LIST_VALUE(1, 2, 3) AS xs;`
- **WHEN** the translator runs
- **THEN** the AST becomes `SELECT [1, 2, 3] AS xs;` and parses against DataFusion semantics

#### Scenario: STRUCT_PACK translates to struct literal
- **GIVEN** `SELECT STRUCT_PACK(a := 1, b := 'x') AS s;`
- **WHEN** the translator runs
- **THEN** the result is a DataFusion struct expression with fields `a` and `b`

#### Scenario: epoch() maps to DataFusion equivalent
- **GIVEN** `SELECT epoch(now()) AS t;`
- **WHEN** the translator runs
- **THEN** the call becomes `SELECT extract(epoch FROM now()) AS t;` (or the canonical DataFusion form)

### Requirement: Unsupported dialect feature surface
The system SHALL return a precise, actionable error when a DuckDB-only feature is used and the session is bound to a DataFusion worker.

#### Scenario: PIVOT returns explicit error
- **GIVEN** a Quack session bound to a DataFusion worker
- **WHEN** the user runs a query containing `PIVOT (...)`
- **THEN** `Error(UNSUPPORTED_DIALECT)` is returned
- **AND** the message names the feature (`PIVOT`)
- **AND** the hint points to `docs/duckdb-dialect-status.md`

#### Scenario: ASOF JOIN returns explicit error
- **GIVEN** a Quack session bound to a DataFusion worker
- **WHEN** the user runs a query with `ASOF JOIN`
- **THEN** the same error shape is returned with feature name `ASOF JOIN`

### Requirement: DuckDB-worker passthrough
The system SHALL skip AST translation when the session targets a DuckDB worker; SQL is text-rewritten by policy and sent to DuckDB unchanged.

#### Scenario: PIVOT works on a DuckDB worker
- **GIVEN** a session with `worker_kind = 'duckdb'` and a DuckDB worker available
- **WHEN** the user runs a `PIVOT` query
- **THEN** the SQL is policy-text-rewritten and forwarded to DuckDB
- **AND** DuckDB executes it natively

#### Scenario: DataFusion-only function fails on DuckDB worker
- **GIVEN** a session with `worker_kind = 'duckdb'`
- **WHEN** the user calls a DataFusion-only function (e.g., a custom UDF registered in SQE only)
- **THEN** DuckDB returns its own parse/binding error
- **AND** the error is propagated back as `Error(EXECUTION_ERROR)` with the DuckDB message
