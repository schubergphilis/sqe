## ADDED Requirements

### Requirement: SQL parsing and statement classification
The system SHALL parse SQL via sqlparser-rs and classify statements into: Query, DDL, DML, View, Catalog, InfoSchema, Policy, Utility.

#### Scenario: SELECT routed to query engine
- **WHEN** a SELECT statement is submitted
- **THEN** it is classified as Query and routed to DataFusion logical planning

#### Scenario: CTAS routed to write path
- **WHEN** `CREATE TABLE ... AS SELECT` is submitted
- **THEN** it is classified as DDL and routed to the write path handler

#### Scenario: Policy statement returns not configured
- **WHEN** a GRANT or REVOKE statement is submitted
- **THEN** it returns "policy engine not configured" error

### Requirement: Query planning and optimization
The system SHALL convert parsed queries to DataFusion LogicalPlans, apply policy enforcement (passthrough), and run the DataFusion optimizer.

#### Scenario: Query optimization with predicate pushdown
- **GIVEN** an Iceberg table with partition columns
- **WHEN** a query includes a predicate on a partition column
- **THEN** the DataFusion optimizer pushes the predicate to the IcebergTableProvider for manifest pruning

### Requirement: Streaming result delivery
The system SHALL stream query results as Arrow RecordBatches without materializing the full result set in coordinator memory.

#### Scenario: Large result set streaming
- **WHEN** a query produces millions of rows
- **THEN** results are streamed incrementally to the client
- **AND** backpressure propagates from client to executor
