## ADDED Requirements

### Requirement: CALL procedure grammar
The system SHALL accept `CALL [catalog.]namespace.procedure(args)` as a top-level SQL statement and route it to the procedure registry.

#### Scenario: Positional arguments
- **GIVEN** a session connected to the default catalog
- **WHEN** the user issues `CALL system.register_table('ns', 't', 's3://bucket/.../metadata/v1.json')`
- **THEN** the parser produces a `ProcedureStmt` with namespace=`system`, procedure=`register_table`, and three positional arguments
- **AND** the statement is routed to the procedure registry, not the optimizer

#### Scenario: Named arguments
- **GIVEN** a session connected to the default catalog
- **WHEN** the user issues `CALL system.register_table(namespace => 'ns', table_name => 't', metadata_location => 's3://...')`
- **THEN** the parser produces a `ProcedureStmt` with three named arguments in any order
- **AND** the registry binds them by name to the procedure's declared schema

#### Scenario: Explicit catalog prefix
- **GIVEN** the user wants to target a non-default catalog
- **WHEN** the user issues `CALL iceberg.system.register_table(...)`
- **THEN** the parser records `catalog=Some("iceberg")` on the statement
- **AND** the procedure executes against that catalog regardless of the session default

#### Scenario: Unknown procedure
- **GIVEN** any session
- **WHEN** the user issues `CALL system.does_not_exist()`
- **THEN** the dispatcher returns a typed error: `unknown procedure: system.does_not_exist`
- **AND** the session remains usable

#### Scenario: Mixed positional + named arguments rejected
- **GIVEN** any session
- **WHEN** the user issues `CALL system.register_table('ns', table_name => 't', 's3://...')`
- **THEN** the parser returns a syntax error before dispatch
- **AND** the error message names "mixed positional and named arguments not allowed"

### Requirement: system.register_table procedure
The system SHALL register an existing Iceberg table — whose data files and metadata.json already exist on the object store — into the session's catalog without copying or rewriting any data.

#### Scenario: Register a previously-managed table
- **GIVEN** a table that was created via CTAS, captured its `metadata_location`, and was then dropped from the catalog with `system.drop_table(..., purge => false)`
- **WHEN** the user calls `system.register_table('ns', 't', '<captured metadata_location>')`
- **THEN** the catalog records the table at the given metadata pointer
- **AND** a subsequent `SELECT * FROM ns.t` returns the same rows as before the drop
- **AND** the original parquet files on the object store are untouched throughout

#### Scenario: Register fails on unreachable metadata
- **GIVEN** a metadata_location that points to a path the catalog backend cannot read
- **WHEN** the user calls `system.register_table('ns', 't', '<bad path>')`
- **THEN** the procedure returns a typed error from the catalog backend
- **AND** no partial state is written (the catalog does not list the table afterwards)

#### Scenario: Register requires CREATE on namespace
- **GIVEN** a session whose principal lacks `CREATE` privileges on namespace `ns`
- **WHEN** the principal calls `system.register_table('ns', 't', '<valid path>')`
- **THEN** the procedure returns a policy error before reaching the catalog backend
- **AND** the catalog remains unchanged

#### Scenario: Result row schema
- **GIVEN** a successful registration
- **WHEN** the procedure returns
- **THEN** the result has columns `(table_identifier: VARCHAR, snapshot_id: BIGINT, metadata_location: VARCHAR)`
- **AND** `table_identifier` is the canonical `<catalog>.<namespace>.<name>` form

### Requirement: system.drop_table procedure
The system SHALL remove a table from the catalog. By default, data files are preserved; with `purge => true`, data files are also deleted.

#### Scenario: Catalog-only drop preserves data
- **GIVEN** a table `ns.t` with data files at `s3://bucket/.../t/data/`
- **WHEN** the user calls `system.drop_table('ns', 't')`
- **THEN** the catalog no longer lists the table
- **AND** the data files at `s3://bucket/.../t/data/` are unchanged
- **AND** the metadata files at `s3://bucket/.../t/metadata/` are unchanged
- **AND** the table can be subsequently re-registered with `system.register_table`

#### Scenario: Purge removes data
- **GIVEN** a table `ns.t` with data files at `s3://bucket/.../t/data/`
- **WHEN** the user calls `system.drop_table('ns', 't', purge => true)`
- **THEN** the catalog no longer lists the table
- **AND** the data and metadata files on the object store are deleted

#### Scenario: Drop requires DROP privilege
- **GIVEN** a session lacking `DROP` on `ns.t`
- **WHEN** the principal calls `system.drop_table('ns', 't')`
- **THEN** the procedure returns a policy error
- **AND** the table remains in the catalog

### Requirement: system.set_current_snapshot procedure
The system SHALL move the current snapshot pointer of a table to a previous snapshot, allowing time-travel queries to default to that snapshot.

#### Scenario: Pin table to a captured snapshot id
- **GIVEN** a table with snapshots S1, S2, S3 where S3 is current
- **WHEN** the user calls `system.set_current_snapshot('ns', 't', S1)`
- **THEN** the catalog records S1 as the current snapshot
- **AND** subsequent `SELECT * FROM ns.t` (without a `FOR VERSION AS OF` clause) returns the rows visible at S1

### Requirement: system.rollback_to_snapshot procedure
The system SHALL roll a table back to a previous snapshot by appending a new snapshot that mirrors the old one, preserving the audit trail.

#### Scenario: Rollback preserves snapshot history
- **GIVEN** a table with snapshots S1, S2, S3 where S3 is current
- **WHEN** the user calls `system.rollback_to_snapshot('ns', 't', S1)`
- **THEN** a new snapshot S4 is appended whose contents match S1
- **AND** S4 is the new current snapshot
- **AND** the snapshot history still lists S1, S2, S3, S4 (no deletions)

### Requirement: Per-backend support matrix
The system SHALL implement each v1 procedure on every catalog backend that supports the underlying iceberg-rust `Catalog` trait operation, and return a typed error naming the backend and procedure on backends that do not.

#### Scenario: Polaris REST supports all v1 procedures
- **GIVEN** a session bound to a Polaris REST catalog
- **WHEN** any of the four v1 procedures is invoked
- **THEN** the procedure dispatches to the Polaris REST endpoint
- **AND** succeeds or returns a typed backend error (HTTP 4xx/5xx surfaced cleanly)

#### Scenario: Hadoop catalog rejects register_table
- **GIVEN** a session bound to a Hadoop (filesystem) catalog
- **WHEN** the user calls `system.register_table(...)`
- **THEN** the procedure returns `unsupported by Hadoop catalog (use filesystem discovery)`
- **AND** other procedures (set_current_snapshot, rollback_to_snapshot) still work
