## ADDED Requirements

### Requirement: A failed CTAS leaves no catalog entry
On an Iceberg REST catalog that supports staged create, a CTAS SHALL register its table only in the commit that adds its first snapshot. A CTAS that fails before that commit SHALL leave no catalog entry, whatever the caller's drop rights.

#### Scenario: Caller may create but not write
- **WHEN** a caller holding create but not write on a namespace runs `CREATE TABLE ns.t AS SELECT ...`
- **THEN** the statement fails as access denied
- **AND** `ns.t` does not exist in the catalog afterwards

#### Scenario: Write fails mid-stream
- **WHEN** the data write fails before the commit
- **THEN** the staged location's prefix is deleted, best-effort
- **AND** no catalog entry exists and no drop is sent

#### Scenario: Successful CTAS unchanged
- **WHEN** a caller with create and write runs a CTAS
- **THEN** the table, its rows, its first snapshot, the lineage event and the audit event are the same as with register-first

### Requirement: Every step runs as the caller
The staged create, the data write and the create-commit SHALL use the caller's identity and delegation. No engine or service identity SHALL take part.

#### Scenario: Staged create carries the caller's token
- **WHEN** a CTAS stages its create
- **THEN** the `createTable` request and the commit carry the caller's bearer

### Requirement: Fallback to register-first
When the catalog refuses `stage-create`, SQE SHALL create the table as before (register first) and roll back with a drop without purge on failure.

#### Scenario: Catalog without staged create
- **WHEN** the catalog answers the staged create with an error that marks it unsupported
- **THEN** the CTAS registers first and behaves as today
