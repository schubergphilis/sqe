## ADDED Requirements

### Requirement: Ingest without options appends to an existing table
A `CommandStatementIngest` without `table_definition_options`, or with both values `UNSPECIFIED`, SHALL behave as `(if_not_exist: FAIL, if_exists: APPEND)`.

#### Scenario: No options, table exists
- **WHEN** a client ingests rows into an existing table without `table_definition_options`
- **THEN** the rows are appended and the response carries the row count

#### Scenario: No options, table missing
- **WHEN** a client ingests into a table that does not exist without `table_definition_options`
- **THEN** the request fails with `TABLE_NOT_FOUND` (gRPC `NotFound`) and no table is created

### Requirement: Create a missing table from the stream schema
With `if_not_exist = CREATE` and a missing table, the server SHALL create the table from the Arrow schema of the uploaded stream, using the CTAS schema conversion and the caller's catalog credentials, and then write the rows into it.

#### Scenario: ADBC create on a missing table
- **WHEN** a client ingests with `(CREATE, FAIL)` into a missing table
- **THEN** the table exists afterwards with the stream's column names and types, holds exactly the uploaded rows, and the response carries the row count

#### Scenario: Empty stream creates an empty table
- **WHEN** the stream announces a schema and carries no batches
- **THEN** the table is created with that schema and holds zero rows

#### Scenario: Stream without a schema
- **WHEN** the stream ends before a schema is known
- **THEN** the request fails with `InvalidArgument` and no table is created

#### Scenario: Caller lacks create privilege
- **WHEN** the catalog refuses the create for the caller
- **THEN** the request fails with the catalog's refusal and no data file is written

#### Scenario: Create inside a BEGIN transaction
- **WHEN** a session holding a SQL `BEGIN` transaction ingests with `CREATE` into a missing table
- **THEN** the request fails with `AutocommitWriteConflict` and no table is created

### Requirement: Fail if the table exists
With `if_exists = FAIL` and an existing table, the server SHALL refuse the request with `DUPLICATE_TABLE` (gRPC `AlreadyExists`) before any data file is written.

#### Scenario: ADBC create on an existing table
- **WHEN** a client ingests with `(CREATE, FAIL)` into an existing table
- **THEN** the request fails with `AlreadyExists` and the table's rows and snapshot are unchanged

#### Scenario: Lost create race
- **WHEN** the table is created by another client between the existence check and the create
- **THEN** the server applies `if_exists` to it as an existing table

### Requirement: Append to an existing or new table
With `if_exists = APPEND` the server SHALL append the rows to an existing table; combined with `if_not_exist = CREATE` it SHALL create a missing table first.

#### Scenario: ADBC create_append, twice
- **WHEN** a client ingests with `(CREATE, APPEND)` into a missing table and then again into the same table
- **THEN** the table holds the rows of both requests

### Requirement: Replace the contents of an existing table
With `if_exists = REPLACE` and an existing table, the server SHALL replace the table's rows with the uploaded rows in one overwrite snapshot, keep the table's schema and history, and refuse the request where INSERT OVERWRITE would be refused under policy.

#### Scenario: ADBC replace
- **WHEN** a client ingests with `(CREATE, REPLACE)` into a table holding rows
- **THEN** the table holds exactly the uploaded rows, and the previous snapshot is still in its history

#### Scenario: Replace with a mismatched stream
- **WHEN** the stream's batches do not fit the table schema
- **THEN** the request fails and the table's rows and snapshot are unchanged

#### Scenario: Replace under a row filter or mask
- **WHEN** the caller has a row filter or column mask on the table
- **THEN** the request is refused as INSERT OVERWRITE is, and nothing is written

### Requirement: Ingest in a transaction
Ingest SHALL be admitted through `admit_txn_write` like every other write. Inside a client-held Flight transaction, appended and replaced rows SHALL stay invisible to other sessions until commit and SHALL disappear on rollback. A table created by the ingest SHALL be created autocommit.

#### Scenario: Create then rollback
- **WHEN** a client ingests with `CREATE` into a missing table inside a Flight transaction and rolls back
- **THEN** the table exists and holds zero rows
