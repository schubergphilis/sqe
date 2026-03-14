## ADDED Requirements

### Requirement: CREATE TABLE AS SELECT (CTAS)
The system SHALL create Iceberg tables from query results.

#### Scenario: CTAS creates readable table
- **WHEN** user submits `CREATE TABLE ns.totals AS SELECT region, SUM(amount) FROM ns.txns GROUP BY region`
- **THEN** a new Iceberg table is created in Polaris with schema inferred from Arrow
- **AND** query results are written as Parquet files to S3 using vended credentials
- **AND** the table is readable by subsequent SELECT queries

### Requirement: CREATE OR REPLACE TABLE AS SELECT
The system SHALL atomically replace table contents via a new Iceberg snapshot.

#### Scenario: Atomic table replacement
- **WHEN** user submits `CREATE OR REPLACE TABLE ns.totals AS SELECT ...`
- **THEN** a new snapshot replaces the current data
- **AND** old snapshots remain accessible via Iceberg time-travel
- **AND** concurrent readers see either old or new version (never partial)

### Requirement: INSERT INTO SELECT
The system SHALL append query results to existing Iceberg tables.

#### Scenario: Append data to existing table
- **WHEN** user submits `INSERT INTO ns.events SELECT ... WHERE date = '2026-03-13'`
- **THEN** new data files are written and a new snapshot is committed

### Requirement: MERGE INTO
The system SHALL support conditional insert/update/delete based on a join condition using Merge-on-Read with position deletes.

#### Scenario: Upsert via MERGE
- **WHEN** user submits a MERGE INTO with WHEN MATCHED UPDATE and WHEN NOT MATCHED INSERT
- **THEN** matched rows are marked with position deletes and rewritten
- **AND** unmatched rows are inserted as new data files
- **AND** all changes are committed atomically in one snapshot

### Requirement: DELETE FROM with predicate
The system SHALL delete rows matching a predicate using Iceberg position delete files.

#### Scenario: Delete matching rows
- **WHEN** user submits `DELETE FROM ns.events WHERE date = '2026-03-13'`
- **THEN** matching rows are recorded as position deletes
- **AND** a new snapshot is committed

### Requirement: DROP TABLE
The system SHALL drop tables via the Polaris REST catalog.

#### Scenario: Drop existing table
- **WHEN** user submits `DROP TABLE ns.tmp_staging`
- **THEN** the table is removed from Polaris catalog

#### Scenario: Drop non-existent table with IF EXISTS
- **WHEN** user submits `DROP TABLE IF EXISTS ns.nonexistent`
- **THEN** no error is raised

### Requirement: ALTER TABLE RENAME
The system SHALL rename tables within a namespace via Polaris REST.

#### Scenario: Rename table
- **WHEN** user submits `ALTER TABLE ns.old_name RENAME TO ns.new_name`
- **THEN** the table is renamed in Polaris catalog

### Requirement: CREATE VIEW / DROP VIEW
The system SHALL support Iceberg views via Polaris REST.

#### Scenario: Create view
- **WHEN** user submits `CREATE VIEW ns.my_view AS SELECT ...`
- **THEN** the view SQL is persisted in Polaris via the Iceberg REST view API

#### Scenario: Query view
- **GIVEN** a view `ns.my_view` exists
- **WHEN** user queries `SELECT * FROM ns.my_view`
- **THEN** the view SQL is resolved, parsed, and inlined into the query plan

### Requirement: Write operations execute on coordinator only
All write operations (Parquet file writing, Iceberg snapshot commits) SHALL execute on the coordinator. The SELECT portion may be distributed for reads, but the materialization step is coordinator-local.

#### Scenario: No orphan files on worker failure
- **GIVEN** a CTAS query with distributed read execution
- **WHEN** a worker dies mid-read-fragment
- **THEN** no orphan data files exist in S3 from the failed write
- **AND** the coordinator re-assigns the read fragment or fails the query
