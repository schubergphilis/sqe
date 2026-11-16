## ADDED Requirements

### Requirement: Materialized views are stored as plain Iceberg tables
The system SHALL support `CREATE [OR REPLACE] MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, `DROP MATERIALIZED VIEW [IF EXISTS]` and `SHOW CREATE MATERIALIZED VIEW`. An MV SHALL be stored as one Iceberg table whose properties hold the definition, the definer and each source's table uuid and snapshot id.

#### Scenario: Lifecycle readable by another engine
- **WHEN** a user creates an MV, refreshes it, reads it from Spark and drops it
- **THEN** Spark reads the MV as a plain table with the refreshed rows
- **AND** after the drop the table is gone

### Requirement: A refresh is atomic
The system SHALL perform a refresh as one replace commit. A refresh that fails SHALL leave the previous contents readable.

#### Scenario: Failed refresh
- **GIVEN** an MV with contents X
- **WHEN** a refresh fails before its commit
- **THEN** the MV still returns X

### Requirement: Freshness is reported
`system.metadata.materialized_views` SHALL list each MV with, per source, the snapshot used by the last refresh and the current snapshot, and SHALL mark the MV `STALE` when they differ.

#### Scenario: Stale then fresh
- **GIVEN** a refreshed MV
- **WHEN** a row is inserted into its source
- **THEN** the MV is shown as `STALE`
- **AND** after a refresh it is shown as `FRESH`

### Requirement: No materialized view over policed sources
The system SHALL refuse to create or refresh an MV when any source has a column mask or row filter in force for the acting user, and the refusal SHALL name the source.

#### Scenario: Masked source
- **GIVEN** a source table with a column mask for the user
- **WHEN** the user creates an MV over it
- **THEN** the statement is refused with the source's name

### Requirement: Refresh runs as the caller
A refresh SHALL run with the caller's credentials. Only the definer or a catalog administrator SHALL be allowed to refresh or replace an MV.

#### Scenario: Other user refreshes
- **GIVEN** an MV defined by user A
- **WHEN** user B, who is not a catalog administrator, runs `REFRESH MATERIALIZED VIEW`
- **THEN** the statement is refused
