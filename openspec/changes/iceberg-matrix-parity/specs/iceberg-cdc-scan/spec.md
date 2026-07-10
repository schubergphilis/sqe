## ADDED Requirements

### Requirement: Snapshot-range incremental scan syntax

The system SHALL extend SELECT syntax to accept `FOR INCREMENTAL BETWEEN SNAPSHOT <start_id> AND SNAPSHOT <end_id>` that returns only rows added in data files whose parent snapshot falls in the range `(start_id, end_id]`.

#### Scenario: Appended rows returned between snapshots

- **GIVEN** table ns.t with snapshots [100, 101, 102, 103]
- **AND** snapshot 101 added 10 rows, snapshot 102 added 15 rows, snapshot 103 added 20 rows
- **WHEN** the user runs `SELECT count(*) FROM ns.t FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 103`
- **THEN** the result is 45

#### Scenario: Deleted rows excluded from incremental scan

- **GIVEN** snapshot 102 deleted 5 rows from files added in snapshot 101
- **WHEN** the user runs `... FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 102`
- **THEN** the result excludes the 5 deleted rows

#### Scenario: Invalid range rejected

- **GIVEN** snapshots [100, 101, 102]
- **WHEN** the user runs `... FOR INCREMENTAL BETWEEN SNAPSHOT 102 AND SNAPSHOT 100` (descending)
- **THEN** the query fails with an error that start must be older than end

#### Scenario: Non-existent snapshot rejected

- **WHEN** the user runs `... FOR INCREMENTAL BETWEEN SNAPSHOT 99999 AND SNAPSHOT 100000`
- **THEN** the query fails with an error naming the missing snapshot

### Requirement: Change data meta columns

The system SHALL expose three pseudo-columns `_change_type`, `_change_ordinal`, `_commit_snapshot_id` that materialise only when referenced in a SELECT using `FOR INCREMENTAL BETWEEN`. Values: `_change_type ∈ {'insert', 'delete'}`, `_change_ordinal` is a per-snapshot sequence, `_commit_snapshot_id` is the snapshot that produced the change.

#### Scenario: Meta columns reveal change type

- **GIVEN** a range scan covering snapshots with INSERTs and DELETEs
- **WHEN** the user runs `SELECT id, _change_type, _commit_snapshot_id FROM ns.t FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 103`
- **THEN** each row shows `_change_type` as `insert` or `delete`
- **AND** `_commit_snapshot_id` identifies which snapshot produced the change

#### Scenario: Meta columns absent in non-incremental queries

- **WHEN** the user runs `SELECT *, _change_type FROM ns.t` without incremental syntax
- **THEN** the query fails because `_change_type` is not a regular column
- **AND** the error message explains the column is available only in incremental scans

### Requirement: dbt incremental materialisation integration

The dbt-sqe adapter SHALL recognise a new incremental strategy `append_changes` that translates to `FOR INCREMENTAL BETWEEN` based on the last successful run's end snapshot stored in the dbt state.

#### Scenario: dbt incremental model reads only new data

- **GIVEN** a dbt model configured with `materialized='incremental', incremental_strategy='append_changes'`
- **AND** the previous run ended at snapshot 100
- **WHEN** the model runs again
- **THEN** the generated SQL reads `... FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT <current>`
- **AND** the resulting inserts only contain rows added since snapshot 100
