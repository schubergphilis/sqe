## ADDED Requirements

### Requirement: Equality delete writer

The system SHALL emit Iceberg equality delete files via the vendored `EqualityDeleteFileWriter` when a MoR-mode DELETE or UPDATE is planned. Equality deletes SHALL reference one or more schema field IDs that match values to delete.

#### Scenario: DELETE by primary key writes equality delete

- **GIVEN** a table with `write.delete.mode = 'merge-on-read'` and a declared primary key `id`
- **WHEN** the user runs `DELETE FROM ns.t WHERE id IN (1, 2, 3)`
- **THEN** the commit writes exactly one equality delete file
- **AND** no data files are rewritten
- **AND** subsequent reads from ns.t exclude the deleted rows

#### Scenario: Equality delete read by Spark 4.1

- **GIVEN** SQE wrote an equality delete file with field IDs [1]
- **WHEN** Spark 4.1 reads the table
- **THEN** Spark's scan excludes rows matching the equality delete
- **AND** the total row count matches SQE's scan

### Requirement: RowDeltaAction commit path

The system SHALL provide a transaction action that atomically commits a set of data files, position delete files, and equality delete files in one snapshot. This action cherry-picks iceberg-rust PR #2203.

#### Scenario: Atomic row delta commit

- **WHEN** a MoR MERGE produces 3 new data files, 2 position delete files, and 1 equality delete file
- **AND** the commit succeeds
- **THEN** the resulting snapshot references all 6 files
- **AND** the snapshot operation is `overwrite` with `added-data-files=3, added-delete-files=3`

#### Scenario: Conflict detection on concurrent commits

- **GIVEN** a RowDeltaAction reads table state at snapshot S
- **WHEN** another writer commits snapshot S+1 before the RowDeltaAction commits
- **THEN** the RowDeltaAction commit fails with a retryable conflict
- **AND** client-side retry logic re-reads state and re-applies the delta

### Requirement: Write mode dispatch based on table properties

The system SHALL choose between Copy-on-Write and Merge-on-Read paths based on the Iceberg table properties `write.update.mode`, `write.merge.mode`, and `write.delete.mode`. Each accepts `copy-on-write` or `merge-on-read`. Default is `copy-on-write` for backward compatibility.

#### Scenario: Default write mode is CoW

- **GIVEN** a table with no explicit write.*.mode property
- **WHEN** the user runs `UPDATE ns.t SET v = v + 1 WHERE id = 1`
- **THEN** the update takes the CoW path (existing behavior)
- **AND** affected data files are rewritten

#### Scenario: MoR mode enabled via table property

- **GIVEN** `ALTER TABLE ns.t SET TBLPROPERTIES ('write.update.mode' = 'merge-on-read')`
- **WHEN** the user runs `UPDATE ns.t SET v = v + 1 WHERE id = 1`
- **THEN** the update emits equality delete file + new data file for row 1
- **AND** existing data files are NOT rewritten
- **AND** total row count is unchanged

#### Scenario: Per-operation mode

- **GIVEN** `write.delete.mode = 'merge-on-read'` but `write.update.mode = 'copy-on-write'`
- **WHEN** the user runs DELETE
- **THEN** the MoR path is used
- **WHEN** the user runs UPDATE
- **THEN** the CoW path is used

### Requirement: SF100 update scalability

The system SHALL complete the TPC-E SF100 `trade_result_update_holding` query in under 60 seconds when the table has `write.update.mode = 'merge-on-read'`. (Baseline: CoW path times out at 120s in production harness.)

#### Scenario: SF100 trade_result completes under 60s with MoR

- **GIVEN** TPC-E SF100 with `trade_result.write.update.mode = 'merge-on-read'`
- **WHEN** the harness runs `trade_result_update_holding`
- **THEN** it completes in under 60 seconds
- **AND** the result is correct (matches CoW baseline on SF10)
