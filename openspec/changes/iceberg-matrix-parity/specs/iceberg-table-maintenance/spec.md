## ADDED Requirements

### Requirement: CALL system.rewrite_data_files procedure

The system SHALL expose `CALL system.rewrite_data_files(table => 'schema.table'[, options])` that compacts small data files by invoking the vendored iceberg-rust `RewriteFilesAction`. Options SHALL include `target_file_size_bytes` (default 512 MiB), `min_input_files` (default 5), and `max_concurrent_file_group_rewrites` (default 4).

#### Scenario: Compaction reduces file count

- **GIVEN** a table with 50 data files each 10 MiB
- **WHEN** the user runs `CALL system.rewrite_data_files(table => 'ns.t')`
- **THEN** a new snapshot is committed
- **AND** the new snapshot has at most 5 data files
- **AND** total row count is unchanged

#### Scenario: Target file size respected

- **GIVEN** a table with 100 data files totaling 5 GiB
- **WHEN** the user runs `CALL system.rewrite_data_files(table => 'ns.t', target_file_size_bytes => 268435456)`
- **THEN** the rewritten files are 256 MiB +/- 20%
- **AND** total row count is unchanged

#### Scenario: Concurrent writer conflict detected

- **GIVEN** a rewrite is in progress
- **WHEN** a concurrent INSERT commits a new snapshot
- **THEN** the rewrite commit fails with a retryable conflict error
- **AND** no partial state is left in the catalog

### Requirement: CALL system.expire_snapshots procedure

The system SHALL expose `CALL system.expire_snapshots(table => 'schema.table'[, older_than => TIMESTAMP, retain_last => N])` that removes old snapshots via the vendored `RemoveSnapshotAction`. Default retention is 5 snapshots or 7 days, whichever is greater.

#### Scenario: Expire snapshots by time

- **GIVEN** a table with 20 snapshots spanning 30 days
- **WHEN** the user runs `CALL system.expire_snapshots(table => 'ns.t', older_than => CURRENT_TIMESTAMP - INTERVAL '7' DAY)`
- **THEN** snapshots older than 7 days are removed from metadata
- **AND** any snapshot referenced by a branch or tag is NOT removed
- **AND** the current snapshot is NOT removed

#### Scenario: Expire snapshots by count

- **GIVEN** a table with 20 snapshots
- **WHEN** the user runs `CALL system.expire_snapshots(table => 'ns.t', retain_last => 5)`
- **THEN** exactly 5 snapshots remain plus any branch/tag-referenced snapshots

### Requirement: CALL system.remove_orphan_files procedure

The system SHALL expose `CALL system.remove_orphan_files(table => 'schema.table'[, older_than => TIMESTAMP])` that deletes files under the table's storage prefix not referenced by any current snapshot manifest. Default `older_than` is 3 days to avoid races with in-flight writes.

#### Scenario: Orphan files older than threshold removed

- **GIVEN** a table with 3 data files referenced in manifests
- **AND** 2 additional orphan files under the table prefix older than 3 days
- **WHEN** the user runs `CALL system.remove_orphan_files(table => 'ns.t')`
- **THEN** the 2 orphan files are deleted from object storage
- **AND** the 3 referenced files are preserved
- **AND** the command returns a list of deleted paths

#### Scenario: Recent files preserved to avoid races

- **GIVEN** a table with 1 orphan file created 1 hour ago
- **WHEN** the user runs `CALL system.remove_orphan_files(table => 'ns.t')` (default 3-day threshold)
- **THEN** the orphan file is NOT deleted
- **AND** the command reports 0 files removed

### Requirement: CALL system.rewrite_manifests procedure

The system SHALL expose `CALL system.rewrite_manifests(table => 'schema.table')` that consolidates small manifest files via the vendored `RewriteManifestsAction`, using the RisingWave fork's parallel-loading optimisation.

#### Scenario: Many small manifests consolidated

- **GIVEN** a table with 200 manifest files
- **WHEN** the user runs `CALL system.rewrite_manifests(table => 'ns.t')`
- **THEN** a new snapshot is committed with fewer manifest files
- **AND** data file references are unchanged
- **AND** a subsequent SELECT against the table returns identical results to before the rewrite

### Requirement: Maintenance procedures require write privileges

The system SHALL enforce that the calling user has write privileges on the target table before executing any maintenance procedure. The privilege check SHALL use the existing policy enforcement chain (OPA/Cedar/passthrough per `sqe-policy`).

#### Scenario: Unauthorised user rejected

- **GIVEN** a user with read-only access to `ns.t`
- **WHEN** the user runs `CALL system.rewrite_data_files(table => 'ns.t')`
- **THEN** the command fails with an authorisation error
- **AND** no commit is attempted
- **AND** the attempt is recorded in the audit log
