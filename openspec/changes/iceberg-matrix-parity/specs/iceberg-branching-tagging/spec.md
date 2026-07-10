## ADDED Requirements

### Requirement: Create and drop branches

The system SHALL support creating and dropping named branches on Iceberg tables via SQL DDL. A branch is a mutable reference to a snapshot, following the Iceberg V2 spec semantics for `SnapshotReference` with `type: branch`.

#### Scenario: Create branch from current snapshot

- **WHEN** the user runs `ALTER TABLE ns.t CREATE BRANCH feature_x`
- **THEN** a new branch `feature_x` is created pointing at the current snapshot
- **AND** `SELECT * FROM ns.t$refs` returns a row `(name='feature_x', type='branch', snapshot_id=<current>)`

#### Scenario: Create branch from specific snapshot

- **GIVEN** a table with snapshot 12345 in its history
- **WHEN** the user runs `ALTER TABLE ns.t CREATE BRANCH historical AS OF VERSION 12345`
- **THEN** the branch points at snapshot 12345

#### Scenario: Drop branch

- **GIVEN** a branch `feature_x` exists
- **WHEN** the user runs `ALTER TABLE ns.t DROP BRANCH feature_x`
- **THEN** the branch is removed from table metadata
- **AND** data files exclusive to that branch become eligible for orphan file cleanup

#### Scenario: Main branch cannot be dropped

- **WHEN** the user runs `ALTER TABLE ns.t DROP BRANCH main`
- **THEN** the command fails with an error that main is a protected branch

### Requirement: Create and drop tags

The system SHALL support creating and dropping named tags on Iceberg tables. A tag is an immutable reference to a specific snapshot. Attempting to re-create a tag that already exists SHALL fail unless `REPLACE` is specified.

#### Scenario: Create tag at current snapshot

- **WHEN** the user runs `ALTER TABLE ns.t CREATE TAG release_v1`
- **THEN** a tag `release_v1` is created pointing at the current snapshot
- **AND** `SELECT * FROM ns.t$refs WHERE type='tag'` includes `release_v1`

#### Scenario: Tag retention prevents snapshot expiry

- **GIVEN** a tag `release_v1` pointing at snapshot S
- **WHEN** `CALL system.expire_snapshots` runs with `older_than` that would remove S
- **THEN** S is NOT removed because it is tag-referenced

#### Scenario: Replace existing tag

- **GIVEN** a tag `latest` points at snapshot 100
- **WHEN** the user runs `ALTER TABLE ns.t CREATE OR REPLACE TAG latest`
- **THEN** the tag now points at the current snapshot
- **AND** the previous snapshot reference is removed

### Requirement: Query from branch or tag

The system SHALL extend `SELECT ... FOR VERSION AS OF` to accept a branch or tag name in addition to a snapshot ID or timestamp.

#### Scenario: Read from branch

- **GIVEN** a branch `feature_x` with different data than main
- **WHEN** the user runs `SELECT count(*) FROM ns.t FOR VERSION AS OF 'feature_x'`
- **THEN** the result reflects the branch's snapshot
- **AND** subsequent writes to main do not affect this query

#### Scenario: Read from tag

- **WHEN** the user runs `SELECT * FROM ns.t FOR VERSION AS OF 'release_v1'`
- **THEN** the result reflects the snapshot that `release_v1` points at

#### Scenario: Ambiguous reference preferred by tag then branch

- **GIVEN** a tag and a branch both named `foo`
- **WHEN** the user queries `FOR VERSION AS OF 'foo'`
- **THEN** the tag resolution takes precedence
- **AND** a warning is logged about the ambiguity

### Requirement: Write to a specific branch

The system SHALL allow INSERT, UPDATE, DELETE, MERGE to target a specific branch via a session setting `SET WRITE_BRANCH = 'branch_name'`. When unset, writes go to the main branch.

#### Scenario: Write isolation between branches

- **GIVEN** a session runs `SET WRITE_BRANCH = 'feature_x'`
- **WHEN** the session runs `INSERT INTO ns.t VALUES (...)`
- **THEN** rows are appended to the feature_x branch
- **AND** a parallel session reading main does NOT see the new rows
- **AND** a read of feature_x DOES see them

### Requirement: Retention configuration on branches

The system SHALL accept retention options when creating a branch: `WITH RETENTION (min_snapshots_to_keep => N, max_snapshot_age_ms => M, max_ref_age_ms => R)`. These apply to snapshot expiry on that branch.

#### Scenario: Branch retention overrides table default

- **WHEN** the user runs `ALTER TABLE ns.t CREATE BRANCH qa WITH RETENTION (min_snapshots_to_keep => 100)`
- **THEN** the branch keeps at least 100 snapshots regardless of the table-level expire policy
