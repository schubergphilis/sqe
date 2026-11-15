## ADDED Requirements

### Requirement: Embedded mode runs the catalog-only procedures
`sqe-cli --embedded` SHALL run `CALL system.expire_snapshots`, `rewrite_manifests`, `set_current_snapshot`, `rollback_to_snapshot`, `table_health` and `register_table` against a writable Iceberg catalog. They SHALL return the same result columns as on an SQE cluster.

#### Scenario: expire snapshots
- **WHEN** an embedded user has written three snapshots to `lake.s.t` and runs `CALL system.expire_snapshots(table => 'lake.s.t', older_than => <a time after the third commit>, retain_last => 1)`
- **THEN** the result has one row with `procedure = 'expire_snapshots'`, `input_count = 3` and `output_count = 1`, and `lake.s.t` has one snapshot left

#### Scenario: rollback is visible to the next read
- **WHEN** an embedded user inserts rows in two commits, rolls `lake.s.t` back to the first snapshot with `rollback_to_snapshot`, then runs `SELECT count(*) FROM lake.s.t`
- **THEN** the count is the first commit's row count

#### Scenario: rollback to a non-ancestor
- **WHEN** an embedded user calls `rollback_to_snapshot` with a snapshot id that is not an ancestor of the current snapshot
- **THEN** the call fails with the cluster's "is not an ancestor" message and the table is unchanged

#### Scenario: register a table
- **WHEN** an embedded user calls `register_table` with the metadata location of an existing table under a new name
- **THEN** the result row says `registered` and a `SELECT` on the new name in the same session returns the table's rows

#### Scenario: table health
- **WHEN** an embedded user calls `table_health` on a table
- **THEN** the result is one row with the cluster's `table_health` columns, computed with the default compaction settings

#### Scenario: manifest rewrite
- **WHEN** an embedded user calls `rewrite_manifests` on a table with several data manifests in one partition spec
- **THEN** the result row has `output_count` lower than `input_count`, and the table's rows are unchanged

### Requirement: Embedded procedures need a writable Iceberg catalog
A procedure in embedded mode SHALL resolve its catalog from the table reference, or from the session's default catalog when the reference names none. It SHALL refuse a catalog that is not a writable Iceberg catalog, before it changes anything.

#### Scenario: catalog that is not Iceberg
- **WHEN** an embedded user calls `expire_snapshots` on a table in a Hive or file catalog
- **THEN** the call fails naming the catalog as not a writable Iceberg catalog

### Requirement: Cluster-only procedures are refused by name
In embedded mode, `rewrite_data_files`, `remove_orphan_files`, `purge_orphan_locations`, `drop_table`, `refresh_catalog_cache`, `reproject_column_tags` and `suggest_bloom_filter_columns` SHALL fail with an error naming the procedure and saying it is not available in embedded mode. They SHALL fail before any catalog or storage call.

#### Scenario: rewrite_data_files
- **WHEN** an embedded user runs `CALL system.rewrite_data_files(table => 'lake.s.t')`
- **THEN** the error names `rewrite_data_files` and says it is not available in embedded mode, and the table has no new snapshot

#### Scenario: drop_table
- **WHEN** an embedded user runs `CALL system.drop_table(table => 'lake.s.t')`
- **THEN** the error says it is not available in embedded mode and names `DROP TABLE` as the alternative

### Requirement: Embedded procedures run as the CLI user
Embedded mode SHALL run procedures with the catalog credentials of the attached catalog, without a role check. The catalog's commit authorization is the only gate.

#### Scenario: no role is required
- **WHEN** an embedded user, whose session has no roles, calls `set_current_snapshot`, an admin-tier procedure on a cluster
- **THEN** the main ref moves

Catalog-side refusal is the catalog's behaviour and is covered by its own tests. A sqlite catalog has no commit authorization, so this change adds no test for it.

### Requirement: The cluster's procedures are unchanged
On an SQE cluster, the six procedures SHALL keep their result rows, authorization tiers, audit entries and cache invalidation when they call the shared implementation.

#### Scenario: coordinator maintenance tests
- **WHEN** the coordinator's existing maintenance unit and integration tests run after the move
- **THEN** they pass without changes to their assertions
