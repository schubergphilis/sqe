## ADDED Requirements

### Requirement: Persisted partition index
The system SHALL persist the partition list of a Hive external table in the manifest's `sqe.partition_index` slot. Each entry SHALL carry the partition key values and the partition location, because a Hive partition may live outside the table prefix. When an index is present, read planning SHALL derive its file list from the index instead of listing the table location. When an index is absent, reads SHALL keep cycle-1 listing behavior unchanged.

#### Scenario: Indexed read plans without listing
- **WHEN** a table's manifest carries a populated `partition_index` and a query scans it
- **THEN** the scan is planned from index entries and no recursive LIST of the table location is performed for planning

#### Scenario: Un-indexed table keeps discovery behavior
- **WHEN** a table's manifest has `partition_index: null` and a query scans it
- **THEN** partition discovery behaves exactly as cycle 1, including leading-equality prefix pruning and full-prefix fallback

#### Scenario: Out-of-prefix partition is readable through the index
- **WHEN** an entry was added with a LOCATION outside the table prefix and that partition is queried
- **THEN** files from the out-of-prefix location are scanned with the correct partition values

#### Scenario: Index entry locations pass the allowlist on load
- **WHEN** a manifest is loaded whose index contains an entry location failing the `[storage.tvf]` check
- **THEN** the load fails closed, as manifest `storage_descriptor.location` does today

### Requirement: Index typing and NULL sentinel
Index entries SHALL store partition values as strings and type them on load from the declared partition-key types, supporting the same key types as cycle-1 path discovery (`string`, `varchar`, `int`, `bigint`, `date`). The value `__HIVE_DEFAULT_PARTITION__` in an entry SHALL normalize to NULL on read.

#### Scenario: Typed partition values prune correctly
- **WHEN** a query filters an int-typed partition column to `= 42` against an indexed table
- **THEN** only entries whose typed value equals 42 are scanned

#### Scenario: Default-partition sentinel normalizes to NULL
- **WHEN** an index entry stores `__HIVE_DEFAULT_PARTITION__` for a key and a query filters `WHERE key IS NULL`
- **THEN** that entry matches and its rows return with the partition column NULL

### Requirement: Staleness model
A stale index SHALL omit partitions, never invent them. The system MUST NOT validate index entries against storage at plan time, because that validation reintroduces the listing cost the index exists to remove. `MSCK REPAIR TABLE` SHALL be the documented remedy for staleness, named in documentation and in repair-related messages.

#### Scenario: Partition written out of band is missing until repair
- **WHEN** files land in a new partition directory after the index was built and the table is queried without repair
- **THEN** the new partition's rows are absent from results, and re-running after `MSCK REPAIR TABLE` returns them

### Requirement: ALTER TABLE ADD PARTITION
The system SHALL support `ALTER TABLE ... ADD PARTITION (k = 'v', ...) [LOCATION 'url']` on Hive external tables by appending one index entry and rewriting the manifest, without touching data files or re-listing the table. File count and size for the entry SHALL be derived from the partition location metadata at add time. A duplicate ADD without `IF NOT EXISTS` SHALL be an error naming the partition; with `IF NOT EXISTS` it SHALL be a no-op.

#### Scenario: Add one partition without a re-scan
- **WHEN** `ALTER TABLE orders ADD PARTITION (dt='2026-01-03') LOCATION 's3://raw/orders/dt=2026-01-03/'` runs
- **THEN** the manifest gains exactly one entry and no listing of other partitions occurs

#### Scenario: Duplicate ADD errors without IF NOT EXISTS
- **WHEN** the same partition is added twice, the second time without `IF NOT EXISTS`
- **THEN** the second statement fails naming the partition, and the manifest is unchanged

#### Scenario: Duplicate ADD with IF NOT EXISTS is a no-op
- **WHEN** the same partition is added twice, the second time with `IF NOT EXISTS`
- **THEN** the statement succeeds, the manifest keeps one entry, and the original entry is preserved

### Requirement: ALTER TABLE DROP PARTITION
The system SHALL support `ALTER TABLE ... DROP PARTITION (k = 'v', ...)` on Hive external tables by removing matching index entries and rewriting the manifest. Data files MUST NOT be deleted. `DROP PARTITION ... PURGE` SHALL be rejected with a message naming cycle 4. Dropping an absent partition without `IF EXISTS` SHALL be an error; with `IF EXISTS`, a no-op.

#### Scenario: Drop removes only the index entry
- **WHEN** `ALTER TABLE orders DROP PARTITION (dt='2026-01-02')` runs against a table with three indexed partitions
- **THEN** the index has two entries, the dropped partition no longer appears in queries, and all data objects remain in storage

#### Scenario: PURGE is rejected
- **WHEN** `ALTER TABLE orders DROP PARTITION (dt='2026-01-02') PURGE` runs
- **THEN** the statement fails with a rejection naming `hive_external` and cycle 4

### Requirement: MSCK REPAIR TABLE
The system SHALL support `MSCK REPAIR TABLE <t>` on Hive external tables by recursively listing the table location, deriving partition entries from `k=v/` path segments using cycle-1 discovery rules, and rewriting the whole index. Paths that are not a complete typed `k=v/` partition (a file at the table root, a non-`k=v` directory, an undeclared or missing key, or a value that does not parse to the declared type) SHALL be skipped, matching Hive and Athena, and SHALL NOT fail the repair. Neighboring valid partitions SHALL still be indexed. Exceeding the entry cap SHALL still fail naming the cap, leaving the existing index unchanged.

#### Scenario: Repair rebuilds from storage
- **WHEN** `MSCK REPAIR TABLE` runs on a table whose directory holds five `dt=` partitions while its index held two
- **THEN** the rewritten index contains five entries and subsequent queries see all five partitions without listing

#### Scenario: Untypeable segment is skipped
- **WHEN** the table declares `dt date` and a directory contains both `dt=not-a-date/` and `dt=2026-01-02/`
- **THEN** repair succeeds, the index contains only `dt=2026-01-02`, and the untypeable directory is omitted

#### Scenario: Non-k=v directory is skipped
- **WHEN** the table location holds valid `dt=` partitions and a `not-a-partition/` directory with files
- **THEN** repair succeeds and the index contains only the valid `dt=` partitions

#### Scenario: File at the table root is skipped
- **WHEN** a file sits directly under the table location next to valid `dt=` directories
- **THEN** repair succeeds and the index contains only the valid `dt=` partitions

#### Scenario: Projection-enabled table is a no-op
- **WHEN** `MSCK REPAIR TABLE` runs on a table with `projection.enabled=true`
- **THEN** the statement succeeds, no storage listing is performed, and the partition index is left unchanged because projection owns the partition set

### Requirement: SHOW PARTITIONS
The system SHALL support `SHOW PARTITIONS <t>` on Hive external tables. It SHALL read the index when present and list the table location when absent, returning one row per partition.

#### Scenario: Show partitions from the index
- **WHEN** `SHOW PARTITIONS` runs against an indexed table
- **THEN** output lists every index entry without any storage listing

#### Scenario: Show partitions falls back to listing
- **WHEN** `SHOW PARTITIONS` runs against a table with no index
- **THEN** partitions are discovered by listing and returned in the same format

### Requirement: Index caps
A new `max_partition_index_entries` configuration value SHALL bound the number of index entries, enforced at ADD, REPAIR, and manifest load. Exceeding the cap SHALL be an error naming the cap and the config key, never a truncated index.

#### Scenario: Cap enforced at repair
- **WHEN** `MSCK REPAIR TABLE` discovers more partitions than `max_partition_index_entries`
- **THEN** repair fails naming the cap and the config key, and the existing index is left unchanged

#### Scenario: Oversized index rejected on load
- **WHEN** a planted manifest carries more index entries than the cap
- **THEN** the manifest load fails closed

### Requirement: Policy gates apply to index maintenance
Every index-maintenance statement (`ADD/DROP PARTITION`, `MSCK REPAIR`, `SHOW PARTITIONS`) SHALL pass the same policy resolution (`resolve_table_kind`) and per-user authorization as cycle-1 statements. Audit events SHALL be emitted with the same resource naming policy uses.

#### Scenario: Denied user cannot mutate the index
- **WHEN** a user without write authorization on the table runs `ALTER TABLE ... ADD PARTITION`
- **THEN** the statement is refused and the manifest is unchanged

## ADDED Requirements

### Requirement: Statement classification
The classifier SHALL recognize `MSCK REPAIR TABLE`, `SHOW PARTITIONS`, and `ALTER TABLE ... ADD/DROP PARTITION` on Hive tables as distinct statement kinds routed to the Hive DDL handler, replacing the current generic not-implemented rejections. Rejections of unsupported variants SHALL name `hive_external` precisely.

#### Scenario: MSCK reaches the handler
- **WHEN** `MSCK REPAIR TABLE lake.sales.orders` is submitted
- **THEN** it classifies as a Hive repair statement and executes against the index, not "Statement type not supported"

#### Scenario: Non-Hive ALTER ADD PARTITION keeps Iceberg path
- **WHEN** `ALTER TABLE ... ADD PARTITION` targets an Iceberg table
- **THEN** routing is unchanged from today
