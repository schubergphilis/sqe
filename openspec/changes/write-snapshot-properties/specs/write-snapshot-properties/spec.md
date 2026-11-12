## ADDED Requirements

### Requirement: Snapshot properties travel in the data commit
Every user write SHALL commit its snapshot with the snapshot properties in effect, in the same commit as its data. This covers INSERT, CTAS, INSERT OVERWRITE, DELETE, UPDATE, MERGE and Flight SQL bulk ingest in every ingest mode, in a transaction or not. Maintenance commits (compaction, expiry, orphan removal) SHALL NOT carry them.

#### Scenario: Autocommit insert carries the marker
- **WHEN** a session runs `SET SNAPSHOT_PROPERTIES ('copy.run' = '42')` and then `INSERT INTO t SELECT ...`
- **THEN** the snapshot that adds the rows has `copy.run = 42` in its summary, and no other commit to `t` was made for the marker

#### Scenario: Every write kind
- **WHEN** properties are set and each of INSERT, CTAS, INSERT OVERWRITE, DELETE, UPDATE, MERGE and bulk ingest (create, append, replace, create-append) runs, in a transaction and outside one
- **THEN** each committed snapshot carries the properties

#### Scenario: Maintenance does not carry them
- **WHEN** compaction rewrites a table whose head snapshot carries `copy.run`
- **THEN** the compaction snapshot's summary has no `copy.run` key

### Requirement: A write that changes nothing still records its properties
When snapshot properties are set and a write has no files to add or remove, SQE SHALL commit an empty snapshot carrying the properties. Without properties, such a write SHALL commit nothing, as before.

#### Scenario: Copying an empty table
- **WHEN** properties are set and `INSERT INTO t SELECT * FROM empty_source` inserts zero rows
- **THEN** `t` gets a new snapshot with `added-records` absent or 0 and the properties in its summary

#### Scenario: No properties, no empty snapshot
- **WHEN** no properties are set and a DELETE matches no rows
- **THEN** the table's snapshot list is unchanged

### Requirement: Transaction properties are scoped to the transaction
`SET SNAPSHOT_PROPERTIES` run inside a transaction SHALL set the properties of that transaction only. They SHALL apply to every write the transaction stages after the statement, and SHALL be dropped at COMMIT or ROLLBACK. Inside a transaction, session-level properties SHALL NOT apply.

#### Scenario: Published head carries the marker
- **WHEN** a client begins a transaction, sets `('copy.run' = '42')`, inserts into `t`, and commits
- **THEN** the snapshot `main` points to after COMMIT carries `copy.run = 42`

#### Scenario: Parallel transactions under one bearer session
- **WHEN** two transactions on the same session set different `copy.table` values and each writes its own table
- **THEN** each table's snapshot carries its own transaction's value

#### Scenario: Cleared at the end of the transaction
- **WHEN** a transaction that set properties commits, and the session then runs an autocommit INSERT with no session properties set
- **THEN** that INSERT's snapshot carries no user keys

### Requirement: Session properties persist until reset
`SET SNAPSHOT_PROPERTIES` run outside a transaction SHALL store the properties on the session. They SHALL apply to every later autocommit write of that session until `RESET SNAPSHOT_PROPERTIES` or `SET SNAPSHOT_PROPERTIES ()`. A new `SET` SHALL replace the whole set.

#### Scenario: Reset clears
- **WHEN** a session sets properties, writes, runs `RESET SNAPSHOT_PROPERTIES`, and writes again
- **THEN** the second snapshot carries no user keys

#### Scenario: Set replaces
- **WHEN** a session sets `('a' = '1', 'b' = '2')` and then `('a' = '3')`
- **THEN** the next write carries `a = 3` and no `b`

### Requirement: Properties that cannot be applied are refused
SQE SHALL return an error, and SHALL NOT ignore the statement, when the coordinator does not hold the session the statement runs in.

#### Scenario: Unheld session
- **WHEN** `SET SNAPSHOT_PROPERTIES` runs outside a transaction on a protocol path whose session the session manager does not hold, such as Trino HTTP
- **THEN** the statement fails with an error that names Flight SQL and transactions as the supported routes, and no later write carries the properties

#### Scenario: Trino HTTP inside a transaction
- **WHEN** a Trino HTTP client starts a transaction, runs `SET SNAPSHOT_PROPERTIES ('copy.run' = '42')`, inserts and commits, replaying `X-Trino-Transaction-Id` on each request
- **THEN** the published snapshot carries `copy.run = 42`

### Requirement: Bulk ingest accepts per-request properties
Flight SQL bulk ingest SHALL read `CommandStatementIngest.options` entries named `snapshot-property.<key>`. It SHALL validate them like `SET SNAPSHOT_PROPERTIES` and apply them to that request's commit only, on top of the transaction or session properties, with the request's value winning for a repeated key.

#### Scenario: Ingest option
- **WHEN** an ingest request carries option `snapshot-property.copy.run = 42`
- **THEN** the ingest snapshot carries `copy.run = 42`, and the next write on the session does not

#### Scenario: Invalid ingest option
- **WHEN** an ingest request carries `snapshot-property.operation = append`
- **THEN** the request fails with InvalidArgument before any data file is written

### Requirement: Keys and values are validated
SQE SHALL refuse a property set when any key:
- is empty, longer than 128 bytes, or holds a character outside `[A-Za-z0-9._-]`;
- matches, case-insensitively, an Iceberg computed summary key, `operation`, `wap.id`, `published-wap-id` or `source-snapshot-id`;
- starts with `partitions.`, `sqe.`, `commit.` or `write.`.

It SHALL also refuse a value longer than 1024 bytes or holding a control character, and a set of more than 32 pairs or more than 8 KiB of keys and values together. The error SHALL name the offending key.

#### Scenario: Reserved prefix
- **WHEN** a client runs `SET SNAPSHOT_PROPERTIES ('sqe.maintenance.kind' = 'x')`
- **THEN** the statement fails naming `sqe.maintenance.kind`, and the session's properties are unchanged

#### Scenario: Computed key
- **WHEN** a client sets `('added-records' = '0')`
- **THEN** the statement fails

### Requirement: Properties are readable as valid JSON
`t$snapshots` SHALL return the `summary` column as valid JSON for any key and value SQE accepts or another writer stored, including quotes, backslashes and non-ASCII text.

#### Scenario: Backslash in a value
- **WHEN** a snapshot's summary holds the value `C:\load\"x"`
- **THEN** `SELECT summary FROM "t$snapshots"` returns a string that a JSON parser reads back to that exact value
