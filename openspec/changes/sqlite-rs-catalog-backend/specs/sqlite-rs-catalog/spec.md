# sqlite-rs-catalog

## ADDED Requirements

### Requirement: A catalog MAY be backed by a pure-Rust SQLite engine
SQE SHALL serve `TYPE sqlite` with a pure-Rust engine selected by `[catalog] sqlite_engine = "rust"`, providing the same Iceberg catalog operations as the C engine over the same file, with no C library in the dependency graph. The user-facing type name and its options SHALL be unchanged: no new `CatalogKind`, no new ATTACH type.

#### Scenario: the pure-Rust backend serves the catalog operations
- **GIVEN** a warehouse configured with `TYPE sqlite` and `sqlite_engine = "rust"`
- **WHEN** a namespace and a table are created, listed, loaded and dropped
- **THEN** every operation succeeds
- **AND** `cargo tree -i libsqlite3-sys` finds nothing in that feature set

#### Scenario: a missing database file is created
- **GIVEN** a configured catalog path with no file at it
- **WHEN** the catalog is first used
- **THEN** a valid SQLite database is created at that path
- **AND** the C engine opens the same file and reports the same empty schema

### Requirement: Both engines read and write one file format
A warehouse SHALL be interchangeable between the C engine and the Rust engine without migration, in both directions.

#### Scenario: C-engine write, Rust-engine read and commit, C-engine read
- **GIVEN** a warehouse created and populated through the C engine
- **WHEN** the Rust engine loads a table from it and commits a new snapshot
- **AND** the C engine then loads that table again
- **THEN** all three steps succeed and the metadata location the C engine reads is the one the Rust engine wrote

#### Scenario: Rust-engine write, C-engine read
- **GIVEN** a warehouse created from nothing through the Rust engine
- **WHEN** the C engine lists namespaces and tables in it
- **THEN** the listing matches what the Rust engine reports, row for row

### Requirement: A commit conflict is reported, never lost
A concurrent metadata-pointer update SHALL fail with `CatalogCommitConflicts` marked retryable, and SHALL NOT overwrite a pointer written since the committing session read it.

#### Scenario: a stale commit is refused
- **GIVEN** a table loaded by session A
- **AND** a newer snapshot committed by session B after A's load
- **WHEN** A commits
- **THEN** A receives `CatalogCommitConflicts` with `retryable` set
- **AND** the stored metadata location is still B's

#### Scenario: the compare and the write share one transaction
- **GIVEN** a commit in progress
- **WHEN** the process is interrupted between reading the current pointer and writing the new one
- **THEN** the database on disk holds the pre-commit pointer, with no partial row visible to either backend

### Requirement: The engine is confined to one thread, the handle is not
The catalog handle SHALL be `Send + Sync` as iceberg's `Catalog` trait requires, while the `!Send` engine state SHALL remain owned by a single thread for the catalog's lifetime.

#### Scenario: the handle crosses threads, the engine does not
- **GIVEN** a `sqlite` catalog handle
- **WHEN** it is cloned into several tokio tasks on a multi-threaded runtime and each runs a catalog operation
- **THEN** every operation succeeds
- **AND** all engine access happened on one thread

#### Scenario: the owning thread is released with the catalog
- **GIVEN** a session that attaches and detaches a `sqlite` catalog repeatedly
- **WHEN** the catalog handle is dropped
- **THEN** the owning thread terminates, and the thread count does not grow across iterations

#### Scenario: a dead engine thread errors instead of hanging
- **GIVEN** an engine thread that has terminated unexpectedly
- **WHEN** a catalog operation is issued
- **THEN** it returns an error naming the failed engine
- **AND** it does not block waiting for a reply that cannot arrive

### Requirement: The embedded catalog persists and is visible to a second process
The Rust engine SHALL serve the embedded warehouse: created on first
use, surviving a restart, and readable by a second process that opens the same
file.

#### Scenario: created on first use and surviving a restart
- **GIVEN** an embedded warehouse path with no file at it
- **WHEN** a table is created, the process exits, and a new process opens the
  same path
- **THEN** the table is listed and loadable, with the metadata location the
  first process wrote

#### Scenario: a second process sees a committed write
- **GIVEN** two processes attached to one warehouse file
- **WHEN** process 1 commits a snapshot
- **THEN** process 2 reads that snapshot, and neither process observes a
  partially written commit

### Requirement: Several catalogs MAY be attached, queried and joined
`ATTACH ... TYPE sqlite` SHALL support multiple simultaneous catalogs,
queryable by three-part name and joinable across catalogs, with `DETACH`
releasing the catalog.

#### Scenario: a join across two attached catalogs
- **GIVEN** three `sqlite` catalogs attached at once
- **WHEN** a query joins tables from two of them
- **THEN** the rows are identical to the same query against the C engine

#### Scenario: detach releases the catalog and its thread
- **GIVEN** an attached `sqlite` catalog
- **WHEN** it is detached
- **THEN** it disappears from the registry and `SHOW CATALOGS`, and the engine
  thread terminates

### Requirement: Two attachments of one file share one engine
Attachments naming the same database file SHALL resolve to a single engine
instance, keyed on the canonicalized path, refcounted across attachments.

This is a correctness requirement, not an efficiency one. POSIX `fcntl` locks
are owned by `(process, inode)`, so two engines inside one process are not
excluded by the file locks that protect two processes, and the failure mode is
interleaved journal writes rather than a busy error.

#### Scenario: the same file under two names
- **GIVEN** one database file attached under two catalog names
- **WHEN** a write goes through the first and a read through the second
- **THEN** the read observes the write, exactly one engine thread exists for
  that file, and `integrity_check` passes under stock `sqlite3`

#### Scenario: a symlinked or relative path resolves to the same engine
- **GIVEN** one file attached once by absolute path and once through a symlink
  or a relative path
- **WHEN** both attachments are active
- **THEN** they share one engine

#### Scenario: refcounted release
- **GIVEN** one file attached under two names
- **WHEN** the first is detached
- **THEN** the engine stays alive and the second keeps working
- **AND** detaching the second releases the engine, after which re-attaching
  builds a fresh one

### Requirement: Any SQLite database MAY be attached and queried
A SQLite file that does not hold an Iceberg catalog SHALL attach as a read-only
data source, exposing its own tables as queryable relations under schema `main`,
joinable against Iceberg tables in the same query. Mode SHALL be detected from
the schema and overridable with an explicit option.

#### Scenario: a foreign database attaches and returns the same rows as sqlite3
- **GIVEN** a database created and populated by stock `sqlite3`, with no Iceberg
  catalog tables in it
- **WHEN** it is attached and its tables are listed and queried
- **THEN** the tables are visible under `<name>.main` and every query returns
  the rows `sqlite3` returns for the same SQL

#### Scenario: joined against an Iceberg table
- **GIVEN** an attached foreign database and an Iceberg catalog
- **WHEN** one query joins a table from each
- **THEN** the result is correct and neither side is materialized incorrectly

#### Scenario: mode detection, and the override
- **GIVEN** a file containing `iceberg_tables` and `iceberg_namespace_properties`
- **WHEN** it is attached with no mode option
- **THEN** it is treated as a catalog
- **AND** attaching it with `MODE data` instead exposes those two tables as
  ordinary relations

#### Scenario: a column with mixed storage classes is refused, not guessed
- **GIVEN** a column declared `INTEGER` that holds text in one row
- **WHEN** the table is scanned
- **THEN** the error names the table, column and row, rather than returning a
  NULL or a coerced value

#### Scenario: writes are refused
- **GIVEN** an attached foreign database
- **WHEN** an `INSERT` targets one of its tables
- **THEN** it is refused with a clear message, and the file is unchanged

#### Scenario: the row ceiling is enforced and documented
- **GIVEN** a scope-3 table larger than the configured row ceiling
- **WHEN** it is scanned
- **THEN** the query fails with an error naming the ceiling and how to raise it,
  rather than materializing the whole table

### Requirement: A foreign catalog is refused for writes, never corrupted
A catalog whose tables carry an index the Rust engine does not maintain
(`sqlite_autoindex_*`, created by any engine that declares a composite
`PRIMARY KEY`) SHALL be readable and SHALL refuse writes with an actionable
error. SQE's own schema SHALL use a named unique index and no declared composite
primary key, so that both engines maintain and enforce it.

Measured on 2026-08-28: writing into such a catalog with the Rust engine leaves
rows out of the index, after which stock SQLite undercounts and
`PRAGMA integrity_check` reports rows missing. The corruption is silent at write
time, which is why this requirement is fail-closed rather than best-effort.

#### Scenario: a foreign catalog is read but not written
- **GIVEN** a warehouse created through the C engine, sqlx, Spark or pyiceberg
- **WHEN** it is opened with the Rust engine
- **THEN** listing and loading tables succeed
- **AND** any write is refused with an error naming the reason and
  `sqlite_engine = "c"` as the remedy

#### Scenario: integrity is checked after every write, not once
- **GIVEN** any write through the Rust engine to a SQE-created warehouse
- **WHEN** stock `sqlite3` runs `PRAGMA integrity_check` on the file
- **THEN** it reports ok

#### Scenario: no silent migration
- **GIVEN** a foreign catalog
- **WHEN** the Rust engine is selected for it
- **THEN** SQE does not rewrite its schema, and the refusal explains that
  adopting it requires recreating the catalog tables

### Requirement: Behavior parity with the C engine is asserted, not assumed
The existing catalog test suites SHALL run against both engines, and a failure under one and not the other SHALL be treated as a defect in the new engine rather than a difference to document.

#### Scenario: the coordinator suite passes on both
- **GIVEN** the 149-test coordinator registry suite
- **WHEN** it runs with the C engine and again with the Rust engine
- **THEN** both runs report the same pass count with zero failures

#### Scenario: the embedded warehouse suite passes on both
- **GIVEN** the `sqe-cli` embedded and `ATTACH` suites
- **WHEN** they run against each engine
- **THEN** the results match, including the persistence-across-restart cases
  and the multi-mount cases
