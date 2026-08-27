# hive-writes

## ADDED Requirements

### Requirement: A Hive external table MAY be written
`INSERT INTO` and `CREATE TABLE AS SELECT` SHALL be supported against a Hive external table, landing data files under the table's `k=v/` partition directories.

#### Scenario: insert into a partitioned table
- **GIVEN** a partitioned Hive external table with existing rows
- **WHEN** `INSERT INTO ... SELECT` supplies rows for two partitions, one new
- **THEN** files land under the correct `k=v/` directories
- **AND** a subsequent query returns the inserted rows plus what was there before

### Requirement: Partition columns are not written into data files
A written data file SHALL contain the table's file columns only. Partition columns are directory-encoded.

#### Scenario: the file itself carries no partition column
- **GIVEN** a table partitioned by `dt`
- **WHEN** a row is inserted
- **AND** the written data file is read DIRECTLY, not through the table
- **THEN** it contains no `dt` column

### Requirement: A write is visible to the next query
A write into a new partition SHALL be visible without a manual `MSCK REPAIR TABLE`.

#### Scenario: a new partition needs no repair
- **GIVEN** a table whose partition index does not mention `dt=2026-09-01`
- **WHEN** rows for that partition are inserted
- **THEN** the next query returns them

#### Scenario: an index update that fails is recoverable and says so
- **GIVEN** a write whose data files landed
- **WHEN** the partition index update fails
- **THEN** the error states that the rows were written but are not yet visible
- **AND** names `MSCK REPAIR TABLE` as the recovery
- **AND** `MSCK REPAIR TABLE` makes them visible

### Requirement: Every format round-trips
Rows written SHALL be readable back through the normal read path with the same count and the same values, per format.

#### Scenario: CSV round-trips under both header declarations
- **GIVEN** two CSV tables, one declaring `skip.header.line.count = 1` and one `= 0`
- **WHEN** the same three rows are inserted into each
- **AND** each is read back
- **THEN** both return exactly three rows

### Requirement: INSERT OVERWRITE is refused, never reinterpreted
`INSERT OVERWRITE` SHALL fail with a reason. It SHALL NOT be executed as an append.

#### Scenario: overwrite leaves the table untouched
- **GIVEN** a Hive external table with rows
- **WHEN** `INSERT OVERWRITE` is issued
- **THEN** the statement fails, naming why an overwrite cannot be made atomic
- **AND** the table's rows are unchanged

### Requirement: Concurrent writers do not lose files
Two writes to the same partition SHALL both land.

#### Scenario: two concurrent inserts
- **GIVEN** a partitioned Hive external table
- **WHEN** two inserts targeting the same partition run concurrently
- **THEN** the partition contains both writes' files
- **AND** a query returns the union of both writes' rows

### Requirement: A reader never sees a torn file
An individual data file SHALL become visible whole.

#### Scenario: local filesystem write
- **GIVEN** a Hive root on a local filesystem
- **WHEN** a file is written
- **THEN** it is written under a temporary name and renamed within the directory

## MODIFIED Requirements

### Requirement: A multi-file write is not atomic
The engine SHALL document, rather than provide, cross-file write atomicity for Hive external tables.

#### Scenario: a concurrent reader mid-write
- **GIVEN** an in-progress multi-file write
- **WHEN** another session queries the table
- **THEN** it may return a subset of the write's rows
- **AND** the reference page states this
