## ADDED Requirements

### Requirement: Nanosecond timestamp columns

The system SHALL support Iceberg V3 nanosecond-precision timestamp types `timestamp_ns` and `timestamptz_ns` end-to-end: DDL parsing, scan, predicate pushdown, INSERT, and result rendering.

#### Scenario: CREATE TABLE with nanosecond timestamp columns

- **GIVEN** a connected SQE session with a configured Iceberg catalog
- **WHEN** the user runs `CREATE TABLE events (id BIGINT, ts TIMESTAMP_NS(9), tsz TIMESTAMPTZ_NS(9))`
- **THEN** the table is created with `PrimitiveType::TimestampNs` and `PrimitiveType::TimestamptzNs` schema fields
- **AND** the iceberg metadata records `format-version: 3`
- **AND** the table is readable from Spark 4.1 with matching schema

#### Scenario: INSERT and SELECT nanosecond timestamp roundtrip

- **GIVEN** a table with `ts TIMESTAMP_NS(9)`
- **WHEN** the user runs `INSERT INTO t VALUES ('2026-04-24 10:00:00.123456789')`
- **AND** then runs `SELECT ts FROM t WHERE id = 1`
- **THEN** the result is `2026-04-24T10:00:00.123456789`
- **AND** no precision is lost

#### Scenario: Predicate pushdown on nanosecond column

- **GIVEN** a partitioned table on `ts` with 100M rows
- **WHEN** the user runs `SELECT count(*) FROM t WHERE ts > TIMESTAMP_NS '2026-04-01 00:00:00'`
- **THEN** EXPLAIN ANALYZE shows partition pruning with `rows_pruned > 0`
- **AND** the predicate is pushed into the Iceberg scan

### Requirement: Column default values on schema evolution

The system SHALL support `DEFAULT <expr>` in `CREATE TABLE` and `ALTER TABLE ADD COLUMN`, mapping to Iceberg V3 `initial-default` and `write-default` field attributes. `write-default` applies to new rows; `initial-default` applies retroactively to existing rows added before the column existed.

#### Scenario: Default value applied to new inserts

- **GIVEN** `CREATE TABLE orders (id BIGINT, status STRING DEFAULT 'pending')` has been run
- **WHEN** the user runs `INSERT INTO orders (id) VALUES (1)`
- **THEN** `SELECT status FROM orders WHERE id = 1` returns `'pending'`

#### Scenario: Initial default on ALTER TABLE ADD COLUMN

- **GIVEN** table `orders (id BIGINT)` with 1M existing rows
- **WHEN** the user runs `ALTER TABLE orders ADD COLUMN region STRING DEFAULT 'unknown'`
- **AND** then runs `SELECT count(*) FROM orders WHERE region = 'unknown'`
- **THEN** the result is 1000000
- **AND** no data files are rewritten (schema-only change)

#### Scenario: Default expressions restricted to literals and simple casts

- **GIVEN** a CREATE TABLE with `status STRING DEFAULT current_timestamp()`
- **WHEN** the parser evaluates the default expression
- **THEN** the statement fails with an error naming `current_timestamp()` as unsupported
- **AND** the error message lists accepted default forms (literals, CAST, arithmetic on literals)

### Requirement: V3 table metadata version

The system SHALL write `format-version: 3` to table metadata when any V3-only feature is used in a CREATE TABLE or ALTER TABLE statement (nanosecond timestamps, column defaults, multi-arg transforms, V3 types). For purely V2 feature sets, `format-version: 2` is retained for maximum compatibility.

#### Scenario: V2 table stays V2

- **WHEN** the user creates a table using only V2 features (int, string, date, bucket partitioning)
- **THEN** `SHOW CREATE TABLE` reports `format-version: 2`

#### Scenario: V3 feature triggers V3 format

- **WHEN** the user creates a table with any `TIMESTAMP_NS` column
- **THEN** `SHOW CREATE TABLE` reports `format-version: 3`
