# trino-table-properties Specification

## Purpose
How SQE handles Trino-style table properties (`format_version`, `partitioning`, `sorted_by`, `location`, a table comment) on CREATE TABLE, CTAS and `ALTER TABLE ... SET PROPERTIES`: each one is honoured or refused, never silently dropped, so a statement written for Trino creates the same Iceberg table in SQE.
## Requirements
### Requirement: The format version a statement asks for is the table's version
CREATE TABLE and CTAS SHALL create the table at the version named by `format_version` (a number or a string), `'format-version'` or `TBLPROPERTIES ('format-version' = ...)`. Without one, the table SHALL get `catalog.default_table_format_version`. Asking for a version below what a column needs SHALL be an error. `format_version` SHALL NOT be stored as a table property.

#### Scenario: dbt-trino mart on a V2-default server
- **WHEN** the server default is 2 and `create table t WITH (format_version = '3') as (select 1 as id)` runs through the Trino door
- **THEN** `t` is format version 3 and has no `format_version` property

#### Scenario: A variant column with a lower version
- **WHEN** `CREATE TABLE t WITH (format_version = 2) AS SELECT parse_json('{}') AS v` runs
- **THEN** it fails naming the column kind that needs version 3

### Requirement: SET PROPERTIES upgrades and never downgrades
`ALTER TABLE t SET PROPERTIES format_version = N` (and `SET TBLPROPERTIES ('format-version' = 'N')`) SHALL upgrade `t` to N in one commit with the other properties of the statement. The same version SHALL be a no-op. A lower version SHALL be refused, and the table SHALL be left unchanged.

#### Scenario: Upgrade then writes
- **WHEN** a V2 table is upgraded with `SET PROPERTIES format_version = 3` and then gets an INSERT, an UPDATE and a DELETE
- **THEN** the table is version 3 and each write succeeds

### Requirement: A table comment survives beside an options list
`create table t comment '...' WITH (...) as (...)`, in either clause order, SHALL parse and store the comment as the `comment` property. A plain `COMMENT '...'` SHALL be stored the same way. `WITH (...)` beside `TBLPROPERTIES (...)` SHALL keep the items of both lists.

#### Scenario: dbt-trino persist_docs
- **WHEN** `create table t comment 'a mart' WITH (format_version = 2) as (select 1 as id)` runs through the Trino door
- **THEN** `t` has property `comment = a mart` and is version 2

### Requirement: Every Trino table property is honoured or refused
Each unquoted key in a `WITH (...)` list SHALL either take effect as `design.md` D2 maps it or fail the statement with an error naming the key. An unknown unquoted key SHALL fail with an error that lists the supported names. A quoted key or a `TBLPROPERTIES` key SHALL be stored as the Iceberg property it names.

#### Scenario: Unknown property
- **WHEN** `CREATE TABLE t WITH (partitionning = ARRAY['id']) AS SELECT 1 AS id` runs
- **THEN** it fails naming `partitionning`, and no table `t` exists

#### Scenario: Partitioning in Trino's argument order
- **WHEN** `CREATE TABLE t WITH (partitioning = ARRAY['bucket(id, 4)', 'day(ts)']) AS SELECT ...` runs
- **THEN** `t`'s default spec is `bucket[4](id), day(ts)`

#### Scenario: Sort order is metadata only
- **WHEN** `CREATE TABLE t WITH (sorted_by = ARRAY['id DESC']) AS SELECT ...` runs
- **THEN** `t`'s default sort order is `id DESC NULLS LAST`, and no data file written by the statement claims that sort order

#### Scenario: Refused location
- **WHEN** `location` names a path the catalog does not allow
- **THEN** the statement fails with the catalog's error and no table exists, at that location or anywhere else
