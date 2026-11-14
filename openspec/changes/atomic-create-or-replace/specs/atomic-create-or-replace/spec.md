## ADDED Requirements

### Requirement: A schema-changing replace is one commit on the existing table
`CREATE OR REPLACE TABLE t AS SELECT ...` on an existing Iceberg table SHALL commit as one catalog update on that table. This holds whatever the SELECT's columns, types and partitioning. The table SHALL never be dropped, renamed or re-created, and no table with a staging name SHALL be created.

#### Scenario: Columns change
- **WHEN** `t` has columns `(a INT, b INT)` and `CREATE OR REPLACE TABLE t AS SELECT 1 AS a, 'x' AS c` runs
- **THEN** `t` has columns `(a, c)` and one row, its UUID is unchanged, and no table whose name contains `__sqe_replace_` exists in the namespace at any point

#### Scenario: Partitioning changes
- **WHEN** an unpartitioned `t` is replaced with `PARTITIONED BY (bucket(4, a))`, and later replaced again with no `PARTITIONED BY`
- **THEN** after the first replace the default spec is the bucket spec; after the second it is unpartitioned; the UUID is the same throughout

#### Scenario: Readers never see a gap
- **WHEN** one session alternates schema-changing replaces of `t` and another runs `SELECT * FROM t` and `information_schema.columns` for `t` in a loop
- **THEN** every SELECT succeeds and every columns query returns the full column list of either the old or the new schema, never zero rows and never an error

#### Scenario: Embedded catalog
- **WHEN** the embedded CLI runs a schema-changing replace on a table in an attached SQLite Iceberg catalog
- **THEN** the same single-commit result holds

### Requirement: History, refs and grants survive
A replace SHALL keep the table's UUID, location, earlier snapshots, branches other than `main`, tags and catalog grants. The replace snapshot SHALL have the previous `main` head as its parent.

#### Scenario: Time travel across the replace
- **WHEN** `t` at snapshot S1 has columns `(a, b)` and a schema-changing replace commits S2
- **THEN** `SELECT * FROM t FOR VERSION AS OF S1` returns the old rows with columns `(a, b)`, and `t$snapshots` lists S1 as the parent of S2

#### Scenario: Rollback across the replace
- **WHEN** after that replace `rollback_to_snapshot(t, S1)` runs
- **THEN** `main` points at S1 and `t` returns S1's rows. The current schema stays the replace's schema, as in Iceberg Java (a rollback moves `main`, not the schema): columns the replace dropped are gone, columns kept by name keep S1's values, and columns the replace added read as NULL

#### Scenario: Branches and tags kept
- **WHEN** `t` has a branch `dev` and a tag `v1` before the replace
- **THEN** both still exist afterwards and point at their old snapshots

#### Scenario: Grants kept
- **WHEN** a role holds a catalog grant on `t` and `t` is replaced with a new schema
- **THEN** the role still holds the grant and can read `t`

### Requirement: Field ids follow the column names
Columns of the new schema whose name exists in the current schema SHALL keep that column's field id, as long as the type is the same or an allowed Iceberg promotion. Every other column SHALL get a new id above the table's last assigned field id. Partition fields SHALL keep their id when source id and transform match. A schema or spec equal to an existing one SHALL reuse its id.

#### Scenario: Kept, promoted and new columns
- **WHEN** `t` has `(a INT, b INT, c STRING)` and is replaced by `SELECT CAST(1 AS BIGINT) AS a, 'x' AS b, 2 AS d`
- **THEN** `a` keeps its id (int to long), `b` gets a new id (int to string is not a promotion), `d` gets a new id, and `last-column-id` grows by two

#### Scenario: Old files read correctly after the type change
- **WHEN** after that replace a reader time-travels to the old snapshot
- **THEN** `b` reads as the old INT values, not as strings decoded from INT data

#### Scenario: Only kept required columns stay NOT NULL
- **WHEN** a column of the new schema is new, gets a new id, or was optional in the current schema
- **THEN** it is optional after the replace, even when the SELECT cannot produce NULL, because files written before the replace lack it or may hold nulls in it; a column that keeps the id of a required column, a map key and an identifier field stay required

#### Scenario: Shape seen before
- **WHEN** `t` is replaced with shape A, then shape B, then shape A again
- **THEN** the third replace reuses shape A's schema id (and spec id), and the table holds two schemas from these replaces, not three

### Requirement: Properties and policies as on the other replace paths
The replace commit SHALL set the merged properties: the statement's keys override, and the live keys stay except the reserved, location-bound and `sqe.json-columns` keys. `sqe.json-columns` SHALL be recomputed from the SELECT. `sqe.column-tags` SHALL keep only the columns that exist in the new schema. The replace SHALL be refused, before any data is written, wherever `refuse_replace_under_policy` refuses it today. Snapshot properties and lineage SHALL be recorded on the replace snapshot.

#### Scenario: Tags follow surviving columns
- **WHEN** `t` has tags on columns `a` and `b` and is replaced by a SELECT with columns `a` and `c`
- **THEN** after the replace `SHOW TAGS` lists the tag on `a` and none on `b` or `c`, and a mask on `a`'s tag still applies

#### Scenario: Dropped and re-added name
- **WHEN** a column `a` with a tag is replaced by a schema without `a`, and a later replace adds `a` back
- **THEN** the tag is gone after the first replace and does not come back with the second

#### Scenario: Snapshot properties
- **WHEN** `SET SNAPSHOT_PROPERTIES ('run' = '7')` precedes a schema-changing replace
- **THEN** the replace snapshot's summary holds `run = 7`

### Requirement: Failures leave the table untouched
When the SELECT, the write or the commit fails, the table SHALL be exactly as it was before the statement. SQE SHALL delete the data files the attempt wrote. The manifests and manifest list of a refused commit are left on storage, unreferenced, as on every other SQE write path; removing them is a follow-up for all write paths.

#### Scenario: Failing SELECT
- **WHEN** the SELECT of a schema-changing replace fails part-way
- **THEN** `t` has its old schema, data and snapshot, and no new files remain under its location

#### Scenario: Concurrent schema change
- **WHEN** an `ALTER TABLE t ADD COLUMN` commits while a schema-changing replace of `t` is writing
- **THEN** the replace fails with an error naming the concurrent schema change, the ALTER's result stands, and the replace's files are removed

#### Scenario: Concurrent append
- **WHEN** an `INSERT INTO t` commits while a schema-changing replace of `t` is writing
- **THEN** the replace retries and succeeds, `t` holds only the replace's rows, and the INSERT's snapshot stays in history

### Requirement: Replaces that cannot be atomic are refused
Before writing any data, SQE SHALL refuse a replace that would lower the table's format version. It SHALL also refuse a schema- or partition-changing replace while the session's `WRITE_BRANCH` is set.

#### Scenario: Format downgrade
- **WHEN** a v3 table is replaced `WITH ('format-version' = '2')`
- **THEN** the statement fails with a message naming both versions, and the table is unchanged

#### Scenario: Write branch
- **WHEN** `SET WRITE_BRANCH = 'dev'` is in effect and a schema-changing replace of `t` runs
- **THEN** the statement fails and tells the user to `RESET WRITE_BRANCH`, and `t` is unchanged
