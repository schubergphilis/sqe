## ADDED Requirements

### Requirement: Column tags follow the column's field id
When `sqe.column-tag-ids` holds a recorded field id for a key of `sqe.column-tags` and its `tags_sha256` matches the live `sqe.column-tags` value, the system SHALL apply that entry's tags to the column holding that field id in the scanned schema, under its current name.

#### Scenario: Foreign rename
- **WHEN** SQE tags `email` with `pii`, and another engine renames `email` to `mail` without touching the table properties
- **THEN** a query through SQE masks `mail` by `pii`
- **AND** `SHOW TAGS` lists `mail` / `pii` with `applied = true`

#### Scenario: Foreign drop and re-add
- **WHEN** another engine drops `mail` and adds a new column `mail`
- **THEN** the new `mail` has no tags
- **AND** `SHOW TAGS` lists the old entry with `applied = false`

#### Scenario: Rename through SQE
- **WHEN** SQE runs `ALTER TABLE t RENAME COLUMN email TO mail`
- **THEN** `sqe.column-tags` has the key `mail` and `sqe.column-tag-ids` maps `mail` to the same id, with a matching digest

### Requirement: Stale or missing ids never orphan an entry
An entry with no recorded id, or any entry when `tags_sha256` does not match the live `sqe.column-tags` value, SHALL apply by name. A malformed `sqe.column-tag-ids` SHALL make the tag state unknown (fail closed).

#### Scenario: Older table
- **WHEN** a table has `sqe.column-tags` = `{"email":["pii"]}` and no `sqe.column-tag-ids`
- **THEN** `email` is masked by `pii`

#### Scenario: Map rewritten by another writer
- **WHEN** ids were recorded, then another writer sets `sqe.column-tags` without updating `sqe.column-tag-ids`
- **THEN** every entry applies by name

#### Scenario: Malformed ids
- **WHEN** `sqe.column-tag-ids` is not valid JSON of the expected shape
- **THEN** a query on a tag-masked column fails closed

### Requirement: SQE writes ids from the committed schema
Every SQE write of `sqe.column-tags` in a commit that does not change the schema SHALL write `sqe.column-tag-ids` in the same commit, from the table's current schema, keyed by the resolved current names. A commit that replaces the schema (CREATE OR REPLACE) SHALL remove `sqe.column-tag-ids`.

#### Scenario: First write after upgrade records ids
- **WHEN** a table has name-only tags and SQE runs `SET TAGS` on any column
- **THEN** `sqe.column-tag-ids` records the id of every key that names a column of the current schema

#### Scenario: Writer follows a foreign rename
- **WHEN** after a foreign rename `email -> mail` a user runs `ALTER TABLE t ALTER COLUMN mail UNSET TAGS (pii)`
- **THEN** the `pii` tag is removed from the entry, and the map no longer holds `email`

### Requirement: The ids property is an admin's to write
A non-admin statement that sets or unsets `sqe.column-tag-ids`, in any case or spelling, SHALL be refused under every tag authority mode.

#### Scenario: Steward edits ids
- **WHEN** a non-admin runs `ALTER TABLE t SET TBLPROPERTIES ('sqe.column-tag-ids' = '{}')`
- **THEN** it is refused and nothing changes
