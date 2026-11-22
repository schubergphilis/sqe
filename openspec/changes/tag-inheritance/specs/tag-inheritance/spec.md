## ADDED Requirements

### Requirement: Columns inherit their table's tags
A column's effective tags SHALL be its own tags plus every tag of its table whose key the column does not carry itself. Tag masks and tag row filters SHALL match effective tags.

#### Scenario: A table tag masks every column
- **WHEN** a tag policy nullifies `pii` and an admin runs `ALTER TABLE ns.t SET TAGS ('pii')`
- **THEN** every column of `ns.t` reads as NULL
- **AND** after `ALTER TABLE ns.t UNSET TAGS ('pii')` the columns read their values again

#### Scenario: SHOW EFFECTIVE POLICY reports inherited masks
- **WHEN** a tag policy masks `pii`, `ns.t` carries `pii` and has the columns `id` and `ssn`
- **THEN** `SHOW EFFECTIVE POLICY ON ns.t` lists a column mask on `id` and on `ssn`

#### Scenario: The column's own key wins
- **WHEN** `ns.t` carries `owner = finance` and its column `ssn` carries `owner = risk`
- **THEN** the effective tags of `ssn` hold `owner = risk` once

### Requirement: Inherited row filters apply to reads and writes
A row filter matched by a table tag SHALL hide rows from SELECT; UPDATE, DELETE and MERGE SHALL NOT change a hidden row, and a MERGE INSERT of a row the filter hides SHALL be refused. `SHOW STATS` SHALL NOT report the table's total row count to a row-filtered caller.

#### Scenario: UPDATE, DELETE and MERGE skip hidden rows
- **WHEN** a tag row filter `id > 1` matches the table tag `eu` and `ns.t` holds ids 1 and 2
- **THEN** `SELECT` returns id 2 only
- **AND** `UPDATE ns.t SET ...` and `DELETE FROM ns.t` leave the row with id 1 unchanged
- **AND** a MERGE matching id 1 (UPDATE or DELETE) leaves it unchanged, and a MERGE INSERT of id 0 is refused

### Requirement: Unreadable table tags fail closed
When the table tags of a table cannot be read (a malformed `sqe.table-tags`), the tag state of the table SHALL be unknown and every read of it SHALL be refused.

#### Scenario: Malformed table tags
- **WHEN** `sqe.table-tags` on `ns.t` is `{"not":"a list"}`
- **THEN** `SELECT ssn FROM ns.t` fails with "tag state unknown"

### Requirement: Inherited masks take part in conflicts
Under `mask_precedence = "refuse"`, a mask inherited from the table and a different mask on a column's own tag SHALL refuse the read.

#### Scenario: Two masks on one column
- **WHEN** `pii` nullifies, `fin` redacts, `ns.t` carries `pii` and its column `ssn` carries `fin`
- **THEN** `SELECT ssn FROM ns.t` fails with a mask conflict

### Requirement: Write authority reads own tags
The tag authority SHALL decide a tag write on the tags written at that level. A table tag SHALL NOT be treated as a column's own tag when the column's tags are set or unset.

#### Scenario: Untagging a column under a tagged table
- **WHEN** `ns.t` carries `pii` and a steward unsets the column tag `fin` on `ssn`
- **THEN** the removal check considers `fin` only

### Requirement: Columns inherit their schema's tags when enabled
With `policy.inherit_schema_tags = true`, a column's effective tags SHALL also hold every tag of its schema whose key neither the column nor its table carries. With the flag off (the default), schema tags SHALL NOT affect enforcement and SQE SHALL NOT read them when planning a query.

#### Scenario: Flag off ignores schema tags
- **WHEN** a tag policy nullifies `pii`, `inherit_schema_tags` is off and an admin runs `ALTER SCHEMA ns SET TAGS ('pii')`
- **THEN** the columns of every table in `ns` read their values

#### Scenario: A schema tag masks every table in the schema
- **WHEN** `inherit_schema_tags` is on, a tag policy nullifies `pii` and `ns` carries `pii`
- **THEN** every column of every table in `ns` reads as NULL, including a table created after the tag
- **AND** a second session that read a table before the tag was set reads NULL on its next statement
- **AND** after `ALTER SCHEMA ns UNSET TAGS ('pii')` the columns read their values again

#### Scenario: The nearest key wins
- **WHEN** `ns` carries `owner = finance`, `ns.t` carries `owner = risk`
- **THEN** the effective tags of every column of `ns.t` hold `owner = risk` once

#### Scenario: Unreadable schema tags fail closed
- **WHEN** `inherit_schema_tags` is on and `sqe.schema-tags` on `ns` is malformed, or the caller may not read the namespace
- **THEN** every read of a table in `ns` is refused with "tag state unknown"

#### Scenario: Schema row filters apply to writes
- **WHEN** `inherit_schema_tags` is on and a tag row filter `id > 1` matches the schema tag `eu`
- **THEN** `UPDATE` and `DELETE` on a table in the schema leave the row with id 1 unchanged
