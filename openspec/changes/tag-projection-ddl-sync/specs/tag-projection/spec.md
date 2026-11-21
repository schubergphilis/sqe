## ADDED Requirements

### Requirement: Table DDL projects its tag change
With `project-tags` on, a statement that changes or removes a table's `sqe.column-tags` SHALL bring Ranger's tag store in line with the committed tags before it returns. This covers SET TBLPROPERTIES, CREATE [OR REPLACE] TABLE, CTAS, RENAME TABLE and every table drop path (DROP TABLE, PURGE, `CALL system.drop_table`, DROP SCHEMA CASCADE).

#### Scenario: Tags written as a table property
- **WHEN** an admin runs `ALTER TABLE t SET TBLPROPERTIES ('sqe.column-tags' = '{"ssn":["pii"]}')`
- **THEN** Ranger's resource for `t` tags `ssn` with `pii` when the statement returns

#### Scenario: Rename
- **WHEN** a tagged table `a` is renamed to `b`
- **THEN** Ranger tags `b`'s columns as `a`'s were, and `a`'s resource carries no tags

#### Scenario: Drop
- **WHEN** a tagged table is dropped by any drop path
- **THEN** its Ranger resource carries no tags

#### Scenario: Replace trims tags
- **WHEN** `CREATE OR REPLACE TABLE` drops a tagged column
- **THEN** that column's tags are removed from Ranger

### Requirement: Projection failures over-mask
A projection failure SHALL leave other engines masking at least what SQE masks. A statement whose failure would leave a live table under-masked SHALL fail, or SHALL report that it committed together with the repair. A failed clean-up that leaves only extra tags SHALL be logged and SHALL NOT fail the statement.

#### Scenario: Ranger down during SET TBLPROPERTIES
- **WHEN** the projection of a new tag fails
- **THEN** nothing is committed and the statement fails

#### Scenario: Ranger down during RENAME
- **WHEN** tagging the destination fails
- **THEN** the table is not renamed and the statement fails

#### Scenario: Clean-up fails after DROP
- **WHEN** the table is dropped and clearing its resource fails
- **THEN** the DROP succeeds and a warning is logged

### Requirement: DDL resets the load observer
After a DDL statement projects a table, the load observer SHALL hold no remembered projection for that table name. The next load of a table under that name SHALL then project its tags.

#### Scenario: Recreated under a dropped name
- **WHEN** a tagged table is dropped, and a table with the same name and tags is created by another client
- **THEN** the next SQE load of the new table projects its tags
