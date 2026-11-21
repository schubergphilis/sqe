# Keep Ranger's tag projection in step with table DDL (#928)

## Why

With `[policy.ranger] project-tags = true`, SQE copies each table's column tags into Ranger's tag store so that other engines reading Ranger (Spark's plugin) mask the same columns. Only `SET/UNSET TAGS` and a column rename projected their change. Four statements changed or removed a table's tags without telling Ranger:

- `ALTER TABLE ... SET TBLPROPERTIES ('sqe.column-tags' = ...)`
- `CREATE [OR REPLACE] TABLE` / CTAS, which can carry `sqe.column-tags` in its properties or trim the tags of a replaced table to its surviving columns (#896)
- `ALTER TABLE ... RENAME TO`
- `DROP TABLE` (plain, `PURGE`, `CALL system.drop_table`, `DROP SCHEMA ... CASCADE`)

The load observer of #903 catches the first two on the next load of the table, so the drift lasts until then. Nothing ever loads a dropped or renamed table's old name again, so for RENAME and DROP the old resource stays in Ranger. A table later created under that name inherits its masks in other engines. The new name of a renamed table has no tags in Ranger, so other engines return its tagged columns raw.

## What Changes

- Each of these statements projects its tag change into Ranger as part of the statement.
- Every intermediate state over-masks. When a projection fails part-way, other engines mask more than SQE does, never less:
  - SET TBLPROPERTIES adds the new tags to Ranger before it commits, and removes the dropped tags after the commit.
  - RENAME tags the destination before the rename and clears the source after it.
  - DROP clears the resource after the drop.
- After a DDL statement projects a table, the load observer forgets what it last projected for that table. Its memory cannot then skip a later projection under the same name.
- A tag value that is not a tag map is not projected, as in the load observer.

## Impact

- With `project-tags` off nothing changes: no extra table load and no Ranger call.
- With it on, DROP and RENAME load the table once more before they run (to know its tags), and CREATE TABLE / CTAS load it once after.
- A failed projection fails the statement only when other engines would otherwise under-mask a live table. In that case the error says the statement committed and how to repair (`CALL system.reproject_column_tags` or re-running the statement). A failed clean-up of a dropped or renamed-away name is a warning, because every remaining state over-masks.
- UNSET TBLPROPERTIES is not supported (the book says so), so it has no projection path.
