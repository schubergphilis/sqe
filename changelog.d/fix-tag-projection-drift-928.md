### Fixed

- Table DDL keeps Ranger's tag projection in step with the table (#928)

    With `[policy.ranger] project-tags = true`, `SET TBLPROPERTIES ('sqe.column-tags' = ...)`, `CREATE [OR REPLACE] TABLE`, CTAS, `RENAME` and every table drop (`DROP TABLE`, `PURGE`, `CALL system.drop_table`, `DROP SCHEMA ... CASCADE`) now project their tag change into Ranger's tag store. Before, a renamed table's new name had no tags in Ranger, so Spark returned its tagged columns raw, and a dropped table's tags stayed behind for any table later created under that name. A failure part-way leaves Spark masking more than SQE, never less. Nothing changes with projection off.
