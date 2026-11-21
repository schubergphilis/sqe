**STATUS 2026-11-21: table DDL projects its tag change into Ranger (`fix/tag-projection-drift-928`).**

With `project-tags` on, only `SET/UNSET TAGS` and a column rename projected; the #903 load observer caught SET TBLPROPERTIES and CREATE OR REPLACE on the next load, and nothing ever caught RENAME or DROP. OpenSpec change `tag-projection-ddl-sync`.

- SET TBLPROPERTIES on `sqe.column-tags`: under the table lock, projects the union of old and new before the commit (a failure commits nothing), then the committed tags after it (a failure there is a warning: Ranger holds extra tags only).
- RENAME: both table locks in name order; tags the destination before the rename (a failure renames nothing), clears the source after it, or clears the destination if the rename fails.
- DROP TABLE, DROP SCHEMA CASCADE, PURGE, `CALL system.drop_table`: read the tags under the table lock before the drop, clear them after it; a failed clear is a warning.
- CREATE [OR REPLACE] TABLE / CTAS: projects the diff after the commit. A failure errors with "was created and COMMITTED" and the `reproject_column_tags` repair.
- The load observer's memory is now shared with `CatalogOps`; every DDL projection forgets the table's entry, so a table later created under the name by another client is projected on its next load.

Gate: `make test-integration FILTER=tag_projection_928` -> see the MR (six tests in `tests/it/tag_projection_ddl_e2e.rs`, recording projector). Unit: `cargo test -p sqe-write --lib -- tag union` -> 61 passed.

Not covered: UNSET TBLPROPERTIES (not supported). Hive tables (the Hive DDL path has its own tag source).

**NEXT:** #927 APPLY TAG access type (track 1.8).
