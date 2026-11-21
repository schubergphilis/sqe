## 1. Shared pieces

- [x] 1.1 The load observer's memory is owned by `CatalogOps` and shared with the observer; `forget` drops a table's entries under every catalog
- [x] 1.2 Key-level projection helper and a "tags before drop" / "clear dropped tags" pair

## 2. Statements

- [x] 2.1 SET TBLPROPERTIES: add before commit, remove after, under the table lock
- [x] 2.2 RENAME TABLE: tag destination, rename, clear source; both table locks in order
- [x] 2.3 DROP TABLE, DROP SCHEMA CASCADE, PURGE and `CALL system.drop_table`: clear after the drop
- [x] 2.4 CREATE [OR REPLACE] TABLE and CTAS: project the change after the commit

## 3. Verification

- [x] 3.1 Unit tests with a recording projector for the ordering and the observer reset
- [x] 3.2 Integration tests with a recording projector over the live catalog, one per statement
