### Fixed

- `CREATE OR REPLACE TABLE ... TBLPROPERTIES (...) AS SELECT` sets the statement's properties when it replaces the table in place (#894)

    A replace with an unchanged schema swaps the contents of the live table and
    kept its old properties; only a schema-changing replace applied the new
    ones. The statement's properties now land in the same commit as the swap
    (also when an empty table is replaced by an empty SELECT). Properties the
    statement does not name stay, as in an Iceberg replace. A `CREATE OR
    REPLACE` that names `sqe.column-tags` needs an admin role, like `ALTER
    TABLE ... SET TBLPROPERTIES`: the replaced table keeps the snapshots those
    tags mask.
