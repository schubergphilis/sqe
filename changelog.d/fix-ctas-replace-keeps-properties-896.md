### Fixed

- A schema-changing `CREATE OR REPLACE TABLE ... AS SELECT` keeps the table's properties and column tags (#896)

    A replace whose SELECT changes the columns or the partitioning is staged
    under a temporary name and renamed into place. The staged table was
    created with the statement's own options only, so every property of the
    live table was lost, `sqe.column-tags` among them, and tag-based masks
    and row filters stopped applying to the replaced table. The staged table
    now takes every live property the statement does not set, as the
    same-schema replace already did (#894). Column tags are kept for the
    columns the new schema still has; a dropped column's tags go. The
    reserved keys, the old table's storage paths and `sqe.json-columns`
    (recomputed from the SELECT) are not carried.
