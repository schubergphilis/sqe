### Fixed

- A distributed time-travel query reads the columns of the snapshot it asks for

    The coordinator resolved a worker's columns against the table's current
    schema. After a column was renamed, or dropped and re-added under the same
    name, `FOR VERSION AS OF` an older snapshot returned NULL for those
    columns, or read the newer column, whenever the scan ran on a worker. The
    coordinator's own scan was correct. Both now use the snapshot's schema.

- A worker no longer drops rows when a filter names a renamed column's old name

    The worker bound the pushed filter to file columns by name. After
    `RENAME COLUMN name TO label` and `ADD COLUMN name`, a filter on the new
    `name` ran against the old column in older files, and `WHERE name IS NULL`
    returned no rows. The worker now matches filter columns by field id and
    skips the filter on a file that lacks the column. Files with physical
    column names, such as Databricks' `col-1`, now get row-group pruning and
    late materialization on a worker too.

- A worker filters the right column in a file with a struct column before it

    The worker's row filter found a filter column by its position among the
    file's leaf columns and read it as a top-level column. With a struct
    column earlier in the file it evaluated another column and could keep
    or drop the wrong rows.
