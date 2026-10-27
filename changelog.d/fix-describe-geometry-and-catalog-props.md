### Added

- `ST_SRID(g)` returns the SRID of a geometry column's CRS

    4326 for `OGC:CRS84`, n for `EPSG:n`, 0 for any other CRS and for a
    computed geometry, which carries no CRS. NULL for a NULL value.

### Fixed

- DESCRIBE and SHOW CREATE TABLE print geospatial types with their CRS

    A GEOMETRY or GEOGRAPHY column printed as its `varbinary` storage, so
    the DDL SHOW CREATE TABLE produced re-created the column as binary. Both
    now print the Spark spelling the DDL accepts back: `geometry(4326)`,
    `geometry(3857)`, `geography(4326, karney)`, and a quoted CRS such as
    `geometry('ESRI:102100')` when it has no SRID.

- SHOW CREATE TABLE output re-creates the same table on any catalog

    It always names the `format-version`, since the server default is
    configurable, and prints a table's sort order as a comment line. Keys
    the catalog owns stay out of `TBLPROPERTIES`: `write.data.path`,
    `write.metadata.path`, `write.object-storage.*`, `gc.*`,
    `history.expire.*`, `databricks.*` and `io.unitycatalog.*`. Copied
    into a new table, the paths pointed it at the old table's files.
    Retention keys set by hand are hidden too; set them on the copy again.
