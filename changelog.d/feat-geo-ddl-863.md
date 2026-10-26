### Added

- `CREATE TABLE` and `ALTER TABLE ADD COLUMN` take GEOMETRY and GEOGRAPHY columns (#863)

    `GEOMETRY[(crs)]` and `GEOGRAPHY[(crs[, algorithm])]` create Iceberg V3
    geospatial columns, where they used to fail with "SQL type not
    supported". The CRS is an SRID or a quoted CRS string; SRIDs map as Spark
    maps them (4326 is Iceberg's default `OGC:CRS84`, 0 is `SRID:0`, any
    other n is `EPSG:n`). The geography algorithm defaults to `spherical`.
    A table with such a column becomes format v3, and asking for v2 is an
    error, also on `ADD COLUMN`. CTAS from a geospatial table keeps the
    types. Spark 4.1 with Iceberg 1.12 describes and reads an SQE-created
    table. A geospatial type inside STRUCT, ARRAY or MAP is refused.
