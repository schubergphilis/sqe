### Added

- Bounding-box pruning for `ST_*` predicates on GEOMETRY columns (#863)

    `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Equals`, `ST_Touches`,
    `ST_Crosses` and `ST_Overlaps` between a geometry column and a constant
    geometry, and `ST_DWithin(column, constant, d)`, skip the data files whose
    manifest bounding box misses the constant's (grown by `d`), and the row
    groups whose Parquet geospatial statistics miss it, on the coordinator and
    on a worker. The predicate still runs on every row read, so a missing or
    unreadable bound only costs reads. `ST_Disjoint`, a negated predicate and
    GEOGRAPHY columns (no planar bounds) are never pruned on. Scan tickets
    carrying the prunes are version 7.
