### Added

- `ALTER TABLE ... SET PROPERTIES` honours the Trino properties `partitioning` (partition evolution), `sorted_by` (replaces the default sort order) and `extra_properties` (#897)

    dbt-trino changes a table's layout with `SET PROPERTIES`, and SQE stored
    `partitioning` and `sorted_by` as plain table properties. They now evolve the
    partition spec and the sort order in one commit; a partition field the table
    had before keeps its field id, and going back to an earlier layout reuses its
    spec. `location` and `p = DEFAULT` are refused. Other keys are stored as
    Iceberg properties, as before, and `SET TBLPROPERTIES` (quoted keys) is
    unchanged.

### Changed

- `ALTER TABLE ... SET PROPERTIES` keeps an identifier key unquoted, so `partitioning`, `sorted_by`, `extra_properties`, `gc_enabled`, the Parquet keys and the refused Trino keys there now mean the Trino property instead of a plain table property; `SET PROPERTIES k = DEFAULT` is refused (#897)
