### Added

- dbt-sqe 1.1.0: a `table_properties` model config (alias `tblproperties`) sets Iceberg table properties, and snapshots run (#891)

    `{{ config(table_properties={'write.merge.mode': 'merge-on-read'}) }}` is
    emitted as `TBLPROPERTIES (...)` on the CTAS of `table` and `incremental`
    models (an incremental model takes new values on `--full-refresh`). The
    delete+insert staging table takes none. dbt's snapshot materialization now
    runs on SQE for the `timestamp` and `check` strategies: the adapter names
    each column's SQL type from the Arrow schema, which dbt's snapshot and
    contract macros need, and drops the `__dbt_tmp` staging table afterwards.

### Fixed

- dbt-sqe: a second `dbt seed` replaces the rows instead of appending them (#891)

    The seed re-run truncates the table through a statement whose result is
    never read, so the TRUNCATE never ran. It is now read on execute, like the
    other DML. Also fixed: `dbt docs generate` (it selected `table_type` from
    `information_schema.columns`, which has none), `--full-refresh` of an
    incremental model (it appended), and an incremental model replacing a view
    of the same name. The requirements lock matches `setup.cfg` again
    (adbc-driver-flightsql 1.13.0).
