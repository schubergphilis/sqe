### Fixed

- dbt-sqe 1.1.1: dbt finds its own relations when the profile's catalog is not the server's default, so a second `dbt seed` keeps the CSV's rows

    The adapter's metadata reads (`list_relations`, `get_columns_in_relation`,
    `list_schemas`, `check_schema_exists`, `get_catalog`) used a bare
    `information_schema`. SQE qualifies that with the session catalog, which
    dbt-sqe never sets, and loads a namespace's tables before an
    information_schema read only in the catalogs the statement names. With the
    profile on another catalog (a workspace catalog, for example) every lookup
    came back empty: a second `dbt seed` created nothing and appended the rows
    again (3 became 6), and incremental and table models ran as on a first run.
    Every read now names the profile's catalog,
    `"<catalog>".information_schema.<view>`. A seed whose table is not found
    also drops it before creating it, instead of `CREATE TABLE IF NOT EXISTS`,
    so a missed lookup can no longer append.
