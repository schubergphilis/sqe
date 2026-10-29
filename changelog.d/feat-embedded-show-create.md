### Added

- Embedded `SHOW CREATE TABLE` for Iceberg tables

    `sqe-cli --embedded` answered `SHOW CREATE TABLE` for Hive tables only.
    On the `--warehouse` catalog and on attached Iceberg catalogs it now
    prints the same DDL as a coordinator: the columns, `PARTITIONED BY`, the
    sort order as a comment and `TBLPROPERTIES`. The rendering moved from the
    coordinator to `sqe_catalog::show_create`, so both print the same text,
    and the printed DDL creates the table again.
