### Added

- `information_schema.table_tags`, `schema_tags` and `catalog_tags` list object tags (#922)

    The three views used to return no rows. `table_tags` reads each table's
    `sqe.table-tags` and shows the same tables as `column_tags`: a table the
    caller cannot load, or that the policy denies entirely, is left out, and
    the policy is asked even when the table has no column tags.
    `schema_tags` reads the namespace property `sqe.schema-tags` of every
    schema Polaris lets the caller load, as `SHOW SCHEMAS` does.
    `catalog_tags` reads the Polaris catalog property `sqe.catalog-tags`
    through the management API on the caller's token; a caller Polaris does
    not let read the catalog entity, and a catalog that is not Polaris REST,
    get no rows. An unreadable property leaves its object out with a
    warning.

### Fixed

- Table functions plan on a sqlite (`jdbc:sqlite`) catalog

    The pure-Rust sqlite catalog, used by `sqe-cli` and `jdbc:sqlite`
    catalogs, reported a missing table as an unexpected error,
    not as "table not found". DataFusion looks up a table function's name
    as a table before calling the function, so every table function query
    on that backend (`table_snapshots`, `information_schema.column_tags`)
    failed to plan. Missing and duplicate tables and namespaces now carry
    the same error kinds as the upstream sql catalog.
