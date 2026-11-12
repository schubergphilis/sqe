### Fixed

- A catalog-qualified Iceberg metadata table reads the catalog it names (#317)

    `cat.ns."t$refs"` (and `$snapshots`, `$manifests`, `$history`, `$files`,
    `$partitions`) rewrites to `table_refs('ns', 't', 'cat')`, but the
    coordinator ignored the catalog argument and loaded `ns.t` from the
    session's default catalog. Over Flight SQL, where no `X-Trino-Catalog`
    sets that default, a table in a second catalog failed with TableNotFound
    (or read a same-named table elsewhere). The coordinator now picks the
    named catalog from the ones the session built with the caller's
    credentials; an unknown name is a planning error.
