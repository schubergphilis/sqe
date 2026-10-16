### Fixed

- A cold or disabled metadata cache no longer refuses CDC incremental reads with "tag state unknown" (#844)

    #842 read column tags off the Iceberg table planning loaded, but an
    incremental read plans through its own provider, so it still fell back
    to the metadata cache and was refused on a miss or with
    `catalog.metadata_cache_ttl_secs = 0`. The incremental provider now
    carries the table's properties. One built without them still falls back
    to the cache, never to "untagged".

- `FOR INCREMENTAL BETWEEN SNAPSHOT` resolves table names like a plain read (#845)

    A catalog-qualified name (`catalog.namespace.table`) now reads that
    catalog's table, a one-part name uses the session's default schema, and
    a name that is not an Iceberg table is refused.
