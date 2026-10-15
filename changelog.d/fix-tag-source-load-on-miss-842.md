### Fixed

- A cold or disabled metadata cache no longer refuses governed Iceberg queries with "tag state unknown" (#842)

    Since #840 an unknown tag state refuses the statement. The tags were read
    from the table metadata cache, so `catalog.metadata_cache_ttl_secs = 0`
    refused every governed query, and an eviction or a commit racing the
    planning load refused single queries at random. The policy rewriter now
    reads `sqe.column-tags` from the Iceberg table that was loaded, with the
    caller's token, to plan the query. That needs no extra catalog call and
    does not depend on the cache. Hive tables, views and anything else the
    rewriter does not recognise still read their tags through the cache. An
    unparseable `sqe.column-tags` is still unknown and still refuses the
    statement.
