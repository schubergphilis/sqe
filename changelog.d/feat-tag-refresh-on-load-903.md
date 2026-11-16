### Fixed

- A column tag committed by another engine masks the next query (#903)

    With a policy engine on, the first read of a table in each statement now
    sends a conditional GET to the catalog (304 when nothing changed) instead
    of trusting a cache entry for up to `metadata_cache_ttl_secs`. Before, a
    tag added by Spark or another SQE replica left the column readable until
    the soft TTL lapsed. Later reads of the table in the same statement still
    serve the cache. The cost is one catalog round trip per table per
    statement; `[catalog] revalidate_each_statement = false` restores the old
    behaviour. Tag lookups that fall back to the shared cache now take the
    most recently validated entry. Projecting the new tag into Ranger on load
    is not part of this change.
