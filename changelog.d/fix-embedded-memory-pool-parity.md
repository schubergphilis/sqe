### Fixed

- Embedded mode (`sqe-cli --embedded`) shares its memory pool the way the coordinator does

    The embedded engine built a `FairSpillPool`, which splits the pool, minus
    every byte held by consumers that cannot spill, evenly across all
    registered spillable consumers. An Iceberg scan's decode reservations
    cannot spill, so each aggregate partition got a small slice: the engine
    bench's q3 (150M rows, 3 GB) spilled its final aggregate 96 times, where
    the coordinator, on the same data and limit, did not spill. The embedded
    pool is now the coordinator's default: first come, first served, with the
    top 20% kept for consumers that cannot spill. Each sort partition is
    capped at its share of the spillable bytes, as in the coordinator's
    per-query pool, so the last partition of a sort to start still finds
    room. Writes in embedded mode admit a sort-on-write against
    `--memory-limit` instead of the server's 256 MB `max_query_memory`
    default. Embedded mode still has no join-fallback retry like the
    coordinator's; the bench's join query (q2) runs without spills at 3 GB.
