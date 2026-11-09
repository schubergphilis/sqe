### Fixed

- CTAS, INSERT, DELETE, UPDATE, MERGE and TRUNCATE run under `query.max_query_memory`, like a SELECT (#788)

    The write statements built their execution context on the shared
    coordinator pool. Their write buffers and the SELECT feeding a CTAS or
    INSERT reserve from that context, so one large write could take the
    whole pool while a read was held to its quota. They now get the same
    per-query quota, unspillable headroom and sort share as a read. A Hive
    write refused by the quota returns `RESOURCE_EXHAUSTED` naming the
    setting, as the Iceberg write path already did, not `EXECUTION_FAILED`.
