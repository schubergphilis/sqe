### Fixed

- Each statement runs under one memory quota, across every execution context it builds, and Flight `DoPut` ingest is admitted and capped like an INSERT (#788)

    Admission now creates one query pool per statement, and every context
    the statement builds reserves from it: the SELECT and the load of a
    CTAS, the source and the target rewrite of a MERGE, an EXPLAIN ANALYZE.
    A MERGE used to collect its source under one quota and rewrite the
    target under a second, full one. An ADBC bulk ingest went straight to
    the writer: no concurrency permit, no per-user reservation, no refusal
    on a red coordinator pool, and its fan-out buffers charged no pool. It
    now passes the same gates as an INSERT and its buffers reserve from the
    statement's pool.
