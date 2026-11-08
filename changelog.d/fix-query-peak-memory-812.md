### Fixed

- `peak_memory_bytes` is the query's own peak (#812)

    `system.runtime.queries`, the web UI and the audit record reported the
    coordinator pool's total use when the query ended, so every query
    running at the same time showed the sum of all of them. The query
    memory pool now keeps a high watermark of the query's own reservations,
    a join or memory retry included.
