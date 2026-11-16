### Added

- `sqe_idle_seconds`: how long the coordinator has run nothing (#902)

    0 while a statement runs, otherwise the seconds since the last one
    finished (or since start), for scale-to-zero. `sqe_active_queries` is
    now set every second from the query tracker; it used to change only
    when a streamed statement ended and could read stale.

### Changed

- Shutdown drains running statements instead of sleeping (#902)

    On SIGTERM the coordinator refuses new and queued statements with
    `RESOURCE_EXHAUSTED` reason `draining`, then waits until no statement
    runs or `coordinator.shutdown_drain_secs` passes, and exits as soon as
    the last one ends. It used to sleep the whole period whatever was
    running. A worker refuses new fragments with `Unavailable`, which the
    coordinator retries on another worker, and waits for its running scans
    within the same bound.
