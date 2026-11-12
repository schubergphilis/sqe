### Changed

- The time-travel and incremental-read handlers moved from `query_handler.rs` into `query_handler/time_travel.rs` (no behaviour change)

    The third statement family moved into its own child module: `FOR SYSTEM_TIME AS OF`,
    `FOR VERSION AS OF` and `FOR INCREMENTAL BETWEEN SNAPSHOT` handling, with the
    `TimeTravelCleanup` guard that deregisters the pinned providers. `query_handler.rs` goes
    from 12,236 to 11,675 lines and its size ceiling drops from 12,500 to 12,000.
