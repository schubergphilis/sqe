### Changed

- The SHOW STATS, ANALYZE, ATTACH / DETACH and secret handlers moved from `query_handler.rs` into `query_handler/catalog.rs` (no behaviour change)

    The fourth statement family moved into its own child module. `query_handler.rs` goes from
    11,672 to 11,286 lines and its size ceiling drops from 12,000 to 11,750.
