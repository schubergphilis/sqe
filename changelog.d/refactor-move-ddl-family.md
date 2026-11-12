### Changed

- The DROP, CREATE VIEW and COMMENT ON handlers moved from `query_handler.rs` into `query_handler/ddl.rs` (no behaviour change)

    The fifth statement family moved into its own child module. `query_handler.rs` goes from
    11,287 to 11,088 lines and its size ceiling drops from 11,750 to 11,500.
