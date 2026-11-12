### Changed

- The GRANT, REVOKE, DENY, SHOW GRANTS, policy DDL and masking-policy handlers moved from `query_handler.rs` into `query_handler/access_control.rs` (no behaviour change)

    The last statement family moved into its own child module. `query_handler.rs` goes from
    11,121 to 9,927 lines and its size ceiling drops from 11,500 to 10,250.
