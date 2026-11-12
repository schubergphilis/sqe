### Changed

- The MERGE, INSERT and CTAS statement handlers moved from `query_handler.rs` into `query_handler/write.rs` (no behaviour change)

    The first statement family moved into its own child module of `query_handler`,
    following `query_handler/admission.rs`. `query_handler.rs` goes from 13,423 to
    13,062 lines.
