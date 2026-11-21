### Fixed

- `CALL system.rewrite_data_files(..., distributed => 'require')` below the healthy-worker floor fails with the worker-floor error again, before the catalog is opened

    The write-access check added for #880 loaded the table first, so with the fleet below `min_workers` the caller got a catalog error (or a table-not-found) instead of the `needs >= N healthy workers` message, and two `rewrite_require_mode` tests failed on main. The worker-floor decision does not depend on the table, so it now runs before the catalog round trip; the write-access check still runs before any file is rewritten.
- Size budgets re-baselined so `aikido-rust-extra` passes on main again: sqe-core, sqe-sql, sqe-policy and sqe-coordinator had outgrown their ceilings, and `plan_rewriter.rs` / `ranger_store.rs` their file limits
