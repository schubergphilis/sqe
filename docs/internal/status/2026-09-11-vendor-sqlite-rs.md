**STATUS 2026-09-11: `vendor/sqlite-rs` refreshed to facade tip `0ef830d` (`chore/vendor-sqlite-rs-0ef830d`); all four workarounds stay.**

Lab271's reply to the four defects: (1) and (2) are one bug, bare-`?`
indices assigned at codegen time (the `UPDATE` values were *swapped*, not
dropped), fixed on `fix/bare-param-numbering-text-order` (their ADR-0044);
our addendum on transactions became `Connection::transaction()` holding
the connection for the guard's lifetime, on `feat/013-api-facade`
(ADR-0045); (3) `rowid` = #708 and (4) `sqlite_master` = #707, open. The
two fixed branches have not met: the param fix has no `src/api.rs` and a
dry-run merge onto the facade tip conflicts in
`src/codegen/expr/value.rs`, so we vendor the facade tip only. What this
refresh brings is the transaction lock; `SharedConn`'s gate stays one
cycle on top of it. `replace_row`, the projected column and the
`table_names()` probe stay until one upstream commit carries facade +
param fix (then `engine_defect_update_binds_only_its_first_parameter`
fails and is the signal to delete `replace_row`). Snapshot byte-identical
to upstream `src` (minus `bin`) + `LICENSE`. Sample catalog files for
their consumer corpus (#711) rebuilt at
`~/Downloads/sqe-embedded-handoff-20260911.tgz` (sqlx-written,
sqlite-rs-adopted, sqlite-rs-created; `integrity_check` ok on each).
