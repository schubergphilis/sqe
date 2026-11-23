**STATUS 2026-11-23: Trino wire test fixed for the second session property (`fix/wire-show-session-write-branch`).**

`sqe-trino-compat` `tests/wire.rs::use_and_session_statements_echo_headers` failed on main (and on every MR pipeline's Rust test job) since `deda8f7d5` added `iceberg.write_branch` to the session allow-list: `SHOW SESSION` returns two rows, the test expected one. Main's own pipelines do not run that job, so main stayed green.

The test now asserts the two names in allow-list order, the replayed `ZSTD` value and an unset write branch; the golden `tests/fixtures/wire/session_headers.json` was regenerated with `UPDATE_GOLDENS=1` (one added row, `processedRows` 2).

Gates: `cargo test -p sqe-trino-compat` exit 0 (wire 15/15).

**NEXT:** rebase !1939 on this once merged so its Rust test job goes green.
