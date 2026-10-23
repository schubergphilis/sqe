**STATUS 2026-10-23: audit flush acknowledges after rotation (`fix/audit-flush-ack-after-rotation`).**

CI job 5658157 (`aikido-test-rust` on !1518, a test-only MR) failed `audit::logger::tests::both_format_rotates_the_ocsf_file_at_the_same_generation`: OCSF generation 1 held 2 records, expected 1.

Cause: in the writer loop (`crates/sqe-metrics/src/audit/logger.rs`), a `Flush` drained by the `try_recv` loop sent its ack at once, but `maybe_rotate` only runs after the drain loop. The caller's next `log_event` was then drained into the same batch and written to the file before it rotated.

Fix: flush acks are collected and sent after rotation and any prune note; a drained `Flush` ends the drain loop.

Gate: `cargo test -p sqe-metrics --lib` 101 passed; `audit::logger::tests::` repeated 300 times, 0 failures (the origin/main writer failed 4 of 300 on the same loop).

**NEXT:** unchanged
