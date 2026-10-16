**STATUS 2026-10-16: scan memory wait test made deterministic (`test/scan-memory-wait-paused-clock`).**

`scan_memory::tests::a_later_denial_keeps_the_budget_an_earlier_wait_left` (added with #788) failed in `aikido-test-rust` on !1445, which does not touch `sqe-catalog`: "waited out of the remaining budget". The gate counts slept time against a 300 ms budget. The test expects about 50 ms spent by the first admission, leaving room for the second's 100 ms wait, but on a runner under load (that pipeline had just run a runner out of disk) the real sleeps overshoot. The gate measures with `tokio::time`, so the test now runs with `start_paused = true` (`test-util` added to the `sqe-catalog` tokio dev-dependency, hakari unchanged): the retry naps are exact, 70 ms then 150 ms of the 300 ms budget.

Verified: `cargo test -p sqe-catalog --lib scan_memory` 18/18; `cargo hakari generate --diff` clean.

**NEXT:** the other wait tests in `scan_memory` assert `elapsed() >= wait` / `< wait` against `std::time::Instant`. They have not flaked, and their margins are wider, but they could move to the paused clock the same way if they do.
