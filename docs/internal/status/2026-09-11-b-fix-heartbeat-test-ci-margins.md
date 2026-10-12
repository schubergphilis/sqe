**STATUS 2026-09-11 (b): worker heartbeat protocol tests get a 5 s wait bound (`fix/heartbeat-test-ci-margins`); the four-tests-at-once CI failure on !983 and !988 was the `2 * INTERVAL` first-hello bound.**

`crates/sqe-worker/tests/protocol/heartbeat_roundtrip.rs` waited at most
200 ms (two 100 ms intervals) for the first hello and 100 to 300 ms for
every later event. On a loaded aikido-test-rust runner all four rigs
start together inside `cargo test --workspace --all-features` and the
first hello, channel connect included, missed that bound twice (jobs
5333198, 5338280), failing all four tests in the same second; locally
5/5 green in 0.66 s. Every `wait_for` now uses one `BOUND` of 5 s.
`wait_for` returns the moment its probe holds, so the healthy-run time is
unchanged; only the failure mode moved from "slow runner" to "broken
worker". Same class as the streaming idle and hive TTL margins of !985.
The wider bound exposed a second, older race in `heartbeat_roundtrip`:
the stub records the hello before its reply reaches the worker, and the
test asserted the worker-side effects (`coordinator_accepted`, pinned
ticket key) on the spot; 1 in 3 local runs failed there. Both are now
polled with `wait_for`. 6/6 local runs of the protocol binary green.
