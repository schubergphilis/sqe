**STATUS 2026-10-27: join-fallback rerun test made deterministic (`test/join-fallback-rerun-race`).**

CI job 5684639 (aikido-test-rust on !1560) failed `join_fallback::tests::a_query_refused_by_a_full_pool_reruns_once_there_is_room` at "the query ran again": 100 rows came back but no rerun happened. `run_beside_a_full_pool` dropped the hog on a 300 ms timer from the start; on a loaded runner the first attempt reserved after that, so the pool never refused it. Test-only race, unrelated to !1560.

- The hog now waits until the refused query holds the `RerunGate` (taken only in `wait_for_room`, after a refusal), then sleeps `release` and lets go. `a_rerun_gives_up_at_the_query_timeout` (no release) is unchanged.

Gates:
- `cargo test -p sqe-coordinator --lib join_fallback` -> 11 passed; the fixed test 5/5 in a row.
- `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --lib` -> 884 passed.
- `cargo clippy -p sqe-coordinator --all-targets -- -D warnings` -> clean.

**NEXT:** rerun the failed job on !1560 after this merges (or retry it now; the race is intermittent).
