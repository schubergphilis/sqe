**STATUS 2026-11-08: cli_smoke expects embedded CTAS to keep partitioning (`test/embedded-ctas-partition-smoke-875`, part of #875).**

What changed:

- `crates/sqe-cli/tests/cli_smoke.rs`: `embedded_warehouse_ctas_refuses_partitioning_it_would_drop`
  became `embedded_warehouse_ctas_keeps_partitioning`. `feat/embedded-ctas-875` (2e3394e20)
  moved embedded CTAS onto the cluster's CTAS handler and dropped the PARTITIONED BY refusal,
  but this older test (dc5d460f6) still asserted it. That MR ran only
  `cargo test -p sqe-cli --bins ctas`, not the `cli_smoke` integration binary, so main went red
  in `aikido-test-rust` and every MR pipeline after it failed (first seen on !1688, job 5800459).

Tests:

- `cargo test -p sqe-cli --test cli_smoke embedded_warehouse` -> 12 passed.

Lesson: an embedded-mode change needs `cargo test -p sqe-cli --test cli_smoke`, not only `--bins`.

**NEXT:** #812 `system.runtime.query_history` (in progress), then rebase open MRs on main.
