**STATUS 2026-11-11: `sqe-catalog` trait-proof time cut from 16-27 s to about 7 s (`refactor/catalog-compile-hubs`).**

`scripts/compile-profile.sh sqe-catalog` attributed most of the crate's
`evaluate_obligation` time to five items, each billed separately because
rustc does not share auto-trait results across parameter environments:

| item | before (two runs) | after |
|---|---|---|
| `HiveSchemaProvider::table` | 6.1 s / 3.3 s | under 0.1 s |
| `HiveTableProvider::scan_with_args` | 6.0 s / 3.6 s | under 0.1 s |
| `HiveTableProvider::scan` | 2.8 s / 3.6 s | under 0.1 s |
| `EngineQueryPlanner::create_physical_plan` | 3.0 s / 3.4 s | under 0.1 s |
| `read_json_buffer` `ReservedMemTable::scan` | 1.7 s / 1.6 s | under 0.1 s |

The `hive` module as a whole went from 10.6-15.1 s to 0.2 s.

What changed:

- `HiveSchemaProvider::table` is in `async_trait`'s expanded form returning
  `Box::pin(self.load_table(name))`; the body moved, unchanged, into an
  inherent `load_table`.
- `HiveTableProvider::scan` / `scan_with_args` computed a cloned
  `SessionState` (`without_footer_statistics`) inside the future, so the
  coroutine owned a `SessionState` across an await. It is now built before
  `Box::pin` and held as `SendErased<SessionState>`.
- `EngineQueryPlanner::create_physical_plan` is in expanded form. The decision
  of whether to tune the state (aggregate filters, variant read hints) moved
  into `EngineQueryPlanner::tuned_state`, which runs before the future; this
  is a restructure, not a pure move. `&LogicalPlan` joined the `KnownSend`
  set (with its line in `known_types_are_send_and_sync`).
- `ReservedMemTable.inner` is `Arc<dyn TableProvider>` rather than a concrete
  `MemTable`; the concrete type made typeck prove `Send + Sync` over `Expr`
  column defaults and sort orders (about 1.7 s).

Measured before and after, alternating (before is `main` in a seeded second
checkout), on a loaded laptop:

| run | `evaluate_obligation` self | `typeck_root` self | profiled crate total |
|---|---|---|---|
| before 1 | 27.30 s | 1.96 s | 58.1 s |
| after 1 | 6.89 s | 1.05 s | 55.2 s |
| before 2 | 23.42 s | 1.34 s | 68.8 s |
| after 2 | 7.66 s | 1.68 s | 40.3 s |
| before 3 | 16.49 s | 1.07 s | 46.9 s |
| after 3 | 7.23 s | 0.98 s | 36.8 s |

`evaluate_obligation` is now 15% of the profiled total (summarize's `% of
total time` column, which includes 17 s of profile string allocation; about
20% of the rustc total) and the largest single item is 0.6 s (`rest_catalog` `commit_transaction` / `rename_table`), so the
profile has no hub left worth boxing. The crate total is noisy (the profile
mode adds about 17 s of query-string allocation, and machine load moved the
baseline by 20 s across runs); trust `evaluate_obligation` self time with
`typeck_root` as the control. The control moved between runs (before 1.07-1.96 s,
after 0.98-1.68 s); pair 3 is the matched one, with `typeck_root` at 1.07 s
before and 0.98 s after.

Verified: `cargo check-server`, `cargo clippy -p sqe-catalog --all-targets
-- -D warnings`, `cargo test -p sqe-catalog --lib` (814 pass),
`cargo xtask budgets --check`, and `make test-integration FILTER=hive`
(4 `hive_external_e2e` tests pass). `cargo fmt -p sqe-catalog --check` is red
from drift that was already on `main`; none of it is in changed lines.

**NEXT:** re-profile `sqe-coordinator` (the policy and coordinator fragments
left about 8 s there) and look at the next crate by `evaluate_obligation`
self time; the remaining catalog items (`rest_catalog`, `info_schema`,
`schema_provider`) are each 0.5-0.6 s and not worth a change on their own.
