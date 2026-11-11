**STATUS 2026-11-11: `sqe-policy` trait-proof time cut from 20.5 s to about 5 s (`refactor/policy-compile-hubs`).**

`scripts/compile-profile.sh sqe-policy` attributed 11.4 s of the crate's 13 s
of `evaluate_obligation` to four `#[async_trait]` methods:
`PolicyPlanRewriter::evaluate`, `PassthroughEnforcer::evaluate`,
`RangerStore::resolve` and `InMemoryPolicyStore::resolve`. Each is a coroutine
that owns a `LogicalPlan` or a `ResolvedPolicy` (masks and row filters, so
`Expr`), and each proved `Send` over that type graph separately, because an
`async_trait` method carries its own lifetime bounds and rustc does not share
auto-trait results across parameter environments.

The bodies moved, unchanged, into non-generic inherent functions that return
`BoxedFuture<'a, _>`. The two `evaluate` methods are written in
`async_trait`'s expanded form, since an `async fn` wrapper would still own the
`plan` argument. One walk of the graph remains (2.9 s, billed to
`InMemoryPolicyStore::resolve_policy`, the first function compiled that needs
it).

Measured before and after, alternating, on a loaded laptop:

| run | `evaluate_obligation` self | profiled crate total |
|---|---|---|
| before 1 | 20.53 s | 36.7 s |
| after 1 | 6.65 s | 19.1 s |
| before 2 | 20.47 s | 32.6 s |
| after 2 | 4.32 s | 12.0 s |

The number of `evaluate_obligation` calls barely moved (45,728 to 43,827).
The time is in how deep each proof walks, not in how many there are, so the
call count is the wrong measure for this kind of change. Timings from separate
sessions are not comparable either: the same baseline read 13.0 s an hour
earlier under lighter load. Compare `typeck_root` self time between two runs
before trusting a difference.

Verified: `cargo test -p sqe-policy` (443 pass), `cargo clippy -p sqe-policy
--all-targets -- -D warnings`, `cargo check-server`, and `make test-integration
FILTER=polic` (12 pass).

**NEXT:** the same treatment for `sqe-coordinator`, where the cost sits in
`FlightSqlService` handlers that own a `LogicalPlan` across an await
(`do_action_create_prepared_statement` first), then `sqe-catalog`
(`HiveTableProvider::scan` / `scan_with_args` / `table`,
`SessionTuning::create_physical_plan`, the `read_json_buffer` scan).
