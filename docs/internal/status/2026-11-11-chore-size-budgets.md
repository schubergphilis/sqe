**STATUS 2026-11-11: Size and structure budgets enforced on every merge request (`chore/size-budgets`, refs #636, #589, #576).**

The build-time and crate-size work since June had nothing holding its results. `sqe-coordinator` was 35K lines on 2026-06-12; today it is 66K, and 93K counting `sqe-write`, which was split out of it in September. A cleanup once removed a one-impl trait object that was acting as a compile firewall, and the cost was found later by profiling, not by CI. Nothing in CI looked at size, coupling or compile cost.

`cargo xtask budgets` now holds the part that can be checked without compiling. `.config/budgets.json` records:

- a line ceiling per crate (`src/` only), all 26 crates listed;
- a 3,000-line default per source file, plus a recorded ceiling for the 22 files already over it (`write_handler.rs` 15,691, `query_handler.rs` 13,172, `config/mod.rs` 12,146);
- the exact first-party dependencies of every crate (normal and build, not dev), so an edge that was cut on purpose, such as `sqe-catalog -> sqe-policy`, cannot come back unreviewed;
- `Cargo.lock`: 779 packages against a ceiling of 825, 42 names at more than one version against 50. Upper bounds only, so a dependency-update MR that removes packages stays green.

The tool only lowers: `--update` tightens ceilings and drops removed edges. Raising a ceiling, adding an edge or adding a crate is a hand edit of the file. Ceilings carry one to two steps of room (250 lines per step for a file, 1,000 for a crate) so an ordinary fix does not trip them; a line ceiling two steps above what `--update` would record fails, so a reduction has to be locked in. `--check` runs in `ci/aikido-rust-extra.sh` in both CI profiles (so on every MR), as a pre-push hook, and as `make check-budgets`. Without flags the command prints the per-crate table including rebuild fan-out (`sqe-core` 16, `sqe-metrics-core` 13, `sqe-sql` 10).

Limits: line counts include inline `#[cfg(test)]` modules. The budgets bound size and coupling, not compile time, which here is mostly auto-trait proofs over async state machines.

**NEXT:** two follow-ups complete the picture. (1) A compile-cost record on main: run `scripts/compile-profile.sh` for `sqe-core`, `sqe-catalog`, `sqe-policy` and `sqe-coordinator` on a schedule, keep the JSON, and alert on a regression; check first whether self-profile query counts are stable enough to gate on, since wall-clock on shared runners is not. (2) A release binary size ceiling in the `release-binaries` job. Then spend the budgets down: #589 (`query_handler.rs`), #636, #576.
