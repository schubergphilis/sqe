**STATUS 2026-09-07 (night): coordinator compile 68 s -> 18 s by boxing four async hubs, on `perf/compile-time-async-hubs`; the crate split is demoted to a structural item.**

The crate-split plan rested on a warm-incremental measurement. Repeated
with `CARGO_INCREMENTAL=0` (what CI runs): the `touch sqe-core` chain is
173 s on main and 177 s with the write path moved into `crates/sqe-write`
(`refactor/extract-sqe-write`, gate green, 187 + 767 = 954 tests, held
unpushed), because the new crate compiles in 2.8 s and the coordinator
library stays at 68 s. Lines were never the cost. With the boxes the chain is 131 s. `scripts/compile-profile.sh`
(stable toolchain, `RUSTC_BOOTSTRAP=1` + `-Zself-profile`, measureme
`summarize`/`crox`, `scripts/compile-profile-attribute.py`) shows 58 of the
68 s are `evaluate_obligation`: `Send`/`Sync` proofs over async state
machines, rooted in the 21 Flight SQL `async_trait` methods (each re-walks
DataFusion's `LogicalPlan` type graph through the prepared-statement cache,
1.3 s apiece), the Quack and MCP executors awaiting the 2,000-line
`QueryHandler::execute`, and the session-cache invalidation. Four
`BoxFuture`s at the definitions (`execute`, `execute_stream_with_id`,
`get_session_from_request`, `invalidate_session_cache`) plus a type-erased
`StoredPlan` in `PreparedStatement` take the library to 18 s with no
behaviour change. Analysis: `docs/internal/plans/2026-09-07-compile-time-async-hubs.md`.
Also today: MR A of the split (!959, write-path decoupling) is up; its red
pipeline was a latent classifier bug (bare `contains("401")` matched a
wiremock port), fixed in !960 and merged into the branch.
Later the same night: the platform side of the 7.5-minute install prelude
landed as aikido v0.12.2 (aikido !161): a prebaked `aikido-rust` image with
the toolchain, clang/mold/protobuf and the supply-chain tools, pinned by
digest as `AIKIDO_RUST_IMAGE`; `ci/aikido-rust-image` moves this repo to
that release and runs `release-binaries` in the same image.
**NEXT:** merge !960, !959 (harmless, and it carries the split plan doc),
and this branch; then the same profile-and-box pass on `sqe-catalog` (43
of its 47 s, `hive::provider` 17 s), which sits on the critical path
behind `sqe-policy`; then decide on `sqe-write` (MR B) as a #636 structural
change; then the review leftovers.
