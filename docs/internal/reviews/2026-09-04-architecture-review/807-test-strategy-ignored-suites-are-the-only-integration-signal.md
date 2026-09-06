# 807: The integration suites that protect the risky paths are `#[ignore]`d, undocumented, and have no CI runner

- Scope: workspace tests, scripts/, .gitlab-ci.yml, Makefile
- Severity: medium
- Category: testing
- Effort: M
- Status: open
- Issue: #639

## Problem

Test inventory (2026-09-04):

| Kind | Count |
|---|---|
| Unit tests in `src/` (`#[test]`, `#[tokio::test]`) | about 4,100 |
| Integration test files under `crates/*/tests/` | 120 files, about 54,000 lines |
| `#[ignore]` tests | 278 (252 in sqe-coordinator, 20 in sqe-catalog) |
| `#[ignore]` with no reason string | 166 |
| Property-based tests (`proptest!`/`quickcheck`) | 2 files |
| Snapshot tests (`insta`) | 0 |

`make test` runs `cargo test --workspace --exclude sqe-cli`
(`Makefile:221-224`), which skips every ignored test. The 252 ignored
coordinator tests are the write path (CTAS, INSERT, MERGE, DELETE, INSERT
OVERWRITE), distributed execution, Ranger and Polaris access control, Glue,
Unity and Postgres catalogs. CLAUDE.md states the integration suite is
"LOCAL ONLY, no CI equivalent (issue #387 removed the dind jobs)".

The reason strings, where present, are the only documentation of which
stack a test needs; 166 bare `#[ignore]` give the next engineer nothing:

```
166 #[ignore]
 34 #[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
 25 #[ignore = "needs docker-compose.test.yml + Polaris"]
 19 #[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; ..."]
```

The suites also encode environment as convention rather than as test
metadata: `RUST_MIN_STACK=33554432` (`Makefile:242`), `--features test-sqlite`,
`DISTRIBUTED=1`, and the `SQE_IT_*` / `SQE_TEST_*` env vars (about 40 distinct
names read from `crates/*/tests` and, in one case, from production code,
`crates/sqe-worker/src/compaction.rs:639-663`).

Coverage gaps by crate: sqe-lineage has 2 unit tests for 1,756 source lines
(its 15 integration files carry the load); sqe-quack-server has 5;
sqe-mcp has 17 for a 1,221-line OAuth resource server. The stack-overflow
guards (`prod_stack_4k..32k`) are release-only and skipped in the default
debug run (`scripts/integration-test.sh:58-60`).

## Why it matters

- The merge gate for the write path, distributed dispatch and access
  control is one engineer running `make test-integration` on a laptop.
  The memory file "Which CI jobs to trust" records that integration CI is
  currently zero signal.
- The JoinStrategyRule regression (5 queries wrong, hidden by an Err-arm
  fallback) reached main because the parity check that would have caught it
  was not part of any required job.
- A bare `#[ignore]` cannot be selected by group, so the suites are run
  all-or-nothing with `--ignored`, which is why the run needs a 32 MiB stack
  and 10+ minutes.

## Proposed change

1. Replace bare `#[ignore]` with a reason in a fixed vocabulary
   (`needs:polaris`, `needs:polaris+workers`, `needs:ranger`, `needs:aws`,
   `needs:postgres`, `needs:spark`). Add a `cargo xtask test-groups` that
   lists tests by group; enforce "no bare ignore" with a grep in CI.
2. Move the write-path and distributed suites to `cargo nextest` filters
   keyed by that vocabulary so `make test-integration GROUP=polaris` runs
   one stack's worth.
3. Bring back one mandatory CI job that runs the `needs:polaris` group
   against the compose stack on a non-dind runner (a shell runner with
   Docker, or a Kubernetes service container). Even 25 tests is more than
   zero signal.
4. Add property tests where the code is a pure transform with an oracle:
   SQL identifier case folding, the Trino type mapping in
   `sqe-trino-compat/src/types.rs`, `SqeErrorCode` classification (see
   800), the OCSF audit event serializer.
5. Delete the `SQE_IT_*` reads from production code.

## Acceptance criteria

- `grep -rn '#\[ignore\]' crates` returns 0 hits (every ignore has a reason).
- One CI job on the merge pipeline runs at least the `needs:polaris` group and is required.
- `cargo test --workspace` unit count does not drop.
- The build-time plan's test-binary consolidation and this grouping agree on binary layout.

## Related

- `docs/internal/plans/2026-09-04-build-test-time-program.md` (binary sprawl, out of scope here).
- July 2026 production readiness review, P1-10 (still holds).
- Memory: Local integration gate; Which CI jobs to trust; JoinStrategyRule SMJ regression.
- 808 (stack size).
- Overlaps 513 (write-path view) and 211, 313, 710 (per-crate gaps); this file is the workspace-level gating and grouping.
