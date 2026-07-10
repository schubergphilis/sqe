# I/O glue and fail-closed branches have no automated tests

- **ID:** untested-io-and-failclosed-branches
- **Pillar:** Operational Excellence
- **Severity:** Medium
- **Status:** Open
- **Files:** crates/sqe-policy/src/ranger_store.rs:166-196 (fetch_bundle breaker wiring),593-608 (resolve_tags deny),548-561 (resolve cache); crates/sqe-policy/src/grants/ranger.rs:258-282,394-421; crates/sqe-policy/src/plan_rewriter.rs:113-122,194-205 (deny injection); crates/sqe-coordinator/src/catalog_ops.rs:691-762,900-935 (commit path)

## Problem
The pure logic and the breaker state machine are well-tested, but the I/O glue and fail-closed branches have no automated tests. These are exactly the paths that decide whether a degraded Ranger denies correctly and whether a tag write commits.

## Proposed fix
Add wiremock / mock-HTTP tests for `fetch_bundle`, `post_grant_revoke`, and `fetch_policies` covering success plus each error branch, asserting breaker transitions. Add an integration test for the `set_column_tags` round-trip against an in-memory catalog. Add a plan test asserting deny-all on resolve failure.

## Acceptance criteria
Each listed path has a test exercising its error / fail-closed branch.
