# 211: Policy and auth negative-case tests cover single scans and happy paths only

- Scope: sqe-policy tests, sqe-auth, sqe-mcp
- Severity: medium
- Category: testing
- Effort: M (days)
- Status: open

## Problem

The rewriter's end-to-end tests (`crates/sqe-policy/tests/rewriter_integration.rs`,
48 functions, and `view_bypass_policy.rs`, 14) all build a plan over one
`TableScan`. There is no case for a subquery, join, union, CTE, aggregate over
a masked column, ORDER BY on a masked column, or a masked column used in a
JOIN key. Issue 200 is a direct consequence.

`crates/sqe-auth` has no `tests/` directory (confirmed by `ls`); provider
behaviour is unit-tested per file, but chain-level negatives (expired token
plus fallthrough provider, JWKS outage mid-session, `AuthFailed` from provider
N after `NotMyCredentials` from N-1) are not exercised together.

`crates/sqe-mcp` has no `tests/` directory. Its inline tests use `MockAuth`,
which accepts everything; there is no test that a payload-only scope claim is
rejected when the provider did not verify the signature (203), nor that
`execute_sql_readonly` rejects `EXPLAIN INSERT ...`, `CALL`, `SET`, `ATTACH`,
or a Trino `$partitions` metadata-table rewrite.

No test asserts that DML and SELECT resolve the same policy key (201), or
that MERGE honours the target row filter (202).

## Why it matters

The three high findings in this review (200, 201, 202) are each a missing
negative test. The 2026-07-25 review's test matrix already lists
"row-filter bypass attempts, view/subquery/DML paths" as required and they
remain absent. Without them, the next refactor of the rewriter or the write
path can reopen any of these without a red build.

## Proposed change

Add a `crates/sqe-policy/tests/rewriter_shapes.rs` suite driven through
`SessionContext::sql` (so DataFusion produces the real plan shapes), with an
InMemory store, covering for each of row filter, mask and restriction:

- `IN (SELECT ...)`, `EXISTS`, scalar subquery, correlated subquery.
- INNER/LEFT JOIN with the governed table on each side; JOIN on a masked key.
- UNION ALL with one governed side.
- `WITH cte AS (...)` referencing the governed table twice.
- Aggregates (`count(distinct masked)`, `min(restricted)`).
- `EXPLAIN` output must not print raw filter literals from a policy.

Add `crates/sqe-auth/tests/chain_wiremock.rs` for chain-level negatives using
the existing wiremock dev-dependency, and `crates/sqe-mcp/tests/gate.rs` for
the read-only allow-list negatives and the unverified-scope rejection.

Add a coordinator integration test `policy_dml_parity` (see 201 and 202).

## Acceptance criteria

- The new suites exist and run under `cargo test -p sqe-policy -p sqe-auth
  -p sqe-mcp`.
- At least one test in `rewriter_shapes.rs` fails against the current
  `transform_down` implementation and passes after 200 is fixed (prove the
  test bites).
- `make test-integration FILTER=policy_dml_parity` passes after 201 and 202.

## Related

- 200, 201, 202, 203.
- docs/internal/reviews/2026-07-25-production-readiness-review.md, Security
  row of the qualification matrix.
