# 902: Result-cache hits bypass current row filters, masks and local policy denials

- Area: Result-cache authorization and correctness
- Priority: P1
- Order within area: 1
- Scope: security and large-data execution
- Severity: high
- Category: security
- Effort: M
- Status: fixed (branch `fix/security-scale-review-2026-09-05`, 2026-09-05)
- Confidence: high in source-level defect; deployment impact depends on stated prerequisites

## Problem

`crates/sqe-coordinator/src/query_handler.rs:1087` returns cached batches after `authorize_cached_tables`. That helper (`:4359`) only checks catalog table access, skipping some virtual resources; it never reapplies or compares the current policy decision. `crates/sqe-catalog/src/rest_catalog.rs:1613` returns success immediately for non-REST backends and uses a table HEAD/load for REST.

`CachedResult` stores batches and table names, not a policy version or effective row/mask decision (`crates/sqe-coordinator/src/query_cache.rs:16`). Cache defaults are enabled with a 300-second TTL (`crates/sqe-core/src/config.rs:527`).

## Why it matters

After a row-filter restriction or new column mask, a user who retains table access can receive previously cached unrestricted rows/values. On non-REST catalogs, even a new in-engine deny is not revalidated by this helper. A role/group change with the same username has the same problem. Small aggregates of very large tables remain eligible for the 5 MiB entry cap, so this is not limited to small source tables.

Applies to the materializing `execute()` cache path; streaming paths that bypass this cache are not asserted vulnerable.

## Proposed change

Before reuse, validate the full effective policy and bind entries to an authorization fingerprint/version plus identity and session context. If policy equivalence cannot be established, bypass the result cache for governed queries. Table existence/access alone cannot validate previously filtered or masked output.

## Validation and acceptance criteria

Warm the cache, then independently add a mask, tighten a row filter, change groups, and deny SELECT while keeping username/SQL unchanged. Each next execution must enforce the new policy. Test REST table access remaining allowed and a non-REST backend. Evidence here is code-path analysis; no live policy server was used.

## Related

Related to 206 and 303, but a separate cache and remediation. Fixing Ranger or metadata-cache freshness alone leaves already-materialized results reusable.

## Resolution (2026-09-05)

`PolicyEnforcer::permits_result_cache()` (default `false`; `PassthroughEnforcer` returns `true`) gates the materialized result cache in `QueryHandler::new` (`crates/sqe-coordinator/src/query_handler.rs`): with any governing enforcer (Ranger, OPA, Cedar, in-memory, plan rewriter) the cache is disabled, so a policy change can never be served from previously materialized rows. This is the conservative remediation the finding allows; a fingerprint-bound cache that re-enables reuse for governed queries is follow-up work. `rewriter_integration::subqueries_enforce_row_filters_and_masks` asserts the rewriter does not permit result reuse. Latency impact on governed deployments is not measured.
