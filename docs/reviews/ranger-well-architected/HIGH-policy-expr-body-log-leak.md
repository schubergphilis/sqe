# WARN logs leak raw row-filter and mask template bodies

- **ID:** policy-expr-body-log-leak
- **Pillar:** Operational Excellence
- **Severity:** High
- **Status:** Resolved in commit 722d1b2
- **Files:** crates/sqe-policy/src/ranger_store.rs:393-397,536-541; crates/sqe-policy/src/plan_rewriter.rs:397-407

## Problem
WARN lines logged the raw row-filter `filterExpr` and CUSTOM mask `template` bodies. These embed sensitive literals (for example `region='EU'` or keyed values), so the log bypassed the SQL-07 `redact_pii` / `strip_sql_literals` machinery and wrote sensitive data straight to the log stream.

Related and left OPEN under the OE backlog: the two `debug!` plan dumps in `query_handler.rs:1751,1927` still serialize user predicate literals (see `no-policy-decision-audit`).

## Proposed fix
Drop the expression and template bodies from the WARN lines. Log only the policy id, column, and tag.

## Acceptance criteria
A literal-bearing policy produces no literal in captured WARN output.
