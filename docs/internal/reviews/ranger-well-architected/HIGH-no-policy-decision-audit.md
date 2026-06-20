# No policy-decision field in audit; deny-all looks like an empty result

- **ID:** no-policy-decision-audit
- **Pillar:** Operational Excellence
- **Severity:** High
- **Status:** Open
- **Files:** crates/sqe-metrics/src/audit.rs:164-181; deny sites crates/sqe-policy/src/plan_rewriter.rs:118-120,201-204; crates/sqe-policy/src/ranger_store.rs:602-606

## Problem
`AuditEntry` has no policy-decision field. A row-filtered, masked, restricted, or denied query logs `status:"success"` with no record that a policy fired or what it did. A breaker-trip deny-all (zero rows) is indistinguishable from a legitimate empty result. Operators cannot answer "was user X's access to table T filtered, masked, or denied, and by which policy."

## Proposed fix
Add optional `AuditEntry` fields (`row_filters_applied`, `columns_masked`, `columns_restricted`, `policy_denied`) populated from `ResolvedPolicy` after evaluate. At minimum, emit a structured WARN (with no expression bodies) on every deny-all injection.

## Acceptance criteria
A masked or filtered query's audit entry has populated policy fields. A breaker-trip deny records `policy_denied=true`.
