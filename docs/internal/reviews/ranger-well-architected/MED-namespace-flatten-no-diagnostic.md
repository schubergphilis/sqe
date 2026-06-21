# Namespace flattening silently fails to match Ranger policies, no diagnostic

- **ID:** namespace-flatten-no-diagnostic
- **Pillar:** Operational Excellence
- **Severity:** Medium
- **Status:** Open
- **Files:** crates/sqe-policy/src/plan_rewriter.rs:425-445; crates/sqe-policy/src/ranger_store.rs:222-225,206-214

## Problem
`resolve_policy_key` uses only the LAST dotted namespace component, and `resource_matches` is exact-or-`*` only (no glob). If a Ranger policy is authored against a database value that is not the last component (for example `database="ns1.ns2"` versus SQE's `"ns2"`), the policy silently does not fire and columns return raw. The log looks identical to "no policy intended."

This is the most likely real-world misconfiguration, and it ships with no diagnostic.

## Proposed fix
Emit a debug/trace line with the exact `(database, table)` lookup keys per scan. Document the flattening. Consider a warning when a table resolves zero policies but tags or grants exist.

## Acceptance criteria
A query against a multi-level namespace logs the exact lookup keys.
