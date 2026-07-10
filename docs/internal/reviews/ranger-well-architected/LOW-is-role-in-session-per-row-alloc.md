# is_role_in_session array branch allocates a String per row

- **ID:** is-role-in-session-per-row-alloc
- **Pillar:** Performance
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/session_udf.rs:185

## Problem
The array branch does `roles.contains(&r.to_string())`, a per-row String allocation plus a linear scan. This is mitigated by const-folding (an Immutable UDF with a literal arg folds on the coordinator), so the array path rarely runs at row scale.

## Proposed fix
Use `roles.iter().any(|x| x.as_str() == r)` to drop the per-row allocation.

## Acceptance criteria
No `to_string()` in the per-row closure. The existing test passes.
