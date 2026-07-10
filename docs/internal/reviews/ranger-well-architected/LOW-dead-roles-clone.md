# Dead roles clone in the rewriter

- **ID:** dead-roles-clone
- **Pillar:** Performance
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/plan_rewriter.rs:82

## Problem
`let _roles = user.roles.clone();` is never read. `user_clone` already carries the roles, so the clone is dead work.

## Proposed fix
Delete the line.

## Acceptance criteria
Compiles clean.
