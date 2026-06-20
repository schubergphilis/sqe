# Group-bound policy items emit a WARN burst per cache miss, no counter

- **ID:** group-bound-warn-spam-no-metric
- **Pillar:** Operational Excellence
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/ranger_store.rs:247-262,346,372,467,513

## Problem
Group-bound policy items (not enforced by design, since SQE uses token-roles only) emit a WARN inside the per-item resolution loop. A bundle with many group-bound items emits a WARN burst on every cache miss for every user, with no counter.

## Proposed fix
Rate-limit or dedupe the warning (once per policy id). Add `policy_group_bound_items_skipped_total`.

## Acceptance criteria
Bounded WARNs plus a counter.
