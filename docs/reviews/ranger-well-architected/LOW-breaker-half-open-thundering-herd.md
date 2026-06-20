# Breaker half-open admits all concurrent probes (thundering herd)

- **ID:** breaker-half-open-thundering-herd
- **Pillar:** Reliability
- **Severity:** Low
- **Status:** Open
- **Files:** crates/sqe-policy/src/policy_breaker.rs:68

## Problem
In HALF_OPEN, `check()` returns `Ok` for all concurrent callers, not just the CAS winner, so many probes go in flight at once. This is harmless (no fail-open) but noisy.

## Proposed fix
Gate the half-open `Ok` to the single CAS winner.

## Acceptance criteria
Only one probe in flight in half-open.
