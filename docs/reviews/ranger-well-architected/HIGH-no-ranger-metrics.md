# RangerStore emits zero metrics; breaker-open deny-all is invisible

- **ID:** no-ranger-metrics
- **Pillar:** Operational Excellence
- **Severity:** High
- **Status:** Open
- **Files:** crates/sqe-policy/src/ranger_store.rs (no metrics); contrast crates/sqe-policy/src/opa.rs:90-125; gauge exists crates/sqe-metrics/src/lib.rs:99,479-488

## Problem
`RangerStore` emits zero metrics. There is no `policy_resolve_duration_seconds{backend=ranger}`, no cache hit/miss counters, and `policy_circuit_breaker_state{backend=ranger}` is never set, even though the breaker exposes `state_code()`. When Ranger degrades, the breaker opens and every query fail-closes to deny-all with nothing to alert on. OPA already has the full surface; Ranger shipped without it.

## Proposed fix
Add `with_metrics(Arc<MetricsRegistry>)` mirroring OPA. Record duration plus breaker state in `fetch_bundle`, and cache hits/misses in `resolve` and `resolve_tags`, all labeled `backend=ranger`. Wire it in `policy_wiring.rs`.

## Acceptance criteria
After a Ranger fetch failure trips the breaker, the gauge reads open, and the duration metric has observations.
