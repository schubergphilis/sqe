# Ranger config numerics never bounded; zero values cause fleet-wide deny-all

- **ID:** ranger-config-zero-validation
- **Pillar:** Operational Excellence
- **Severity:** Medium
- **Status:** Resolved in commit 722d1b2
- **Files:** crates/sqe-core/src/config.rs validate(); crates/sqe-policy/src/ranger_store.rs:139-159; crates/sqe-policy/src/policy_breaker.rs:86

## Problem
`validate()` never bounded the `policy.ranger` numerics. With `timeout_secs=0` the HTTP client gets a zero timeout, so every fetch fails and the engine denies all queries. With `breaker_failure_threshold=0` the breaker opens on the first failure (`count >= 0` is always true), causing permanent deny. A single typo silently breaks the whole fleet.

## Proposed fix
When `engine == Ranger`, reject `timeout_secs == 0` and `breaker_failure_threshold == 0` in `validate()`. Note `cache_ttl_secs == 0` is deliberately NOT rejected: always-fresh is a valid and safer choice.

## Acceptance criteria
A config with `engine=ranger` and `timeout_secs=0` fails `validate()` with an actionable message.
