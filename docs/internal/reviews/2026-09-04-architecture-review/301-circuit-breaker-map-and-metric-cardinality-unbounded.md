# 301: Per-token circuit breakers grow without bound and leak into Prometheus label cardinality

- Scope: sqe-catalog (rest_catalog.rs), sqe-metrics
- Severity: high
- Category: operability
- Effort: S (hours)
- Status: open
- Issue: #567

## Problem

Circuit breakers are keyed by bearer-token fingerprint and never removed.

- `crates/sqe-catalog/src/rest_catalog.rs:34-35`: `static USER_CIRCUIT_BREAKERS: LazyLock<DashMap<String, Arc<CircuitBreaker>>>`.
- `rest_catalog.rs:49-63`: `user_circuit_breaker` inserts `format!("{token_fingerprint}|{catalog}")` and there is no `remove` anywhere (`grep -rn USER_CIRCUIT_BREAKERS crates/` shows only get and entry).
- `rest_catalog.rs:54-55`: the breaker name is `format!("polaris-user-{token_fingerprint}-{catalog}")`.
- `rest_catalog.rs:1685-1696`: on every catalog call, `record_catalog_call` does `metrics.catalog_circuit_breaker_state.with_label_values(&[self.circuit_breaker.name()])`.
- `crates/sqe-metrics/src/lib.rs:591-596`: `sqe_catalog_circuit_breaker_state` is a `GaugeVec` with label `circuit`; nothing bounds the label set.

OIDC access tokens rotate (short lifetimes, refresh on every dbt run, per-login tokens), so the number of distinct fingerprints is proportional to logins over process lifetime, not to concurrent users. The `REST_CATALOG_CACHE` entries expire after 300s (`rest_catalog.rs:183-190`); the breaker map and the gauge series do not.

## Why it matters

Two unbounded growth paths in a long-lived coordinator. The `DashMap` is small per entry but permanent. The Prometheus gauge series is the worse one: each new token adds a time series with a 16-hex label, so `/metrics` output and the scrape payload grow with every login until the exporter is restarted, and Prometheus-side cardinality alerts fire on a label that carries no operator meaning (nobody can act on a token hash). This is the same class of leak the review found in session caches but on the metrics plane, where it costs money downstream.

## Proposed change

1. Key the breaker by `(catalog_url, warehouse)` plus the session username or `SessionUser.subject`, not the raw token fingerprint, so rotation of a token does not create a new breaker. If per-token isolation is really wanted, keep it but evict.
2. Replace the `DashMap` with a moka cache with `time_to_idle(300s)` matching `REST_CATALOG_CACHE`, or evict from `invalidate_rest_catalog_cache_for_token`.
3. Label the gauge by catalog only (`circuit = "polaris:<warehouse>"`) and export the per-user state as a log line or an aggregate count (`sqe_catalog_circuit_breakers_open{catalog}`). Prometheus labels must be low-cardinality by construction.

## Acceptance criteria

- Unit test: 10,000 distinct fingerprints against one catalog leave the breaker registry at or below the concurrent-user bound after idle eviction.
- `curl :9090/metrics | grep sqe_catalog_circuit_breaker_state | wc -l` equals the number of configured catalogs, not the number of logins, after a run of `make test-integration`.
- No label value on any `sqe_catalog_*` metric contains a token fingerprint.

## Related

- 2026-07-25 review P1-6 (per-identity invalidation; the fingerprint key came from that fix and this is its side effect).
- 300 (timeouts, which is what makes the breaker useful at all).
