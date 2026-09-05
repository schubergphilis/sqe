# 300: No HTTP timeout on any Polaris request path

- Scope: sqe-catalog (rest_catalog.rs, runtime_bridge.rs, vendored iceberg-catalog-rest)
- Severity: high
- Category: reliability
- Effort: S (hours)
- Status: open
- Issue: #566

## Problem

Every request the catalog crate sends to Polaris uses a `reqwest::Client` built with defaults, which has no connect or request timeout.

- `crates/sqe-catalog/src/rest_catalog.rs:24-25`: `static SHARED_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);`
- `crates/sqe-catalog/src/rest_catalog.rs:1256`: `let http_client = http_client.unwrap_or_else(|| SHARED_HTTP_CLIENT.clone());` and every production caller passes `None` (`for_session_with`, `rest_catalog.rs:937-947`).
- The vendored REST client does the same: `vendor/iceberg-rust/crates/catalog/rest/src/client.rs:107`: `client: cfg.client().unwrap_or_default(),`. SQE never calls `with_client`, so `list_namespaces`, `list_tables`, `load_table`, `update_table` all run without a deadline.
- `grep -n 'timeout' crates/sqe-catalog/src/rest_catalog.rs` finds only doc comments and the breaker recovery window; no request deadline exists.

The raw HTTP paths are on the hot path, not just DDL:

- `rest_catalog.rs:1432-1438`: every fresh cache hit in `load_table` first calls `authorize_table_access`, which is a raw `HEAD` (`rest_catalog.rs:1623-1635`). One stalled Polaris connection blocks planning for every table lookup.
- `rest_catalog.rs:1475-1485`: the ETag revalidation `GET`.
- `rest_catalog.rs:1841-1853, 1889-1900, 1960-1971, 2032-2043, 2113-2124`: view DDL and schema commits.

The circuit breaker cannot help: it only records a failure when a request returns an error (`rest_catalog.rs:1518-1526, 1632-1635`), and a hung request never returns. The sync bridge does not bound it either: on the multi-thread runtime `block_on_compat` deliberately has no deadline (`crates/sqe-catalog/src/runtime_bridge.rs:137-143`), so `SqeSchemaProvider::table_names` (`schema_provider.rs:204-207`) parks a tokio worker for as long as Polaris stays silent.

## Why it matters

A Polaris pod that accepts TCP but stops answering (GC pause, overloaded JVM, half-open connection through a load balancer) turns into an unbounded stall of every query that touches metadata, including cache hits. Queries pile up behind `authorize_table_access`, tokio workers are consumed by `block_in_place` in `table_names`, and the breaker never opens because nothing fails. The production-readiness review's P1-6 asked for token-rotation resilience against Polaris; this is the simpler precondition: bounded calls.

## Proposed change

1. Build `SHARED_HTTP_CLIENT` with `reqwest::Client::builder().connect_timeout(..).timeout(..)`, sourced from a new `[catalog] request_timeout_secs` (default 30) and `connect_timeout_secs` (default 5) on `CatalogConfig` in `sqe-core`.
2. Pass the same client to the vendored REST catalog through `RestCatalogBuilder::with_client` in `SessionCatalog::new` (`rest_catalog.rs:1243-1248`), so iceberg-rust calls share the deadline and the connection pool.
3. Treat a timeout as a breaker failure (it already is once the error surfaces) and classify it `CatalogUnavailable` via a typed variant rather than substring (see 305).
4. Give the multi-thread `block_on_compat` path an optional deadline, or document that the HTTP timeout is the bound it relies on.

## Acceptance criteria

- A wiremock test in `crates/sqe-catalog/tests/` that delays `/v1/config` or `HEAD .../tables/{t}` beyond the configured timeout asserts `load_table` returns `CatalogUnavailable` within timeout + 1s, and that the breaker records a failure.
- `SessionCatalog::new` is the only place a `reqwest::Client` is constructed for Polaris; `grep -rn 'Client::new\b' crates/sqe-catalog/src` returns nothing outside tests.
- `cargo clippy --all-targets --all-features -- -D warnings` is clean.

## Related

- 2026-07-25 production readiness review P1-6 (auth failure blast radius; still open in spirit).
- 301 (breaker keyed per token), 305 (error classification).
