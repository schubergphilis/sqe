# 311: View resolution picks the last version and first SQL representation, and view REST calls are hand-rolled JSON outside the breaker and metrics

- Scope: sqe-catalog (rest_catalog.rs view methods), schema_provider.rs plan_view
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: open

## Problem

The Iceberg view spec identifies the active version by `current-version-id`; SQE ignores it:

- `crates/sqe-catalog/src/rest_catalog.rs:2000-2006`: `load_view_sql` takes `body["metadata"]["versions"].as_array().and_then(|v| v.last())` and then the first representation with `type == "sql"` regardless of `dialect`.
- `rest_catalog.rs:1821-1837`: `create_view` posts a hand-built document with `"version-id": 1`, `"schema-id": 0`, `"dialect": "sqe"`, `"default-namespace"`, no `default-catalog`, and no `replace` semantics; `CREATE OR REPLACE VIEW` in the coordinator is drop-then-create (`crates/sqe-coordinator/src/catalog_ops.rs:397-400`).
- The six raw REST helpers (`create_view` 1802-1875, `list_views` 1879-1935, `load_view_sql` 1941-2009, `drop_view` 2015-2065, `commit_schema_update` 2078-2148, plus the ETag `GET` at 1460-1527) each rebuild `ns_str` with the `\u{1F}` join, interpolate the table or view name into the URL path unescaped, and duplicate the 429/409/other error mapping. None of them call `circuit_breaker.check()` or `record_catalog_call`, unlike `list_namespaces` and `load_table` (`rest_catalog.rs:1314-1322`), so view-heavy workloads are invisible to `sqe_catalog_request_duration_seconds` and can hammer an open circuit.
- Failures in `load_view_sql` are swallowed to `debug!` and reported as "table not found" (`schema_provider.rs:315-326`).

## Why it matters

Views replaced by Spark or Trino append a new version and move `current-version-id`; `versions.last()` is the same version only if the writer appended in order and never trimmed history (the spec allows `version-log` trimming). A Trino-created view carries a `trino` dialect SQL representation first; SQE will plan Trino SQL as SQE SQL and either fail or, for dialect-sensitive functions, return different results. The unescaped names break for any identifier with `/`, `?` or `%`. Missing breaker and metric coverage means the operational story in 300 and 301 does not extend to views.

## Proposed change

1. Use `current-version-id` to select the version; prefer representation `dialect in {"sqe","trino","spark"}` in that order, and refuse with a clear error when none matches rather than planning foreign SQL silently.
2. Replace the hand-built JSON with the vendored `iceberg::spec::view_metadata` types (the fork already vendors `ViewMetadata`, `ViewVersion`, `SqlViewRepresentation`), and percent-encode path segments with `url::form_urlencoded` or `percent_encoding`.
3. Factor one `rest_request(op: CatalogOp, method, url, body)` helper that applies `circuit_breaker.check()`, the trace headers, `record_catalog_call`, and the typed status mapping from 305; route all six call sites through it.
4. Propagate non-404 `load_view_sql` errors from `schema_provider.rs:321-323` like the table path does at `307-311`.

## Acceptance criteria

- Unit test with a view document whose `current-version-id` points at the first of two versions asserts the first version's SQL is returned.
- Unit test: a view whose only representation has dialect `trino` returns a typed "unsupported dialect" error, not a planning failure.
- `grep -c 'join("\\u{1F}")' rest_catalog.rs` is 1.
- `sqe_catalog_request_duration_seconds{op="create_view"}` appears after `make test-integration FILTER=view`.

## Related

- 300, 301, 305, 307 (plan_view cost).
