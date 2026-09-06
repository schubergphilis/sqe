# 306: Metadata listing failures still degrade to empty results (P1-7 still holds)

- Scope: sqe-catalog (schema_provider.rs, info_schema.rs, hive/provider.rs, catalog_provider.rs)
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: fixed (branch `fix/errors-audit-observability`, 2026-09-05)
- Issue: #572

## Problem

The 2026-07-25 production-readiness review (P1-7) reported that `information_schema` and metadata paths turn outages into empty results. The Iceberg `table()` path was fixed to propagate non-404 errors (`crates/sqe-catalog/src/schema_provider.rs:307-311`), but the listing paths were not:

- `schema_provider.rs:208-221`: `table_names()` logs `error!` and returns `Vec::new()` on a `list_tables` failure, and again for `list_views` at `226-234`.
- `schema_provider.rs:241-264`: `table_exist()` is derived from `table_names()`, so during a Polaris outage every table "does not exist". `CREATE TABLE IF NOT EXISTS` proceeds to a create that then fails with a confusing conflict, and `DROP TABLE IF EXISTS` silently no-ops.
- `info_schema.rs:344-350`: a non-authorization `list_namespaces` failure returns `Ok(Vec::new())` with a `warn!`, so `information_schema.tables` and `.columns` return zero rows. The comment argues one dead catalog must not blind the others; that is true for a multi-catalog sweep but the single-catalog case gets the same empty answer.
- `info_schema.rs:195-201`: a `load_table` failure for one table drops that table from `information_schema.columns` with a `warn!`.
- `hive/provider.rs:1123-1133` and `1189-1201`: `schema_names()` and `table_names()` return empty on error.
- `catalog_provider.rs:62-69`: for static anchors, any error that is not one of five transient codes degrades the catalog to an empty namespace list. A deserialization bug, a TLS misconfiguration whose message lacks the magic words, or a new Polaris error shape all land here with a `warn!`.

The `SchemaProvider` trait forces `table_names()` to return `Vec<String>`, so the crate cannot return an error there, but the behaviour is not documented to callers, and the coordinator's `SHOW TABLES`, Flight SQL `GetTables`, and dbt's `list_relations` all consume it.

## Why it matters

For discovery clients "empty" and "unavailable" are materially different: dbt concludes models must be created and issues DDL; BI catalogs mark schemas as deleted; operators debugging an incident see healthy-looking empty listings instead of errors. The `error!` log is the only signal and it is not correlated to the query that returned the wrong answer.

## Proposed change

1. Record listing failures on the provider (`RwLock<Option<SqeError>>` set by `table_names()`), and have `table_exist()` return an error-aware answer: expose a `last_listing_error()` the coordinator checks before `IF EXISTS` decisions, or make `table_exist()` fall back to a direct `table_exists` probe when the cached list is poisoned.
2. In `info_schema.rs:344-350`, only degrade when `cached_namespaces` came from a multi-catalog sweep; for the single-catalog provider, propagate the error as `DataFusionError::External`.
3. Emit a per-query warning row or status through the coordinator's existing warnings channel when a metadata answer is partial, so clients see "partial" not "empty".
4. Tighten `should_degrade_to_empty_namespaces` to an allow-list of degradable codes (`SchemaNotFound`, `CatalogError` with 404) instead of a deny-list.

## Acceptance criteria

- Wiremock test: `list_tables` returns 503; `table_exist("t")` does not return `false`, and a subsequent `DROP TABLE IF EXISTS t` through the coordinator errors with `CatalogUnavailable`.
- Wiremock test: `information_schema.tables` on a single catalog whose `list_namespaces` returns 500 errors instead of returning zero rows.
- The static-anchor degrade path has a test for an unexpected error code proving it propagates.

## Related

- 2026-07-25 production readiness review P1-7 (still holds).
- 305 (classification decides what counts as transient).

## Resolution (2026-09-05)

`SqeSchemaProvider` records `listing_failed` on `list_tables` errors and `table_exist` then returns true so DROP/CREATE IF EXISTS hit the catalog instead of no-op. `should_degrade_to_empty_namespaces` is an allow-list of not-found codes; unexpected `CatalogError` propagates. Test updated in `catalog_provider.rs`.
