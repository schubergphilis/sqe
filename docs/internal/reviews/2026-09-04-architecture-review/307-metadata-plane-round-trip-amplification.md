# 307: Metadata-plane round trips multiply per query: per-lookup providers, HEAD on every cache hit, N+1 in information_schema.columns, full catalog rebuild per view

- Scope: sqe-catalog (catalog_provider.rs, schema_provider.rs, rest_catalog.rs, info_schema.rs, table_provider.rs)
- Severity: medium
- Category: performance
- Effort: M (days)
- Status: open

## Problem

Several independent decisions each add a Polaris round trip per table per query; together they dominate planning latency for dbt-style workloads.

1. **Fresh cache hit still costs a HEAD.** `crates/sqe-catalog/src/rest_catalog.rs:1432-1445`: `load_table` on a fresh entry calls `authorize_table_access`, a raw `HEAD` to Polaris, before returning the cached `Table`. The metadata cache therefore saves the body download only, never the round trip. A 12-table query does 12 HEADs at planning.
2. **Schema providers are rebuilt on every lookup, so their cache never warms.** `catalog_provider.rs:550-573`: `schema()` constructs a new `SqeSchemaProvider` with a new `moka::sync::Cache` each call. `schema_provider.rs:252-257` admits the 5s `table_names_cache` "only applies within a single provider instance's lifetime". Every `table_exist` on a new lookup re-lists tables and views (two REST calls, `schema_provider.rs:204-234`).
3. **information_schema.columns is O(tables) REST calls.** `info_schema.rs:179-201`: for every namespace, `list_tables`, then `load_table` for every table (each a GET on a miss or a HEAD on a hit per point 1). dbt-trino and JDBC drivers call `information_schema.columns` frequently; a 600-table catalog is 600+ round trips per call.
4. **A view load rebuilds a whole catalog provider.** `schema_provider.rs:345-355`: `plan_view` calls `SqeCatalogProvider::try_new`, which lists namespaces and probes each with `get_namespace` (`catalog_provider.rs:456-466`, up to `NAMESPACE_PROBE_CONCURRENCY = 8` in flight). It repeats this for every sibling warehouse the view text names (`schema_provider.rs:365-401`). Referencing one view in a query costs 1 + N namespace probes plus a nested `mini_ctx.sql()` plan.
5. **Puffin NDV cache is per provider instance.** `table_provider.rs:100`: `puffin_ndv_cache: Arc::new(PuffinNdvCache::default())` inside `try_new`, which runs per `table()` lookup (`schema_provider.rs:278`); "shared by clones" never happens across queries.

## Why it matters

Planning latency for small queries is bounded by Polaris RTT times the number of touched tables, not by SQE. The 2026-07-25 review's P1-6 measured the `/v1/config` rebuild at ~250ms; every HEAD is a comparable network hop. Under a Polaris slowdown these multiply into the timeouts described in 300. It also makes the metadata caches misleading: operators raise `table_cache_ttl_secs` and see no change in Polaris QPS.

## Proposed change

1. Cache the `HEAD` authorization result per `(token, table)` with its own short TTL (for example 10s, configurable `catalog.authorization_recheck_secs`), so a burst of lookups inside one query or one dbt step pays one HEAD. Document that Ranger revokes are observed within that window.
2. Cache `SqeSchemaProvider` instances per namespace inside `SqeCatalogProvider` (a `DashMap<String, Arc<SqeSchemaProvider>>`), so the table-names TTL becomes real; invalidate on DDL via the existing `invalidate_table_names`.
3. In `info_schema.rs`, build `columns` from cached `Table` metadata when fresh and fall back to one `load_table` per miss; add a `namespace` filter pushdown so `WHERE table_schema = 'x'` lists one namespace.
4. In `plan_view`, reuse the calling session's catalog provider (pass it in) instead of `try_new`, and register siblings lazily only when planning fails to resolve a reference.
5. Move `PuffinNdvCache` next to `TableMetadataCache` keyed by `(table uuid, snapshot id)`.

## Acceptance criteria

- A wiremock counting test: planning `SELECT` over 5 tables twice in the same session issues at most 5 HEADs total (not 10), and 0 `list_tables` calls on the second run.
- `information_schema.columns` on a 3-namespace, 30-table mock catalog issues 3 `list_tables` and at most 30 `load_table` on first call and 0 on the second within TTL.
- View planning issues 0 `list_namespaces` when the view references the same warehouse.

## Related

- 300, 306, 308. 2026-07-25 review P1-6.
- `docs/internal/plans/2026-05-29-polaris-catalog-discovery.md`.
