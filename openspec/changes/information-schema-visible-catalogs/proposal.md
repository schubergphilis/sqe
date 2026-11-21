# information_schema lists the catalogs the caller may see

## Why

`information_schema` only lists catalogs that are already registered in the session:
- the catalogs in config;
- the session catalog (`X-Trino-Catalog` / Flight SQL catalog);
- catalogs an earlier statement in the session named in a 3-part name.

So with `catalog_discovery = "polaris-auto"`, a user's other Polaris catalogs never appear. dbt's catalog queries (`docs generate`, `get_columns_in_relation` on another database) and editor completion find nothing for them.

A session limited to a signed catalog scope (#908) shows the gap too. Its default catalog is discovered and registered. The scope's other catalogs are not, so `information_schema` does not list them even though `SHOW CATALOGS` does.

A downstream consumer asked for Unity Catalog's behaviour: every catalog the caller may see, with its schemata, tables and columns, read as the caller, cached and capped.

The 2026-05-29 discovery design (`docs/internal/specs/2026-05-29-polaris-catalog-discovery-design.md`) rejected listing every catalog at session start, for three reasons:
- it needs management-API permissions the query bearer usually lacks;
- it needs a separate client;
- every session would register every visible warehouse.

Three things have changed since then.
1. **The scope already names the catalogs.** A #908 scope is a signed list of catalogs, so a scoped session needs no Polaris listing at all.
2. **The management list is a policy decision.** It is no longer an admin-only call. Under the Ranger authorizer, Polaris decides `GET /api/management/v1/catalogs` by a Ranger `catalog-list` policy on the root. Measured on Polaris 1.9.0 with the access-control fixture, as non-admin personas:
   - five users get 200;
   - one gets 403;
   - the same 403 user still reads its catalog (`getConfig` 200, `listNamespaces` 200).

   A deployment can grant listing to its users, and SQE uses it only when it answers.
3. **The cost can be kept to the statements that need it.** Catalogs are registered only for statements that enumerate them, and only the catalog a statement pins. Nothing happens at session build.

## What Changes

- **One source of visible catalogs.** It feeds `information_schema`, `SHOW CATALOGS` and `system.jdbc` / `system.metadata`. The candidates come from four places:
  - configured catalogs, filtered by the scope;
  - the session catalog;
  - the scope's catalogs;
  - unscoped and `polaris-auto` only: the management catalog list, fetched as the caller and used only when it answers 200.

  A candidate counts as visible only once it has answered the caller's own bearer, through the existing lazy probe and its `listing_succeeded` rule. A failed probe leaves the catalog out, with no error and no name.
- **Only for enumerating statements.** A statement that reads `information_schema.*`, `SHOW CATALOGS` or `system.jdbc.*` registers the visible catalogs it needs before planning. Other statements and session build are unchanged.
- **Narrowing.**
  - `cat.information_schema.<view>` answers for `cat` only, as in Trino and Unity Catalog.
  - An unqualified read with a `table_catalog = '<lit>'` / `catalog_name = '<lit>'` predicate (dbt-sqe's macros) probes and walks only that catalog.
  - An unpinned read walks every visible catalog, up to the cap.
- **Cap and cache.**
  - `[query] catalog_enumeration_max` (default 32) caps how many candidates one enumeration probes. Candidates past the cap are left out, with a warning and a metric.
  - The candidate list and negative probe results are cached per token and scope digest for `[query] catalog_list_ttl_secs` (default 60).
- **Access-denied tables are skipped while enumerating.** During an enumerating read, a table whose load returns access denied is left out of the rows instead of failing the statement, as Trino filters `information_schema` by access. A 5xx, an open breaker or a schema SQE cannot convert still fails the statement, as #890 decided. A SELECT that names the table keeps its access-denied error.
- `static` discovery mode and sessions that never enumerate behave as before.

## Capabilities

### New Capabilities

- `catalog-enumeration`: which catalogs a session lists, how they are proven, narrowed, capped and cached.

### Modified Capabilities

None.

## Impact

- **`sqe-coordinator`**:
  - `session_context.rs`: a `visible_catalogs` source, plus registration of discovered catalogs from it;
  - `query_handler.rs`: the information_schema pre-plan hook (~4555) and the enumerating-statement detection;
  - `query_handler/show.rs`: `handle_show_catalogs` and `system_schema_names` read the shared source;
  - `system_catalog_entries` for `system.jdbc`.
- **`sqe-catalog`**:
  - a management-list call on `SessionCatalog` (REST only), as the caller;
  - an enumeration wrapper over `SqeSchemaProvider` that maps access denied to "no table".
- **`sqe-sql`**: pinned catalog extraction, next to `pinned_table_schema_from_sql`.
- **`sqe-core`**: config keys `catalog_enumeration_max` and `catalog_list_ttl_secs` under `[query]`.
- **Clients:**
  - A qualified `cat.information_schema` read that returned other catalogs' rows now returns only `cat`'s rows.
  - dbt-sqe's macros already filter on `table_catalog`, so they are unaffected.
