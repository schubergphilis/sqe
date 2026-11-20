# Design: information_schema lists the catalogs the caller may see

## Context

`with_information_schema(true)` makes DataFusion's built-in `InformationSchemaProvider` answer every `information_schema` reference, whether qualified or not.
- It walks every catalog registered in the session's `SessionContext` and ignores the qualifier.
- Its `columns` view calls `SchemaProvider::table()` for every listed table and stops at the first error.

Before an unpinned read, the pre-plan hook (`query_handler.rs`, ~4555) calls `refresh_all_namespaces()` on every registered catalog.

Catalogs reach the context in three ways:
- **Session build:** the configured catalogs, the session catalog and a scoped default.
- **The lazy probe:** `discover_catalog_provider`, run when a statement names an unknown 3-part qualifier.

`SHOW CATALOGS` builds its own list: configured, session, scope and Hive roots.

Registering more catalogs therefore multiplies the cost of every unpinned read. The design rests on three principles:
- register only when a statement enumerates;
- narrow to what the statement pins;
- skip what the caller may not read.

## Decisions

### D1. Candidate sources

`visible_catalog_candidates(session)` returns names in a fixed order, with duplicates removed:
1. configured catalogs the scope allows;
2. the session catalog;
3. the scope's catalogs, in scope order;
4. only in an unscoped `polaris-auto` session: the management list `GET /api/management/v1/catalogs`, fetched with the caller's bearer and sorted by name.

The management call is a source, not the mechanism:
- any status other than 200 (403 under a Ranger policy that denies `catalog-list`, 401, 5xx) drops that source silently;
- the rest of the list stands.

A 200 lists every catalog in the realm, unfiltered (Polaris `listCatalogs` does not filter by grants), so every name still goes through D2. The response also carries storage configuration. SQE reads only `name` and drops the rest unlogged.

Hive roots and attached catalogs stay as `SHOW CATALOGS` lists them today. They are already registered at session build.

*Rejected:*
- **Management list as the only source.** It is denied to some users (measured 403), and a scoped session must not widen past its scope.
- **A configured candidate list.** It is a second place to keep in step with Polaris, and the management list covers deployments that allow it.

### D2. Proof of visibility

A candidate becomes visible only through the existing `discover_catalog_provider`, run with the caller's bearer. It is registered only when `listing_succeeded()`, the same proof that `tvf_caller_for` uses for `tvf_prefixes`. A probe that fails for any reason (404, 403, 5xx, timeout) leaves the name out. The statement goes on without an error, and the name appears nowhere in its result.

*To verify before building (task 1.1):* under both the Ranger authorizer and Polaris internal RBAC, with a second catalog the persona has no grant on, find out what `getConfig` and `listNamespaces` return:
- 403 or 404 means D2 holds;
- 200 with an empty list would mean listing alone is not proof. Visibility would then need a grant-bearing call, which this design does not pick yet.

### D3. When catalogs are registered

Catalogs are registered only for enumerating statements:
- a reference to `information_schema.<view>`;
- `SHOW CATALOGS`;
- `system.jdbc.*` / `system.metadata.*`.

`SHOW SCHEMAS FROM cat` names its catalog and already reaches it through the lazy probe. Registration runs in the same pre-plan hook that refreshes namespaces today, before planning.

Catalogs found this way register into the per-token `SessionContext` cache, as lazily discovered catalogs do now, so later statements reuse them. Session build does not change.

### D4. Narrowing

`pinned_catalog(stmt)` in `sqe-sql`, next to `pinned_table_schema_from_sql`, returns the catalog an enumeration is limited to. It takes the first match:
1. the `information_schema` qualifier, as in `cat.information_schema.columns`;
2. a top-level conjunct `table_catalog = '<lit>'` / `catalog_name = '<lit>'` on an unqualified reference (dbt-sqe's `get_catalog`, `list_schemas` and `check_schema_exists`);
3. none.

With a pinned catalog:
- only that catalog is probed (the cap does not apply);
- only that catalog is refreshed;
- only that catalog is walked.

Unpinned, every visible catalog up to the cap is walked.

Walking one catalog needs an answering provider over a reduced catalog list, because the built-in provider walks the whole `SessionContext`. Task 2.1 is a spike that picks between two options:
- **(a) Per-statement rewrite.** The pre-plan step rewrites the information_schema reference to a statement-scoped table: DataFusion's `InformationSchemaProvider::new(list)` over a `CatalogProviderList` holding the narrowed, wrapped catalogs (D6).
- **(b) Per-catalog mount.** Turn off the built-in route and mount DataFusion's `InformationSchemaProvider` over a single-catalog list as each catalog's `information_schema` schema. An unqualified pinned read is then qualified with its pinned catalog. An unpinned unqualified read goes through (a).

Either way, the view and column shapes stay DataFusion's, because dbt-sqe parses them. SQE's own `sqe_catalog::info_schema` provider is not a drop-in (SQL type names, a different column set).

*Behaviour change:* a qualified `cat.information_schema` read returns only `cat`'s rows. This matches Trino and Unity Catalog. Before this change it returned the rows of every registered catalog.

### D5. Cap

`[query] catalog_enumeration_max` (default 32, 0 = unlimited) bounds how many candidates one unpinned enumeration probes, counted in D1 order. The candidates in the first two sources are configured or already registered, so they cost no probe and always count. Candidates past the cap are not probed and are left out. In that case SQE:
- logs a `warn` with the counts, but not the names;
- increments `sqe_catalog_enumeration_truncated_total`.

The cap does not bound namespaces or tables. Their cost is the existing refresh and load path.

Truncating silently fits a listing better than an error would, because an error would break dbt `docs generate` for a user who can see many catalogs. The warning and the metric make the truncation observable.

### D6. Access-denied tables while enumerating

During an enumerating read, catalogs are handed to the answering provider wrapped in an enumeration-only `SchemaProvider`:
- its `table()` maps an error classified `AccessDenied` (Polaris 403) to `Ok(None)`;
- every other error passes through unchanged.

This matches Trino, which lists only accessible tables in `information_schema`. It does not reverse #890:
- a 5xx, an open breaker or a schema SQE cannot convert still fails the read;
- `SqeSchemaProvider::table()` itself is unchanged, so a SELECT naming a denied table still reports access denied, never "table not found".

The wrapper exists only inside the answering provider of D4, so no shared provider changes its behaviour.

### D7. Cache

Two caches are keyed by `token_fingerprint | scope digest`, with a TTL of `[query] catalog_list_ttl_secs` (default 60):
- the management-list result;
- negative probe results, so that a catalog denied to the caller is not probed again on every statement.

Positive results live in the session's registered catalogs (D3). A scope change produces a new digest and therefore a new key.

### D8. One source for every listing surface

`handle_show_catalogs`, `system_catalog_entries` (`system.jdbc` / `system.metadata`) and information_schema all read `visible_catalogs(session)`, the proven subset of D1 under D5. `SHOW CATALOGS` keeps its order: configured first, with the default leading.

## Invariants

- A catalog is listed only after it has answered this caller's own bearer. No service account is used and no shared credential.
- A scoped session never lists a catalog outside its scope (#908).
- A failed probe produces no error and no name in any result or log line.
- `static` discovery mode is unchanged.
- A SELECT on a named table reports the same errors as before.
- A catalog registered through enumeration gets `tvf_prefixes` only under the `listing_succeeded` rule.

## Risks

- **Probe fan-out.** An unpinned read probes up to the cap the first time, then hits the D7 cache. Each probe is one `getConfig` plus one `listNamespaces`.
- **Clients that read qualified `information_schema` across catalogs.** None are known in the repo (dbt-sqe and the driver matrix go unqualified or through `system.jdbc`). The changelog states the change.
- **D2 proof.** If listing alone turns out not to prove access, D2 needs a stronger check before anything is built.
