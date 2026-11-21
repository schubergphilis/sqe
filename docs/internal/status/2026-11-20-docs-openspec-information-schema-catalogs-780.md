**STATUS 2026-11-20: OpenSpec proposal for information_schema listing the caller's visible catalogs (`docs/openspec-information-schema-catalogs-780`, openspec `information-schema-visible-catalogs`).**

This branch adds the design and changes no code. Today `information_schema` lists only catalogs already registered in the session. Of a #908 catalog scope, only the default catalog is ever registered.

The proposal changes that in five ways.
- **Sources.** The candidate catalogs are the configured ones, the session catalog, the scope's catalogs and, in unscoped `polaris-auto` sessions, the Polaris management catalog list fetched as the caller. Each candidate is proven by the existing lazy probe.
- **When.** Catalogs are registered only for enumerating statements, never at session build.
- **Narrowing.** A read narrows to one catalog when it qualifies `information_schema` with it or filters on `table_catalog` / `catalog_name`.
- **Cap and cache.** Probes are capped, and results are cached per token and scope.
- **Denied tables.** An access-denied table is skipped while enumerating. Other errors still fail the read, as #890 decided.

The design rests on these measurements, all on Polaris 1.9.0 with the Ranger authorizer in the access-control fixture:
- `GET /api/management/v1/catalogs` returns 200 for five non-admin personas and 403 for one.
- That 403 persona still gets 200 from `getConfig` and `listNamespaces` on its catalog.
- The list is decided by the Ranger `catalog-list` policy on the root. Polaris `listCatalogs` does not filter its result by grants.

Open before any code is written: task 1.1. It checks that `listNamespaces` on a catalog without a grant is refused with 403 or 404, not answered with an empty 200.

**NEXT:** task 1.1 on both authorizers, then the 2.1 spike that chooses between a per-statement rewrite and a per-catalog mount.
