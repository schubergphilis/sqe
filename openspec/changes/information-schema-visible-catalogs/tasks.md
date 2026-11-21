## 1. Verify before building

- [ ] 1.1 D2 proof. Use a second catalog the persona has no grant on. Record `getConfig` and `listNamespaces` status under the Ranger authorizer and under Polaris internal RBAC. If either returns 200 with an empty list, stop and revise D2.
- [ ] 1.2 Record the management-list status per persona on both authorizers, and record that the 200 body is unfiltered.
- [ ] 1.3 List the repo's readers of qualified `cat.information_schema` (drivers, dbt-sqe, MCP, the Trino door, tests) that D4 would change.

## 2. Narrowed answering provider

- [ ] 2.1 Spike: choose between D4 option (a), a per-statement rewrite, and option (b), a per-catalog mount. Prove that dbt-sqe's `get_catalog`, `get_columns_in_relation` and `list_schemas` return the same shapes.
- [ ] 2.2 `sqe_sql::pinned_catalog` (qualifier; `table_catalog` / `catalog_name` equality), with unit tests.
- [ ] 2.3 Enumeration `SchemaProvider` wrapper: access denied becomes `Ok(None)`; every other error passes through. Unit tests for 403, 5xx and a conversion failure.

## 3. Visible catalogs

- [ ] 3.1 `SessionCatalog::list_catalogs_as_caller` (REST only; any status other than 200 means no source; reads `name` only).
- [ ] 3.2 `visible_catalog_candidates` (D1 order, scope filter) and `visible_catalogs` (D2 probe through `discover_catalog_provider`, D5 cap, D7 caches).
- [ ] 3.3 The pre-plan hook registers the visible catalogs, or only the pinned one, for enumerating statements; the registrations land in the session cache.
- [ ] 3.4 `handle_show_catalogs` and `system_catalog_entries` read `visible_catalogs`.
- [ ] 3.5 Config `[query] catalog_enumeration_max`, `catalog_list_ttl_secs`, plus `sqe.toml.example`; metric `sqe_catalog_enumeration_truncated_total`.

## 4. Tests and docs

- [ ] 4.1 Integration tests in `catalog_discovery_test`:
  - a scoped session lists both scope catalogs;
  - an unconfigured catalog appears through the management list;
  - a catalog without a grant is absent, from both rows and logs;
  - a denied management list falls back;
  - `A.information_schema` narrows;
  - the cap truncates;
  - one denied table is skipped while SELECT still reports access denied;
  - static mode makes no new calls.
- [ ] 4.2 A live dbt-sqe `docs generate` across two catalogs, asserting the catalog JSON's contents.
- [ ] 4.3 Book: a catalog-discovery page section on enumeration, the cap and the Ranger `catalog-list` policy. A changelog note on narrowing qualified `information_schema`.
