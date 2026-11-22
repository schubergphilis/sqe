## 1. Engine credential identity (#934)

- [x] 1.1 `CatalogConfig.credentials` + startup validation (D2, D9); config docs and env table
- [x] 1.2 Read client without the delegation header; delegated client for `load_table_for_write` and `create_table`; route every write path through it; table cache split by purpose (D1, D3)
- [x] 1.3 `sqe.key-source = engine` marker; writer and DML storage probe refuse it (D1)
- [x] 1.4 `ReadGate` trait in `sqe-catalog`, required with the identity; coordinator impl on `resolve_effective_policy` + `policy_denies_all` with the rewriter's `TableScope` (D4)
- [x] 1.5 `RangerStore` default-deny `select` for identity catalogs only; startup requires `catalog_scoped = true` (D4, D9)
- [x] 1.6 `sqe.read-denied` marker: empty scan before `plan_files`; metadata tables, `SHOW STATS`, Puffin check it; marked tables not cached (D4)
- [x] 1.7 `/credentials` raw GET, longest-prefix match on the person's location, merge via `table_file_io_props` (vendor SQE PATCH, made `pub`) (D5, D6)
- [x] 1.8 `TableReloader` path (D3)
- [x] 1.9 `VendedKeys` redacting `Debug` + unit test (D7)
- [x] 1.10 Audit callback, `AuditKind::Auth` with person actor and engine `app_name` (D8)
- [x] 1.11 wiremock tests: no header with identity; no `/credentials` on 403/404 load, on denied select, without a gate; no-prefix-match and engine-403 fail closed; unset sends the header
- [x] 1.12 Ranger store unit tests: default deny for identity catalogs, other catalogs unchanged
- [x] 1.13 IT on the test stack with `InMemoryPolicyStore` (the Polaris stack runs passthrough; the Ranger stack does not vend): metadata-only person reads via a read-data engine; engine with write-data, reader refused each DML kind and CTAS; denied select gives 0 rows (count, fallible predicate) with no `/credentials` call
- [x] 1.14 Docs: catalog configuration page, vended credentials page (read-only engine principal, add `select` policies first)
