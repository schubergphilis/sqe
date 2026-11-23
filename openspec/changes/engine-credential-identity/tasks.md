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

## 2. Engine identity names (#940)

- [x] 2.1 Discovery template drops `credentials`: a discovered warehouse reads with the person's delegated keys, never the engine's (fails closed, like the template's non-per-user `auth`)
- [x] 2.2 `SqeConfig::engine_read_catalog_names`: the policy store default-denies `select` under each engine catalog's config key and its `warehouse` alias

## 3. Engine secret rotation (#937)

- [x] 3.1 `client_secret_file` on `client_credentials` blocks; exactly-one-source and readable, non-empty file checked at startup for `auth` and `credentials` on every catalog (D10)
- [x] 3.2 `per_catalog` reads the secret on every resolve; cache key carries the secret fingerprint; rotation reported once per identity and the old entry dropped (D10)
- [x] 3.3 Engine wiring audits a rotation as `AuditKind::Auth` `engine_secret_rotated` (D10)
- [x] 3.4 wiremock tests (rotated file sent on the next resolve, unreadable file errors, inline secret one token request); IT on the test stack rotates a principal's secret at Polaris, rewrites the file and keeps reading without a restart
- [x] 3.5 Docs: catalog operations page, configuration reference, getting-started example
