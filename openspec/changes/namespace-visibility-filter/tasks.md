## 1. Probe helper

- [x] 1.1 Add `SessionCatalog::namespace_visible(&self, ns: &NamespaceIdent) -> bool` (or equivalent free helper) in `crates/sqe-catalog`: calls `get_namespace` with the session bearer; `Ok(_)` → true; 403/Forbidden error → false; any other error → true (fail-open) with a `debug!` log
- [x] 1.2 Make the 403 detection robust to both iceberg-rust error shapes (typed catalog error and wrapped HTTP status) — unit test both
- [x] 1.3 Unit test: fail-open on timeout/5xx-shaped errors

## 2. Filter at provider build

- [x] 2.1 In `SqeCatalogProvider` construction (`crates/sqe-catalog/src/catalog_provider.rs` — where `cached_namespaces` is built), probe all listed namespaces concurrently (cap 8 in flight) and drop the denied ones
- [x] 2.2 Skip probing entirely when the backend is not REST (reuse the existing backend match from the single-identity warning) or when `namespace_visibility_filter` is false
- [x] 2.3 Keep `information_schema` appended after filtering, never probed
- [x] 2.4 Unit test with a mock catalog: mixed allow/deny probe results → only allowed names in `schema_names()`
- [x] 2.5 Verify `information_schema.schemata` and Flight SQL `GetDbSchemas` both derive from the filtered provider (no second unfiltered path); add a regression test if either has its own listing call

## 3. Config

- [x] 3.1 Add `namespace_visibility_filter: bool` (default `true`) to the catalog config structs + TOML deserialization; document in `sqe.toml.example`
- [ ] 3.2 Unit test: flag off → no probes issued (assert via mock call count)

## 4. Integration verification

- [ ] 4.1 Integration test against the OPA-backed Polaris compose stack: non-privileged user's `SHOW SCHEMAS` hides the ungranted namespace; privileged user sees it; SELECT/`SHOW TABLES` behavior unchanged
- [ ] 4.2 Run the data-platform visibility suite against a stack with this build (`data-platform/scripts/visibility/test_trino_visibility.py`): the `[WARN] namespace name 'limited' visible in SHOW SCHEMAS` line is gone, all checks PASS
- [ ] 4.3 Coordination check (data-platform repo): confirm OPA's `LOAD_NAMESPACE_METADATA` decision also allows callers whose only grant is table-level inside the namespace (matching the platform's `namespace_visible` rule); if not, extend the rego seed there and note the cross-repo dependency in the MR

## 5. Docs

- [x] 5.1 Document the behavior + flag in `docs/` (catalogs/security page) including the fail-open posture and single-identity backend exemption
- [x] 5.2 CHANGELOG entry
