# 313: The REST cache state machine, statistics path and non-REST backends are untested outside ignored or docker-only tests

- Scope: sqe-catalog tests
- Severity: medium
- Category: testing
- Effort: M (days)
- Status: open
- Issue: #579

## Problem

The riskiest logic in the crate has no test that runs in `cargo test`:

- **Table metadata cache state machine.** `SessionCatalog::load_table` (`crates/sqe-catalog/src/rest_catalog.rs:1422-1599`) has four paths: fresh hit plus HEAD, stale plus ETag `304`, stale plus changed ETag, cold miss plus background HEAD. `grep -rln 'If-None-Match\|NOT_MODIFIED\|304' crates/sqe-catalog/tests crates/sqe-coordinator/tests` returns nothing. The only wiremock test on this code, `tests/authorization_revalidation_test.rs:43`, covers the revoke-on-HEAD case. Soft TTL expiry, `revalidate`, `update_etag`, `invalidate_table_all_tokens` through the bridge, and the races in 304 are all untested. The unit tests in `rest_catalog.rs:2276-2761` test pure helpers only.
- **Statistics and scan planning.** `SqeTableProvider::scan_impl` (`table_provider.rs:223-347`), `compute_table_statistics` (`iceberg_scan.rs:2439-2511`), `apply_requested_puffin_ndv` and the three manifest walks have no unit tests; they run only under `make test-integration`, which has no CI equivalent (CLAUDE.md: "LOCAL ONLY").
- **information_schema.** The unit tests at `info_schema.rs:531-570` construct an Arrow `Schema` inline and assert its own field count (`test_tables_schema`, `test_columns_schema`, `test_schemata_schema`); they never call `build_tables_table` or `build_columns_table`, so the N+1 and degrade-to-empty behaviour in 306 and 307 has no test.
- **Backends.** `tests/backends_integration.rs` has 13 tests, 12 `#[ignore]` (Glue, HMS, JDBC/Postgres x3, Hadoop/MinIO, Nessie, Glue REST, Unity x2). `tests/glue_source_live.rs` is 4 of 4 ignored, `tests/mount_jdbc_test.rs` 2 of 5. The `mount_*_test.rs` files exercise option parsing and error strings, not a catalog round trip. So `for_session_other_backend_with` (`rest_catalog.rs:971-1013`) and `build_backend_catalog` (`1029-1144`) have zero executed coverage in CI for any backend but REST.
- **Sync bridges under load.** `runtime_bridge.rs` has deadlock tests for the current-thread flavor, but nothing exercises `SqeSchemaProvider::table_names` or `HiveCatalogProvider::schema_names` concurrently on a small multi-thread runtime (`worker_threads = 2`), which is where `block_in_place` starvation would show.

## Why it matters

Issues 302, 303, 304, 306 and 307 in this review are all in code paths that a wiremock test could have pinned. Each fix will need to prove itself, and today there is nowhere to put that proof except a docker stack that runs on one laptop. The backends matrix means "supports Glue/HMS/S3 Tables" is a claim verified by hand, occasionally.

## Proposed change

1. Add `tests/table_cache_state_machine_test.rs` using wiremock (already a dev-dependency) and a `TableMetadataCache::new(1)` soft TTL, with `tokio::time::pause()` to step the clock: cover the four `load_table` paths, `304` revalidation, `invalidate_table_all_tokens`, and the insert-after-invalidate race from 304 using a `Mock` with a `set_delay`.
2. Build a `fake_iceberg_table()` fixture with a real manifest list and two manifests written through `iceberg::writer` into a `tempfile` FS `FileIO` (the `test_table()` fixture at `rest_catalog.rs:2560-2588` is the seed); use it to unit test `compute_table_statistics`, `collect_data_files_for_pruning`, and `scan_impl`'s statistics attachment.
3. Replace the tautological `info_schema.rs` tests with tests that drive `InformationSchemaProvider` against a wiremock Polaris and assert rows and call counts.
4. For backends, add an `iceberg::Catalog` trait mock (`Arc<dyn Catalog>`) so `for_session_other_backend_with` and the `CatalogHandle::Other` arms of `dispatch_catalog!` are exercised without Docker; keep the live tests ignored but run them in the nightly `make test-integration` and record the last green date in the file header.
5. Add a `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` test that calls `table_names` from 8 concurrent tasks against a slow mock and asserts completion.

## Acceptance criteria

- `cargo test -p sqe-catalog` runs at least one test per `load_table` branch, per `information_schema` table, and per `CatalogHandle` variant without Docker or credentials.
- `cargo llvm-cov -p sqe-catalog` (or `tarpaulin`) shows `rest_catalog.rs` line coverage above 70% and `info_schema.rs` above 60%; record the numbers in the MR.
- The ignored backend tests each carry a `last verified` date in their doc comment.

## Related

- 302, 303, 304, 306, 307. Memory notes "Local integration gate", "Which CI jobs to trust".
- Test-inventory review (this batch) for the cross-crate view.
