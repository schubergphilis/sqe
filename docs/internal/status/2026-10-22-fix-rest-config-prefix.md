**STATUS 2026-10-22: raw REST calls use the server-advertised prefix (`fix/rest-config-prefix`, #873).**

Found while proving the quick-testing paths for #866: `quickstart/nessie`
reads worked but `INSERT INTO demo.events` failed with
`Catalog HTTP 400 (load_table): Authorization check failed for table`.
`SessionCatalog::rest_prefix` built `{catalog_url}/v1/{warehouse}`; Nessie
advertises `prefix = main%7Cwarehouse` in `/v1/config`, and the HEAD access
check under `/v1/warehouse/...` answers 400. Polaris advertises the
warehouse name, which is why nothing caught it.

- `vendor/iceberg-rust/crates/catalog/rest/src/catalog.rs`:
  `RestCatalog::path_prefix` (`SQE PATCH (sqe#873)`) returns the prefix from
  the client's merged config. Recorded in the vendor README patch table.
- `crates/sqe-catalog/src/rest_catalog.rs`: `rest_prefix` / `rest_url` and
  the table/view URL helpers are async and use that prefix verbatim (it is
  already percent-encoded, so it is parsed with the base, never re-encoded).
  No advertised prefix means no segment, as in the client. The warehouse
  name stands in when the config fetch fails or the breaker is open, so an
  open breaker still sends nothing.
- Tests: `rest_prefix_tests` (Nessie-style encoded prefix; no prefix), both
  assert the `/v1/warehouse/...` route gets zero requests.

**NEXT:** #866: run the Nessie, HMS and JDBC live tests in CI.
