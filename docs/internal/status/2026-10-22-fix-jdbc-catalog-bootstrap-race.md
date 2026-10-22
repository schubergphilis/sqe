**STATUS 2026-10-22: JDBC (Postgres) catalog bootstrap tolerates concurrent builds (`fix/jdbc-catalog-bootstrap-race`, #872).**

Found while proving the Nessie, HMS and JDBC live tests for #866. On an
empty Postgres database the three `sql_postgres::` tests failed 2 of 3 when
run in parallel ("operation failed for hitting sqlx error" while building
`SqlCatalog`); serially, or with the catalog tables present, they passed.
Cause: `SqlCatalog::new` runs `CREATE TABLE IF NOT EXISTS` on every build,
and Postgres can fail one of two concurrent ones for the same table (unique
violation on `pg_type`). SQE builds a JDBC catalog per session, so this
also hit the first concurrent sessions in production.

- `vendor/iceberg-rust/crates/catalog/sql/src/catalog.rs`:
  `create_table_if_missing` (`SQE PATCH (sqe#872)`) probes the table after
  a failed create and only returns the error when it is still missing.
  Recorded in the vendor README patch table.

Gate (local, `sqlengine-postgres-1`, tables dropped before every run):
- without the patch: 3 of 3 parallel runs failed (1 passed, 2 failed);
- with the patch: 5 of 5 parallel runs passed (3 passed);
- same run: `nessie::nessie_namespace_round_trip` and
  `hms::hms_namespace_round_trip` pass against `docker-compose.nessie.yml`
  and `docker-compose.hms.yml`.

**NEXT:** #866: run the Nessie, HMS and JDBC live tests in CI.
