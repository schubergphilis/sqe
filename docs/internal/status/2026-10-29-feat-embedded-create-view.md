**STATUS 2026-10-29: embedded CREATE VIEW in Iceberg catalogs (`feat/embedded-create-view`).**

`sqe-cli --embedded` turned `CREATE VIEW iceberg.s.v` into an empty Iceberg
table: DataFusion's `register_table` on the datafusion-iceberg schema
provider accepts a `ViewTable` and creates a table. Every `CREATE VIEW` /
`DROP VIEW` naming a `WritableIcebergCatalog` is now intercepted.

- REST attach (`TYPE iceberg_rest`, bearer `TOKEN`/`SECRET`): the attach
  also builds a `SessionCatalog` from the same props
  (`sqe_catalog::mount::rest_attach_props`, split out of
  `build_iceberg_rest`), and `CatalogOps::create_view` / `drop_view` write
  the view as on a coordinator. The schema is the body's analyzed schema;
  `arrow_schema_to_iceberg_json` moved from the coordinator to
  `sqe_write::view_schema`.
- Read path: `WritableIcebergCatalog::with_views` gives its `BaseTablesOnly`
  schema providers a `ViewSource`; a name that is not a table is loaded with
  `load_view_sql` and planned the way the coordinator's `plan_view_inner`
  does (`view_planning_context` / `view_table`, extracted from it), with the
  catalog registered under the attach name and the warehouse name.
- OAuth2 attach: views refused (raw view calls have no token).
- SQLite warehouse: no view support; CREATE refused, DROP IF EXISTS no-op.

Verified live against the `docker-compose.test.yml` Polaris (attach `pol`
over warehouse `test_warehouse`): create, read, DESCRIBE, duplicate CREATE
(409), OR REPLACE, DROP, DROP IF EXISTS, a view over an Iceberg table joined
back to it. Unit test with wiremock pins the request bodies; cli_smoke covers
the SQLite refusal.

Limits: `SHOW TABLES` / information_schema don't list views (`table_names`
is sync, no view listing snapshot); `SHOW CREATE TABLE` on a view says
TableNotFound; the body is stored verbatim, so `pol.` in it does not resolve
on a coordinator that names the catalog differently; a self-referencing
`CREATE OR REPLACE VIEW v AS SELECT * FROM v` recurses on read (no guard on
the coordinator either); each REST attach fetches `/v1/config` twice; the
`--catalog-backend glue|s3tables` read-only catalogs likely still turn
`CREATE VIEW` into DataFusion's `register_table` path.

**NEXT:** list Iceberg views in embedded `SHOW TABLES` / information_schema
and render them in `SHOW CREATE`; then rewrite the attach name to the
warehouse name in stored view bodies, and guard view self-reference on both
paths.
