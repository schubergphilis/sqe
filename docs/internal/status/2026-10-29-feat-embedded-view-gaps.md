**STATUS 2026-10-29: embedded Iceberg view gaps (`feat/embedded-view-gaps`).**

Closes the limits `feat/embedded-create-view` (!1593) left:

- Listing: `ViewSource` keeps a per-namespace view list
  (`refresh_views` / `refresh_all_views`, one `list_views` GET per
  namespace at attach, and again for the namespace after each embedded
  `CREATE VIEW` / `DROP VIEW`). `BaseTablesOnly::table_names`,
  `table_type` and `table_exist` consult it, so `SHOW TABLES` and
  `information_schema.tables` show the views as `VIEW`. A server without
  view endpoints or a missing namespace lists none. A view created by
  another client after attach loads by name but is not listed until the
  next attach.
- `SHOW CREATE VIEW`: renders `CREATE VIEW <name> AS\n<body>`, with the
  body's warehouse name mapped back to the attach name. `SHOW CREATE TABLE`
  on a view and `SHOW CREATE VIEW` on a table fail with a message naming
  the kind (`obj_type` tells them apart; the classifier maps both to
  `ShowCreateTable`).
- Body portability: `sqe_sql::rename_catalog` rewrites 3-part relations
  and 4-part column references from the attach name to the warehouse
  before `create_view`. The coordinator's `plan_view_inner` registers the
  planning catalog under the warehouse, so the stored body resolves there.
- Recursion guard: `plan_view_guarded` (sqe-catalog `schema_provider.rs`)
  keeps a task-local stack of `warehouse.namespace.view` keys, used by the
  coordinator's `plan_view` and the embedded `ViewSource::view`. A cycle
  fails with `references itself through a -> b -> a`; depth > 32 fails too.
- Backend catalogs writable (`--catalog-backend glue|s3tables`): split out
  as `fix/embedded-backend-read-only` (!1594).

Verified live against the test-stack Polaris (attach `pol`, warehouse
`test_warehouse`): SHOW TABLES and information_schema list two views,
a view over a view reads, SHOW CREATE VIEW renders the body under `pol`,
the stored representation reads `test_warehouse.test_ns.gap_t`, both
wrong-kind SHOW CREATE errors, and a self-referencing OR REPLACE fails on
read through the outer view. DROP removes them from SHOW TABLES. Unit
tests: wiremock lifecycle in sqe-cli, guard chain in sqe-catalog,
`rename_catalog` in sqe-sql.

Dropped: the double `/v1/config` fetch per REST attach. The attached
catalog (`mount::build_iceberg_rest`) and the view `SessionCatalog`
(`new_with_props`) are built from different property sets (the latter adds
the engine storage config's S3 location and fallback keys), and
`SessionCatalog` exposes no inner catalog to share. Unifying them changes
attach semantics for one GET per attach; not worth it.

**NEXT:** OAuth2 attaches still refuse views: the raw view calls need the
token the vendored client exchanged (and its refresh). Then coordinator
`SHOW CREATE VIEW`, which today renders the view's columns as a
`CREATE TABLE`.
