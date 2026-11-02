**STATUS 2026-11-02: sqe-catalog builds again without the `rest` feature (`fix/catalog-no-rest-build`).**

`writable_iceberg_catalog` (from `feat/embedded-create-view`) imports
`rest_catalog`, `schema_provider` and `table_provider`, which only exist with
`rest`, so CI's `cargo check -p sqe-catalog --no-default-features` on main
failed to compile. MR pipelines run the minimal profile and skip that matrix,
so it showed only after the merge. The module is now gated on `rest`; its only
users, sqe-cli and the coordinator, always build with it.

The same build also warned about two items with no caller in that feature
set: `note_not_iceberg` (only the REST schema provider calls it) and the
`build_jdbc_sqlx` stub (only reached from the `sql-sqlite-rs` path). Both are
now gated or allowed to match their callers.

**NEXT:** unchanged
