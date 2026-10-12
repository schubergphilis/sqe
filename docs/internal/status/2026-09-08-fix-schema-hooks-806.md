**STATUS 2026-09-08 (evening): finding 806 (#638) closes with `fix/schema-hooks-806`: the last three `block_on_compat` sites outside the unwired `read_delta.rs` are gone.**

`SqeCatalogProvider::schema()` re-listed the catalog from inside the
synchronous hook when a namespace was missing from its snapshot (#368). It
now answers from the snapshot only; the miss is resolved on the async path
by `refresh_namespace`, which existed since the first 806 MR but had no
caller: `QueryHandler::refresh_listing_snapshots` now calls it for every
schema a statement names (session schema, `db.t`, `cat.db.t`) on whichever
registered catalog is an `SqeCatalogProvider`, cooldown-gated by the
extracted `relist_due`. `WritableIcebergCatalog`'s `register_schema` /
`deregister_schema` (DataFusion's own CREATE/DROP SCHEMA executor) used to
bridge onto `iceberg::Catalog`; they now refuse with an error naming the
async methods `create_namespace` / `drop_namespace`, which the embedded
CLI's DDL interceptor calls for `CREATE SCHEMA cat.ns` / `DROP SCHEMA
cat.ns` on a `--catalog` warehouse or an `ATTACH`ed Iceberg catalog (the
coordinator already went through `catalog_ops`). The interceptor resolves
the catalog through the client's own `IcebergCatalogMap`, now holding the
`WritableIcebergCatalog` handles: with URL tables enabled DataFusion hands
back a `DynamicFileCatalogProvider` wrapper, so a downcast of
`ctx.catalog(..)` cannot recover the provider. Gates: catalog unit 627,
sqe-cli 109, `it` 150, runtime_catalog 7, five namespace-resolution cases
against the live stack, clippy clean.
**NEXT:** 205/212 (#653), 610 (#657), 627, #659 partials; the
docker-capable CI runner for the `polaris` group (807 item 3).
