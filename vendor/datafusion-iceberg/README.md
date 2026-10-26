# datafusion-iceberg (SQE vendored copy)

apache/datafusion-iceberg @ `b12e9871` (2026-09-24), vendored unmodified apart from
the `iceberg` / `iceberg-catalog-rest` paths in `Cargo.toml`, which point at
`../iceberg-rust`, and one SQE patch. Only `crates/datafusion` is vendored.

SQE patch: `IcebergSchemaProvider::try_new` (`src/schema.rs`) skips a table that
fails to load, with a `tracing` warning, where upstream fails the whole catalog. A
Unity Catalog namespace lists its Delta tables too, and each answers 400 "not an
Iceberg compatible table". Carry the patch over on a re-vendor.

This is the DataFusion integration that apache moved out of iceberg-rust on
2026-09-17. It is not published to crates.io (the crates.io `datafusion-iceberg` is an
unrelated project).

SQE uses only `IcebergCatalogProvider`, for ATTACHed catalogs and the embedded CLI.
SQE's own scan (`sqe-catalog::iceberg_scan`) does not go through this crate, and SQE's
runtime-filter converter lives in `sqe-catalog::physical_to_predicate`.

The old vendor carried an SQE patch that pushed hash-join runtime filters into this
crate's `IcebergTableScan`. It was not carried over, so queries on ATTACHed catalogs
and in the embedded CLI no longer get runtime-filter row-group pruning. Results are
unaffected.
