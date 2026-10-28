# datafusion-iceberg (SQE vendored copy)

apache/datafusion-iceberg @ `b12e9871` (2026-09-24), vendored unmodified apart from
the `iceberg` / `iceberg-catalog-rest` paths in `Cargo.toml`, which point at
`../iceberg-rust`, and four SQE patches. Only `crates/datafusion` is vendored.

SQE patch: `IcebergSchemaProvider::try_new` (`src/schema.rs`) skips a table that
fails to load, with a `tracing` warning, where upstream fails the whole catalog. A
Unity Catalog namespace lists its Delta tables too, and each answers 400 "not an
Iceberg compatible table". Carry the patch over on a re-vendor.

SQE patch: `IcebergCatalogProvider::schema_provider` (`src/catalog.rs`) builds the
provider of one namespace. The embedded CLI creates tables through SQE's write
handler, outside DataFusion, and reloads only that namespace afterwards. Carry it
over on a re-vendor.

SQE patch: `project_with_partition` (`src/physical_plan/project.rs`) checks the
input schema with `Schema::contains` instead of `!=`, so a non-nullable input
column writes into a nullable table column. Upstream refused every
`INSERT ... SELECT 1, ...` into a partitioned table.

SQE patch: `PartitionExpr` (`src/physical_plan/project.rs`) has one `Column`
child per top-level table column. Upstream declared none, so DataFusion's
projection merge evaluated it on the input of the projection below, a
zero-column batch, and the partition calculator panicked.

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
