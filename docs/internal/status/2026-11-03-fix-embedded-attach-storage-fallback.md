**STATUS 2026-11-03: embedded `iceberg_rest` ATTACH falls back to the CLI's storage settings (`fix/embedded-attach-storage-fallback`).**

Found while adding `sqe-embedded-polaris` to engine-bench: `sqe-cli --embedded`
with `SQE_STORAGE__S3_*` set still failed an ATTACH to Polaris on RustFS with
"region is missing", because `build_catalog` / `rest_attach_props` only read
the ATTACH's own `S3_*` options. `with_storage_fallback` in
`crates/sqe-cli/src/embedded.rs` fills them from the embedded `StorageConfig`
when the ATTACH names none (all or nothing, so keys of two stores never mix).
It applies to the catalog and to the view `SessionCatalog`. The vendored
`table_file_io_props` order keeps vended credentials on top. `mount.rs` maps the
new `S3_SESSION_TOKEN` option. Server ATTACH is untouched on purpose (no
service account). Checked live against the engine-bench Polaris: an ATTACH
with only `WAREHOUSE` and `SECRET` counts 10,000,000 rows of `bench_10m.sales`,
and the old error remains when no storage is configured.

**NEXT:** engine-bench drops the inline `S3_*` options from `polaris_attach()`
once this merges; then the 150M Polaris rerun with the local engines, and #358.
