# fix/iceberg-bloom-spec-keys-856 (2026-10-20)

Closes #856: SQE writers honour the Iceberg spec bloom keys.

## What changed

- `crates/sqe-compaction/src/parquet_writer_config.rs`: `build_writer_props`
  reads `write.parquet.bloom-filter-enabled.column.<col>` (`true` adds,
  `false` removes whatever chose the column) and
  `write.parquet.bloom-filter-fpp.column.<col>` (per-column fpp, validated to
  (0, 1) because parquet-rs panics outside it). SQE's own keys stay as they
  are; the column set is the union, and auto-detection runs only when neither
  names a column. Columns resolve through the Iceberg schema, so a struct
  member `s.x` gets Parquet path `s.x`; list/map members and non-primitive
  fields are skipped with a warning. `bloom-filter-max-bytes` is ignored
  (parquet-rs has no byte cap).
- 10 new unit tests (spec enable, per-column fpp, out-of-range fpp, fpp
  alone, `false` vs auto-detection and vs the SQE list, union, non-boolean
  value, struct/list paths).
- New integration test `bloom_spec_spark_interop` (V2 and V3): Spark creates
  the table with the spec keys, SQE inserts, the footer of SQE's data file
  has blooms on exactly `id` and `s.x`, and Spark reads the file.
  `variant_spark_interop::test_store` / `split_s3` are `pub(crate)` for it.
- Docs: `features/iceberg.md` (spec keys; stale `sqe-catalog` path fixed),
  `sql-reference/ddl.md` example. Stale comments in `sqe-write/src/writer.rs`
  (named a non-existent test file) and `v3_e2e.rs` fixed.
- Matrix: bloom-filters v2/v3 lose the spec-key caveat and cite the new
  test; the remaining caveat is max-bytes and list/map members. Score
  unchanged (148/162 with !1495; the cells were already full).

## Gates

- `cargo test -p sqe-compaction --lib parquet_writer_config`: 22 passed.
- `make test-integration FILTER=bloom_spec_spark_interop`: 2 passed (v2, v3),
  Spark container up, no skip.

## Open

- None for #856.

**NEXT:** identifier-field DDL (`ALTER TABLE t SET/DROP IDENTIFIER FIELDS`),
then switch the copy-on-write "MoR" tests in v3_e2e and the
`identifier_field_ids` property tests to it; then #858/#859 nested unknown;
then the DataFusion decimal unwrap-cast fix.
