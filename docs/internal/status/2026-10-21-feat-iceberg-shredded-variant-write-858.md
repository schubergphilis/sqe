**STATUS 2026-10-21: SQE writes shredded variants when `write.parquet.shred-variants = true` (`feat/iceberg-shredded-variant-write-858`, #858; stacked on `fix/iceberg-nested-unknown-859`).**

New vendored module `vendor/iceberg-rust/.../writer/file_writer/variant_shred.rs`
(SQE PATCH v3-shredded-variant). `ParquetWriterBuilder::with_shred_variants`
turns it on; `sqe-compaction/src/writer.rs` sets it from the table property,
so INSERT, CTAS and `rewrite_data_files` all shred (they share that writer).

- Each data file infers its shredding from its first batch: object fields
  holding one of int, double, string, boolean or date in every row (nested
  objects too, at most 64 fields), or the whole value when every row is one
  such type. Objects win over non-object rows: an array or scalar row next to
  object rows goes to the residual `value`.
- `shred_variant` casts leniently (an int field accepts a boolean), so rows
  that do not fit the plan are flagged and written unshredded: full `value`,
  `typed_value` NULL. No value changes type.
- Column sizes of `typed_value` leaves are added to the variant field's
  `column_sizes` entry (they have no field id of their own).
- Only top-level variant columns are shredded; reads still unshred.

Test `variant_spark_interop::spark_reads_an_sqe_shredded_variant_table`: two
INSERTs (objects, an extra key, an array row, NULL, a field of another type),
asserts `typed_value` in every file footer, Spark 4.1 + Iceberg 1.12 and SQE
return the same JSON and typed fields, then `rewrite_data_files` leaves one
shredded file with the same rows. Vendored unit tests in `variant_shred.rs`
(the vendored crate is not a workspace member:
`cargo test --manifest-path vendor/iceberg-rust/Cargo.toml -p iceberg --lib variant_shred`).

Gates: `make test-integration FILTER=shredded_variant_table` -> 2 passed, no
skips; `FILTER=preserves_variant_values` -> 1 passed. Vendored unit tests
(`CARGO_TARGET_DIR=<target>/vendor-iceberg`) -> 3 passed. `cargo clippy -p
sqe-compaction -p sqe-coordinator --all-targets -D warnings` clean;
`cargo xtask matrix-report --print-only` -> 149/162 (92.0%).
`variant_distributed::shredded_variant_column_scans_on_a_worker` needs the
worker rig (`make test-distributed`); not rerun here.

Matrix: `sqe:shredded-variant:v3` partial -> full; the "read-only" caveat is
gone, new caveat: nested variants are written unshredded.

Docs: `ddl.md`, `features/iceberg.md`, `reference/limitations.md`,
`development/roadmap.md`.

**NEXT:** after !1498 merges, retarget this MR to main. Then pruning and
typed access on shredded fields (push `json_get_*` / `variant_get` down to
`typed_value`), and shredding variants nested in structs.
