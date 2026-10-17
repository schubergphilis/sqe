**STATUS 2026-10-16: Iceberg V3 `VARIANT` columns, unshredded (`feat/iceberg-variant`, #849).**

The vendored iceberg-rust already maps `Type::Variant` to the Arrow storage `Struct{metadata, value}` tagged `arrow.parquet.variant`; SQE had no SQL type for it. Now `VARIANT` parses in CREATE / ALTER ADD COLUMN, the table is created as V3, the write path (`apply_stamped_schema`) parses text as JSON with `parquet-variant-compute::json_to_variant`, and the encoders render a variant as JSON text (Trino type `json`, Flight `Utf8`). DataFusion drops the extension metadata through expressions, so detection falls back to the storage shape (`sqe_core::variant`), and the CTAS / CREATE schema is re-tagged before the arrow->iceberg conversion.

Verified: unit tests in sqe-core, sqe-sql, sqe-write, sqe-compaction, sqe-trino-protocol, sqe-trino-compat; `variant_e2e` (4 tests) on the local stack; manual probe through the Trino CLI and ADBC Flight (JSON text out, invalid JSON refused, V2 ADD COLUMN refused).

Not verified: Spark interop (the local Spark stack runs iceberg-spark-runtime 1.7.1, which predates variant), the distributed worker scan of a variant table, compaction of a variant table.

**NEXT:**
- MR2: JSON DDL type -> variant for new tables, closing #848 for them; existing string columns stay strings (Iceberg forbids string -> variant).
- MR3: `variant_get` / `parse_json` / `json_extract` over variant, `typeof`, and shredded reads by running `unshred_variant` in the coordinator and worker Parquet readers (files from Spark 4 / Iceberg 1.11 shredded writes fail today).
- Upgrade the Spark test stack to iceberg-spark-runtime-4.0_2.13 1.10.x and add an SQE-writes / Spark-reads variant test.
- Flight SQL: convert nested variants (inside ROW / ARRAY) to JSON text; only top-level columns are converted.
- Tests for compaction and the two-worker scan of a variant table.
