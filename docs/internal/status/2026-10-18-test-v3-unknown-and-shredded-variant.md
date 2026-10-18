**STATUS 2026-10-18: Iceberg V3 unknown columns and shredded variant reads, matrix 147/162 (90.7%) (`test/v3-unknown-and-shredded-variant`).**

Both were measured against Spark 4.1 + Iceberg 1.12 on the local stack.

- unknown-type V3: ? -> partial. SQE declares `UNKNOWN` / `VOID`, reads a Spark `VOID` column, inserts around it, promotes it, and Spark reads every stage. The vendored parquet writer drops top-level unknown columns from data files (`SQE PATCH (v3-unknown)`), and the coordinator's direct read turns a decoded `Null` column into typed NULLs. Nested unknown fields fail both ways: SQE cannot read a Spark `STRUCT<a INT, u VOID>` table (the struct decodes without `u`), and the writer refuses one.
- shredded-variant V3: none -> partial (read-only). Coordinator direct read, the iceberg-rust reader (`projection.rs` validator, `record_batch_transformer.rs` promote) and the worker (`emit_batch`, `out_schema`) unshred via `parquet-variant-compute`; `_serde.rs` skips variant manifest bounds (`SQE PATCH (v3-shredded-variant)`).

Tests: `unknown_type_spark_interop.rs` (3), `variant_spark_interop.rs::sqe_reads_a_spark_shredded_variant_table`, `variant_distributed.rs::shredded_variant_column_scans_on_a_worker` (needs a native worker, as the other worker tests do).

**NEXT:**
- Iceberg-rust backport MR (apache Tier-1 fixes plus RisingWave `c5498953`, `d294ab23`, `30a72274`) on the current vendor, then a full re-vendor onto apache main.
- Nested unknown fields: fill missing struct children on read (all three read paths) and prune them on write.
- Writing shredded variants (`write.parquet.shred-variants`) and typed field access.
- Refresh docs.getsqe.com and getsqe.com/compare/iceberg with the new score.
