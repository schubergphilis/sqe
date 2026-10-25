**STATUS 2026-10-25: variants nested in structs, lists and maps are shredded on write and read back intact (`feat/variant-nested-shred-870`, #870).**

Three changes:

- Write: the vendored `VariantShredder` (`variant_shred.rs`) walks each column as a `Node` tree. A variant anywhere under a struct, list, large list or map gets its own shredding plan, inferred from the first batch like a top-level variant. Field metadata (ids, the variant extension) is kept on every rebuilt field.
- INSERT: `apply_stamped_schema` sends a struct, list or map column that holds a variant through `sqe_core::variant::from_arrow_nested`. JSON text members become variant storage; members without a variant are cast with a checked cast. Before this, `named_struct('v', '{...}')` into `STRUCT<v JSON>` failed with "Casting from Utf8 to Struct(metadata, value) not supported".
- Read: `unshred_batch` / `unshred_schema` recurse, and `fill_unknown` unshreds a shredded variant it meets while rebuilding a nested column. Before this, a nested shredded variant dropped its `typed_value`, so shredded fields read back missing. The fix covers files written by SQE and by any other engine.

Spark 4.1 + Iceberg 1.12 does not shred nested variants itself (probed: its files keep nested variants as plain `{metadata, value}`), so the interop proof is Spark reading an SQE-written file.

Gate: `make test-integration FILTER=nested_shredded_variant` -> 2 passed (`variant_spark_interop::spark_reads_an_sqe_nested_shredded_variant_table`, `variant_distributed::nested_shredded_variant_scans_on_a_worker`, the latter on an in-process worker). Unit tests: `sqe-core` variant tests, and `cargo test --offline -p iceberg --lib -- variant_shred` inside `vendor/iceberg-rust`.

`scripts/integration-test.sh` skipped all of `variant_distributed` in a default run, so a test there on an in-process worker never ran in the full suite. It now skips only the two tests that need a native worker on :50052 (`--list` with the new skips shows only `nested_shredded_variant_scans_on_a_worker`). `prunes_shredded_row_groups_on_a_worker` (!1542) gets the same coverage once both merge. Both MRs append to the end of `variant_distributed.rs`, so the second to merge needs a rebase.

The nested-shredding caveat is removed from `docs/evidence/iceberg-matrix-state.json`.

Left open on #870: data-file pruning from variant manifest bounds, typed read without unshredding, Spark multi-row-group evidence, and the remaining caveat in the matrix state.

**NEXT:** #870 data-file pruning from variant manifest bounds.
