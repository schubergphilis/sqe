**STATUS 2026-10-25: SQE writes variant manifest bounds (`feat/variant-write-bounds-870`, part of #870).**

- **Vendor (`variant_shred.rs`, `SQE PATCH (v3-shredded-variant)`)**: `VariantShredder::shred` (now `&mut self`) walks each top-level shredded column's values along its shredding plan and keeps, per path, the smallest and largest value. It reads the values, not `typed_value`, so a row the shredder writes unshredded (a misfit) still counts; a value of another type at a path drops that path's bound. `variant_bounds()` encodes them as the spec says (variant metadata followed by an object keyed `$['a']['b']`, the format of the Spark bytes in `variant_prune.rs::SPARK_BOUND`). Int (as Int64), string up to 64 bytes, boolean and date paths get bounds; a double, a longer string, the whole-value path `$` and a key holding `'` or `\` do not. Nested variants (in a struct, list or map) get none: `VariantPrune` only resolves top-level columns.
- **Vendor (`parquet_writer.rs`)**: `close()` sets `variant_lower_bounds` / `variant_upper_bounds` on the data file. Compaction writes through the same writer, so a rewritten file gets bounds too.

Gates:
- Vendor `cargo test -p iceberg --lib` -> 2143 passed (3 new: bounds over two batches incl. a nested path, a boolean, nulls and a non-object row; a type change drops only that path; doubles, long strings, quoted keys and columns without a field id get none).
- `make test-integration FILTER=variant_spark_interop` -> 7 passed, none skipped. Clippy clean on sqe-coordinator (no vendor lint in the changed files); `cargo xtask matrix-report --print-only` -> 152/162. New `sqe_prunes_its_own_files_on_variant_bounds`: three SQE INSERTs, `EXPLAIN FULL` files_scanned/files_total `>= 9` 2/3 (baseline 3/3), `$.s = 'd'` 1/3, `int < 2` 2/3; after `rewrite_data_files` the merged file is skipped for `$.s = 'z'` (0/1); Spark 4.1 returns the same rows with SQE's bounds in the manifest.

Open:
- Typed read without unshredding; Spark multi-row-group pruning evidence.
- String bounds are left out above 64 bytes rather than truncated (Iceberg Java truncates to 16 with an incremented upper).

**NEXT:** typed read of shredded variants without unshredding (#870).
