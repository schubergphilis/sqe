**STATUS 2026-10-24: iceberg-rust backport round 2, promoted-column filters fixed (`fix/iceberg-backport-round2`).**

Nine upstream picks went into `vendor/iceberg-rust`. From apache: #3502 (NaN row-group bounds), #3476 (float JSON reads), #3473 (empty projection), #3483 (promoted-type pruning), #3431 (evolved spec loading) and #3413 (field id reuse). From RisingWave: #261, #262 and #246 (`rewrite_files` properties, a manifest per spec, non-wrapping summary totals). `vendor/iceberg-rust/README.md` lists each one with the adjustments made.

The new integration test `v3_e2e::type_promotion_keeps_old_files_in_filtered_scans` showed that main is broken in two ways:

- **Wrong results, no error.** After `ALTER COLUMN p SET DATA TYPE BIGINT`, a probe on main returned `p = 2` -> no rows, `p >= 2` -> only the new row, and `p IN (1, 3)` -> no rows. Pruning compared the old files' INT bounds with the widened predicate. #3483 fixes this.
- **DELETE crash.** A DELETE that removed a pre-promotion file panicked at `datum.rs:349` while building the partition path. With that fixed, its commit failed with "value is not compatible with type". Two SQE vendor patches fix this: `Transform::to_human_string` widens the value first, and `ManifestWriter::write_manifest_file` widens the carried-over partition literals to the current partition type, as iceberg-java does.

Gates:

- Vendor lib: `cargo test -p iceberg --lib` in `vendor/iceberg-rust` -> 2136 passed, 0 failed.
- `make test-integration FILTER=type_promotion` -> 4 passed, gate=0.
- `make test-integration` (full suite) -> 322 passed, 0 failed, gate=0.

Left open: apache #3267 (multi-arg transforms, `source-ids`) is not taken. #864 tracks it.

**NEXT:** #864 `source-ids` serde acceptance, then the #866 catalog CI job and #870 shredded variant pruning.
