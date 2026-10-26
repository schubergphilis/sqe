**STATUS 2026-10-26: zstd advisories cleared with a vendored zstd 0.14.3 (`fix/zstd-advisories`).**

deps-check failed on every MR: zstd 0.13.3 (RUSTSEC-2026-0391) and its zstd-safe 7.3.0 (RUSTSEC-2026-0395..0406). arrow/parquet 59.3, DataFusion 55.2 and apache-avro all require zstd ^0.13, and no 0.13.x release carries the fixes. DataFusion main is on arrow 60 (zstd 0.14), but DataFusion 56 is not released.

- `vendor/zstd`: zstd 0.14.3 from the cargo registry (checksum-verified copy), version relabeled to 0.13.4, with the `[[example]]` / `[[test]]` targets and dev-dependencies removed (those files are not vendored). Patched in via `[patch.crates-io]`, excluded from the workspace, like `vendor/object_store`.
- zstd-safe 7.3.0 leaves Cargo.lock; everything runs on zstd-safe 8.2.0. workspace-hack regenerated (`zstd` path, `zstd-safe` 8).
- 0.14 only adds API over 0.13: the full workspace builds unchanged (`cargo dev`).
- cargo-deny does not match path crates: `advisories ok` with no new ignore. cargo-audit matches the 0.13.4 label against RUSTSEC-2026-0391 (patched `>= 0.14.3`), so `.cargo/audit.toml` ignores that one id with a review date; `vendor/reviewed-advisories.txt` lists the three zstd-name advisories.

Gates:
- `cargo deny check advisories bans sources` -> ok; `cargo audit` -> exit 0 (the clipboard-win yanked warning is fixed by !1554).
- `cargo dev` -> builds; `cargo test -p sqe-write --features test-sqlite --lib` (RUST_MIN_STACK=32MiB) -> 252 passed. Parquet writes default to zstd, so these write and read through the shim.
- `make test-integration FILTER=mor_spark_interop` -> ok, 9 passed (Spark reads SQE's zstd files and the reverse). A first run failed 3 of 9 on a Spark driver NPE (`BlockManagerId ... is null`) during CREATE TABLE; after `docker restart sqe-spark-iceberg` all 9 pass.
- `make test-integration FILTER=insert_value_integrity` -> ok, 1 passed.

**NEXT:** drop `vendor/zstd`, the patch and the audit ignore when DataFusion 56 moves to arrow 60.
