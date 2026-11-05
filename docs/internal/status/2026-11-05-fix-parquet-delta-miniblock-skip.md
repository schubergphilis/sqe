**STATUS 2026-11-05: parquet 59.3.2 for 512-value delta miniblocks (`fix/parquet-delta-miniblock-skip`).**

The Snowflake Horizon interop run found that a 4M-row Snowflake table with
merge-on-read deletes (V2 position deletes and V3 deletion vectors) failed in
SQE with "cannot skip miniblock of size 512". Small tables never showed it:
Snowflake rewrites them copy-on-write, so no row selection runs. The bug is in
parquet's `DeltaBitPackDecoder::skip` (skip buffer of 32 / 64 values, arrow-rs
#9793), fixed upstream in arrow-rs #11021 (Photon's 256-value miniblocks,
issue #11018) and released in 59.3.2. `Cargo.lock` moves arrow-* and parquet
to 59.3.2; `crates/sqe-catalog/src/parquet_delta_skip_tests.rs` hand-encodes a
512-value-miniblock page and skips inside and across miniblocks.

arrow-ipc 59.3.2 also writes a 0-length validity buffer for null-free columns,
which broke `sqe-bench`'s `generation_golden` (it hashed IPC bytes). The test
now hashes field names, types, null flags and displayed values; the new golden
was recorded on 59.3.1 and checked unchanged on 59.3.2.

Verified with the rebuilt server: Snowflake harness 40/40 (1M-row MoR tables
included) and Databricks harness 27/27, each in embedded, coordinator-local and
distributed mode.

**NEXT:** merge this, then !1661 (harness, docs, blog).
