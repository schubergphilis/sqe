**STATUS 2026-11-05: parquet 59.3.2 for 512-value delta miniblocks (`fix/parquet-delta-miniblock-skip`).**

The Snowflake Horizon interop run found that a 4M-row Snowflake table with
merge-on-read deletes (V2 position deletes and V3 deletion vectors) failed in
SQE with "cannot skip miniblock of size 512". Small tables never showed it:
Snowflake rewrites them copy-on-write, so no row selection runs. The bug is in
parquet's `DeltaBitPackDecoder::skip`, fixed upstream in arrow-rs #11021
(Photon's 256-value miniblocks, issue #11018) and released in 59.3.2.
`Cargo.lock` moves arrow-* and parquet to 59.3.2;
`crates/sqe-catalog/src/parquet_delta_skip_tests.rs` hand-encodes a
512-value-miniblock page and skips inside and across miniblocks.

**NEXT:** rerun the Snowflake Horizon harness with merge-on-read tables
(`test/snowflake-horizon-interop`, !1661) on top of this bump and !1660.
