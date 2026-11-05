**STATUS 2026-11-03: coordinator spill settings reach DataFusion (`fix/spill-settings-788`, #788).**

- `coordinator.spill_compression` was dead config: `runtime.rs` logged it, nothing set
  `datafusion.execution.spill_compression`, so spills were written uncompressed (the
  DataFusion default). `session_context.rs` now sets it per session
  (`FlightCompression::spill_codec`: lz4 -> `lz4_frame`, zstd -> `zstd`, none -> `uncompressed`).
  The default stays lz4, so every spill is now lz4-compressed where it was raw.
- New `coordinator.spill_max_bytes` (default `100GB`, DataFusion's own default) sets
  `with_max_temp_directory_size`. DataFusion's refusal is an `io::Error` naming
  `datafusion.runtime.max_temp_directory_size`; `query_memory::execution_error` maps it to
  `ResourceExhausted` and names `coordinator.spill_max_bytes`. Not a `ResourcesExhausted`
  DataFusion error, so the join fallback does not retry on it.
- Book: `spill_compression` default was documented as `zstd` and as a `[worker]` key; it is
  coordinator-only, default lz4. `spill_max_bytes` documented in configuration, sizing and
  streaming-execution pages.

Measured on the engine-bench server (release build of this branch, 3 GB pool and query cap,
`SELECT order_id, customer_id, price FROM sales ORDER BY price, order_id` over 150M rows, local
SSD spill dir sampled every 200 ms):

| `spill_compression` | wall (runs) | peak spill dir |
|---|---|---|
| `lz4` (now applied) | 18.9 s, 17.6 s, 16.8 s | 1.3 GB |
| `none` (old behaviour) | 17.7 s, 16.4 s, 16.9 s | 2.9 GB |

lz4 halves the spill bytes at no measurable wall-time cost (the spread is run-to-run noise), so the
lz4 default stays.

Tests: `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it -- spill_settings_e2e join_fallback_e2e`
-> 6 passed (`spill_compression_reaches_the_session`, `a_spill_past_spill_max_bytes_fails_naming_the_setting`,
`the_same_sort_completes_under_the_default_cap` + the join fallback suite). Lib:
`a_spill_past_the_disk_cap_names_spill_max_bytes`. Clippy clean on sqe-core and sqe-coordinator.

Finding (not fixed here): a full sort of 1.5M two-column rows under a 16 MB
`max_query_memory` fails in the merge phase (`ExternalSorterMerge[3] requests 1048576 additional
bytes with 16252928 already reserved`) instead of spilling further. `merge_reservation` gives each
sorter a quarter of the quota but never less than its 1 MB floor (`MIN_MERGE_RESERVATION`), so 16
partition sorters reserve the whole 16 MB cap before any merge reads a run. At 32 MB it spills and
completes. Only tiny caps hit it; the first-attempt plan does not narrow partitions the way the
join-fallback retry does (`retry_partitions`).

**NEXT:** #784 join fallback precision test (`test/join-fallback-precision-784`), then #785, the
embedded join-fallback move (#786), the SF10 compare for #787, and the 150M engine-bench rerun +
`feat/engine-bench-polaris-readers` MR.
