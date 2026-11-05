**STATUS 2026-11-03: embedded mode uses the coordinator's memory pool (`fix/embedded-memory-pool-parity`, #788).**

`sqe-cli --embedded` built a `FairSpillPool`; the coordinator has defaulted to
greedy with a 20% unspillable headroom (`HeadroomPool`) for a while. Under the
fair pool every unspillable byte (the Iceberg scan's fetch and decode
admissions) came off each spillable consumer's equal share, so aggregates
spilled far more than on the server at the same limit. `memory_headroom.rs`
moved from `sqe-coordinator` to `sqe-core` (unchanged apart from `pub`), and
the embedded engine now wraps `TrackConsumersPool<GreedyMemoryPool>` in it.

Engine bench, 150M rows on Polaris/RustFS, 3 GB, final aggregate of q3:

| run | before | after |
|---|---|---|
| embedded, scan tracking on | 20.3s, 96 spills / 3.4 GB | 14.2s, 0 spills |
| embedded, tracking off | 13.8s, 16 spills | 13.7s, 0 spills |
| embedded local `read_parquet` | 5.8s, 24 spills | 5.2s, 0 spills |
| coordinator `memory_pool = fair`, on / off | 88 / 44-55 spills | (unchanged knob) |
| coordinator greedy (default), on / off | 0 / 0 spills | 0 / 0 spills |

The scan's reservation sizes (`scan_memory.rs`) were not the lever: on the
greedy pool the tracked/untracked gap is noise. q1 and q4 also run without
spills, and so does q2, the join (5.1s on Polaris, 1.3s on local Parquet):
under greedy a late starter such as a hash-join build could find the
spillable 80% taken, and embedded has no #784 join-fallback retry, but at
3 GB the build fits. Wall times are single runs on a busy laptop; spill counts
are the stable signal.

Sort share: the coordinator caps each sort buffer (`ExternalSorter[n]`) at an
equal share of the spillable bytes in the per-query pool it builds for every
statement (`query_memory.rs`, `with_sort_share`). Embedded has no per-query
pool, so a greedy pool alone let the first sort partitions take every
spillable byte and fail the query on a sibling that had not started (the
starvation #788 fixed on the server). `HeadroomPool::wrap_single_query` puts
the share on the embedded pool; its refusals name `sqe-cli --memory-limit`
instead of a coordinator setting. Measured after the change (release, 3 GB,
150M rows on Polaris): q3 12.0-15.3s, 0 spills (unchanged); a full
`ORDER BY price, order_id` of 150M rows 13.8s, 39 spills, completes.

Write path: the embedded write handlers ran on `SqeConfig::embedded_defaults()`,
so a sort-on-write was admitted against the server's 256 MB
`query.max_query_memory`. `EmbeddedClient::handler_config` sets it from the
pool (`--memory-limit`).

Gate: `cargo test -p sqe-cli` (110 unit + 21 smoke passed; new:
`embedded_pool_keeps_a_sort_share_for_every_partition`,
`handler_config_takes_the_memory_limit`); `cargo test -p sqe-core --lib
memory_headroom` (14 passed); `cargo test -p sqe-coordinator --lib --
memory_headroom query_memory` (7 passed); clippy `-D warnings` on sqe-core
and sqe-cli.

Finding (next branch, #788): `EXPLAIN ANALYZE` of the same full sort on
`sqe-server` at 3 GB fails, `Resources exhausted: Additional allocation failed
for ExternalSorterMerge[8] ... greedy(used: 3.0 GB, pool_size: 3.0 GB)`. The
query itself completes (150M rows, 18.2s): `EXPLAIN ANALYZE` runs on the
session context without the per-query pool (`with_query_quota`), so it is
under neither `max_query_memory` nor the sort share, and it has no
join-fallback retry either. Its numbers therefore do not describe the query
it explains.

**NEXT:** `EXPLAIN ANALYZE` under the per-query quota (#788), then the 150M
engine-bench rerun and the `feat/engine-bench-polaris-readers` MR.
