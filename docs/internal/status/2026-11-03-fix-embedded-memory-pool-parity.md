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
spillable 80% taken, and embedded has no per-query sort share or #784
join-fallback retry, but at 3 GB the build fits. Wall times are single runs on a busy laptop; spill counts are the
stable signal.

**NEXT:** 150M engine-bench rerun with the new pool, then the
`feat/engine-bench-polaris-readers` MR.
