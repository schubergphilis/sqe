# SF100+ scaling risks

Where SQE breaks above SF10. This is extrapolation, not measurement: SF100 cannot
run on the current bench rig (see risk 6), so every claim here is grounded in a
mechanism we actually observed at SF1/SF10, projected forward. Read it as a list
of predicted failure modes with the evidence that motivates each.

The meta-point: **SF1 and SF10 reward single-node tactics. SF100 inverts every one
of them.** Broadcasting the build side, building hash tables in memory, and
emitting one scan stream all win at SF10 (we proved the broadcast point this
session: forcing partitioned scans regressed q09 5x). At SF100 each becomes the
failure. The SF100 roadmap is therefore two things: make every operator spillable,
and prove distributed execution at real multi-node scale.

## Ranked failure modes

### 1. Non-spillable sort merge OOMs hard (availability, not just speed)

Verified against DataFusion 53.1.0 `physical-plan/src/sorts/sort.rs`, not just the
error text. The `ExternalSorter` has two reservations: the in-memory sort buffer is
created `.with_can_spill(true)` (line 283), but the `merge_reservation`
(`MemoryConsumer::new("ExternalSorterMerge[...]")`, line 286) is created **without**
`with_can_spill`, so it defaults to **non-spillable**. The final merge takes **all**
finished spill files in a **single pass** (`with_sorted_spill_files(take(finished_spill_files))`,
line 358), so the non-spillable `merge_reservation` grows with the spill-file count
(roughly one max-size batch per run). There is no bounded multi-pass merge.

Consequence: the sort *phase* spills fine, but the final *merge* reserves
non-spillable memory proportional to (spill runs) x (concurrent sorter instances).
It OOMs hard rather than degrading once that product exceeds the pool. We hit
exactly this at SF10 on a partitioned sort-on-write CTAS: many partitions each ran
their own `ExternalSorterMerge` (the log showed several `(can spill: false)`
consumers holding ~1.2 GB each) and the pool exhausted. The memory-safe-write MR
worked around it by skipping the sort on partitioned tables; the engine gap is
open.

Why it is the SF100 headline: at scale both multipliers grow. A single large
`ORDER BY` / sort-merge join / window / sort-on-write spills into many more runs
(bigger single-pass merge reservation), and wide or partitioned plans run many
merges concurrently. It is not literally *every* sort, but the threshold where the
non-spillable merge reservation exceeds the pool drops as data grows, and past it
the failure is a crash, not a slowdown, regardless of pool size.

Levers: (a) the real fix is a bounded multi-pass spill merge or a spillable merge
reservation; (b) `datafusion.execution.sort_spill_reservation_bytes` (line 262)
pre-reserves room for the merge and can delay the OOM, but does not remove the
non-spillable single-pass merge. This gates almost everything below.

### 2. Broadcast (CollectLeft) joins stop fitting

At SF1/SF10, broadcasting the build side is the winning join strategy on a single
node, and we have direct evidence: emitting N-partition scans flipped joins to
`Partitioned` mode and hash-shuffled the 60M-row lineitem, regressing TPC-H q09
from 17s to 90s. CollectLeft-broadcast was strictly better there.

At SF100 the build sides grow 10x. More of them cross the 64 MB
`hash_join_single_partition_threshold` and flip to `Partitioned`, and a single-node
hash-shuffle of 600M+ row facts is both CPU- and memory-bound. The build hash
tables stop fitting one node. Distributed execution stops being optional.

Lever: SQE's own shuffle-hash join across workers (`ShuffleHashJoinPlan`) already
exists, but it is unproven at scale. Every distributed measurement this session was
confounded by running coordinator plus workers on one box (CPU and memory
co-tenancy). A real multi-node verdict is missing.

### 3. Scan/decode throughput stays single-partition

`IcebergScanExec` emits one output partition. The parallel Tier-2 filter MR fixed
the *filter*-bound queries (TPC-DS q50 11s to 4.5s, TPC-H SF10 flipped to winning),
but it deliberately kept the single output partition to avoid the join-mode
regression in risk 2. Throughput-bound scans that cannot prune (SSB's uniform
foreign-key distribution: 0.47x at SF10, unchanged by the filter fix) still funnel
every row through one stream. At SF100 that is ~600M lineorder rows decoded on one
output partition.

Lever: the broadcast-parallel-probe design in `sf10-bucket-a-scan-parallelism-design.md`
(N-partition probe feeding a CollectLeft join, no fact-table shuffle). Designed,
not built, and it is the hard part because it requires a custom optimizer rule to
keep CollectLeft while parallelizing the probe.

### 4. Dynamic-filter membership sets scale with the fact

The q50 win parallelized 15 build-side IN-set evaluations over 86M rows. At SF100
those are ~860M-row evaluations, and the key sets themselves are 10x larger (the
membership sets cost memory, not just CPU). Parallelism keeps the wall-clock
bounded but the absolute CPU and the set memory grow linearly with scale.

Lever: push the resolved membership sets into the parquet RowFilter (pre-decode)
where the projection allows it, so fewer rows are materialized before filtering.
The SSB key-set work already does this for some shapes; q50's projection is all
join keys, so it does not benefit there.

### 5. Monster queries and large intermediates

TPC-DS q72 is 140s at SF10 (a 1.17B-row inventory join, the single biggest query).
At SF100 the inputs are 10x and the join intermediates (38M output rows at SF10)
grow with them. Single-node memory for the intermediate hash tables and the
non-spillable sort/merge (risk 1) is the wall. These queries need distribution plus
spill, both of which are the gaps above.

### 6. Benchmarking is blocked before the engine runs

The data generator buffers a whole table in memory before writing it: TPC-DS
inventory at SF10 already required the entire 31 GB box (we had to stop the engines
to generate it). SF100 would need hundreds of GB to generate a single fact table,
and the sort-on-write load then hits risk 1. So SF100 cannot be measured on the
current tooling at all until the generator streams row groups to disk and the write
path is spill-safe. Fix this first or none of the above is observable.

## Summary

| # | Failure mode | First bites | Lever | State |
|---|---|---|---|---|
| 1 | Non-spillable single-pass sort merge OOM | large sorts (many spill runs) + wide/partitioned plans | bounded multi-pass / spillable merge | engine fix, open (extends the partitioned-write issue) |
| 2 | Broadcast joins overflow | big fact-fact joins | distributed shuffle join | exists, unproven at multi-node scale |
| 3 | Single-partition scan | scan-bound (SSB) | broadcast-parallel probe | designed, not built |
| 4 | Membership-set eval/memory | dim-filtered fact scans | pre-decode RowFilter pushdown | partial (SSB key-set) |
| 5 | Monster queries / intermediates | q72-class | distribution + spill | depends on 1 and 2 |
| 6 | Generator/load OOM | generation itself | streaming generator + spill-safe load | blocks measurement |

Single-node perf work (the parallel filter, the memory-safe write) is necessary and
tops out around SF10 to SF30. SF100 needs spillable operators (risk 1) and a proven
multi-node distributed path (risk 2) before anything else is worth tuning.
