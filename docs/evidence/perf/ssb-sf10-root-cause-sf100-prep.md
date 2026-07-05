# SSB SF10 root cause and SF100 preparation

Date: 2026-07-05. Status: root cause pinned to three layers; SF100 blocker list
verified in code. Supersedes the "cause pinned to engine maturity" wording of
`docs/superpowers/specs/2026-06-25-ssb-sf10-gap-investigation-findings.md` with a
measured mechanism. Companion evidence: `sf10-slow-queries.md`,
`sf10-loser-aggregation.md`, `sf10-bucket-a-scan-parallelism-design.md`,
`sf100-scaling-risks.md`.

## Summary

SSB is the one suite where SQE still trails Trino at SF10 (0.53x on the DF53 clean
rig, 0.46x on DF54). The candidate causes were type mismatch or inference, Iceberg,
or a DataFusion limitation. Measurement rules out the first two entirely. The loss
decomposes into three layers:

1. **q1.x and q3.4 are healthy.** Direct fact predicates prune row groups; SQE wins
   or ties these at SF10.
2. **q4.1 and q4.2 lose their part filter at SF10.** Their build side
   (`p_mfgr = 'MFGR#1' OR 'MFGR#2'`) is 320,000 distinct partkeys, which crosses
   DataFusion's InList pushdown cap (`hash_join_inlist_pushdown_max_distinct_values`,
   SQE default 65,536). Above the cap the join emits an opaque hash-map probe; only
   useless min/max bounds survive translation to the scan. One config raise recovers
   this layer.
3. **Everything else is single-output-partition scan throughput.** The dynamic
   filters arm correctly and cut decode proportionally at every practical key count.
   What they cannot cut is I/O (uniformly scattered FKs put matches in every row
   group) or the key-column decode of the full fact table, and all of it funnels
   through one output stream. At 10x the rows, the funnel loses to Trino's
   split-parallel pipelines. The one prior attempt at output parallelism (#235) was
   perf-neutral, so this layer needs profiling before more construction.

## 1. Where the gap lives

Clean-rig A (2026-06-16, DF53, Trino 465), from
`benchmarks/results/compare-ssb-sf10-2026-06-16T10:07:31.json` and the matching SF1
file. Ratio above 1 means SQE faster.

| Query | SQE SF10 ms | Trino SF10 ms | Ratio | SQE SF1 ms | Trino SF1 ms | Ratio |
|---|---:|---:|---:|---:|---:|---:|
| q1.1 | 617 | 522 | 0.85x | 216 | 407 | 1.88x |
| q1.2 | 193 | 377 | 1.95x | 90 | 339 | 3.77x |
| q1.3 | 148 | 259 | 1.75x | 100 | 346 | 3.46x |
| q2.1 | 3582 | 1903 | 0.53x | 487 | 716 | 1.47x |
| q2.2 | 3169 | 1632 | 0.51x | 411 | 660 | 1.61x |
| q2.3 | 2764 | 1302 | 0.47x | 419 | 588 | 1.40x |
| q3.1 | 3471 | 2346 | 0.68x | 448 | 668 | 1.49x |
| q3.2 | 2734 | 1169 | 0.43x | 388 | 596 | 1.54x |
| q3.3 | 1973 | 945 | 0.48x | 290 | 528 | 1.82x |
| q3.4 | 311 | 347 | 1.12x | 144 | 452 | 3.14x |
| q4.1 | 7049 | 3327 | 0.47x | 1107 | 784 | 0.71x |
| q4.2 | 2768 | 1719 | 0.62x | 361 | 613 | 1.70x |
| q4.3 | 3028 | 996 | 0.33x | 315 | 582 | 1.85x |
| Total | 31807 | 16844 | 0.53x | 4776 | 7279 | 1.52x |

The loss is not uniform. q1.x and q3.4 win or tie at SF10. The multi-dimension
family q2.x/q3.x/q4.x trails in a 0.33x to 0.68x band. q4.1 alone is a quarter of
the total gap and is the only query that also loses at SF1. q4.3 flips hardest,
1.85x win at SF1 to 0.33x loss at SF10. The DF54 rerun two days later
(`compare-ssb-sf10-2026-06-18T07:29:13.json`) corroborates the pattern at 0.46x.
Single cold run per query; the SF1 noise floor is 5 to 12 percent.

## 2. Ruled out, with evidence

- **Type mismatch or inference.** Both engines run byte-identical SQL from
  `benchmarks/queries/ssb/` against the same Iceberg tables (Trino is a read-only
  consumer of the same Polaris catalog). The landed schema has Int32 join keys on
  both fact and dim sides, Int64 `lo_orderkey` unused in joins, Float64 money.
  EXPLAIN VERBOSE of q4.1 contains zero casts. Refuted at the storage layer and in
  the plan.
- **Iceberg storage asymmetry.** Same files, both engines. Refuted.
- **Seal-race timing.** The dim hash builds seal in under 1ms; `filter_wait_time`
  on the fact scan measured 0.18ms; raising `runtime_filters.wait_ms` from 100 to
  1000 changed nothing (identical `rows_decoded`). Refuted.
- **Filter shape degradation as the suite-wide cause.** A controlled key-count sweep
  (section 4) shows the hash-set RowFilter cutting decode proportionally at every
  step up to 64,051 build-side keys. q3.1/q3.2/q3.3 join no part table, cross no
  threshold, and still lose in the same band. q4.3 has every filter armed at SF10
  (largest build 32,000 keys) and loses hardest. Refuted as primary driver;
  confirmed as a real secondary effect for q4.1/q4.2 only (section 5).
- **Raw decode throughput.** ClickBench, which is decode-heavy with no joins, wins
  2.06x. Refuted.
- **Scan task parallelism inside the file.** The #131 intra-file split is live on
  this path (`with_task_split_target_size`, 32MB), fanning a 154MB file into ~5
  concurrently decoding subtasks. Present, yet the funnel remains (section 3).

## 3. Measured at SF1: the filters work, the funnel is the wall

Instrumented run on ssb_sf1 (2026-07-05, docker-compose.test.yml stack, one 154MB
lineorder parquet file, unsorted, unpartitioned; instrumentation dumps the named
DataFusion metrics from `crates/sqe-catalog/src/iceberg_scan.rs:795-813` during
EXPLAIN ANALYZE).

q4.1: `pushed_filters=3`, `rows_decoded=234,889` of 6,000,000 (a 25x cut),
`bytes_scanned` 80MB of 154MB. q2.1: 48,368 decoded. q1.1 takes the static-predicate
path instead: 7.7MB scanned via row-group pruning, 112,190 decoded.

The wall sits above the scan, not in the joins: `fetch_time` on the RepartitionExec
directly over the lineorder scan is 727ms of q4.1's ~1.07s, while all three hash
joins together cost ~20ms and the aggregates ~4ms. Per-operator `elapsed_compute`
under-reports async decode; `fetch_time` carries the truth. The scan advertises
`UnknownPartitioning(1)`, so the ~5 decode subtasks re-merge into one output stream
and every surviving row crosses one partition boundary.

## 4. Measured: no practical InList threshold, and I/O is constant

Controlled sweep on ssb_sf1, stepping the build-side key count with scattered-key
dim filters (bounds are useless on these, so only an armed IN-set/hash-set can cut
decode):

| build filter | keys | rows_decoded | expected | bytes_scanned |
|---|---:|---:|---:|---:|
| c_city, 1 city | 118 | 23,479 | 23,599 | 43.6MB |
| c_nation, 1 | 1,195 | 239,088 | 239,000 | 43.6MB |
| c_region, 1 | 5,959 | 1,192,095 | 1,191,800 | 43.6MB |
| c_region IN 2 | 11,861 | 2,373,033 | 2,372,200 | 43.6MB |
| p_mfgr, 1 | 15,752 | 1,182,549 | 1,181,400 | 45.8MB |
| p_mfgr IN 2 | 31,981 | 2,398,790 | 2,398,575 | 45.8MB |
| p_mfgr IN 3 | 48,048 | 3,598,163 | 3,603,600 | 45.8MB |
| p_mfgr IN 4 | 64,051 | 4,804,499 | 4,800,000 | 45.8MB |

Two load-bearing facts. First, `rows_decoded` tracks predicted selectivity within
one percent at every step; there is no key count in the tested range where the
filter stops working. Second, `bytes_scanned` never moves. A 118-key filter and a
no-filter scan fetch the same bytes, because uniformly scattered FKs put matches in
every row group and page. The dynamic filter saves value-column decode CPU. It saves
no I/O, and the key column is always decoded across all 6M rows to evaluate the
semijoin. Both costs scale linearly with fact size while the output stays one
partition. That is the SF1-to-SF10 crossover: the same fraction survives, but 10x
rows pay key decode and 10x survivors cross the single-stream funnel.

## 5. The threshold that is real but narrow

The shape decision lives in DataFusion 54's hash join build
(`datafusion-physical-plan/src/joins/hash_join/exec.rs:2036-2060`): the dynamic
filter is a translatable InList only while distinct keys stay at or under
`hash_join_inlist_pushdown_max_distinct_values` AND the estimated size stays under
`hash_join_inlist_pushdown_max_size`; above either it becomes an opaque hash-map
probe, and only min/max bounds survive `convert_physical`
(`vendor/.../physical_to_predicate.rs:100-231`) into the scan. SQE already raised
the caps from DataFusion's defaults (150 values, 128KB) to 65,536 values and 4MB
(`crates/sqe-core/src/config.rs:280-287`, applied in
`session_context.rs:296-308`), which is exactly why the sweep in section 4 found no
cliff below 64K.

Build-side distinct keys per query (dims: customer 30K x SF, supplier 2K x SF,
part 80K x SF, date 2,557 fixed):

| query | largest build | keys SF1 | keys SF10 | vs 65,536 |
|---|---|---:|---:|---|
| q2.1 | part p_category | 3,200 | 32,000 | under |
| q3.1 | customer c_region | 6,000 | 60,000 | under, barely |
| q4.1 | part p_mfgr, 2 of 5 | 32,000 | 320,000 | crosses |
| q4.2 | part p_mfgr, 2 of 5 | 32,000 | 320,000 | crosses |
| q4.3 | part p_category | 3,200 | 32,000 | under |

Only q4.1 and q4.2 cross, and only on the partkey filter (their custkey and suppkey
filters still arm). The Int32 byte cost of 320K keys is 1.28MB, under the 4MB byte
cap, so the value cap is the binding one. There is no second cap downstream: the
vendored `MembershipSet` builds an uncapped hash set, and `IN_PREDICATE_LIMIT=200`
in iceberg-rust gates only min/max pruning, not the RowFilter. Caveat: SF10 arming
for the "under" rows is inferred from key counts, not yet measured; the clean-rig
discriminator in section 7 converts it to fact.

## 6. Fix plan, ranked

1. **Raise `runtime_filter_inlist_max_values` past 320K and A/B q4.1/q4.2 at SF10.**
   Cheapest lever, targets the two queries that carry ~32 percent of the gap. Watch
   two costs: seal-time construction of a 320K-literal InList, and TPC-H/TPC-DS
   regression (the cap exists for a reason upstream). If seal cost bites, the
   alternative is teaching the pushdown to hand over the build's key set as a
   hash-set predicate directly instead of an InListExpr.
2. **Profile the single-stream funnel at SF10 before building anything.** The
   evidence says output-partitioning is the suite-wide wall, but the opt-in
   ParallelProbeScanRule (#235) measured perf-neutral (34.1s on, 31.7s off), so a
   naive multi-output scan is not the fix. Instrument where the merged stream
   saturates (merge poll thread, the RepartitionExec above the scan, Tier-2
   re-evaluation) and why #235 failed to move it. `fetch_time` per partition is the
   metric. Only then re-attempt source-parallel output partitioning (the Phase 2
   broadcast-parallel-probe design in `sf10-bucket-a-scan-parallelism-design.md`).
3. **Distributed SSB stays gated on shipping membership sets to workers.** The
   serialized range predicate cannot carry hash-set selectivity, which is why
   distributed-2w (53.6s) is slower than single-node (42.0s) on the level rig. The
   DF54 `DynamicFilter` protobuf spike from `roadmap-tracker.md` is the entry point.
4. **Do not spend on: longer `wait_ms`, `clustering_skip` as an SSB fix (11 percent,
   real but small), bloom filters for I/O on scattered keys (every page has matches;
   the sweep's constant `bytes_scanned` shows page skipping cannot win), or more
   filter wiring (the filters already arm and cut decode).**

## 7. SF100 readiness

Blockers in dependency order, states verified in code this session:

1. **Generator buffers whole tables.** `generate_lineorder`
   (`crates/sqe-bench/src/generate/ssb.rs:616-739`) returns the full table as
   `Vec<RecordBatch>`; SF100 lineorder is 600M rows, roughly 68GB resident. The
   streaming infrastructure already exists and is shared (`parallel_generate_table`
   in `generate/mod.rs:71-165`, `write_parquet_stream` rotating at 128MB); TPC-H
   uses it since commit e211820. Port scope is modest: rewrite lineorder generation
   as a range function like `generate_lineitem_range`. One wrinkle: lineorder
   carries cross-batch sequential state for multi-line orders. `lo_orderkey` is not
   a join key in any SSB query, so relaxing the grouping to a deterministic function
   of row offset is low risk.
2. **Sort-on-write must degrade, not die.** The lo_orderdate clustering sort OOMs
   at scale (`ExternalSorterMerge` reports `can_spill=false`) and the loader falls
   back to an unsorted write, silently giving up q1.x pruning at exactly the scale
   where it matters most.
3. **Shuffle has no spill.** The in-memory bounded-channel shuffle bounds every
   distributed SF100 run (`sf100-scaling-risks.md` risks 2 and 5).
4. **Worker scan backpressure.** Decode outruns Flight shipment and exhausts the
   4GB worker pool at SF10 TPC-DS already; SF100 makes it a certainty.
5. **Memory-pool discipline under concurrency.** The fair pool is opt-in and
   regresses TPC-DS q39; plan-adaptive pool selection and the upstream per-consumer
   cap (apache/datafusion#17334) remain open.
6. **Rig.** SF100 needs a dedicated box per the clean-rig recipe. Items 1 and 2 are
   prerequisites for loading the dataset at all; measurement before they land is
   not possible on any box.

The immediate SF10 confirmation run for section 6 needs only the clean rig and the
one-line instrumentation: dump named Count/Time metrics in
`crates/sqe-coordinator/src/explain.rs` `walk_analyze`, run with
`RUST_LOG='sqe_coordinator=info,sqe_catalog=debug,warn'`, and read `rows_decoded`
for q2.1/q3.1/q4.3 (expected well under 60M if the arming inference holds) and
q4.1/q4.2 (expected near 60M on the partkey path until fix 1 lands).

## 8. Side findings

- **SSB has no value-level oracle.** `benchmarks/expected/canonical_rows_duckdb.json`
  has no `ssb` key, so compare runs check row counts only, and SSB money columns are
  Float64. A value-level divergence would be invisible today. Add SSB to the
  canonical-rows manifest.
- **`benchmarks/schemas/ssb.sql` disagrees with the generator** on money types
  (BIGINT vs Float64), `lo_shippriority` (VARCHAR vs Int32), and the spelling of
  `lo_ordtotalprice`. The file is documentation-only and never executed; fix it or
  delete it.
- **Doc drift**: `docs/site/book/src/design-notes/runtime-filter-pushdown.md` still
  states `with_dynamic_predicate` is intentionally absent from `IcebergScanExec`.
  The current code registers it by default (MR #220 plus the seal-wait). A pointer
  note has been added there.
- SF1 evidence run: `benchmarks/results/ssb-sf1-flight-2026-07-05T13:24:57.json`
  (contended dev box, structure-only signal).
