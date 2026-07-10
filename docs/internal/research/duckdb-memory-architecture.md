# DuckDB Memory Architecture: What Transfers to SQE

Research date: 2026-07-07 (Opus research agent; synthesized into
`openspec/changes/scan-throughput-memory-safety`). Context: SQE on
DataFusion 54, GreedyMemoryPool + TrackConsumersPool, spill via
DiskManager; pain points at the time of research: (1) DataFusion's
ExternalSorter merge phase cannot spill; (2) glibc malloc parks freed
arenas after large sorts (RSS stays at peak live set, measured 22.4GB
parked on the rig); (3) a fixed pool cap must be hand-configured per box;
(4) no unified budget across caches (parquet footer, metadata, moka
policy) and operator memory.

## Executive summary

DuckDB's memory story is one architectural idea plus one operational
habit. The idea: a single buffer manager owns ALL memory (persistent
pages and query intermediates alike) under one `memory_limit`, so it
decides globally what stays resident and what spills, and every
pipeline-breaking operator (sort, hash join, hash aggregate) can degrade
to disk instead of failing. The habit: they ship a tuned jemalloc so RSS
actually returns to the OS after big operations.

DataFusion cannot adopt the buffer manager wholesale. It has no page
pool; operators reserve against a `MemoryPool` counter, spilling is
per-operator and uncoordinated, and SQE's caches allocate entirely
outside that counter. But three of SQE's four pain points map to small,
SQE-owned changes, and the two hard ones (sort-merge spill, grace
hash-join spill) are upstream-tracked in DataFusion, so the right move
there is track/vendor rather than build.

A load-bearing fact for prioritization: SQE is already on DataFusion 54,
and DF 54 (June 2026) shipped morsel-driven parquet scans and spilling
NESTED-LOOP joins. It did NOT ship spilling hash joins.

## 1. The buffer manager: one budget for everything

DuckDB routes every allocation through a buffer manager built on
fixed-size blocks (256 KB storage unit). No separate buffer-pool
reservation carved from RAM: persistent table pages and transient query
intermediates share one global eviction decision.

- Unified limit: `memory_limit` defaults to 80% of physical RAM. The
  docs are explicit that some allocations fall outside the manager,
  which is why 100%-of-RAM settings still OOM.
- Eviction: unpinned blocks LRU; blocks re-readable from the database
  file are cheap to drop; `MANAGED_BUFFER` intermediates must be written
  to the temp directory before eviction; tiny buffers go last.
- Temp spill: `temp_directory` bounded by `max_temp_directory_size`
  (default 90% of remaining disk).
- Intermediates are buffer-managed blocks, so spilling is transparent to
  operators. `duckdb_memory()` breaks usage down by component;
  `duckdb_temporary_files()` lists spilled files.

Transfer verdict: the architecture is not portable to DataFusion, but
two behaviors are: a RAM-fraction default limit, and counting caches
against the same budget as operators.

## 2. Out-of-core operators vs DataFusion 54

- Hash aggregate (out-of-core since 0.9.0): thread-local pre-aggregation
  into small non-resized tables with radix-partitioned backing data;
  when full, reset and unpin (spillable); phase 2 combines partitions
  with over-partitioning so each fits in memory. Proof point: 1B groups
  in 4.5 min on a 16GB laptop vs 8.6s on a 256GB box: slower, not dead.
- Hash join: radix-partitioned, falls back to grace hash join with
  recursive sub-partitioning. Known failure: Zipfian key skew.
- Sort: k-way merge (redesigned 2021 and again Sept 2025, PR #17584)
  spills sorted runs page-by-page and streams merged output with a
  bounded working set per run.

The contrast that matters: DataFusion's ExternalSorter spills sorted
runs fine, but the MERGE phase accumulates output into a non-spillable
reservation (`ExternalSorterMerge` `can_spill=false`, which SQE has hit).
Upstream tracking: EPIC #1568, #16132 (stabilize external sort), #14692,
#14748 (accounting), #17334.

DataFusion 54 status, precisely: spilling landed for NestedLoopJoinExec
(INNER/LEFT/LEFT SEMI/LEFT ANTI/LEFT MARK), NOT for HashJoinExec. Hash
join spill is proposal-stage (#12952, #17267), architecturally blocked:
per-partition reservations against a shared pool cannot coordinate
partition-wise spilling. GroupedHashAggregate spilling exists with open
efficiency issues (EPIC #13123).

## 3. Allocator strategy (SQE's cheapest win)

DuckDB bundles tuned jemalloc on Linux. They initially disabled
jemalloc's `opt.retain` (in-process library, memory retained after
connection close), then found with the jemalloc developers that the RSS
problem was a misconfiguration, re-enabled retain, and settled on:

```
metadata_thp:always,oversize_threshold:268435456,dirty_decay_ms:10000,
muzzy_decay_ms:10000,narenas:<cpu_count>,max_background_threads:<cpu_count/32>
```

Overridable via `DUCKDB_JE_MALLOC_CONF`. Community guidance for
aggressive RSS return: `dirty_decay_ms:0,muzzy_decay_ms:0,
background_thread:true`. The recurring lesson (jemalloc #2688, #2751):
decay settings only reliably take effect with background threads
enabled.

Transfer: SQE can swap `#[global_allocator]` to tikv-jemallocator (or
mimalloc) with an adapted decay config in an afternoon. The transferable
part is the RSS-decay behavior for a long-running server, which glibc's
per-arena retention specifically fails at (the measured 22.4GB parking).

## 4. Streaming execution and the parquet reader

DuckDB's 2048-row vector pipeline gives constant memory for
scan-filter-project; only pipeline breakers accumulate. DataFusion is
already a streaming batch engine, so SQE largely has this property; the
gap is breaker spill behavior (section 2) and un-budgeted caches
(section 5). DF 54 reworked parquet scans around a morsel-driven design
(idle threads pull small work units; up to ~2x on skewed scans). SQE's
issue #131 intra-file split already addressed the equivalent problem in
the vendored Iceberg reader; not a build item.

## 5. Why DataFusion cannot host a unified buffer manager

DataFusion's `MemoryPool` implementations are counters operators reserve
against, plus per-operator spill logic that cannot coordinate
partition-wise spilling. Caches allocate outside the pool entirely. No
upstream proposal changes this; it is a foundational design difference.
The transferable version of DuckDB's unified budget: register SQE's
caches as `MemoryPool` consumers and default the pool to a RAM fraction.

## 6. Prioritized shortlist for SQE

| # | DuckDB mechanism | SQE integration point | Effort | Risk |
|---|---|---|---|---|
| 1 | Tuned jemalloc (background purge + non-zero decay) so RSS returns to the OS | Swap global allocator; adapt DuckDB's config for a long-running server | S | Low; validate RSS after a large sort/write |
| 2 | `memory_limit` defaults to 80% of RAM | RAM-fraction default in the pool builder (start ~70%; DF has more out-of-pool allocation), fixed cap and env override remain | S | Low |
| 3 | One budget covers pool and intermediates | Register footer/metadata/moka caches as `MemoryPool` consumers | M | Medium; eviction-under-pressure wiring must be right or OOM becomes thrash |
| 4 | `duckdb_memory()` / `duckdb_temporary_files()` introspection | Per-consumer pool usage + spill files via sqe-metrics / information_schema (extends the phase-0 observer) | S | Low |
| 5 | Sort spills merge runs page-by-page | Track/vendor DataFusion #16132 / #14692 / #14748; do not build | L | Medium-High |
| 6 | Radix-partitioned grace hash join | Track HashJoinExec spill #12952 / #17267; SF100-critical candidate for upstream contribution | L | High |

## Sources

- DuckDB, "Memory Management in DuckDB" (2024-07-09):
  https://duckdb.org/2024/07/09/memory-management
- DuckDB, "External Aggregation" (2024-03-29):
  https://duckdb.org/2024/03/29/external-aggregation
- DuckDB, "Fastest Table Sort in the West" (2021-08-27):
  https://duckdb.org/2021/08/27/external-sorting
- DuckDB, "Redesigning DuckDB's Sort, Again" (2025-09-24):
  https://duckdb.org/2025/09/24/sorting-again
- Kuiper et al., ICDE 2024 out-of-core aggregation:
  https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf
- Kuiper, Muehleisen, ICDE 2023 sorting:
  https://duckdb.org/pdf/ICDE2023-kuiper-muehleisen-sorting.pdf
- "Saving Private Hash Join", VLDB 2025:
  https://www.vldb.org/pvldb/vol18/p2748-kuiper.pdf
- DuckDB jemalloc internals: https://duckdb.org/docs/current/internals/jemalloc
- DuckDB discussions #11455 (opt.retain), #10350 (glibc ptmalloc)
- jemalloc issues #2688, #2751
- DataFusion 54.0.0 release notes (2026-06-12)
- DataFusion issues #1568, #16132, #17334, #14692, #14748, #1599,
  #12952, #17267, #13123
