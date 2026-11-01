**STATUS 2026-11-01: Iceberg scan admissions no longer crowd out the query's sorts (`fix/scan-admission-budget`).**

With scan memory tracking on, the engine-bench "latest row per customer"
query (150M rows, Iceberg, 3 GB) spilled its sort 165 times; with tracking
off, 77 times, but at 3.47 GiB RSS, over the cap. The scan's unspillable
reservations were the difference: 33 fetch-ahead subtasks reserved 1x their
projected bytes and 11 decoding subtasks 4x, about 1.9 GB, while a scan with
nothing downstream peaked at 1.2 GiB RSS.

Two changes in `crates/sqe-catalog/src/scan_memory.rs`:

- The vendored streaming reader's decode admission reserves
  `STREAMING_DECODE_FACTOR` (2x) instead of `DECODE_MEMORY_ESTIMATE_FACTOR`
  (4x). It decodes one row group at a time. The direct-read path, which
  holds whole files, keeps 4x and corrects to the decoded size.
- `FetchBudget`: a scan-wide byte bound on the fetch stage, 10% of a finite
  pool (at least 64 MiB), in KiB permits. A fetch admission over it waits
  for the node's own earlier fetches to move to decode; one admission takes
  at most the whole budget. The decode upgrade never waits on it. Off when
  `SQE_SCAN_DECODE_TRACKING=off` or the pool is unbounded. Time spent
  waiting is the scan's `fetch_budget_wait` metric in EXPLAIN ANALYZE.

The trade: over-reserving makes queries spill (slow), under-reserving lets
real memory pass the pool uncounted (OOM risk). 2x comes from this dataset's
compression; tables that decompress far more per projected byte need TPC-H /
TPC-DS confirmation before the MR leaves draft. At 3 GB the budget admits
~12 fetch-ahead subtasks of ~25 MB instead of 33; harmless on local disk,
unmeasured against a remote object store.

Measured on one release binary, local-disk warehouse, host under load:

| | before | after |
|---|---|---|
| q3 sort spills | 165 | 99 |
| q3 peak RSS | 2.52 GiB | 2.23-2.25 GiB |
| scan-only peak RSS | 1.22 GiB | 0.74 GiB |

**NEXT:** measure on the Polaris/RustFS path (scan throughput and
`fetch_budget_wait` against the previous binary, and whether q3 still hits
`ResourcesExhausted` at 3 GB); TPC-H/TPC-DS SF10 against the last baseline.
Then the remaining 99 vs 77 spills is the sort's spill share: DataFusion's
`FairSpillPool` divides by every spillable consumer, including each
`RepartitionExec` partition, so 22 consumers share 3 GB.
