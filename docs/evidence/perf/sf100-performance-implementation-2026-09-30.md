# SF100 performance implementation evidence

Date: 2026-09-30. Local changes on top of `29aa2b0d189bf7ceb5def3eb9289e27c57bafe88`;
published for review in the merge requests below; not merged or deployed. This is implementation evidence and small local
experiments, not SF100 qualification.

## Merge requests and issue scope

| Merge request | Issue scope |
|---|---|
| [!1220](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/1220) | Closes #780 on merge: retained buffered-result memory. |
| [!1221](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/1221) | Closes #782 on merge: bounded JSON blocking work and cancellation. |
| [!1222](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/1222) | Implements streaming for #783; full-scale RSS and live Iceberg checks remain open. |
| [!1223](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/1223) | Advances #785; larger-scale and numeric qualification remain open. |

The experiment script and raw JSON measurement file are supplied by !1223.
Merge that MR before, or together with, this evidence document. Issues #779 and
#781 (authentication) are not addressed. Issues #784, #786, #787, #788 and #416
remain qualification or implementation follow-ups; #380, #382, #407 and #755
are not resolved by these changes.

## Changes

- **Lazy generation (#783):** SSB scalable dimensions and lineorder, every TPC-DS
  table, and both TPCBB tables yield one batch at a time. Stateful RNG, baskets,
  multiline orders and sessions persist across batches. The fixed 2,557-row SSB
  date dimension remains a small eager table. These generators retain one serial
  shard to preserve the existing draw sequence; this does not introduce parallel
  row generation. Staged writes now honor compression and row-group configuration.
- **Direct Iceberg sink:** the four-batch producer queue now feeds the writer
  directly. The previous receiver collected a whole shard into a vector before
  writing. A write error drops the receiver and allows the blocking producer to
  exit; cancellation also drops the receiver. Writer/upload buffers and returned
  file metadata remain additional bounded-by-writer or file-count costs.
- **Buffered results (#780):** actual Arrow batch memory is reserved in shared and
  distinct query pools, with a cumulative `query.max_query_memory` cap. The
  configured row cap is also enforced. Reservations follow immutable buffers
  through clones, slices, nested/view arrays, ArrayData conversion and cache
  retention, with no payload copy. Cap and pool failures use ResourceExhausted.
  A single upstream batch has already been allocated before result admission;
  this does not replace operator-level allocation accounting. Shared buffers may
  be conservatively charged more than once. Cached buffers keep their pool alive
  until released, so retained results can reduce new-query admission headroom.
- **Buffered JSON (#782):** decompression, array reshaping, schema inference and
  Arrow decoding run on the blocking executor with at most two active jobs.
  Permits stay held until blocking work exits. Dropping the async waiter signals
  cooperative cancellation, checked at decompression chunks, JSON elements and
  inference/decoder reads. Cancellation is not instantaneous inside one unusually
  large JSON element or decoder operation. Existing byte caps/reservations remain.
- **TPCBB q01 (#785):** a separate pre-aggregation SQL variant avoids the
  many-to-many fact intermediate by combining per-item counts and paid sums.
  Weighted AVG uses the non-null paid count. The benchmark's original SQL remains
  the comparison baseline; no general optimizer rewrite is enabled.

## Verification

- Benchmark library: **223 passed**, including pre-migration Arrow IPC value
  fingerprints for all 31 SSB/TPC-DS/TPCBB tables at SF0.01, staged Parquet round
  trips and compression/row-group checks, and lazy SF100 prefix generation.
- Coordinator library at the implementation checkpoint: **754 passed, 1 ignored**.
  Additional focused tests pass for the TPCBB query and all **5** buffered-result
  lifetime/admission cases, including nested/view arrays and concurrent results.
- JSON and buffered JSON tests: **37 passed**, including current-thread Tokio
  responsiveness, cancellation cleanup, decompression limits and malformed input.
- `cargo check -p sqe-catalog -p sqe-coordinator --tests` and `git diff --check`
  passed at their respective checkpoints.

The configured local LLVM Mach-O linker failed to resolve macOS system symbols.
Tests used a temporary RUSTC_WRAPPER that selected Apple clang/ld with the active
SDK for the relevant final links. Repository linker configuration was unchanged.

## Generation memory observations

Same command shape, debug profile, one generation thread:

```sh
sqe-bench generate ssb --scale 0.1 --threads 1 --output <temporary-directory>
```

| Metric | Existing eager binary | Streaming implementation |
|---|---|---|
| Peak RSS | 142,688,256 bytes (136.1 MiB) | 71,155,712 bytes (67.9 MiB) |
| Wall time | 4.04 s | 5.78 s |
| User CPU time | 3.73 s | 3.61 s |

All five staged Parquet files were **byte-identical** by SHA-256. Peak RSS fell
about 50%. This single local run does not establish a latency improvement; the
streaming run's wall time was higher. Writer row-group/upload buffering still
needs to be included when sizing a real load.

A test constructing all 31 sources at SF100 and consuming at most two batches
from each completed in 2.21 s with **31,752,192 bytes (30.3 MiB) peak RSS**.
This establishes lazy source construction and early termination, not the cost or
correctness of loading every SF100 row.

**Dataset sizing warning:** current TPC-DS inventory uses
`261 * item_rows(scale) * warehouse_rows(scale) / 2`. Both dimensions scale
linearly in this generator. At SF100 it requests **117,450,000,000 inventory rows**,
versus 1,174,500,000 at SF10. This synthetic formula scales quadratically and needs
an explicit fidelity/sizing decision before a complete SF100 load. The streaming
change preserves it; it does not claim official TPC-DS cardinalities.

## TPCBB experiment

Run from the repository root (requires Python duckdb and an existing sqe-cli):

```sh
python3 benchmarks/experiments/tpcbb-preaggregation.py
```

Fixture: 50,000 rows per fact, 1,000 item keys, nullable sale identifiers and paid
values; local Parquet, embedded SQE with a 512 MiB pool. The script compares all
750 grouped results, with a 1e-9 relative / 1e-8 absolute tolerance for floating
aggregates, before measuring. It counterbalances three runs per query and includes
process startup. The engine unit test additionally covers duplicate dimension keys,
groups spanning item keys, all-null paid groups, null keys and unmatched rows.

Measured medians: **236.43 ms original, 58.02 ms pre-aggregated (4.08x)**.
[Raw measurements](tpcbb-preaggregation-2026-09-30.json).

The original query failed with a clean hash-build resource error on an earlier
200,000-row-per-fact fixture at the same 512 MiB limit. The timed comparison used
50,000 rows so both queries completed. Neither fixture is a benchmark scale-factor
run. Parallel floating accumulation can alter near-tie ordering; compare complete
groups rather than requiring identical identities at an arbitrary LIMIT tie boundary.

## Remaining qualification

Use the [readiness plan](sf100-scaling-risks.md) for SF10/SF30/SF100 on separate
Linux hosts. Unknown join-size handling, scan fetch-ahead, TopK/layout and concurrent
spill defaults need current-scale evidence. No full SF100 load, live REST-catalog
load test, or distributed throughput qualification was performed here.
