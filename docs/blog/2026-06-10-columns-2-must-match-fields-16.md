---
title: "columns(2) must match fields(16)"
description: "Differential testing against Trino caught a distributed-scan bug the whole test suite missed: every projected query failed once a scan actually distributed. The first fix restored correctness by disabling projection pushdown. Then a Claude agent on the Fable model found the real bug, one line in the worker's streaming rewrite, and got the speed back: 3.1x overall, 10x on the worst query."
pubDate: "2026-06-10"
author: "Jacob Verhoeks"
tags:
  - "distributed"
  - "testing"
  - "performance"
  - "debugging"
  - "ai-agents"
---

*June 10, 2026*

## Another engine as the oracle

We had just merged a stack of audit fixes into the distributed scan path: HMAC-signed scan tickets, predicate and LIMIT pushdown to workers, streaming reads. Unit tests green. Integration tests green. For a query engine, that is not enough. Your tests encode your own assumptions, and the bug you shipped lives inside exactly those assumptions.

So we ran the differential harness. `sqe-bench compare` executes the same TPC-H queries against SQE and Trino 465, both pointed at the same Polaris catalog and the same parquet files on S3, and diffs the results row by row with a float epsilon. Trino does not share our assumptions. That is the point.

One catch at SF0.1: every table lands as a single parquet file, which sits below the distribution thresholds. The coordinator quietly runs everything local, and the new code never executes. Gates that decide whether code runs are also gates that decide whether code gets tested. We zeroed the thresholds and forced every scan through a worker.

## 0 of 22

Every query failed. Same error, every time:

```
Invalid argument error: number of columns(2) must match number of fields(16) in schema
```

The control experiments told us where to look. `SELECT * FROM lineitem WHERE l_shipdate <= ...` distributed fine: 572,112 rows, identical to Trino. `SELECT l_orderkey, l_extendedprice FROM lineitem WHERE ...` failed with columns(2) vs fields(16). Predicates pushed down correctly. LIMIT pushed down correctly. Streaming worked. Projection was broken, and only projection.

The bug had been on main for almost a month. Nothing in CI caught it because CI data never crosses the distribution gate. A latent bug behind an untested gate is still a production bug; it just has not met production yet.

## The tourniquet

The first fix ([!327](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/327)) was deliberately boring: stop pushing projection to workers. Workers return full rows, the coordinator narrows them by name, and the reassembly path got extracted into a function with regression tests pinning the contract. Parity went from 0/22 to 22/22 against Trino. Shipped.

Correct, but at a cost we named in the MR. A query that needs two columns of `lineitem` now reads all sixteen from S3 and ships all sixteen over Flight. We then ran the harness at SF1 and saw it: 22/22 matched, but the scan-heavy queries ran around 5x slower than Trino.

The review question that mattered came from looking at history, not at code: was it faster before? Yes. The April distributed baseline was 22/22 at 12 seconds with projection pushdown working. So between April and June something broke the pushdown, queries started erroring instead of pruning, and our tourniquet had quietly traded the speed away to get correctness back. The floor was in. The fix was still missing.

## Handing it to Fable

We gave the second pass to a Claude Code agent running on the Fable model, in an isolated worktree, with one instruction that mattered more than the rest: if you cannot make it both correct and faster, do not force it. Report what you found and leave the safe behavior in place.

The handoff brief included my root-cause hypothesis: the coordinator builds `DistributedScanExec` with the full table schema while telling the worker to project, so the two sides disagree by construction. The agent's first deliverable was proving me wrong. `IcebergScanExec` builds its projected schema and its projection list together, in the same constructor. The coordinator side was consistent all along.

Then it went where the evidence pointed instead of where I had pointed. The May streaming rewrite of the worker's parquet reader contained this:

```rust
let schema = builder.schema().clone();
```

`ParquetRecordBatchStreamBuilder::schema()` returns the full parquet file schema. Always. The `ProjectionMask` only applies to the stream you get after `build()`. So the worker handed the Flight encoder a 16-field schema and then shipped 2-column batches through it. The coordinator's Flight decode rejected the mismatch before any of our reassembly code ran, which is why the error never pointed at the code that owned the contract.

The old buffering path had read the schema from `batches[0].schema()`, the actual data. That is why April worked. The rewrite swapped the source of truth from the data to the builder, one line, and nothing in the worker's tests compared the advertised schema against the emitted batches.

The fix ([!329](https://sbp.gitlab.schubergphilis.com/vpf-data-ai/chameleon/applications/sqlengine/-/merge_requests/329)):

```rust
let schema = stream.schema().clone();
```

Plus the parts that make it stick: projection re-enabled in the coordinator's scan tasks, the reassembly hardened for column-order and rename cases, and a regression test that asserts the returned schema matches the emitted batches. The agent ran that test against the old code first and watched it fail. Red on the bug, green on the fix. That discipline is what makes an agent's claim of "fixed" worth something.

## The numbers

Same harness, same forced-distribution setup, A/B against an image built from main. Median of three runs, TPC-H SF0.1, single worker:

| Query | Full columns (!327) | Pushdown restored (!329) | Speedup |
|-------|--------------------:|-------------------------:|--------:|
| q14   | 287ms               | 28ms                     | 10.3x   |
| q06   | 280ms               | 31ms                     | 9.0x    |
| q19   | 299ms               | 53ms                     | 5.6x    |
| q15   | 321ms               | 76ms                     | 4.2x    |
| q17   | 312ms               | 85ms                     | 3.7x    |
| q01   | 301ms               | 142ms                    | 2.1x    |
| **Total (22)** | **4794ms** | **1567ms**               | **3.1x** |

Parity stayed 22/22 on every run. The scan-heavy six went from 1800ms to 415ms. Reading two columns instead of sixteen is not a subtle optimization, and the harness makes the price of losing it visible in a way no unit test did.

## What I keep

Differential testing is the cheapest oracle we own. Trino disagreed with us 22 times in a row, and every disagreement was our bug. A second engine on the same data turns "all tests pass" into a claim someone else gets to veto.

Fix correctness and performance in that order, in separate MRs. The tourniquet stopped wrong results the same day. The real fix took the time a real root cause takes, and the floor underneath it meant nobody was waiting on it with broken queries.

State your hypothesis in the handoff, and let the agent kill it. Mine was wrong in a useful way: written down, specific, checkable. The agent checked it, said no, and followed the error to a line neither of us was looking at. The combination of a falsifiable brief, a red-then-green regression test, and a standing instruction to prefer reporting over forcing is what made the result trustworthy enough to merge.

And force your gates open in tests. The distribution threshold protected production from small scans and protected the bug from CI in the same motion. Any gate in front of code is a gate in front of coverage. Zero it somewhere that runs.
