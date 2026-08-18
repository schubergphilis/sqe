# Coordinator-to-worker transport: receive windows and ticket size

Date: 2026-08-18. Branch: `perf/ticket-size-432`. Issues: #432, follow-on to
#414/#415.

Two defects on the same link, found by reading the dispatch path rather than by
running a benchmark. Both are measured here; neither is visible on a loopback
rig, which is why the earlier SF1 and SF10 A/B work did not surface them.

## 1. The worker channels ran on h2's 64 KB windows

`ChannelPool::get` built every coordinator-to-worker channel with a bare
`Endpoint::new(...)`. tonic leaves `init_stream_window_size` and
`init_connection_window_size` as `None`, so h2 falls back to 65,535 bytes for
each. The coordinator's own Flight SQL *server* was lifted to 8 MB / 16 MB long
ago, with a comment saying tonic's defaults "cap Flight SQL DoGet throughput on
multi-GB result sets". The same reasoning was never applied to the link that
carries every distributed scan result.

A receive window caps one stream at `window / RTT` regardless of link
bandwidth:

| RTT | 64 KB window | 8 MB window |
|---|---|---|
| 0.05 ms (loopback) | ~1.3 GB/s | not the limit |
| 1 ms (LAN / same rack) | ~64 MB/s | ~8 GB/s |
| 10 ms (cross-AZ) | ~6.4 MB/s | ~800 MB/s |

The connection window is worse than the per-stream number suggests: it is
shared, so every scan fragment multiplexed onto one pooled channel to a worker
drew from the same 64 KB.

This is why the number is a calculation and not a measurement here. Every rig
in this repo's history runs coordinator and workers on one host, where even
64 KB clears 1 GB/s. Confirming it needs the separate-host rig from #416.

## 2. The ticket predicate rode the wire as a JSON number array

`ScanTask` is a JSON document and `predicate_proto` was a `Vec<u8>`, which
serde_json writes as `[13,255,7,...]`. Measured expansion on real serialized
`Expr` payloads (an `InList` of k `Int64` literals on `lo_partkey`, the shape
SF10 SSB produces):

| k | raw proto | JSON ticket | expansion |
|---|---|---|---|
| 1,000 | 6 KiB | 18 KiB | 2.78x |
| 10,000 | 68 KiB | 194 KiB | 2.85x |
| 50,000 | 374 KiB | 1,077 KiB | 2.88x |
| 100,000 | 765 KiB | 2,200 KiB | 2.88x |
| 200,000 | 1,546 KiB | 4,481 KiB | 2.90x |
| 500,000 | 3,890 KiB | 11,513 KiB | 2.96x |

A single ticket crosses tonic's 4 MiB decode ceiling at about **180,000
distinct keys**. That settles which message was refused during the 2026-08-18
SF10 run: one ticket is a sufficient explanation, no fan-out multiplier needed.

It also condemns the guard shipped in !875. `MAX_PUSHED_PREDICATE_BYTES` was
3 MiB measured on *raw* proto bytes, which at 2.9x is an ~8.9 MB ticket, and
9.1 MB is exactly what the workers were observed refusing. The guard admitted
the failing case it was written to prevent.

base64 costs 1.34x instead of 2.9x, a 2.2x reduction in ticket bytes, and the
budget is now derived from the wire ceiling (`max_message_bytes / 4 * 3`, the
exact inverse of base64's expansion) rather than guessed.

## The refusal, reproduced and then fixed

Native two-worker rig (no docker image), SSB SF10 on the already-loaded test
stack, distribution forced (`distribution_threshold = "1MB"`,
`distribution_file_threshold = 1`), `runtime_filter_inlist_max_values` raised to
1,000,000. Same rig, same config, same data; only the binaries differ.

| | pre-fix | fixed |
|---|---|---|
| suite | 12 pass, **1 error** | **13 pass, 0 error** |
| queries distributed | 12 (1 fell back to local) | 13 |
| filters pushed | 204 | 221 |
| worker refusals | **22** | 0 |
| tickets shed / over budget | n/a | 0 / 0 |
| q4.1 | error after 606 ms, 0 rows | pass, 6,352 ms, 35 rows |
| suite total | 34,722 ms | 40,549 ms |

The refused message was **9,145,356 bytes against a 4,194,304 limit**, and the
size is the tell: 9,145,356 / 3,145,728 = 2.907, so the ticket carried a
predicate sitting exactly on the old 3 MiB raw budget. The guard passed it, and
the wire expanded it past the ceiling. That is the miscalibration, measured.

**Which of the three changes actually fixed it.** Not the encoded-ticket guard:
`ticket_predicate_dropped_oversize` is 0, so it never fired. base64 took the
same predicate from 9,145,356 bytes to about 4.2 MB, and 3,145,728 * 4/3 =
4,194,304 exactly, so base64 alone lands the predicate ON tonic's old 4 MiB
default and the rest of the ticket puts it over. The raised ceiling is
load-bearing here too. The guard is insurance that has not yet fired in a real
run, and it should be described that way.

The suite total went **up**, and the reason is not a regression: q4.1 went from
erroring at 606 ms to running for 6,352 ms, which is +5.75 s of the +5.83 s
total. Every other query is flat inside rep-to-rep noise. A failed query costs
almost nothing on the clock, so a suite total that includes one is not
comparable to a suite total that does not. This pair is a correctness result
(the query now returns its 35 rows), not a speed result.

Neither run belongs in `benchmarks/results/` as a baseline. Both use a forced
distribution threshold and a non-default InList cap, so they measure this defect
and nothing else.

## What this licenses

- Quoting "coordinator-to-worker throughput is no longer window-capped" only
  after a separate-host run. The change removes a ceiling; it does not prove a
  speedup on any rig in this repo.
- Raising `runtime_filter_inlist_max_values` far enough that SF10 SSB partkey
  membership survives (the #415 drop), because the 4 MiB ceiling is no longer
  the binding constraint. Whether that *helps* is still unmeasured: the arm
  above pushes 221 filters and drops 0 conjuncts, but its total is not
  comparable to the shipped-cap arm.

## What it does not

- It is not the #415 fix. Membership sets large enough to matter still ship
  hundreds of KB per ticket, duplicated across every fragment. A bloom summary
  built where the build side materializes is the bounded answer, and it stays
  open.
- The `codec.rs` `EncodedDistributedScan` path duplicates the predicate and the
  S3 credentials once per task, so a 100-task fragment with a k=10,000
  predicate encodes to 19 MB. It is left alone deliberately:
  `SqePhysicalCodec` has no caller outside its own tests, so hoisting the
  shared fields would be optimizing dead code. Fix it when a real plan-fragment
  path starts using it.

## Follow-on: now that the big membership set CAN cross, should it?

The transport fix makes an obvious next move available: raise
`runtime_filter_inlist_max_values` so the SF10 SSB partkey membership set
survives instead of being dropped (#415). Measured, and the answer is no.

Three arms, native two-worker rig, SSB SF10, distribution forced, interleaved,
all on the fixed binaries. Arm B is only runnable at all because of the fix
above; before it, q4.1 errored.

| arm | cap | dropped conjuncts | suite mean |
|---|---|---|---|
| A | 65,536 (shipped) | 34 | 46,929 ms |
| B | 1,000,000 | 0 | 54,520 ms (+16.2%) |
| C | 1,000,000, `distributed_scan_pushdown = false` | 0 | 45,706 ms (-2.6%) |

Per query (means), the cost lands exactly where the membership set is largest:

| q | A | B | C | B-A | C-A |
|---|---|---|---|---|---|
| q4.1 | 6,446 | 9,496 | 7,288 | **+3,050** | +842 |
| q4.2 | 4,322 | 5,840 | 5,006 | **+1,518** | +684 |
| q2.3 | 4,742 | 5,482 | 3,874 | +739 | -868 |
| q3.2 | 3,861 | 4,526 | 3,405 | +664 | -456 |
| q3.3 | 3,854 | 4,422 | 3,404 | +569 | -450 |
| q1.1 | 2,077 | 1,754 | 2,151 | -323 | +74 |

Arm C is the discriminator: it materializes the same large `InList` on the
coordinator but ships nothing. C tracks A (slightly better), so **coordinator-side
materialization is close to free and the +16% is transport plus worker-side
evaluation**. Raising the cap alone, without pushing, costs nothing.

### The filter works. It just does not pay.

Worker-side counters, scraped from each worker's `/metrics` before teardown,
because the coordinator's `sqe.scan.*` fields only observe its own local dim
scans (82 MB / 72 files / 2.4M rows, byte-identical in every arm) and say
nothing about the workers:

| arm | worker rows scanned | worker bytes read |
|---|---|---|
| A (34 dropped) | 6,968,724 | 110,378,809,632 |
| B (0 dropped) | 5,100,172 (**-26.8%**) | 110,378,809,632 (identical) |

So the membership set does prune: a quarter of the rows the workers would
otherwise emit. And it prunes **rows only, never bytes** - the fetch and decode
volume is identical to the byte, because a runtime `InList` arrives as a
`RowFilter` after the row groups have already been chosen. Paying full I/O and
full decode to emit 27% fewer rows, while evaluating a ~200K-value `InList`
rebuilt per `FileScanTask`, is a net loss of 16%.

### What this means for #415

The issue's premise inverts. The dropped `hash_lookup` conjunct is not a missed
optimization worth transporting; transporting it, exactly and correctly, makes
SF10 SSB slower. Two things have to be true together for it to pay, and a bloom
only delivers the first:

1. cheap to represent and evaluate (a bloom: ~150 KB instead of 4.2 MB, and a
   few hashes per row instead of a rebuilt InList), and
2. able to prune at **row-group** level so bytes fall, not just rows - which is
   #369's runtime-filter-to-bloom probe, not this issue.

The prior #369 attempt regressed TPC-H SF10 q21 from 16s to 24s by unioning
CASE-of-InLists into one uncapped non-pruning `RowFilter`, which is the same
failure mode measured here from the other direction. The lever is bytes.

Corollary worth keeping: the shipped cap of 65,536 is doing the right thing at
SF10, and `dynamic_filter_conjuncts_dropped` being non-zero is not by itself a
problem to fix.
