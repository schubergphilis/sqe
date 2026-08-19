# Where SSB SF10 time is not going: three measured negatives

Date: 2026-08-19. Rig: native local coordinator (`worker_urls = []`), SSB SF10,
`caffeinate` held so no arm resumes from a suspended host.

Context: worker row-group pruning (!879) took SSB SF10 distributed from 44.8 s to
39.2 s, and the follow-up showed it is inert on TPC-H SF10 and at SF1. Scan
pruning looked close to exhausted, so the question became where the remaining
time actually goes. Three candidates were measured. All three are negatives, and
the last one explains the other two.

## The starting anomaly

q4.1's operator profile attributes about **1.25 s of a 6.9 s execute phase**:

| ms | operator |
|---|---|
| 6,886 | `[phase] execute` |
| 331 | HashJoinExec |
| 313 | IcebergScanExec |
| 224 | HashJoinExec |
| 128 | RepartitionExec |
| 80 + 80 | HashJoinExec x2 |
| 37 | AggregateExec |

About 82% is unaccounted for. Two plausible explanations were tested.

## Negative 1: it is not object-store latency

The coordinator's scan path pipelines fetch and decode
(`catalog.scan_fetch_ahead`, default 3x CPU count); the worker path has only a
depth-1 next-file prefetch. If the gap were I/O latency, deepening the pipeline
would help and the worker would be the obvious place to add it.

Interleaved, two reps, `scan_fetch_ahead = 0` versus the default:

| arm | suite (ms) | mean |
|---|---|---|
| staging OFF (`= 0`) | 52,808 / 48,214 | **50,511** |
| staging ON (default) | 55,456 / 56,398 | 55,927 |

**Staging is 10.7% SLOWER**, consistently, with non-overlapping ranges. On a
loopback object store there is no latency to hide, so deeper pipelining buys
buffering and pool pressure and nothing else. That kills the plan to give the
worker fetch staging, and it says the missing time is not I/O wait on this rig.

## Negative 2: it is not runtime expression rebuilding

A 20-second CPU sample (macOS `sample`, 30,056 frames) while q4.1 ran put
expression-tree work at the largest identifiable non-allocator family, ahead of
parquet decode, zstd and the hash join:

| frames | symbol family |
|---|---|
| ~1,026 | `TreeNode::transform_up` |
| ~700 | allocator (`_xzm_free`, `malloc_tiny`) |
| ~657 | parquet `cached_array_reader` |
| ~617 | `iceberg::PredicateConverter::visit` |
| ~486 | mutex wait |
| ~382 | ZSTD decompress |
| ~295 | HashJoinStream |

The stack named the caller exactly:

```
iceberg_datafusion::physical_to_predicate::convert_physical
  -> DynamicFilterPhysicalExpr::current()
    -> TreeNode::transform_up (remap_children)
```

`DynamicPredicate::current()` is sampled once per file scan task, and
`RuntimeFiltersDynamicPredicate::current()` re-runs the whole conversion every
call, rebuilding a `CASE hash % N WHEN i THEN <IN-list>` carrying tens of
thousands of literals. That is the same defect !371 fixed for the Tier-2
per-batch wrapper, one level out and uncached, and #131's intra-file splitting
multiplies how many tasks pay it.

A caching wrapper was written (per filter slot, so a slot sealing later still
strengthens the predicate; `iceberg::expr::DynamicPredicate` is public so no
vendor patch was needed) and measured:

| arm | suite mean | q4.1 mean |
|---|---|---|
| cache OFF | 44,382 ms | 8,251 ms |
| cache ON | 43,943 ms (-1.0%) | 8,108 ms (-1.7%) |

Both inside the arm spreads. Suspecting the win was hidden by spare cores, it was
re-measured under six-way concurrency, where wasted CPU cannot hide:

| arm | batch wall mean |
|---|---|
| cache OFF | 60,865 ms |
| cache ON | 61,683 ms (**+1.3%**) |

Null again, marginally worse. **The wrapper was not shipped.** The work is real
and redundant, but it is not on the critical path: it runs in spawned tasks
across 11 cores while the query waits on something else.

## Negative 3, which explains the first two: the profile's 82% is an accounting gap, not a mystery

Operator `elapsed_ms` is exclusive compute measured on the polling thread. The
scan deliberately moves its expensive work OFF that thread:

- the Tier-2 membership filter runs on `spawn_blocking` with `buffer_unordered`,
  precisely so it scales across cores;
- fetch and decode run as staged subtasks under their own admission;
- Tier-1 predicate sampling happens per file scan task inside the reader.

None of that is attributed to any operator, by design. So the 82% is not
unexplained time waiting to be reclaimed by an optimization; it is CPU already
running in parallel, which is why removing a chunk of it (negative 2) changed
nothing measurable.

## What this licenses

- **Do not add fetch staging to the worker** on the strength of "the worker lacks
  what the coordinator has". Measured, that lever is negative here, and it would
  need a high-latency object store to pay.
- **Do not chase the operator profile's unattributed time** as if it were idle.
  Read it as "work happening in spawned tasks", and use a CPU sample plus a
  concurrency A/B when the question is whether some CPU is on the critical path.
- The next lever for SSB SF10 is not scan CPU. Both remaining candidates from the
  audit are elsewhere: byte-bounded, spillable shuffle (#406) and the
  separate-host qualification (#416), which !878 unblocked.

## One thing worth fixing separately

Under six-way concurrency every query logs `pool residue after query completion`
at WARN, listing the other queries' live `iceberg-scan-decode` reservations
(~120 MB each). That is not a leak, and `memory.rs` documents the intent: the
64 MiB threshold is meant to "keep the report quiet during normal concurrent
operation". It does not: five concurrent scans clear it easily. The report is
useful on a sequential sweep and pure noise under concurrency, so it wants an
in-flight-query check rather than a byte threshold.
