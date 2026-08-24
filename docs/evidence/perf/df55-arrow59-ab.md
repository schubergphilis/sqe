# DataFusion 55 / arrow 59: A/B against main

Branch `chore/df55-vendor-iceberg` vs `main` at `d5a45be`, which is the exact
commit the branch is rebased onto. SF1, `dev-release`, Flight, single node.
Raw reports in `df55-arrow59-ab/`.

`dev-release` on purpose: this is a relative A/B, and both arms use it. These
numbers are NOT release-profile baselines and must not be filed as such.

## Correctness: unchanged

Across six sweeps (tpch x4, ssb, tpcds) every query passes on both arms and
**every row count is identical**. Not one mismatch in 22 + 13 + 99 queries.

Trino parity on the branch, clean run: **22/22 matched**, zero vacuous, zero
dialect-diff.

## Performance

tpch was run ABBA-counterbalanced because the box degrades within a session.

| run | arm | total |
|---|---|---|
| pos 1 | DF55 | 15006 ms |
| pos 2 | DF54 | 18064 ms |
| pos 3 | DF54 | 22125 ms |
| pos 4 | DF55 | 14609 ms |

Mean DF54 20094 ms, mean DF55 14808 ms, **-26.3%**. The counterbalancing is
what makes this readable: DF55 held positions 1 and 4, so if session decay were
driving the result its last run would be the slowest. It is the fastest. DF54
also swings 4061 ms between two runs of identical code while DF55 swings 397 ms,
so the new stack is markedly steadier as well.

Single-run, not counterbalanced, so read the direction and not the magnitude:

| suite | DF54 | DF55 | delta | slower queries |
|---|---|---|---|---|
| ssb | 7750 ms | 4410 ms | -43.1% | none |
| tpcds | 65824 ms | 35982 ms | -45.3% | none |

The "slower queries" column is the one that matters for this branch. The
statistics migration (`partition_statistics` -> `statistics_from_inputs`) feeds
`JoinStrategyRule` and `star_schema_reorder`, so a mistake there shows up as
individual queries flipping to a bad plan, not as a uniform shift. Across 99
tpcds queries, zero regressed by more than 30% / 150 ms.

## Two rig artifacts, recorded so they are not re-discovered

**The comparison phase starves and reports zeros.** The first parity attempts
produced nonsense: `main` matched 9/22, and from q12 on both engines returned
0 rows with 39-50 s timings. No error was logged. Cause: `sqlengine-rustfs-1`
had grown to 7.8 GiB of Docker's 16.55 GiB across six data loads, an unrelated
`ai-accel-platform` compose stack held ~1.7 GiB, and Trino then asks for 8 GiB.
It does not fit, so Trino degrades silently. `docker compose -f
docker-compose.test.yml down -v` between parity runs fixes it.

The same starvation invented two fake findings worth naming, because both look
exactly like real regressions:

| query | starved | clean |
|---|---|---|
| q21 | 27812 ms | 2389 ms |
| q22 | 31475 ms, 0/0 rows | 672 ms, 7/7 rows |

q21 is a query with a genuine history of being slow, which is precisely why the
artifact was convincing. Check `docker stats` before believing a tail-of-sweep
regression.

**SQE-vs-Trino timing on macOS is not a fair comparison** and is not claimed
here. SQE runs native while Trino runs in-VM, and SQE's object-store traffic
crosses the Docker proxy. The A/B above is unaffected: both arms are SQE on the
same rig.
