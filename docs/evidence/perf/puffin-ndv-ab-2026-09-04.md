# Puffin NDV statistics A/B (matrix task 7.14)

Date: 2026-09-04. Branch `feat/puffin-ndv-statistics`. Profile `dev-release`,
local Polaris + RustFS stack on a shared laptop. All arms load identical data
with `--clean`; the flag arms add `write.puffin.stats = 'true'` at load and
start the coordinator with the corresponding `SQE_QUERY__*` env override.

Three arms:

| Arm | `query.puffin_statistics` | `query.statistics_registry` |
|---|---|---|
| off | false | false |
| ndv | true | false |
| on | true | true |

Plans per query for every arm are in `puffin-ndv/` (`sf0.1-*`, `sf1-*`).
Bench reports are the `tpch-sf1-flight-2026-09-04T12:4*` and `12:5*` files in
`benchmarks/results/`.

## Correctness

SF0.1 Trino compare with the `on` arm: TPC-H 22/22 matched, SSB 11/13 matched
(the two misses are the known vacuous pair, identical to the baseline sweep
earlier the same day). Every SF1 run passed the harness's expected-result
checks in all three arms.

## Sidecar coverage

After the streaming-path fix every CTAS writes one blob per column. Orders at
SF0.1:

| column | ndv | true |
|---|---|---|
| o_orderkey | 148715 | 150000 |
| o_custkey | 10044 | 9999 |
| o_orderstatus | 3 | 3 |
| o_orderdate | 2435 | 2406 |
| o_clerk | 1000 | 1000 |
| o_shippriority | 1 | 1 |

Before the fix the streaming handlers never emitted a sidecar, Decimal128 and
Date32 columns carried `ndv = 0`, and string columns (read as `Utf8View`) had
no blob.

## Plan changes at SF1 (nine TPC-H queries explained)

| query | ndv vs off | on vs off |
|---|---|---|
| q01 | identical | identical |
| q03 | identical | build side flips to lineitem; join goes Partitioned |
| q05 | identical | build side flips to lineitem |
| q07 | identical | identical |
| q08 | identical | identical |
| q09 | identical | identical (flipped at SF0.1 only) |
| q17 | LeftSemi becomes RightSemi with the lineitem aggregate as build | same, plus Partitioned mode |
| q18 | identical | build side flips to lineitem |
| q21 | identical | anti/semi self-joins flip to RightAnti/RightSemi |

In every `on` flip the fact table or its aggregate becomes the hash build and
the dynamic filter reverses direction (the orders/part scan gains a
`pushed_down_filters` entry, lineitem loses one). That is the opposite of the
star-schema shape SQE's `dim_build_swap` and `star_schema_reorder` rules
produce on purpose.

## Runtime at SF1

Noise on this box is large: the same `off` arm measured 21.8s, 19.7s and
16.1s for TPC-H across the afternoon, and join-free q01/q06 move by 20 to 40
percent between arms. Only differences that repeat across counterbalanced
rounds and coincide with a plan change count.

`on` vs `off`, two rounds (off-on then on-off), TPC-H means:

| query | off mean (ms) | on mean (ms) | ratio | plan changed |
|---|---|---|---|---|
| q03 | 774 | 956 | 1.23 | yes |
| q05 | 970 | 1192 | 1.23 | yes |
| q17 | 1732 | 2428 | 1.40 | yes |
| q18 | 1562 | 1804 | 1.16 | yes |
| q21 | 2243 | 2304 | 1.03 | yes |
| suite | 20769 | 22683 | 1.09 | |

q17 was slower in both rounds (2068 to 3201, 1397 to 1655). SSB showed no
consistent direction.

`ndv` vs `off`, fresh back-to-back pair: TPC-H 16.1s to 14.6s, SSB 5.1s to
4.7s. The gain is spread evenly across queries whose plans did not change
(q06 0.58x), so it is run-order noise, not the statistics. q17, the one query
whose plan did change, measured 953 to 1003 ms.

## Verdict

- `query.statistics_registry` stays off. DataFusion 55's built-in provider
  chain, fed with real distinct counts, moves TPC-H build sides onto the fact
  table and reverses runtime-filter direction. That is a costing disagreement
  with SQE's post-planning join rules, not a statistics-quality problem.
- `query.puffin_statistics` alone is plan-neutral except q17 and
  runtime-neutral within this box's noise. It is safe to leave on for further
  experiments but earns nothing yet, because stock DataFusion only consumes
  `distinct_count` through the registry or in join-cardinality estimates that
  SQE's own rules already override.
- `write.puffin.stats` default stays off until a consumer pays for the
  sidecar write.

## Next

1. Repeat `on` vs `off` on the clean rig (see `project_clean_rig_sf1_baseline`)
   to confirm the q03/q05/q17 direction with less noise.
2. Decide whether to teach `dim_build_swap` / `star_schema_reorder` to read
   `distinct_count`, or to register an SQE `StatisticsProvider` ahead of the
   built-in join provider, before enabling the registry anywhere.
3. Peak memory was not captured; the harness reports wall time only.
