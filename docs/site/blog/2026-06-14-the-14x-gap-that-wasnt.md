---
title: "The 14x gap that wasn't: q95, contention, and the number we almost fixed"
description: "TPC-DS q95 was our worst query: 18 seconds against Trino's 1.3, a 14x loss that justified building a whole optimizer feature. Before we wrote a line of it, we pulled the plan and the profile. The 12-million-row self-join the feature was meant to shrink did not exist, the engine ran the query in under half a second, and the 18 seconds lived only in a benchmark harness running both engines on one starved host. On a clean rig SQE runs q95 in 240ms and beats Trino 12x. A slow benchmark number is a hypothesis until you reproduce it in isolation."
pubDate: "2026-06-14"
author: "Jacob Verhoeks"
tags:
  - "benchmarks"
  - "performance"
  - "datafusion"
  - "testing"
---

*June 14, 2026*

For weeks q95 was the query we apologized for. TPC-DS, scale factor 1, SQE at 18 seconds against Trino's 1.3. A 14x loss, the worst single number in any suite we ran. It had a story attached, and the story was good enough to plan a feature around.

The story: q95 self-joins `web_sales` on `ws_order_number` to find orders shipped from more than one warehouse, then filters that against a narrow outer query and a `NOT IN` over returns. The self-join was supposed to blow up to roughly 12 million intermediate rows and grind through them on one thread. The fix was supposed to be predicate transfer, a Yannakakis-style semi-join reduction that pushes the outer query's selective key set across the subquery boundary so the join only sees the orders that matter. Real engine work. Multiple files, a new optimizer rule, correctness arguments about three-valued logic and nullable anti-joins.

We had the blueprint written. Then we did the one thing that should always come before building: we looked.

## The plan already disagreed

The first move was to pull q95's actual physical plan. Not the plan we assumed, the plan the engine produced.

The 12-million-row self-join was not there. DataFusion had fused it. The `ws_wh` CTE became a single `LeftSemi` join with the warehouse inequality inlined as a join filter. The outer query's filters, the date window and `ca_state = 'IL'` and `web_company_name = 'pri'`, ran first and reduced the left side to 389 rows before the semi-join touched anything. The semi-join then probed the full `web_sales` table once, in about 800 microseconds, and produced at most 389 rows. The anti-join over returns probed 71 thousand rows. Nothing materialized into the millions.

The feature was designed to shrink a join that the optimizer had already shrunk for us. That alone was worth catching before writing the rule.

But it left a sharper question. If the work is microseconds, where do 18 seconds come from?

## The engine never saw 18 seconds

We had a dozen query profiles sitting in old coordinator logs. Every one of them carried an `elapsed_ms` line we had never read. So we read it.

q95 at SF1: between 154 and 477 milliseconds. At SF10, 1.8 seconds. The probe-side `web_sales` scan that one of our own notes had pinned at "15.44 seconds of compute" showed about 100 microseconds. The engine had never recorded a slow q95. Not once, in any log we still had.

The 18 seconds existed in exactly one place: the compare harness. The tool that runs SQE and Trino side by side and diffs their rows.

That is a specific, testable claim. So we tested it.

## Reproduce with the tool that lied

We brought up the distributed rig against the existing SF1 warehouse, coordinator plus a worker, and ran q95 through the bench client. 0.16 to 0.67 seconds, every time. Then the full 99-query sweep: q95 at 0.16 seconds, the whole suite in 16.4 seconds, nothing spiking.

That still was not proof, because the 18-second number came from `compare`, not from a plain test run, and `compare` runs Trino on the same host. So we brought up Trino and ran the exact command that had reported the loss:

```
q95 | SQE 240ms | Trino 2897ms | 12.1x | 1/1 | OK
```

Same tool. Trino co-tenant. q95 in 240 milliseconds, twelve times faster than Trino, not fourteen times slower.

The 18 seconds was contention. The earlier numbers came from long full-suite sweeps with SQE and Trino fighting for the same cores and the same disk on an aged stack, and the heaviest query in the suite took the worst of it. The giveaway was there the whole time and we had talked past it: in that bad run the average query was 573 milliseconds and q95 alone was 17 seconds. Uniform rig decay slows everything. A single 30x outlier is not decay, it is starvation landing on the query with the most work to lose.

## What the clean rig actually shows

Once the rig was up, re-running every loaded suite at SF1 took minutes. The honest numbers, same `compare` tool, one uncontended host:

| Suite | SQE | Trino | Speedup |
|---|---:|---:|---:|
| TPC-H (22) | 8.6s | 20.2s | 3.2x |
| SSB (13) | 4.8s | 6.3s | 2.3x |
| TPC-DS (99) | 21.1s | 72.4s | 3.7x |
| TPC-C (8) | 0.8s | 2.2s | 5.5x |
| TPC-E (11) | 1.9s | 7.2s | 6.8x |

SQE wins every suite, 2.3 to 6.8 times over. The dramatic losses that had been steering our roadmap, q95 worst among them, were the rig. What remains after you remove the noise is small and real: SSB q4.1, q2.1, and q2.2, the part-dimension join queries, still trail Trino by a few hundred milliseconds. That is the one place left where engine work moves the needle. Everything else, SQE already won and the contention was hiding it.

## The lesson, stated plainly

We almost built a multi-file optimizer feature to fix a number that did not exist. The plan was sound, the research was sound, the correctness arguments were sound. The premise was a measurement artifact.

A benchmark that runs two engines on one machine measures the machine as much as the engines. Run them long enough on a stack that has been up for half a day and the scheduler, the page cache, and the memory pool will write a story into your numbers that has nothing to do with your code. We wrote one of those stories down and believed it for weeks.

The rule we follow now is cheap and it would have saved the weeks: before you optimize a slow query, reproduce the slowness in isolation. One run on a fresh rig. If the gap survives, profile it. If it vanishes, you just saved yourself a feature.

The dead `predicate_transfer` module stays dead. It was the right tool for a problem we turned out not to have.
