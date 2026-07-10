---
title: "DataFusion 54: what it actually unblocked"
description: "DataFusion 54 landed and we bumped to it. The port was mostly mechanical, with one real behavioral change in the shuffle hasher. The interesting part is what the release notes implied and the engine did not deliver: LATERAL joins are logical-plan only, array lambdas still fail, and the one compatibility win we found was a documentation bug, not a DataFusion feature. We tested every claim before we wrote it down."
pubDate: "2026-06-18"
author: "Jacob Verhoeks"
tags:
  - "datafusion"
  - "iceberg"
  - "benchmarks"
  - "sql"
---

*June 18, 2026*

DataFusion 54 shipped. We moved SQE onto it the same week.

The headline most people will read is "LATERAL joins." That is the line that pulled me in too. SQE wraps DataFusion, so when DataFusion grows a SQL feature, we usually inherit it for free. LATERAL has been a real gap against Trino and DuckDB. So the first thing I did after the build went green was write a test for it.

It failed. That is the story.

## The bump itself was boring

DataFusion 54 removed the boilerplate `as_any()` method from the core traits. `ExecutionPlan`, `TableProvider`, `PhysicalExpr`, the catalog and schema providers. They all gained an `Any` supertrait and a provided `downcast_ref` instead. So the port is delete the hand-written `as_any` impls, and rewrite `x.as_any().downcast_ref::<T>()` to `x.downcast_ref::<T>()`. The compiler points at every site. You keep the Arrow array downcasts as they are, because Arrow did not make the same change, and you keep the protobuf `as_any` calls, because those are a different trait entirely.

A few other shapes moved. `partition_statistics` returns an `Arc` now. `PruningStatistics::row_counts` dropped a parameter. `Expr::Cast` holds a field reference instead of a raw type. None of it is interesting. It is a compiler-guided sweep, and the test suite tells you when you are done.

One change was not mechanical. DataFusion 54 swapped its hash backend from ahash to foldhash, and `RandomState` is now an alias for a fixed-seed foldhash state. SQE's distributed shuffle computes `hash % num_partitions` to route rows to workers, so the hasher matters: every node in the cluster has to agree on it. We point the shuffle at DataFusion's own fixed seed-0 state, the same one its repartition operator uses. The cluster runs one image, so the seed is identical everywhere, and the shuffle stays deterministic. We proved it the only way that counts: a distributed run of all 222 benchmark queries, where a broken hasher would have produced wrong join results, not a crash. Every row matched.

While we were in the parser, we aligned one more pin. SQE pinned sqlparser at 0.54 for a derive macro, while DataFusion 54 pulled 0.62, so two parser versions shipped in the tree and our pre-parser saw a different grammar than the engine. We moved our pin to 0.62. One parser now, same grammar as DataFusion. That port was larger than the version bump, because 0.62 reshaped a lot of the AST, but the integration suite covers the SQL surface and it came back green.

## LATERAL is logical-only

Here is what the failing test taught me.

DataFusion 54 parses LATERAL and builds a logical plan for it. Then physical planning fails: "Physical plan does not support logical expression OuterReferenceColumn." DataFusion has no physical dependent-join operator yet. The correlated subquery shapes that matter, a projection that references the outer row, a top-N-per-group with an inner `LIMIT`, hit that wall. The only LATERAL shapes that run are the ones that decorrelate into an ordinary join, and those already ran on DataFusion 53.

So the compatibility list does not move. I checked the rest of the candidates the same way, in one pass against the running engine. Array lambdas like `transform` and `filter` still return "Invalid function," because DataFusion has no higher-order-function support. `PIVOT`, `UNPIVOT`, and `ASOF JOIN` are still rejected by the planner. DataFusion 54 unblocked none of them.

The one surprise went the other way. `QUALIFY` works. Our DuckDB comparison doc listed it as unsupported, and that was simply wrong: the SQL planner has handled `QUALIFY` since 53.1, with identical code in 54. A stale doc, not a missing feature. We added a test for it and corrected the doc. That is the honest shape of this release for the SQL surface: nothing new unblocked, one documentation bug found.

There was a real catch hiding in the error path, and an integration test caught it. DataFusion 54 reworded its function argument-type-mismatch error. It used to say `TypeSignatureClass`, which our error classifier keyed on. Now it says "No function matches the given name and argument types. You might need to add explicit type casts." Our classifier saw "no function matches" and bucketed a type mismatch as a missing-function error, which sends the wrong code to clients. The fix keys on "add explicit type casts" instead, which DataFusion emits only when a function exists but the argument types do not match a signature. A genuinely missing function says "Invalid function." Worth knowing if you string-match DataFusion errors anywhere.

## Performance: parity at SF1, wins at SF10, one suite still trailing

DataFusion 54's gains are execution-level. Faster repartition, a better hash-join comparator, faster semi and anti joins. None of that shows at SF1, where the data is small and per-query planning dominates. We measured it: SF1 single-node is at parity with 53, slightly slower on TPC-H, slightly faster on TPC-DS, inside the run-to-run noise.

SF10 is where it shows, though I want to be careful about attribution. On the dedicated rig, single-node, cache off, on DataFusion 54: TPC-H runs 89 seconds against Trino's 106, TPC-DS runs 234 against 448. Both are wins. SSB still loses, 32 against 15, because lineorder's uniform foreign-key distribution defeats row-group pruning and the runtime filter only helps at the row level. That last suite is the honest gap.

The TPC-H and TPC-DS wins are not DataFusion 54 alone. The parallel scan filter we shipped earlier the same month did most of the lifting, and these are single-run numbers on one box. Read them as directional. The thing I will not claim is a clean DataFusion-53-versus-54 A/B, because the 53 image is gone and the rig conditions are not frozen.

Distribution earns almost nothing here, and that is its own lesson. Two workers co-tenant with the coordinator and Trino on eight cores: TPC-H gains four percent, TPC-DS actually regresses, and three inventory queries exhaust the 4GB worker pool. Distribution pays off on separate hosts, not on a shared box. The real multi-node verdict is still missing. That is the SF100 question, and it is the next thing we write down.

## What did not come along

Delta Lake. The `read_delta` table function depends on delta-rs, and delta-rs has no DataFusion 54 release: its table provider implements an older DataFusion's trait, so it will not compile against 54. We removed the optional feature and the dependency, kept the module file on disk, and left a note to restore it when delta-rs catches up. The iceberg side is the opposite story: we vendor a fork, and our in-tree DataFusion 54 port is now ahead of both apache main and the RisingWave rebase branch, which are still on 53. When upstream ships a 54 rebase, we drop our patch.

That is DataFusion 54 for an engine that wraps it. The version bump is a day of compiler-guided edits. The release-notes feature you wanted may be logical-plan only. The compatibility win you find may be a doc bug. Test the claim before you write it down.
