---
title: "Runnable docs, or how the quickstarts became a test suite"
description: "We turned every 'how to run SQE for X' into a self-contained run.sh that goes from clean state to captured output. The point was documentation. The payoff was a test suite that caught a missing metric, an overclaimed capability, and a benchmark that would have polluted our baselines."
pubDate: "2026-06-07"
author: "Jacob Verhoeks"
tags:
  - "documentation"
  - "testing"
  - "quickstart"
  - "observability"
  - "benchmarks"
---



*June 7, 2026*

The configuration knowledge for SQE was scattered. Some in the book, some in design docs, some in test fixtures, some in my head. A new user asking "how do I point SQE at Glue" had to assemble the answer from four places.

So we wrote quickstarts. One directory per use case, each one self-contained: a compose file or a CLI invocation, an annotated config, and a `run.sh` that goes from a clean state to captured output and back down. The README explains the why and the how. The `OUTPUT.md` is the real result, committed as evidence.

That was the goal. Documentation a new user can run.

The thing I did not plan for is that the quickstarts became a test suite.

## The shape

Every quickstart runs the same way:

```bash
cd quickstart/<name>
cp .env.example .env
./run.sh
```

`run.sh` brings up exactly what the use case needs, does the thing, captures the output to `OUTPUT.md`, and tears the stack down. From a clean state. No leftover containers, no assumed local setup.

A prose doc claims something works. A `run.sh` either exits zero with the captured rows, or it does not. The difference matters more than I expected.

## What running them caught

**A metric that does not exist.** The observability quickstart scrapes SQE's Prometheus endpoint with VictoriaMetrics and renders a Grafana dashboard. I had drafted the dashboard against `sqe_queries_total`. That metric does not exist. The real ones are `sqe_rows_returned_total`, `sqe_active_sessions`, `sqe_cache_hits_total`, and friends. A prose doc with a panel definition would have shipped the wrong name and nobody would have noticed until a user copied it. Running the scrape and querying the series back surfaced it in the first pass.

**A capability we do not have.** The Glue quickstart's README promised that a dedicated quickstart would show "fine-grained Lake Formation grants." Building the runnable version forced me to grep the engine for the API that would implement it. It is not there. SQE reads S3 directly and never calls Lake Formation's credential vending, so it gates the catalog, not the rows. That correction has [its own post](./2026-06-07-lake-formation-gates-the-catalog.md). The point here is that writing a script I had to actually run is what turned a comfortable assumption into a grep.

**A benchmark that would have polluted the baselines.** The benchmark quickstart runs TPC-H through `sqe-bench`. The `test` command writes a JSON report to `benchmarks/results/`, which is where our committed SF1 performance baselines live. A small demo run at scale factor 0.01 would have dropped a file that looks like a baseline but is not. I only saw it because running the thing printed the path it wrote to. The fix was to keep the demo report ephemeral, inside the container, and never mount that directory.

**A claim we could not make yet.** The quack quickstart enables SQE's DuckDB wire protocol endpoint. A clean-state `run.sh` could prove the server answers, but a full client round-trip needed a quack-capable DuckDB build that is not a stock release. So the status stayed "experimental," not "validated," until a real `duckdb 1.5.3` on the bench actually queried an SQE Iceberg table over the protocol and the run captured the rows. The script would not let me round up.

## Why this works

Prose rots silently. A function signature changes, a metric gets renamed, a flag flips its default, and the doc keeps saying the old thing until a reader hits the wall. Nobody gets paged when a paragraph goes stale.

A script that runs from a clean state fails loudly. Rename the metric and the dashboard query returns nothing. Remove the flag and `run.sh` exits non-zero. The doc cannot drift far from the engine without the next run catching it, because the doc is the run.

There is a second effect, quieter. Writing a script you will execute forces honesty that prose does not. You cannot hand-wave a step. You cannot describe an output you have not seen. The captured `OUTPUT.md` is the real bytes, including the ugly ones, like a 132-byte AWS `AccessDeniedException` in full.

## The cost

This is more work than writing prose. Some quickstarts need a live AWS account and careful teardown, so a run costs money and the failure mode is a leaked resource, not a red test. Some are pinned to pre-release tools and will need a version bump when those tools move. The output is committed, so it has to be regenerated when behaviour changes.

I will take that trade. A doc that might be wrong is worth less than a script that was right the last time it ran, with the output to prove it. The quickstarts are the source of truth now precisely because they are the thing we run.
