---
title: "How Agentic AI Helped Us Beat Trino"
description: "221 queries, 7 suites, one week — how an AI assistant running automated benchmarks drove a major performance breakthrough."
pubDate: "2026-04-13"
author: "Jacob Verhoeks"
tags:
  - "ai"
  - "performance"
  - "benchmarks"
  - "development-process"
---



*April 13, 2026*

A week ago, SQE was slower than Trino on every benchmark suite. Today it is 2.5x to 8.8x faster. The speed improvement is real. But the more interesting story is how we built it, with an AI assistant that does not just write code but runs benchmarks, reads results, diagnoses problems, and proposes fixes in a continuous loop.

This is not a story about AI generating boilerplate. It is about agentic AI as a force multiplier for systems engineering.

## The loop that changed everything

Traditional development:
```
Write code -> Build -> Run test -> Read output -> Think -> Repeat
```

What we did:
```
Describe intent -> AI writes code -> AI builds -> AI runs benchmarks ->
AI reads results -> AI diagnoses failure -> AI proposes fix ->
Human reviews -> AI implements -> AI validates -> Commit
```

The difference is not just speed, though it is faster. The difference is that the AI holds the entire context: the benchmark results, the source code, the Trino comparison numbers, the error messages, the config files. When TPC-H q06 returned 40.7 million rows instead of 68.2 million, the AI did not just report the failure. It traced the discrepancy to `0.06 - 0.01 = 0.049999999999999996` (floating-point), found the DataFusion config flag `parse_float_as_decimal`, applied the fix, rebuilt, reran the benchmark, and verified the correct result. One conversation turn.

A human would have done the same investigation. It would have taken half a day. The AI did it in minutes. Not because it is smarter. Because it does not lose context between steps.

## What the AI actually did

Here is the concrete work from one week, driven by an AI assistant.

### Day 1-2: Trino compatibility (Apr 6-8)
- Implemented 70+ Trino-compatible UDFs (`year()`, `month()`, `day_of_week()`, `date_add()`, `date_diff()`, `date_format()`, `date_parse()`, `soundex()`, `regexp_extract()`, `word_stem()`, etc.)
- Fixed date function return types (Float64 to Int64 to match Trino)
- Built the `--compare-trino` flag: starts a Trino 465 container, runs identical queries against both engines, compares row counts

### Day 3: Streaming writes + correctness (Apr 9-10)
- Converted CTAS/INSERT from `collect().await` (buffers all rows) to `execute_stream()` (constant memory)
- Built IN-subquery rewrite for UPDATE/DELETE (DataFusion limitation workaround)
- Implemented safe Iceberg sort order: only trust partition columns, warn on non-partition sorts
- Fixed `prefix_tables` to not qualify CTE column references (`store.item_sk`)
- Fixed ClickBench q18/q28 SQL compat (alias-in-GROUP-BY rejected by Trino)

### Day 4: The caching breakthrough (Apr 12)
- Diagnosed 540ms per-query overhead via profiling
- Built 5-layer caching stack:
  1. RestCatalog cache (eliminates 250ms/query)
  2. Table metadata cache (global, 30s TTL)
  3. Manifest file cache (immutable, 512MB LRU)
  4. SessionContext cache (per-user, invalidated after DDL)
  5. OAuth service token cache (eliminates 120ms/query)
- Found and fixed the cache invalidation bug (CTAS then query: "table not found")
- Found and fixed DECIMAL precision bug (TPC-H q06 wrong results)
- Server-side query latency: 540ms down to <1ms

### The validation loop

Every change went through the same cycle:

1. **Build**: `cargo build --all`. The AI waits for compilation, reads errors, fixes them.
2. **Test**: `cargo test --all`. 1,334 unit tests. The AI reads failures and fixes them.
3. **Clippy**: `cargo clippy -- -D warnings`. Zero tolerance for warnings.
4. **Benchmark**: `BENCH_SCALE=0.01 ./scripts/benchmark-test.sh`. 222 queries across 7 suites.
5. **Compare**: `--compare-trino`. Row-by-row validation against Trino 465.

The AI ran this loop dozens of times in a single day. Each iteration took 5-10 minutes (build + test + benchmark). A human running the same loop manually would do it 3-4 times per day. The AI did it 20+ times, catching regressions within minutes of introducing them.

## What made it work

**Context retention.** The AI held the full benchmark results, the source code of all 10 crates, the Trino comparison numbers, and the git history in a single conversation. When the SessionContext cache caused "table not found" errors, it connected the symptom (tables created in load phase not visible in query phase) to the root cause (cached namespace list) without needing to be told the architecture.

**Automated validation.** Every proposed change was immediately validated by the benchmark suite. The AI did not trust its own code. It ran the tests. When 22 trino_functions tests failed after changing return types from Float64 to Int64, it fixed all 22 tests in one pass. It could see the pattern: all `run_query` helpers used `Float64Array` downcast.

**No pride in code.** The AI proposed a fix, the tests showed it was wrong, and it immediately tried a different approach. No attachment to the first solution. No "but it should work." Just: build, test, read, fix.

**Build-measure-fix loop.** The `--compare-trino` flag was the single most valuable tool. It turned every optimization from "I think this is faster" into "this is 8.8x faster on TPC-H, here are the per-query numbers." The AI built the tool, then used the tool to validate every subsequent change.

## The numbers

| Suite | Before (Apr 5) | After (Apr 12) | vs Trino |
|---|---|---|---|
| TPC-H | 13.6s, 22/22 | 1.6s, 22/22 | **8.8x faster** |
| SSB | 7.7s, 13/13 | 0.7s, 13/13 | **3.2x faster** |
| TPC-DS | 68.3s, 93/99 | 13.0s, 99/99 | **2.6x faster** |
| ClickBench | 23.5s, 43/43 | 0.6s, 43/43 | **2.5x faster** |
| TPC-C | 2.8s, 5/8 | 0.9s, 17/17 | **5.5x faster** |
| TPC-E | 3.6s, 6/11 | 1.0s, 17/18 | **5.3x faster** |
| TPC-BB | 6.9s, 10/10 | 1.1s, 10/10 | **3.1x faster** |

192/222 to 221/222 queries passing. 126s to 19s total. Every suite faster than Trino.

## What the AI cannot do

Design decisions. The choice to cache by username instead of token fingerprint was a human insight about how OIDC works. The decision to use `partition_only` as the default sort mode was a human call about safety vs performance. The architecture of the five caching layers came from studying Trino's own caching strategy. The AI read the docs. The human chose which layers to implement and which to skip.

The AI is a force multiplier, not a replacement. It eliminates the mechanical parts of software engineering: the build-test-fix loop, the grep-read-edit cycle, the "find all 22 tests that need updating" tedium. That frees the human to focus on the hard parts. What to build, why, and what tradeoffs to accept.

## The tooling that matters

Three things made this possible.

**Automated benchmarks with comparison.** `--compare-trino` runs both engines on the same queries and reports per-query speedup. Without this, every optimization is a guess. With this, every optimization is measured.

**Strict CI loop.** Zero clippy warnings. Zero test failures. The AI enforced this automatically. It would not propose a commit that failed any check. This prevented the "fix one thing, break another" cycle that slows manual development.

**Safe defaults.** Spill-to-disk enabled by default. Non-partition sorts stripped by default. DECIMAL parsing by default. These are not performance features. They are correctness features that prevent the kind of bugs that only appear at scale.

## What is next

The engine is fast. The benchmarks prove it. The automated comparison ensures it stays fast. The next challenge is not performance. It is trust. Can we run this in production? Can we migrate real workloads from Trino without users noticing, except that their queries are faster?

That is a story for next week.

---

*SQE is built on Apache DataFusion, Apache Iceberg, and Apache Polaris. The benchmark suite (`sqe-bench`) runs 222 queries across 7 standard suites. All benchmark results are committed to the repo for historical comparison.*
