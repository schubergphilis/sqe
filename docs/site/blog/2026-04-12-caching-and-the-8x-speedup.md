---
title: "Five Layers of Caching and an 8.8x Speedup Over Trino"
description: "How multi-layer caching took SQE from slower than Trino to 2.5-8.8x faster across every benchmark suite."
pubDate: "2026-04-12"
author: "Jacob Verhoeks"
tags:
  - "performance"
  - "caching"
  - "trino"
  - "benchmarks"
---



*April 12, 2026*

Two days ago, SQE was slower than Trino on warm queries. Today it is 2.5x to 8.8x faster across every benchmark suite. The fix was not a better algorithm. It was eliminating work that should never have happened in the first place.

## The problem: 540ms of overhead on every query

We built the `--compare-trino` flag into `benchmark-test.sh` to run identical queries against both engines and compare wall-clock times. The first comparison run was humbling. SQE was *correct*, every answer matched, but Trino beat us on ClickBench, on short TPC-H queries, on anything that completed in under a second.

Profiling told us the query execution was fast. DataFusion parsed, planned, and executed `SELECT 1` in under 1ms. The other 539ms was overhead:

| Component | Cost per query | What it does |
|---|---|---|
| RestCatalog creation | ~250ms | Build an iceberg-rust REST client to Polaris |
| OAuth token fetch | ~120ms | `client_credentials` grant to Polaris |
| SessionContext build | ~50ms | Register 70+ UDFs, TVFs, system tables |
| Table metadata lookup | ~100ms | Polaris REST round-trip per table |

Every single query paid all four costs. Trino paid them once at startup.

## The fix: five caching layers

**Layer 1: RestCatalog cache.** `moka::future::Cache` keyed by `polaris_url + token_fingerprint`, 5-minute TTL. The iceberg-rust `RestCatalog` is stateless. Each `loadTable` call goes to Polaris independently. Safe to reuse.

**Layer 2: Table metadata cache.** Global, shared across all sessions. 30-second TTL. A TPC-DS run that touches 24 tables across 99 queries makes 24 Polaris calls instead of 2,376.

**Layer 3: Manifest file cache.** Iceberg manifests are immutable by spec. Cache parsed entries by S3 path with no TTL, 512MB LRU eviction. This eliminates the most expensive I/O in scan planning.

**Layer 4: SessionContext cache.** DataFusion's `SessionContext` wraps `Arc<SessionState>`. Cloning is O(1). Cache per username, not per token, because OIDC creates fresh tokens per request. 5-minute TTL, max 100 entries. Invalidated after DDL/DML.

**Layer 5: OAuth service token cache.** The `client_credentials` grant returns the same-scope token every time. Cache it in-process, reuse until near-expiry. One `tokio::sync::RwLock`, zero contention on the read path.

## The cache invalidation bug

Caching the SessionContext created a correctness bug we found immediately in benchmarks. The load phase creates tables via CTAS. The cached SessionContext holds a catalog provider with a frozen namespace list. The query phase gets the cached context. "Table not found."

The fix: call `invalidate_session_cache(username)` after every schema-modifying operation. CREATE TABLE, DROP TABLE, CREATE SCHEMA, CTAS, any DDL invalidates the cached context. The next query pays the 50ms rebuild cost. Every subsequent query gets the cache hit.

This is the kind of bug that only appears under the exact load/query pattern benchmarks use. Unit tests create tables and query them in the same function. The cache is empty. No bug. Benchmarks create tables in phase 2 and query them in phase 3. The cache is warm. Bug.

## The results

We ran all seven benchmark suites with `--compare-trino` against Trino 465:

| Suite | Queries | SQE (ms) | Trino (ms) | Speedup | Match |
|---|---|---|---|---|---|
| TPC-H | 22 | 1,646 | 10,796 | **8.8x** | 22/22 |
| SSB | 13 | 710 | 2,045 | **3.2x** | 13/13 |
| TPC-DS | 99 | 19,650 | 46,989 | **2.6x** | 93/99 |
| TPC-C | 8 | 304 | 1,528 | **5.5x** | 8/8 |
| TPC-E | 11 | 474 | 2,175 | **5.3x** | 11/11 |
| TPC-BB | 10 | 1,223 | 2,193 | **3.1x** | 10/10 |
| ClickBench | 43 | 904 | 2,205 | **2.5x** | 43/43 |

221/222 queries pass. One TPC-E error (execution failure on a complex UPDATE). Six TPC-DS "diff" results are ROLLUP edge cases where DataFusion and Trino disagree on the grand total row for empty inputs (filed as apache/datafusion#21570).

The standout: TPC-H q01 runs in 34ms on SQE versus 2,275ms on Trino. That is 66.9x. The query itself is trivial, a filtered aggregation. All of Trino's time is overhead.

## The DECIMAL precision fix

While building the caching strategy, we discovered a critical correctness bug. `0.06 - 0.01` was returning `0.049999999999999996` instead of `0.05`. DataFusion's default behavior parses `0.06` as `Float64`, introducing floating-point imprecision.

TPC-H q06 has a `WHERE l_discount BETWEEN 0.05 AND 0.07` predicate. With float parsing, some rows that should match `0.05` exactly get compared as `0.049999...` and excluded. The query returned **40.7 million** instead of the correct **68.2 million**.

One config line fixed it:

```rust
.set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
```

Now `0.06` parses as `Decimal128(2, 2)`. Exact arithmetic. Correct results. This is the kind of bug that benchmarks find and unit tests miss, because you have to run TPC-H q06 with the actual data to trigger it.

## One week, start to finish

The transformation is easier to see in a table. All numbers are SF0.01, same hardware, same Polaris, same S3.

### SQE solo performance

| Suite | Apr 2 | Apr 10 | **Apr 12** | **Speedup** |
|---|---|---|---|---|
| TPC-H | 13.6s (22/22) | 18.5s (22/22) | **1.6s** (22/22) | **8.5x** |
| SSB | 7.7s (13/13) | 8.6s (13/13) | **0.7s** (13/13) | **11x** |
| TPC-DS | 68.3s (93/99) | 77.1s (99/99) | **13.0s** (99/99) | **5.3x** |
| ClickBench | 23.5s (43/43) | 24.3s (43/43) | **0.6s** (43/43) | **39x** |
| TPC-C | 2.8s (5/8) | 7.6s (17/17) | **0.9s** (17/17) | **3.1x** |
| TPC-E | 3.6s (6/11) | 9.1s (17/18) | **1.0s** (17/18) | **3.6x** |
| TPC-BB | 6.9s (10/10) | 7.4s (10/10) | **1.1s** (10/10) | **6.3x** |
| **Total** | **126.4s** (192) | **153.6s** (218) | **18.9s** (221) | **6.7x** |

Note that Apr 10 is *slower* than Apr 2 on some suites. We added 70+ UDFs, streaming writes, and sort-order safety checks that week. Correctness before speed. The pass count went from 192 to 218. Then caching made everything fast.

### SQE vs Trino: the reversal

| Suite | Apr 10 | **Apr 12** |
|---|---|---|
| TPC-H | SQE 0.6x Trino (lost) | **SQE 8.8x Trino** |
| SSB | SQE 0.3x Trino (3x slower) | **SQE 3.2x Trino** |
| TPC-DS | SQE 0.5x Trino (2x slower) | **SQE 2.6x Trino** |
| ClickBench | SQE 0.1x Trino (10x slower) | **SQE 2.5x Trino** |
| TPC-C | SQE 0.5x Trino | **SQE 5.5x Trino** |
| TPC-E | SQE 0.4x Trino | **SQE 5.3x Trino** |
| TPC-BB | broken (0/10 match) | **SQE 3.1x Trino** |

On April 10, we lost every comparison. On April 12, we won every one.

### What changed, day by day

- **Apr 6**: Distributed execution landed. Coordinator + 2 workers, 3.1x on SF1 TPC-H.
- **Apr 10**: Streaming writes, sort-order safety, IN-subquery rewrite, 70+ Trino-compat UDFs. Correctness went from 192/222 to 218/222. Slower overall because every query now did more setup work.
- **Apr 12 morning**: RestCatalog cache, table metadata cache, manifest file cache. SQE reached parity with Trino, roughly 1.0-1.4x.
- **Apr 12 afternoon**: SessionContext cache + OAuth token cache + cache invalidation + DECIMAL fix. SQE pulled ahead on every suite.

The inflection point was the SessionContext cache. Eliminating 540ms of per-query overhead turned a 0.6x deficit into an 8.8x lead. The query execution was always fast. The infrastructure around it was not.

## What we learned

The performance gap between SQE and Trino was never about query execution. DataFusion's execution engine was always competitive. The gap was infrastructure overhead: catalog creation, token management, session setup. Trino amortizes this across its JVM lifecycle. SQE was paying it per-query.

Caching fixed the overhead. But caching introduced a correctness risk (stale catalogs) that required careful invalidation. And the process of building automated Trino comparisons surfaced a precision bug that had been silently producing wrong results since day one.

Three lessons.

First, you cannot optimize what you do not measure. The `--compare-trino` flag made every improvement and every regression immediately visible. Same queries, same data, same network, row-count verification on every query.

Second, correctness before speed. We spent April 6-10 making things slower but more correct (192 to 218 pass). Then April 12 made them fast (218 to 221 pass, 6.7x faster). If we had optimized first, we would have built caches for code paths that did not work yet.

Third, the hardest bugs hide in the infrastructure. The DECIMAL precision bug had been silently corrupting TPC-H q06 results since day one. The SessionContext cache invalidation bug only appeared under the exact load/query pattern that benchmarks produce. Neither would have surfaced from unit tests. Both required running real SQL against real data with real comparisons.

221 out of 222. We will take it.
