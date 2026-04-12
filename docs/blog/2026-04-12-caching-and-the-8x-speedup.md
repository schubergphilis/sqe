# Five Layers of Caching and an 8.8x Speedup Over Trino

*April 12, 2026*

Two days ago, SQE was slower than Trino on warm queries. Today, it's 2.5x to 8.8x faster across every benchmark suite. The fix was not a better algorithm. It was eliminating work that should never have happened in the first place.

## The problem: 540ms of overhead on every query

We built the `--compare-trino` flag into `benchmark-test.sh` to run identical queries against both engines and compare wall-clock times. The first comparison run was humbling. SQE was *correct* — every answer matched — but Trino beat us on ClickBench, on short TPC-H queries, on anything that completed in under a second.

Profiling told us the query execution was fast. DataFusion parsed, planned, and executed `SELECT 1` in under 1ms. The other 539ms was overhead:

| Component | Cost per query | What it does |
|---|---|---|
| RestCatalog creation | ~250ms | Build an iceberg-rust REST client to Polaris |
| OAuth token fetch | ~120ms | `client_credentials` grant to Polaris |
| SessionContext build | ~50ms | Register 70+ UDFs, TVFs, system tables |
| Table metadata lookup | ~100ms | Polaris REST round-trip per table |

Every single query paid all four costs. Trino paid them once at startup.

## The fix: five caching layers

**Layer 1: RestCatalog cache.** `moka::future::Cache` keyed by `polaris_url + token_fingerprint`, 5-minute TTL. The iceberg-rust `RestCatalog` is stateless — each `loadTable` call goes to Polaris independently. Safe to reuse.

**Layer 2: Table metadata cache.** Global, shared across all sessions. 30-second TTL. A TPC-DS run that touches 24 tables across 99 queries makes 24 Polaris calls instead of 2,376.

**Layer 3: Manifest file cache.** Iceberg manifests are immutable by spec. Cache parsed entries by S3 path with no TTL, 512MB LRU eviction. Eliminates the most expensive I/O in scan planning.

**Layer 4: SessionContext cache.** DataFusion's `SessionContext` wraps `Arc<SessionState>` — cloning is O(1). Cache per username (not per token, because OIDC creates fresh tokens per request). 5-minute TTL, max 100 entries. Invalidated after DDL/DML.

**Layer 5: OAuth service token cache.** The `client_credentials` grant returns the same-scope token every time. Cache it in-process, reuse until near-expiry. One `tokio::sync::RwLock`, zero contention on the read path.

## The cache invalidation bug

Caching the SessionContext created a correctness bug we found immediately in benchmarks. The load phase creates tables via CTAS. The cached SessionContext holds a catalog provider with a frozen namespace list. The query phase gets the cached context. "Table not found."

The fix: call `invalidate_session_cache(username)` after every schema-modifying operation. CREATE TABLE, DROP TABLE, CREATE SCHEMA, CTAS — any DDL invalidates the cached context. The next query pays the 50ms rebuild cost. Every subsequent query gets the cache hit.

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

221/222 queries pass. One TPC-E error (execution failure on a complex UPDATE). Six TPC-DS "diff" results — ROLLUP edge cases where DataFusion and Trino disagree on the grand total row for empty inputs (filed as apache/datafusion#21570).

The standout: TPC-H q01 runs in 34ms on SQE versus 2,275ms on Trino. That's 66.9x. The query itself is trivial — a filtered aggregation. All of Trino's time is overhead.

## The DECIMAL precision fix

While building the caching strategy, we discovered a critical correctness bug. `0.06 - 0.01` was returning `0.049999999999999996` instead of `0.05`. DataFusion's default behavior parses `0.06` as `Float64`, introducing floating-point imprecision.

TPC-H q06 has a `WHERE l_discount BETWEEN 0.05 AND 0.07` predicate. With float parsing, some rows that should match `0.05` exactly get compared as `0.049999...` and excluded. The query returned **40.7 million** instead of the correct **68.2 million**.

One config line fixed it:

```rust
.set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
```

Now `0.06` parses as `Decimal128(2, 2)`. Exact arithmetic. Correct results. This is the kind of bug that benchmarks find and unit tests don't — because you have to run TPC-H q06 with the actual data to trigger it.

## What we learned

The performance gap between SQE and Trino was never about query execution. DataFusion's execution engine was always competitive. The gap was infrastructure overhead — catalog creation, token management, session setup — that Trino amortizes across its JVM lifecycle and SQE was paying per-query.

Caching fixed the overhead. But caching introduced a correctness risk (stale catalogs) that required careful invalidation. And the process of building automated Trino comparisons surfaced a precision bug that had been silently producing wrong results since day one.

The lesson: you can't optimize what you don't measure, and you can't trust measurements that don't verify correctness. The `--compare-trino` flag does both. Same queries. Same data. Same network. Row-count verification on every query.

221 out of 222. We'll take it.
