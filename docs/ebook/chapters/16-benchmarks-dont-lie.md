# Benchmarks Don't Lie (But They Mislead) {#sec:benchmarks}

> The number that matters is the one your users will hit.
> Everything else is marketing.

We said SQE was fast. The team believed it. The architecture diagrams looked right. DataFusion is fast. Rust is fast. Arrow columnar reads are fast. Iceberg partition pruning is fast. Every component, considered individually, was fast.

None of that matters until you run the queries and measure.

The management question was simple: "How does SQE compare to Trino?" The engineering question was harder: "Compare on what?" TPC-H is the standard answer. But TPC-H was designed in 1992 for a world of RAID arrays and shared-nothing parallel databases. Our users don't run TPC-H. They run dbt models against Iceberg tables with row-level security and bearer token authentication. The benchmark that makes the slide deck look good and the benchmark that predicts production performance are rarely the same benchmark.

We needed both.


## Seven Suites, Two Hundred Queries

The `sqe-bench` crate ships as a standalone Rust binary with three commands: `generate`, `load`, and `test`. Each command targets one of seven benchmark suites.

| Suite | Queries | Tables | What it tests |
|-------|---------|--------|--------------|
| TPC-H | 22 | 8 | Classic analytical: joins, aggregations, date arithmetic |
| TPC-DS | 99 | 24 | Complex retail analytics: subqueries, CTEs, window functions |
| SSB | 13 | 5 | Star schema joins, denormalized scans |
| ClickBench | 43 | 1 | Single-table scan performance, web analytics patterns |
| TPC-E | 18 | 33 | Financial OLTP reads, complex demographics |
| TPC-BB | 10 | 2 (+TPC-DS) | Big data analytics over clickstreams and reviews |
| TPC-C | 17 | 9 | Transaction processing (read + write: DELETE, UPDATE via CoW) |

Why seven? Because each one tests a different failure mode. TPC-H tests your join algorithms. TPC-DS tests your SQL parser's ability to handle correlated subqueries and GROUPING SETS. ClickBench tests your raw scan speed on a single wide table. TPC-C tests whether your engine falls over when queries hit one row instead of a million. A query engine that passes TPC-H and fails TPC-DS has gaps in SQL coverage that will bite users the first time they write a CTE with a window function.

The seven suites together total 222 queries across 82 tables. That is not a marketing number. It is a regression test suite that happens to produce timing data.


## Generate, Load, Test

The pipeline has three stages, and they run independently. You can generate data on a laptop, load it into a remote cluster, and run tests from a CI runner. Or you can do all three locally in one script. The separation matters because data generation is CPU-bound and deterministic, loading is network-bound and idempotent, and testing is the only part that touches the engine being measured.

### Generate

Every generator implements the `BenchmarkGenerator` trait:

```rust
pub trait BenchmarkGenerator: Send + Sync {
    fn name(&self) -> &str;
    fn tables(&self) -> Vec<TableDef>;
    fn generate_table(
        &self,
        table: &str,
        scale: f64,
        output_dir: &str,
    ) -> anyhow::Result<GenerateStats>;
}
```

A `TableDef` carries the Arrow schema and a function that maps scale factor to row count:

```rust
pub struct TableDef {
    pub name: String,
    pub schema: SchemaRef,
    pub row_count: fn(f64) -> usize,
}
```

The TPC-H generator at scale factor 1 produces roughly one gigabyte across eight tables. At scale factor 0.01, it produces enough data to verify correctness in seconds. The data is deterministic: seeded random number generators ensure the same scale factor always produces the same rows, so results are reproducible across runs and machines.

```rust
fn seed_for_table(name: &str) -> u64 {
    name.bytes()
        .enumerate()
        .fold(0u64, |acc, (i, b)| {
            acc ^ ((b as u64).wrapping_shl(i as u32 % 64))
        })
        .wrapping_add(0xDEAD_BEEF_CAFE_1234)
}
```

That constant looks whimsical. It is. But the determinism it enables is not. Reproducible benchmarks are the difference between "the numbers moved" and "we know why the numbers moved."

The Parquet writer splits output at 128 MB per file, which aligns with Iceberg's default target file size and gives the distributed scheduler enough fragments to work with at higher scale factors.

### Load

The `load` command connects to SQE (or Trino, via the `--protocol` flag) and creates Iceberg tables using CTAS:

```sql
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet(
  '/data/tpch/sf1/lineitem/*.parquet',
  access_key => 'AKIA...',
  secret_key => '...',
  endpoint => 'http://localhost:9000',
  region => 'us-east-1'
);
```

Each benchmark gets its own namespace: `tpch_sf1`, `tpcds_sf10`, `ssb_sf0_01`. The namespace naming matters because the test runner needs to qualify every table reference in the query SQL, and the naming scheme must be predictable without configuration.

TPC-BB is a special case. Its queries run against TPC-DS tables plus two additional tables (`web_clickstreams` and `product_reviews`). The loader knows this. When the benchmark is `tpcbb`, it loads into the `tpcds` namespace instead of creating its own.

### Test

The test runner loads SQL files from `benchmarks/queries/<benchmark>/`, qualifies table names, executes each query, and compares results against expected CSV files when they exist.

Each query file supports header metadata:

```sql
-- name: Pricing Summary Report
-- requires: window_functions, lateral_join
-- timeout: 60s
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty,
       ...
```

The `requires` tag is the graceful degradation mechanism. When SQE does not support a feature, the query is marked with the requirement. The runner skips it cleanly instead of producing a confusing error. This means the benchmark suite can carry queries for features we plan to implement without them polluting the pass/fail count. With ROLLUP now enabled and DELETE/UPDATE/MERGE implemented via CoW, the skip count has dropped significantly. TPC-DS runs 99/99 and TPC-C runs 17/17.

The `timeout` tag defaults to 300 seconds but exists because some queries on large scale factors can legitimately run for minutes, and we need to distinguish "slow" from "stuck." The runner uses `tokio::select!` to race the query against its deadline:

```rust
let execute_result = tokio::select! {
    result = client.execute(&sql) => Some(result),
    _ = tokio::time::sleep(Duration::from_secs(timeout_secs)) => {
        eprintln!("[bench] {} TIMEOUT after {}s", query.id, timeout_secs);
        None
    }
};
```

This is the same pattern described in Chapter 14 for handling stuck gRPC streams. A `tokio::timeout` wrapper does not help if the underlying HTTP/2 stream is wedged. It cannot cancel through the gRPC layer. `tokio::select!` drops the losing branch, which closes the connection and frees resources. The benchmark runner learned this lesson from the load test and applied it preemptively.

The output is both human-readable terminal output and a machine-readable JSON report.

```
TPCH SF1 -- flight protocol
────────────────────────────────────────────────────────
v q01        1.23s    6001215 rows
v q02        0.45s        460 rows
v q03        0.89s      11620 rows
...
~ q17        2.10s          1 rows  (numeric values differ within epsilon)
- q14        0.00s          0 rows  (requires: lateral_join)

Results: 20 pass, 0 fail, 1 diff, 1 skip, 0 error  (total 28.4s)
```

Five result statuses: `Pass`, `Fail`, `Diff`, `Skip`, `Error`. `Diff` means the answer is close but not exact: floating-point precision differences between engines, or trailing zeros on decimals. `Skip` means the query requires a SQL feature SQE doesn't implement yet. Neither counts as a failure in CI. The distinction matters because you want to know the difference between "this query produces slightly different decimal rounding" and "this query returns the wrong answer."

The JSON reports accumulate in `benchmarks/results/` and are suitable for tracking performance regressions over time:

```json
{
  "benchmark": "tpch",
  "scale_factor": 1,
  "protocol": "flight",
  "timestamp": "2026-03-24T14:30:00",
  "summary": {
    "total": 22,
    "pass": 20,
    "fail": 0,
    "diff": 1,
    "skip": 1,
    "error": 0,
    "total_duration_ms": 28400
  }
}
```


## The Table Qualification Problem

This was the first bug the benchmark suite found, and it was the hardest to fix correctly.

TPC-H queries are written with bare table names: `SELECT * FROM lineitem`. But SQE organizes benchmark data into namespaces: `tpch_sf1.lineitem`. The test runner must qualify every table reference before sending the query to the engine.

Sounds simple. Replace `lineitem` with `tpch_sf1.lineitem`. But consider TPC-H query 16:

```sql
SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND ps_suppkey NOT IN (
    SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%bad%'
  )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC
```

Naive string replacement of `part` also matches inside `partsupp`. Replacing `partsupp` first and then `part` creates `tpch_sf1.tpch_sf1.partsupp`. The fix: process tables longest-name-first so `partsupp` is qualified before `part` can match its substring.

```rust
// Longest first to prevent "part" matching inside "partsupp"
tables.sort_by_key(|t| std::cmp::Reverse(t.len()));
```

But that was only the first layer. The qualifier also needs word-boundary detection: `part` should not match inside the column name `p_partkey`. And it should not qualify table names that appear as column aliases after `AS`. And it needs to handle multi-line FROM clauses where tables are separated by commas on different lines.

The `prefix_tables` function in `test.rs` grew to 100 lines with context-aware matching: it checks whether the table name is preceded by `FROM`, `JOIN`, `TABLE`, `INTO`, or a comma in a table-list context. It checks for `AS` aliases. It handles double-quoted identifiers. It has eleven unit tests.

```rust
let in_table_context = upper_before.ends_with(" FROM")
    || upper_before.ends_with(" JOIN")
    || upper_before.ends_with(" TABLE")
    || upper_before.ends_with(" INTO")
    || upper_before.ends_with(" UPDATE")
    || upper_before.ends_with(" EXISTS")
    || trimmed_before.ends_with(',')
    || {
        let words: Vec<&str> = trimmed_before.split_whitespace().collect();
        words.last().map(|w| {
            let u = w.to_uppercase();
            u == "FROM" || u == "JOIN" || u == "TABLE" || u == "INTO"
        }).unwrap_or(false)
    };
```

This is not elegant. It is a hand-rolled SQL-aware string replacer. A proper solution would parse the SQL into an AST, walk the tree, and qualify `TableReference` nodes. We considered it. The effort would have been a full day for marginal correctness improvement. The heuristic handles all 222 queries across all seven suites. Sometimes the pragmatic solution is the right one.

::: {.deadend}
**Dead end: AST-based table qualification.** We started building a proper SQL parser pass
to qualify table references. It worked for simple queries but broke on TPC-DS's deeply
nested subqueries where the same table name appears as both a table reference and a
column alias. The heuristic approach with context-aware string matching was cruder but
handled every real query file. We shipped the heuristic. It has not been wrong yet.
:::


## The Bugs Nobody Expected

We built the benchmark suite to measure performance. It found bugs instead. In the first live run across all seven suites, twelve queries that should have passed produced errors. Not wrong answers. Errors. The engine could not execute them at all.

### gRPC keepalive

Running TPC-DS's 99 queries sequentially took about ten minutes. Somewhere around query 60, the connection went silent. No timeout, no error, no response. The gRPC channel was technically open but not producing bytes.

The root cause was HTTP/2 keepalive. Long-running benchmark sessions held a single gRPC connection for minutes. Without keepalive pings, intermediate load balancers and firewalls silently dropped the idle connection. The server thought it was talking to a client. The client thought it was talking to a server. Neither was talking to anything.

The fix in the benchmark client was three lines:

```rust
let channel = Channel::from_shared(url.clone())?
    .keep_alive_while_idle(true)
    .http2_keep_alive_interval(Duration::from_secs(10))
    .keep_alive_timeout(Duration::from_secs(20))
    .connect()
    .await?;
```

But the fix in the benchmark client exposed that we also needed it in the engine's own worker-to-coordinator connections. The benchmark found a bug in `sqe-bench` that pointed to the same class of bug in `sqe-worker`. One fix, two places.

::: {.fieldreport}
**Field report: the silent connection.** This bug would have been invisible in integration
tests because those run one query per connection. It only appears under sustained
sequential load, exactly the pattern a nightly benchmark run produces. The benchmark
suite found it on day one. Without the suite, we would have found it in production
when a user's long-running dbt job silently stalled at 2am.
:::

### Double-quoted identifiers

TPC-DS query 23 uses a column alias `"excess"`. DataFusion treats double-quoted identifiers as case-sensitive column references, not aliases. The query parsed, planned, and started executing, then failed when the physical plan tried to resolve a column named `excess` that didn't exist because it was stored internally as `EXCESS`.

This is a DataFusion behaviour, not a bug. The SQL standard says double-quoted identifiers are case-sensitive. But Trino treats them as case-insensitive aliases, and the standard TPC-DS queries were written for Trino (or Hive, or Presto). We had to modify three TPC-DS query files to use unquoted aliases.

The lesson: standard benchmark queries are not standard. They are written for a specific engine's dialect, and every other engine needs to adapt them.

### Table name as column alias

TPC-DS query 47 used a column alias that happened to share a name with a table: `AS store`. Our table qualifier replaced it with `AS tpcds_sf1.store`, which is not valid SQL. This is the kind of bug you cannot predict in advance. You only find it by running the actual queries.

The fix was adding `AS` detection to the qualifier:

```rust
// Skip if preceded by "AS " (this is an alias, not a table ref)
if upper_before.ends_with(" AS") {
    output.push_str(&remaining[..end]);
    remaining = &remaining[end..];
    continue;
}
```

Three bugs, one pattern: the benchmark suite is a parser fuzzer that uses real SQL instead of random strings.

The remaining bugs from the first run included: a `DATE` literal format that DataFusion expected as `DATE '1998-01-01'` but two TPC-H queries expressed as `'1998-01-01'::DATE` (PostgreSQL syntax); a `BETWEEN` clause that needed explicit type casting on the boundary values; and four ClickBench queries that used `COUNT(DISTINCT column)` with `NULL` values, which DataFusion and Trino handle differently (DataFusion excludes NULLs, which is correct per the SQL standard, but the expected results were generated from ClickHouse which includes them in some edge cases).

Every one of these was a real SQL compatibility issue. Not a performance problem. Not an architecture problem. A "this valid SQL does not work in our engine" problem. The benchmark suite found them all in one afternoon. Without it, users would have found them one at a time, each filed as a support ticket.


## The Dual-Protocol Client

The `BenchClient` trait is the benchmark suite's key abstraction. It allows the same test runner to target SQE (via Flight SQL) or Trino (via HTTP REST) with identical queries and identical result comparison.

```rust
#[async_trait]
pub trait BenchClient: Send + Sync {
    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>>;
    async fn execute_update(&self, sql: &str) -> anyhow::Result<()>;
    fn protocol_name(&self) -> &str;
}
```

The Flight SQL client connects via gRPC, authenticates with handshake or OAuth2 client credentials, and creates a fresh connection per query to avoid the HTTP/2 stream accumulation bug described in Chapter 14.

The Trino client implements the Trino v1 statement protocol: POST the SQL, poll `nextUri` until the state is `FINISHED`, collect all data pages, and convert the JSON-encoded rows into Arrow RecordBatches. This conversion is lossy. Trino returns numbers as JSON, and `decimal(18,2)` becomes `Float64` in our simplified mapping. Good enough for comparison. Not good enough for production.

The Trino client also needed to handle the pagination model correctly. Trino's v1 statement API returns results across multiple pages, each with a `nextUri` to poll. The client accumulates all pages before converting to Arrow. A subtle issue: the first response sometimes carries the column schema, sometimes doesn't. It arrives in a later page when the query planner takes time to resolve types. The client handles both cases.

```rust
// A later page may carry the column metadata when the first didn't.
if columns.is_none() {
    columns = page.columns;
}
```

The type conversion from Trino's JSON representation to Arrow is deliberately simplified. Trino's `decimal(18,2)` becomes Arrow `Float64` because building a proper fixed-point mapping was not worth the effort for a benchmark comparator. The precision loss is within our epsilon tolerance. If we ever needed exact Trino-to-Arrow conversion, we would use the Trino Flight SQL endpoint instead of the HTTP REST protocol.

The dual-protocol design means we can run the exact same 222 queries against both engines on the same data and compare wall-clock times. No excuses about query formulation differences or data format advantages. Same SQL. Same tables. Same network. Same hardware.


## The Caching Story

The first round of Trino comparisons told us something uncomfortable. SQE was correct. Every query returned the right answer. But on ClickBench and short analytical queries, Trino was faster. Not by a little. By 2-3x on warm queries.

The profiling told us where the time went. Not in DataFusion. Not in Parquet reads. In everything *around* the query: creating a REST catalog client (~250ms), fetching an OAuth token from Polaris (~120ms), building a DataFusion SessionContext with 70+ UDFs and TVFs (~50ms). Every single query paid these costs. Trino paid them once at startup and amortized over thousands of queries.

The fix was a multi-layer caching strategy modeled on Trino's own architecture but adapted for SQE's stateless, per-user security model:

**Layer 1: RestCatalog cache.** The iceberg-rust `RestCatalog` is expensive to create. It negotiates with Polaris, discovers endpoints, and builds an HTTP client with S3 credentials. We cache the `RestCatalog` instance per token fingerprint with a 5-minute TTL. The same user's second query skips the 250ms creation cost entirely.

**Layer 2: Table metadata cache.** Polaris returns full Iceberg table metadata on every `loadTable` call: schema, partitions, sort order, current snapshot, all properties. We cache this globally (shared across all sessions) with a 30-second TTL. The TTL is short enough that schema changes propagate within a query cycle, long enough that a 99-query TPC-DS run doesn't hammer Polaris 1,500 times.

**Layer 3: Manifest file cache.** Iceberg manifest files are immutable by specification. Once written, their content never changes. We cache parsed manifest entries by S3 path with no TTL, only LRU eviction at 512MB. This eliminates the most expensive I/O in scan planning: reading and parsing manifest files to determine which data files to scan.

**Layer 4: SessionContext cache.** The DataFusion `SessionContext` wraps an `Arc<SessionState>` internally. Cloning it is O(1). We cache the fully-wired context (UDFs, TVFs, catalog providers, system tables) per username with a 5-minute TTL. The key insight: cache by *username*, not by token fingerprint, because OIDC creates a fresh token per request but the same user has the same catalog access.

**Layer 5: OAuth service token cache.** The `client_credentials` grant to Polaris returned the same-scope token every time, but we were fetching it fresh on every HTTP request. Now it's cached in-process and reused until near-expiry.

The cache invalidation was the hard part. Caching the SessionContext means caching the catalog provider's namespace list. When `CREATE TABLE tpch_sf0_01.lineitem AS SELECT ...` runs, it creates a new table in a namespace. But the cached SessionContext's catalog provider has the *old* namespace list frozen at construction time. The next `SELECT * FROM tpch_sf0_01.lineitem` returns "table not found."

The fix: invalidate the SessionContext cache after every schema-modifying operation: `CREATE TABLE`, `DROP TABLE`, `CREATE SCHEMA`, `ALTER TABLE`, `CTAS`. The invalidation is cheap (one cache remove). The cost of rebuilding the SessionContext on the next query is the original ~50ms. But that only happens once per DDL operation, not once per query.

The result was dramatic. Server-side query execution dropped from ~540ms to under 1ms on cache-warm queries. The `SELECT 1` test showed 0.4ms server-side processing with both caches hitting.

::: {.fieldreport}
**Field report: the token fingerprint that never matched.** The SessionContext cache initially used
a hash of the bearer token as the cache key. Cache hit rate: 0%. Every OIDC request generates a
fresh token with a new JTI claim. Same user, different token, different hash, always a cache miss.
Switching the key to `session.user.username` fixed it immediately. The eprintln debug line that
proved the fix was the fastest 10 seconds of debugging in the project.
:::


## Where SQE Wins

The automated `--compare-trino` benchmark runner tells the story with numbers, not narratives. Every query runs against both engines on the same data, same hardware, same network. The comparison results from April 2026 across all seven suites:

| Suite | SQE (ms) | Trino (ms) | Avg Speedup | Match Rate |
|---|---|---|---|---|
| TPC-H (22 queries) | 1,646 | 10,796 | **8.8x** | 22/22 |
| SSB (13 queries) | 710 | 2,045 | **3.2x** | 13/13 |
| TPC-DS (99 queries) | 19,650 | 46,989 | **2.6x** | 93/99 |
| TPC-C (8 read queries) | 304 | 1,528 | **5.5x** | 8/8 |
| TPC-E (11 queries) | 474 | 2,175 | **5.3x** | 11/11 |
| TPC-BB (10 queries) | 1,223 | 2,193 | **3.1x** | 10/10 |
| ClickBench (43 queries) | 904 | 2,205 | **2.5x** | 43/43 |

SQE is faster than Trino on every suite. Not by a little. By 2.5x to 8.8x. The TPC-H result is the most dramatic: 8.8x average speedup, with individual queries ranging from 1.9x (q15) to 66.9x (q01). That 66.9x is not a typo. TPC-H q01, the classic pricing summary report, runs in 34ms on SQE versus 2,275ms on Trino. Trino's overhead dominates when the actual computation is trivial.

The ClickBench results deserve attention too. 43 queries, all matched, 2.5x average speedup. On a single wide table with 105 columns, SQE's Arrow-native pipeline and direct Parquet read path make the difference. No JSON serialization in the result path. No Trino worker scheduling overhead for a single-partition scan.

The only queries where Trino approaches parity are the tail end of TPC-DS, queries with deeply nested subqueries and 6+ table joins where Trino's mature cost-based optimizer makes better join ordering decisions. Even there, SQE is never slower than 0.6x (TPC-DS q07, q84). The caching layers ensure that catalog overhead never dominates, leaving the comparison purely about query execution.

Six TPC-DS queries show "DIFF" status: row count differences of exactly 1 row. These are ROLLUP edge cases where DataFusion and Trino disagree on the grand total row for empty GROUP BY inputs (apache/datafusion#21570). Not wrong. Just different. The six "diff" queries are q18, q27, q36, q67, q70, q86, all ROLLUP queries returning an extra or missing total row.

Auth overhead. SQE's bearer token is already present in the session. Passthrough to S3 and Polaris adds zero round-trips. Trino's service account model requires an additional token exchange per catalog access. On short queries, this overhead is noise. On a batch of 50 dbt models, each issuing 3-5 queries, the accumulated overhead is measurable. The TPC-H comparison shows this clearly: most of Trino's 10.8 seconds is spent on overhead that has nothing to do with query execution.


## Where Trino Still Has Advantages

Large-scale shuffle at terabyte scale. Trino's exchange operators are battle-tested across thousands of production clusters. At SF0.01, the data fits in memory and SQE's streaming pipeline dominates. At SF1000, when a query requires redistributing billions of rows across workers for a hash join, Trino's network layer may be more efficient. We haven't tested at that scale yet.

Join order optimization for 8+ table queries. Trino's cost-based optimizer has a decade of tuning for complex join graphs. DataFusion's optimizer is good (and improving with every release) but some TPC-DS queries with deeply nested correlated subqueries still show Trino producing marginally better plans. The gap is narrowing with each DataFusion version.

Ecosystem breadth. Trino has connectors for Hive, Delta Lake, MySQL, PostgreSQL, Elasticsearch, and dozens more. SQE targets one format (Iceberg) via one catalog (Polaris). This is intentional. Sovereignty means controlling the stack, not connecting to everything.


## Why That Matters

SQE is not competing with Trino on TPC-DS rankings. It is built for a specific workload: analytical scans of large Iceberg tables with strict per-user authentication and policy enforcement. That workload looks like ClickBench and TPC-H query 1, not TPC-DS query 64.

The benchmark results confirm the architecture matches the use case. But more than that, they confirm something unexpected: the caching work didn't just close the gap with Trino. It opened one. The five-layer caching strategy turned SQE from "competitive" to "dominant" on the workload it was built for.

| Workload pattern | SQE vs Trino | Why |
|---|---|---|
| Single-table scan with filters | SQE 2.5x faster | Arrow-native, no scheduling overhead |
| Projection-heavy (few columns) | SQE 3-8x faster | Direct Parquet read, no serialization |
| 2-3 table joins with aggregation | SQE 2-5x faster | Cached catalog, streaming pipeline |
| Complex TPC-DS analytics | SQE 2.6x faster avg | Caching eliminates metadata overhead |
| Short OLTP-style reads | SQE 5-9x faster | Sub-ms server-side with warm cache |
| Auth-heavy workloads | SQE measurably faster | Zero-overhead passthrough |

If your workload is analytical queries over Iceberg tables (and that is the workload SQE was built for) the numbers are unambiguous. SQE is faster. Not because Rust is faster than Java (though it helps). Because the architecture eliminates overhead that Trino cannot: per-query authentication, per-query catalog creation, JSON serialization in the result path. The caching layers amplify this: warm queries on SQE cost less than 1ms of server overhead. Trino's warm queries still cost the HTTP protocol round-trip plus worker scheduling.

::: {.antipattern}
**Antipattern: Benchmark-Driven Architecture.** TPC-H is a synthetic workload from 1992.
If you are making architectural decisions based on TPC-H rankings, you are optimising for
a workload your users will never run. Profile your actual queries. Identify which pattern
dominates. Then choose the engine that handles that pattern, not the engine that wins
the benchmark nobody runs.
:::


## The Compare Engine

Benchmark results are only useful if you can verify correctness, not just speed. A query that returns wrong answers in half the time is not an improvement.

The `compare.rs` module compares actual Arrow RecordBatches against expected CSV files with type-aware tolerance:

```rust
pub fn compare_results(
    actual: &[RecordBatch],
    expected_csv: &str,
    epsilon: f64,
) -> anyhow::Result<CompareStatus> {
    let (headers, expected_rows) = parse_csv(expected_csv)?;
    let actual_rows = batches_to_string_rows(actual)?;

    if actual_rows.len() != expected_rows.len() {
        return Ok(CompareStatus::Fail(format!(
            "row count mismatch: got {}, expected {}",
            actual_rows.len(), expected_rows.len()
        )));
    }

    // Sort both lexicographically -- order-independent comparison
    let mut actual_sorted = actual_rows;
    actual_sorted.sort();
    let mut expected_sorted = expected_rows;
    expected_sorted.sort();

    // Compare row by row with epsilon tolerance for floats
    // ...
}
```

Both sides are sorted before comparison, making the check order-independent. Floating-point columns get epsilon tolerance (default 1e-4). Decimal columns with trailing zeros are normalized: `123.4500` matches `123.45`. These details sound trivial. They are not. Without them, half of TPC-H produces false failures because DataFusion and the CSV reference use different decimal formatting.

The cell-to-string conversion handles every Arrow type from `Int8` to `Decimal128` to `TimestampMicrosecond`:

```rust
DataType::Decimal128(_, scale) => {
    let raw = array
        .as_primitive::<Decimal128Type>()
        .value(row);
    let scale = *scale as u32;
    if scale == 0 {
        format!("{raw}")
    } else {
        let divisor = 10i128.pow(scale);
        let integer = raw / divisor;
        let frac = (raw % divisor).unsigned_abs();
        format!("{integer}.{frac:0>width$}", width = scale as usize)
    }
}
```

Getting `Decimal128` formatting right took three iterations. The first version used Rust's built-in float formatting, which lost precision. The second version got the integer/fraction split wrong for negative numbers. The third version, above, handles the full range. It has its own unit test with edge cases.


## The Benchmark That Actually Mattered

After two weeks of benchmark development, we had impressive numbers. TPC-H at scale factor 10, all 22 queries passing, competitive with Trino on most, faster on scans. The slide deck looked good.

Then we ran the actual workload.

Fifty dbt models. Nightly batch. Three concurrent users. The kind of workload the engine was built for. It was not a benchmark suite. It was a staging deployment with real data transformations, real schema evolution, and real users running ad-hoc queries while the batch was running.

The results did not match the benchmarks.

TPC-H runs queries one at a time, sequentially, on static data. The real workload runs queries concurrently, with writes happening between reads, with users competing for the same coordinator resources. The scan performance advantage was still there. The auth overhead advantage was still there. But the total wall-clock time was dominated by things TPC-H does not measure: schema discovery latency, CTAS commit time, namespace creation, catalog lock contention.

Total wall-clock time for the nightly batch:

| Metric | SQE | Trino |
|---|---|---|
| 50 dbt models, sequential | 14m 20s | 18m 45s |
| 50 dbt models, 3 concurrent users | 22m 10s | 24m 30s |
| Ad-hoc queries during batch | 0.8s avg | 1.2s avg |
| Time to deploy from zero | 4 minutes | 45 minutes |

SQE was faster. Not dramatically, 10-20% depending on the metric. The dramatic difference was the last row. Deploying SQE is one Helm chart with a coordinator and two workers. Deploying Trino is a coordinator, multiple workers, a service account, a catalog configuration, a Hive metastore (or separate catalog service), and a security configuration that takes longer to get right than the engine itself.

The benchmark that mattered was not query latency. It was operational cost. One Helm chart versus fourteen services. One bearer token model versus a service account matrix. One engineer maintaining it versus a team.

There is another number in that table that deserves attention: ad-hoc query latency during the batch. 0.8 seconds versus 1.2 seconds. That gap is not about raw engine speed. It is about resource isolation. SQE runs each query as the authenticated user, with per-query memory limits and independent DataFusion session contexts. A heavy dbt CTAS running on one session does not starve ad-hoc queries running on another. Trino's resource groups can achieve similar isolation, but configuring them correctly is a project in itself. SQE gets isolation by default because the architecture enforces it.

::: {.fieldreport}
**Field report: the number that convinced management.** We presented the TPC-H numbers.
Management nodded politely. We presented the dbt batch wall-clock comparison. They
nodded more enthusiastically. We presented the deployment comparison, 4 minutes versus
45 minutes, and they approved the migration. The performance was the supporting evidence.
The operational simplicity was the argument.
:::


## One Week: From Losing to Dominant

The benchmark JSON reports accumulate in `benchmarks/results/`. They are not a dashboard. They are a historical record. And the historical record from April 2026 tells a story about what happens when you focus on correctness first and performance second.

On April 2, SQE ran 192 out of 222 benchmark queries. Thirty queries failed: missing UDFs, unsupported SQL features, ROLLUP edge cases. The queries that passed took 126 seconds total. Respectable for a single-node engine, but not competitive with Trino.

On April 10, SQE ran 218 out of 222 queries. We had added 70+ Trino-compatible UDFs, streaming writes, sort-order safety, and IN-subquery rewrite. The pass count jumped from 192 to 218. But the total time *increased* to 154 seconds. Every query now did more work: building SessionContexts with more UDFs, resolving more catalog metadata. We were more correct and slower. The first Trino comparison runs showed SQE losing on every suite. ClickBench: 0.1x Trino. TPC-H: 0.6x Trino. The numbers were discouraging.

On the morning of April 12, we landed the first three caching layers: RestCatalog cache, table metadata cache, manifest file cache. SQE reached rough parity with Trino, 1.0x to 1.4x depending on the suite. Competitive, not dominant.

On the afternoon of April 12, we landed the SessionContext cache and OAuth service token cache. The effect was immediate.

The speedups below are SQE's own improvement over time (April 2 baseline to April 12 final), not SQE vs Trino.

| Suite | Apr 2 | Apr 10 | **Apr 12** | Speedup |
|---|---|---|---|---|
| TPC-H | 13.6s | 18.5s | **1.6s** | 8.5x |
| SSB | 7.7s | 8.6s | **0.7s** | 11x |
| TPC-DS | 68.3s | 77.1s | **13.0s** | 5.3x |
| ClickBench | 23.5s | 24.3s | **0.6s** | 39x |
| TPC-C | 2.8s | 7.6s | **0.9s** | 3.1x |
| TPC-E | 3.6s | 9.1s | **1.0s** | 3.6x |
| TPC-BB | 6.9s | 7.4s | **1.1s** | 6.3x |
| **Total** | **126s** (192/222) | **154s** (218/222) | **19s** (221/222) | **6.7x** |

The Trino comparison reversed completely:

| Suite | Apr 10 vs Trino | **Apr 12 vs Trino** |
|---|---|---|
| TPC-H | SQE 0.6x (lost) | **SQE 8.8x** |
| SSB | SQE 0.3x (3x slower) | **SQE 3.2x** |
| TPC-DS | SQE 0.5x (2x slower) | **SQE 2.6x** |
| ClickBench | SQE 0.1x (10x slower) | **SQE 2.5x** |
| TPC-C | SQE 0.5x | **SQE 5.5x** |
| TPC-E | SQE 0.4x | **SQE 5.3x** |
| TPC-BB | 0/10 match (broken) | **SQE 3.1x** (10/10) |

On April 10, SQE lost every Trino comparison. On April 12, it won every one at SF0.01. Then came the real test: scale factor 1.

At SF1 (1 GB per suite, real data volumes), the picture is more nuanced. Caching overhead is amortized. I/O and join execution dominate. The May 7 SF1 numbers, after the column-statistics work landed:

| Suite | SQE | Trino | Avg speedup | Winner |
|---|---|---|---|---|
| TPC-H (22) | 19.3s | 26.6s | **2.3x** | SQE |
| SSB (13) | 7.6s | 8.3s | **1.1x** | SQE |
| TPC-DS (99) | 57.1s | 39.7s | **1.4x** | Mixed |
| TPC-C (8 read) | 0.45s | 3.4s | **9.6x** | SQE |
| TPC-E (11) | 10.4s | 138.8s | **7.8x** | SQE |
| TPC-BB (10) | 36.9s | 323.6s | **5.5x** | SQE |
| ClickBench (43) | 1.7s | 6.3s | **4.6x** | SQE |

SQE wins 6 of 7 suites. The "avg speedup" column is the per-query mean: SQE wins most TPC-DS queries handily, but a small number of pathological queries skew the total against us. q72 is the headline case: a 10-table join with an 11.7 million row inventory cross-reference that takes 16 seconds on SQE versus 1.2 seconds on Trino. DataFusion lacks the full cost-based join enumeration with NDV that Trino uses to reorder this chain optimally (upstream DF#3843). Without q72, SQE's TPC-DS total flips from a loss to a win.

The optimizations that closed the SF1 gap, in chronological order: table-level statistics from Iceberg snapshot summary (April), the star-schema join reorder rule (April), broadcast threshold raised to 64 MB matching Trino and Spark (April), dynamic filter type coercion with Int32-to-Int64 widening (April), and column-level statistics aggregated from manifest entries (May). The last one was the missing ingredient. Without per-column min/max/null_count, DataFusion's `JoinSelection` could swap build and probe sides but could not estimate filter selectivity or pick join order on multi-way chains. Adding it dropped TPC-DS SF1 by 21% on the dedicated comparison run, with q72 falling from 24.8s back to its April baseline.

::: {.fieldreport}
**Field report: q73 was a planning sensitivity, not a regression.** Right after column-stats landed, q73 looked 84% slower (458ms -> 842ms). The next run came back at 258ms. Same code, different cache state, different plan. With more selectivity bounds available the optimizer has more degrees of freedom, and q73's OR-heavy `WHERE` clause sits in a region where small changes in estimated row counts flip the join order. We logged it, kept the change, and noted the lesson: better statistics make most plans better and a few plans more variance-prone. The fix when it bites is column histograms or NDV, both still upstream gaps in DF.
:::

::: {.fieldreport}
**Field report: correctness before speed.** We spent April 6-10 making SQE slower but more correct. Adding UDFs increased SessionContext build time. Adding streaming writes increased CTAS overhead. Adding sort-order safety added metadata checks. Every feature made the pass count go up and the runtime go up with it. Then caching made everything fast. If we had optimized first, we would have built caches for code paths that didn't work yet. Correctness first, speed second. The order matters.
:::


## The Benchmark as Regression Suite

The seven suites serve double duty. On commit, CI runs TPC-H at scale factor 0.01, just enough data to verify correctness, fast enough to finish in under a minute. The test is not "is SQE fast?" The test is "did this commit break any of the 22 queries that worked yesterday?"

Nightly, CI runs the full suite at scale factor 0.01 with the `--compare-trino` flag. This catches both correctness regressions and performance regressions in a single run. The Trino container starts automatically, the same queries run against both engines, and the comparison JSON report captures per-query timing and row-count matching.

The shell scripts orchestrate the full pipeline:

```bash
# Generate + load + test, all seven benchmarks
./scripts/benchmark-test.sh

# Just TPC-H and SSB at scale factor 10
BENCH_SCALE=10 ./scripts/benchmark-test.sh tpch ssb

# Compare SQE vs Trino on all suites
./scripts/benchmark-test.sh --compare-trino

# Compare on a single suite
BENCH_SCALE=0.01 ./scripts/benchmark-test.sh --compare-trino tpch
```

The exit code is 0 if every query passes or is skipped. Non-zero if any query fails or errors. CI gates on this. You cannot merge a commit that breaks a benchmark query.

Every benchmark run produces a JSON report with per-query timing. The comparison runs produce a second JSON report with SQE-vs-Trino speedup per query. Over time, these reports build a performance history. We do not have a fancy dashboard. We have a directory of JSON files and a grep command. It is enough.

The `benchmark-test.sh` script produces a summary table at the end that gives a single-screen overview across all suites:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Benchmark Results (SF0.01, FLIGHT + Trino comparison)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Benchmark       Pass  Fail  Diff  Skip Error Total     Time
  ──────────────────────────────────────────────────────────────
  tpch              22     0     0     0     0    22     1.6s
  ssb               13     0     0     0     0    13      .6s
  tpcds             99     0     0     0     0    99    12.9s
  tpcc              17     0     0     0     0    17      .8s
  tpce              17     0     0     0     1    18     1.0s
  tpcbb             10     0     0     0     0    10     1.0s
  clickbench        43     0     0     0     0    43      .6s
  ──────────────────────────────────────────────────────────────
  TOTAL            221     0     0     0     1   222    18.8s
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

221 out of 222 queries passing (99.5%). One error, a known `trade_result_update_holding` execution failure on TPC-E. Zero failures, zero diffs, zero skips: no crashes, no timeouts, no connection hangs. TPC-DS runs 99/99 with ROLLUP now enabled. TPC-C runs all 17 queries including write-path DML (DELETE, UPDATE via CoW). ClickBench runs 43/43. The Trino side-by-side comparison shows SQE winning every suite, with 93+ of 99 TPC-DS queries producing identical row counts.

The automated comparison runs both engines against every query and reports three things: timing, row count, and match status. "OK" means both engines returned the same number of rows. "DIFF" means they disagreed, usually a ROLLUP edge case. "FAIL SQE" or "FAIL Trino" means one engine errored. The comparison found six TPC-DS ROLLUP diffs and zero SQE-only failures on the core analytical suites.


## What We Learned

Building the benchmark suite took about as long as building the distributed execution layer. That surprised us. We expected data generators and a test runner, a week's work. We got a SQL dialect compatibility layer, a type-aware result comparator, a dual-protocol client abstraction, and a namespace-aware table qualifier with eleven unit tests. The complexity was not in measuring performance. The complexity was in making the measurement honest.

Three takeaways.

First, benchmarks find bugs faster than unit tests. Unit tests verify the behaviour you anticipated. Benchmark queries exercise the behaviour your users will actually trigger. Every one of the twelve bugs the benchmark suite found on its first run was a real SQL compatibility issue that would have hit production users.

Second, the benchmark that convinces engineers and the benchmark that convinces management are different. Engineers care about p99 query latency. Management cares about total cost of ownership. Both are valid. Build both.

Third, benchmarks mislead when taken in isolation. SQE is 40% faster than Trino on single-table scans. SQE is 30% slower than Trino on complex multi-way joins. Both statements are true. Neither tells you which engine is right for your workload. Only your workload tells you that.

The `sqe-bench` binary is 222 queries of truth. It does not care about your architecture diagrams. It does not care about your Rust evangelism. It runs the queries, measures the time, compares the results, and writes a JSON file. The numbers are what they are.


## The Streaming Execution Effect

After building the streaming execution engine (Chapter 13: coordinator spill-to-disk, late materialization, file-level pruning, S3 I/O pipeline, distributed shuffle) we had a new baseline to compare against. The numbers told a clear story.

### Three configurations, one workload

We ran all 22 TPC-H SF1 queries against three deployments:

| Configuration | Memory | Workers | Pass | Total time |
|---|---|---|---|---|
| Single-node, 8GB (Apr 2 baseline) | 8 GB | 0 | 22/22 | 37.5s |
| Single-node, 512MB + spill | 512 MB | 0 | 21/22 | 33.3s |
| Distributed (coordinator + 2 workers) | 8 GB | 2 | 22/22 | 12.0s |

The 512MB test was deliberately adversarial. We wanted to prove that a coordinator with less memory than a Raspberry Pi could execute analytical queries over 6 million rows without crashing. 21 out of 22 passed. The one failure (q18, the most memory-intensive TPC-H query) hit a known DataFusion limitation where the hash aggregate exhausts its memory reservation before the spill mechanism triggers (DF#17334). With two workers sharing the load, q18 completed in 0.74 seconds.

### Per-query breakdown

The speedup was not uniform. That is the interesting part.

```
Query   Single-node (8GB)   Distributed (2 workers)   Speedup
──────────────────────────────────────────────────────────────
q01            3.21s                1.29s               2.5x
q02            0.89s                0.27s               3.3x
q03            2.23s                0.94s               2.4x
q04            1.14s                0.32s               3.6x
q05            1.89s                0.55s               3.4x
q06            1.13s                0.30s               3.7x
q07            2.07s                0.85s               2.4x
q08            1.81s                0.54s               3.4x
q09            1.78s                0.60s               3.0x
q10            2.47s                0.63s               3.9x
q11            0.74s                0.11s               6.8x
q12            1.71s                0.57s               3.0x
q13            1.10s                0.18s               6.1x
q14            1.46s                0.55s               2.7x
q15            2.24s                0.72s               3.1x
q16            0.75s                0.10s               7.4x
q17            1.89s                0.63s               3.0x
q18            3.19s                0.74s               4.3x
q19            1.68s                0.79s               2.1x
q20            1.39s                0.53s               2.6x
q21            2.11s                0.68s               3.1x
q22            0.67s                0.09s               7.7x
──────────────────────────────────────────────────────────────
TOTAL          37.5s                12.0s               3.1x
```

Three patterns emerge:

**Metadata-light queries (q11, q13, q16, q22) saw 6-8x speedup.** These are small scans over dimension tables or subquery-heavy queries where the bottleneck is plan execution overhead, not I/O. The Parquet footer cache eliminates repeated metadata reads. File-level min/max pruning skips files entirely. The coordinator barely touches S3.

**Scan-heavy queries (q01, q03, q07, q19) saw 2-2.5x speedup.** These read millions of rows from the lineitem table. The speedup is roughly proportional to the worker count: two workers scan in parallel, each reading half the files. Add more workers, get proportional improvement. This is the Amdahl's Law case: the scan is the parallelizable part.

**Join-heavy queries (q05, q08, q09, q18) saw 3-4x speedup.** This is where the streaming execution architecture pays off. The SortMergeJoin fallback prevents OOM on large hash tables. Late materialization reduces the data flowing into the join (read only the predicate columns, filter, then fetch the rest). Predicate transfer pushes join keys from the build side to the probe side, skipping files that cannot match.

### What the 512MB test proved

The 512MB test was not about performance. It was about safety. Before the streaming execution engine, a coordinator with 512MB would be killed by the OS after the first analytical query. After: 21 of 22 TPC-H queries completed. The coordinator allocated memory, hit the watermark, spilled sorted runs to disk, and continued processing. The `sqe_coordinator_memory_pressure` gauge ticked from green (0) through yellow (1) and back, never reaching red (3). That is the design working as intended.

The single failure (q18) is instructive. DataFusion's `GroupedHashAggregateStream` does not yet support cooperative spill. It allocates memory for its hash table, and if the pool is exhausted before the table is complete, the operator fails rather than spilling. This is a known upstream limitation (DataFusion issue #17334). The fix is either more memory (1GB is enough), distributed aggregation (workers each handle a partition of the hash table), or an upstream improvement to the hash aggregate's memory accounting. We chose to document it rather than hide it. The benchmark is not there to make us look good. It is there to show what works and what does not.

### The full matrix: five suites, three configs

We did not stop at TPC-H. The benchmark matrix ran all five suites across all three deployment configurations.

```
Suite (queries)   single-512mb     single-8gb     distributed-2w
──────────────────────────────────────────────────────────────────
TPC-H  (22)       21/22 (29.6s)   22/22 (28.6s)   22/22 (13.5s)
TPC-DS (99)       92/99 (94.1s)   99/99 (99.4s)   98/99 (36.1s)
SSB    (13)        4/13 (14.4s)   13/13 (14.3s)   13/13  (5.3s)
TPC-C  (17)       17/17 (21.5s)   17/17 (22.0s)   17/17  (8.6s)
TPC-E  (18)       12/18  (8.4s)   13/18 (127.4s)  10/18 (56.0s)
──────────────────────────────────────────────────────────────────
Total (169)       146 (86%)       164 (97%)        162 (96%)
```

The spill data told a story we did not expect:

| Config | Sort Spills | Bytes Spilled |
|---|---|---|
| single-512mb | 30 | 1.1 GB |
| single-8gb | 128 | 27.7 GB |
| distributed-2w | 3 | 49 MB |

The 8GB configuration spilled *more* than the 512MB one. This is not a bug. It is an artifact of success: 8GB successfully runs TPC-E queries that 512MB cannot even start. Those TPC-E queries involve multi-table joins across 33 brokerage tables (trade to customer_account to customer to address to zip_code) producing 27GB of intermediate sorted data. With 512MB, the hash aggregate runs out of memory before any data reaches the sort operator. With 8GB, the join completes, the sort starts, and the sort spills. The spill is the system working as designed.

With two workers, spill dropped to 49MB. Workers absorb scan and partial aggregation work. The coordinator barely touches raw data. It merges small, pre-processed result sets.

One finding surprised us: at SF1, the distributed-2w configuration ran all queries locally on the coordinator (`scheduler_decisions{local}=120+`). Not a single query was distributed to workers. SF1 tables have 1-2 data files each, below the distribution threshold of 4 files. The 2.5x speedup we measured was not from distribution. It was from the streaming execution improvements: spill-to-disk, late materialization, scan planning optimizations. The workers were idle. To see actual distribution, run at SF10 or higher, where tables have enough files to justify splitting across workers.

### Storing results for history

All benchmark JSON results are committed to `benchmarks/results/` in the repository. This is deliberate. A benchmark run that is not committed is a benchmark run that never happened. When a future change introduces a regression (and it will) the historical results provide the baseline. You do not need to remember what the numbers were. You `git log benchmarks/results/` and the history is there.

The naming convention encodes everything you need: `tpch-sf1-flight-2026-04-06T20:57:10.json` tells you the benchmark, scale factor, protocol, and exact timestamp. Compare any two files and you have a regression test.

::: {.ailog}
**AI Logbook:** The benchmark generators were pure AI work: 24 TPC-DS tables, 8 TPC-H tables, 9 TPC-C tables, all with correlated random data using seeded RNGs. The human specified which columns should correlate and what scale factor functions to use. The table qualification bug that broke 12 queries (`part` matching inside `partsupp`) was introduced by the AI's naive string replacement and found by the AI during the first live run. The context-aware `prefix_tables` function with its 11 unit tests was the AI's fix; the human's contribution was the rule "longest-name-first."
:::

The hard part is knowing which numbers to look at.


## Agreement Is Not Validation

Three months after the suites stabilised, a compare run produced the best report we had ever seen. Every query on every suite, run against both SQE and Trino on the same Iceberg tables, every result row diffed. Zero mismatches. The screenshot kind of report.

It was hiding twelve broken queries and a benchmark suite with zero warehouses.

The hole in a differential harness is structural. Two engines read the same files. If the generated data contains no rows a query can select, both return empty, empty equals empty, and the harness prints Match. The diff validates the engines against each other and validates the data against nothing. We had already made the blind spot visible by reporting zero-rows-on-both as `Vacuous` instead of `Match`. At SF0.1, TPC-DS showed 29 of them. The comfortable explanation was scale: small data, selective predicates, some queries legitimately come up empty. The explanation was plausible and untestable from inside the harness.

The way out is a referee that does not share the data path. DuckDB ships the official TPC-DS generator as an extension: `CALL dsdgen(sf=0.1)` produces the spec's own data. So we ran all 99 queries inside DuckDB twice, once against official data, once against ours. No SQE, no Trino. A query that returns rows on official data and none on ours is a generator bug, proven without either engine in the loop.

Sixteen of the 29 vacuous queries failed that test.

The causes were vocabulary. Counties drawn from a random name generator, where the qualification queries probe `Williamson County` by name. Eight invented colors where dsdgen has 92 and a query wants `slate`, `blanched`, `burnished`. Item classes named `Class1` through `Class5` where the real ones are `romance` and `dvd/vcr players`. The deepest was q63: it filters brand AND category AND class together, and in dsdgen output the brand name is a deterministic function of the category and class. Every `Electronics`/`portable` item is some `scholaramalgamalg #N`. Draw brands independently and the conjunction is unsatisfiable. Synthetic data has structure the queries depend on, and the structure goes deeper than any column profile shows.

TPC-C was a one-line bug with total reach. `let num_warehouses = scale as i32` truncates to zero at scale 0.1, and a `.min(num_warehouses)` clamp pinned every warehouse foreign key to a `w_id` of 0. The warehouse table had one row, with id 1. Every join in every query returned the empty set, and both engines agreed it did.

The same pass settled the one genuine engine disagreement of the day, and not the way we expected. TPC-DS q75 returned 57 rows on SQE and 55 on Trino. Our track record says assume SQE is broken. The query keeps rows where a year-over-year sales ratio, computed as `DECIMAL(17,2)` divided by `DECIMAL(17,2)`, is below 0.9. The two extra rows had true ratios of 0.8983 and 0.8984. Trino computes that division at scale 2, both ratios round up to 0.90, and the rows vanish. DataFusion keeps a higher-scale quotient. DuckDB, on the same parquet files, returns SQE's 57 rows exactly. Decimal division scale is implementation-defined, so nobody gets a bug report. But without the third engine we would have spent the afternoon hunting a defect in our own decimal kernel that does not exist.

Three rules came out of that day.

Agreement is not validation. Count your empty results and treat them as debt with a name attached.

The oracle must not share the data path. The reference generator runs in-process in DuckDB and a full validation pass costs minutes.

When two engines disagree, get a third opinion before you debug. The bug you are about to hunt in your own code might be the other engine rounding at scale 2.

::: {.ailog}
**AI Logbook:** The vacuous investigation was a single AI session: the agent proposed DuckDB's dsdgen as the oracle, wrote the validation harness, diffed the vocabulary distributions, and reverse-engineered the brand-name function by querying the official data for `(category, class, brand)` triples. The human's contribution was one sentence of direction: "validate the vacuous with DuckDB, might be still error in data generation or storage in Iceberg." That sentence contained the key insight the agent then operationalised: the split between generator bugs and storage bugs is exactly the split between DuckDB-on-parquet failing and DuckDB-on-parquet passing while both engines fail.
:::

The hard part is knowing which numbers to look at. The harder part is knowing when a clean report means nothing happened.
