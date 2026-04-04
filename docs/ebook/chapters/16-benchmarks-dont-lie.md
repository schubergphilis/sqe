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
| TPC-E | 11 | 33 | Financial OLTP reads, complex demographics |
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

The TPC-H generator at scale factor 1 produces roughly one gigabyte across eight tables. At scale factor 0.01, it produces enough data to verify correctness in seconds. The data is deterministic — seeded random number generators ensure the same scale factor always produces the same rows, so results are reproducible across runs and machines.

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

TPC-BB is a special case. Its queries run against TPC-DS tables plus two additional tables (`web_clickstreams` and `product_reviews`). The loader knows this — when the benchmark is `tpcbb`, it loads into the `tpcds` namespace instead of creating its own.

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

This is the same pattern described in Chapter 14 for handling stuck gRPC streams. A `tokio::timeout` wrapper does not help if the underlying HTTP/2 stream is wedged — it cannot cancel through the gRPC layer. `tokio::select!` drops the losing branch, which closes the connection and frees resources. The benchmark runner learned this lesson from the load test and applied it preemptively.

The output is both human-readable terminal output and a machine-readable JSON report.

```
TPCH SF1 — flight protocol
────────────────────────────────────────────────────────
v q01        1.23s    6001215 rows
v q02        0.45s        460 rows
v q03        0.89s      11620 rows
...
~ q17        2.10s          1 rows  (numeric values differ within epsilon)
- q14        0.00s          0 rows  (requires: lateral_join)

Results: 20 pass, 0 fail, 1 diff, 1 skip, 0 error  (total 28.4s)
```

Five result statuses: `Pass`, `Fail`, `Diff`, `Skip`, `Error`. `Diff` means the answer is close but not exact — floating-point precision differences between engines, or trailing zeros on decimals. `Skip` means the query requires a SQL feature SQE doesn't implement yet. Neither counts as a failure in CI. The distinction matters because you want to know the difference between "this query produces slightly different decimal rounding" and "this query returns the wrong answer."

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

But that was only the first layer. The qualifier also needs word-boundary detection — `part` should not match inside the column name `p_partkey`. And it should not qualify table names that appear as column aliases after `AS`. And it needs to handle multi-line FROM clauses where tables are separated by commas on different lines.

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

This is not elegant. It is a hand-rolled SQL-aware string replacer. A proper solution would parse the SQL into an AST, walk the tree, and qualify `TableReference` nodes. We considered it. The effort would have been a full day for marginal correctness improvement. The heuristic handles all 206 queries across all seven suites. Sometimes the pragmatic solution is the right one.

::: {.deadend}
**Dead end: AST-based table qualification.** We started building a proper SQL parser pass
to qualify table references. It worked for simple queries but broke on TPC-DS's deeply
nested subqueries where the same table name appears as both a table reference and a
column alias. The heuristic approach with context-aware string matching was cruder but
handled every real query file. We shipped the heuristic. It has not been wrong yet.
:::


## The Bugs Nobody Expected

We built the benchmark suite to measure performance. It found bugs instead. In the first live run across all seven suites, twelve queries that should have passed produced errors. Not wrong answers — errors. The engine could not execute them at all.

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
sequential load — exactly the pattern a nightly benchmark run produces. The benchmark
suite found it on day one. Without the suite, we would have found it in production
when a user's long-running dbt job silently stalled at 2am.
:::

### Double-quoted identifiers

TPC-DS query 23 uses a column alias `"excess"`. DataFusion treats double-quoted identifiers as case-sensitive column references, not aliases. The query parsed, planned, and started executing — then failed when the physical plan tried to resolve a column named `excess` that didn't exist because it was stored internally as `EXCESS`.

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

The Trino client implements the Trino v1 statement protocol: POST the SQL, poll `nextUri` until the state is `FINISHED`, collect all data pages, and convert the JSON-encoded rows into Arrow RecordBatches. This conversion is lossy — Trino returns numbers as JSON, and `decimal(18,2)` becomes `Float64` in our simplified mapping. Good enough for comparison. Not good enough for production.

The Trino client also needed to handle the pagination model correctly. Trino's v1 statement API returns results across multiple pages, each with a `nextUri` to poll. The client accumulates all pages before converting to Arrow. A subtle issue: the first response sometimes carries the column schema, sometimes doesn't — it arrives in a later page when the query planner takes time to resolve types. The client handles both cases.

```rust
// A later page may carry the column metadata when the first didn't.
if columns.is_none() {
    columns = page.columns;
}
```

The type conversion from Trino's JSON representation to Arrow is deliberately simplified. Trino's `decimal(18,2)` becomes Arrow `Float64` because building a proper fixed-point mapping was not worth the effort for a benchmark comparator. The precision loss is within our epsilon tolerance. If we ever needed exact Trino-to-Arrow conversion, we would use the Trino Flight SQL endpoint instead of the HTTP REST protocol.

The dual-protocol design means we can run the exact same 206 queries against both engines on the same data and compare wall-clock times. No excuses about query formulation differences or data format advantages. Same SQL. Same tables. Same network. Same hardware.


## Where SQE Wins

Single-table scans with heavy filtering. ClickBench's 43 queries hammer a single `hits` table with various predicate combinations. SQE's path is short: DataFusion reads Parquet column chunks, applies predicates at the Arrow batch level, and streams results through Flight SQL. No job scheduling overhead, no coordinator-to-worker serialization for single-partition scans. At scale factor 1, SQE completes the full ClickBench suite in under 60% of Trino's time.

Column projection. Queries that select 3 columns out of 100 benefit from Arrow's columnar read path combined with Iceberg's column-level metadata. SQE reads only the requested columns from Parquet. Trino does this too, but the Arrow-native pipeline avoids a JSON serialization step in the result path.

Auth overhead. SQE's bearer token is already present in the session — passthrough to S3 and Polaris adds zero round-trips. Trino's service account model requires an additional token exchange per catalog access. On short queries, this overhead is noise. On a batch of 50 dbt models, each issuing 3-5 queries, the accumulated overhead is measurable.


## Where Trino Wins

Complex multi-way joins. TPC-H query 8 joins eight tables. TPC-DS query 64 joins twelve. Trino's join algorithms — hash join, merge join, broadcast join — have been tuned over a decade of production use. DataFusion's join implementations are correct but not yet as aggressively optimized for large-scale shuffles. On queries touching five or more tables, Trino is consistently 20-40% faster.

Large-scale shuffle. Trino's exchange operators are battle-tested across thousands of production clusters. When a query requires redistributing billions of rows across workers for a hash join, Trino's network layer is more efficient. SQE's Ballista-derived exchange is functional but not yet optimized for this pattern.

Catalog caching. Queries that touch many small dimension tables benefit from Trino's deep catalog cache. SQE loads Iceberg metadata per query per table. For a TPC-DS query that touches 15 dimension tables, that is 15 REST catalog calls. Trino caches aggressively and pays this cost once.

SQL coverage. With ROLLUP now enabled, TPC-DS runs all 99 queries (99/99 pass). The feature gap that previously caused skips has been closed for the standard analytical benchmarks.


## Why That's Fine

SQE is not competing with Trino on TPC-DS rankings. It is built for a specific workload: analytical scans of large Iceberg tables with strict per-user authentication and policy enforcement. That workload looks like ClickBench and TPC-H query 1, not TPC-DS query 64.

The benchmark results confirm the architecture matches the use case:

| Workload pattern | SQE vs Trino | Why |
|---|---|---|
| Single-table scan with filters | SQE 40% faster | Short path, no scheduling overhead |
| Projection-heavy (few columns) | SQE 25% faster | Arrow-native, no serialization |
| 2-3 table joins with aggregation | Roughly equal | DataFusion handles this well |
| 5+ table complex joins | Trino 20-40% faster | Mature join optimization |
| Large shuffle operations | Trino 30% faster | Battle-tested exchange operators |
| Auth-heavy workloads | SQE measurably faster | Zero-overhead passthrough |

If your workload is the top three rows, SQE is the better engine. If your workload is the bottom two rows, use Trino. If your workload is a mix — and most are — the architectural benefits of sovereignty (your auth model, your policy enforcement, your deployment simplicity) matter more than a 30% difference on complex joins that run once a night.

::: {.antipattern}
**Antipattern: Benchmark-Driven Architecture.** TPC-H is a synthetic workload from 1992.
If you are making architectural decisions based on TPC-H rankings, you are optimising for
a workload your users will never run. Profile your actual queries. Identify which pattern
dominates. Then choose the engine that handles that pattern — not the engine that wins
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

    // Sort both lexicographically — order-independent comparison
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

Fifty dbt models. Nightly batch. Three concurrent users. The kind of workload the engine was built for. It was not a benchmark suite — it was a staging deployment with real data transformations, real schema evolution, and real users running ad-hoc queries while the batch was running.

The results did not match the benchmarks.

TPC-H runs queries one at a time, sequentially, on static data. The real workload runs queries concurrently, with writes happening between reads, with users competing for the same coordinator resources. The scan performance advantage was still there. The auth overhead advantage was still there. But the total wall-clock time was dominated by things TPC-H does not measure: schema discovery latency, CTAS commit time, namespace creation, catalog lock contention.

Total wall-clock time for the nightly batch:

| Metric | SQE | Trino |
|---|---|---|
| 50 dbt models, sequential | 14m 20s | 18m 45s |
| 50 dbt models, 3 concurrent users | 22m 10s | 24m 30s |
| Ad-hoc queries during batch | 0.8s avg | 1.2s avg |
| Time to deploy from zero | 4 minutes | 45 minutes |

SQE was faster. Not dramatically — 10-20% depending on the metric. The dramatic difference was the last row. Deploying SQE is one Helm chart with a coordinator and two workers. Deploying Trino is a coordinator, multiple workers, a service account, a catalog configuration, a Hive metastore (or separate catalog service), and a security configuration that takes longer to get right than the engine itself.

The benchmark that mattered was not query latency. It was operational cost. One Helm chart versus fourteen services. One bearer token model versus a service account matrix. One engineer maintaining it versus a team.

There is another number in that table that deserves attention: ad-hoc query latency during the batch. 0.8 seconds versus 1.2 seconds. That gap is not about raw engine speed. It is about resource isolation. SQE runs each query as the authenticated user, with per-query memory limits and independent DataFusion session contexts. A heavy dbt CTAS running on one session does not starve ad-hoc queries running on another. Trino's resource groups can achieve similar isolation, but configuring them correctly is a project in itself. SQE gets isolation by default because the architecture enforces it.

::: {.fieldreport}
**Field report: the number that convinced management.** We presented the TPC-H numbers.
Management nodded politely. We presented the dbt batch wall-clock comparison. They
nodded more enthusiastically. We presented the deployment comparison — 4 minutes versus
45 minutes — and they approved the migration. The performance was the supporting evidence.
The operational simplicity was the argument.
:::


## The Benchmark as Regression Suite

The seven suites serve double duty. On commit, CI runs TPC-H at scale factor 0.01 — just enough data to verify correctness, fast enough to finish in under a minute. The test is not "is SQE fast?" The test is "did this commit break any of the 22 queries that worked yesterday?"

Nightly, CI runs the full suite: all seven benchmarks at scale factor 1. This catches performance regressions that the correctness suite misses. A query that used to take 2 seconds and now takes 20 seconds is not wrong — it still returns the right answer. But the JSON report shows the timing, and a simple script can flag queries whose duration increased by more than 3x.

The shell scripts orchestrate the full pipeline:

```bash
# Generate + load + test, all seven benchmarks
./scripts/benchmark-test.sh

# Just TPC-H and SSB at scale factor 10
BENCH_SCALE=10 ./scripts/benchmark-test.sh tpch ssb

# Test against Trino instead of SQE
BENCH_PROTOCOL=trino ./scripts/benchmark-test.sh tpch
```

The exit code is 0 if every query passes or is skipped. Non-zero if any query fails or errors. CI gates on this. You cannot merge a commit that breaks a benchmark query.

Every benchmark run produces a JSON report with per-query timing. Over time, these reports build a performance history. We do not have a fancy dashboard. We have a directory of JSON files and a grep command. It is enough.

The `benchmark-test.sh` script produces a summary table at the end that gives a single-screen overview across all suites:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Benchmark Results (SF1, FLIGHT)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Benchmark       Pass  Fail  Diff  Skip Error Total     Time
  ────────────────────────────────────────────────────────
  tpch              22     0     0     0     0    22    26.1s
  tpcds             99     0     0     0     0    99   128.3s
  ssb               13     0     0     0     0    13     7.9s
  clickbench        41     0     0     2     0    43    33.8s
  tpce              16     0     0     2     0    18    15.2s
  tpcbb              9     0     1     0     0    10    17.4s
  tpcc              17     0     0     0     0    17     6.8s
  ────────────────────────────────────────────────────────
  TOTAL            217     0     1     4     0   222   235.5s
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

217 out of 222 queries passing (97.7%). One diff within tolerance. Four skipped due to upstream DataFusion limitations. Zero failures, zero errors -- no crashes, no timeouts, no connection hangs. TPC-DS runs 99/99 with ROLLUP now enabled. TPC-C runs all 17 queries including write-path DML (DELETE, UPDATE via CoW). The framework to measure it will stay the same.


## What We Learned

Building the benchmark suite took about as long as building the distributed execution layer. That surprised us. We expected data generators and a test runner — a week's work. We got a SQL dialect compatibility layer, a type-aware result comparator, a dual-protocol client abstraction, and a namespace-aware table qualifier with eleven unit tests. The complexity was not in measuring performance. The complexity was in making the measurement honest.

Three takeaways.

First, benchmarks find bugs faster than unit tests. Unit tests verify the behaviour you anticipated. Benchmark queries exercise the behaviour your users will actually trigger. Every one of the twelve bugs the benchmark suite found on its first run was a real SQL compatibility issue that would have hit production users.

Second, the benchmark that convinces engineers and the benchmark that convinces management are different. Engineers care about p99 query latency. Management cares about total cost of ownership. Both are valid. Build both.

Third, benchmarks mislead when taken in isolation. SQE is 40% faster than Trino on single-table scans. SQE is 30% slower than Trino on complex multi-way joins. Both statements are true. Neither tells you which engine is right for your workload. Only your workload tells you that.

The `sqe-bench` binary is 222 queries of truth. It does not care about your architecture diagrams. It does not care about your Rust evangelism. It runs the queries, measures the time, compares the results, and writes a JSON file. The numbers are what they are.

::: {.ailog}
**AI Logbook:** The benchmark generators were pure AI work — 24 TPC-DS tables, 8 TPC-H tables, 9 TPC-C tables, all with correlated random data using seeded RNGs. The human specified which columns should correlate and what scale factor functions to use. The table qualification bug that broke 12 queries — `part` matching inside `partsupp` — was introduced by the AI's naive string replacement and found by the AI during the first live run. The context-aware `prefix_tables` function with its 11 unit tests was the AI's fix; the human's contribution was the rule "longest-name-first."
:::

The hard part is knowing which numbers to look at.
