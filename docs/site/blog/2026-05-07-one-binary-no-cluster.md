---
title: "One Binary, No Cluster: SQE Goes Embedded"
description: "We built SQE for distributed Iceberg, but most of the time you just want to look at a parquet file. Here's how we made the engine work both ways without forking the codebase."
pubDate: "2026-05-07"
author: "Jacob Verhoeks"
tags: ["cli", "embedded", "duckdb", "datafusion", "developer-experience"]
---

Sometimes you just want to look at a parquet file.

You have a 200 MB extract from a colleague. Maybe it came out of a Spark job, maybe a dbt run, maybe an analyst exported it from a notebook. The schema is opaque. You want to know how many rows it has, what the columns look like, whether the date range matches what you expected. You don't want to start docker, you don't want to log in to a cluster, you don't want to write Python.

DuckDB owns this workflow. One binary, no setup, `SELECT COUNT(*) FROM 'file.parquet'` and you have your answer.

SQE is built for distributed Iceberg with Polaris, OIDC auth, OPA policies, and Flight SQL. Different problem. But the engine underneath is DataFusion, and DataFusion runs anywhere. So why do you need the whole stack to look at a file?

You don't. Here's how we made SQE work both ways.

## What was already in the box

The first surprise when we sat down to scope this: most of the work was already done.

`sqe-cli` existed. It already had a rustyline-based REPL, multi-line input, `-e` for one-shots, and four output formats (table, csv, tsv, json). It connected to a remote coordinator over Flight SQL or Trino HTTP, but the shape was right.

The query engine itself sits behind a small `SqlClient` trait:

```rust
#[async_trait::async_trait]
pub trait SqlClient: Send {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>>;
}
```

Two implementations: `FlightClient`, `HttpClient`. Each owns a network connection, sends SQL, receives Arrow batches, formats them as `QueryResult`. The REPL doesn't know or care which one is in the box.

The `read_parquet` table-valued function existed too. It already powered Trino-shape queries like `SELECT * FROM read_parquet('s3://bucket/path/*.parquet')` against the cluster coordinator. No catalog needed, no DDL, just point at a file and read.

The cluster's `SessionContext` setup, the part that registers all the Trino-dialect functions, the JSON helpers, the sha256 UDF for column masking, the dynamic filter pushdown config, all of that lived in one helper that took a `SqeConfig`, a `Session`, a `PolicyStore`, a `QueryTracker`, and a `MetricsRegistry`.

That last bit was the wall.

## The wall: cluster assumptions all the way down

The cluster session builder assumes everything. It expects an authenticated user. It assumes there's a policy store to consult on every query. It threads a query tracker through for observability. It plumbs a Prometheus registry for metrics. It caches sessions by token fingerprint with a 5-minute TTL.

For embedded mode none of those exist. There is no user. There is no policy. The metrics endpoint is closed. The cache is overhead.

We had two options. Refactor the cluster builder to take optional everything, plumb `Option<&PolicyStore>` and `Option<&QueryTracker>` and `Option<&MetricsRegistry>` and degrade gracefully when they're absent. Or write a small parallel builder for the embedded path that registers the same DataFusion config and the same UDFs, and skip the rest.

We picked the second. The parallel builder is 50 lines. Refactoring the cluster path would have been a week of plumbing for an Option flag that's always `None` in the new use case. The duplication is tiny: a handful of `set_bool` config calls, four `register_udf` calls, one `register_udtf`. If the two paths ever diverge meaningfully we revisit. They probably won't.

```rust
pub fn build_embedded_context(memory_limit_bytes: usize) -> anyhow::Result<SessionContext> {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("default", "default")
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
        .set_usize("datafusion.optimizer.hash_join_single_partition_threshold",
                   64 * 1024 * 1024)
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true);

    let pool_size = memory_limit_bytes.max(64 * 1024 * 1024);
    let pool = Arc::new(FairSpillPool::new(pool_size));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()?;

    let mut ctx = SessionContext::new_with_config_rt(session_config, runtime);

    ctx.register_udf(sqe_policy::sha256_udf::sha256_udf());
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);
    datafusion_functions_json::register_all(&mut ctx)?;

    ctx.register_udtf(
        "read_parquet",
        Arc::new(sqe_catalog::read_parquet::ReadParquetFunction::new(
            StorageConfig::default(),
        )),
    );

    Ok(ctx)
}
```

Same DataFusion tuning the cluster uses. Same Trino-dialect aliases. Same JSON helpers. The same SQL text runs against either path. That was the design constraint: do not fork the dialect.

## What it looks like

Hello world:

```bash
$ sqe-cli --embedded -e "SELECT 42 AS answer"
sqe-cli 0.15.0 embedded engine (1GB memory pool)
+--------+
| answer |
+--------+
| 42     |
+--------+
(1 rows)
```

Trino-dialect functions just work. No special setup, same UDFs the cluster registers:

```bash
$ sqe-cli --embedded -e "SELECT year(DATE '2026-05-07') AS y"
+------+
| y    |
+------+
| 2026 |
+------+
```

A real parquet file from a local path:

```bash
$ sqe-cli --embedded -e \
    "SELECT count(*) AS n, min(o_orderdate) AS earliest, max(o_orderdate) AS latest
     FROM read_parquet('orders.parquet')"
+--------+------------+------------+
| n      | earliest   | latest     |
+--------+------------+------------+
| 150000 | 1992-01-01 | 1998-08-02 |
+--------+------------+------------+
```

S3 if you want it. Inline credentials in the TVF call:

```sql
SELECT *
FROM read_parquet(
    's3://my-bucket/year=2026/*.parquet',
    access_key  => 'AKIA...',
    secret_key  => '...',
    region      => 'eu-central-1'
)
LIMIT 100;
```

A multi-statement script:

```bash
$ cat schema_audit.sql
-- TPC-H schema audit
SELECT 'orders' AS tab, count(*) AS rows
FROM read_parquet('orders.parquet');

SELECT 'lineitem' AS tab, count(*) AS rows
FROM read_parquet('lineitem.parquet');

SELECT 'customer' AS tab, count(*) AS rows
FROM read_parquet('customer.parquet');

$ sqe-cli --embedded --file schema_audit.sql --format csv
tab,rows
orders,150000
tab,rows
lineitem,600572
tab,rows
customer,15000
```

Different format, same data:

```bash
$ sqe-cli --embedded --format json -e "SELECT 1 AS x, 'hello' AS y"
{"x":"1","y":"hello"}
```

Memory cap if you want to be careful:

```bash
$ sqe-cli --embedded --memory-limit 256MB --file big-aggregation.sql
```

Interactive REPL works exactly like before, just without the network:

```
$ sqe-cli --embedded
sqe-cli 0.15.0 embedded engine (1GB memory pool)
Type SQL queries, or \q to quit. End multi-line queries with ;

sqe> SELECT
  ->   region,
  ->   count(*) AS orders,
  ->   sum(amount) AS total
  -> FROM read_parquet('sales.parquet')
  -> WHERE order_date >= DATE '2026-01-01'
  -> GROUP BY region
  -> ORDER BY total DESC;
```

## The script splitter

`-f script.sql` was a small file in its own right.

You can naively split a SQL script on `;` and get the right answer for ninety percent of inputs. The other ten percent is where you spend a Saturday afternoon. Semicolons inside string literals. Semicolons in identifiers (because someone, somewhere, wrote `CREATE TABLE "foo;bar"`). Semicolons in line comments. Semicolons in block comments.

We wrote a tiny lexical state machine. Five states: code, single-quoted string, double-quoted identifier, line comment, block comment. The transitions are obvious from the names. SQL escapes a literal quote by doubling it, so when the state machine sees `'foo''bar'` it stays in the string state across the doubled quote and only exits on the third quote. Same trick for `"foo""bar"`.

```rust
match state {
    State::Code => match c {
        ';' => { /* emit statement, clear buf */ }
        '\'' => state = State::SingleQuote,
        '"' => state = State::DoubleQuote,
        '-' if next == Some('-') => state = State::LineComment,
        '/' if next == Some('*') => state = State::BlockComment,
        _ => buf.push(c),
    },
    State::SingleQuote => {
        buf.push(c);
        if c == '\'' {
            if next == Some('\'') { i += 1; buf.push('\''); }
            else { state = State::Code; }
        }
    }
    // ...
}
```

A full sqlparser-based splitter would catch more corner cases, the obvious one being Postgres dollar-quoted strings (`$tag$ ... $tag$`). DataFusion and Trino dialects don't use those, and SQE only needs to be correct on the dialect we speak. The state machine is 110 lines including ten unit tests covering every escape and comment case we could think of. That's the right cost for the right correctness.

## What we deliberately left out

Anyone who's used DuckDB will notice the omissions.

There's no persistent catalog. `CREATE TABLE foo AS SELECT ... FROM read_parquet(...)` won't survive across sessions yet. You query files directly via the TVF, or you set up a Hadoop / SQLite catalog manually. We know this is the obvious next step. It's V2.

There are no dot-commands. No `.tables`, no `.schema`, no `.read script.sql` (you use `--file` from the shell instead). The REPL today is for SQL, not for catalog introspection. That's V3.

There's no auth, no RBAC, no column masking. Embedded mode runs as the local user, full stop. If you need policy enforcement, run the cluster path. The point of embedded mode is to get out of the way when you don't need any of that.

There's no distributed execution. Single process by design. If you have a 50 GB query, run the cluster.

These are not bugs. They are the line between "ad-hoc analysis on a parquet file" and "production analytical platform". DuckDB drew the same line. So do we.

## What V2 looks like

The plan for V2 is a SQLite-backed embedded catalog at `~/.sqe/warehouse/`. The Iceberg JDBC catalog already exists in our tree (we've been using it for the JDBC backend in the cluster), and SQLite is just one URL prefix away. Default the embedded path to it. `CREATE TABLE` lands metadata in SQLite, data files in `~/.sqe/warehouse/iceberg/<table>/`. `SHOW TABLES` works. Re-attach across sessions.

`--memory` opt-out for ephemeral runs that don't want to leave a footprint on disk.

Everything you write through embedded mode stays valid Iceberg. If you upgrade to a real cluster later, you point the cluster at the same warehouse path and the tables come along. No migration. No re-export. That's the whole point of building on a real table format instead of inventing one.

V3 is the polish layer. Dot-commands. Tab completion. A `.timer on`. The kinds of small affordances that turn a CLI into something you actually want to live in.

## Why bother

We're a team of three building a distributed query engine, and we just spent a few days adding a single-binary mode for ad-hoc parquet analysis. Worth it?

Yes, for two reasons. The first is dogfooding. Every SQE developer is now one shell command away from running our engine on real data. Bench output is parquet. Test fixtures are parquet. Customer extracts are parquet. We were ssh-ing into bench machines to run `duckdb -c "SELECT * FROM ..."` against our own outputs. That's embarrassing.

The second is the funnel. Someone reading our docs lands on Polaris setup, Iceberg REST configuration, OIDC providers, and bounces. The same person, told `cargo install sqe-cli && sqe-cli --embedded -e "SELECT * FROM read_parquet('your-data.parquet')"`, has a working SQE installation in 90 seconds. They've felt the thing run. The full stack is the same engine they just used.

The cost was about three days of work, half of which was tests. We did not have to fork the dialect, the function library, the SQL parser, or the optimizer. The same `SELECT year(DATE '2026-05-07')` runs against either path. That was the design constraint, and it held.

Single binary. No cluster. The whole engine, in a shell command.
