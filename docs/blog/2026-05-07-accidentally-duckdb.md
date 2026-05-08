---
title: "How we accidentally created a DuckDB"
description: "SQE started as a distributed Iceberg query engine. Five MRs later it queries CSVs from disk, Parquet from S3, and Parquet from HuggingFace. We did not plan that."
pubDate: "2026-05-07"
author: "Jacob Verhoeks"
tags:
  - "duckdb"
  - "iceberg"
  - "datafusion"
  - "embedded"
  - "developer-experience"
---



*May 7, 2026*

A user pasted this query into the CLI yesterday:

```sql
sqe> SELECT * FROM 'hf://datasets/datasets-examples/doc-formats-csv-1/data.csv';
```

It almost worked. The hf:// URL was rewritten to HTTPS, the file fetched, but the auto-detect path tripped over a quirk in DataFusion's URL-table catalog. We pushed a fix that evening. The next morning the same user came back with:

```sql
sqe> SELECT * FROM read_delta('s3://my-bucket/orders');
```

That worked.

Here is what the user actually did over the past week. Connect the embedded CLI. Query a Parquet on disk. Query a CSV in S3. Query a Delta table. Query a public HuggingFace dataset. None of it touched a catalog. None of it touched OIDC. The user never said "Iceberg".

That is not what we built SQE to be. We built SQE to be a Rust replacement for Trino against Apache Iceberg, with OIDC bearer-token passthrough, distributed coordinator and worker fragments, and a Polaris REST catalog as the authoritative table store. That is still what it is. But somewhere between V8 and V12, **we also became a DuckDB**.

We did not plan that. We say it out loud now because it changes the positioning conversation.

## The one-feature-at-a-time path

The blame for this drift lands on five merge requests, each of which made sense in isolation.

### V8: file-format TVFs and `SELECT * FROM 'file.parquet'`

A user asked: "I have a Parquet I want to query for ten seconds. Do I need to register it as an external table first?"

The honest answer was yes. We wrote `read_parquet('s3://bucket/file.parquet')` and `read_csv('/data/orders.csv')` and `read_json('/var/log/events.jsonl')`. We then turned on DataFusion's `enable_url_table()` so a quoted path inside a `FROM` clause auto-detects format from extension:

```sql
SELECT * FROM '/data/sales.parquet';
SELECT * FROM 's3://bucket/sales.csv';
```

Same syntax DuckDB pioneered. Same expectation. Three TVFs and one feature toggle. Twenty lines of registration code in the embedded session.

### V9: SQL surface niceties

DuckDB users live in `.describe`, `.summarize`, `EXCLUDE`, `REPLACE`. They do `SELECT * EXCLUDE (password_hash) FROM users` without thinking. We added the dot-commands, then documented that DataFusion already supports `EXCLUDE` and `REPLACE`. The audit revealed `DESCRIBE` works natively. We were sitting on functionality nobody knew existed.

### V10: httpfs and HuggingFace

The next request was "let me query a CSV from raw.githubusercontent.com directly." DataFusion's default object-store registry is a static map; arbitrary HTTPS URLs fail with "no suitable object store found". We wrote a `LazyHttpObjectStoreRegistry` that wraps the default and builds an `HttpStore` on first request for unrecognised `https://host` pairs. Then we added an `hf://datasets/<owner>/<name>/<path>` resolver that translates HuggingFace dataset URLs to their HTTPS form on the Hub.

A user can now do:

```sql
SELECT count(*) FROM read_csv(
    'https://raw.githubusercontent.com/datasets/airport-codes/main/data/airport-codes.csv'
);

SELECT * FROM read_parquet(
    'hf://datasets/wikimedia/wikipedia/20231101.en/train-00000-of-00041.parquet'
);
```

No catalog. No `CREATE EXTERNAL TABLE`. The CLI is one binary on a developer laptop reaching out to two public servers and joining results. That is the DuckDB experience.

### V11: Delta Lake reader

Then we filled the obvious format gap. SQE's whole point is Iceberg, but users with Delta tables wanted to query them too. The `delta-rs` crate (`deltalake-core 0.32.1`) is a clean wrapper. One TVF later:

```sql
SELECT * FROM read_delta('/data/delta/sales');

SELECT * FROM read_delta('s3://bucket/delta/orders',
    version => '5');

SELECT * FROM read_delta('/data/delta/sales',
    timestamp => '2026-04-01T00:00:00Z');
```

Read-only for now. Time travel works. The shape is exactly the same as `read_parquet`.

### V12: hf:// in URL-table auto-detect

The fix that started this whole post. V10 made `read_csv('hf://...')` work. The auto-detect path (`SELECT * FROM 'hf://...'`) needed an SQL pre-rewriter to translate hf:// URLs to HTTPS before DataFusion sees the query. Five files of code, eight unit tests.

That same MR added DuckDB-style inline revision spec: `hf://datasets/foo/bar@~parquet/data.parquet` resolves to HuggingFace's auto-generated parquet view branch (`refs/convert/parquet`).

And quietly, in the same week, we made `read_csv` smarter: extension-based delimiter detection (`.tsv` -> tab, `.psv` -> pipe), DuckDB-style aliases (`sep`, `delim`, `header`, `nullstr`), and compression auto-detect (`.csv.gz`, `.tsv.zst`).

```sql
SELECT * FROM read_csv('events.tsv.gz');  -- delimiter and codec inferred
SELECT * FROM read_csv('data.psv', sep => '|', header => 'true');
```

Each of these MRs answered a clear user request. None of them said "let's clone DuckDB."

## The architecture luck

We got here because DataFusion got us most of the way already.

`CsvFormat`, `JsonFormat`, `ParquetFormat`, and `AvroFormat` are first-class in DataFusion 53. `ListingTable` glues them to an `ObjectStore`. `ListingTableUrl::parse` understands `s3://`, `file://`, and (with `enable_url_table()`) any `scheme://host` registered in the runtime. We did not build a new file reader. We did not build a new Parquet decoder. We wrote glue code that pulled DataFusion's existing pieces into a TVF surface and a URL auto-detect path.

The vendored `iceberg-rust` (the RisingWave fork) gave us the same head start on the catalog side. We did not build an Iceberg writer. We exposed it through SQL.

This is the convergent design pattern: when the building blocks are good, every engine that picks them up ends up looking similar at the user level. DuckDB built their own engine and added an extension story. We picked DataFusion and got the engine for free, then added the same extension story.

## What SQE has that DuckDB does not

Reading this post you might wonder why anyone uses SQE over DuckDB. Three answers.

**OIDC bearer-token passthrough.** Every SQE query in cluster mode runs as the authenticated user. No service account. No sudo-as-postgres pattern. The user's bearer token flows through the coordinator to the workers to the Polaris catalog to S3. Every layer enforces its own ACLs against the user. DuckDB has no concept of an authenticated user.

**Iceberg V3 read and write.** Position deletes. Manifest column statistics. Partition evolution. Branch and tag DDL. Equality deletes. Merge-on-Read for UPDATE and MERGE. SQE writes Iceberg v3 tables that Spark 4.1 reads. DuckDB has an Iceberg extension that reads, but its write story is still partial.

**Multi-catalog cluster.** Polaris, Nessie, AWS Glue, Hive Metastore, and S3 Tables in one engine, behind one auth chain. DuckDB is extension-by-extension and runs on one machine.

**Trino HTTP wire compatibility.** dbt models that already work against Trino 465 work against SQE without changes. DuckDB has no Trino wire support.

**Distributed execution.** Coordinator and stateless workers, shuffle, spill, adaptive sort. DuckDB is single-node for analytics.

The accidental DuckDB experience is additive. It does not replace any of the above.

## What DuckDB has that SQE does not

To be honest about the gap.

**A pure analytics persona.** DuckDB markets itself as the analytics SQLite. The whole stack from the read path to the optimizer to the wire protocol is tuned for "one developer running one query." SQE is tuned for "many users running many queries against shared catalogs."

**SQL parser features.** PIVOT, UNPIVOT, QUALIFY, ASOF JOIN, FROM-first syntax, list comprehensions, lambdas. DataFusion's parser does not support these. We track upstream and ship them when they land. DuckDB has a custom parser and ships them now.

**Niche extensions.** Spatial (`spatial`). Vector search (`vss`). Full-text search (`fts`). Excel reader. Postgres scanner. We documented these as out of scope for this cycle. Users who need them either use DuckDB alongside SQE or wait for upstream demand to align with our investment direction.

**A 30 MB binary.** Our embedded CLI is 180 MB. DuckDB is 30 MB. The difference is mostly DataFusion plus the Iceberg crates plus the AWS SDK plus the deltalake transitive dependencies. We can slim through Cargo features but the floor is higher than DuckDB's.

## What this means for positioning

We have not changed our positioning. SQE is a sovereign query engine for Apache Iceberg. The cluster mode is still where the production users live. The Trino wire and the OIDC passthrough are still the differentiators against everything else on the matrix.

But there is a second tier of users who showed up sideways. They want a CLI. They want one binary. They want CSV / JSON / Parquet from disk, S3, HuggingFace, GitHub raw, Delta tables. They want `SELECT * FROM 'file.csv'` without registering anything first.

SQE serves them now. The cluster path and the embedded path share one binary. The same SQL works against both. The same TVFs work against both. The same compatibility audit drives both.

## Lessons

**Listen to the queries users paste.** The hf:// auto-detect bug came from a real user pasting a real URL. We did not anticipate it. We fixed it in five files. The DuckDB drift came from many small bugs of the same shape, each from a real user.

**Compatibility audits beat compatibility plans.** We listed every DuckDB feature and asked, for each one, "does SQE have this and we never documented it, or do we genuinely lack it?" Two false negatives became one-line documentation fixes. Eight genuine gaps became V8-V12. The audit is at `docs/duckdb-comparision.md`.

**Convergence is the default.** When the building blocks are good (DataFusion, iceberg-rust, object_store), every engine that picks them up ends up looking similar at the user level. The work is in the integration, not the engine.

**Single-binary embedded mode is cheaper than you think.** We added the `EmbeddedClient` in two days and got the entire SQE feature surface from the CLI. We did not duplicate the coordinator. We composed the same components into a different shape.

## What is next

V12.2: a custom `HfObjectStore` so glob queries (`hf://datasets/foo/bar/**/*.parquet`) work without SQL pre-rewriting. The HF tree-API cache shipped today (MR 158) is the prerequisite. After that the hf:// scheme registers natively in `LazyHttpObjectStoreRegistry` and DuckDB-equivalent globs become a non-event.

The Trino-compatible distributed engine still has the bigger roadmap. Iceberg V3 catalog parity (last 5%). Variant type. Geometry type. CDC changelog views. Multi-arg partition transforms. Those land in the iceberg-matrix-parity workstream.

If you reached this far and the embedded mode story is what you wanted: try it.

```bash
cargo install --path crates/sqe-cli
sqe-cli --embedded
sqe> SELECT * FROM 'hf://datasets/squad/plain_text/train-00000-of-00001.parquet' LIMIT 5;
```

If the cluster mode is what you wanted: that is still the default.

## Side by side with DuckDB

The fair test is the one a developer actually runs. We pulled a public Parquet from DuckDB's own blob store and timed `CREATE TABLE AS SELECT` in both engines on the same machine.

The SQE CLI:

```text
sqe> .help
Dot commands:
  .help                show this list
  .exit, .quit         leave the REPL
  .tables [schema]     list tables (optionally filter by schema)
  .schema <table>      describe a table's columns
  .catalogs            list catalogs visible to the session
  .read <path>         execute a SQL script file
  .timer on|off        toggle per-query elapsed-time output
  .format [fmt]        show or set output format (table|csv|tsv|json)

SQL: type a query and end it with `;`. End-of-input or .exit to quit.

sqe> .timer on
Timer: on
sqe> create table test2 as select * from read_parquet(
       'https://blobs.duckdb.org/train_services.parquet');
(0 rows)
Time: 1.618s
```

The same query in DuckDB v1.4.4:

```text
DuckDB v1.4.4 (Andium) 6ddac802ff
Enter ".help" for usage hints.
Connected to a transient in-memory database.
D .timer on
D create table test2 as select * from read_parquet(
    'https://blobs.duckdb.org/train_services.parquet');
Run Time (s): real 1.815 user 0.237842 sys 0.132715
```

SQE: 1.618s. DuckDB: 1.815s. Same machine. Same query. Same source. SQE wins by about 200ms on a network-bound load.

Two notes on the comparison.

The result is partly DataFusion's Parquet reader being well-tuned, partly that V10's `LazyHttpObjectStoreRegistry` reuses the HTTP connection across the file's row groups, partly that we pay the JVM-free Rust startup cost only once. SQE in embedded mode does not load a Polaris catalog or boot any worker fragments; the binary that opens the prompt is the binary that runs the query.

This is one query against one URL. Real workloads bounce between local Parquet, S3 buckets, hf:// datasets, and Iceberg tables in a Polaris catalog. The 200 ms is not a benchmark claim. It is a smoke test that says "the embedded mode is at least as fast as DuckDB on the basic file-load case." We have not yet built a head-to-head benchmark suite for embedded mode; the existing TPC-H / TPC-DS / SSB / ClickBench results are all in cluster mode against Trino 465.

The dot-command surface is the part the screenshot above probably told you faster. SQE's REPL has the same dot commands DuckDB users type without thinking: `.tables`, `.schema`, `.catalogs`, `.read`, `.timer`, `.format`. Plus `.help`. Plus `.exit` and `.quit`. Each maps to either a built-in REPL action or a standard SQL query that DataFusion already knows how to run. The hardest part of writing the dot-command layer was getting `.timer on` to wrap the `df.collect().await` call cleanly without leaking timing into the test paths.
