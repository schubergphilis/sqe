# The DuckDB Drift {#sec:duckdb-drift}

> The shape of an engine is not what it was designed to do.
> It is what users actually paste into the prompt.

Six months after the matrix chapters closed, a user pasted this into the SQE CLI:

```sql
sqe> SELECT * FROM 'hf://datasets/datasets-examples/doc-formats-csv-1/data.csv';
```

The query failed. Three more queries from the same user followed in the same week. Two of those also failed. By the time we patched the last one, SQE had stopped looking quite like the Trino-replacement we set out to build.

This chapter is the story of that drift. Five MRs (V8 through V12) added file-format TVFs, a lazy HTTPS object store, a HuggingFace path resolver, a Delta Lake reader, and a smart CSV parser. None of them were planned in the original roadmap. Each of them came from a single user request. Together they put SQE in a category we did not name when we started: "distributed query engine that also runs as one binary on a developer laptop and queries CSV from HuggingFace."

## How users move

The matrix chapter framed SQE in terms of capabilities the public icebergmatrix.org rubric tracks: position deletes, partition evolution, equality deletes, and the rest. Users do not write queries against rubrics. They write queries against the path bar of their text editor.

The first request was simple. A teammate had a Parquet file on their laptop. They wanted to inspect it. They typed:

```sql
SELECT * FROM '/tmp/orders.parquet' LIMIT 5;
```

It failed. The error mentioned `default.default.tmp.orders.parquet` and "table not found." DataFusion was treating the quoted string as a fully-qualified table name. The user fix was obvious: register the file as a table first, with three lines of DDL. The user fix was also wrong. They were not going to remember three lines of DDL when DuckDB lets them paste a path.

V8 enabled `enable_url_table()` on the SQE session and added three TVFs: `read_parquet`, `read_csv`, `read_json`. A quoted path inside a `FROM` clause now auto-detects format from extension. The fix was twenty lines of registration code. The user request closed. The drift began.

## The DuckDB-shaped audit

The right response to "we should be more like DuckDB" is "where, exactly?" We wrote `docs/duckdb-comparision.md`: a flat table with two columns. What DuckDB has. Whether SQE has it. We checked every line.

Two findings worth quoting. First, there were features we had that we never documented. `array_append` works. `map_keys` works. `cosh`, `sinh`, `tanh` work. `QUALIFY` works (in DataFusion 53). The `->`/`->>` JSON operators work via `datafusion-functions-json`. The matrix audit had been treating these as gaps in our documentation, not gaps in our engine. That alone closed eight rows.

Second, the genuine gaps clustered. They were not random. They were file-format ergonomics: read CSV, read JSON, read Avro, write CSV, write JSON, COPY TO. They were SQL niceties: `SELECT * EXCLUDE (col)`, `SUMMARIZE`, `DESCRIBE`, dot-commands. They were extensions: httpfs, hf://, delta. None of these were impossible. None of them were even hard. Every one of them was a few hundred lines of glue against DataFusion or `delta-rs`.

The audit became the V8-V12 roadmap. Five MRs. Each MR tracked back to a user request, then to an audit row, then to a few hundred lines of code.

## V8: file-format TVFs

The pattern is identical for `read_parquet`, `read_csv`, `read_json`. Each TVF wraps DataFusion's existing format reader (`ParquetFormat`, `CsvFormat`, `JsonFormat`) in a `ListingTable`. Path parsing, S3 credential extraction, and named-arg validation live in a shared module:

```rust
pub fn parse_file_tvf_args<F>(
    fn_name: &str,
    exprs: &[Expr],
    mut extra: F,
) -> DFResult<FileTvfArgs>
where
    F: FnMut(&str, &str) -> bool,
{
    // First positional arg: the path. Required.
    // Subsequent args are key => value pairs. The closure handles
    // format-specific names; common names (access_key, secret_key,
    // endpoint, region) are handled centrally.
}
```

The `enable_url_table()` toggle wraps the catalog list in DataFusion's `DynamicFileCatalog`. When the planner cannot resolve a table name, the dynamic catalog parses it as a URL and dispatches to a `ListingTableFactory` keyed on the file extension. This was a feature DataFusion already had; we had been ignoring it.

Twenty lines of registration in `EmbeddedClient::build_embedded_context`:

```rust
let ctx = ctx.enable_url_table();

ctx.register_udtf("read_parquet",
    Arc::new(ReadParquetFunction::new(StorageConfig::default())));
ctx.register_udtf("read_csv",
    Arc::new(ReadCsvFunction::new(StorageConfig::default())));
ctx.register_udtf("read_json",
    Arc::new(ReadJsonFunction::new(StorageConfig::default())));
```

V8 closed five audit rows. The first user request from the previous section worked. So did `SELECT * FROM 's3://bucket/sales/*.csv'`. So did the COPY TO output path. Five rows for five hundred lines of code.

## V9: SQL niceties

This was the chapter the audit caught us cheating on. `DESCRIBE table` works. `SELECT * EXCLUDE (col) FROM t` works. `SELECT * REPLACE (lower(name) AS name) FROM t` works. We had inherited all three from DataFusion 53 and never documented them. The MR was three sentences in `getting-started/cli.md`.

What V9 actually shipped was the dot-commands every DuckDB user types unconsciously: `.describe`, `.summarize`, `.tables`, `.schemas`. These live in the SQE CLI's input loop, not in the SQL surface. They translate to standard SQL queries and pass through to DataFusion. `.summarize <table>` becomes a long `SELECT min(col), max(col), count(*), count(DISTINCT col), ...` that DataFusion already knows how to run.

Forty lines of input-handling code. Seven dot-commands. The coordinate space mattered: a user who has typed `.describe` ten thousand times into DuckDB does not want to type `DESCRIBE table_name;` into ours. The keystroke distance between systems is real friction.

## V10: httpfs and HuggingFace

The user request that triggered V10 was small: "let me query a CSV from raw.githubusercontent.com." DataFusion's default `ObjectStoreRegistry` is a static `scheme://host` -> store map. Anything not pre-registered fails with "no suitable object store found." For S3 the coordinator registers buckets at startup; for arbitrary HTTPS it cannot, because the host space is unbounded.

The fix is a wrapper:

```rust
pub struct LazyHttpObjectStoreRegistry<R: ObjectStoreRegistry> {
    inner: R,
}

impl<R: ObjectStoreRegistry> ObjectStoreRegistry for LazyHttpObjectStoreRegistry<R> {
    fn get_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        if let Ok(store) = self.inner.get_store(url) {
            return Ok(store);
        }
        match url.scheme() {
            "https" | "http" => {
                let store = build_http_store(url)?;
                self.inner.register_store(url, store.clone());
                Ok(store)
            }
            _ => self.inner.get_store(url),  // surface the original error
        }
    }
}
```

On a miss for `http`/`https`, build an `HttpStore` from `object_store::http::HttpBuilder` for the URL's `scheme://host` and cache it on the inner registry. Subsequent requests for the same host hit the cache. Anything other than `http`/`https` falls back to the inner registry's error.

The HuggingFace resolver was a function, not a store. `hf://datasets/<owner>/<name>/<path>` translates to `https://huggingface.co/datasets/<owner>/<name>/resolve/main/<path>`. The TVFs call `rewrite_hf_path_in_place` on their parsed args before opening the store. Same machinery DataFusion already had; just an URL transform.

V10 closed three audit rows. It also opened a new shape of bug: users started pasting hf:// URLs that exercised parts of the resolver we had not tested. We discovered them one query at a time over the next month.

## V11: Delta Lake

The audit row read: "Delta Lake reader: missing." The fix wrapped `deltalake-core 0.32.1` in a TVF identical in shape to `read_parquet`:

```sql
SELECT * FROM read_delta('/data/delta/sales');
SELECT * FROM read_delta('s3://bucket/delta/orders', access_key => '...');
SELECT * FROM read_delta('/data/delta/sales', version => '5');
SELECT * FROM read_delta('/data/delta/sales',
    timestamp => '2026-04-01T00:00:00Z');
```

Read-only. Time travel via `version` (snapshot id) or `timestamp` (RFC3339), mutually exclusive. The first commit landed against deltalake 0.31's API; the rebase onto 0.32.1 cost three lines (i64 to u64 for version, `load_with_datetime` instead of `load_with_datestring`).

V11 closed one audit row and unlocked a population we had not specifically targeted: teams with Delta tables who wanted to query them through the same CLI as their Iceberg tables. The cross-format query is simple now:

```sql
SELECT i.region, sum(d.amount)
FROM iceberg.sales.orders i
JOIN read_delta('/data/legacy-delta/transactions') d
    ON i.id = d.order_id
GROUP BY i.region;
```

The mixed-format join goes through the same DataFusion plan, with the same shuffle, the same spill, and the same cost-model. Users do not have to know that one source is Iceberg and the other is Delta.

## V12: hf:// in the URL-table path

The bug that opens this chapter. V10 made `read_csv('hf://...')` work; V12 made `SELECT * FROM 'hf://...'` work too. The fix was an SQL pre-rewriter: scan the SQL for single-quoted `'hf://...'` literals, resolve each to its HTTPS form, substitute. The rewritten query sees `'https://huggingface.co/...'`, which V10's lazy registry already handles.

```rust
pub fn rewrite_hf_urls_in_sql(sql: &str) -> DFResult<Cow<'_, str>> {
    if !sql.contains("hf://") { return Ok(Cow::Borrowed(sql)); }
    // Walk the input one char at a time, tracking single-quote state.
    // For each `'hf://...'` literal, resolve via resolve_hf_url and
    // substitute. Doubled-single-quote escapes ('O''Brien') honoured.
}
```

Less elegant than a custom `ObjectStore` would be. More elegant than waiting for DataFusion's URL parser to accept arbitrary schemes. We wrote V12.2 onto the roadmap as a follow-up: a real `HfObjectStore` that implements `ObjectStore::list` against HuggingFace's tree API, so glob queries (`**/*.parquet`) work without SQL surgery.

V12 also closed a long-standing CSV ergonomics gap. `read_csv` now picks delimiter from extension (`.tsv` -> tab, `.psv` -> pipe), detects compression from extension (`.csv.gz`, `.tsv.zst`), and accepts DuckDB-style aliases (`sep`, `delim`, `header`, `nullstr`, `compress`). The user writes the natural form for each shape:

```sql
SELECT * FROM read_csv('events.tsv.gz');                 -- nothing else needed
SELECT * FROM read_csv('financial.ssv', sep => ';');      -- semicolon-separated
SELECT * FROM read_csv('logs.tsv.zst', compress => 'auto'); -- explicit codec
```

## What V8 through V12 add up to

A fair description of SQE before V8: "Trino-replacement Iceberg engine with OIDC passthrough, distributed coordinator-worker, write path through `iceberg-rust`."

A fair description of SQE after V12: "Trino-replacement Iceberg engine with OIDC passthrough, distributed coordinator-worker, write path through `iceberg-rust`. Plus single-binary embedded mode for laptop analytics. Plus file-format TVFs against local disk, S3, HTTPS, HuggingFace. Plus a Delta Lake reader. Plus DuckDB-style ergonomics (auto-detect, dot-commands, EXCLUDE/REPLACE)."

The first half of that paragraph is what we set out to build. The second half is what users dragged us toward. The work to get there was modest: about 1500 lines of glue code, two new TVFs, one URL resolver, one lazy object-store wrapper, an SQL pre-rewriter, and a docs audit. None of it touched the cluster mode. None of it touched OIDC. None of it changed the shape of the coordinator-worker protocol.

## What we did not become

We are not a pure DuckDB clone. The matrix has not flipped.

We do not have PIVOT, UNPIVOT, QUALIFY (the DuckDB grouping forms), ASOF JOIN, FROM-first syntax, list comprehensions, or lambdas. DataFusion's parser does not support these. We track upstream and ship them when they land.

We do not have spatial, vector search, full-text search, Excel reader, or Postgres scanner. These are out of scope.

The binary is 180 MB to DuckDB's 30. DataFusion plus the AWS SDK plus the deltalake transitive dependencies plus the Iceberg crates do not slim down further without painful Cargo feature surgery. We could ship a 70 MB minimal build; we have not yet bothered.

A pure analytics persona is not what SQE optimises for. Cluster mode plus auth plus catalog routing plus distributed execution still adds overhead a single-node DuckDB does not pay. SQE in embedded mode is fast for laptop queries; SQE in cluster mode is fast for many users hitting shared catalogs. They are not the same workload.

## What we did become

The user from the start of the chapter has used SQE every day for the past two months. Most of their queries do not touch a catalog. They do not write `iceberg.warehouse.schema.table`. They paste paths. They paste URLs. They paste HuggingFace dataset names. The CLI prompt is the same the whole time. The same binary that handles their Iceberg cluster work also handles their dataset previews.

That is the DuckDB experience. We did not plan it. We landed it accidentally over five MRs in five weeks. The architecture luck was real: DataFusion plus `iceberg-rust` plus `delta-rs` plus `object_store` plus `enable_url_table` did most of the work.

The lesson is not "we should have planned for this." The lesson is that when the building blocks are good, every engine that adopts them ends up looking similar at the user level. DuckDB built their own engine and added an extension story. We picked DataFusion and got the engine for free, then added the same extension story. The convergence between distributed SQL engines and embedded analytics is not coming. It is here.

The next chapter ("What We Would Do Differently") names a few of the assumptions we still hold from the cluster era and would revisit. The DuckDB drift is in that list. We would name the embedded persona earlier next time. We would split the audit table from "DuckDB compat" into "audit table from a DuckDB-shaped persona." We would build the embedded CLI as a first-class deliverable rather than a side-effect of a second binary.

We would not, however, change the architecture. The fact that one binary serves both modes is the win. Naming it earlier would not have changed the code.
