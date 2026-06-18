# SQE vs DuckDB

A DuckDB compatibility matrix for SQE (Sovereign Query Engine), based on the
DuckDB [data overview](https://duckdb.org/docs/current/data/overview),
[SQL introduction](https://duckdb.org/docs/current/sql/introduction), and
[core extensions](https://duckdb.org/docs/current/core_extensions/overview).

The goal is not to *be* DuckDB. SQE is positioned as the Iceberg-first
analytical engine that runs both as an embedded single binary and as a
distributed cluster with OIDC pass-through and policy enforcement. DuckDB is
process-local, single-tenant, file-first. Where SQE matches DuckDB on the
embedded side, the same binary scales to a multi-tenant cluster.

## What SQE has that DuckDB doesn't

| Capability | SQE | DuckDB |
|---|---|---|
| Distributed execution | bespoke coordinator/worker scheduler over Arrow Flight | single-process |
| Per-query OIDC bearer pass-through to Iceberg / S3 | yes | no |
| OPA / Cedar policy enforcement (row filters, column masks) | yes | no |
| Multi-catalog: Polaris + Nessie + Glue + HMS + S3Tables in one engine | yes (V6) | extension-by-extension |
| Per-catalog auth (SessionBearer, ClientCredentials, Anonymous, Static, Aws) | yes (V7) | no |
| Iceberg V3 read + write (position deletes, manifest column stats) | yes | extension, read-only |
| Arrow Flight SQL wire protocol | yes | extension |

## What DuckDB has that SQE doesn't (yet)

The audit groups gaps by category. The roadmap section below shows which slice
each item lands in. **V8 through V12.1 have shipped**; the table reflects current
state.

### Data import / export

| DuckDB | SQE today | Status |
|---|---|---|
| `read_parquet(path)` | have, with inline S3 creds + HTTPS + hf:// | **done** (V8/V10) |
| `SELECT * FROM 'file.parquet'` (auto-detect) | works for parquet, csv, json, avro on local / s3 / https / hf:// | **done** (V8/V10/V12) |
| `read_csv(path, ...)` | full DuckDB-parity surface: `delimiter`/`delim`/`sep`, `header`, `compression`/`compress`, `nullstr`, extension-based delimiter and codec auto-detect | **done** (V8 + V12 follow-up) |
| `read_json(path, ...)` / `read_json_auto` | reads NDJSON, schema inference samples first batch | **done** (V8) |
| `read_avro(path, ...)` | available via DataFusion `datafusion-datasource-avro`; auto-detected on `.avro` extension | **done** (V8) |
| `COPY tbl TO 'file' (FORMAT csv\|json\|parquet)` | DataFusion-native `COPY ... TO` | **done** (V8) |
| `COPY tbl FROM 'file.csv'` | inverse via `INSERT INTO tbl SELECT * FROM read_csv(...)` | **done** (V8 pattern) |
| Gzip / zstd / xz / bz2 compressed CSV / JSON | extension-based codec auto-detect for CSV (V12 follow-up); same path for JSON | **done** (V8 + V12 follow-up) |
| `INSERT INTO ... VALUES (...)` | works against persistent SQLite catalog at `~/.sqe/warehouse/` | **done** (V5/V12) |
| `CREATE TABLE ... AS SELECT ...` | works in both embedded and cluster modes; cross-format CTAS (Iceberg from Delta from Parquet from hf://) | **done** (V5+) |
| `read_delta(path, ...)` (Delta Lake) | wraps `deltalake-core 0.32.1`; time travel via `version` / `timestamp` | **done** (V11) |
| HuggingFace `hf://` URLs in TVFs | `hf://datasets/<owner>/<name>/<path>`, `?revision=<rev>`, `@<rev>`, `@~parquet` view | **done** (V10/V12.1) |
| HuggingFace glob (`hf://.../**/*.parquet`) | tree-API cache landed; HfObjectStore wiring is V12.2 | **partial** (V12.2 in progress) |

### SQL surface

| DuckDB | SQE | Status |
|---|---|---|
| Standard `CREATE / DROP / INSERT / UPDATE / DELETE` | have | done |
| `SELECT / WHERE / GROUP BY / HAVING / ORDER BY / LIMIT / DISTINCT` | have | done |
| Joins (INNER, LEFT/RIGHT/FULL OUTER, CROSS) | have, with dynamic filter pushdown | done |
| CTEs (`WITH ...`), recursive CTEs | have | done |
| Window functions | have | done |
| Aggregates (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE) | have | done |
| `COUNT(*) FILTER (WHERE ...)` | have | done |
| `DESCRIBE table` | DataFusion-native + `.describe` dot-command shortcut | **done** (V9) |
| `SUMMARIZE table` | per-column UNION ALL via `.summarize` dot-command | **done** (V9) |
| `SELECT * EXCLUDE (col)` | works (DataFusion 53 native) | **done** (documented in V9) |
| `SELECT * REPLACE (expr AS col)` | works (DataFusion 53 native) | **done** (documented in V9) |
| `FROM tbl SELECT ...` (FROM-first) | missing | not planned (DataFusion parser does not support) |
| Struct / list / map literals (`{a: 1}`, `[1, 2]`, `MAP {...}`) | partial (nested types work, syntax less ergonomic) | not planned |
| List comprehensions, lambdas | missing | not planned (DataFusion does not support) |
| `PIVOT` / `UNPIVOT` | missing | not planned (DataFusion does not support) |
| `QUALIFY` | have (DataFusion SQL planner handles it) | done (row was stale; verified working, test `sql_compat 06_qualify`) |
| `ASOF JOIN` | missing | not planned (DataFusion has open issue, not landed) |

### Extensions

| DuckDB extension | SQE coverage | Status |
|---|---|---|
| `httpfs` (HTTP/HTTPS/S3 filesystem) | `LazyHttpObjectStoreRegistry` lazily builds `HttpStore` for any `scheme://host` on first request; works in TVFs and URL-table auto-detect | **done** (V10) |
| `aws` (SDK provider chain) | falls back to env / `~/.aws/credentials` / IMDS / IRSA when inline creds absent | **done** (V10) |
| Cloudflare R2 | works as S3-compatible: pass `endpoint => 'https://<account>.r2.cloudflarestorage.com'` and `region => 'auto'` | **done** (S3-compat) |
| `azure` (ADLS Gen2 / Blob) | full: shared key, SAS, Azurite emulator; `abfss://` + `azure://` + `az://` URL schemes; `azure_*` inline TVF args; `[storage.azure*]` config block | **done** |
| `gcp` (GCS) | full: service-account JSON path or inline + ADC fallback; `gs://` + `gcs://` URL schemes; `gcs_*` inline TVF args; `[storage.gcs*]` config block | **done** |
| `iceberg` | stronger than DuckDB (full read+write, V3, multi-catalog, OIDC vending, branches, MoR + CoW) | **done** |
| `delta` (Delta Lake) | `read_delta()` TVF read-only | **done** (V11) |
| `parquet` | have | done |
| `json` (functions) | `datafusion-functions-json` (json_get, json_as_text, ->, ->>, etc.) | **done** (V10 verify) |
| `avro` | `read_avro` + auto-detect on `.avro` extension | **done** (V8) |
| `postgres` / `mysql` / `sqlite` connectors | missing | not planned (Iceberg-first positioning) |
| `spatial`, `vss`, `fts`, `excel` | missing | not planned (use a real spatial / vector / FTS DB) |
| `icu` (timezone, collation) | partial (chrono-tz for timezones; collation gaps) | not planned |
| HuggingFace `hf://` URL resolver | inline + query-param revision, `@~parquet` auto-generated view branch | **done** (V10/V12.1) |

## Roadmap

V8 through V12.1 have shipped. The audit-appendix item numbers below identify
which slice each merged change corresponds to.

### V8: File-format TVFs + auto-detect + COPY TO ✅

Items 1, 2, 3.

- `read_csv(path, [delimiter=..., has_header=..., ...])`
- `read_json(path, [...])`
- `read_avro(path, [...])` via DataFusion's existing format reader
- `SELECT * FROM 'file.csv'` auto-detect on extension
- `COPY <source> TO 'file' (FORMAT csv|json|parquet)`

DataFusion 53 already shipped the format readers; V8 wrapped them in TVF
traits matching `ReadParquetFunction` and turned on `enable_url_table()`.

### V9: SQL surface niceties ✅

Items 4, 5.

- `.describe <table>` dot-command + `DESCRIBE` integration
- `.summarize <table>` dot-command + `SUMMARIZE <table>` SQL form
- `SELECT * EXCLUDE (col1, col2) FROM t`
- `SELECT * REPLACE (lower(name) AS name) FROM t`

The audit caught us under-documenting features DataFusion 53 already shipped.
`EXCLUDE` and `REPLACE` work natively. `DESCRIBE` works natively. The MR was
mostly the dot-commands plus updated docs. Three rows flipped from "missing"
to "done" by reading the source.

### V10: Network-transparent file access + auth ✅

Items 7, 10, 11.

- `LazyHttpObjectStoreRegistry`: wraps DataFusion's default registry. On a
  miss for `http`/`https`, builds an `HttpStore` for the URL's
  `scheme://host[:port]` and caches on the inner registry. Works for TVFs
  and URL-table auto-detect.
- HuggingFace `hf://(datasets|models|spaces)/<owner>/<name>/<path>[?revision=<rev>]`
  resolver. Default revision `main`. Reads `HF_TOKEN` env var for private
  datasets.
- AWS provider chain: when `[storage]` has no `s3_access_key`, falls back to
  env vars, `~/.aws/credentials`, IMDS, IRSA.
- JSON UDF surface verified against DuckDB; documented at
  [docs/features/json.md](features/json.md).

### V11: Delta Lake reader ✅

Item 6.

- `read_delta(path, [version | timestamp])` wraps `deltalake-core 0.32.1`.
- Time travel via `version => '<u64>'` (`load_version`) or
  `timestamp => '<RFC3339>'` (`load_with_datetime`); mutually exclusive.
- S3 storage options propagate from inline TVF args with fallback to
  `StorageConfig`.
- Read-only. Writes land in a follow-up.

Cross-format joins work today: a query can join an Iceberg table with a
Delta Lake reader and an HTTPS-resolved HuggingFace Parquet, all in one plan.

### V12: hf:// in URL-table auto-detect ✅

`SELECT * FROM 'hf://...'` (URL-table auto-detect) failed against V10 because
DataFusion's `enable_url_table()` does not recognise the `hf` scheme. V12
adds an SQL pre-rewriter that translates quoted `'hf://...'` literals to
their HTTPS equivalent before DataFusion sees the SQL.

### V12.1: hf:// inline revision + `@~parquet` view ✅

Inline `@<revision>` syntax in addition to `?revision=<rev>`. Special-case
`@~parquet`: HuggingFace's auto-generated parquet view lives on the
`refs/convert/parquet` branch; the resolver URL-encodes the slashes:

```sql
SELECT * FROM read_parquet('hf://datasets/foo/bar@~parquet/data.parquet');
-- -> https://huggingface.co/datasets/foo/bar/resolve/refs%2Fconvert%2Fparquet/data.parquet
```

Slashes in arbitrary revisions get the same URL-encoding treatment.
Conflicting `@<rev>` plus `?revision=<rev>` rejects with a clear error.

### V12 follow-up: smarter `read_csv` ✅

Extension-based delimiter detection (`.tsv` is tab, `.psv` is `|`, `.ssv` is
`;`). Compression auto-detect from the path (`.gz`, `.bz2`, `.xz`, `.zst`).
DuckDB-friendly aliases: `sep`, `delim`, `header`, `nullstr`, `compress`.
`derive_file_extension` walks a `.csv.gz` path correctly so directory globs
match `.csv.gz`.

### V12.2: HuggingFace `ObjectStore` for globs (in progress)

The HF tree-API cache shipped (`crates/sqe-catalog/src/hf_tree_cache.rs`)
as the prerequisite. Next: a custom `HfObjectStore` that implements
`ObjectStore::list` against the tree API, registered in
`LazyHttpObjectStoreRegistry` for the `hf` scheme. With that in place,
`SELECT * FROM 'hf://datasets/foo/bar/**/*.parquet'` flows through the
standard DataFusion glob-expansion path. The V12 SQL pre-rewriter retires
when V12.2 lands.

See [`hf-glob-research.md`](./hf-glob-research.md) for the design.

V11 ships the `read_delta()` TVF rather than a catalog backend. CLI users
can query a Delta root directly:

```sql
SELECT * FROM read_delta('/data/delta/sales');
SELECT * FROM read_delta('s3://bucket/delta/orders', access_key => '...');
SELECT * FROM read_delta('/data/delta/sales', version => '5');
```

Read-only; the writer pipeline lives in a follow-up. Cluster `[catalogs.X]
type = "delta"` registration is the next step on top of the TVF.

### V12: hf:// in URL-table auto-detect

Closes a V10 follow-up. V10 made `read_csv('hf://...')` work but
`SELECT * FROM 'hf://...'` (the URL-table auto-detect path) still failed
with "table not found" because DataFusion's `enable_url_table()` does
not recognise the `hf` scheme.

V12 adds an SQL pre-rewriter: `'hf://...'` quoted literals are resolved
to their HTTPS equivalent before DataFusion sees the SQL. The resulting
URL flows through V10's `LazyHttpObjectStoreRegistry` and detects format
from the file extension (`.csv`, `.parquet`, `.json`).

```sql
-- All of these now work:
SELECT * FROM 'hf://datasets/datasets-examples/doc-formats-csv-1/data.csv';
SELECT * FROM 'hf://datasets/squad/plain_text/train-00000-of-00001.parquet';
SELECT * FROM 'hf://datasets/foo/bar/data.csv?revision=v1.0';
```

Out of scope for V12 (deferred): glob patterns (`**/*.parquet`) and the
`@~parquet` revision spec for HuggingFace's auto-generated parquet view.
DuckDB supports both via their httpfs extension; SQE follows up once
DataFusion's `ListingTableUrl` learns about virtual revisions.

## Out of scope (DataFusion-blocked or niche)

Items the audit lists as not landing in this cycle. The header notes whether the
block is upstream parser work or a positioning decision.

| Item | Reason |
|---|---|
| `PIVOT` / `UNPIVOT` | DataFusion planner rejects the parsed AST node (`Unsupported ast node Pivot`) |
| `ASOF JOIN` | DataFusion has an open issue; not landed (parser wants `MATCH_CONDITION`) |
| `FROM`-first syntax | DataFusion parser does not support |
| List comprehensions, lambdas | DataFusion does not support |
| `postgres` / `mysql` / `sqlite` TVFs | positioning: SQE is Iceberg-first |
| `spatial`, `vss`, `fts`, `excel`, `azure` | niche; deferred until concrete demand |

When DataFusion adds parser support upstream, we revisit. The `iceberg-rust` and
`datafusion` versions used in this repo are pinned via the
[DF 53 upgrade constraint](https://gitlab.com/sbp-cap/cap-product/sqe/sqlengine/-/blob/main/Cargo.toml)
that the RisingWave fork imposes on us.

## Audit appendix

The original audit, run on 2026-05-07 against DuckDB stable docs, ordered the
work as:

**High priority (direct user value, low effort):**

1. `read_csv()` / `read_json()` TVFs (V8)
2. `COPY tbl TO 'file' (FORMAT ...)` (V8)
3. `SELECT * FROM 'file.parquet'` auto-detect (V8)
4. `DESCRIBE` and `SUMMARIZE` (V9)
5. `SELECT * EXCLUDE` / `REPLACE` (V9)

**Medium priority:**

6. Delta Lake reader (V11)
7. `httpfs`-equivalent (V10)
8. PIVOT / UNPIVOT / QUALIFY (out of scope, parser-blocked)
9. ASOF JOIN (out of scope, parser-blocked)
10. AWS provider chain (V10)

**Lower priority:**

11. `json` extension (have it via `datafusion-functions-json`; verify in V10)
12. `postgres` / `mysql` / `sqlite` connectors (out of scope)
13. Spatial (out of scope)
14. `vss` (out of scope; use a vector DB)
15. `fts` (out of scope)
16. Excel / Avro extensions (Avro reader covered in V8; Excel out of scope)

## Side by side

A real query a developer would actually run. Source URL: DuckDB's own public
blob store. Same machine, same network. Numbers from a smoke test on
2026-05-07.

```text
sqe> create table test2 as select * from read_parquet(
       'https://blobs.duckdb.org/train_services.parquet');
Time: 1.618s
```

```text
D create table test2 as select * from read_parquet(
    'https://blobs.duckdb.org/train_services.parquet');
Run Time (s): real 1.815 user 0.237842 sys 0.132715
```

SQE 1.618s. DuckDB v1.4.4 1.815s. Single query, single source. Real workloads
mix local Parquet, S3, hf://, and Iceberg tables; this number is not a
benchmark claim, just a smoke test that says "embedded mode is at least as
fast as DuckDB on basic file load."

The full V8-V12 narrative lives in
[the blog post](blog/2026-05-07-accidentally-duckdb.md) and ebook chapter
[16d "The DuckDB Drift"](ebook/chapters/16d-the-duckdb-drift.md).

## Related docs

- [Embedded CLI reference](cli-embedded.md): all flags, dot-commands, TVFs,
  catalog backends, storage backends, write paths in one place
- [Architecture](architecture.md): overall SQE design
- [Catalogs](book/src/getting-started/catalogs.md): multi-catalog config
  reference
- [CLI](book/src/getting-started/cli.md): cluster-mode CLI usage
- [Trino compatibility](trino-compatibility.md): separate compatibility track
- [Roadmap](roadmap.md): phase-by-phase plan
- [HF glob research](hf-glob-research.md): V12.2 design
- [The DuckDB drift (blog)](blog/2026-05-07-accidentally-duckdb.md): the V8-V12 narrative
