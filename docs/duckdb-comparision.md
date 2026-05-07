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
| Distributed execution | Ballista-derived scheduler + workers | single-process |
| Per-query OIDC bearer pass-through to Iceberg / S3 | yes | no |
| OPA / Cedar policy enforcement (row filters, column masks) | yes | no |
| Multi-catalog: Polaris + Nessie + Glue + HMS + S3Tables in one engine | yes (V6) | extension-by-extension |
| Per-catalog auth (SessionBearer, ClientCredentials, Anonymous, Static, Aws) | yes (V7) | no |
| Iceberg V3 read + write (position deletes, manifest column stats) | yes | extension, read-only |
| Arrow Flight SQL wire protocol | yes | extension |

## What DuckDB has that SQE doesn't (yet)

The audit groups gaps by category. The roadmap section below shows which slice
each item lands in.

### Data import / export

| DuckDB | SQE today | Plan |
|---|---|---|
| `read_parquet(path)` | have (with inline S3 creds) | done |
| `SELECT * FROM 'file.parquet'` (auto-detect) | missing | V8 |
| `read_csv(path, ...)` | missing | V8 |
| `read_json(path, ...)` / `read_json_auto` | missing | V8 |
| `read_avro(path, ...)` | missing | V8 |
| `COPY tbl TO 'file' (FORMAT csv\|json\|parquet)` | missing | V8 |
| `COPY tbl FROM 'file.csv'` | missing | V8 (paired with above) |
| Gzip / zstd compressed CSV / JSON | partial (parquet only) | V8 |
| `INSERT INTO ... VALUES (...)` | partial (default catalog only) | V5+ ongoing |
| `CREATE TABLE ... AS SELECT ...` | partial (cluster ok, embedded V5 partial) | ongoing |

### SQL surface

| DuckDB | SQE | Plan |
|---|---|---|
| Standard `CREATE / DROP / INSERT / UPDATE / DELETE` | have | done |
| `SELECT / WHERE / GROUP BY / HAVING / ORDER BY / LIMIT / DISTINCT` | have | done |
| Joins (INNER, LEFT/RIGHT/FULL OUTER, CROSS) | have, with dynamic filter pushdown | done |
| CTEs (`WITH ...`), recursive CTEs | have | done |
| Window functions | have | done |
| Aggregates (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE) | have | done |
| `COUNT(*) FILTER (WHERE ...)` | have | done |
| `DESCRIBE table` | partial (DataFusion-native works, no `.describe` shortcut) | V9 |
| `SUMMARIZE table` | missing (DuckDB-specific column-stats query) | V9 |
| `SELECT * EXCLUDE (col)` | missing | V9 |
| `SELECT * REPLACE (expr AS col)` | missing | V9 |
| `FROM tbl SELECT ...` (FROM-first) | missing | not planned (DataFusion parser does not support) |
| Struct / list / map literals (`{a: 1}`, `[1, 2]`, `MAP {...}`) | partial (nested types work, syntax less ergonomic) | not planned |
| List comprehensions, lambdas | missing | not planned (DataFusion does not support) |
| `PIVOT` / `UNPIVOT` | missing | not planned (DataFusion does not support) |
| `QUALIFY` | missing | not planned (DataFusion does not support) |
| `ASOF JOIN` | missing | not planned (DataFusion has open issue, not landed) |

### Extensions

| DuckDB extension | SQE coverage | Plan |
|---|---|---|
| `httpfs` (HTTP/HTTPS/S3 filesystem) | partial (S3 in `read_parquet` only) | V10 |
| `aws` (SDK provider chain) | partial (explicit creds required) | V10 |
| `azure` (blob storage) | missing | not planned (file an issue if needed) |
| `iceberg` | stronger than DuckDB (full read+write, V3, vending, multi-catalog) | done |
| `delta` (Delta Lake) | missing | V11 |
| `parquet` | have | done |
| `json` (functions) | have via `datafusion-functions-json` | V10 verify + document |
| `avro` | partial (no TVF, see V8) | V8 |
| `postgres` / `mysql` / `sqlite` connectors | missing | not planned (Iceberg-first positioning) |
| `spatial`, `vss`, `fts`, `excel` | missing | not planned (niche; use a real spatial / vector / FTS DB) |
| `icu` (timezone, collation) | partial (chrono-tz; collation gaps) | not planned |
| HuggingFace `hf://` URL resolver | missing | V10 (not in DuckDB core; aligned with `httpfs`) |

## Roadmap

Each slice is one MR. The numbers reference the high / medium priority list in
the audit (`docs/duckdb-comparision.md` -> [Audit appendix](#audit-appendix)).

### V8: File format TVFs + auto-detect + COPY TO

Items 1, 2, 3.

- `read_csv(path, [delimiter=...,  has_header=true, ...])`
- `read_json(path, [...])`, `read_json_auto(path)`
- `read_avro(path, [...])`
- `SELECT * FROM 'file.csv'` auto-detect on extension
- `COPY <source> TO 'file' (FORMAT csv|json|parquet)`

DataFusion 53 already ships `datafusion-datasource-csv`,
`datafusion-datasource-json`, and `datafusion-datasource-avro` in our build.
The work is wrapping them in TVF traits matching the shape of the existing
`ReadParquetFunction`.

### V9: SQL surface niceties

Items 4, 5.

- `.describe <table>` dot-command + `DESCRIBE` integration
- `.summarize <table>` dot-command + `SUMMARIZE <table>` SQL form
- `SELECT * EXCLUDE (col1, col2) FROM t`
- `SELECT * REPLACE (lower(name) AS name) FROM t`

`SUMMARIZE` is a per-column UNION ALL: count, distinct, min, max, null_count.
`EXCLUDE` / `REPLACE` is a pre-planner rewrite: parse `*`, expand against the
schema, drop or substitute columns.

### V10: Network-transparent file access + auth

Items 7, 10, 11.

- `httpfs`-equivalent: HTTP/HTTPS URL fetcher (`https://...`) usable from any
  file TVF, not only `read_parquet`. Generic `[storage.http]` block for
  headers and auth.
- HuggingFace `hf://datasets/<org>/<name>/resolve/<revision>/<path>` resolver
  layered on `httpfs`. Reads `HF_TOKEN` env var; revision pinning via
  `?revision=<sha>`.
- AWS provider chain: when `[storage]` has no `s3_access_key`, fall back to
  the standard SDK chain (env vars, `~/.aws/credentials`, IMDS, IRSA).
- JSON UDF documentation: confirm `datafusion-functions-json` covers
  DuckDB's JSON surface and document the gaps.

### V11: Delta Lake reader

Item 6.

- Wrap `delta-rs` like the engine wraps `iceberg-rust`.
- Same `ListingTableProvider` shape used in V5.
- Embedded mode: `[catalogs.delta_warehouse]` with `type = "delta"` and a
  filesystem path or S3 URI.
- Cluster mode: same, behind whatever auth the catalog's storage block declares.

## Out of scope (DataFusion-blocked or niche)

Items the audit lists as not landing in this cycle. The header notes whether the
block is upstream parser work or a positioning decision.

| Item | Reason |
|---|---|
| `PIVOT` / `UNPIVOT` | DataFusion parser does not support |
| `QUALIFY` | DataFusion parser does not support |
| `ASOF JOIN` | DataFusion has an open issue; not landed |
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

## Related docs

- [Architecture](architecture.md): overall SQE design
- [Catalogs](book/src/getting-started/catalogs.md): multi-catalog config
  reference
- [CLI](book/src/getting-started/cli.md): embedded mode usage
- [Trino compatibility](trino-compatibility.md): separate compatibility track
- [Roadmap](roadmap.md): phase-by-phase plan
