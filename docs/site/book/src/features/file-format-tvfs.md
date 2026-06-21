# File-format TVFs

The four TVFs `read_parquet`, `read_csv`, `read_json`, and `read_delta` query files directly without registering an external table. They share a uniform calling convention and a uniform path-resolution layer (local filesystem, S3, HTTPS, HuggingFace `hf://`).

This chapter covers `read_csv`, `read_json`, and `read_delta`. The dedicated `read_parquet` chapter covers Parquet specifically.

## Common path forms

All four TVFs accept the same path shapes:

```sql
-- Local
SELECT * FROM read_csv('/data/sales.csv');

-- S3 (anywhere object_store understands)
SELECT * FROM read_csv('s3://bucket/key.csv',
    access_key => 'AKIA...', secret_key => '...',
    endpoint => 'http://localhost:9000', region => 'us-east-1');

-- HTTP / HTTPS (V10)
SELECT * FROM read_csv('https://raw.githubusercontent.com/.../data.csv');

-- HuggingFace (V10)
SELECT * FROM read_csv('hf://datasets/squad/plain_text/train.csv');

-- HuggingFace with revision (V12.1)
SELECT * FROM read_parquet('hf://datasets/foo/bar@v1.0/data.parquet');

-- HuggingFace auto-generated parquet view
SELECT * FROM read_parquet('hf://datasets/foo/bar@~parquet/default/train/0.parquet');
```

S3 credentials default to the engine's `[storage]` block when not supplied inline. HTTPS and `hf://` paths flow through V10's `LazyHttpObjectStoreRegistry`, which constructs an `HttpStore` for the host on first request.

## Quoted-string auto-detect

V8 introduced a shortcut. With the embedded CLI, the engine recognises a quoted string in a `FROM` clause as a file URL and dispatches to the right TVF based on extension:

```sql
SELECT * FROM '/data/sales.parquet';
SELECT * FROM 's3://bucket/orders.csv';
SELECT * FROM 'hf://datasets/foo/bar/data.csv';
```

Format dispatch happens by extension. `.parquet` -> `read_parquet`, `.csv` / `.tsv` / `.psv` / `.ssv` -> `read_csv`, `.json` / `.jsonl` / `.ndjson` -> `read_json`, `.avro` -> the Avro reader. Compressed extensions are recognised: `.csv.gz`, `.tsv.zst`, `.json.bz2` all dispatch to the right reader with the right codec.

## `read_csv`

```sql
SELECT * FROM read_csv(
    '<path>',
    [delimiter | delim | sep => '<byte>',]
    [has_header | header => '<bool>',]
    [quote => '<byte>',]
    [escape => '<byte>',]
    [comment => '<byte>',]
    [null_regex | nullstr => '<regex>',]
    [compression | compress => 'auto|none|gzip|bz2|xz|zstd',]
    [file_extension => '<.ext>']
);
```

**Smart defaults**:

- Delimiter detected from the path extension. `.csv` is `,`, `.tsv` is tab, `.psv` is `|`, `.ssv` is `;`. Compression suffixes are stripped first, so `.tsv.gz` still picks tab.
- Compression detected from the path extension. `.gz`, `.bz2`, `.xz`, `.zst` are recognised.
- `has_header` defaults to true (DataFusion default).

DuckDB-style aliases: `sep`, `delim` for `delimiter`; `header` for `has_header`; `nullstr` for `null_regex`; `compress` for `compression`.

```sql
-- All three are equivalent
SELECT * FROM read_csv('events.tsv');
SELECT * FROM read_csv('events.tsv', sep => '\t');
SELECT * FROM read_csv('events.tsv', delimiter => '\t', has_header => 'true');

-- Compressed, with explicit override
SELECT * FROM read_csv('events.tsv.zst', compression => 'auto');

-- Semicolon-separated file
SELECT * FROM read_csv('financial.ssv', sep => ';');
```

## `read_json`

```sql
SELECT * FROM read_json(
    '<path>',
    [access_key | secret_key | endpoint | region | file_extension]
);
```

Reads NDJSON (one JSON document per line). Schema inference samples the first batch.

```sql
SELECT * FROM read_json('/var/log/events.jsonl');
SELECT * FROM read_json('s3://logs/2026-05-07/events.json.gz');
SELECT * FROM read_json('hf://datasets/nyu-mll/glue/cola/train.jsonl');
```

## `read_delta`

```sql
SELECT * FROM read_delta(
    '<path>',
    [access_key | secret_key | endpoint | region,]
    [version => '<u64>',]
    [timestamp => '<RFC3339>']
);
```

Read-only Delta Lake reader, V11. Wraps `deltalake-core 0.32.1`. Time travel via `version` (snapshot id) or `timestamp` (RFC3339); the two are mutually exclusive.

```sql
SELECT * FROM read_delta('/data/delta/sales');

SELECT * FROM read_delta('s3://bucket/delta/orders',
    access_key => 'AKIA...');

-- Time travel
SELECT * FROM read_delta('/data/delta/sales', version => '5');
SELECT * FROM read_delta('/data/delta/sales',
    timestamp => '2026-04-01T00:00:00Z');
```

Writes are not exposed. The Delta transaction pipeline is substantial; the read path covers the most common ad-hoc query case.

## HuggingFace specifics

The hf:// path uses a slightly different shape than S3 or HTTPS because HuggingFace expects a revision in the URL.

Two revision spellings work:

1. **Inline `@<rev>`** (DuckDB-style):
   ```sql
   SELECT * FROM read_parquet('hf://datasets/foo/bar@v1.0/train.parquet');
   ```

2. **Query parameter `?revision=<rev>`**:
   ```sql
   SELECT * FROM read_parquet('hf://datasets/foo/bar/train.parquet?revision=v1.0');
   ```

Default is `main` when neither is specified. Specifying both rejects with a clear error.

`@~parquet` is special. HuggingFace auto-generates a Parquet conversion of every dataset on the `refs/convert/parquet` branch. The TVF translates this:

```sql
-- Equivalent to https://huggingface.co/datasets/foo/bar/resolve/refs%2Fconvert%2Fparquet/data.parquet
SELECT * FROM read_parquet('hf://datasets/foo/bar@~parquet/default/train/0.parquet');
```

Glob expansion (`**/*.parquet`) is on the V12.2 roadmap; today the path must point to a specific file.

## When to use which

- **`read_parquet`**: ad-hoc queries against Parquet on disk, S3, HTTPS, or hf://. Anything Iceberg-aware that does not need the catalog.
- **`read_csv`**: ETL ingestion, log analysis, dataset preview before deciding to load into Iceberg.
- **`read_json`**: NDJSON logs, HuggingFace `train.jsonl` style splits.
- **`read_delta`**: query a Delta Lake table without converting to Iceberg.

For tables with metadata that you want to write back to, register them in a catalog. The TVFs are reads only.

## Implementation references

- `crates/sqe-catalog/src/read_parquet.rs`
- `crates/sqe-catalog/src/read_csv.rs`
- `crates/sqe-catalog/src/read_json.rs`
- `crates/sqe-catalog/src/read_delta.rs`
- `crates/sqe-catalog/src/file_tvf_common.rs`: shared parsing + S3 / HTTPS / hf:// resolver
- `crates/sqe-catalog/src/lazy_object_store.rs`: V10's lazy HTTPS object-store registry
