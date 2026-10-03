# read_parquet TVF

`read_parquet()` is a table-valued function registered on every SQE `SessionContext`. It reads Parquet files from local disk or S3-compatible storage and returns them as a DataFusion table scan, making Parquet files directly queryable without first loading data into Iceberg.

## Syntax

```sql
SELECT * FROM read_parquet(
  '<path>',
  [access_key => '<key>',]
  [secret_key => '<secret>',]
  [endpoint => '<url>',]
  [region => '<region>']
)
```

The first argument is the file path or glob pattern. All other arguments are named (keyword) parameters for S3 credentials. Named parameters are optional and fall back to the engine's configured storage defaults when omitted.

## Local files

Absolute paths and glob patterns both work:

```sql
-- Single file
SELECT * FROM read_parquet('/data/tpch/sf1/lineitem/part-0000.parquet');

-- All files in a directory
SELECT * FROM read_parquet('/data/tpch/sf1/lineitem/*.parquet');

-- Recursive glob
SELECT * FROM read_parquet('/data/tpch/sf1/lineitem/**/*.parquet');
```

The schema is inferred from the Parquet metadata of the first matched file. All matched files must share the same schema.

## S3 with inline credentials

Pass credentials directly in the SQL statement. This is the primary mechanism used by `sqe-bench load` to inject credentials at load time without relying on environment variables or configuration files.

```sql
SELECT * FROM read_parquet(
  's3://bench-data/tpch/sf1/lineitem/*.parquet',
  -- nosemgrep: opt.aikido.semgrep-rules.generic.secrets.gitleaks.aws-access-token -- AWS documentation example key
  access_key => 'AKIAIOSFODNN7EXAMPLE',
  secret_key => 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
  endpoint   => 'http://localhost:9000',
  region     => 'us-east-1'
);
```

All four named parameters are optional independently. Omit `endpoint` for AWS S3 (uses the default AWS endpoint for the given region). Omit `region` to default to `us-east-1`.

## S3 with default credentials

When no inline credentials are provided, `read_parquet()` falls back to the storage configuration in `sqe.toml`:

```sql
-- Uses [storage] section from sqe.toml
SELECT * FROM read_parquet('s3://bench-data/tpch/sf1/lineitem/*.parquet');
```

This is convenient for internal workloads where the engine already has ambient S3 credentials configured.

### Which paths the engine's credentials may read

On a server, a read with the engine's own credentials is denied unless the
path falls under an allowed prefix. The same rule covers `read_csv` and
`read_json`. The global list applies to every session:

```toml
[storage.tvf]
allowed_object_store_prefixes = ["s3://shared-landing/", "s3://scratch/{user}/"]
```

A catalog can add prefixes of its own. They apply only to sessions scoped to
that catalog, so one coordinator can serve several workspaces that each read
only their own landing buckets:

```toml
[catalogs.ws_energy]
tvf_prefixes = ["s3://ws-energy-landing/"]

[catalogs.ws_retail]
tvf_prefixes = ["s3://ws-retail-landing/"]
```

A session is scoped to the catalog it resolves as its default: the one named
by `X-Trino-Catalog`, `USE` or the Flight SQL catalog, or the configured
default catalog when it names none. A session on `ws_retail` that reads
`s3://ws-energy-landing/...` is refused, and the error names the session
catalog. A warehouse discovered from Polaris, not declared in `[catalogs.*]`,
adds no prefixes. Catalog prefixes match on path-segment boundaries and
expand `{user}` just like the global list. An entry that is not an
object-store URL with a bucket fails config validation at startup. Hive
external tables use the global list only, because a Hive root is shared by
every session that reads it.

## Glob patterns

`read_parquet()` supports the same glob syntax as object_store:

| Pattern | Matches |
|---------|---------|
| `*.parquet` | All `.parquet` files in the named directory |
| `**/*.parquet` | All `.parquet` files in any subdirectory |
| `part-00[0-9][0-9].parquet` | Files matching the character class |

For S3 paths, globbing is applied to the key prefix after the bucket name.

## Using with CTAS for data loading

The primary use case for `read_parquet()` is ingesting external Parquet data into Iceberg tables via CTAS. This avoids an intermediate format conversion step. The Parquet files are read directly and written as Iceberg data files in one operation.

```sql
-- Load TPC-H lineitem from local disk
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet('/data/tpch/sf1/lineitem/*.parquet');

-- Load from S3 with inline credentials
CREATE TABLE tpch_sf1.lineitem AS
SELECT * FROM read_parquet(
  's3://bench-data/tpch/sf1/lineitem/*.parquet',
  access_key => 'AKIA...',
  secret_key => '...',
  endpoint   => 'http://localhost:9000',
  region     => 'us-east-1'
);

-- Transform during load
CREATE TABLE analytics.orders_summary AS
SELECT
  o_orderdate,
  o_orderstatus,
  COUNT(*) AS order_count,
  SUM(o_totalprice) AS total_revenue
FROM read_parquet('/data/tpch/sf1/orders/*.parquet')
GROUP BY o_orderdate, o_orderstatus;
```

Because `read_parquet()` returns a standard DataFusion table scan, it participates in the full optimizer pipeline: predicate pushdown, projection pruning, and partition pruning all apply.

## Querying without loading

`read_parquet()` can also be used as a one-off query target, without creating an Iceberg table:

```sql
-- Inspect schema
DESCRIBE SELECT * FROM read_parquet('/data/tpch/sf1/orders/*.parquet') LIMIT 0;

-- Quick aggregation over raw Parquet
SELECT o_orderstatus, COUNT(*) AS cnt
FROM read_parquet('/data/tpch/sf1/orders/*.parquet')
GROUP BY o_orderstatus;

-- Join Parquet with an Iceberg table
SELECT p.p_name, l.l_quantity
FROM read_parquet('/data/tpch/sf1/lineitem/*.parquet') AS l
JOIN warehouse.tpch_sf1.part AS p ON l.l_partkey = p.p_partkey
LIMIT 20;
```

## Implementation

`read_parquet()` is registered in `sqe-catalog` as a DataFusion `TableFunction`. On each invocation:

1. The path argument is parsed to detect `s3://` vs local (`/` or `file://`) paths.
2. For S3: an `AmazonS3Builder` is constructed from the inline named parameters, with fallback to the `StorageConfig` from `sqe-core` for any omitted fields.
3. For local paths: the built-in DataFusion local filesystem `ObjectStore` is used.
4. Glob patterns are expanded against the chosen `ObjectStore`.
5. A `ListingTable` is constructed over the matched files and returned as the table scan node.

The function is registered on every `SessionContext` at startup, so it is always available without any special configuration.

## Limitations

- All matched Parquet files must share an identical Arrow schema. Schema evolution across files in the same glob is not supported.
- `read_parquet()` is read-only. It cannot be used as the target of an INSERT INTO.
- Credential parameters are passed as SQL literals. Avoid logging or displaying these queries in audit logs without redaction. SQE's audit logger redacts named parameter values that match `access_key`, `secret_key`, and `session_token` patterns.
- Very large numbers of matched files (>10,000) may cause slow planning due to the object listing step.
