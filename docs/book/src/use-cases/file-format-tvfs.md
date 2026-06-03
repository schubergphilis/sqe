# File-format TVFs

SQE can read raw files directly, without first registering them as tables,
through table-valued functions. Point them at a local path or an object-store
URL and select from the result like any table. The common use is loading data
into Iceberg with `CREATE TABLE ... AS SELECT * FROM read_*(...)`.

Three formats are covered here: `read_csv`, `read_parquet`, and `read_json`.
A fourth, `read_delta`, reads Delta Lake tables; see
[File-format TVFs reference](../features/file-format-tvfs.md) for its options.

## Prerequisites

Local-path reads are gated by config; the flag is on in the test profile:

```toml
[storage.tvf]
allow_local_paths = true
```

Without it, only object-store URLs (`s3://`, `https://`, ...) are allowed. The
gate exists so a server does not expose its local filesystem to query authors.

## read_parquet

```sql
CREATE TABLE test_ns.from_parquet AS
  SELECT * FROM read_parquet('/data/test.parquet');
SELECT * FROM test_ns.from_parquet ORDER BY id;
```

## read_csv

DuckDB-compatible options are accepted (`delim`/`sep`, `header`/`has_header`,
`quote`, `escape`, `nullstr`, `compression`):

```sql
CREATE TABLE test_ns.from_csv AS
  SELECT * FROM read_csv('/data/test.csv');
-- with options:
SELECT * FROM read_csv('/data/pipe.csv', delim => '|', header => true);
```

## read_json

Reads newline-delimited JSON (one object per line):

```sql
CREATE TABLE test_ns.from_json AS
  SELECT * FROM read_json('/data/test.json');
```

## Verified round-trip

All three TVFs were exercised end to end against the live Polaris stack:
generate a fixture file, `CREATE TABLE ... AS SELECT * FROM read_*()`, read the
Iceberg table back, and assert the three known rows. The CSV and JSON tests
were added in this round; `read_parquet` was already covered.

```text
test test_read_json_local_file ... ok
test test_read_csv_local_file ... ok
test test_read_parquet_local_file ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 68 filtered out; finished in 23.61s
```

## How it is tested

- `crates/sqe-coordinator/tests/integration_test.rs`:
  `test_read_parquet_local_file`, `test_read_csv_local_file`,
  `test_read_json_local_file`. Run them with:

```bash
docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
cargo test -p sqe-coordinator --test integration_test -- --ignored local_file
```

## Notes

- Schema is inferred: integer columns come back as `BIGINT`, text as `VARCHAR`.
- Object-store reads use the same credential passthrough as table scans, so a
  `read_parquet('s3://...')` runs as the authenticated user.
