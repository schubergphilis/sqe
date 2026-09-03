# Handover: querying an AWS Glue Hive catalog

Branch `feat/glue-sidecar-bridge`, merge request !908, 10 commits ahead of
`main` and clean. Everything below was verified against the live catalog in
account 088044431914, eu-central-1.

## What you can do now

Point the server at Glue and query every database in the region, with no
copy of the catalog anywhere:

```bash
eval "$(aws configure export-credentials --format env)"
export AWS_REGION=eu-central-1
./target/dev-release/sqe-server --config <toml> \
  --mcp-anonymous --mcp-port 8099 \
  --glue-catalog glue --glue-region eu-central-1
```

Tables resolve as `glue.<database>.<table>`. Build with `--features glue`
on `sqe-coordinator`; it is off by default because the AWS SDK is the
heavyweight dependency the slim build exists to avoid.
`[[hive.roots]] kind = "glue"` is the config equivalent.

Measured working: `SHOW TABLES FROM glue.claude` lists 6 tables,
`claude.user_usage` returns 72,946 rows, gzipped JSON (`afas_raw.absence`)
returns 5,370, a nested `struct` column (`claude_raw.user_usage`) reads
`actor.email` as real values, and a projected table resolves a `NOW` range
across date x hour axes.

## How it fits together

`HiveMetadataSource` (`hive/source.rs`) is the seam: five read methods,
`list_databases`, `list_tables`, `get`, `etag`, `limits`. Two
implementations. `HiveManifestStore` reads `_sqe/table.json` sidecars, for
a warehouse with no metastore. `GlueMetadataSource` (`hive/glue_source.rs`)
reads Glue directly, which is what you want when a metastore already
exists.

`put`/`delete` are deliberately off the trait. SQE owns a sidecar warehouse
and does not own Glue, so `HiveRoot::sidecar()` returns `None` for a Glue
root and DDL refuses by name. Reads go through `source_for`, writes through
`store_for`; keep that split when adding statements.

The Glue source resolves lazily. Never enumerate a namespace and load every
table in it: that is exactly what makes the upstream Iceberg Glue provider
fail the entire attach on one unreadable table, and a 692-table catalog
always has one.

## The mirror is superseded

`scripts/glue-to-sidecar.py` and `quickstart/glue-readonly/` predate the
Glue source. They still work and remain useful for a warehouse with no
metastore, but for Glue they are obsolete and actively misleading: a copy
of a catalog is a cache with no invalidation. Snapshot tables rotate their
directory nightly, so a mirrored table read 72,946 rows one day and 0 the
next, silently, because the copied `Location` named a prefix that had been
deleted. Zero rows is also what a legitimately empty table looks like.

## Known gaps, in the order they will bite

**Gzipped JSON reads as nulls.** Decompression works now, but DataFusion
matches JSON keys case-sensitively while Glue lowercases column names. The
files hold `Medewerker`, Glue declares `medewerker`, so every column whose
file key is not already lowercase comes back null. Athena reads these
because openx JsonSerDe defaults `case.insensitive = true`. Affects most of
the 131 gzipped tables. The fix is case-insensitive key resolution; nothing
in the current code does it.

**`information_schema.tables` still fails**, after about 29 seconds of
work. It now fails on the dead Iceberg anchor's `/v1/config` call rather
than on anything Hive. Two tolerance points were fixed (an unreadable table
is hidden, an unlistable catalog contributes no rows) and it is still one
layer short. The right fix is not more error tolerance: make
`catalog.catalog_url` optional so a Glue-only deployment does not need a
fake anchor at all. That also removes the 186ms the anchor probe costs on
every single query.

**Unpruned projected tables take about 44 seconds.** `claude_raw.*` project
`airflow_execution_date` over `2026-03-12,NOW`, roughly 175 dates. Without
a partition predicate each becomes a prefix that is listed and read, one at
a time. The same query with `WHERE airflow_execution_date = '2026-09-01'`
returns in under a second. Not a bug, but the listings are sequential;
fanning them out with bounded concurrency, the way `list_metadata_tables`
already does at 16-wide, is the real lever.

**`SHOW PARTITIONS` refuses on a Glue root.** `list_partitions` needs an
object store for the data location, not just metadata, so it is
sidecar-only.

**56 `VIRTUAL_VIEW` tables are skipped.** Their Trino SQL lives
base64-encoded in `presto_view`.

**ORC, Avro, SequenceFile and RCFile are rejected by name.** None appear in
this catalog. Avro is the cheapest to add: DataFusion 55 ships an `avro`
feature, so it is a cargo flag plus one `HiveFileFormat` variant.

## Traps that cost real time

**The server reads `SQE_STORAGE__S3_*`, and now `AWS_*` too.** It did not
before, which produced a server whose every read fell through to instance
metadata. A query failing with `PUT http://169.254.169.254/...` after ten
retries means the engine has no credentials: on a laptop that is an expired
SSO token, not an engine fault. Credentials are read once at startup and
never refreshed, so renew and restart.

**Audit logging is a silent no-op until you set a path.**
`[metrics] audit_log_path` writes one JSON line per query with the actor,
the fully-qualified tables the plan touched, the operation, timing and a
hash chain. Empty by default, which reads as "SQE does not log table
access".

**Under `--mcp-anonymous` every caller logs as `mcp-anonymous`.** The trail
shows what was read, not by whom. That is the cost of anonymous mode;
`static` or `oauth` puts a real principal in `actor`.

**Nested type field names matter.** List children are named `element` and
map entries `key_value` with a non-null key, matching what arrow-rs
produces reading Parquet. A mismatch there reads as a missing column rather
than an error, which is why the tests assert on the names rather than just
on the shape.

## Testing

```bash
cargo test -p sqe-catalog --features glue
cargo test -p sqe-coordinator --lib          # 798 pass
```

The live Glue tests need credentials and are `--ignored`:

```bash
eval "$(aws configure export-credentials --format env)"
AWS_REGION=eu-central-1 cargo test -p sqe-catalog --features glue \
  --test glue_source_live -- --ignored --nocapture
```

`in_subquery_or_stack_overflow` stack-overflows on `main` too; it is not
this branch. `sqe-coordinator` has two pre-existing dead-code clippy errors
in `maintenance.rs`, also on `main`.

## Next

1. Make `catalog.catalog_url` optional. Fixes `information_schema`, drops
   186ms per query, and delivers the no-config startup that prompted this.
2. Case-insensitive JSON key resolution. Unblocks the ~131 gzipped tables
   that currently return nulls.
3. Concurrent partition listing. Turns 44 seconds into a few.
4. An `--s3tables` flag. Separate and smaller: S3 Tables is Iceberg, so it
   bypasses this trait entirely.
