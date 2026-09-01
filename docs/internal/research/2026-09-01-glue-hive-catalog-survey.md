# Querying an AWS Glue Hive catalog from SQE

Survey of a live Glue Data Catalog (account 088044431914, eu-central-1, 618
tables) against what SQE can read today, plus what the gap costs to close.

## What the catalog actually holds

All 618 tables are Hive `EXTERNAL_TABLE`. **Zero Iceberg tables.** No table
carries the `table_type=ICEBERG` parameter.

| SerDe | Tables |
|---|---|
| `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe` | 433 |
| `org.openx.data.jsonserde.JsonSerDe` | 181 |
| `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` | 3 |
| `org.apache.hadoop.hive.serde2.OpenCSVSerde` | 1 |

224 of 618 tables are partitioned. Data spans six buckets (`-raw-`,
`-refined-`, `-refined-sensitive-`, `-transient-`, `-ingestion-`,
`-staging-datazone`).

All four SerDe classes are already in `serde_map.rs`'s supported set, and the
three text SerDes are all comma-delimited. SerDe coverage is 100% today.

## Why `--catalog-backend glue` does not help

That flag builds `iceberg_catalog_glue` and wraps it in
`IcebergCatalogProvider`. It resolves a Glue table by reading
`Parameters.metadata_location` and loading Iceberg metadata from it. A Hive
external table has no such pointer, so every one of the 618 tables fails to
load. The flag is correct for what it does; it just addresses a catalog shape
this account does not have.

Extending that backend is not the move either. `iceberg::Catalog::load_table`
returns an Iceberg `Table` built from a metadata file. Making it return
something else means forking upstream iceberg-rust and changing a trait whose
whole contract is Iceberg.

## The right seam: a second metadata source for the existing Hive provider

`hive/manifest.rs` already states the design intent in its first four lines:
field names mirror Glue's `TableInput` and `StorageDescriptor` exactly, "so a
later Glue or Hive Metastore backend is a field mapping." The manifest struct
*is* Glue's data model.

Everything downstream of the manifest already works: `serde_map.rs` picks the
file format from the SerDe class, `partition.rs` handles the partition index,
`scan_exec.rs` runs the scan. Only the source of the manifest changes, from
`_sqe/table.json` in object storage to Glue `GetTable` / `GetPartitions`.

Scope the trait to reads only: `get(db, table) -> HiveManifest`,
`list_databases`, `list_tables`. Leave `put`/`delete` off it, matching the
read-only posture `--catalog-backend` already has.

## Two findings that decide whether any of this reads a byte

Sampled the first objects under all 618 table locations:

| SerDe | File extension | Tables |
|---|---|---|
| ParquetHiveSerDe | *(none)* | 343 |
| ParquetHiveSerDe | `.parquet` | 52 |
| ParquetHiveSerDe | *(empty prefix)* | 38 |
| JsonSerDe | `.json.gz` | 127 |
| JsonSerDe | `.json` | 48 |
| JsonSerDe | other `.gz` | 4 |
| JsonSerDe | *(empty prefix)* | 2 |
| LazySimpleSerDe / OpenCSVSerde | `.csv` | 4 |

**Extensionless Parquet (343 tables).** Athena CTAS writes
`20260901_064011_00178_rk5z8_<uuid>` with no suffix. The file TVFs
(`read_parquet`) filter on extension and report "No files found," which reads
like a permissions problem and is not one. The Hive provider is already
correct here: `resolve_format` returns `file_extension: String::new()`, so
`ListingOptions` matches every object under the prefix.

**Gzipped JSON (131 tables).** The two read paths disagree here, and the
difference matters because the Glue backend inherits the weaker one.

`read_json` handles it: the TVF has its own `compression` argument defaulting
to `auto`, detected from the path extension. Confirmed by reading a
`.json.gz` object directly, which returned rows.

`serde_map::listing_options` does not: it builds `JsonFormat::default()`,
which is `FileCompressionType::UNCOMPRESSED`, and never calls
`with_file_compression_type`. A Hive table whose data is gzipped feeds gzip
bytes to the JSON parser. Glue's `StorageDescriptor` does not record
compression at all, so the Glue backend has to infer it from the object key
suffix the same way the TVF already does. 72% of the JSON tables here are
gzipped.

One table (`JsonSerDe` over `.xml.gz`) is XML that Glue has recorded as JSON.
No amount of compression handling makes that readable, and it should be
reported as an unsupported table rather than a parse failure.

40 table locations hold no data objects at all. Those should report an empty
table, not an error.

## Prerequisite, now fixed: S3 session tokens

`StorageConfig` had no `s3_session_token` field, so every S3 object store
built from it signed requests without `x-amz-security-token`. With SSO or any
assumed role the request fails as `InvalidAccessKeyId`, which looks like a
wrong key rather than a missing token. The distributed worker path
(`ScanTask.s3_session_token`) already carried a token end to end; the
coordinator and embedded paths dropped it, and `mount.rs::store_for_secret`
destructured a secret's own token into `session_token: _`.

Fixed on `feat/glue-hive-catalog`: the field exists, the three S3 builders
attach it alongside an explicit key pair, `hive_s3_access` forwards it, and
`EmbeddedS3Options` reads `SQE_STORAGE__S3_SESSION_TOKEN` then
`AWS_SESSION_TOKEN`.

Verified against the live account. Before the fix, listing returned 403
`InvalidAccessKeyId`. After, a full query runs:

```
$ eval "$(aws configure export-credentials --format env)"
$ sqe-cli --embedded --memory -e "SELECT count(*) AS n FROM read_parquet(
    's3://<bucket>/snapshots/dotcomregistration/latest/public/customer/')"
+----+
| n  |
+----+
| 14 |
+----+
```

Reading a Glue table through a TVF today means naming its S3 location by
hand and picking the matching TVF, and it only works for the 56 tables whose
files carry a recognizable extension. That is the gap the Glue metadata
source closes.

`sqe-spill`'s `S3SpillConfig` has its own credential struct and still has no
token field. Spilling to S3 under SSO remains broken, deliberately left out
of this change.

## Sequencing

1. S3 session token. Done, prerequisite for reading anything with SSO.
2. Gzip detection in `serde_map::listing_options`, keyed on the object
   suffix. `read_json` already does this; the Hive provider does not, and
   131 tables need it.
3. The Glue metadata source behind the read-only trait.
4. Partition-level `StorageDescriptor` divergence. Glue allows a partition's
   SerDe to differ from its table's; a table migrated JSON to Parquet
   mid-life has both under one definition. That produces wrong columns rather
   than an error, so it must fail loud rather than take the table's SerDe on
   faith.
5. The TVF path allowlist (`[storage.tvf]`) has to cover all six buckets, or
   the Glue source hands back locations that get rejected downstream and the
   mapping looks broken when it is not.
