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

**Gzipped JSON (131 tables).** `serde_map::listing_options` builds
`JsonFormat::default()`, which is `FileCompressionType::UNCOMPRESSED`.
Pointed at `.json.gz` it feeds gzip bytes to the JSON parser. This is the one
format gap the Glue backend has to close itself, since Glue's
`StorageDescriptor` does not record compression; it has to be inferred from
the object key suffix. 72% of the JSON tables here are affected.

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

Verified against the live account: before the fix, listing returned 403
`InvalidAccessKeyId`; after, the same query lists successfully.

`sqe-spill`'s `S3SpillConfig` has its own credential struct and still has no
token field. Spilling to S3 under SSO remains broken, deliberately left out
of this change.

## Sequencing

1. S3 session token. Done, prerequisite for reading anything with SSO.
2. Gzip detection for JSON, keyed on the object suffix. Unblocks 131 tables.
3. The Glue metadata source behind the read-only trait.
4. Partition-level `StorageDescriptor` divergence. Glue allows a partition's
   SerDe to differ from its table's; a table migrated JSON to Parquet
   mid-life has both under one definition. That produces wrong columns rather
   than an error, so it must fail loud rather than take the table's SerDe on
   faith.
5. The TVF path allowlist (`[storage.tvf]`) has to cover all six buckets, or
   the Glue source hands back locations that get rejected downstream and the
   mapping looks broken when it is not.
