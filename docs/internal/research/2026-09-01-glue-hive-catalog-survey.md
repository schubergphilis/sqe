# Querying an AWS Glue Hive catalog from SQE

Survey of a live Glue Data Catalog (account 088044431914, eu-central-1)
against what SQE can read today, plus what the gap costs to close.

Two passes, hours apart. The first read a field subset and saw 618 tables;
the second pulled full definitions and saw 692. The catalog is not static,
so treat every count here as a snapshot, not a constant.

## What the catalog actually holds

692 tables: 636 `EXTERNAL_TABLE` and 56 `VIRTUAL_VIEW`. **Zero Iceberg
tables.** No table carries the `table_type=ICEBERG` parameter.

| SerDe | Tables |
|---|---|
| `org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe` | 451 |
| `org.openx.data.jsonserde.JsonSerDe` | 181 |
| `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` | 3 |
| `org.apache.hadoop.hive.serde2.OpenCSVSerde` | 1 |

224 external tables are partitioned. Data spans six buckets (`-raw-`,
`-refined-`, `-refined-sensitive-`, `-transient-`, `-ingestion-`,
`-staging-datazone`).

The file-extension and empty-location breakdowns further down were measured
against the first pass's 618 and have not been re-measured against 692. The
proportions hold; the absolute numbers have drifted upward.

All four SerDe classes are already in `serde_map.rs`'s supported set, and the
three text SerDes are all comma-delimited. SerDe coverage is 100% today.

## Why `--catalog-backend glue` does not help

That flag builds `iceberg_catalog_glue` and wraps it in
`IcebergCatalogProvider`. It resolves a Glue table by reading
`Parameters.metadata_location` and loading Iceberg metadata from it. A Hive
external table has no such pointer, so every one of the 636 external tables
fails to load. The flag is correct for what it does; it just addresses a
catalog shape this account does not have.

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

## Findings that decide whether any of this reads a byte

Sampled the first objects under all 618 table locations, then re-listed in
full every location the sample called empty:

| SerDe | File extension | Tables |
|---|---|---|
| ParquetHiveSerDe | *(none)* | 356 |
| ParquetHiveSerDe | `.parquet` | 52 |
| ParquetHiveSerDe | *(location absent)* | 25 |
| JsonSerDe | `.json.gz` | 127 |
| JsonSerDe | `.json` | 48 |
| JsonSerDe | other `.gz` | 4 |
| JsonSerDe | *(location absent)* | 2 |
| LazySimpleSerDe / OpenCSVSerde | `.csv` | 4 |

The 13 tables the first pass could not classify were transient-zone tables
mid-rewrite; on re-listing they held extensionless Athena output, so they
fold into the 356. Nothing in this catalog uses ORC, Avro, or a SerDe outside
the four listed.

**Extensionless Parquet (356 tables).** Athena CTAS writes
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

**Locations that hold nothing (27 tables).** A first pass counted 40, from a
three-object sample per table. Re-listing them in full split that number in
two, and both halves say something.

13 were a sampling race. They live in `-transient-datazone` (`afas_tmp`,
`afas_sensitive_tmp`) and were genuinely empty at 14:05, then held a fresh
Athena CTAS file 40 minutes later. The transient zone is rewritten on every
ETL pass, so "this table is empty" is a statement with a shelf life of
minutes. Any cache over Glue metadata or file listings needs a TTL short
enough to survive that, and a survey that samples once will disagree with
itself.

The other 27 are not empty. **The directory Glue points at does not exist.**
Listing the exact `StorageDescriptor.Location` returns nothing, and the
prefix is absent from its parent's `CommonPrefixes` entirely. Meanwhile the
parent holds up to 79 sibling snapshot directories that do have data:

```
glue Location:  s3://<bucket>/thecore/user/20260901-fd1c98f9-.../   (absent)
actually there: s3://<bucket>/thecore/user/20260710-dce562be-.../   (+16 more)
```

Nine `datadog_tmp` tables are the degenerate case, where the parent is gone
too. The rest are snapshot tables whose Glue pointer was advanced to a
directory that was never written, or whose target has since been
lifecycle-deleted while older snapshots survive.

This is the strongest argument for reading Glue metadata rather than
sniffing files, and it is worth stating plainly. **All 27 still carry full
column metadata in Glue**, 2 to 28 columns each, with no table missing its
schema. A metadata-driven catalog answers them correctly: right column names
and types, zero rows, which is what Athena does. A TVF cannot answer them at
all, because it has nothing to infer a schema from and fails with "Cannot
infer schema from an empty location."

So the 27 are not an edge case to tolerate. They are 4.4% of the catalog that
only a metadata-driven path can serve.

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
files carry a recognizable extension (52 `.parquet` plus 4 `.csv`). The 179
JSON tables need the TVF's own compression handling, the 356 extensionless
Parquet tables need the Hive provider's empty-extension listing, and the 27
with absent locations need Glue's schema. That is the gap the Glue metadata
source closes.

`sqe-spill`'s `S3SpillConfig` has its own credential struct and still has no
token field. Spilling to S3 under SSO remains broken, deliberately left out
of this change.

## Partition projection is not optional

A second pass pulled the full table definitions rather than a field subset,
and the catalog is bigger and stranger than the first count said: 692 tables,
636 `EXTERNAL_TABLE` plus 56 `VIRTUAL_VIEW`.

**199 of the 636 external tables set `projection.enabled = true`**, and
**192 of those have zero partitions registered in Glue.** Their partitions do
not exist as metadata at all; Athena computes them from a formula. A backend
that resolves partitions through `GetPartitions` returns an empty list for
those tables, and an empty partition list means zero rows. No error, no
warning, just a third of the catalog quietly answering nothing.

That makes projection a correctness requirement, not a feature to defer.

The shape is mercifully narrow. Three projection types appear (`date` 189,
`integer` 187, `enum` 27) and **no table sets
`storage.location.template`**, so every one uses the default Hive layout of
`<Location>/<key>=<value>/`. Verified on S3:

```
Location:  s3://<bucket>/claude/projects/
partitions: claude/projects/airflow_execution_date=2026-01-16/<uuid>.json.gz
config:    projection.airflow_execution_date.type  = date
           projection.airflow_execution_date.range = 2026-03-12,NOW
           projection.airflow_execution_date.format = yyyy-MM-dd
```

One parity trap worth naming. That table's projection range starts
2026-03-12, but S3 holds partition directories back to 2026-01-16. Projection
*restricts* what is visible, so Athena cannot see the earlier data. An
implementation that discovers partitions by listing directories would return
more rows than Athena for the same query, and "more rows" is still a parity
failure. Honor the declared range rather than what the bucket happens to
hold.

Partition keys never appear in `StorageDescriptor.Columns`. Checked across
all 224 partitioned tables: zero name overlap between `Columns` and
`PartitionKeys`. The query schema is `Columns` followed by `PartitionKeys`,
which is the order `append_partition_columns` already produces.

## Following Glue's columns, and where they need repairing

Glue should be the source of truth for the schema. It is the only source for
the 27 tables whose data directory is gone, and it is what makes the 356
extensionless tables readable at all. But its column list does not match the
files exactly, and the two ways it drifts are both silent.

Comparing Glue's declared columns against real Parquet footers:

- **Case.** Glue stores column names lowercased (Hive convention). The files
  preserve the writer's case. Four of nine readable samples had Glue `op`
  against Parquet `Op`. A case-sensitive match yields an all-NULL column.
- **Order.** Glue's column order differs from the file's on two of nine
  samples. Matching by position rather than by name would return the wrong
  column's data under the right column's name, which is the failure shape
  already recorded for plan-node width substitution and field-id projection.

So the rule is: take names and types from Glue, resolve them against the file
schema **by case-insensitive name, never by position**, fill a column absent
from the file with NULLs, and ignore a file column Glue does not declare.
That is Hive and Athena semantics, and it is what makes "follow Glue, fix
where needed" concrete.

The sample is small because of a separate gap: `read_parquet` hardcodes
`.with_file_extension(".parquet")` with no override, so it cannot read an
extensionless object even when handed its exact key. `read_json` and
`read_csv` both expose a `file_extension` argument. Adding the same argument
to `read_parquet` is a small change that would make 356 tables reachable
ad-hoc before the Glue backend lands.

## The other Glue parameters, and whether we need them

| Parameter | Tables | Use it? |
|---|---|---|
| `projection.*` | 199 | **Required.** 192 have no registered partitions. |
| `presto_view` / `VIRTUAL_VIEW` | 56 | Separate decision. Base64 Trino view SQL. |
| `classification` (`json`, `parquet`) | 286 | Cheap cross-check on the SerDe mapping. |
| `numFiles`, `totalSize` | 333 | Free statistics, no footer reads. |
| `recordCount`, `averageRecordSize` | 25 | Better statistics where the crawler ran. |
| `skip.header.line.count` | few | Already handled in `resolve_format`. |
| `compressionType` | 25 | **Not trustworthy.** Every value is `none`, yet 127 tables are `.json.gz`. Infer compression from the key suffix instead. |
| `has_encrypted_data`, `EXTERNAL`, `auto.purge`, `transactional`, `trino_*` | many | Ignore. |

The 56 views are the one open question rather than a gap. They are Trino view
definitions stored base64-encoded in `presto_view`, so serving them means
parsing Trino SQL and resolving it against the same catalog. Skipping them
means `bi_views` looks empty. Worth deciding deliberately rather than by
omission.

## Sequencing

1. S3 session token. Done, prerequisite for reading anything with SSO.
2. Gzip detection in `serde_map::listing_options`, keyed on the object
   suffix. `read_json` already does this; the Hive provider does not, and
   131 tables need it.
3. The Glue metadata source behind the read-only trait. Schema is
   `Columns ++ PartitionKeys`, resolved against files by case-insensitive
   name and never by position.
3a. Partition projection, in the same change rather than after it. Without
   it 192 tables return zero rows instead of an error, and a silent wrong
   answer is worse than an unsupported-table message.
4. Partition-level `StorageDescriptor` divergence. Glue allows a partition's
   SerDe to differ from its table's; a table migrated JSON to Parquet
   mid-life has both under one definition. That produces wrong columns rather
   than an error, so it must fail loud rather than take the table's SerDe on
   faith.
5. The TVF path allowlist (`[storage.tvf]`) has to cover all six buckets, or
   the Glue source hands back locations that get rejected downstream and the
   mapping looks broken when it is not.
6. A missing location is a valid empty table, not an error. Build the schema
   from `StorageDescriptor.Columns` and return zero rows when the prefix does
   not exist, matching Athena. Inferring schema from files would fail 27
   tables outright, and the failure would arrive as "cannot infer schema,"
   which points at the engine rather than at a dangling Glue pointer.
7. Cache with a short TTL. Transient-zone tables are rewritten on every ETL
   pass, so both the file listing and any "this table is empty" conclusion go
   stale within minutes.
