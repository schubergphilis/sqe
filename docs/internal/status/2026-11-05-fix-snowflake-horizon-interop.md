**STATUS 2026-11-05: Snowflake Horizon reads: embedded mode, partial page index (`fix/snowflake-horizon-interop`).**

First live run against a Snowflake account's Horizon Iceberg REST endpoint.
The coordinator (`bearer_passthrough`, `[catalog] access_delegation =
"vended-credentials"`) loaded and planned every probe table: V2 and V3,
VARIANT, GEOMETRY, GEOGRAPHY, identity and month/bucket/truncate partitions,
nested object/array/map and the scalar types. The tables were still empty
then, so this proved schemas, not data. Embedded mode had two gaps, both fixed
here:

- No way to request vended credentials: new ATTACH option `ACCESS_DELEGATION`
  (`'vended-credentials'` | `'none'`, DuckDB's `ACCESS_DELEGATION_MODE`
  spelling accepted) in `sqe-catalog/src/mount.rs`.
- Horizon answered ATTACH's concurrent `loadTable` burst with 429, and the
  throttled tables vanished silently. Vendored REST client replays 429s
  (`SQE PATCH (catalog-429)`, `client.rs`), and datafusion-iceberg's
  `IcebergSchemaProvider::try_new` loads 16 tables at a time.

Seeding the probe tables surfaced a third: Snowflake writes no offset index
for VARIANT and nested columns, and SQE loaded the page index as
`PageIndexPolicy::Required` (vendored reader via `PageIndexPolicy::from(true)`,
`iceberg_scan.rs` small-file path, worker executor), so any read of such a
table failed with "missing offset index". All three are `Optional` now
(`SQE PATCH (partial-page-index)`); VARIANT paths and struct/array/map values
then read correctly. Snowflake also pins an Iceberg GEOMETRY column to SRID
4326, and SQE reads it as `geometry(4326)` with correct WKT and area.

Environment findings, not SQE bugs: an external volume encrypted with a
customer KMS key fails, because Snowflake's down-scoped vended session lacks
`kms:Decrypt` (even `loadTable` 403s), so use `SNOWFLAKE_MANAGED` storage.
Creating a table through Horizon needs the database's `EXTERNAL_VOLUME` and
`CATALOG = 'SNOWFLAKE'` parameters set.

Open gaps: Snowflake upper-cases unquoted names, and SQE folds them to lower
case (#337), so Snowflake tables need quoted names. Trino's
`case-insensitive-name-matching` is the model for a fix. sqe-cli installs no
tracing subscriber, so a table skipped at ATTACH (Delta in Unity Catalog, an
unreadable volume) still disappears without a message.

**Update, same day: the full matrix run.** The harness
(`scripts/interop/snowflake-horizon/`, own MR) ran Snowflake writes + SQE
reads, SQE writes + Snowflake reads, and DML from both engines on both sides'
tables. Phases A-D: 0 errors. Value compare: 31/31 in embedded,
coordinator-only and distributed (one worker, `distribution_threshold = 0`).
Fixes it took, all in this MR:

- Writes to a `[catalogs.<name>]` target used the legacy `[catalog]`
  warehouse (`configured_write_target`, `CatalogOps` and the write handler).
- Horizon refuses an explicit table `location` (400): `create_table_at_location`
  resends without it.
- Horizon paces commits: a commit within ~0.5 s of the last gets a 409
  "concurrently modified" with no failed requirement, or a 500
  "Indeterminate result during conflict resolution check", neither applied.
  Schema commits resend unchanged (`rest_catalog.rs`, 4 x 500 ms); data
  commits back off >= 500 ms on that 409 (`commit_backoff_ms`). An unknown
  outcome retries only after a 1 s settle + reload shows the metadata
  location unchanged (`commit_with_retry`, `commit_rewrite_with_retry`);
  `WriteCleanupGuard::keep_if_outcome_unknown` keeps files on every write
  and maintenance path.
- Distributed: a worker without a projection fill labelled batches with the
  first file's column order. A Snowflake-written file (`ts, id,
  METADATA$RL_ROW_ID, region, ...`) bound by position. The column list and
  the fill are now sent on every scan that reads columns, unprojected
  `SELECT *` included (`IcebergScanExec::output_columns` /
  `projection_fill`), closing the worker-path gap left by !784. Only
  `COUNT(*)` goes without.

Snowflake quirks, not SQE bugs: `ST_AREA` on an Iceberg GEOMETRY is negative
whichever engine wrote it; Snowflake parquet carries row-lineage columns
(`METADATA$RL_*`, field ids 2147483539/40) in a non-schema order.

Snowflake's DELETE / UPDATE / MERGE were copy-on-write on the harness's small
tables (no delete files in any snapshot). That is a size effect, not a rule:
with `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'enabled'` a 4M-row table gets V2
position deletes and V3 deletion vectors. Reading those failed in SQE with
"cannot skip miniblock of size 512", a parquet decoder limit fixed by
parquet 59.3.2 (!1662). A merge-on-read SQE table (`write.delete.mode`)
covers the other direction: SQE's DELETE writes a deletion vector, Snowflake
reads the table correctly, and a later Snowflake DELETE folds the vector into
a rewritten file that SQE reads back. The harness keeps one such table
(`sqe_v3_mor`), so the compare is 33/33 in all three modes.

Regression check on the same build: the Databricks UC harness
(`scripts/interop/databricks-uc/`, catalog `pg3-bronze`) ran phases A-D with
0 errors and 27/27 in embedded, coordinator-local and distributed mode.
Its V3 tables carry Databricks deletion vectors, so the always-sent fill
works alongside worker-applied deletes. The always-sent fill is no rollout
risk: the coordinator refuses to register a worker with an older scan-ticket
version (`WorkerHello::check_compat`), so a mixed cluster fails at startup,
not at query time.

Still open: `$files` has no `content` column, so delete files and deletion
vectors are not listed; the case-folding gap above (drafted as a follow-up
issue to #337: a per-catalog fold has to resolve before policy rewriting).

**NEXT:** merge this MR, then the harness MR (matrix cells
`sqe:snowflake-horizon-catalog:v2/v3` to measured) and the blog post.
