# Design: Bank benchmark with direct-to-Iceberg generation

## Architecture

```
sqe-bench generate bank --sink iceberg
  |
  |-- RestCatalog (vendored iceberg-catalog-rest, OAuth2 client credentials or bearer token)
  |     create namespace + 5 tables with partition specs   -> Polaris
  |
  |-- work queue of (table, day, shard)
  |     N worker threads (default: cores)
  |       row generator (deterministic RNG per unit)
  |         -> Arrow RecordBatch (fixed rows_per_batch)
  |         -> DataFileWriter<RollingFileWriter<ParquetWriter>>  (zstd, ~512 MB files, stats)
  |         -> table FileIO (S3-compatible, multipart)           -> s3://warehouse/.../data/day=.../
  |
  |-- commit coordinator
        all shards of day D done -> one fast_append(D's DataFiles)
        snapshot summary: sqe-bench.day = YYYY-MM-DD             -> Polaris
```

The engine never sees the load. Polaris owns the table metadata from creation, so there is no post-hoc registration step and no hand-built metadata JSON.

## Schema

Namespace: `bank_<tag>` (default tag derives from days and bytes/day, e.g. `bank_12d_4t`; `--namespace` overrides).

Dimensions, generated once, unpartitioned:

| Table | Rows | Columns |
|---|---|---|
| `customer` | `--customers` (default 10M) | c_id (long), c_name (string), c_dob (date), c_country (string, ISO2), c_segment (retail/private/sme/corporate), c_created (date) |
| `account` | ~2.5 per customer | a_id (long), a_c_id (long FK), a_iban (string), a_type (current/savings/brokerage), a_currency (EUR/USD/GBP/CHF), a_status (open/dormant/closed), a_opened (date) |
| `kyc_profile` | 1 per customer | k_c_id (long FK), k_risk_rating (low/medium/high), k_pep (boolean), k_sanctions_hit (boolean), k_last_review (date), k_next_review (date), k_source_of_funds (string) |

Facts, one Iceberg partition per calendar day (identity transform on the `date` column):

| Table | Rows/day | Columns |
|---|---|---|
| `transaction` | derived from `--bytes-per-day` | t_id (long), t_day (date, partition), t_ts (timestamp), t_a_id (long), t_counterparty_iban (string), t_counterparty_bic (string), t_amount (decimal(15,2)), t_currency (string), t_direction (debit/credit), t_channel (sepa/swift/card/internal/instant), t_category (string, MCC-like), t_status (settled/pending/rejected), t_description (string, 30-80 chars), t_balance_after (decimal(15,2)), t_country (string) |
| `account_balance` | 1 per account | b_day (date, partition), b_a_id (long), b_balance (decimal(15,2)), b_currency (string), b_txn_count (int) |

`transaction` carries the volume. The two free-text-ish columns (`t_description`, `t_counterparty_iban`) keep compressed bytes/row realistic (target ~150-250 B) so 4 TB/day means a plausible row count, not a degenerate one.

Iceberg schemas are built with explicit field IDs; the Arrow schemas used by the generator match them via `PARQUET_FIELD_ID_META_KEY` metadata so written Parquet carries the field IDs Iceberg requires.

## Determinism and sharding

- Seed = `hash64("bank", table, day_index, shard_index)`. Any unit regenerates independently and identically, on any box. Days can be split across machines later with no coordination.
- Each `transaction` shard owns a disjoint account-id range and emits rows time-ordered within the day. Files then carry tight min/max on both `t_ts` and `t_a_id` with no sort step. Point-account and time-window queries prune inside the day partition for free.
- Dimensions shard by primary-key range, same as today's `parallel_generate_table`.

## Write path detail

Per (table, day, shard) unit:

1. Load the `Table` once per table from the catalog (metadata + FileIO). Workers share it.
2. Build `ParquetWriterBuilder` (zstd, row groups sized by config) over the table's current schema, wrap in `RollingFileWriterBuilder` (target file size ~512 MB) with a `DefaultLocationGenerator` and a `DefaultFileNameGenerator` whose prefix encodes `(day, shard)`. Deterministic names make a re-run of a crashed unit overwrite its own partials.
3. `DataFileWriterBuilder::new(rolling).build(Some(partition_key))` where the partition key holds the unit's day. Written files land under `data/t_day=YYYY-MM-DD/` and their `DataFile` entries carry the partition value and column stats.
4. `close()` returns `Vec<DataFile>`; the worker hands them to the commit coordinator.

FileIO gets explicit S3-compatible properties (`s3.endpoint`, `s3.access-key-id`, `s3.secret-access-key`, `s3.region`, path-style) so bulk write throughput does not depend on Polaris credential vending; vended credentials remain the default when no explicit keys are passed.

Commit coordinator: when the last shard of day D completes, run `Transaction::new(&table).fast_append().add_data_files(files).set_snapshot_properties({"sqe-bench.day": D}).apply(tx)` and `tx.commit(catalog)`. Commits for different days may race; on `CommitFailedException`-class errors the table is reloaded and the append retried (appends never conflict logically). Dimensions commit as a single append each.

Runtime: generation stays on OS threads (CPU-bound); each worker drives its async writer with a small per-worker `block_on` current-thread runtime handle onto a shared multi-thread tokio runtime for I/O. One runtime, `threads` workers, bounded buffers.

## Memory model

Per worker: one in-progress RecordBatch (~64k rows), the parquet writer's current row group buffer, and the FileIO multipart upload buffer. Roughly 300-500 MB per worker, independent of scale. 64 workers stay under ~32 GB. No `Vec<RecordBatch>` accumulation anywhere in the bank path.

## Sizing and calibration

`--bytes-per-day 4t --days 12`:

1. Pilot: generate one calibration shard (~256 MB compressed) of `transaction` to a scratch prefix, measure compressed bytes/row, delete it.
2. `rows_per_day = bytes_per_day / bytes_per_row`; `shards_per_day = ceil(day_bytes / target_shard_bytes)` with shard size chosen so each shard emits a handful of 512 MB files.
3. Print the plan: rows/day, files/day, total files, projected duration at measured per-worker throughput times worker count, capped by a `--link-gbit` hint if given. `--dry-run` stops here.

## Failure handling and resume

- Committed days are visible as snapshots with `sqe-bench.day` summary properties. `--resume` lists snapshots, builds the set of done days, and schedules only the rest.
- A failed shard fails its day; other days proceed. The day's files stay uncommitted (invisible to readers). Re-running regenerates the day; deterministic file names overwrite the partials. A final sweep can list `data/t_day=D/` prefixes of uncommitted days and delete leftovers.
- Ctrl-C is safe at any point: Iceberg readers only see committed snapshots.

## CLI

`generate` gains:

- `--sink parquet|iceberg` (default `parquet`, existing behavior)
- `--days N`, `--start-date YYYY-MM-DD`, `--bytes-per-day SIZE` (accepts `4t`, `500g`), `--customers N` (bank only)
- `--catalog-uri`, `--warehouse`, `--namespace`, `--token-endpoint`, `--client-id`, `--client-secret` (or `--bearer-token`)
- existing `--s3-*` flags reused for FileIO properties
- `--dry-run`, `--resume`, `--target-file-size`

`bank` is also a normal benchmark name for the parquet sink (scale maps to a small fixed day count) so the sweep test and local smoke runs work without a catalog.

## Key decisions

1. **Direct commit via iceberg-rust, not `register_table` or CTAS.** Single write of every byte, real stats in manifests, incremental day snapshots, and the catalog owns metadata from birth. CTAS at 48 TB funnels through one coordinator; register_table means hand-building metadata.
2. **Identity partition on a date column, one partition per day.** Matches the "last 14 days" demo query shape exactly; 10-12 partitions of ~4 TB each, ~8k files of 512 MB per partition, well within manifest comfort.
3. **Shard by account range, time-ordered within shard.** Zone maps on both hot columns without any sort, so the SF10 sort-on-write OOM class of problems cannot occur here.
4. **Per-day commits, not per-shard.** 10-12 snapshots tell a clean "daily ingest" story and keep manifest churn trivial; per-shard commits would create thousands of snapshots for no benefit.
5. **Bytes-per-day sizing with pilot calibration, not a scale factor.** The demo requirement is stated in TB/day; calibration converts it to rows honestly instead of hard-coding a bytes/row guess.
