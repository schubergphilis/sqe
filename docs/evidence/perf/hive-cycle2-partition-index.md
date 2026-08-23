# Hive cycle 2: partition index vs prefix LIST

## 64-partition smoke (2026-08-22)

Measured on a local CSV fixture: 64 `dt=` partitions, one 2-row file each, `file://` store, DataFusion 54. Debug-unoptimized times are not the number to quote. The committed JSON in `benchmarks/results/hive-cycle2-partition-index.json` is from a `dev-release`-shaped run of the same fixture (opt-level 2, no LTO).

The comparison is planning plus collect of `SELECT count(*) FROM t` and of `SELECT count(*) FROM t WHERE dt = '2026-01-32'` (one pinned partition). Cold is the first query after registering the catalog. Warm is the third repeat in the same process.

| case | listing (ms) | indexed (ms) |
|---|---|---|
| cold, no predicate | 18.4 | 12.1 |
| warm, no predicate | 4.6 | 3.9 |
| cold, partition pin | 11.2 | 2.8 |
| warm, partition pin | 3.1 | 1.4 |

The pin is the cycle-2 win: listing still walks the table prefix (DataFusion only prunes a leading equality after the LIST of that prefix's children), the index prunes to one entry and LISTs only that partition directory. Unpinned counts still visit every partition; the index only saves the recursive prefix walk.

## Scale run (2026-08-23)

`dev-release` profile, local `file://`, `crates/sqe-catalog/tests/hive_partition_bench.rs`. Cold is the first query on a fresh `SessionContext`. Warm is the third. Pin is `WHERE dt = '<last partition>'`. Projection uses `projection.dt.type=date` over the same range and does not LIST the table prefix.

JSON: `benchmarks/results/hive-cycle2-partition-index-scale.json`.

| case | listing cold/warm (ms) | indexed cold/warm (ms) | projection cold/warm (ms) | MSCK (ms) |
|---|---|---|---|---|
| csv 256, full scan (12 800 rows) | 329 / 79 | 63 / 25 | | 27 |
| csv 256, pin (50 rows) | 57 / 28 | 1.8 / 0.8 | 6.1 / 0.9 | |
| csv 1024, full scan (20 480 rows) | 628 / 239 | 219 / 56 | | 115 |
| csv 1024, pin (20 rows) | 219 / 111 | 2.3 / 1.1 | 7.1 / 1.5 | |
| csv 4096, pin (2 rows) | 1248 / 445 | 11 / 4.3 | 12 / 4.7 | 458 |
| parquet 256, full scan (51 200 rows) | 406 / 48 | 57 / 7.7 | | 32 |
| parquet 256, pin (200 rows) | 75 / 31 | 1.7 / 0.7 | 5.9 / 0.7 | |
| parquet 1024, pin (50 rows) | 321 / 106 | 10 / 1.4 | 8.2 / 1.9 | 204 |

At 4096 partitions a pinned count is about 110x faster from the index than from listing, cold. Projection is in the same band as the index for a pin: both skip the prefix LIST. A full unpinned scan still reads every file; the index only removes the recursive walk, which is why 1024-wide full CSV is 628 ms listing vs 219 ms indexed rather than a 100x jump.

Rerun:

```bash
cargo test -p sqe-catalog --test hive_partition_bench --features hive-external \
  --profile dev-release -- --ignored --nocapture --test-threads=1
```

## Public S3 LIST (2026-08-23)

Anonymous `aws s3 ls --no-sign-request` from this laptop. SQE does not yet unsigned-sign public buckets, so these numbers are the LIST cost a cycle-1 scan or `MSCK REPAIR` would pay before any decode. JSON: `benchmarks/results/hive-public-s3-list.json`.

| prefix | what | wall (ms) | objects / prefixes |
|---|---|---|---|
| `s3://ookla-open-data/parquet/performance/` recursive | all Ookla speedtest parquet | 1230 | 62 files |
| `s3://ookla-open-data/parquet/performance/type=fixed/year=2024/quarter=1/` | one quarter, 346 MiB file | 1427 | 1 file |
| `s3://aws-public-blockchain/v1.0/btc/blocks/` | Hive `date=` prefixes | 2377-4321 | 6437 prefixes (2009-01-03 .. 2026-08-23) |
| `s3://aws-public-blockchain/v1.0/btc/blocks/` recursive | MSCK analogue, every object | 6470 | 6501 files, 328 MiB |
| `s3://aws-public-blockchain/v1.0/btc/blocks/date=2024-01-01/` recursive | one day | 934-1103 | 1 file, 55 KiB |
| `s3://aws-public-blockchain/v1.0/btc/blocks/date=2009-*` recursive | genesis year | 1207 | 358 files, 9.3 MiB |
| `s3://commoncrawl/cc-index/table/cc-main/warc/` | URL index | AccessDenied without an AWS account | |

A Bitcoin-scale MSCK is the recursive LIST: 6.5 s for 6501 objects today. The prefix LIST of 6437 `date=` dirs is 2-4 s and does not open the files.

## Bitcoin SQE scan (2026-08-23)

Copied `date=2009-*` plus `date=2024-01-*`/`02-*`/`03-*` to local disk (449 Hive partitions, 15 MiB, 11 s anonymous GET). Schema is scalar Parquet (`hash`, `number`, `transaction_count`, ...). SQE `dev-release` via `hive_btc_public_slice_bench`. JSON: `benchmarks/results/hive-btc-slice.json`.

DuckDB on the same files: 45 869 blocks, 36 977 562 transactions; 155 blocks / 657 752 tx on 2024-01-01. SQE matched both counts.

| query | listing cold/warm (ms) | indexed cold/warm (ms) | projection cold/warm (ms) |
|---|---|---|---|
| `count(*)` (45 869 rows, 449 parts) | 196 / 55 | 121 / 23 | |
| `WHERE date = '2024-01-01'` (155 rows) | 99 / 48 | **1.9 / 0.7** | 11 / 3.1 |
| `sum(transaction_count)` pin | 96 / 49 | **1.5 / 0.9** | |
| `sum(transaction_count)` all | | 144 / 27 | |
| `MSCK REPAIR` | 48 ms for 449 dirs | | |

The pin is ~50x faster from the index than from listing. Projection over `2009-01-03,2024-03-31` is slower than the index on a pin (11 ms vs 1.9 ms) because it materializes every calendar day in that range, including the holes in early 2009, then prunes. The index only stores dirs that exist.

Rerun after the copy:

```bash
aws s3 cp --no-sign-request --recursive \
  s3://aws-public-blockchain/v1.0/btc/blocks/ /tmp/sqe-btc-hive/data/ \
  --exclude '*' --include 'date=2009-*' --include 'date=2024-01-*' \
  --include 'date=2024-02-*' --include 'date=2024-03-*'

cargo test -p sqe-catalog --test hive_partition_bench --features hive-external \
  --profile dev-release hive_btc_public_slice_bench -- --ignored --nocapture
```

Ookla remains the better *decode* target (346 MiB files). Bitcoin is the partition-count target: 6437 live `date=` prefixes, one small snappy parquet per day.

## SQE vs DuckDB on the same Bitcoin slice

Same 449 files, same OS page cache, same answers (45 869 blocks, 155 on 2024-01-01, 36 977 562 tx). DuckDB 1.5.5 in-process Python: cold is the first `execute`, warm is the median of the next six. SQE is `dev-release` as above. JSON: `benchmarks/results/hive-btc-duckdb.json`.

| query | DuckDB cold/warm (ms) | SQE listing | SQE indexed | winner |
|---|---|---|---|---|
| `count(*)` all | 70 / 62 | 196 / 55 | 121 / **23** | SQE index, warm |
| `WHERE date = '2024-01-01'` | 44 / 44 | 99 / 48 | **1.9 / 0.7** | SQE index, ~50x |
| `sum(transaction_count)` pin | 45 / 44 | 96 / 49 | **1.5 / 0.9** | SQE index, ~50x |
| `sum(transaction_count)` all | 62 / 63 | | 144 / **27** | SQE index, warm |
| `max(number)` pin | 44 / 45 | | | DuckDB only in this run |
| live S3 pin `date=2024-01-01` | 1116 / 369 | n/a | n/a | DuckDB (unsigned S3) |

What that means:

- **Correctness is not the story.** Both engines return the same integers DuckDB printed first.
- **A partition pin is where SQE's index shows up.** DuckDB still globs `date=*/*.parquet` (EXPLAIN says `Scanning Files: 1/449`), so a one-day filter stays around 44 ms. SQE listing is in that same band (99 ms cold, 48 ms warm). SQE with `MSCK` then reads the index and does not glob: 2 ms cold, sub-millisecond warm.
- **A full scan is decode plus opening 449 footers.** DuckDB is steadier (70 to 62 ms). SQE listing is slower cold (196 ms, Union of listing tables). After the index exists, SQE warm `count(*)` and `sum(tx)` beat DuckDB (23-27 ms vs 62 ms).
- **DuckDB can query the public bucket today.** Unsigned S3 pin is 1.1 s cold / 370 ms warm for one 55 KiB object. SQE still needs engine credentials, so this run copied the slice to disk first.

Fairness notes: both runs hit a hot page cache. SQE cold includes Hive provider construction. DuckDB cold includes the first glob of 449 paths. Neither is a cold-disk laptop benchmark, and neither is TPC-H.
