# Tasks: Bank benchmark with direct-to-Iceberg generation

## Phase 1: Bank generator

- [x] 1.1 `generate/bank.rs`: Iceberg + Arrow schemas (field IDs wired) for customer, account, kyc_profile, transaction, account_balance
- [x] 1.2 Streaming row generators: fixed-size batches, seed per (table, day, shard), account-range shards, time-ordered transactions
- [x] 1.3 Implement `BenchmarkGenerator` for the parquet sink (scale maps to small day count); register in `get_generator`
- [x] 1.4 Unit tests: determinism (same seed, same bytes), day/shard independence, batch size bounds

## Phase 2: Iceberg sink

- [x] 2.1 `sink/iceberg.rs`: RestCatalog builder (client-credentials or bearer token), namespace + table creation with partition specs, explicit S3 FileIO props
- [x] 2.2 Writer unit: DataFileWriter over RollingFileWriter/ParquetWriter with per-(day, shard) deterministic file names and partition key
- [x] 2.3 Work queue + worker pool + shared tokio runtime; bounded per-worker memory
- [x] 2.4 Commit coordinator: per-day fast_append with `sqe-bench.day` snapshot property, retry on commit conflict
- [x] 2.5 Dimension path: generate once, single append per table

## Phase 3: Sizing, resume, CLI

- [x] 3.1 Calibration: pilot shard, bytes/row measurement, rows/day + shard plan, `--dry-run` report
- [x] 3.2 `--resume`: read day-marker table properties (snapshot summaries are not durable on Nessie), skip committed days
- [x] 3.3 CLI flags on `generate` (`--sink`, `--days`, `--start-date`, `--bytes-per-day`, `--customers`, catalog/auth flags, `--dry-run`, `--resume`, `--target-file-size`)
- [x] 3.4 Unit tests: size parsing, calibration math, plan shape

## Phase 4: Queries and validation

- [x] 4.1 `benchmarks/queries/bank/q1..q8.sql`: 14-day windowed demo queries
- [x] 4.2 Add bank to the generator sweep test
- [x] 4.3 `cargo test -p sqe-bench` and workspace clippy clean
- [x] 4.4 Integration smoke against quickstart stack (2 days, ~200 MB): tables created, snapshots per day, SQE row counts match plan (manual, documented)

## Phase 5: Docs and finish

- [x] 5.1 Update README roadmap + nextsteps.md
- [x] 5.2 Commit, push branch, open MR
