# Spec: bank benchmark and direct Iceberg sink

## Requirement: Bank schema generation

GIVEN `sqe-bench generate bank --sink parquet --scale 0.001`
WHEN generation completes
THEN five tables are produced (customer, account, kyc_profile, transaction, account_balance)
AND transaction and account_balance rows carry a valid `t_day`/`b_day` date column
AND every foreign key (a_c_id, k_c_id, t_a_id, b_a_id) references a generated primary key.

## Requirement: Deterministic units

GIVEN two runs with the same seed inputs for a `(table, day, shard)` unit
WHEN their batches are generated
THEN the produced RecordBatches are identical
AND regenerating one unit does not depend on any other unit having run.

## Requirement: Direct Iceberg creation and commit

GIVEN `--sink iceberg` with a reachable Polaris and S3-compatible store
WHEN the run starts
THEN the namespace and five tables are created through the REST catalog with identity day partition specs on the fact tables
WHEN all shards of a day complete
THEN exactly one snapshot is committed for that day carrying summary property `sqe-bench.day = YYYY-MM-DD`
AND its data files land under the fact table's `data/t_day=.../` prefix with column stats in the manifest.

## Requirement: Bounded memory

GIVEN any `--bytes-per-day` value
WHEN generation runs with N workers
THEN per-worker memory is bounded by one batch, one row-group buffer, and one multipart buffer
AND no code path accumulates a day's batches in memory.

## Requirement: Sizing by target bytes

GIVEN `--bytes-per-day 4t --days 12 --dry-run`
WHEN calibration completes
THEN the printed plan states measured bytes/row, rows/day, shards/day, files/day, and projected duration
AND no table data is committed.

## Requirement: Resume

GIVEN a run interrupted after committing days 1..k
WHEN rerun with `--resume`
THEN days 1..k are skipped
AND days k+1.. are regenerated and committed
AND readers never observe partial days.

## Requirement: 14-day demo queries

GIVEN the queries in `benchmarks/queries/bank/`
WHEN run against a loaded bank namespace
THEN each query filters facts to a trailing window of at most 14 days
AND at least one query joins transaction to kyc_profile on flagged customers.
