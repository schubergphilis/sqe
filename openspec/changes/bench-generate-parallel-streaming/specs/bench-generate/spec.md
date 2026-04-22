## MODIFIED Requirements

### Requirement: Generate data for a benchmark

The system SHALL generate benchmark data (TPC-H, TPC-DS, SSB, TPC-C, TPC-E, TPC-BB, ClickBench) for a given scale factor into Parquet files on disk. Generation SHALL run in parallel across multiple OS threads up to a configured `BENCH_GEN_THREADS` limit. Batches SHALL stream from generators into Parquet writers without accumulating the whole table in memory.

#### Scenario: Default parallel generation on a multi-CPU host

- **GIVEN** a host with N logical CPUs
- **AND** `BENCH_GEN_THREADS` is unset
- **WHEN** `sqe-bench generate tpch --scale 1` is invoked
- **THEN** the generator uses `min(N, num_cpus::get())` worker threads
- **AND** the generated row set matches the pre-change implementation (order-insensitive)
- **AND** all subsequent TPC-H query tests pass with the same results

#### Scenario: Bounded memory under large-scale generation

- **GIVEN** a generate invocation at SF1000 on a 32-CPU host
- **WHEN** lineitem is generated
- **THEN** peak RSS stays proportional to `threads × batch_size × row_size`
- **AND** does NOT scale with `total_rows × row_size`
- **AND** peak RSS stays under 4 GiB for SF1000 lineitem at `BENCH_GEN_THREADS=32`

#### Scenario: Wall clock scales with CPU count

- **GIVEN** a generate invocation at SF1000
- **WHEN** it runs with `BENCH_GEN_THREADS=1` vs `BENCH_GEN_THREADS=32`
- **THEN** the 32-thread run completes at least 10x faster than the 1-thread run for the dominant table (lineitem)
- **AND** both runs produce the same row set (order-insensitive)

#### Scenario: Single-threaded determinism preserved

- **GIVEN** a generate invocation at any scale factor
- **AND** `BENCH_GEN_THREADS=1`
- **WHEN** generation completes
- **THEN** the output Parquet files are byte-identical to the pre-change implementation for the same `(benchmark, scale, compression)` tuple
- **AND** their SHA-256 hashes match a committed golden value

## ADDED Requirements

### Requirement: Configurable generator parallelism

The system SHALL expose a configuration field `BENCH_GEN_THREADS` (env var) and `--threads N` (CLI flag) that bounds the maximum number of concurrent worker threads during `sqe-bench generate`. The default SHALL be `num_cpus::get()`. The effective value SHALL be clamped to `[1, 256]` and a warning logged on clamp.

#### Scenario: CLI flag overrides env var overrides default

- **GIVEN** `BENCH_GEN_THREADS=4` is exported
- **AND** the user invokes `sqe-bench generate tpch --scale 1 --threads 8`
- **WHEN** generation runs
- **THEN** 8 worker threads are used
- **AND** the env var is ignored

#### Scenario: Threads=1 produces byte-identical output

- **GIVEN** `--threads 1` (or `BENCH_GEN_THREADS=1`)
- **WHEN** `sqe-bench generate tpch --scale 1` runs
- **THEN** the output is byte-identical to the pre-change implementation's output at the same scale

#### Scenario: Invalid thread count rejected

- **GIVEN** `BENCH_GEN_THREADS=0` or `BENCH_GEN_THREADS=abc`
- **WHEN** generation starts
- **THEN** the command fails with a clear error message
- **AND** does NOT fall back silently

### Requirement: Configurable compression for generated Parquet

The system SHALL expose `BENCH_GEN_COMPRESSION` (env) and `--compression X` (CLI) to control the Parquet codec for generator output. Accepted values: `zstd1`, `zstd3` (default), `zstd9`, `snappy`, `none`. Invalid values SHALL fail fast with a message listing accepted values.

#### Scenario: Default compression matches pre-change behaviour

- **GIVEN** no compression is specified
- **WHEN** generation runs
- **THEN** output files are compressed with ZSTD level 3

#### Scenario: Faster-compression knob produces readable output

- **GIVEN** `--compression snappy`
- **WHEN** generation completes
- **THEN** the output files are valid Snappy-compressed Parquet
- **AND** a subsequent `sqe-bench load` reads them correctly

#### Scenario: Invalid compression value rejected

- **GIVEN** `BENCH_GEN_COMPRESSION=gzip`
- **WHEN** generation starts
- **THEN** the command exits with a message naming `zstd1 zstd3 zstd9 snappy none` as the accepted values

### Requirement: Per-partition file naming

The system SHALL name generator output files with a per-partition prefix to allow parallel writers to produce files without coordination. File naming SHALL follow the pattern `{partition_index:04}{file_index:05}.parquet` under `{output_dir}/{table_name}/`. Non-contiguous numbering (gaps from partitions producing fewer files than their stride reserves) SHALL NOT affect the load step.

#### Scenario: 32-way parallel generation produces non-overlapping file names

- **GIVEN** `BENCH_GEN_THREADS=32` on a generate invocation
- **WHEN** generation completes
- **THEN** every output file has a unique name of the form `{partition_index:04}{file_index:05}.parquet`
- **AND** no two worker threads wrote to the same file path

#### Scenario: Load step tolerates file-name gaps

- **GIVEN** a generated table whose partitions produced a non-contiguous set of files (e.g. `00000.parquet`, `00001.parquet`, `01000.parquet`, `02000.parquet`, ...)
- **WHEN** `sqe-bench load <benchmark>` runs against the output
- **THEN** every file is ingested into Iceberg
- **AND** the row count in the Iceberg table matches the expected SF row count
