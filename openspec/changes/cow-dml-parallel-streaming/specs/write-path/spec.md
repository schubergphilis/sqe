## MODIFIED Requirements

### Requirement: UPDATE with predicate

The system SHALL update rows matching a WHERE predicate via Copy-on-Write rewrite. The handler SHALL rewrite affected data files concurrently up to a configured `cow_dml.writer_parallelism` limit. The rewrite SHALL stream rewritten batches to the parquet writer rather than collecting them in memory. The affected-row count returned to the client SHALL be computed from the rewrite pass itself, not from a second SQL round trip.

#### Scenario: UPDATE with 4 affected data files and parallelism 4

- **GIVEN** a table with 4 Iceberg data files, each containing rows that match the UPDATE predicate
- **AND** `cow_dml.writer_parallelism = 4`
- **WHEN** an UPDATE statement is submitted
- **THEN** the handler rewrites all 4 files concurrently
- **AND** a single atomic `rewrite_files` commit replaces all 4 old files with all 4 new files
- **AND** the result set and reported affected-row count match exactly what `writer_parallelism = 1` would have produced on the same input

#### Scenario: UPDATE output streams batch-by-batch

- **GIVEN** a data file whose rewrite produces more than one output RecordBatch
- **WHEN** the handler runs `apply_update`
- **THEN** every output batch is delivered to the parquet writer
- **AND** no batches are silently dropped (regression gate for the `.next()` latent bug in the prior implementation)
- **AND** peak memory for the in-flight file rewrite is bounded to one batch plus writer buffers

#### Scenario: Affected-row count matches matched predicate evaluations

- **GIVEN** a deterministic UPDATE predicate matching 12,345 rows across 3 data files
- **WHEN** the statement completes
- **THEN** the handler returns `affected_rows = 12345`
- **AND** this count is derived from a `__sqe_matched` projection inside the CoW SELECT, not from a second `SELECT COUNT(*)` round trip

### Requirement: DELETE FROM with predicate

The system SHALL delete rows matching a WHERE predicate. Per-file rewrites SHALL run concurrently up to `cow_dml.writer_parallelism`. Both the CoW (full-file rewrite) and MoR (position-delete) code paths SHALL stream rewritten or delete-record batches to their writers.

#### Scenario: CoW DELETE with parallelism matches serial output

- **GIVEN** a table with 8 data files
- **AND** `cow_dml.writer_parallelism = 4`
- **WHEN** a DELETE targets rows distributed across all 8 files
- **THEN** the resulting table state is identical to a run with `writer_parallelism = 1`
- **AND** a single atomic `rewrite_files` commit lands

#### Scenario: MoR DELETE with parallelism writes one position-delete file per input file

- **GIVEN** a table configured for MoR DELETE and 6 data files with matching rows
- **AND** `cow_dml.writer_parallelism = 3`
- **WHEN** a DELETE is submitted
- **THEN** 6 position-delete files are written (one per input data file)
- **AND** they are committed in a single transaction

## ADDED Requirements

### Requirement: Configurable writer parallelism

The system SHALL expose a configuration field `cow_dml.writer_parallelism` that bounds the maximum number of concurrent per-file rewrites for `UPDATE` and `DELETE` CoW statements. The field SHALL default to `min(logical_cpus, 8)` at config load. The system SHALL clamp the loaded value to the range `[1, 64]` and log a warning on clamp.

#### Scenario: Default parallelism is CPU-bounded

- **GIVEN** a coordinator started with no explicit `cow_dml.writer_parallelism` in config
- **WHEN** the coordinator initialises
- **THEN** the effective parallelism is `min(num_cpus, 8)`

#### Scenario: Operator override to single-threaded mode

- **GIVEN** an operator sets `cow_dml.writer_parallelism = 1`
- **WHEN** an UPDATE statement runs
- **THEN** the per-file loop executes sequentially
- **AND** the result matches the pre-change serial implementation exactly

#### Scenario: Out-of-range value is clamped

- **GIVEN** a config containing `cow_dml.writer_parallelism = 128`
- **WHEN** the coordinator loads the config
- **THEN** the effective value is clamped to 64
- **AND** a warning log records the clamp

### Requirement: Parallel commit atomicity

The system SHALL perform exactly one Iceberg `rewrite_files` commit per DML statement regardless of how many data files were rewritten or how many tasks ran in parallel. A per-file rewrite failure SHALL abort the whole statement and leave the table's committed state unchanged.

#### Scenario: Mid-stream failure aborts the transaction

- **GIVEN** a 6-file UPDATE with `writer_parallelism = 3`
- **WHEN** the third file's rewrite produces an error
- **THEN** no new data files are committed to the table
- **AND** the other in-flight rewrites are aborted
- **AND** the table's committed state is the pre-UPDATE state
- **AND** any orphan parquet objects left in object storage are the responsibility of lifecycle GC (documented, unchanged from prior behaviour)

### Requirement: Per-batch match count via projection

The system SHALL count matched rows during UPDATE by projecting an additional boolean-cast column `__sqe_matched` in the per-batch CoW SELECT and summing its values across all result batches. The system SHALL strip this column before handing the RecordBatch to the parquet writer. The system SHALL NOT execute a separate `SELECT COUNT(*)` pass over the same batch.

#### Scenario: Match count equals what the old double-query path reported

- **GIVEN** a deterministic UPDATE predicate
- **WHEN** the same statement runs on a post-change coordinator and (hypothetically) on a pre-change coordinator
- **THEN** both report the same `affected_rows` count
- **AND** the post-change path performs one SQL round trip per batch, not two

#### Scenario: `__sqe_matched` does not appear in the output file

- **GIVEN** any UPDATE statement
- **WHEN** the rewritten parquet file is read back
- **THEN** its schema matches the original table schema
- **AND** no `__sqe_matched` column is present
