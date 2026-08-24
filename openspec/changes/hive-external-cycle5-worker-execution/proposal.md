## Why

Hive scans run coordinator-local. A distributed deployment gets zero scan parallelism on a Hive table: the coordinator reads every file itself while the workers idle. Cycle 2 made *planning* cheap (the partition index replaced the prefix LIST), which sharpened the contrast rather than fixing it, because execution never moved.

This is the last structural gap in the Hive read path, and it is invisible until it hurts: the table works, returns correct rows, and simply does not scale with the cluster.

`ScanTask`, the unit a coordinator ships to a worker, cannot carry a Hive scan today. Measured, not assumed:

- **It is Parquet-shaped.** Its sub-file addressing is `row_group_start` / `row_group_end`, and `crates/sqe-worker/src/executor.rs` builds every reader through `ParquetRecordBatchStreamBuilder`. Hive external tables are CSV, line-delimited JSON, *or* Parquet. Two of the three formats have no reader on the worker at all.
- **It is Iceberg-shaped.** Projection travels as `projected_field_ids`, resolved against `PARQUET:field_id` metadata. A Hive table has no field IDs; it projects by name. `projected_columns` already exists as the fallback, so this half is nearly free.
- **It has nowhere to put partition values.** This is the real obstacle. Hive partition columns are directory-encoded (`k=v/`) and do not exist in the data file. `HiveTableProvider` materializes them coordinator-side in `attach_partition_values`, wrapping each per-partition scan in a `ProjectionExec` that substitutes a literal per partition column. A worker has no equivalent and no field to carry the values.

## What Changes

- `ScanTask` v3: an optional Hive descriptor carrying file format, SerDe parameters, the partition-value map for the task's files, and the full (file + partition) schema. Absent on an Iceberg task, so an Iceberg ticket is byte-identical to v2 and old workers are unaffected.
- Worker-side format dispatch: CSV and line-delimited JSON readers alongside the existing Parquet path, honoring the SerDe parameters the manifest recorded (delimiter, quote, escape, header).
- Worker-side partition materialization: rebuild `attach_partition_values`' projection from the task's partition-value map, so a worker produces the same columns in the same order the coordinator would have.
- Coordinator planning: split a Hive scan into per-partition tasks from the partition index (cycle 2) and dispatch them, falling back to coordinator-local execution when the worker path cannot serve the table.
- A named, tested fallback rather than a silent one. A format or feature a worker cannot serve must run coordinator-local and say so, not return wrong rows.

## Capabilities

### New Capabilities
- `hive-worker-scan`: the v3 ticket shape, worker-side format dispatch and partition materialization, coordinator-side task splitting, and the fallback contract.

### Modified Capabilities
- `hive-external-tables`: scans may execute on workers. The cycle-1 statement surface is unchanged.

## Success Criteria

- A partitioned Hive table scans across workers, and the rows match coordinator-local execution exactly, per format (CSV, JSON, Parquet).
- Partition columns materialize identically on the worker path, including `__HIVE_DEFAULT_PARTITION__` and typed non-string keys.
- An Iceberg `ScanTask` is byte-identical to its v2 encoding, proven by a serialization test, so this cannot regress the Iceberg path.
- A table the worker path cannot serve runs coordinator-local and is observable as having done so.
- Measured scan parallelism on a multi-worker rig, against the cycle-2 coordinator-local baseline.

## Rollback

The Hive descriptor is optional and the coordinator decides whether to emit it. Disabling emission returns every Hive scan to coordinator-local execution with no ticket change, so rollback is a config flip rather than a revert.
