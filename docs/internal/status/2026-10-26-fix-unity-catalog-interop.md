**STATUS 2026-10-26: Unity Catalog interop fixes from a playground3 run (`fix/unity-catalog-interop`).**

A full SQE <-> Databricks Unity Catalog run on the playground3 workspace (UC Iceberg REST, `pg3-bronze`, scratch schema `sqe_probe`, dropped afterwards) found three bugs. This branch fixes them:

- **`timestamptz` written by another engine failed the scan** (`sqe-catalog/src/iceberg_scan.rs`). A Parquet file without an Arrow schema decodes a UTC-adjusted timestamp as `Timestamp(us, "UTC")`; the planned schema says `"+00:00"`; `conform_batch_to_projection` refused with `column types must match schema types`. `is_timezone_relabel` now casts (a relabel only). Spark-written files had the same bug; no Spark interop test had a timestamp column.
- **Copy-on-write DML committed `replace` snapshots** (`sqe-write/src/write_handler.rs`, `commit_rewrite_with_retry`). Every caller changes data (truncate, CREATE OR REPLACE, CoW DELETE / UPDATE / MERGE, INSERT OVERWRITE), so it uses `overwrite_files()` now. `replace` tells incremental readers the data did not change. Compaction (`maintenance*.rs`) keeps `rewrite_files()`.
- **ATTACH failed on one non-Iceberg table** (`vendor/datafusion-iceberg/.../schema.rs`, recorded as an SQE patch in its README). `IcebergSchemaProvider::try_new` loaded every table with `try_join_all`; a UC Delta table answers 400 `not an Iceberg compatible table`, which failed the whole ATTACH (embedded CLI and the coordinator's runtime ATTACH). It skips the table with a warning now.

playground3 results (coordinator with `bearer_token` auth validating the Databricks U2M JWT and passing it through to UC: no service account):
- PASS, SQE reading Databricks tables: basic types (after the timestamp fix; values match Databricks SQL), nested STRUCT / ARRAY / MAP, partition filter, V3 with Databricks deletion vectors, VARIANT + `variant_get`, GEOMETRY(4326) with `ST_*`, time travel by snapshot id, a Databricks INSERT seen on the next query.
- PASS, SQE writes Databricks reads: CREATE / INSERT / UPDATE / DELETE / MERGE / ADD COLUMN / CTAS / PARTITIONED BY (UC shows it as liquid clustering) / VARIANT / GEOMETRY(4326) with SRID / V2 with deletes.
- PASS, mixed: Databricks DELETE and UPDATE on an SQE table write DVs that SQE reads; SQE DELETE and UPDATE on a table with Databricks DVs rewrite copy-on-write and drop the DVs, and Databricks reads the result.
- Expected refusal: a Delta table read through UC Iceberg REST answers HTTP 400.

Findings not fixed here:
- The embedded CLI's attached catalogs do not route DDL through `sqe-sql`: VARIANT, GEOMETRY, `ARRAY<>`, `PARTITIONED BY`, `WITH (...)` and `ALTER ... ADD COLUMN` are refused there. The coordinator path handles all of them.
- The Databricks U2M token lasts about an hour and SQE has no refresh: a long session ends with 401s from UC.
- Not run: a worker scan with UC vended credentials.

Gates:
- `make test-integration FILTER=spark_interop` -> ok, 29 passed (2053 s): every bloom / geo / mor / timestamp / unknown_type / variant Spark interop test, including the new `timestamp_spark_interop::sqe_reads_and_deletes_from_a_spark_timestamp_table` (Spark sees `overwrite` after SQE's DELETE).
- Unit: `iceberg_scan` `a_utc_timestamp_relabels_to_the_declared_offset`; `sqe-write --features test-sqlite` lib 253 passed, including `cow_delete_and_update_commit_overwrite_snapshots`; `sqe-cli` `attach_skips_a_table_that_does_not_load`, which fails without the vendor patch with the same error as playground3.
- clippy `-D warnings` on sqe-catalog, sqe-write, sqe-cli (all targets) and datafusion-iceberg (lib; its test targets do not build in this workspace on main either).

**NEXT:** embedded CLI DDL on attached catalogs through `sqe-sql`; then #863 spatial file pruning.
