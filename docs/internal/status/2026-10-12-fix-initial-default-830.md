**STATUS 2026-10-12: rows written before `ALTER TABLE ADD COLUMN ... DEFAULT` read the default (`fix/initial-default-830`, #830, openspec iceberg-matrix-parity 5.11/5.13).**

A data file that predates a column has no values for it. Iceberg says such a file reads the column's `initial_default`. SQE read NULL. Two places caused it:

- **SQE's direct-read scan** (`IcebergScanExec`, small files). `conform_batch_to_projection` NULL-filled every column a file lacked. It now takes per-column defaults from `initial_default_scalars`, computed once per scan. The default is looked up by name in the scanned snapshot's schema, then in the current one, so an ALTER with no write since still applies. Each value is cast to the scan's declared Arrow type. Binary, fixed and uuid are built from their bytes; everything else goes through the Iceberg JSON text and an Arrow cast. A defaulted column fills even when it is NOT NULL.
- **The vendored iceberg-rust reader** (`to_arrow()` path, large files, or threshold 0). Its projection already filled the default, but its row-filter `PredicateConverter` treated a missing column as NULL. So `WHERE region = 'eu'` dropped every pre-ALTER row before DataFusion's own filter saw it. Now `bound_reference` resolves a missing column with an `initial_default` to `PredicateColumn::Default` (a one-row array, repeated per batch). Marked `SQE PATCH (sqe#830)`.

Tests:
- Unit: fill (nullable and required, plus a column with no default) and literal conversion per type in `iceberg_scan.rs`. Vendored predicate test for `=`, `starts_with`, `IS NULL`, `IS NOT NULL`.
- Polaris: `v3_e2e.rs::alter_add_column_with_default_on_v3_table` covers four cases: ALTER then SELECT with no write; a post-ALTER insert; `WHERE region = 'eu'`; `WHERE region IS NULL`. The V3 suite passes 17/17 on the direct-read path and with `SQE_CATALOG__SMALL_FILE_THRESHOLD_MB=0` (the `to_arrow()` path).

The worker path has a separate, older bug, #831. A distributed scan over any file that predates `ADD COLUMN` fails on the worker ("number of columns(1) must match number of fields(2)"), with or without a DEFAULT, because the worker never reshapes the decoded batch. Reproduced on the 2-worker rig. The matrix entry is `full` with #831 as a caveat. Scans below `query.distribution_threshold` run on the coordinator.

`make test-distributed` could not boot on main. The rig config had drifted behind four startup guards: plaintext transport, the empty worker secret, coordinator memory over the container limit, and the scan-ticket audience (`advertise_url`). That fix ships on its own branch.

**NEXT:** #831 (worker reshape plus `initial_default`); then dbt-sqe per-connection transactions (openspec `single-table-transactions` 5.4), then #816 and #803.
