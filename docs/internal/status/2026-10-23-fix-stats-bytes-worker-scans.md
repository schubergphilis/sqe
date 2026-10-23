**STATUS 2026-10-23: distributed scans report `physicalInputBytes` (`fix/stats-bytes-worker-scans`).**

Follow-up to the Trino stats bytes work (portal request) and !1530.

Cause: `extract_plan_metrics` sums counters named `bytes_scanned`. `DistributedScanExec` had none, so a worker scan reported 0 physical input bytes. `processedBytes` was already right: the exec's `BaselineMetrics::record_poll` records output bytes and its name matches the scan test.

Fix (coordinator only, no ticket or Flight change):
- `planned_scan_bytes(task)` in `distributed_scan.rs`: the morsel's `end_byte - start_byte`, else the sum of `file_sizes_bytes` (filled for Iceberg and Hive tickets).
- `DistributedRecordBatchStream` adds it to a `bytes_scanned` counter once: at the first batch (a LIMIT may stop polling early) or at the end of an empty stream. An error clears it, so a failed fragment counts nothing. Retries run before the first batch, so they do not double count.
- `changelog.d/feat-trino-stats-bytes.md` (unreleased): the "not counted yet" sentence now states the semantics.

Semantics: planned bytes, an upper bound of what the worker fetched once it prunes row groups or projects columns. The local Iceberg direct-read path reads whole files, so the two agree for full scans. Exact worker-fetched bytes would need a counting object store on the worker and a trailer frame on the Flight stream; not done.

Gate:
- `make test-integration FILTER=trino_stats`: 3 passed (new `trino_stats_report_bytes_for_a_distributed_scan` through an in-process worker: `0 < physicalInputBytes <= table file bytes`, `processedBytes > 0`, 9000 rows).
- `make test-integration FILTER=distributed_failure`: 6 passed.
- `cargo test -p sqe-coordinator --features test-sqlite --lib -- distributed_scan plan_metrics`: 47 passed (new `bytes_scanned_counts_planned_file_bytes`: whole files, morsel range, empty stream, failed fragment).
- `cargo clippy -p sqe-coordinator --all-targets -- -D warnings` clean.

**NEXT:** unchanged
