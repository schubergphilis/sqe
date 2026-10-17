**STATUS 2026-10-17: `test_distributed_select` runs on the worker (`test/distributed-select-worker`).**

The test could not pass against a local worker. Under the default distribution thresholds (`distribution_file_threshold` 4, byte threshold 128 MiB) its one-file CTAS table was scanned on the coordinator. It also searched `system.runtime.tasks.node_id` for "worker", but a distributed fragment records the worker URL there. It now uses `distributed_failure_e2e::rig` (thresholds zeroed, the server's channel pool) and matches `node_id == worker_url`. Verified against a worker on :50052 started with `SQE_WORKER__ADVERTISE_URL=http://localhost:50052`: 1 passed.

**NEXT:**
- A V3-aware delete trigger for compaction and `table_health` (deletion-vector cardinality / data-file record count).
