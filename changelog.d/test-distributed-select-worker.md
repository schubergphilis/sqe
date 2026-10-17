### Fixed

- `test_distributed_select` passes against a running worker

    It planned a one-file table under the default distribution thresholds
    (4 files, 128 MiB), so the scan stayed on the coordinator, and it looked
    for "worker" in `system.runtime.tasks.node_id`, which holds the worker's
    URL. It now builds its handler with `distributed_failure_e2e::rig`,
    which zeroes both thresholds, and matches `node_id` against the worker
    URL.
