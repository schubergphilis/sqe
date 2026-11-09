### Added

- `system.runtime.query_history` and a web UI History tab: finished queries read back from the audit log, across the tracker TTL and coordinator restarts (#812)

    `system.runtime.query_history` has the same columns as
    `system.runtime.queries`. It adds the finished queries in the native
    audit log (rotated generations included) to the tracker's records,
    deduplicated by query id. A filter on `end` or `created` picks the part
    of the log that is read; without one, the last
    `[query_history] lookback_secs` (new, default 86400) is read. One scan
    returns at most the newest 100000 audited queries. Non-admin users see
    only their own queries. With `metrics.audit.format = "ocsf"` the table
    shows the tracker only and warns once. The dashboard's new History tab
    (`GET /api/v1/queries/history`, admin-gated) pages through the same
    rows, 50 at a time, with a window and state filter. The log is read
    newest first with a binary search to the window end: on a week of log
    (about a million queries) a one-hour window reads in about 25 ms and a
    50-row page in under 3 ms.
