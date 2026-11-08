### Added

- `system.runtime.query_history`: finished queries read back from the audit log, across the tracker TTL and coordinator restarts (#812)

    Same columns as `system.runtime.queries`. The table adds the finished
    queries in the native audit log (rotated generations included) to the
    tracker's records, deduplicated by query id. A filter on `end` or
    `created` picks the part of the log that is read; without one, the last
    `[query_history] lookback_secs` (new, default 86400) is read. One scan
    returns at most the newest 100000 audited queries. Non-admin users see
    only their own queries. With `metrics.audit.format = "ocsf"` the table
    shows the tracker only and warns once. The web UI history view is a
    follow-up.
