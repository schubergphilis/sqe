### Added

- Audit records carry what a query history needs (#812)

    A streaming SELECT's audit record now holds its scan stats (bytes and rows
    scanned, spill, peak memory) and its planning time on success, failure and
    cancel; before, it held only the row count and the total duration. Every
    statement record gains `query.query_fingerprint`, a hash of the text with
    its literals replaced, so one query shape groups across values and stays
    when `omit_query_text` drops the text. The OCSF record adds `duration`,
    `status_code`, and under `unmapped` the query id, trace id, fingerprint,
    phase timings and error type.

### Fixed

- Streaming queries show as Running and report their planning time (#812)

    The streaming path never marked a query running, so `system.runtime`
    and the web UI listed a streaming SELECT as Queued until it ended, with
    `planning_ms` 0. `queued_ms` no longer counts planning time.
