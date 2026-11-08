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

- `queued_ms` no longer counts planning time, and a cancel during planning stays a cancel (#812)

    `system.runtime` reported a query's planning time twice, once as
    `planning_ms` and again as `queued_ms`. A query cancelled while it
    planned was set back to Running once planning returned.
