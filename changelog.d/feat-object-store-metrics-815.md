### Added

- Object-store request metrics on coordinators and workers (#815)

    `sqe_object_store_requests_total`, `sqe_object_store_errors_total`,
    `sqe_object_store_request_duration_seconds` and `sqe_object_store_ttfb_seconds`
    cover the object_store client (worker data-file reads, Hive external tables, file
    functions), labelled by `scheme`, `bucket` and `operation`; errors also carry an
    `error` class. OpenDAL's Prometheus layer now exports the `opendal_operation_*` and
    `opendal_http_*` families for Iceberg metadata and manifest I/O; the gap between
    them is the retry count. Spill to S3 is not counted.
