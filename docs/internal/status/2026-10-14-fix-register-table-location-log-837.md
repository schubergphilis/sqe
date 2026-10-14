**STATUS 2026-10-14: `register_table` logs only the scheme, bucket and a path hash of the metadata location (`fix/register-table-location-log-837`, #837).**

`maintenance.rs::register_table` logged the caller-supplied `metadata_location` verbatim at INFO, so a presigned URL's query string or a bucket layout ended up in shared logs. `location_for_log` now drops the query string and fragment and logs `<scheme>://<bucket>/<path sha256:12 hex>`. A location without a scheme is fully hashed. The hash covers the path without its query, so the same table location correlates across log lines. The result batch still echoes the full location to the caller, as the issue allows.

Tests: four unit tests in `maintenance::location_log_tests` (bucket kept and path hidden, presigned query dropped, stable and distinct hashes, a location without a scheme).

**NEXT:** unchanged
