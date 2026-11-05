### Fixed

- TPC-DS q35, q73 and q75 in the Trino compare sort deterministically

    Their ORDER BY left rows tied on every key, and the LIMIT cut through the ties, so SQE and Trino each kept different but equally valid rows. q35 and q75 now sort on the keys the TPC-DS spec lists; q73 adds `ss_ticket_number` after the spec's keys, because one customer can hold two tickets with the same count.

### Added

- `SQE_BENCH_DUMP_DIFFS=<dir>` writes the SQL and both engines' result sets for every query that differs in the Trino compare
