**STATUS 2026-11-14: the REST table-name concurrency test no longer races the process-wide listing epoch (`test/listing-snapshot-race`).**

`eight_concurrent_table_names_refreshes_complete_on_two_workers` ended with
`assert!(!table_exist("missing"))`. Since #890 the invalidation epoch is
process-wide, so an invalidation from any other test in the `sqe-catalog` `it`
binary, landing during a refresh, marks the snapshot invalidated and
`table_exist` answers "unknown" (true) by design. On the Hive data-prefix
branch the full suite failed it 2 of 2 runs; it passed alone 3 of 3. The test
now retries the absence probe only while the snapshot it answered from is
invalidated or stale (at most 20 refreshes), and fails if a clean snapshot
still reports the name. Test-only; no engine change.

Gate: `cargo test -p sqe-catalog --test it` -> 249 passed, 3 of 3 runs on
main and 3 of 3 on the Hive data-prefix branch (where it had failed).

**NEXT:** unchanged
