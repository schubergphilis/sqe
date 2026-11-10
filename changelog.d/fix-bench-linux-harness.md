### Fixed

- Benchmark harness runs SF100 on a small Linux host, and Trino starts there (#784, #787)

    `benchmark-test.sh` wrote the Trino catalog file as mode 600, which the Trino container
    (uid 1000) cannot read on Linux; Trino exited at startup and the comparison was skipped.
    The file is now 640 and the container joins the caller's group. A container that exits
    during startup ends the wait at once, and `benchmark-make-run.sh` fails a compare run when
    Trino never came up. When unset, the script now raises the open-file limit, uses a 600s
    query timeout at SF100 and above, caps `SQE_MEMORY_LIMIT` at 75% of physical RAM, and
    spills under `$TMPDIR` when `/tmp` is tmpfs. TPC-H SF100 on an 8-core 31 GB box had hit
    EMFILE on q07/q09 and the 60s timeout on q18/q21.

### Added

- `scripts/bench-profile.py` prints the hot operators per benchmark query from coordinator logs

    It reads the `query_profile` records, labels them with bench query ids from the report
    JSON, and lists per query the operators with the most CPU time, scan bytes, pruning and
    spill, plus operator totals and the queries that left no profile.
