### Fixed

- `sqe-server` raises its open-file soft limit at startup (#788)

    A full-table `ORDER BY` at SF100 kept more than 800 spill files open and
    failed with "Too many open files" under the common 1024 soft limit, while
    the hard limit allowed far more. The server now raises the soft limit to
    the hard limit (10240 on macOS when the hard limit is unlimited), logs the
    change, and warns when the result stays below 8192 so operators know to
    raise `LimitNOFILE` or `ulimit -Hn`. Containers that already run at the
    hard limit see no change.
