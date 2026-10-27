### Fixed

- Distributed scans no longer fail on files written with older column types

    Since workers bind pushed predicates by field id, a predicate typed by
    the table schema met the file's own types: an `int` column since
    promoted to `bigint`, or a timestamptz the file labels `UTC` where the
    plan says `+00:00`. The comparison failed at evaluation and the whole
    scan errored. Workers now coerce the predicate against the file's
    types before binding it.

- No "Invalid scan task signature" errors right after a coordinator restart

    A restarted coordinator signs scan tickets with a new key, and the
    worker learns that key from its next heartbeat. The health check
    marked the worker ready before then. Workers now name the ticket key
    they hold in the health check reply, and the coordinator waits until
    it is its own key before it sends scans there. A worker that stays on
    another coordinator's key for more than a minute is logged as a warning.

- `make test-distributed` without a filter no longer aborts before the distributed tests

    The debug-only stack-overflow repros are now skipped in the distributed
    run as well.
