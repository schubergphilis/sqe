### Fixed

- engine-bench's summary names the error of a failed run instead of its last stack frame

    A failed Spark run printed `... 1 more` in OUTPUT.md. The summary now
    picks the line that names the error and skips the Py4J wrapper, for
    instance `SparkOutOfMemoryError: [UNABLE_TO_ACQUIRE_MEMORY]`.

- `make leak-scan` no longer fails on gitignored `.work/` directories under `quickstart/`

    After a local `make bench`, test files in the bench's Python venv tripped
    the account-id check, although they are never committed.
