### Fixed

- The Ranger quickstart's `data-seed` finishes again, and every wait in the
  access-control harnesses is bounded

    Since results moved to `nextUri` pages (#767), a Trino `POST` never
    reports `FINISHED`, so the seed's readiness probe (one `POST`, grep for
    `FINISHED`) could never pass and looped forever. Each attempt also left
    a statement holding 256 MB of the user's 1 GB budget until its 5-minute
    idle eviction, so the loop kept the seed user at the limit. The probe
    now follows `nextUri` to the end, every request has `--max-time`, and
    readiness gives up after 60 attempts with the last response.
    `make test-access-control-spark` started `spark` with its `data-seed`
    dependency despite saying it does not, and `--wait` had no timeout, so
    it hung for hours; it now starts `spark` with `--no-deps`, and every
    `docker compose up --wait` in the access-control scripts has
    `--wait-timeout`. The integration tests' `docker`, `spark-sql` and
    `trino-cli` calls run through `output_with_timeout` and fail on expiry
    instead of hanging the suite.
