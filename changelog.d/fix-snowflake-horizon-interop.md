### Fixed

- Snowflake Horizon catalogs attach and read in embedded mode

    `ATTACH ... (TYPE iceberg_rest, ACCESS_DELEGATION 'vended-credentials')`
    sends `X-Iceberg-Access-Delegation`. Horizon and Polaris vend storage
    credentials only when asked, so without it every data read failed with
    "failed to load signing credential". The option is off by default, as in
    Trino and Spark. The coordinator already had `[catalog] access_delegation`.

    The REST catalog client now replays a request the catalog answers with 429
    Too Many Requests, up to six times, honouring `Retry-After` (capped at
    10 s). ATTACH loads at most 16 tables of a namespace at a time. Horizon
    throttled the old unbounded fan-out, and every throttled table dropped out
    of the attached catalog with only a log warning.
