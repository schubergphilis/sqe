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

- Snowflake-written tables with a VARIANT or nested column read again

    Snowflake writes parquet files with no offset index on VARIANT and nested
    columns. SQE loaded the page index as required, so every read of such a
    table failed with "missing offset index", even `SELECT id`. The page index
    is now optional in the vendored Iceberg reader, the small-file scan path
    and the worker: an incomplete index is dropped and the file reads without
    page pruning.

- Writes to a `[catalogs.<name>]` catalog go to that catalog

    `INSERT`, `CREATE TABLE` and the other writes resolved a catalog-qualified
    target through the legacy `[catalog]` entry and its warehouse, so a write
    to `sf.schema.t` reached the wrong endpoint. They now use the named
    catalog's own endpoint, warehouse and auth, as reads already did.

- Creating a table in a catalog that places tables itself

    Snowflake Horizon refuses a `CREATE TABLE` that names a location (HTTP
    400). SQE sent one on every create. On that refusal it now sends the
    create again without the location and lets the catalog choose it.

- Commits to Snowflake Horizon no longer fail when they come close together

    Horizon refuses a commit that follows the previous one within about half
    a second: a 409 "concurrently modified" with no failed requirement, or a
    500 "Indeterminate result during conflict resolution check" that it never
    applied. Schema commits (ALTER TABLE) now send the same request again,
    up to four times, 500 ms apart. Data commits back off at least 500 ms on
    such a 409.

    A commit whose outcome is unknown is retried only when the table provably
    did not move: SQE waits a second, reloads the table and compares its
    metadata location with the one the commit was based on. A moved table,
    or one without a metadata location, returns the original error. Every
    write and maintenance path now keeps its written files when the outcome
    is unknown, because a commit that landed references them.

- Distributed scans of files whose column order differs from the table schema

    A worker emits a file's columns in the order the file stores them.
    Snowflake writes `ts, id, region` into a table declared `id, region, ts`,
    and a worker without a projection fill bound those columns by position:
    a distributed `SELECT id, region, ts` read `id` as NULL and `region` as
    the id. The coordinator now sends the fill on every projected scan, so
    the worker matches columns by field id. Workers older than scan task
    version 4 refuse such tasks.
