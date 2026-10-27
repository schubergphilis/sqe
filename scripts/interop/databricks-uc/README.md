# SQE <-> Databricks Unity Catalog interop run

A live run against a Databricks workspace, through Unity Catalog's Iceberg
REST endpoint. Each engine writes tables the other reads, both run DML on
the other's tables, and every read is run on both engines and compared
value by value. "No error" is not the gate: the bugs this run has found
were wrong values and wrong snapshot operations.

Nothing here runs in CI. It needs a workspace, a SQL warehouse and a user
who may create tables in one scratch schema.

## Phases

| Phase | Engine | File |
|---|---|---|
| A | Databricks | `dbx-setup.sql`: create the `dbx_run_*` tables, then DELETE / UPDATE (deletion vectors on V3) |
| B | SQE | `sqe-writes.sql`: create the `sqe_*` tables with CREATE, CTAS, INSERT, UPDATE, DELETE, MERGE, ALTER |
| C | Databricks | `dbx-dml.sql`: DELETE / UPDATE / INSERT on SQE's tables |
| D | SQE | `sqe-dml.sql`: DELETE / UPDATE / MERGE on Databricks' tables |
| E | both | `compare.tsv`: every read on both engines, values compared |
| F | SQE | `sqe-probes.sql`: metadata tables, DESCRIBE, SHOW CREATE TABLE, information_schema, the refusals |

Phase E runs once per SQE mode: `embedded` (sqe-cli with ATTACH),
`coordinator` (Flight SQL, single process) and `distributed` (coordinator
plus one worker, `distribution_threshold = "0"`, so every scan runs on the
worker with vended credentials).

## Running

```bash
export DBX_PROFILE=<databricks CLI profile>   # U2M login, `databricks auth login`
export DBX_HOST=https://<workspace host>
export DBX_WAREHOUSE_ID=<SQL warehouse id>
export DBX_CATALOG=<UC catalog>               # e.g. a bronze catalog you may write to
export DBX_SCHEMA=sqe_probe                   # scratch schema; the run drops and creates tables in it
export SQE_CLI=target/debug/sqe-cli            # default
export SQE_PORT=50061                         # default, the coordinator's Flight SQL port

R=scripts/interop/databricks-uc/run.py
python3 $R setup              # phases A-D; B and D need a running coordinator
python3 $R compare embedded   # phase E through sqe-cli with ATTACH, no server
python3 $R compare flight     # phase E through the coordinator on $SQE_PORT
python3 $R probes flight      # phase F
```

`compare flight` tests whichever coordinator is listening: run it once
against a coordinator-local server and once against the distributed one.
`sqe.toml.example` holds both configs: a `bearer_token` provider that
validates the Databricks user's JWT against the workspace JWKS, and
`[catalogs.dbx]` pointing at the Unity Catalog Iceberg REST URL. The
user's token is passed through to Unity Catalog. There is no service
account. The token lives about an hour, so a long run may need a fresh
`databricks auth login`.

The write phases run through the coordinator because the embedded CLI only
reads an attached catalog yet: it refuses UPDATE, DELETE, MERGE and ALTER,
and CREATE TABLE only takes plain columns.

Output goes to `$OUT` (default `/tmp/sqe-dbx-interop`): `phase-X.txt`,
`compare-<mode>.txt`, `probes-<mode>.txt`. `compare` exits non-zero on any
mismatch and prints both rows. The token is redacted from every file.

Databricks needs two table properties for liquid clustering on a managed
Iceberg table: `CLUSTER BY` fails unless deletion vectors and row tracking
are off, which is why `dbx_run_part` sets both to `false`.
