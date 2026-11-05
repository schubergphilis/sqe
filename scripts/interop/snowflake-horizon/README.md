# SQE <-> Snowflake Horizon interop run

A live run against a Snowflake account, through Horizon's Iceberg REST
endpoint (`/polaris/api/catalog`). Each engine writes tables the other reads,
both run DML on the other's tables, and every read runs on both engines and
is compared value by value. "No error" is not the gate: a read that returns
the wrong column under the right name passes every smoke test.

Nothing here runs in CI. It needs a Snowflake account, a user with a key
pair and a role that may create Iceberg tables in one scratch database.

## Phases

| Phase | Engine | File |
|---|---|---|
| A | Snowflake | `sf-setup.sql`: create the `SF_RUN_*` tables (V2 and V3, nested, partitioned, VARIANT, GEOMETRY, GEOGRAPHY, nanosecond timestamps, schema evolution), then DELETE / UPDATE |
| B | SQE | `sqe-writes.sql`: create the `sqe_*` tables with CREATE, CTAS, INSERT, UPDATE, DELETE, MERGE, ALTER |
| C | Snowflake | `sf-dml.sql`: DELETE / UPDATE / INSERT on SQE's tables |
| D | SQE | `sqe-dml.sql`: DELETE / UPDATE / MERGE on Snowflake's tables |
| E | both | `compare.tsv`: every read on both engines, values compared |
| F | SQE | `sqe-probes.sql`: metadata tables, DESCRIBE, SHOW CREATE TABLE, the case-folding refusal |

Phase E runs once per SQE mode: `embedded` (sqe-cli with ATTACH),
coordinator-local (Flight SQL, one process) and distributed (coordinator
plus one worker, `distribution_threshold = "0"`, so every scan runs on the
worker with vended credentials).

## Snowflake side, once

The database carries the defaults Horizon needs to create a table:

```sql
CREATE DATABASE SQE_INTEROP EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED' CATALOG = 'SNOWFLAKE';
CREATE SCHEMA SQE_INTEROP.SQE_PROBE;
```

Use Snowflake-managed storage. An external volume encrypted with a customer
KMS key fails: the down-scoped credentials Horizon vends lack `kms:Decrypt`,
so even `loadTable` answers 403.

## Running

```bash
export SF_ACCOUNT=<orgname>-<accountname>
export SF_USER=<user with a registered public key>
export SF_ROLE=<role that owns SQE_INTEROP>
export SF_PRIVATE_KEY_FILE=<path to the PKCS#8 private key>
export SF_DATABASE=SQE_INTEROP                # default
export SF_SCHEMA=SQE_PROBE                    # default; the run replaces tables in it
export SQE_CLI=target/debug/sqe-cli            # default
export SQE_PORT=50061                         # default, the coordinator's Flight SQL port

R=scripts/interop/snowflake-horizon/run.py
python3 $R setup              # phases A-D; B and D need a running coordinator
python3 $R compare embedded   # phase E through sqe-cli with ATTACH, no server
python3 $R compare flight     # phase E through the coordinator on $SQE_PORT
python3 $R probes flight      # phase F
python3 $R token              # print a Horizon access token
```

`compare flight` tests whichever coordinator is listening: run it once
against a coordinator-local server and once against the distributed one.
`sqe.toml.example` holds both configs.

Output goes to `$OUT` (default `/tmp/sqe-sf-interop`). `compare` exits
non-zero on any mismatch and prints both rows. The token, account, user,
role, key path, bucket names and AWS account ids are redacted from every
output file.

## Auth

`run.py token` signs a JWT with the user's key (`snow connection
generate-jwt`), trades it at Horizon's OAuth endpoint (`client_credentials`,
`scope=session:role:<ROLE>`) and passes the access token to SQE. There is no
service account. The coordinator uses the `bearer_passthrough` provider:
Horizon's token is opaque, there is no public JWKS to validate it against,
so SQE forwards it and Horizon authenticates it. Use that provider on a test
rig only. The token lives about an hour; `run.py` reuses it for 40 minutes
and then mints a new one. A coordinator started with `run.py token` output
needs the same refresh on a long run.

The legacy `[catalog]` block must exist even when every catalog is a
`[catalogs.<name>]` entry. A placeholder URL is enough.

A distributed worker's config is a full copy of the coordinator's (it still
reads `[coordinator]`, `[auth]` and the catalogs) plus a `[worker]` block
and its own `[metrics] prometheus_port`. Start the worker first.

## Snowflake behaviour the harness works around

- **Names.** Snowflake upper-cases unquoted names and SQE folds them to
  lower case, so SQE quotes `"SF_RUN_*"` and Snowflake quotes `"sqe_*"`.
  The last probe reads `sf_run_basic` unquoted on purpose and must fail.
- **Geometry.** An Iceberg GEOMETRY column is pinned to SRID 4326.
  `ST_AREA` on it comes back negative in Snowflake, whichever engine wrote
  the table (-12 for the ring that gives 12 as a literal), so the Snowflake
  side of the compare takes `ABS`.
- **Arrays.** Snowflake indexes from 0, SQE from 1.
- **Timestamps** compare as epoch microseconds (nanoseconds for
  `SF_RUN_NS`).
- **VALUES.** Snowflake refuses `PARSE_JSON` and structured casts inside
  `VALUES`, so phase A inserts with `SELECT ... UNION ALL`.
- **Merge-on-read DML.** Snowflake rewrites small tables copy-on-write
  whatever the table setting, so `SF_RUN_V2` / `SF_RUN_V3` never get a
  delete file. `SF_RUN_MOR_V2` / `SF_RUN_MOR_V3` hold 1M rows with
  `ICEBERG_MERGE_ON_READ_BEHAVIOR = 'enabled'`: Snowflake's DELETE writes a
  position delete file (V2) or a deletion vector (V3), and the `mor_*`
  compare rows check that SQE applies them. The phase F `$snapshots`
  probes must show `total-delete-files` above 0. Reading them needs parquet
  59.3.2 or later (Snowflake writes 512-value `DELTA_BINARY_PACKED`
  miniblocks; 59.3.1 fails with "cannot skip miniblock of size 512").
  The other direction: `sqe_v3_mor` is merge-on-read, SQE's DELETE on it
  writes a deletion vector (the `$snapshots` probe must show `added-dvs`
  1), and phase E checks that Snowflake applies it.
- **Checksums.** The redactor masks any 12-digit number as an AWS account
  id, so the `mor_*_agg` rows sum `ID % 997` instead of `ID`.

## Snowflake behaviour SQE handles

- **Table placement.** Horizon refuses a `CREATE TABLE` that names a
  location (HTTP 400). SQE resends the create without one.
- **Commit pacing.** A commit within about half a second of the previous one
  gets a 409 "concurrently modified" with no failed requirement, or a 500
  "Indeterminate result during conflict resolution check". Neither applied.
  SQE resends schema commits unchanged and backs off data commits at least
  500 ms. An unknown outcome is retried only after a reload shows the table
  did not move.
- **File layout.** Snowflake writes parquet columns in its own order and adds
  row-lineage columns (`METADATA$RL_ROW_ID`, `METADATA$RL_LAST_UPDATED_SEQUENCE_NUMBER`).
  SQE matches columns by field id in every mode, the distributed worker
  included.
- **Page index.** Snowflake writes no offset index for VARIANT and nested
  columns. SQE reads such files without page pruning.
- **Throttling.** Horizon answers a burst of `loadTable` calls with 429. The
  REST client replays them and ATTACH loads 16 tables at a time.
