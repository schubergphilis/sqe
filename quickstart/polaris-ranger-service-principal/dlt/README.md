# dltHub ingestion through Polaris and SQE

This is an end-to-end test and executable example for loading Iceberg data with
[dltHub's `dlt` Python library](https://dlthub.com/docs/intro). It proves two
independent write paths against the same Polaris catalog and RustFS warehouse:

For the design story and the lessons found beyond the 1,000-row batch boundary,
read [One dlt pipeline, three roads to Apache Polaris](../../../docs/site/blog/2026-07-13-dlt-three-roads-to-polaris.md).

```text
                         Keycloak service principal
                                    |
                    +---------------+---------------+
                    |                               |
                    v                               v
       dlt native Iceberg destination       dlt custom SQE destination
                    |                               |
                    v                    +----------+----------+
          Polaris Iceberg REST           |                     |
                    |                 Trino HTTP          Arrow Flight SQL
                    |                     |                     |
                    |                     +----------+----------+
                    |                               |
                    |                              SQE
                    |                               |
                    +---------------+---------------+
                                    |
                         Polaris + Apache Ranger
                                    |
                                  RustFS
```

The two paths intentionally converge at Polaris. Apache Ranger therefore
authorizes the same `sp-admin` identity regardless of whether dlt commits an
Iceberg transaction directly or sends SQL DML through SQE.

## Choose the write path

Both routes produce ordinary Apache Iceberg tables in the same catalog. The
difference is where write semantics are executed.

| Consideration | Direct Polaris REST | SQE through Trino or Flight SQL |
|---|---|---|
| dlt integration | Native Iceberg destination | Small custom dlt destination |
| Data path | PyIceberg writes files and commits metadata | SQE executes SQL and commits Iceberg changes |
| Best fit | Python-native ingestion and portable Iceberg commits | SQL-governed ingestion shared with existing clients |
| Full replace | Native `replace` disposition | `DELETE` once, then batched `INSERT` |
| Incremental upsert | Native Iceberg `merge`/`upsert` | Iceberg `MERGE INTO` |
| SCD2 | Materialize history, then replace the snapshot | Close and open versions with `MERGE INTO` |
| Client protocol | Iceberg REST | Trino-compatible HTTP or Arrow Flight SQL |
| Query audit | Polaris/Ranger catalog activity | SQE canonical and OCSF query audit plus Polaris/Ranger |

Choose direct REST when the pipeline should speak the open Iceberg catalog
protocol and does not need a SQL engine in the write path. Choose SQE when the
load should use the same SQL surface, identity propagation, audit events, and
row-level DML as other Trino or Flight clients. A deployment can use both: the
tests deliberately write through one route and observe through the other.

## What is tested

Each path runs the same six loading patterns:

| Pattern | Initial load | Second load | Expected state |
|---|---|---|---|
| Replace/overwrite | IDs 1 and 2 | only ID 1 | The old contents are gone |
| Delta/upsert | IDs 1 and 2 | update 1, add 3 | IDs 1, 2 and 3; ID 1 is updated |
| SCD Type 2 | IDs 1 and 2 | change 1, add 3 | ID 1 has retired and active versions |
| Ordered SCD Type 2 | ID 1 is bronze | one run contains silver, then gold | bronze and silver are closed in order; only gold is active |
| Batched full replace | 2,005 rows | replace with 1,501 different rows | all replacement batches survive; no initial rows survive |
| Batched delta/upsert | 2,505 rows | update 1,505 and add 1,000 | 3,505 rows, with no loss across 1,000-row batch boundaries |

The two batched cases also include apostrophes, backslashes, Unicode, emoji,
and a nullable value. Their deterministic key ranges make row loss, duplicate
rows, incorrect escaping, and a destination that accidentally repeats
`DELETE` for every batch immediately visible.

The quickstart sets SQE's `query.sort_mode` to `strict`. SQE's default adaptive
mode may remove a non-partition `ORDER BY` under memory pressure; that is useful
for availability-oriented analytics but is not Trino-compatible ordering
semantics. The large multi-file cases deliberately verify boundary rows in
order so this configuration cannot regress silently.

The native route uses dlt's Iceberg REST destination for `replace` and `upsert`.
That destination currently advertises `delete-insert` and `upsert`, but not
dlt's built-in `scd2` strategy. The native SCD2 case therefore materializes the
history rows in Python and loads the resulting snapshot through dlt's supported
`replace` operation against Polaris REST. The SQE route uses a small dlt custom
destination in [`test_dlt_load_paths.py`](./test_dlt_load_paths.py). That adapter
maps normalized dlt batches to SQE's Iceberg `DELETE`, `INSERT`, `MERGE`, and
`UPDATE` support.

Why is the SQE adapter custom? dlt's generic SQLAlchemy Trino destination does
not advertise merge or SCD2 because standard Trino tables have no primary-key
constraints. SQE has the necessary Iceberg DML semantics, so this test makes
that capability explicit instead of weakening the coverage to append-only.

The adapter is an example, not a claim that standard Trino provides primary
keys. dlt supplies the business-key metadata, while the adapter turns that
contract into explicit Iceberg DML. SQE supports the required `MERGE INTO`,
`DELETE`, `INSERT`, and `UPDATE` statements through both SQL protocols.

## Loading models and runnable examples

The executable source for every example below is
[`test_dlt_load_paths.py`](./test_dlt_load_paths.py). The examples use this
small customer dataset:

```python
INITIAL = [
    {"id": 1, "name": "Ada", "tier": "bronze"},
    {"id": 2, "name": "Grace", "tier": "silver"},
]

CHANGED = [
    {"id": 1, "name": "Ada", "tier": "gold"},
    {"id": 3, "name": "Linus", "tier": "bronze"},
]
```

The same calls work through the native Polaris REST path and the SQE path. For
SQE, `SQE_TRANSPORT=trino` sends the SQL through the Trino-compatible HTTP
endpoint and `SQE_TRANSPORT=flight` sends exactly the same SQL through Arrow
Flight SQL.

### Full load: replace the complete table

Use a full load when the incoming rows are the complete desired table state.
Anything in the target but absent from the new input must disappear.

The native Iceberg destination expresses that directly with dlt's `replace`
write disposition:

```python
pipeline = native_pipeline("customers")

# Seed the newly created table. The next call proves replacement semantics.
native_run(pipeline, "customers", INITIAL, "append")
native_run(pipeline, "customers", CHANGED[:1], "replace")
```

The underlying resource configuration is:

```python
dlt.resource(
    rows,
    name="customers",
    primary_key="id",
    write_disposition="replace",
    table_format="iceberg",
)
```

The SQE custom destination implements the same contract as an Iceberg
`DELETE` followed by `INSERT` in the dlt load:

```python
if mode == "replace":
    if not self._replace_initialized:
        sql(f"DELETE FROM {table}")
        self._replace_initialized = True
    insert(rows)
```

The guard is per dlt run. dlt invokes a custom destination once for every
normalized batch, so an unconditional `DELETE` inside the callback would erase
previous batches from the same full load.

After the second full load, ID 2 is gone because it was not present in the
replacement input:

| id | name | tier |
|---:|---|---|
| 1 | Ada | gold |

### Incremental load: update and insert only the supplied keys

Use an incremental/upsert load when each run contains only changed or new
records. Rows not mentioned by the input remain untouched. In this example,
the second load updates ID 1 and inserts ID 3; ID 2 remains in the table.

For native Polaris REST, dlt maps the merge/upsert disposition to the Iceberg
destination:

```python
mode = {"disposition": "merge", "strategy": "upsert"}

native_run(pipeline, "customers", INITIAL, "append")
native_run(pipeline, "customers", CHANGED, mode)
```

For SQE, the custom destination receives dlt's normalized batch and executes a
keyed `MERGE INTO`:

```sql
MERGE INTO customers AS target
USING (VALUES
  (1, 'Ada', 'gold'),
  (3, 'Linus', 'bronze')
) AS source(id, name, tier)
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET name = source.name, tier = source.tier
WHEN NOT MATCHED THEN
  INSERT (id, name, tier)
  VALUES (source.id, source.name, source.tier)
```

The resulting table is:

| id | name | tier | Why |
|---:|---|---|---|
| 1 | Ada | gold | Updated by the delta |
| 2 | Grace | silver | Preserved because the delta did not mention it |
| 3 | Linus | bronze | Inserted by the delta |

Here, *incremental* describes how the destination applies the batch. The test
does not configure a dlt extraction cursor such as `dlt.sources.incremental`;
in a production pipeline, cursor-based extraction can feed the same upsert
destination behavior.

### SCD Type 2: retain history from current-state snapshots

SCD2 stores a new version instead of updating an existing row in place. Each
business key has at most one active row:

- `_dlt_valid_from` is inclusive;
- `_dlt_valid_to` is exclusive;
- `_dlt_valid_to IS NULL` identifies the active version;
- `(id, _dlt_valid_from)` uniquely identifies a historical version.

The basic SCD2 test treats every input as a complete snapshot of the current
source state:

```python
native_scd2_run(pipeline, "customer_history", INITIAL)
native_scd2_run(pipeline, "customer_history", CHANGED)
```

On the second run:

- Ada changed from bronze to gold, so bronze is closed and gold is opened;
- Grace is absent from the complete source snapshot, so her active row is
  closed without a successor;
- Linus is new, so a new active row is opened.

Conceptually, SQE performs the change in two phases. First it closes active
versions that changed or disappeared:

```sql
MERGE INTO customer_history AS target
USING source_to_expire AS source
ON target.id = source.id AND target._dlt_valid_to IS NULL
WHEN MATCHED THEN UPDATE SET _dlt_valid_to = source.boundary
```

It then opens successors and new keys:

```sql
MERGE INTO customer_history AS target
USING source_to_insert AS source
ON target.id = source.id AND target._dlt_valid_to IS NULL
WHEN NOT MATCHED THEN INSERT
  (id, name, tier, _dlt_valid_from, _dlt_valid_to)
VALUES
  (source.id, source.name, source.tier, source.boundary, NULL)
```

The exact timestamps are generated at the load boundary, but the logical
result is:

| id | name | tier | valid from | valid to | State |
|---:|---|---|---|---|---|
| 1 | Ada | bronze | first load | second load | Historical |
| 1 | Ada | gold | second load | `NULL` | Active |
| 2 | Grace | silver | first load | second load | Historical; removed from source |
| 3 | Linus | bronze | second load | `NULL` | Active |

dlt's native Iceberg destination does not currently advertise its built-in
`scd2` merge strategy. The native example therefore reads the existing
history, materializes the new version set in Python, and commits that complete
history with the supported `replace` operation. SQE performs the close/open
operations through Iceberg `MERGE`. Both paths assert the same final history.

### Ordered SCD2: multiple changes for one key in one run

Event-based SCD2 differs from snapshot-based SCD2: each input row is a change
event with an effective timestamp. Missing keys mean “no event,” not deletion.
Events must be processed in `(business key, effective_at)` order.

The regression case starts with bronze and delivers both silver and gold in a
single subsequent dlt run:

```python
initial = [
    {"id": 1, "name": "Ada", "tier": "bronze",
     "effective_at": datetime(2026, 1, 1)},
]
changes_in_one_run = [
    {"id": 1, "name": "Ada", "tier": "silver",
     "effective_at": datetime(2026, 1, 2)},
    {"id": 1, "name": "Ada", "tier": "gold",
     "effective_at": datetime(2026, 1, 3)},
]

sqe_run("customer_history", initial, "scd2_events")
sqe_run("customer_history", changes_in_one_run, "scd2_events")
```

The loader sorts the batch and applies each event sequentially:

```python
for event in sorted(events, key=lambda row: row["effective_at"]):
    close_active_version(event["id"], event["effective_at"])
    open_new_version(event)
```

This ordering is essential. A single set-based merge that matches both source
rows to the same active target row can produce an ambiguous merge or skip the
intermediate silver version on engines with multi-match limitations. The SQE
adapter closes and opens each version before processing the next event. The
native path computes the same ordered history in memory and commits one
replacement snapshot.

The deterministic result is:

| id | tier | `_dlt_valid_from` | `_dlt_valid_to` |
|---:|---|---|---|
| 1 | bronze | `2026-01-01 00:00:00` | `2026-01-02 00:00:00` |
| 1 | silver | `2026-01-02 00:00:00` | `2026-01-03 00:00:00` |
| 1 | gold | `2026-01-03 00:00:00` | `NULL` |

The test additionally asserts that exactly one row for ID 1 has
`_dlt_valid_to IS NULL`.

### Verified suite and audit result

One transport run executes six loading patterns over both ingestion paths,
for twelve parameterized cases. Running both SQE transports executes twenty-four
cases in total:

```bash
./dlt/run.sh --both -q
```

Verified on July 13, 2026:

```text
Trino-compatible HTTP: 12 passed
Arrow Flight SQL:       12 passed
Total:                  24 passed
```

The clean run produced 624 canonical SQE audit records and 624 OCSF records.
All records were valid JSON and successful, the sequence and previous-hash
links formed one intact chain, every actor was `sp-admin`, and no policy event
was denied. The DML subset contained 32 merges, 14 inserts, and 8 deletes; all
resources resolved to the intended `sales_wh` catalog. SQE logged no error,
panic, failed query, or missing-table probe.

The resilience cases found three bugs while they were being built:

1. A custom replace destination must delete once per dlt run, not once per
   1,000-row callback. Repeating the delete silently keeps only the last batch.
2. dlt normalization may omit a dictionary key whose value is `None`. Optional
   columns must use `row.get(...)` and emit SQL `NULL` instead of raising a
   retrying `KeyError`.
3. SQE's adaptive sort mode may remove a non-partition `ORDER BY` under memory
   pressure. This quickstart uses `sort_mode = "strict"` because Trino-compatible
   ordering is a correctness contract, not a performance hint.

These are permanent regression tests rather than notes about one successful
run. The large cases cross dlt's configured batch boundary and assert complete
counts, key ranges, escaped values, nullable values, and deterministic order.

### Inspect the before/after state and generated SQL

The test runner uses pytest's `-s` option, so each dlt run prints:

1. the target table before the load;
2. the input batch and write mode;
3. every SQL statement emitted by the dlt custom SQE destination;
4. the target table after the load.

For large tables and inputs, the trace prints the total row count plus the
first and last ten rows. Assertions still examine the complete table and
targeted boundary keys; limiting display volume does not weaken verification.
Generated SQL longer than 4,000 characters is displayed with its total length,
first 2,000 characters, and last 2,000 characters. The complete statement is
still sent to SQE; only console rendering is shortened.

For example, the second incremental SQE run includes output shaped like:

```text
=== BEFORE: SQE/trino target sales_wh.sales."dlt_sqe_merge" ===
id | name  | tier
---+-------+-------
1  | Ada   | bronze
2  | Grace | silver

=== DLT INPUT: SQE/trino target sales_wh.sales."dlt_sqe_merge"; mode='merge' ===
{'id': 1, 'name': 'Ada', 'tier': 'gold'}
{'id': 3, 'name': 'Linus', 'tier': 'bronze'}

--- SQL emitted by dlt custom SQE destination (trino, 308 characters) ---
MERGE INTO sales_wh.sales."dlt_sqe_merge" AS target
USING (VALUES (1,'Ada','gold'),(3,'Linus','bronze'))
  AS source(id,name,tier)
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name, tier = source.tier
WHEN NOT MATCHED THEN INSERT (id,name,tier)
VALUES (source.id,source.name,source.tier)

=== AFTER: SQE/trino target sales_wh.sales."dlt_sqe_merge" ===
id | name  | tier
---+-------+-------
1  | Ada   | gold
2  | Grace | silver
3  | Linus | bronze
```

The native destination deliberately prints `SQL generated by dlt: <none>`.
That route does not translate the load into SQL: dlt/PyIceberg writes Parquet
files and commits Iceberg metadata through Polaris's REST API. The before and
after snapshots are still queried through SQE so both paths are verified with
the same observer. Each inspection query carries a unique inert SQL comment:
native REST commits bypass SQE and therefore cannot invalidate SQE's SQL-text
query cache, so cache-unique inspection queries are required to display fresh
Iceberg metadata immediately after a load.

To focus on one trace:

```bash
./dlt/run.sh --trino -k 'delta and sqe' -vv
./dlt/run.sh --flight -k 'orders_multiple and sqe' -vv
./dlt/run.sh --trino -k 'replace and native' -vv
```

SQE also writes canonical and OCSF audit records inside its disposable
quickstart container. Inspect them after a run with:

```bash
docker compose exec sqe sh -c 'tail -n 20 /tmp/sqe-audit.jsonl'
docker compose exec sqe sh -c 'tail -n 20 /tmp/sqe-audit.ocsf.jsonl'
```

Audit records contain identity, statement type, outcome, policy decision,
tables touched, timing, and row counts. Full result-set audit logging remains
disabled because it may expose sensitive table data. Missing targets are
checked with an authoritative Polaris REST `HEAD` before the first load, so a
healthy run does not pollute the SQE audit log with expected `TABLE_NOT_FOUND`
failures or depend on a stale SQE session-catalog listing.

## Prerequisites

- Docker with Compose v2
- The parent `polaris-ranger-service-principal` quickstart
- Enough memory for Polaris, Ranger, Keycloak, RustFS, and SQE
- Access to pull/build the images and Python dependencies on the first run

No Python installation is required on the host. The additive Compose overlay
builds a disposable Python 3.12 test container from [`Dockerfile`](./Dockerfile).

## Start the platform

From the repository root:

```bash
cd quickstart/polaris-ranger-service-principal
cp .env.example .env       # optional; the development defaults already work
./run.sh
```

Ranger's first initialization can take several minutes. The dlt runner waits on
SQE's health check, but the parent stack must already have been created.

## Choose the SQE protocol

The direct Polaris tests are identical in every run. The option below selects
the protocol used by the SQE custom destination *and* by the assertions that
read the resulting tables.

### Trino-compatible HTTP

This is the default:

```bash
./dlt/run.sh
# equivalent:
./dlt/run.sh --trino
```

The runner reuses the existing `dlt-tests` image. Rebuild it after changing
the Dockerfile, requirements, or test source:

```bash
./dlt/run.sh --build
./dlt/run.sh --build --both
```

The Python `trino` client connects to `http://sqe:8080` with HTTP Basic
credentials. SQE exchanges those client credentials with Keycloak and forwards
the resulting bearer identity to Polaris.

### Apache Arrow Flight SQL

```bash
./dlt/run.sh --flight
```

This uses Apache Arrow ADBC's Flight SQL driver and connects to
`grpc://sqe:50051`. The ADBC driver performs the Flight SQL username/password
handshake; SQE again runs the service-principal client-credentials flow.

Flight SQL here is used as the SQL command and result transport. The dlt custom
destination still owns the loading semantics. It sends DDL/DML through Flight
SQL and receives query results as Arrow before ADBC exposes DB-API rows to the
test assertions.

### Verify both SQE protocols

```bash
./dlt/run.sh --both
```

This executes the complete suite twice, once over Trino HTTP and once over
Flight SQL. Tables are removed after every test, so the runs do not share data.

## Run individual tests

Arguments after the transport option are forwarded to pytest:

```bash
./dlt/run.sh --flight -k scd2 -vv
./dlt/run.sh --trino -k 'replace or delta' -vv
```

`--build`, `--trino`, `--flight`, and `--both` are runner options and may be
combined in any order. Use `--` if a future pytest option has the same name as
a runner option.

The logical cases are parameterized by ingestion path (`native` and `sqe`).
With `--both`, every case executes over both verification/SQL transports.

## Configuration

The defaults are defined in [`docker-compose.dlt.yml`](../docker-compose.dlt.yml):

| Variable | Default | Purpose |
|---|---|---|
| `SQE_TRANSPORT` | `trino` | Select `trino` or `flight` |
| `SQE_TRINO_URL` | `http://sqe:8080` | Trino-compatible endpoint |
| `SQE_FLIGHT_URI` | `grpc://sqe:50051` | Arrow Flight SQL endpoint |
| `POLARIS_REST_URI` | `http://polaris:8181/api/catalog` | Iceberg REST endpoint |
| `POLARIS_WAREHOUSE` | `sales_wh` | Polaris catalog/SQE catalog |
| `S3_ENDPOINT` | `http://rustfs:9000` | Internal RustFS S3 endpoint |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | development credentials | Static storage credentials |
| `S3_REGION` | `us-east-1` | RustFS signing region |
| `KEYCLOAK_TOKEN_URL` | internal Keycloak URL | Direct-path bearer-token issuer |
| `SP_ADMIN_CLIENT_ID` | `sp-admin` | Ranger-authorized principal |
| `SP_ADMIN_SECRET` | development secret | Client-credentials secret |
| `DLT_DATA_DIR` | `/tmp/dlt` | Disposable dlt local state |

The values use Compose service names because the runner executes inside the
stack network. This is particularly important for the `rustfs:9000` endpoint,
which is resolvable through Compose DNS. The demo catalog is configured with
`stsUnavailable=true`, so the direct PyIceberg path uses the static development
credentials above instead of requesting credential vending from Polaris.

To override a value for a manual Compose invocation:

```bash
SQE_TRANSPORT=flight docker compose \
  -f docker-compose.yml -f docker-compose.dlt.yml \
  --profile dlt run --rm --build dlt-tests
```

Do not commit production secrets. Put local overrides in the parent `.env` or
inject them from CI.

## Authentication and authorization

There are two authentication flows:

1. **Direct Iceberg REST:** the test obtains an `sp-admin` access token from
   Keycloak and gives it to dlt/PyIceberg. Polaris validates the token and asks
   Ranger whether that principal may create and modify the table.
2. **SQE:** the client presents `sp-admin` and its secret to Trino HTTP or Flight
   SQL. SQE obtains the Keycloak token and forwards it while accessing Polaris.

The parent quickstart grants `sp-admin` wildcard administrative access in the
Ranger `polaris` service. A denied or read-only principal is unsuitable for the
positive write suite; the parent `test.sh` separately covers those negative
authorization cases.

## Test data and cleanup

Tests use `sales_wh.sales` and table names beginning with:

```text
dlt_native_...
dlt_sqe_...
```

Every test starts with `DROP TABLE IF EXISTS` and repeats cleanup in a `finally`
block. A hard interruption may leave a table behind; it is safe to drop any
remaining `dlt_native_*` or `dlt_sqe_*` table before rerunning.

Pipeline state lives only in the disposable runner container under `/tmp/dlt`.
This suite validates destination behavior, not durable dlt state restoration.

## Troubleshooting

### Flight handshake fails

- Confirm the parent stack exposes Flight SQL on container port `50051`.
- Confirm `SQE_TRANSPORT=flight` and the URI starts with `grpc://` for this
  plaintext development stack.
- Check `docker compose logs sqe keycloak` for authentication errors.

### Polaris returns 401 or 403

- Verify `sp-admin` and its secret match the Keycloak realm import.
- Confirm `polaris-setup` and `ranger-setup` completed successfully.
- Ranger policies are polled; immediately after bootstrap, allow a few seconds
  for the policy cache to refresh.

### Direct Iceberg writes cannot reach RustFS

Run the tests through `./dlt/run.sh`, not directly on the host. The direct
PyIceberg destination uses the internal `rustfs:9000` endpoint, which is
intentionally resolved via Compose DNS.

### `Delete operation did not match any records`

This PyIceberg message is a `UserWarning`, not a failed transaction. PyIceberg
implements `overwrite` as a delete followed by an append and warns when the
delete predicate matches no files. It commonly appears when `replace` is used
to seed a newly created, empty table.

The examples avoid that unnecessary operation by using `append` for the first
load and exercising `replace` or `upsert` only once target data exists. Do not
globally suppress this warning: on a non-empty table it can reveal an incorrect
delete predicate. A load is considered failed only when dlt reports failed jobs
or the expected table assertions fail.

### A test process was interrupted

Rerun the suite. Each case removes its target table before loading. For a full
reset, tear down the parent stack including volumes and start it again:

```bash
./run.sh --down
docker compose down -v
./run.sh
```

The volume-removal command destroys only this quickstart's local demo data.
