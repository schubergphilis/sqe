# dltHub ingestion through Polaris and SQE

This is an end-to-end test and executable example for loading Iceberg data with
[dltHub's `dlt` Python library](https://dlthub.com/docs/intro). It proves two
independent write paths against the same Polaris catalog and RustFS warehouse:

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

## What is tested

Each path runs the same four loading patterns:

| Pattern | Initial load | Second load | Expected state |
|---|---|---|---|
| Replace/overwrite | IDs 1 and 2 | only ID 1 | The old contents are gone |
| Delta/upsert | IDs 1 and 2 | update 1, add 3 | IDs 1, 2 and 3; ID 1 is updated |
| SCD Type 2 | IDs 1 and 2 | change 1, add 3 | ID 1 has retired and active versions |
| Ordered SCD Type 2 | ID 1 is bronze | one run contains silver, then gold | bronze and silver are closed in order; only gold is active |

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

### A test process was interrupted

Rerun the suite. Each case removes its target table before loading. For a full
reset, tear down the parent stack including volumes and start it again:

```bash
./run.sh --down
docker compose down -v
./run.sh
```

The volume-removal command destroys only this quickstart's local demo data.
