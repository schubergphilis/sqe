# Flight SQL

Arrow Flight SQL is SQE's primary wire protocol. It is columnar end to end:
results come back as Arrow record batches over gRPC, with no row-by-row
serialization tax. Every official client (the `sqe-cli`, ADBC drivers, the
dbt-sqe adapter) speaks it. This page covers both topologies against Apache
Polaris.

## Single server

One coordinator parses, plans, and executes. Good for development, small
deployments, and the default Docker image.

### Prerequisites

```bash
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
```

This brings up Polaris (`http://localhost:18181`) and an S3-compatible store,
then creates the `test_warehouse` warehouse and the `default` / `test_ns`
namespaces.

### Configuration

The coordinator reads one TOML file. The Flight SQL listener is on `50051`:

```toml
[auth]
token_endpoint = "http://localhost:18181/api/catalog/v1/oauth/tokens"
client_id      = "root"

[catalog]
polaris_url = "http://localhost:18181/api/catalog"
warehouse   = "test_warehouse"

[storage]
s3_endpoint   = "http://localhost:19000"
s3_region     = "us-east-1"
s3_path_style = true
```

### Run

```bash
# Start the coordinator (Flight SQL on 50051).
./target/release/sqe-server --config sqe.toml

# Connect with the CLI over Flight.
./target/release/sqe-cli --protocol flight --host localhost --port 50051 \
    -u root -e "SELECT 1 AS one"
```

### Expected output

```
 one
-----
 1
(1 row)
```

The in-process equivalent is exercised by `integration_test.rs`:
`test_authentication` (OIDC client-credentials against Polaris),
`test_simple_select`, and the file-format TVF tests all run the full
Flight SQL query path against this stack.

### How it is tested

- `crates/sqe-coordinator/tests/integration_test.rs` (run with
  `./scripts/integration-test.sh`): authentication, SELECT, CTAS round-trip,
  information_schema, and `read_parquet`/`read_csv`/`read_json`.

## Distributed (coordinator + workers)

The coordinator parses and plans, then ships secured plan fragments to
stateless workers over Arrow Flight. Workers hold no catalog state; they
receive the plan and the user's bearer token and execute.

### Prerequisites

```bash
docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml up --build -d
./scripts/bootstrap-test.sh
```

This adds a coordinator (Flight SQL on `60051`) and two workers (internal
`50052`, exposed as `60061` / `60062`) on the shared Polaris and storage.

### Run

```bash
./scripts/distributed-test.sh
```

The script builds `sqe-cli`, runs SQL over Flight on `60051`, and verifies
worker dispatch through `system.runtime.tasks` (proving fragments actually
reach the workers rather than silently falling back to local execution).

### Expected output

```text
<!-- FILL: distributed-test.sh tail -->
```

### How it is tested

- `crates/sqe-coordinator/tests/integration_test.rs::test_distributed_select`
  (ignored by default; needs workers listening on `:50052`).
- `scripts/distributed-test.sh`: full coordinator + worker harness.
- `scripts/concurrent-test.sh`: N parallel Flight clients, cache behaviour.

## Notes

- Auth is bearer-token passthrough: the CLI authenticates the user via OIDC,
  and the token rides to Polaris and S3. There is no service account.
- The internal Flight port is `50051`; compose maps it to `60051` to avoid
  colliding with a local coordinator.
- Workers are stateless. Scale by adding worker replicas; the coordinator
  load-balances fragments across registered workers.
