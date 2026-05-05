# Quickstart

Get SQE running locally in under 5 minutes.

## Prerequisites

- **Rust** 1.85+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- A running data platform stack: **Keycloak**, **Polaris**, **MinIO/S3** (see the quickstart stack in `data-platform/quickstart/full/`)

## 1. Clone and Build

```bash
git clone https://github.com/schuberg/sqe.git
cd sqe
cargo build --release --bin sqe-server --bin sqe-cli
```

Or use the build script:
```bash
./scripts/build.sh release
```

## 2. Configure

Copy the example config and adjust for your environment:

```bash
cp sqe.toml.example sqe.toml
```

Key settings to update:

```toml
[auth]
keycloak_url = "https://your-keycloak:8443"   # Your Keycloak URL
realm = "iceberg"                               # Your realm
client_id = "sqe-client"                        # OIDC client ID

[catalog]
polaris_url = "http://your-polaris:8181/api/catalog"
warehouse = "your-warehouse"

[storage]
s3_endpoint = "http://your-minio:9000"
s3_region = "us-east-1"
s3_access_key = "minioadmin"                    # Or set via SQE_STORAGE__S3_ACCESS_KEY
s3_secret_key = "minioadmin"                    # Or set via SQE_STORAGE__S3_SECRET_KEY
```

## 3. Start the Server

```bash
# Single-node coordinator (default mode)
./target/release/sqe-server --config sqe.toml
```

You should see:
```
INFO Starting sqe-server mode=Coordinator config="sqe.toml"
INFO Health endpoints on port 9091 (/healthz, /readyz)
INFO Prometheus metrics on port 9090
INFO SQE coordinator listening on 0.0.0.0:50051
```

## 4. Connect with the CLI

```bash
./target/release/sqe-cli --host localhost --port 50051
```

```
Username: alice
Password: ****
sqe-cli 0.1.0 connected to http://localhost:50051 (flight)
Type SQL queries, or \q to quit. End multi-line queries with ;

sqe> SHOW SCHEMAS;
 schema_name
-------------
 analytics
 raw
(2 rows)

sqe> SELECT * FROM raw.orders LIMIT 5;
 order_id | customer_id | amount | region
----------+-------------+--------+--------
 1        | 100         | 250.00 | EU
 2        | 101         | 150.00 | US
 3        | 100         | 300.00 | EU
 4        | 102         | 75.00  | APAC
 5        | 103         | 500.00 | EU
(5 rows)
```

## 5. Run a Single Query

```bash
./target/release/sqe-cli -H localhost -p 50051 -u alice -e "SELECT COUNT(*) FROM raw.orders;"
```

Set `SQE_PASSWORD` to avoid the password prompt:
```bash
export SQE_USER=alice
export SQE_PASSWORD=secret
./target/release/sqe-cli -e "SHOW TABLES IN raw;"
```

## Health Check

```bash
curl http://localhost:9091/healthz   # → ok
curl http://localhost:9091/readyz    # → 200 when ready
```

## Pointing at a different catalog

The walkthrough above runs SQE against the local Polaris stack
over Iceberg REST. SQE supports five other catalog backends out
of the box: AWS Glue (native SDK), AWS S3 Tables (managed
Iceberg), Hive Metastore (Thrift), JDBC (Postgres / MySQL /
SQLite), and Hadoop (filesystem-only). Each uses the same
binary, just with a different `[catalog.backend]` block.

See [Catalog backends](./catalogs.md) for the full per-backend
recipe with TOML examples, AWS credential setup, verification
queries, and a troubleshooting checklist. Glue and S3 Tables are
verified live against AWS deployments.

For the operator-friendly version of the same content (BI tool
connection, slim builds, cargo features), see
[`QUICKSTART.md`](https://github.com/schuberg/sqe/blob/main/QUICKSTART.md)
in the repo root.

## Next Steps

- [Catalog backends](./catalogs.md): per-backend TOML, credentials, verification queries
- [Configuration Reference](../deployment/configuration.md): all settings and env vars
- [Docker](../deployment/docker.md): run in containers
- [Kubernetes & Helm](../deployment/kubernetes.md): production deployment
- [Using the CLI](./cli.md): full CLI reference
