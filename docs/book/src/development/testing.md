# Testing

## Test Structure

```
tests/
└── integration_test.rs    # Cross-crate integration tests

crates/
├── sqe-core/src/          # No tests (simple types)
├── sqe-coordinator/src/
│   ├── mode.rs            # 10 unit tests (mode selection)
│   ├── worker_registry.rs # 5 unit tests (health checking)
│   ├── write_handler.rs   # 2 unit tests (schema conversion)
│   ├── catalog_ops.rs     # 5 unit tests (table ref parsing)
│   └── distributed_scan.rs # 3 unit tests
├── sqe-catalog/src/
│   ├── credential_vending.rs # 5 unit tests
│   └── info_schema.rs     # 4 unit tests
├── sqe-sql/src/
│   └── classifier.rs      # 29 unit tests (statement classification)
├── sqe-planner/src/
│   ├── scan_task.rs       # 2 unit tests (serialization)
│   └── splitter.rs        # 5 unit tests (file splitting)
├── sqe-metrics/src/
│   ├── lib.rs             # 4 unit tests (metrics registry)
│   ├── server.rs          # 1 unit test (metrics endpoint)
│   ├── audit.rs           # 3 unit tests (audit logging)
│   └── otel.rs            # 1 unit test
├── sqe-trino-compat/src/  # 12 unit tests (type mapping, serialization)
└── sqe-worker/src/
    └── executor.rs        # 3 unit tests (S3 URL parsing)
```

## Running Tests

```bash
# All workspace tests (fast — unit tests only)
cargo test --workspace

# Specific crate
cargo test -p sqe-sql
cargo test -p sqe-coordinator

# Specific test
cargo test -p sqe-coordinator -- mode

# Integration tests (require quickstart stack)
cargo test --workspace -- --ignored

# With output
cargo test --workspace -- --nocapture
```

## Unit Tests

Unit tests run without external dependencies. They test:

- **SQL classification** — every statement type routes correctly
- **Mode selection** — config/env var priority, case insensitivity, error cases
- **Worker health** — state transitions, failure thresholds, recovery
- **Schema conversion** — Arrow → Iceberg type mapping
- **Serialization** — ScanTask JSON roundtrip
- **File splitting** — even/uneven distribution across workers
- **Metrics** — counter increment, histogram observation
- **Audit** — JSONL serialization, file writing, no-op mode

## Integration Tests

Integration tests require a running stack (Keycloak, Polaris, MinIO). They are marked `#[ignore]` and run with `--ignored`:

| Test | What it validates |
|---|---|
| `test_keycloak_authentication` | ROPC grant → session creation |
| `test_different_users_get_different_sessions` | Session isolation |
| `test_simple_select` | SELECT 1 end-to-end |
| `test_ctas_roundtrip` | CREATE TABLE → SELECT → verify → DROP |
| `test_insert_into` | CTAS + INSERT + verify row count |
| `test_drop_table` | CREATE → DROP → verify gone |
| `test_information_schema_tables` | information_schema queries |
| `test_distributed_select` | Coordinator → worker scan |

### Running Integration Tests

```bash
# Start the quickstart stack (Keycloak, Polaris, MinIO)
cd data-platform/quickstart/full/
docker compose up -d

# Wait for services to be ready, then:
cd sqe/
cargo test --workspace -- --ignored
```

The test config is at `tests/sqe-test.toml`.

## Test Configuration

```toml
# tests/sqe-test.toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080
mode = "local"

[auth]
keycloak_url = "https://keycloak.local:8443"
realm = "iceberg"
client_id = "sqe-client"
ssl_verification = false

[catalog]
polaris_url = "http://polaris.local:8181/api/catalog"
warehouse = "iceberg"

[storage]
s3_endpoint = "http://minio.local:9000"
s3_region = "us-east-1"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_path_style = true
```
