# Testing

## Test Structure

```
crates/
├── sqe-core/src/              # 40 unit tests (config validation, error types, session, memory limit parsing)
├── sqe-auth/src/              # 17 unit tests (authenticator, OIDC, session)
├── sqe-coordinator/
│   ├── src/
│   │   ├── mode.rs            # 10 unit tests (mode selection)
│   │   ├── worker_registry.rs # 5 unit tests (health checking)
│   │   ├── write_handler.rs   # 2 unit tests (schema conversion)
│   │   ├── catalog_ops.rs     # 5 unit tests (table ref parsing)
│   │   └── distributed_scan.rs # 3 unit tests
│   └── tests/
│       ├── integration_test.rs  # 45 integration tests (end-to-end)
│       └── sql_compat_test.rs   # 5 SQL compatibility tests
├── sqe-catalog/src/
│   ├── credential_vending.rs  # 5 unit tests
│   └── info_schema.rs         # 4 unit tests
├── sqe-sql/src/
│   └── classifier.rs          # 29 unit tests (statement classification)
├── sqe-planner/src/
│   ├── scan_task.rs           # 2 unit tests (serialization)
│   └── splitter.rs            # 5 unit tests (file splitting)
├── sqe-policy/src/            # 8 unit tests (policy enforcer, passthrough)
├── sqe-metrics/src/
│   ├── lib.rs                 # 4 unit tests (metrics registry)
│   ├── server.rs              # 1 unit test (metrics endpoint)
│   ├── audit.rs               # 3 unit tests (audit logging)
│   └── otel.rs                # 1 unit test
├── sqe-trino-compat/src/      # 12 unit tests (type mapping, serialization)
└── sqe-worker/src/
    └── executor.rs            # 3 unit tests (S3 URL parsing)
```

## Running Tests

```bash
# All workspace tests (fast -- unit tests only)
cargo test --workspace

# Specific crate
cargo test -p sqe-sql
cargo test -p sqe-coordinator

# Specific test
cargo test -p sqe-coordinator -- mode

# Integration tests (require test stack)
cargo test --workspace -- --ignored

# With output
cargo test --workspace -- --nocapture
```

## Unit Tests

Unit tests run without external dependencies. They test:

- **Config validation** -- environment variable parsing, default values, memory limit parsing
- **Error types** -- error construction, display formatting, conversion
- **Session management** -- session creation, token fingerprint, expiry
- **Authentication** -- OIDC flow, token validation, client credentials
- **Policy enforcement** -- passthrough policy, enforcer trait behavior
- **SQL classification** -- every statement type routes correctly
- **Mode selection** -- config/env var priority, case insensitivity, error cases
- **Worker health** -- state transitions, failure thresholds, recovery
- **Schema conversion** -- Arrow to Iceberg type mapping
- **Serialization** -- ScanTask JSON roundtrip
- **File splitting** -- even/uneven distribution across workers
- **Metrics** -- counter increment, histogram observation
- **Audit** -- JSONL serialization, file writing, no-op mode

## Integration Tests

Integration tests live in `crates/sqe-coordinator/tests/integration_test.rs` and require a running test stack (Polaris, S3-compatible storage). They are marked `#[ignore]` and run with `--ignored`.

### Test inventory (45 tests)

| Category | Tests | What they validate |
|---|---|---|
| **Core queries** | `test_simple_select`, `test_where_conditions`, `test_order_limit_offset`, `test_case_expression`, `test_math_expressions`, `test_string_functions` | Basic SELECT, filtering, ordering, CASE/WHEN, arithmetic, string functions |
| **Joins** | `test_inner_join`, `test_left_join`, `test_right_join`, `test_full_outer_join`, `test_cross_join`, `test_self_join`, `test_three_way_join`, `test_join_with_aggregation` | All join types including multi-table and join+GROUP BY |
| **Aggregation** | `test_aggregation_basic`, `test_having_clause`, `test_window_functions`, `test_window_running_total` | GROUP BY, HAVING, OVER(), running totals |
| **Subqueries** | `test_subquery_where`, `test_in_subquery`, `test_exists_subquery`, `test_scalar_subquery_select` | Correlated and uncorrelated subqueries, IN, EXISTS |
| **CTEs** | `test_cte_join`, `test_multiple_ctes` | WITH clauses, multi-CTE queries |
| **Set operations** | `test_union_all` | UNION ALL across tables |
| **DDL/DML** | `test_ctas_roundtrip`, `test_insert_into`, `test_drop_table`, `test_drop_table_if_exists_no_error`, `test_create_and_drop_view`, `test_view_with_aggregation` | CREATE TABLE AS, INSERT INTO, DROP TABLE, views |
| **EXPLAIN** | `test_explain_plan`, `test_explain_analyze`, `test_explain_full`, `test_explain_policy_aware` | Plan output, execution stats, policy annotation |
| **Metadata** | `test_information_schema_tables`, `test_information_schema_schemata` | information_schema virtual tables |
| **Auth** | `test_authentication`, `test_token_fingerprint`, `test_keycloak_auth_with_test_users`, `test_keycloak_token_refresh`, `test_different_user_catalog_visibility` | Token flow, session fingerprinting, per-user catalog isolation |
| **Distributed** | `test_distributed_select`, `test_local_fallback_without_workers` | Coordinator-to-worker scan, graceful fallback to local mode |
| **Trino compat** | `test_trino_http_query` | Query via Trino HTTP protocol adapter |

### Running Integration Tests

The lightweight test stack is the primary way to run integration tests:

```bash
# Start the lightweight test stack (Polaris in-memory + RustFS)
docker compose -f docker-compose.test.yml up -d

# Bootstrap (idempotent: creates buckets, warehouse, namespaces)
./scripts/bootstrap-test.sh

# Run all integration tests
./scripts/integration-test.sh

# Run a single test by name
./scripts/integration-test.sh test_simple_select
```

The test config is at `tests/sqe-test.toml` and uses `token_endpoint` (client_credentials mode) against Polaris's built-in OAuth.

## SQL Compatibility Tests

SQL compatibility tests live in `crates/sqe-coordinator/tests/sql_compat_test.rs`. These 5 tests validate SQL semantic correctness beyond what the integration tests cover -- they focus on edge cases in SQL behavior that must match ANSI SQL or Trino semantics to avoid surprises for users migrating queries.

The SQL compat tests use the same test stack and configuration as the integration tests. They are also marked `#[ignore]` and run as part of `./scripts/integration-test.sh`.

## Test Configuration

```toml
# tests/sqe-test.toml
[coordinator]
flight_sql_port = 50051
trino_http_port = 8080

[auth]
token_endpoint = "http://localhost:8181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"

[catalog]
polaris_url = "http://localhost:8181/api/catalog"
warehouse = "test_warehouse"

[storage]
s3_endpoint = "http://localhost:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true
```
