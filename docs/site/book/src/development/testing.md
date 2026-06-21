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

Each `.sql` file under `crates/sqe-coordinator/tests/sql/` is one `#[tokio::test]` registered in `sql_compat_test.rs`. The files use a simple block format and rely on CTEs rather than fixture tables, so each block is self-contained:

```
--- test_name
SQL statement;
--- expect
col1 | col2
val1 | val2
```

Add a new case by appending a block to an existing file, or by creating a new file and registering it in `sql_compat_test.rs`.

### Fixture data

Most join, aggregation, view, and window integration tests share two fixture tables, created fresh per test and torn down after:

`test_ns.employees`

| id | name    | dept_id | salary   |
|----|---------|---------|----------|
| 1  | Alice   | 10      | 90000.00 |
| 2  | Bob     | 10      | 85000.00 |
| 3  | Charlie | 20      | 70000.00 |
| 4  | Dave    | 20      | 75000.00 |
| 5  | Eve     | 30      | 95000.00 |
| 6  | Frank   | 99      | 60000.00 |

`test_ns.departments`

| id | dept_name   | budget     |
|----|-------------|------------|
| 10 | Engineering | 500000.00  |
| 20 | Marketing   | 200000.00  |
| 30 | Executive   | 1000000.00 |
| 40 | HR          | 150000.00  |

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
catalog_url = "http://localhost:8181/api/catalog"
warehouse = "test_warehouse"

[storage]
s3_endpoint = "http://localhost:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true
```

## Benchmark Testing

Beyond unit and integration tests, SQE ships with `sqe-bench` — a benchmark CLI that validates SQL correctness and measures performance across industry-standard query suites.

Benchmark tests differ from integration tests in scope and purpose:

| | Integration tests | Benchmark tests |
|---|---|---|
| Data | Synthetic fixtures (small) | TPC/SSB scale factor data (GB scale) |
| Queries | Targeted feature tests | Full benchmark query suites (22–99 queries) |
| Validation | Pass/fail assertions | PASS / DIFF / FAIL / SKIP / ERROR with timing |
| Purpose | Regression detection | SQL correctness + performance tracking |

### Quick benchmark run

```bash
# Generate TPC-H data at scale factor 1
cargo run -p sqe-bench -- generate tpch --scale 1 --output ./data

# Load into SQE (requires running stack)
cargo run -p sqe-bench -- load tpch --scale 1 --data ./data \
  --host localhost --port 60051 --username root --password ""

# Run all 22 TPC-H queries
cargo run -p sqe-bench -- test tpch --scale 1 \
  --host localhost --port 60051 --username root --password ""

# Or use the script wrapper
./scripts/benchmark-test.sh tpch
```

### Supported benchmarks

| Benchmark | Queries | Notes |
|-----------|---------|-------|
| `tpch` | 22 | Standard first check for any SQL engine |
| `tpcds` | 99 | Complex SQL: correlated subqueries, window functions, GROUPING SETS |
| `ssb` | 13 | Fast smoke test — denormalized star schema |
| `tpcc` | 8 | OLTP reads; write queries skip until DELETE/MERGE land |
| `tpce` | 11 | Brokerage OLTP reads |
| `tpcbb` | 10 | SQL-only subset over TPC-DS data |

### Benchmark test in CI

TPC-H at SF1 runs as a post-merge smoke test. The full suite (TPC-H + TPC-DS + SSB) runs nightly. JSON reports are written to `benchmarks/results/` and archived as CI artifacts for regression tracking.

```bash
# CI smoke test (TPC-H SF1 only, fails on any ERROR or FAIL)
./scripts/benchmark-test.sh tpch

# Nightly full suite
./scripts/benchmark-test.sh tpch
./scripts/benchmark-test.sh tpcds
./scripts/benchmark-test.sh ssb
```

For full documentation of benchmark commands, scale factors, result formats, and how to add new benchmarks, see [Benchmark Suite](../features/benchmarks.md).
