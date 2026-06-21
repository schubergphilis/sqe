# Testing

SQE has two test tiers, both reached through one entry point: `scripts/test.sh`.

| Tier | What it proves | Stack | Entry point |
|---|---|---|---|
| **Tier 1 -- engine integration** | The query engine is correct: SQL semantics, joins, DDL/DML, auth, distributed dispatch | One shared `docker-compose.test.yml` (Polaris in-memory + RustFS) | `scripts/test.sh engine` |
| **Tier 2 -- scenarios** | Each documented use-case works end to end from a clean state | One stack per scenario (a quickstart's own `docker-compose.yml`) | `scripts/test.sh scenario <name>` |

Tier 1 is `cargo` tests against a single lightweight stack. Tier 2 runs the quickstarts: every `quickstart/<name>/run.sh --check` brings up that scenario's stack, runs its demo queries, and asserts the invariants that make the scenario correct.

## How to run

```bash
# Tier 1: engine integration tests (cargo, shared test stack)
scripts/test.sh engine
scripts/test.sh engine test_simple_select   # single test by name

# Tier 2: scenario tests
scripts/test.sh scenario nessie              # one scenario
scripts/test.sh scenario all                 # every self-contained scenario

# Tier 1 + all self-contained scenarios (what CI runs)
scripts/test.sh ci
```

`scripts/test.sh engine` delegates to `scripts/integration-test.sh`; any trailing argument is passed through as a test-name filter. `scripts/test.sh scenario <name>` runs `quickstart/<name>/run.sh --check`. You can also run a quickstart directly:

```bash
cd quickstart/nessie
cp .env.example .env
./run.sh --check        # up -> queries -> assert invariants
./run.sh                # up -> queries -> capture OUTPUT.md (no assertions)
./run.sh --down         # tear the stack down
```

The `distributed` scenario is heavy (it builds the SQE image and runs four containers plus Polaris and RustFS), so it is deliberately absent from the self-contained set and never runs under `all` or `ci`. Invoke it explicitly:

```bash
scripts/test.sh scenario distributed
```

## Tier 1: engine integration

Tier 1 covers the engine itself. Unit tests run with no external dependencies; integration and SQL-compat tests run against the shared `docker-compose.test.yml` stack (Polaris in-memory + RustFS).

### Test structure

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

### Unit tests

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

Run them directly with `cargo`:

```bash
cargo test --workspace          # all workspace unit tests (fast, no stack)
cargo test -p sqe-sql           # one crate
cargo test -p sqe-coordinator -- mode   # one test
```

### Integration tests

Integration tests live in `crates/sqe-coordinator/tests/integration_test.rs` and require the shared test stack (Polaris, RustFS). They are marked `#[ignore]` and run via `scripts/test.sh engine`, which starts the stack, bootstraps it, and runs the ignored tests.

#### Test inventory (45 tests)

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

The `docker-compose.test.yml` stack runs a coordinator with no worker behind it. `test_distributed_select` intentionally fails when no worker listens on `:50052` (issue #122, where local fallback masked distributed dispatch bugs), so full distributed coverage is exercised by the `distributed` scenario (`scripts/test.sh scenario distributed`) on `docker-compose.distributed.yml`, not by this stack.

#### Running a single integration test

```bash
scripts/test.sh engine test_simple_select
```

### SQL compatibility tests

SQL compatibility tests live in `crates/sqe-coordinator/tests/sql_compat_test.rs`. These 5 tests validate SQL semantic correctness beyond what the integration tests cover. They focus on edge cases in SQL behavior that must match ANSI SQL or Trino semantics, so queries migrating from another engine behave the same way.

The SQL compat tests use the same test stack and configuration as the integration tests. They are also marked `#[ignore]` and run as part of `scripts/test.sh engine`.

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

### Test configuration

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

The config uses `token_endpoint` (client_credentials mode) against Polaris's built-in OAuth.

## Tier 2: scenario tests

Tier 2 runs the quickstarts as tests. Each `quickstart/<name>/` directory is a self-contained use-case: it brings up everything the scenario needs, runs a few demo queries, and captures the real output. The quickstarts are the user-facing source of truth for "how do I run SQE for X," and they double as a validation base.

### How a scenario asserts

Every `run.sh` supports three modes:

```bash
./run.sh          # up -> queries -> capture OUTPUT.md
./run.sh --check  # up -> queries -> assert the scenario's invariants
./run.sh --down   # tear the stack down (some embedded scenarios use --clean)
```

`--check` runs the same demo queries the plain run captures, then asserts the invariants that define correctness for that scenario. The assertion vocabulary lives in `quickstart/_shared/lib.sh`, shared by every scenario:

- `assert_contains <label> <output> <substring>` -- output must contain a value (case-insensitive)
- `assert_not_empty <label> <output>` -- output is non-empty and not `0 rows`
- `assert_not_contains <label> <output> <substring>` -- output must not contain a value (for example `error`)
- `check_summary` -- prints the pass/fail totals and exits non-zero if any assertion failed

For example, the `nessie` scenario asserts that the catalog shows the demo namespace, that the purchase total reads back as `55.25`, and that the run produced no `error` line.

### OUTPUT.md and --check: one scenario, no drift

Each quickstart commits an `OUTPUT.md`: the captured output of a real run, shown in the quickstart README and the docs. The same scenario and the same `queries.sql` produce both the committed evidence and the asserted invariants. A plain `./run.sh` captures `OUTPUT.md`; `./run.sh --check` re-runs the same query file and asserts against it. Because both come from one scenario over one query file, the documented output and the tested behavior cannot drift: changing the queries changes both at once.

### Scenario catalog

Scenarios fall into three buckets. The 11 self-contained scenarios run under `scenario all` and `ci`. The `distributed` scenario has its own overlay stack and runs on demand only. The 3 cloud-gated AWS scenarios need real cloud credentials and run only through the manual `scenario-test-aws` CI job.

| Scenario | What it covers | Category |
|---|---|---|
| `polaris-keycloak-client-id` | Polaris + Keycloak; SQE mints user tokens via the OIDC password grant (client credentials) | self-contained |
| `polaris-keycloak-user-token` | Same stack; clients bring a pre-minted Keycloak token (`--token`), SQE validates and passes it through | self-contained |
| `polaris-ranger-keycloak` | Polaris + Apache Ranger access control: SQE writes GRANT/REVOKE to Ranger, Polaris enforces, column masks match Spark and Kyuubi byte for byte | self-contained |
| `nessie` | Project Nessie as the Iceberg REST catalog (auth-less, anonymous SQE) | self-contained |
| `unity-oss` | Unity Catalog OSS over Iceberg REST (read-only upstream; catalog-browse demo) | self-contained |
| `embedded-files` | Read local and remote files directly with the `read_*` TVFs (no server, no catalog) | self-contained |
| `embedded-sqlite-catalog` | Local persistent Iceberg catalog backed by SQLite (no server) | self-contained |
| `attach-catalogs` | Attach multiple persistent catalogs in embedded mode plus a cross-catalog JOIN | self-contained |
| `quack` | SQE's DuckDB Quack RPC, both ways: a DuckDB CLI queries SQE, and SQE's `quack_query()` pulls from a DuckDB server | self-contained |
| `observability` | Scrape SQE's Prometheus metrics with VictoriaMetrics + Grafana (provisioned dashboard) | self-contained |
| `benchmark` | Generate, load, and run TPC-H / TPC-DS / SSB against SQE with per-query timings (`sqe-bench`) | self-contained |
| `distributed` | A real cluster: coordinator + two stateless DataFusion workers over Arrow Flight, querying Polaris + RustFS (worker dispatch, system tables, query history, CTAS round-trip, result cache, Trino HTTP) | own stack, on-demand |
| `aws-glue` | AWS Glue Data Catalog; CDK bootstrap and teardown, SQE creates the database | cloud-gated |
| `aws-s3-tables` | AWS S3 Tables (managed Iceberg); CDK bootstrap and teardown, SQE creates the namespace | cloud-gated |
| `glue-lake-formation` | Glue database governed by Lake Formation: SQE denied until an explicit LF grant, then succeeds (table/DB-level gating, not column or row masking) | cloud-gated |

The `distributed` scenario replaces the retired standalone distributed test script: it brings up `docker-compose.test.yml` plus the `docker-compose.distributed.yml` overlay (which adds the coordinator and workers and inherits Polaris, RustFS, and Postgres), bootstraps Polaris, and asserts the distributed invariants through the same `_shared/lib.sh` helpers.

The AWS `run.sh` scripts do not yet support `--check`; the `scenario-test-aws` job invokes their default deploy, verify, and destroy flow directly. Adding a `--check` mode to the AWS scenarios is a follow-up.

## CI

| Job | Tier | Runs | When |
|---|---|---|---|
| `integration-test` | Tier 1 | `scripts/integration-test.sh` | Scheduled pipelines, merge-to-main push; manual (non-blocking) on MR pipelines |
| `scenario-test` | Tier 2 | `scripts/test.sh scenario all` (11 self-contained) | Scheduled pipelines and merge-to-main push (on changes to `quickstart/`, `crates/`, `scripts/test.sh`, or compose files); manual (non-blocking) on matching MR pipelines |
| `scenario-test-aws` | Tier 2 (cloud) | the three AWS quickstart `run.sh` flows | Manual only; gated on `RUN_AWS_SCENARIOS=1` and AWS credentials, never automatic |

All three jobs run docker-in-docker (a `docker:24-dind` sidecar) so each can stand up its own compose stack. `scenario-test` skips the heavy `distributed` scenario; run it on demand with `scripts/test.sh scenario distributed`. The AWS scenarios cost real money against a real account, which is why they are manual and credential-gated.

## Benchmark testing

`sqe-bench` validates SQL correctness and measures performance across industry-standard query suites. The `benchmark` scenario (`scripts/test.sh scenario benchmark`) wraps a generate, load, and run cycle, but the benchmark CLI also runs standalone:

```bash
# Generate, load, and run TPC-H at scale factor 1 (requires a running stack)
cargo run -p sqe-bench -- generate tpch --scale 1 --output ./data
cargo run -p sqe-bench -- load tpch --scale 1 --data ./data \
  --host localhost --port 60051 --username root --password ""
cargo run -p sqe-bench -- test tpch --scale 1 \
  --host localhost --port 60051 --username root --password ""

# Or use the script wrapper
./scripts/benchmark-test.sh tpch
```

Benchmark tests differ from integration tests in scope: GB-scale TPC/SSB data instead of small fixtures, full query suites (22 to 99 queries) instead of targeted feature tests, and PASS / DIFF / FAIL / SKIP / ERROR timing reports instead of pass/fail assertions. TPC-H at SF1 runs as a post-merge smoke test; the full suite runs nightly. JSON reports land in `benchmarks/results/` and are archived as CI artifacts for regression tracking.

For benchmark commands, scale factors, result formats, and how to add a benchmark, see [Benchmark Suite](../features/benchmarks.md).
