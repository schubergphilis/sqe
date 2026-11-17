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

## Trino parity

The integration tests check that SQE answers correctly. The parity harness checks that it answers the way Trino does. It runs the same SQL on SQE and on a real Trino 484, both pointed at one Polaris catalog, so both engines read the same Iceberg files. Any difference in the result is an engine difference, never a data difference.

```bash
scripts/parity-test.sh --all --protocol both   # queries + Iceberg scenarios + functions
scripts/parity-test.sh --protocol flight       # queries only, SQE over Flight SQL
scripts/parity-test.sh --no-build --reload     # reuse the image, reload the demo data
```

The script brings up RustFS, Polaris, SQE and Trino, loads TPC-H tiny into `iceberg.tpch_demo` through Trino's `tpch` connector, and runs three suites. Trino is always read over its own HTTP protocol. SQE is read over the Trino HTTP protocol, Flight SQL (through the ADBC driver), or both. Flight needs `pip install -r tests/parity/requirements.txt` in a virtualenv, with `PARITY_PYTHON` pointing at its `python`.

| Suite | What it runs | Denominator |
|---|---|---|
| `parity_compare.py` | the read-only queries in `tests/parity/queries.json`: aggregates, joins, windows, arrays, maps, JSON, casts | 153 queries |
| `iceberg_parity.py` | DDL and DML sequences on each engine's own table, then a diffed read; plus interop, where one engine writes and both read | 20 scenarios |
| `function_coverage.py` | every scalar, aggregate and window signature in Trino's `SHOW FUNCTIONS`, called with sample arguments of its declared types | 296 functions |

The comparison is strict. Two results MATCH only when the column types are the same, the values are the same after canonicalising both sides by Trino's type (decimals as decimals, doubles to 9 significant digits), and the row order is the same under a top-level `ORDER BY`. A result that differs only in type is `TYPE`; a different value or row count is `DIFF`. A statement both engines reject counts as parity, because error text is not part of the contract.

The function list comes from Trino, not from us. Signatures whose types Iceberg cannot store (HyperLogLog, Geometry, IPADDRESS) are reported but not counted. A signature the generator cannot call on Trino counts as `UNTESTED`, against SQE, until the generator learns it. A function counts as covered only when every counted signature matches with real values; one that matched only on typed NULLs is reported apart.

First strict baseline, 2026-11-17, against Trino 484:

| Suite | Trino HTTP | Flight SQL |
|---|---|---|
| Queries | 137 of 153 | 129 of 153 |
| Iceberg scenarios | 20 of 20 | 20 of 20 |
| Functions | 121 of 296 | 115 of 296 |

The same queries scored 153 of 153 under the old comparator, which rounded numbers to two places and ignored column types. The per-query and per-function tables are in [`docs/evidence/trino-parity/`](https://github.com/schubergphilis/sqe/tree/main/docs/evidence/trino-parity), and the work to close each gap is the OpenSpec change [`trino-sql-parity`](https://github.com/schubergphilis/sqe/tree/main/openspec/changes/trino-sql-parity). The suite passes when `scripts/parity-test.sh --all --protocol both` exits 0.

### Driver probes

SQL parity says nothing about the metadata calls a BI tool makes before it runs a query. Two probes under [`testing/drivers/`](https://github.com/schubergphilis/sqe/tree/main/testing/drivers) drive SQE through the real client drivers and print one line per check, so one run lists every gap:

| Probe | Driver | Protocol | Last result (2026-11-17) |
|---|---|---|---|
| `jdbc/Matrix.java` | Trino JDBC 480 | Trino HTTPS | 65 of 74 |
| `jdbc/Matrix.java` | Arrow Flight SQL JDBC 19.0.0 | Flight SQL | 71 of 72 |
| `odbc/` | Arrow Flight SQL ODBC 26.0.1 | Flight SQL | 105 of 114 |

The results are the same with a password and with a bearer token. Every remaining failure is a call the driver itself does not implement (`SQLFeatureNotSupportedException` in Trino JDBC, `IM001` in ODBC), except Flight SQL JDBC's `setSchema`, which the driver ignores.

## CI

Every suite that needs a compose stack runs **locally**, not in CI. The jobs that
used to run them (`integration-test`, `distributed-smoke`, `scenario-test`,
`access-control-test`) all required a privileged `docker:dind` sidecar, and the
sidecar never passed its health check on the shared runners. None of them ever
executed a line of engine code there.

Keeping them as non-blocking jobs was worse than removing them. A permanently
yellow job reads as coverage when there is none, and two real breakages on the
default branch hid behind exactly that. `scenario-test` was worse again: it sat in
the `test` stage, so its failure skipped the container build and publish jobs
behind it.

| Suite | Local command |
|---|---|
| Engine integration | `make test-integration` (`FILTER=name` for one test) |
| Distributed dispatch | `make test-distributed` |
| Quickstart scenarios | `scripts/test.sh scenario all` (11 self-contained) |
| Distributed scenario | `scripts/test.sh scenario distributed` |
| Access control | `make test-access-control` |
| Access control via Spark | `make test-access-control-spark` |
| Trino parity | `scripts/parity-test.sh --all --protocol both` |
| BI driver probes | see [`testing/drivers/README.md`](https://github.com/schubergphilis/sqe/tree/main/testing/drivers) |

Run the ones your change touches before merging. `make test-integration` brings up
its own Polaris and RustFS stack, preflights the fixed host ports, and names any
container already holding one.

One CI job still stands up a stack:

| Job | Tier | Runs | When |
|---|---|---|---|
| `scenario-test-aws` | Tier 2 (cloud) | the three AWS quickstart `run.sh` flows | Manual only; gated on `RUN_AWS_SCENARIOS=1` and AWS credentials, never automatic |

The AWS scenarios cost real money against a real account, which is why they are
manual and credential-gated. Restoring any of the removed jobs needs a runner with
a working dind sidecar, or a shell runner with a mounted docker socket.

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
