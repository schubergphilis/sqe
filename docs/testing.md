# Testing

## Running Integration Tests

```bash
./scripts/integration-test.sh
```

The script:
1. Starts a lightweight test stack (Polaris in-memory + RustFS) via Docker Compose
2. Bootstraps the catalog (`test_warehouse`, namespaces `default` and `test_ns`)
3. Runs all integration tests sequentially (`--test-threads=1`)
4. Prints SQE engine, Polaris, and RustFS logs
5. Tears down the stack

**Run a single test:**
```bash
./scripts/integration-test.sh test_inner_join
```

**Increase log verbosity:**
```bash
RUST_LOG=debug ./scripts/integration-test.sh
```

**Keep the stack running after tests** (edit the script and comment out `docker compose down`).

### Prerequisites

- Docker with Compose plugin
- `cargo` (Rust toolchain)
- `python3` (used by bootstrap script for JSON parsing)

---

## Test Inventory

### Unit Tests — no stack required

| Test | SQL / What it checks |
|---|---|
| `test_sql_classification` | Parser correctly classifies `SELECT`, `CREATE TABLE AS SELECT`, `INSERT INTO`, `DELETE FROM` |
| `test_delete_returns_not_implemented` | `DELETE FROM` is parsed as `Delete` kind; error message mentions "overwrite transaction support" |
| `test_worker_registry_no_workers` | Empty registry returns no healthy workers |
| `test_scan_task_roundtrip` | `ScanTask` serializes and deserializes correctly |
| `test_metrics_registry` | `MetricsRegistry` counter increments |
| `test_audit_logger_noop` | `AuditLogger` with empty path does not panic |
| `test_trino_type_mapping` | `Int64 → bigint`, `Utf8 → varchar`, `Float64 → double` |
| `test_trino_batches_to_json` | `RecordBatch` serializes to Trino JSON wire format |

---

### Integration Tests — require `./scripts/integration-test.sh`

#### Authentication

| Test | SQL |
|---|---|
| `test_authentication` | *(no SQL)* — OAuth2 client_credentials flow, asserts non-empty access token |
| `test_token_fingerprint` | *(no SQL)* — token fingerprint starts with username |

#### Basic Query Execution

| Test | SQL |
|---|---|
| `test_simple_select` | `SELECT 1` |
| `test_local_fallback_without_workers` | `SELECT 1 as x` |

#### Write Path (CTAS / INSERT / DROP)

| Test | SQL |
|---|---|
| `test_ctas_roundtrip` | `CREATE TABLE test_ns.ctas_test AS SELECT 1 as id, 'hello' as name` → `SELECT * FROM test_ns.ctas_test` → `DROP TABLE test_ns.ctas_test` |
| `test_insert_into` | `CREATE TABLE test_ns.insert_test AS SELECT 1 as id, 'first' as name` → `INSERT INTO test_ns.insert_test SELECT 2 as id, 'second' as name` → `SELECT * FROM test_ns.insert_test` |
| `test_drop_table` | `CREATE TABLE test_ns.drop_test AS SELECT 1 as id` → `DROP TABLE test_ns.drop_test` → `SELECT * FROM test_ns.drop_test` *(expect error)* |
| `test_drop_table_if_exists_no_error` | `DROP TABLE IF EXISTS test_ns.nonexistent_table_xyz` *(must not error)* |

#### Distributed Execution

| Test | SQL |
|---|---|
| `test_distributed_select` | `CREATE TABLE test_ns.dist_test AS SELECT 1 as id, 'distributed' as name` → `SELECT * FROM test_ns.dist_test` → `DROP TABLE test_ns.dist_test` *(requires worker on localhost:50052)* |

#### Catalog Metadata

| Test | SQL |
|---|---|
| `test_information_schema_tables` | `SELECT * FROM information_schema.tables` |
| `test_information_schema_schemata` | `SELECT * FROM information_schema.schemata` |

#### Views

Fixture tables: `test_ns.employees` (6 rows: id, name, dept_id, salary), `test_ns.departments` (4 rows: id, dept_name, budget)

| Test | SQL |
|---|---|
| `test_create_and_drop_view` | `CREATE VIEW test_ns.eng_view AS SELECT id, name, salary FROM test_ns.employees WHERE dept_id = 10` → `SELECT * FROM test_ns.eng_view` → `DROP VIEW test_ns.eng_view` |
| `test_view_with_aggregation` | `CREATE VIEW test_ns.dept_stats AS SELECT dept_id, COUNT(*) as headcount, AVG(salary) as avg_salary FROM test_ns.employees GROUP BY dept_id` → `SELECT dept_id, headcount, avg_salary FROM test_ns.dept_stats ORDER BY dept_id` |

#### Joins

Fixture tables: `test_ns.employees`, `test_ns.departments`

| Test | SQL |
|---|---|
| `test_inner_join` | `SELECT e.id, e.name, d.dept_name, e.salary FROM test_ns.employees e INNER JOIN test_ns.departments d ON e.dept_id = d.id ORDER BY e.id` |
| `test_left_join` | `SELECT e.id, e.name, d.dept_name FROM test_ns.employees e LEFT JOIN test_ns.departments d ON e.dept_id = d.id ORDER BY e.id` |
| `test_right_join` | `SELECT d.dept_name, e.name, e.salary FROM test_ns.employees e RIGHT JOIN test_ns.departments d ON e.dept_id = d.id ORDER BY d.id, e.id` |
| `test_full_outer_join` | `SELECT e.id, e.name, d.dept_name FROM test_ns.employees e FULL OUTER JOIN test_ns.departments d ON e.dept_id = d.id ORDER BY e.id, d.id` |
| `test_cross_join` | `SELECT color, size FROM test_ns.colors CROSS JOIN test_ns.sizes ORDER BY color, size` *(3×3 = 9 rows)* |
| `test_self_join` | `SELECT e.name as employee, m.name as manager FROM test_ns.org e LEFT JOIN test_ns.org m ON e.mgr_id = m.id ORDER BY e.id` |
| `test_three_way_join` | `SELECT e.name, d.dept_name, p.project_name FROM test_ns.employees e INNER JOIN test_ns.departments d ON e.dept_id = d.id INNER JOIN test_ns.projects p ON e.dept_id = p.owner_dept ORDER BY e.name, p.project_name` |

#### Aggregations

| Test | SQL |
|---|---|
| `test_aggregation_basic` | `SELECT dept_id, COUNT(*) as headcount, SUM(salary) as total_salary, AVG(salary) as avg_salary, MIN(salary) as min_salary, MAX(salary) as max_salary FROM test_ns.employees GROUP BY dept_id ORDER BY dept_id` |
| `test_having_clause` | `SELECT dept_id, AVG(salary) as avg_salary FROM test_ns.employees GROUP BY dept_id HAVING AVG(salary) > 75000.0 ORDER BY dept_id` |
| `test_join_with_aggregation` | `SELECT d.dept_name, COUNT(e.id) as headcount, AVG(e.salary) as avg_salary FROM test_ns.departments d LEFT JOIN test_ns.employees e ON d.id = e.dept_id GROUP BY d.dept_name ORDER BY headcount DESC, d.dept_name` |

#### Complex Queries

| Test | SQL |
|---|---|
| `test_cte_join` | `WITH high_earners AS (SELECT id, name, dept_id FROM test_ns.employees WHERE salary > 80000) SELECT h.name, d.dept_name FROM high_earners h INNER JOIN test_ns.departments d ON h.dept_id = d.id ORDER BY h.name` |
| `test_multiple_ctes` | `WITH dept_avg AS (...), high_depts AS (...) SELECT e.name, e.salary FROM test_ns.employees e INNER JOIN high_depts hd ON e.dept_id = hd.dept_id ORDER BY e.salary DESC` |
| `test_subquery_where` | `SELECT name, salary FROM test_ns.employees WHERE salary > (SELECT AVG(salary) FROM test_ns.employees) ORDER BY salary DESC` |
| `test_scalar_subquery_select` | `SELECT name, salary, salary - (SELECT AVG(salary) FROM test_ns.employees) as salary_vs_avg FROM test_ns.employees ORDER BY salary_vs_avg DESC` |
| `test_in_subquery` | `SELECT name, dept_id FROM test_ns.employees WHERE dept_id IN (SELECT id FROM test_ns.departments WHERE dept_name LIKE '%ing%') ORDER BY name` |
| `test_exists_subquery` | `SELECT dept_name FROM test_ns.departments d WHERE EXISTS (SELECT 1 FROM test_ns.employees e WHERE e.dept_id = d.id AND e.salary > 85000) ORDER BY dept_name` |
| `test_union_all` | `SELECT quarter, product, qty FROM test_ns.q1_sales UNION ALL SELECT quarter, product, qty FROM test_ns.q2_sales ORDER BY quarter, product` |
| `test_order_limit_offset` | `SELECT name, salary FROM test_ns.employees ORDER BY salary DESC LIMIT 3 OFFSET 1` |
| `test_where_conditions` | `SELECT name, dept_id, salary FROM test_ns.employees WHERE (dept_id = 10 OR dept_id = 20) AND salary >= 75000.0 ORDER BY salary DESC` |
| `test_case_expression` | `SELECT name, salary, CASE WHEN salary >= 90000.0 THEN 'Senior' WHEN salary >= 75000.0 THEN 'Mid' ELSE 'Junior' END as level FROM test_ns.employees ORDER BY salary DESC` |
| `test_string_functions` | `SELECT UPPER(name), LOWER(name), LENGTH(name), CONCAT(name, ' (id=', CAST(id AS VARCHAR), ')') as label FROM test_ns.employees ORDER BY id LIMIT 3` |
| `test_math_expressions` | `SELECT name, salary, ROUND(salary * 1.1, 0) as salary_plus_10pct, FLOOR(salary / 1000.0) as salary_k FROM test_ns.employees ORDER BY id` |

#### Window Functions

| Test | SQL |
|---|---|
| `test_window_functions` | `SELECT name, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as row_num, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rnk FROM test_ns.employees WHERE dept_id IN (10, 20) ORDER BY dept_id, salary DESC` |
| `test_window_running_total` | `SELECT name, salary, SUM(salary) OVER (ORDER BY salary ROWS UNBOUNDED PRECEDING) as running_total FROM test_ns.employees ORDER BY salary` |

---

## Fixture Data

Most join/aggregation/view/window tests share these fixture tables (created fresh per test, torn down after):

**`test_ns.employees`**

| id | name    | dept_id | salary   |
|----|---------|---------|----------|
| 1  | Alice   | 10      | 90000.00 |
| 2  | Bob     | 10      | 85000.00 |
| 3  | Charlie | 20      | 70000.00 |
| 4  | Dave    | 20      | 75000.00 |
| 5  | Eve     | 30      | 95000.00 |
| 6  | Frank   | 99      | 60000.00 |

**`test_ns.departments`**

| id | dept_name   | budget     |
|----|-------------|------------|
| 10 | Engineering | 500000.00  |
| 20 | Marketing   | 200000.00  |
| 30 | Executive   | 1000000.00 |
| 40 | HR          | 150000.00  |
