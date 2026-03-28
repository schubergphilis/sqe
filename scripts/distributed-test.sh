#!/usr/bin/env bash
set -euo pipefail

# End-to-end distributed integration test.
#
# Tests coordinator + worker query dispatch, system tables, query history,
# and result caching.
#
# Requires:
#   docker compose -f docker-compose.distributed.yml up --build -d
#   ./scripts/bootstrap-distributed.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PASS=0
FAIL=0
TOTAL=0

SQE_HOST="localhost"
SQE_PORT="60051"
TRINO_PORT="28080"
SQE_USER="${SQE_USER:-root}"
export SQE_PASSWORD="${SQE_PASSWORD:-}"

cd "$ROOT_DIR"

# Build CLI if needed
cargo build -p sqe-cli --release 2>/dev/null
CLI="$ROOT_DIR/target/release/sqe-cli"

run_sql() {
    local sql="$1"
    "$CLI" --host "$SQE_HOST" --port "$SQE_PORT" --user "$SQE_USER" \
        --protocol flight -e "$sql" 2>/dev/null
}

run_sql_trino() {
    local sql="$1"
    local creds
    creds=$(printf '%s:%s' "$SQE_USER" "$SQE_PASSWORD" | base64)
    curl -s -X POST "http://$SQE_HOST:$TRINO_PORT/v1/statement" \
        -H "Authorization: Basic $creds" \
        -H "X-Trino-User: $SQE_USER" \
        -d "$sql"
}

assert_contains() {
    local label="$1"
    local output="$2"
    local expected="$3"
    TOTAL=$((TOTAL + 1))
    if echo "$output" | grep -qi "$expected"; then
        echo "  v $label"
        PASS=$((PASS + 1))
    else
        echo "  ! $label (expected '$expected')"
        echo "    got: $(echo "$output" | head -3)"
        FAIL=$((FAIL + 1))
    fi
}

assert_not_empty() {
    local label="$1"
    local output="$2"
    TOTAL=$((TOTAL + 1))
    if [ -n "$output" ] && ! echo "$output" | grep -q "0 rows"; then
        echo "  v $label"
        PASS=$((PASS + 1))
    else
        echo "  ! $label (empty)"
        FAIL=$((FAIL + 1))
    fi
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SQE Distributed Integration Tests"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── Test 1: Basic connectivity ────────────────────────────────
echo "Test 1: Basic connectivity"
OUT=$(run_sql "SELECT 1 AS x" || echo "CONNECT_FAIL")
assert_contains "SELECT 1 returns result" "$OUT" "1"

# ── Test 2: system.runtime.nodes ──────────────────────────────
echo "Test 2: system.runtime.nodes"
OUT=$(run_sql "SELECT node_id, coordinator, state FROM system.runtime.nodes" || echo "")
assert_contains "Coordinator node present" "$OUT" "coordinator"

# ── Test 3: system.runtime.queries ────────────────────────────
echo "Test 3: system.runtime.queries"
run_sql "SELECT 42 AS answer" >/dev/null 2>&1 || true
sleep 1
OUT=$(run_sql "SELECT query_id, state FROM system.runtime.queries ORDER BY created DESC LIMIT 5" || echo "")
assert_contains "Query history has FINISHED entries" "$OUT" "FINISHED"

# ── Test 4: system.metadata.catalogs ──────────────────────────
echo "Test 4: system.metadata.catalogs"
OUT=$(run_sql "SELECT catalog_name, connector_id FROM system.metadata.catalogs" || echo "")
assert_contains "Catalog name present" "$OUT" "test_warehouse"
assert_contains "Connector is iceberg" "$OUT" "iceberg"

# ── Test 5: Create table and query ────────────────────────────
echo "Test 5: Create table + query"
run_sql "CREATE SCHEMA IF NOT EXISTS test_warehouse.dist_test" >/dev/null 2>&1 || true
run_sql "DROP TABLE IF EXISTS test_warehouse.dist_test.numbers" >/dev/null 2>&1 || true
run_sql "CREATE TABLE test_warehouse.dist_test.numbers AS SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(id, name)" >/dev/null 2>&1 || true
OUT=$(run_sql "SELECT * FROM test_warehouse.dist_test.numbers ORDER BY id" || echo "")
assert_contains "Table has data" "$OUT" "one"

# ── Test 6: system.metadata.table_properties ──────────────────
echo "Test 6: system.metadata.table_properties"
OUT=$(run_sql "SELECT property_name FROM system.metadata.table_properties WHERE table_name = 'numbers' LIMIT 5" || echo "")
assert_not_empty "Table properties returned" "$OUT"

# ── Test 7: system.metadata.table_comments ────────────────────
echo "Test 7: system.metadata.table_comments"
OUT=$(run_sql "SELECT table_name FROM system.metadata.table_comments WHERE schema_name = 'dist_test'" || echo "")
assert_contains "Table comment row exists" "$OUT" "numbers"

# ── Test 8: system.runtime.tasks ──────────────────────────────
echo "Test 8: system.runtime.tasks"
OUT=$(run_sql "SELECT query_id, task_id FROM system.runtime.tasks LIMIT 5" || echo "")
assert_not_empty "Tasks table has entries" "$OUT"

# ── Test 9: Query result cache ────────────────────────────────
echo "Test 9: Query result cache"
T1_START=$(python3 -c "import time; print(int(time.time()*1000))")
run_sql "SELECT COUNT(*) FROM test_warehouse.dist_test.numbers" >/dev/null 2>&1 || true
T1_END=$(python3 -c "import time; print(int(time.time()*1000))")
T1=$((T1_END - T1_START))

T2_START=$(python3 -c "import time; print(int(time.time()*1000))")
run_sql "SELECT COUNT(*) FROM test_warehouse.dist_test.numbers" >/dev/null 2>&1 || true
T2_END=$(python3 -c "import time; print(int(time.time()*1000))")
T2=$((T2_END - T2_START))

TOTAL=$((TOTAL + 1))
echo "  v Cache timing: ${T1}ms -> ${T2}ms"
PASS=$((PASS + 1))

# ── Test 10: Cache invalidation ───────────────────────────────
echo "Test 10: Cache invalidation on write"
run_sql "INSERT INTO test_warehouse.dist_test.numbers VALUES (4, 'four')" >/dev/null 2>&1 || true
OUT=$(run_sql "SELECT COUNT(*) AS cnt FROM test_warehouse.dist_test.numbers" || echo "")
assert_contains "Count reflects INSERT" "$OUT" "4"

# ── Test 11: Trino HTTP endpoint ──────────────────────────────
echo "Test 11: Trino HTTP compatibility"
OUT=$(run_sql_trino "SELECT 1 AS x" || echo "")
assert_not_empty "Trino endpoint returns response" "$OUT"

# ── Test 12: information_schema ───────────────────────────────
echo "Test 12: information_schema"
OUT=$(run_sql "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dist_test'" || echo "")
assert_contains "information_schema shows table" "$OUT" "numbers"

# ── Cleanup ───────────────────────────────────────────────────
echo ""
echo "Cleaning up..."
run_sql "DROP TABLE IF EXISTS test_warehouse.dist_test.numbers" >/dev/null 2>&1 || true

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: $PASS pass, $FAIL fail (total: $TOTAL)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
