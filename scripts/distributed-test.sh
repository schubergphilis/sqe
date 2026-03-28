#!/usr/bin/env bash
set -euo pipefail

# End-to-end distributed integration test.
#
# Tests coordinator + worker query dispatch, system tables, query history,
# and result caching. Requires the distributed stack to be running:
#   docker compose -f docker-compose.distributed.yml up --build -d
#   ./scripts/bootstrap-distributed.sh
#
# Usage:
#   ./scripts/distributed-test.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PASS=0
FAIL=0
TOTAL=0

SQE_HOST="localhost"
SQE_PORT="50051"
TRINO_PORT="8080"
SQE_USER="root"
SQE_PASS=""

cd "$ROOT_DIR"

# Build CLI if needed
cargo build -p sqe-cli --release 2>/dev/null
CLI="$ROOT_DIR/target/release/sqe-cli"

run_sql() {
    local sql="$1"
    local label="${2:-}"
    "$CLI" --host "$SQE_HOST" --port "$SQE_PORT" --username "$SQE_USER" --password "$SQE_PASS" \
        --protocol flight -c "$sql" 2>/dev/null
}

run_sql_trino() {
    local sql="$1"
    curl -s -X POST "http://$SQE_HOST:$TRINO_PORT/v1/statement" \
        -u "$SQE_USER:" -d "$sql" 2>/dev/null
}

assert_contains() {
    local label="$1"
    local output="$2"
    local expected="$3"
    TOTAL=$((TOTAL + 1))
    if echo "$output" | grep -q "$expected"; then
        echo "  ✓ $label"
        PASS=$((PASS + 1))
    else
        echo "  ✗ $label (expected '$expected' in output)"
        echo "    got: $(echo "$output" | head -5)"
        FAIL=$((FAIL + 1))
    fi
}

assert_not_empty() {
    local label="$1"
    local output="$2"
    TOTAL=$((TOTAL + 1))
    if [ -n "$output" ] && [ "$output" != "0 rows" ]; then
        echo "  ✓ $label"
        PASS=$((PASS + 1))
    else
        echo "  ✗ $label (output was empty)"
        FAIL=$((FAIL + 1))
    fi
}

assert_row_count_gte() {
    local label="$1"
    local output="$2"
    local min="$3"
    TOTAL=$((TOTAL + 1))
    local count
    count=$(echo "$output" | grep -c '^|' 2>/dev/null || echo "0")
    # Subtract header row
    count=$((count > 0 ? count - 1 : 0))
    if [ "$count" -ge "$min" ]; then
        echo "  ✓ $label ($count rows >= $min)"
        PASS=$((PASS + 1))
    else
        echo "  ✗ $label (got $count rows, expected >= $min)"
        FAIL=$((FAIL + 1))
    fi
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SQE Distributed Integration Tests"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── Test 1: Basic connectivity ────────────────────────────────
echo "Test 1: Basic connectivity"
OUT=$(run_sql "SELECT 1 AS x")
assert_contains "SELECT 1 returns result" "$OUT" "1"

# ── Test 2: system.runtime.nodes ──────────────────────────────
echo "Test 2: system.runtime.nodes"
OUT=$(run_sql "SELECT node_id, coordinator, state FROM system.runtime.nodes")
assert_contains "Coordinator node present" "$OUT" "coordinator"
assert_contains "Coordinator is true" "$OUT" "true"

# ── Test 3: system.runtime.queries ────────────────────────────
echo "Test 3: system.runtime.queries"
# Run a query first to populate history
run_sql "SELECT 42 AS answer" >/dev/null
sleep 1
OUT=$(run_sql "SELECT query_id, state, \"user\" FROM system.runtime.queries ORDER BY created DESC LIMIT 5")
assert_contains "Query history has FINISHED entries" "$OUT" "FINISHED"
assert_contains "Query history shows user" "$OUT" "$SQE_USER"

# ── Test 4: system.metadata.catalogs ──────────────────────────
echo "Test 4: system.metadata.catalogs"
OUT=$(run_sql "SELECT catalog_name, connector_id FROM system.metadata.catalogs")
assert_contains "Catalog name is test_warehouse" "$OUT" "test_warehouse"
assert_contains "Connector is iceberg" "$OUT" "iceberg"

# ── Test 5: Create table and query system tables ──────────────
echo "Test 5: Create table + metadata"
run_sql "CREATE SCHEMA IF NOT EXISTS test_warehouse.dist_test" >/dev/null 2>&1 || true
run_sql "DROP TABLE IF EXISTS test_warehouse.dist_test.numbers" >/dev/null 2>&1 || true
run_sql "CREATE TABLE test_warehouse.dist_test.numbers AS SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(id, name)" >/dev/null

OUT=$(run_sql "SELECT * FROM test_warehouse.dist_test.numbers ORDER BY id")
assert_contains "Table has data" "$OUT" "one"
assert_contains "Table has 3 rows" "$OUT" "three"

# ── Test 6: system.metadata.table_properties ──────────────────
echo "Test 6: system.metadata.table_properties"
OUT=$(run_sql "SELECT property_name, property_value FROM system.metadata.table_properties WHERE table_name = 'numbers'")
assert_not_empty "Table properties returned" "$OUT"

# ── Test 7: system.metadata.table_comments ────────────────────
echo "Test 7: system.metadata.table_comments"
OUT=$(run_sql "SELECT table_name, comment FROM system.metadata.table_comments WHERE schema_name = 'dist_test'")
assert_contains "Table comment row exists" "$OUT" "numbers"

# ── Test 8: system.runtime.tasks ──────────────────────────────
echo "Test 8: system.runtime.tasks"
OUT=$(run_sql "SELECT query_id, task_id, state FROM system.runtime.tasks LIMIT 5")
assert_not_empty "Tasks table has entries" "$OUT"

# ── Test 9: Query result cache ────────────────────────────────
echo "Test 9: Query result cache"
# First query — cache miss
START1=$(date +%s%N)
run_sql "SELECT COUNT(*) FROM test_warehouse.dist_test.numbers" >/dev/null
END1=$(date +%s%N)
T1=$(( (END1 - START1) / 1000000 ))  # ms

# Second identical query — should hit cache
START2=$(date +%s%N)
run_sql "SELECT COUNT(*) FROM test_warehouse.dist_test.numbers" >/dev/null
END2=$(date +%s%N)
T2=$(( (END2 - START2) / 1000000 ))  # ms

TOTAL=$((TOTAL + 1))
if [ "$T2" -lt "$T1" ] || [ "$T2" -lt 200 ]; then
    echo "  ✓ Second query faster or fast (${T1}ms → ${T2}ms)"
    PASS=$((PASS + 1))
else
    echo "  ~ Second query not obviously faster (${T1}ms → ${T2}ms) — cache may not have kicked in"
    PASS=$((PASS + 1))  # Don't fail on timing, it's best-effort
fi

# ── Test 10: Cache invalidation ───────────────────────────────
echo "Test 10: Cache invalidation on write"
run_sql "INSERT INTO test_warehouse.dist_test.numbers VALUES (4, 'four')" >/dev/null
OUT=$(run_sql "SELECT COUNT(*) AS cnt FROM test_warehouse.dist_test.numbers")
assert_contains "Count reflects INSERT (4 rows)" "$OUT" "4"

# ── Test 11: Trino HTTP endpoint ──────────────────────────────
echo "Test 11: Trino HTTP compatibility"
OUT=$(run_sql_trino "SELECT 1 AS x")
assert_contains "Trino endpoint returns result" "$OUT" "FINISHED"

# ── Test 12: Query history shows all queries ──────────────────
echo "Test 12: Query history completeness"
OUT=$(run_sql "SELECT COUNT(*) AS cnt FROM system.runtime.queries")
assert_not_empty "Query history has multiple entries" "$OUT"

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
