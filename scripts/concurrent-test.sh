#!/usr/bin/env bash
set -euo pipefail

# Concurrent client load test for distributed SQE.
#
# Runs N parallel queries against the coordinator and reports timing/results.
#
# Requires:
#   docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml up --build -d
#   ./scripts/bootstrap-test.sh
#   # Create a test table first (the script will create one if missing)
#
# Usage:
#   ./scripts/concurrent-test.sh                  # 10 clients, default queries
#   ./scripts/concurrent-test.sh 20               # 20 concurrent clients
#   ./scripts/concurrent-test.sh 50 heavy         # 50 clients with heavy queries

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

NUM_CLIENTS="${1:-10}"
MODE="${2:-mixed}"       # mixed | heavy | light
SQE_HOST="localhost"
SQE_PORT="60051"
export SQE_PASSWORD="${SQE_PASSWORD:-}"

cd "$ROOT_DIR"
cargo build -p sqe-cli --release 2>/dev/null
CLI="$ROOT_DIR/target/release/sqe-cli"

run_sql() {
    "$CLI" --host "$SQE_HOST" --port "$SQE_PORT" --user root --protocol flight -e "$1" 2>/dev/null
}

# ── Ensure test table exists ──────────────────────────────────
echo "Checking test table..."
ROW_COUNT=$(run_sql "SELECT COUNT(*) AS cnt FROM test_warehouse.default.big" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo "0")

if [ "${ROW_COUNT:-0}" -lt 1000 ]; then
    echo "Creating 200K row test table (2 data files for distribution)..."
    run_sql "DROP TABLE IF EXISTS test_warehouse.default.big" >/dev/null 2>&1 || true
    run_sql "
    CREATE TABLE test_warehouse.default.big AS
    WITH d AS (SELECT * FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS t(d))
    SELECT CAST(d1.d*10000+d2.d*1000+d3.d*100+d4.d*10+d5.d AS BIGINT) AS id,
           CONCAT('user_',CAST(d1.d AS VARCHAR),CAST(d2.d AS VARCHAR)) AS name,
           CAST((d1.d+1)*(d2.d+1)*(d3.d+1) AS DOUBLE)*0.99 AS amount
    FROM d d1 CROSS JOIN d d2 CROSS JOIN d d3 CROSS JOIN d d4 CROSS JOIN d d5
    " >/dev/null
    run_sql "
    INSERT INTO test_warehouse.default.big
    WITH d AS (SELECT * FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS t(d))
    SELECT CAST(100000+d1.d*10000+d2.d*1000+d3.d*100+d4.d*10+d5.d AS BIGINT) AS id,
           CONCAT('extra_',CAST(d1.d AS VARCHAR),CAST(d2.d AS VARCHAR)) AS name,
           CAST((d1.d+1)*(d2.d+1) AS DOUBLE)*2.5 AS amount
    FROM d d1 CROSS JOIN d d2 CROSS JOIN d d3 CROSS JOIN d d4 CROSS JOIN d d5
    " >/dev/null
    echo "Done — 200K rows in 2 files."
else
    echo "Table exists with $ROW_COUNT rows."
fi

# ── Define query sets ─────────────────────────────────────────
LIGHT_QUERIES=(
    "SELECT COUNT(*) FROM test_warehouse.default.big"
    "SELECT 1"
    "SELECT MIN(amount), MAX(amount) FROM test_warehouse.default.big"
    "SELECT * FROM system.runtime.nodes"
    "SELECT COUNT(*) FROM system.runtime.queries"
)

HEAVY_QUERIES=(
    "SELECT COUNT(*), SUM(amount), AVG(amount) FROM test_warehouse.default.big"
    "SELECT SUBSTRING(name,1,5) AS p, COUNT(*) AS c, ROUND(AVG(amount),2) AS a FROM test_warehouse.default.big GROUP BY 1 ORDER BY c DESC"
    "SELECT id, name, amount FROM test_warehouse.default.big WHERE amount > 500 ORDER BY amount DESC LIMIT 100"
    "SELECT name, amount, RANK() OVER (ORDER BY amount DESC) AS rnk FROM test_warehouse.default.big WHERE amount > 800 LIMIT 20"
    "SELECT COUNT(DISTINCT name) FROM test_warehouse.default.big"
)

case "$MODE" in
    light) QUERIES=("${LIGHT_QUERIES[@]}") ;;
    heavy) QUERIES=("${HEAVY_QUERIES[@]}") ;;
    mixed) QUERIES=("${LIGHT_QUERIES[@]}" "${HEAVY_QUERIES[@]}") ;;
    *) echo "Unknown mode: $MODE (use: light, heavy, mixed)"; exit 1 ;;
esac

NUM_QUERIES=${#QUERIES[@]}
RESULTS_DIR=$(mktemp -d /tmp/sqe-concurrent-XXXXXX)

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Concurrent Load Test: $NUM_CLIENTS clients, $MODE mode"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── Launch concurrent clients ─────────────────────────────────
OVERALL_START=$(python3 -c "import time; print(int(time.time()*1000))")

for i in $(seq 1 "$NUM_CLIENTS"); do
    (
        # Pick a query round-robin from the set
        idx=$(( (i - 1) % NUM_QUERIES ))
        sql="${QUERIES[$idx]}"

        START=$(python3 -c "import time; print(int(time.time()*1000))")

        OUTPUT=$("$CLI" --host "$SQE_HOST" --port "$SQE_PORT" --user root \
            --protocol flight -e "$sql" 2>&1)
        EXIT_CODE=$?

        END=$(python3 -c "import time; print(int(time.time()*1000))")
        ELAPSED=$((END - START))

        if [ $EXIT_CODE -eq 0 ]; then
            ROWS=$(echo "$OUTPUT" | grep -c '^|' 2>/dev/null || echo "0")
            ROWS=$((ROWS > 0 ? ROWS - 1 : 0))  # subtract header
            echo "OK ${ELAPSED}ms ${ROWS}rows" > "$RESULTS_DIR/client-$i.txt"
        else
            ERROR=$(echo "$OUTPUT" | grep -oE 'Error:.*' | head -1 | cut -c1-80)
            echo "FAIL ${ELAPSED}ms $ERROR" > "$RESULTS_DIR/client-$i.txt"
        fi
    ) &
done

# Wait for all clients
wait

OVERALL_END=$(python3 -c "import time; print(int(time.time()*1000))")
OVERALL_ELAPSED=$((OVERALL_END - OVERALL_START))

# ── Collect results ───────────────────────────────────────────
PASS=0
FAIL=0
TOTAL_MS=0
MIN_MS=999999
MAX_MS=0

for f in "$RESULTS_DIR"/client-*.txt; do
    LINE=$(cat "$f")
    STATUS=$(echo "$LINE" | awk '{print $1}')
    MS=$(echo "$LINE" | awk '{print $2}' | sed 's/ms//')

    if [ "$STATUS" = "OK" ]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
    fi

    TOTAL_MS=$((TOTAL_MS + MS))
    if [ "$MS" -lt "$MIN_MS" ]; then MIN_MS=$MS; fi
    if [ "$MS" -gt "$MAX_MS" ]; then MAX_MS=$MS; fi
done

AVG_MS=$((TOTAL_MS / NUM_CLIENTS))
QPS=$(python3 -c "print(f'{$NUM_CLIENTS / ($OVERALL_ELAPSED / 1000.0):.1f}')")

# ── Show individual results ───────────────────────────────────
echo "  Client results:"
for i in $(seq 1 "$NUM_CLIENTS"); do
    LINE=$(cat "$RESULTS_DIR/client-$i.txt")
    printf "    client-%02d: %s\n" "$i" "$LINE"
done

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: $PASS pass, $FAIL fail / $NUM_CLIENTS total"
echo "  Wall time:  ${OVERALL_ELAPSED}ms"
echo "  Per-query:  min=${MIN_MS}ms  avg=${AVG_MS}ms  max=${MAX_MS}ms"
echo "  Throughput: ${QPS} queries/sec"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── Show system tables ────────────────────────────────────────
echo ""
echo "  Active queries during test:"
run_sql "SELECT COUNT(*) AS total_queries FROM system.runtime.queries" 2>/dev/null | grep -v "sqe-cli"
echo ""
echo "  Worker load distribution:"
run_sql "SELECT node_id, COUNT(*) AS fragments, SUM(output_rows) AS total_rows FROM system.runtime.tasks GROUP BY node_id ORDER BY fragments DESC" 2>/dev/null | grep -v "sqe-cli"

# Cleanup
rm -rf "$RESULTS_DIR"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
