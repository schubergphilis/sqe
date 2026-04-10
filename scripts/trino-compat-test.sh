#!/usr/bin/env bash
# scripts/trino-compat-test.sh
#
# Side-by-side SQL compatibility test: execute identical queries against
# SQE (Trino HTTP endpoint) and Trino, compare outputs.
#
# Requirements:
#   - SQE running on port 8080 (Trino HTTP) with Keycloak auth
#   - Trino running on port 38080 with same Polaris catalog
#   - trino-cli installed
#   - curl, python3
#
# Usage:
#   ./scripts/trino-compat-test.sh                    # run all tests
#   ./scripts/trino-compat-test.sh --category scalar  # run scalar tests only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration -- defaults to test stack (no OPA, simple auth)
SQE_HOST="${SQE_HOST:-http://localhost:28080}"
SQE_CATALOG="${SQE_CATALOG:-test_warehouse}"
TRINO_HOST="${TRINO_HOST:-http://localhost:38080}"
TRINO_USER="${TRINO_USER:-admin}"
TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
SCHEMA="${SCHEMA:-default}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
PASS=0
FAIL=0
DIFF=0
SKIP=0
TOTAL=0

# ── Get SQE auth token ───────────────────────────────────────────
get_sqe_token() {
    if [ -n "${SQE_TOKEN:-}" ]; then return; fi

    echo -n "Getting SQE auth token... "
    # Try test stack Polaris OAuth (simple, no Keycloak)
    SQE_TOKEN=$(curl -sf -X POST "http://localhost:18181/api/catalog/v1/oauth/tokens" \
        -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null)

    if [ -z "$SQE_TOKEN" ]; then
        # Fallback: Keycloak token from live stack
        SQE_TOKEN=$(docker exec sqe-sqe-1 sh -c \
            'curl -s -X POST "http://keycloak:8080/realms/iceberg/protocol/openid-connect/token" \
             -H "Content-Type: application/x-www-form-urlencoded" \
             -d "grant_type=password&client_id=polaris-frontend-client&username=root&password=root123"' \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
    fi
    echo "done (${#SQE_TOKEN} chars)"
}

# ── Query runners ─────────────────────────────────────────────────

# Execute SQL on SQE via Trino HTTP protocol, return CSV-like output
sqe_query() {
    local sql="$1"
    local result=$(curl -s -X POST "$SQE_HOST/v1/statement" \
        -u "${SQE_USER:-root}:${SQE_PASS:-s3cr3t}" \
        -H "X-Trino-User: ${SQE_USER:-root}" \
        -H "X-Trino-Catalog: $SQE_CATALOG" \
        -H "Content-Type: text/plain" \
        -d "$sql" 2>&1)

    local state=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('stats',{}).get('state','?'))" 2>/dev/null)

    if [ "$state" != "FINISHED" ]; then
        local err=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message','unknown error'))" 2>/dev/null)
        echo "ERROR: $err"
        return 1
    fi

    # Format output as CSV rows (matching Trino CSV_UNQUOTED)
    echo "$result" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for row in d.get('data', []):
    print(','.join(str(v) if v is not None else 'NULL' for v in row))
" 2>/dev/null
}

# Execute SQL on Trino via trino-cli
trino_query() {
    local sql="$1"
    # Redirect stderr to /dev/null to suppress Java warnings
    local result=$(trino --server "$TRINO_HOST" \
        --user "$TRINO_USER" \
        --catalog "$TRINO_CATALOG" \
        --schema "$SCHEMA" \
        --execute "$sql" \
        --output-format CSV_UNQUOTED 2>/dev/null)

    if echo "$result" | grep -q "^Query.*failed:\|^Error"; then
        echo "ERROR: $result"
        return 1
    fi

    echo "$result"
}

# ── Comparison framework ──────────────────────────────────────────

# Compare a query's output between SQE and Trino
compare_query() {
    local name="$1"
    local sqe_sql="$2"
    local trino_sql="${3:-$sqe_sql}"  # default: same SQL for both
    local expect_diff="${4:-no}"       # "yes" = expected to differ

    TOTAL=$((TOTAL + 1))

    # Run on SQE
    local sqe_out
    sqe_out=$(sqe_query "$sqe_sql" 2>&1) || true

    # Run on Trino
    local trino_out
    trino_out=$(trino_query "$trino_sql" 2>&1) || true

    # Normalize for comparison:
    # - Trim whitespace
    # - Remove trailing .0 from integers (SQE returns 4.0, Trino returns 4)
    # - Normalize NULL (SQE=NULL, Trino=empty string)
    # - Normalize timestamp precision (strip trailing zeros/nanoseconds)
    # - Sort for order-independent comparison
    normalize() {
        sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | \
        sed 's/\.0\b//g' | \
        sed 's/\.000000000//g; s/\.000000//g; s/\.000//g' | \
        sed 's/ 00:00:00$//g' | \
        sed '/^$/d' | \
        sort
    }
    local sqe_norm=$(echo "$sqe_out" | normalize)
    local trino_norm=$(echo "$trino_out" | normalize)

    # Handle NULL normalization: SQE returns "NULL", Trino returns empty
    if [ "$sqe_norm" = "NULL" ] && [ -z "$trino_norm" ]; then
        printf "  ${GREEN}PASS${NC} %-45s (NULL match)\n" "$name"
        PASS=$((PASS + 1))
        return
    fi

    # Normalize NULL in multi-column rows (e.g., "Alice,NULL" vs "Alice,")
    sqe_norm=$(echo "$sqe_norm" | sed 's/,NULL/,/g; s/NULL,/,/g')
    trino_norm=$(echo "$trino_norm" | sed 's/,$/,/g')

    # Check for errors
    if echo "$sqe_out" | grep -q "^ERROR:"; then
        if echo "$trino_out" | grep -q "^ERROR:"; then
            printf "  ${YELLOW}SKIP${NC} %-45s (both engines error)\n" "$name"
            SKIP=$((SKIP + 1))
            return
        fi
        printf "  ${RED}FAIL${NC} %-45s SQE error: %s\n" "$name" "$(echo "$sqe_out" | head -1)"
        FAIL=$((FAIL + 1))
        return
    fi

    if echo "$trino_out" | grep -q "^ERROR:"; then
        printf "  ${YELLOW}SKIP${NC} %-45s Trino error: %s\n" "$name" "$(echo "$trino_out" | head -1)"
        SKIP=$((SKIP + 1))
        return
    fi

    # Compare outputs
    if [ "$sqe_norm" = "$trino_norm" ]; then
        printf "  ${GREEN}PASS${NC} %-45s\n" "$name"
        PASS=$((PASS + 1))
    elif [ "$expect_diff" = "yes" ]; then
        printf "  ${BLUE}DIFF${NC} %-45s (expected difference)\n" "$name"
        DIFF=$((DIFF + 1))
    else
        printf "  ${RED}FAIL${NC} %-45s\n" "$name"
        echo "       SQE:   $(echo "$sqe_out" | head -1)"
        echo "       Trino: $(echo "$trino_out" | head -1)"
        FAIL=$((FAIL + 1))
    fi
}

# ══════════════════════════════════════════════════════════════════
# TEST CASES
# ══════════════════════════════════════════════════════════════════

run_scalar_tests() {
    echo ""
    echo "=== Scalar Functions ==="

    # String
    compare_query "upper()" "SELECT upper('hello')"
    compare_query "lower()" "SELECT lower('HELLO')"
    compare_query "length()" "SELECT length('hello')"
    compare_query "trim()" "SELECT trim('  hello  ')"
    compare_query "concat()" "SELECT concat('hello', ' ', 'world')"
    compare_query "replace()" "SELECT replace('hello', 'l', 'r')"
    compare_query "substr()" "SELECT substr('hello world', 1, 5)"
    compare_query "reverse()" "SELECT reverse('hello')"
    compare_query "position()" "SELECT position('lo' IN 'hello')"

    # Math
    compare_query "abs()" "SELECT abs(-42)"
    compare_query "ceil()" "SELECT ceil(3.14)"
    compare_query "floor()" "SELECT floor(3.14)"
    compare_query "round()" "SELECT round(3.14159, 2)"
    compare_query "power()" "SELECT power(2, 10)"
    compare_query "sqrt()" "SELECT sqrt(16.0)"
    compare_query "pi()" "SELECT pi()"

    # Conditional
    compare_query "CASE WHEN" "SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END"
    compare_query "COALESCE" "SELECT COALESCE(NULL, 42)"
    compare_query "NULLIF" "SELECT NULLIF(1, 1)"
    compare_query "GREATEST" "SELECT GREATEST(1, 2, 3)"
    compare_query "LEAST" "SELECT LEAST(1, 2, 3)"
    compare_query "TRY_CAST" "SELECT TRY_CAST('abc' AS INTEGER)"
}

run_datetime_tests() {
    echo ""
    echo "=== Date/Time Functions ==="

    compare_query "year()" \
        "SELECT year(CAST('2024-03-15' AS DATE))" \
        "SELECT year(DATE '2024-03-15')"
    compare_query "month()" \
        "SELECT month(CAST('2024-03-15' AS DATE))" \
        "SELECT month(DATE '2024-03-15')"
    compare_query "day()" \
        "SELECT day(CAST('2024-03-15' AS DATE))" \
        "SELECT day(DATE '2024-03-15')"
    compare_query "day_of_week()" \
        "SELECT day_of_week(CAST('2024-03-15' AS DATE))" \
        "SELECT day_of_week(DATE '2024-03-15')"
    compare_query "day_of_year()" \
        "SELECT day_of_year(CAST('2024-03-15' AS DATE))" \
        "SELECT day_of_year(DATE '2024-03-15')"
    compare_query "quarter()" \
        "SELECT quarter(CAST('2024-08-15' AS DATE))" \
        "SELECT quarter(DATE '2024-08-15')"
    compare_query "date_trunc()" \
        "SELECT date_trunc('month', CAST('2024-03-15' AS DATE))" \
        "SELECT date_trunc('month', DATE '2024-03-15')"
}

run_json_tests() {
    echo ""
    echo "=== JSON Functions ==="

    compare_query "json_extract_scalar()" \
        "SELECT json_extract_scalar('{\"name\":\"alice\",\"age\":30}', '\$.name')" \
        "SELECT json_extract_scalar('{\"name\":\"alice\",\"age\":30}', '\$.name')"
    compare_query "json_array_length()" \
        "SELECT json_array_length('[1,2,3,4,5]')"
    compare_query "json_format()" \
        "SELECT json_format('{\"a\":1}')" \
        "SELECT json_format(JSON '{\"a\":1}')" \
        "yes"  # Trino requires JSON type cast
}

run_table_tests() {
    echo ""
    echo "=== Table Queries (compat_test) ==="

    # Both engines use schema-qualified names for SQE, bare names for Trino (schema set in session)
    compare_query "SELECT count(*)" \
        "SELECT count(*) FROM default.compat_test" \
        "SELECT count(*) FROM compat_test"
    compare_query "SELECT with WHERE" \
        "SELECT name, amount FROM default.compat_test WHERE amount > 100 ORDER BY name" \
        "SELECT name, amount FROM compat_test WHERE amount > 100 ORDER BY name"
    compare_query "GROUP BY" \
        "SELECT name, sum(amount) FROM default.compat_test GROUP BY name ORDER BY name" \
        "SELECT name, sum(amount) FROM compat_test GROUP BY name ORDER BY name"
    compare_query "ORDER BY LIMIT" \
        "SELECT name, amount FROM default.compat_test ORDER BY amount DESC LIMIT 3" \
        "SELECT name, amount FROM compat_test ORDER BY amount DESC LIMIT 3"
    compare_query "DISTINCT" \
        "SELECT DISTINCT created_date FROM default.compat_test ORDER BY created_date" \
        "SELECT DISTINCT created_date FROM compat_test ORDER BY created_date"
}

run_aggregate_tests() {
    echo ""
    echo "=== Aggregate Functions ==="

    compare_query "count(*)" \
        "SELECT count(*) FROM default.compat_test" \
        "SELECT count(*) FROM compat_test"
    compare_query "sum()" \
        "SELECT sum(amount) FROM default.compat_test" \
        "SELECT sum(amount) FROM compat_test"
    compare_query "avg()" \
        "SELECT avg(amount) FROM default.compat_test" \
        "SELECT avg(amount) FROM compat_test" \
        "yes"  # precision may differ
    compare_query "min() / max()" \
        "SELECT min(amount), max(amount) FROM default.compat_test" \
        "SELECT min(amount), max(amount) FROM compat_test"
    compare_query "count(DISTINCT)" \
        "SELECT count(DISTINCT name) FROM default.compat_test" \
        "SELECT count(DISTINCT name) FROM compat_test"
}

run_window_tests() {
    echo ""
    echo "=== Window Functions ==="

    compare_query "row_number()" \
        "SELECT name, row_number() OVER (ORDER BY amount DESC) AS rn FROM default.compat_test" \
        "SELECT name, row_number() OVER (ORDER BY amount DESC) AS rn FROM compat_test"
    compare_query "rank()" \
        "SELECT name, rank() OVER (ORDER BY amount DESC) AS rnk FROM default.compat_test" \
        "SELECT name, rank() OVER (ORDER BY amount DESC) AS rnk FROM compat_test"
    compare_query "lag()" \
        "SELECT name, lag(amount) OVER (ORDER BY id) AS prev_amount FROM default.compat_test" \
        "SELECT name, lag(amount) OVER (ORDER BY id) AS prev_amount FROM compat_test"
}

run_ddl_tests() {
    echo ""
    echo "=== DDL/DML ==="

    compare_query "SHOW SCHEMAS" \
        "SHOW SCHEMAS IN test_warehouse" \
        "SHOW SCHEMAS IN iceberg"
    compare_query "SHOW TABLES" \
        "SHOW TABLES IN test_warehouse.default" \
        "SHOW TABLES IN iceberg.default"
}

# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

echo "============================================="
echo "  SQE vs Trino SQL Compatibility Test"
echo "============================================="
echo "SQE:   $SQE_HOST (catalog: $SQE_CATALOG)"
echo "Trino: $TRINO_HOST (catalog: $TRINO_CATALOG)"
echo "Schema: $SCHEMA"
echo ""

get_sqe_token

CATEGORY="${1:-all}"

case "$CATEGORY" in
    --category)
        shift
        case "${1:-all}" in
            scalar) run_scalar_tests ;;
            datetime) run_datetime_tests ;;
            json) run_json_tests ;;
            table) run_table_tests ;;
            aggregate) run_aggregate_tests ;;
            window) run_window_tests ;;
            ddl) run_ddl_tests ;;
            *) echo "Unknown category: $1" ;;
        esac
        ;;
    *)
        run_scalar_tests
        run_datetime_tests
        run_json_tests
        run_table_tests
        run_aggregate_tests
        run_window_tests
        run_ddl_tests
        ;;
esac

# ── Summary ───────────────────────────────────────────────────────
echo ""
echo "============================================="
printf "  PASS: ${GREEN}%d${NC}  FAIL: ${RED}%d${NC}  DIFF: ${BLUE}%d${NC}  SKIP: ${YELLOW}%d${NC}  TOTAL: %d\n" \
    "$PASS" "$FAIL" "$DIFF" "$SKIP" "$TOTAL"
echo "============================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
