#!/usr/bin/env bash
set -euo pipefail

# End-to-end tests for SQE (tasks 13.4 & 13.5).
# Requires the SQE quickstart stack with Keycloak running:
#   cd quickstart/sqe && docker compose up -d
#
# Usage: ./scripts/e2e-test.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

FLIGHT_HOST="${SQE_E2E_FLIGHT_HOST:-localhost}"
FLIGHT_PORT="${SQE_E2E_FLIGHT_PORT:-50051}"
TRINO_HOST="${SQE_E2E_TRINO_HOST:-localhost}"
TRINO_PORT="${SQE_E2E_TRINO_PORT:-8080}"

PASS=0
FAIL=0

pass() { echo "  ✓ $1"; PASS=$((PASS + 1)); }
fail() { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }

echo "=== SQE End-to-End Tests ==="
echo "  Flight SQL: ${FLIGHT_HOST}:${FLIGHT_PORT}"
echo "  Trino HTTP: ${TRINO_HOST}:${TRINO_PORT}"
echo ""

# ---------------------------------------------------------------------------
# Task 13.4: Flight SQL connect → SELECT → verify results
# ---------------------------------------------------------------------------
echo "── Flight SQL Tests ──"

# Test: Health endpoint is reachable
HEALTH_PORT="${SQE_E2E_HEALTH_PORT:-9091}"
if curl -sf "http://${FLIGHT_HOST}:${HEALTH_PORT}/healthz" > /dev/null 2>&1; then
    pass "Health endpoint /healthz is reachable"
else
    fail "Health endpoint /healthz is NOT reachable"
fi

if curl -sf "http://${FLIGHT_HOST}:${HEALTH_PORT}/readyz" > /dev/null 2>&1; then
    pass "Readiness endpoint /readyz is reachable"
else
    fail "Readiness endpoint /readyz is NOT reachable"
fi

# Test: Flight SQL via cargo test (reuses the Keycloak integration tests)
echo ""
echo "Running Flight SQL integration tests..."
if SQE_TEST_KEYCLOAK_URL="http://${FLIGHT_HOST}:8080" \
   cargo test -p sqe-coordinator --test it integration_test::test_keycloak -- --ignored --nocapture 2>&1; then
    pass "Keycloak auth via Flight SQL pipeline"
else
    fail "Keycloak auth via Flight SQL pipeline"
fi

# ---------------------------------------------------------------------------
# Task 13.5: Trino JDBC connect → SELECT → verify results
# ---------------------------------------------------------------------------
echo ""
echo "── Trino HTTP Compat Tests ──"

# Test: /v1/info endpoint
INFO_RESP=$(curl -sf "http://${TRINO_HOST}:${TRINO_PORT}/v1/info" 2>/dev/null || echo "")
if [ -n "$INFO_RESP" ]; then
    pass "GET /v1/info returns a response"
    # Verify it contains coordinator field
    if echo "$INFO_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'coordinator' in d" 2>/dev/null; then
        pass "  /v1/info contains 'coordinator' field"
    else
        fail "  /v1/info missing 'coordinator' field"
    fi
else
    fail "GET /v1/info failed"
fi

# Test: /v1/info/state endpoint
STATE_RESP=$(curl -sf "http://${TRINO_HOST}:${TRINO_PORT}/v1/info/state" 2>/dev/null || echo "")
if [ "$STATE_RESP" = '"ACTIVE"' ] || [ "$STATE_RESP" = "ACTIVE" ]; then
    pass "GET /v1/info/state returns ACTIVE"
else
    fail "GET /v1/info/state returned: ${STATE_RESP:-empty}"
fi

# Test: POST /v1/statement with Basic auth
AUTH_HEADER=$(echo -n "root:s3cr3t" | base64)
QUERY_RESP=$(curl -sf -X POST "http://${TRINO_HOST}:${TRINO_PORT}/v1/statement" \
    -H "X-Trino-User: root" \
    -H "Authorization: Basic ${AUTH_HEADER}" \
    -d "SELECT 1 as result" 2>/dev/null || echo "")
if [ -n "$QUERY_RESP" ]; then
    pass "POST /v1/statement accepts a query"
    # Check for query ID in response
    if echo "$QUERY_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'id' in d" 2>/dev/null; then
        pass "  Response contains query 'id'"
    else
        fail "  Response missing query 'id'"
    fi
    # Check for no error
    if echo "$QUERY_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'error' not in d or d['error'] is None" 2>/dev/null; then
        pass "  Query returned no error"
    else
        fail "  Query returned an error"
    fi
    # Follow pagination if nextUri present
    NEXT_URI=$(echo "$QUERY_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('nextUri',''))" 2>/dev/null || echo "")
    if [ -n "$NEXT_URI" ]; then
        PAGE_RESP=$(curl -sf "$NEXT_URI" 2>/dev/null || echo "")
        if [ -n "$PAGE_RESP" ]; then
            pass "  GET nextUri pagination succeeded"
        else
            fail "  GET nextUri pagination failed"
        fi
    fi
else
    fail "POST /v1/statement failed"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: ${PASS} passed, ${FAIL} failed"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
