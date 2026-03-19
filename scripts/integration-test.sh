#!/usr/bin/env bash
set -euo pipefail

# Run integration tests against the lightweight test stack (Polaris in-memory + RustFS).
# Usage: ./scripts/integration-test.sh [filter]
# Example: ./scripts/integration-test.sh test_authentication      # single test by name
#          ./scripts/integration-test.sh test_sql_compat          # all SQL compat tests
#          ./scripts/integration-test.sh                          # run all

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.test.yml"

cd "$ROOT_DIR"

# ── Start test stack ──────────────────────────────────────────
echo "Starting test stack..."
docker compose -f "$COMPOSE_FILE" up -d

# ── Bootstrap (idempotent) ────────────────────────────────────
"$SCRIPT_DIR/bootstrap-test.sh"

# ── Run integration tests ─────────────────────────────────────
SQE_LOG_FILE="$(mktemp /tmp/sqe-test-XXXXXX.log)"

echo ""
echo "Running integration tests..."
# Runs all test binaries in sqe-coordinator (integration_test + sql_compat_test).
# Capture SQE coordinator tracing output (requires RUST_LOG to be set for structured logs)
RUST_LOG="${RUST_LOG:-sqe_coordinator=info,sqe_catalog=info,sqe_auth=info,warn}" \
    cargo test -p sqe-coordinator -- --ignored --test-threads=1 --nocapture "$@" 2>&1 \
    | tee "$SQE_LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "=== All integration tests passed ==="
else
    echo "=== Integration tests FAILED (exit $EXIT_CODE) ==="
fi

# ── Show service logs before teardown ─────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SQE Engine logs (last 200 lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
tail -200 "$SQE_LOG_FILE"
rm -f "$SQE_LOG_FILE"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Polaris logs (last 100 lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" logs --no-log-prefix --tail=100 polaris

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  RustFS logs (last 50 lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" logs --no-log-prefix --tail=50 rustfs

# ── Tear down test stack ───────────────────────────────────────
echo ""
echo "Tearing down test stack..."
docker compose -f "$COMPOSE_FILE" down

exit $EXIT_CODE
