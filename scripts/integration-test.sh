#!/usr/bin/env bash
set -euo pipefail

# Run integration tests against the lightweight test stack (Polaris in-memory + RustFS).
# Usage: ./scripts/integration-test.sh [cargo test args...]
# Example: ./scripts/integration-test.sh test_authentication
#          ./scripts/integration-test.sh                      # run all

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
echo ""
echo "Running integration tests..."
cargo test -p sqe-coordinator --test integration_test -- --ignored "$@"
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "=== All integration tests passed ==="
else
    echo "=== Integration tests FAILED (exit $EXIT_CODE) ==="
fi

exit $EXIT_CODE
