#!/usr/bin/env bash
set -euo pipefail
# Run the upstream Trino Iceberg product-tests via tempto, against SQE (default)
# or the real Trino baseline. Layers on the existing parity stack
# (docker-compose.test.yml + docker-compose.compare.yml).
#
# Usage:
#   scripts/tempto-test.sh                 # run allow-list against SQE
#   scripts/tempto-test.sh --baseline      # run allow-list against real Trino
#   scripts/tempto-test.sh --no-build      # skip the SQE image rebuild
#
# Requires: Docker. Everything else (Gradle, JDK, Caddy, the trino-product-tests
# jar) runs in containers. Reports land in testing/tempto/reports/.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose -f docker-compose.test.yml -f docker-compose.compare.yml -f docker-compose.tempto.yml)
CONFIG="/work/tempto-configuration.yaml"
TARGET="SQE (via TLS proxy)"
SVCS=(sqe tls-proxy)
READY_NAME="SQE"
READY_URL="http://localhost:28080/v1/info"
BUILD_FLAG="--build"
for arg in "$@"; do
  case "$arg" in
    --baseline)
      CONFIG="/work/tempto-configuration-baseline.yaml"; TARGET="real Trino (baseline)"
      SVCS=(trino); READY_NAME="Trino"; READY_URL="http://localhost:38080/v1/info" ;;
    --no-build) BUILD_FLAG="" ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

echo "=== Tempto Iceberg compatibility run -- target: $TARGET ==="
# rustfs (S3) is not in any depends_on chain; start it explicitly.
"${COMPOSE[@]}" up -d $BUILD_FLAG rustfs "${SVCS[@]}"

echo "Bootstrapping test stack (idempotent)..."
"$SCRIPT_DIR/bootstrap-test.sh"

echo "Waiting for $READY_NAME endpoint..."
timeout 90 bash -c "until curl -sf $READY_URL >/dev/null; do sleep 1; done" \
  || { echo "ERROR: $READY_NAME not reachable at $READY_URL"; exit 1; }

TESTS=$(grep -vE '^\s*#|^\s*$' testing/tempto/allowlist.txt | paste -sd, -)
echo "Running tempto allow-list: $TESTS"
set +e
"${COMPOSE[@]}" run --rm tempto-runner -q run \
  --args="--config $CONFIG --report-dir /work/reports --tests $TESTS"
RC=$?
set -e

echo ""
if [ $RC -eq 0 ]; then
  echo "RESULT: PASS (allow-list green) against $TARGET"
else
  echo "RESULT: FAIL (rc=$RC) against $TARGET -- see testing/tempto/reports/ and testing/tempto/exclusions.md"
fi
exit $RC
