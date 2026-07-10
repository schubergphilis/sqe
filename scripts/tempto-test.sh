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
#   scripts/tempto-test.sh --rest          # run the full iceberg REST-catalog
#                                          # group (Hive-metastore/Spark classes
#                                          # excluded) instead of the allow-list
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
MODE="allowlist"
for arg in "$@"; do
  case "$arg" in
    --baseline)
      CONFIG="/work/tempto-configuration-baseline.yaml"; TARGET="real Trino (baseline)"
      SVCS=(trino); READY_NAME="Trino"; READY_URL="http://localhost:38080/v1/info" ;;
    --no-build) BUILD_FLAG="" ;;
    --rest) MODE="rest" ;;
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

set +e
if [ "$MODE" = "rest" ]; then
  # Full iceberg REST-catalog run: everything in the `iceberg` group except the
  # Hive-metastore/Spark/HDFS-coupled classes (see exclude-hive-spark.txt) and
  # the `hms_only` group. This is the broad compatibility sweep against SQE's
  # Polaris REST catalog with the environmental (metastore) noise removed.
  EXCLUDED=$(grep -vE '^\s*#|^\s*$' testing/tempto/exclude-hive-spark.txt | paste -sd, -)
  echo "Running tempto iceberg REST-catalog group (excluding Hive/Spark classes)"
  "${COMPOSE[@]}" run --rm tempto-runner -q run \
    --args="--config $CONFIG --report-dir /work/reports --groups iceberg --excluded-groups hms_only --excluded-tests $EXCLUDED"
  RC=$?
else
  TESTS=$(grep -vE '^\s*#|^\s*$' testing/tempto/allowlist.txt | paste -sd, -)
  echo "Running tempto allow-list: $TESTS"
  "${COMPOSE[@]}" run --rm tempto-runner -q run \
    --args="--config $CONFIG --report-dir /work/reports --tests $TESTS"
  RC=$?
fi
set -e

echo ""
if [ $RC -eq 0 ]; then
  echo "RESULT: PASS against $TARGET ($MODE)"
else
  echo "RESULT: FAIL (rc=$RC) against $TARGET ($MODE) -- see testing/tempto/reports/ and testing/tempto/exclusions.md"
fi
exit $RC
