#!/usr/bin/env bash
#
# scripts/start-spark-interop.sh -- bring up the Spark interop stack and
# wait for it to be query-ready.
#
# Starts Polaris + RustFS via docker-compose.test.yml if they are not
# already running, then layers docker-compose.spark.yml on top. Returns
# 0 once `spark-sql -e 'SELECT 1'` succeeds inside the Spark container.
# The first boot takes a minute or two because Spark downloads the Iceberg
# runtime and hadoop-aws bundle; later invocations hit the ivy cache.
#
# Usage:
#   ./scripts/start-spark-interop.sh
#   ./scripts/spark-interop.sh "SELECT * FROM rest.test_ns.t"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "${ROOT_DIR}"

TIMEOUT="${SPARK_INTEROP_TIMEOUT:-180}"
CONTAINER="${SPARK_CONTAINER:-sqe-spark-iceberg}"

echo "=== SQE Spark interop bootstrap ==="
echo "Root:      ${ROOT_DIR}"
echo "Timeout:   ${TIMEOUT}s"
echo ""

# Bring the stack up. --wait returns once services report healthy, which
# covers Polaris + RustFS. Spark's own readiness needs the spark-sql probe
# below because --packages downloads cannot be healthchecked quickly.
echo "Starting stack (Polaris + RustFS + Spark)..."
docker compose \
    -f docker-compose.test.yml \
    -f docker-compose.spark.yml \
    up -d

# Run the Polaris/RustFS bootstrap if the warehouse catalog and namespaces
# are not yet registered. Idempotent: the script is safe to re-run.
if [ -x "${SCRIPT_DIR}/bootstrap-test.sh" ]; then
    echo ""
    echo "Bootstrapping Polaris warehouse + namespaces..."
    "${SCRIPT_DIR}/bootstrap-test.sh"
fi

echo ""
echo -n "Waiting for Spark spark-sql readiness"
ELAPSED=0
INTERVAL=5
while : ; do
    if docker exec "${CONTAINER}" /opt/spark/bin/spark-sql \
        --conf spark.ui.enabled=false \
        -e 'SELECT 1' >/dev/null 2>&1; then
        echo " ready"
        break
    fi

    if [ "${ELAPSED}" -ge "${TIMEOUT}" ]; then
        echo ""
        echo "spark-interop: timed out after ${TIMEOUT}s" >&2
        echo "  Recent logs:" >&2
        docker logs --tail 50 "${CONTAINER}" >&2 || true
        exit 1
    fi

    echo -n "."
    sleep "${INTERVAL}"
    ELAPSED=$((ELAPSED + INTERVAL))
done

echo ""
echo "=== Spark interop ready ==="
echo "Submit SQL with: ./scripts/spark-interop.sh 'SELECT 1'"
