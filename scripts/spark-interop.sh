#!/usr/bin/env bash
#
# scripts/spark-interop.sh -- run one SQL statement in the Spark container
# and print results as TSV on stdout. The Rust integration test shells out
# here so the test code stays language-agnostic.
#
# Usage:
#   ./scripts/spark-interop.sh "SELECT COUNT(*) FROM rest.test_ns.t"
#   echo "SELECT * FROM rest.test_ns.t ORDER BY id" | ./scripts/spark-interop.sh
#
# Exit codes:
#   0  Success, rows on stdout as TSV (columns tab-separated, one row per line).
#   2  Spark container is not up; run ./scripts/start-spark-interop.sh first.
#   3  spark-sql returned a non-zero exit status.

set -euo pipefail

CONTAINER="${SPARK_CONTAINER:-sqe-spark-iceberg}"

# Accept SQL on argv or stdin. argv takes priority if both are set.
if [ "$#" -ge 1 ] && [ -n "$1" ]; then
    SQL="$1"
else
    SQL="$(cat)"
fi

if [ -z "${SQL// }" ]; then
    echo "spark-interop: empty SQL input" >&2
    exit 2
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "spark-interop: container ${CONTAINER} is not running." >&2
    echo "  Bring it up with: ./scripts/start-spark-interop.sh" >&2
    exit 2
fi

# spark-sql's default output is "column\tcolumn\t...\n". That is already
# TSV, but it also prints a SLF4J banner and query timings on stderr.
# Drop those by only forwarding stdout to the caller.
#
# --conf overrides silence the transient Spark UI on localhost:4040 so the
# test harness does not leak open ports across invocations.
OUT=$(docker exec -i "${CONTAINER}" /opt/spark/bin/spark-sql \
    --conf spark.ui.enabled=false \
    --conf spark.ui.showConsoleProgress=false \
    --hiveconf hive.cli.print.header=false \
    -S \
    -e "${SQL}" 2>/dev/null) || {
    echo "spark-interop: spark-sql failed, re-running with stderr for diagnosis:" >&2
    docker exec -i "${CONTAINER}" /opt/spark/bin/spark-sql \
        --conf spark.ui.enabled=false \
        -e "${SQL}" >&2 || true
    exit 3
}

printf '%s\n' "${OUT}"
