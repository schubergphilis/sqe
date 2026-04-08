#!/usr/bin/env bash
# scripts/trino-parity-test.sh — Run side-by-side SQE vs Trino comparison
#
# Usage: ./scripts/trino-parity-test.sh [benchmark] [scale]
#   benchmark: tpch (default), tpcds, ssb
#   scale: 1 (default)
#
# Requires: docker compose stack running (docker-compose.test.yml + docker-compose.compare.yml)

set -euo pipefail

BENCHMARK="${1:-tpch}"
SCALE="${2:-1}"

echo "=== SQE vs Trino Parity Test: ${BENCHMARK} SF${SCALE} ==="

# Check services are up
echo "Checking SQE..."
timeout 5 bash -c 'until curl -sf http://localhost:28080/v1/info > /dev/null; do sleep 1; done' \
    || { echo "ERROR: SQE not reachable on port 28080"; exit 1; }

echo "Checking Trino..."
timeout 30 bash -c 'until curl -sf http://localhost:38080/v1/info > /dev/null; do sleep 1; done' \
    || { echo "ERROR: Trino not reachable on port 38080"; exit 1; }

echo "Both engines ready. Running comparison..."

cargo run -p sqe-bench --release -- compare "$BENCHMARK" \
    --scale "$SCALE" \
    --sqe-host localhost \
    --sqe-port 60051 \
    --trino-url "http://localhost:38080" \
    --trino-user admin \
    --output "benchmarks/results"

echo "Done. Report saved to benchmarks/results/"
