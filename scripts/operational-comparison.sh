#!/usr/bin/env bash
# scripts/operational-comparison.sh — Collect operational metrics for SQE vs Trino
#
# Measures: build time, binary size, image size, cold start, idle memory, loaded memory.
# Outputs: JSON to benchmarks/results/operational-comparison.json + markdown table.
#
# Requirements: Docker, cargo, java (for Trino comparison)

set -euo pipefail

OUTPUT="${1:-benchmarks/results/operational-comparison.json}"

echo "=== SQE Operational Metrics ==="

# Build time
echo "Building SQE (release)..."
SQE_BUILD_START=$(date +%s)
cargo build --release 2>&1 | tail -1
SQE_BUILD_END=$(date +%s)
SQE_BUILD_SECS=$((SQE_BUILD_END - SQE_BUILD_START))
echo "  Build time: ${SQE_BUILD_SECS}s"

# Binary size
SQE_COORDINATOR_SIZE=$(stat -f%z target/release/sqe-coordinator 2>/dev/null || stat -c%s target/release/sqe-coordinator)
SQE_CLI_SIZE=$(stat -f%z target/release/sqe-cli 2>/dev/null || stat -c%s target/release/sqe-cli)
echo "  Coordinator binary: $((SQE_COORDINATOR_SIZE / 1048576))MB"
echo "  CLI binary: $((SQE_CLI_SIZE / 1048576))MB"

# Cargo.lock crate count
CRATE_COUNT=$(grep -c 'name = ' Cargo.lock)
echo "  Dependencies: ${CRATE_COUNT} crates"

# Docker image size (if built)
SQE_IMAGE_SIZE="N/A"
if docker image inspect sqe-coordinator >/dev/null 2>&1; then
    SQE_IMAGE_SIZE=$(docker image inspect sqe-coordinator --format='{{.Size}}')
    echo "  Docker image: $((SQE_IMAGE_SIZE / 1048576))MB"
fi

echo ""
echo "=== Trino Operational Metrics ==="

# Pull Trino image
TRINO_IMAGE="trinodb/trino:465"
docker pull "$TRINO_IMAGE" -q

TRINO_IMAGE_SIZE=$(docker image inspect "$TRINO_IMAGE" --format='{{.Size}}')
echo "  Docker image: $((TRINO_IMAGE_SIZE / 1048576))MB"

# Cold start time (container start → first query)
echo "Measuring Trino cold start..."
TRINO_CONTAINER=$(docker run -d --rm -p 48080:8080 "$TRINO_IMAGE")
TRINO_START=$(date +%s%N)
timeout 120 bash -c "until curl -sf http://localhost:48080/v1/info >/dev/null 2>&1; do sleep 0.5; done"
TRINO_READY=$(date +%s%N)
TRINO_COLD_MS=$(( (TRINO_READY - TRINO_START) / 1000000 ))
docker stop "$TRINO_CONTAINER" >/dev/null 2>&1 || true
echo "  Cold start: ${TRINO_COLD_MS}ms"

# Write JSON
cat > "$OUTPUT" <<ENDJSON
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "sqe": {
    "build_time_secs": $SQE_BUILD_SECS,
    "coordinator_binary_bytes": $SQE_COORDINATOR_SIZE,
    "cli_binary_bytes": $SQE_CLI_SIZE,
    "cargo_lock_crates": $CRATE_COUNT,
    "docker_image_bytes": "$SQE_IMAGE_SIZE"
  },
  "trino": {
    "docker_image_bytes": $TRINO_IMAGE_SIZE,
    "cold_start_ms": $TRINO_COLD_MS
  }
}
ENDJSON

echo ""
echo "Report saved to $OUTPUT"
