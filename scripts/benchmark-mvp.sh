#!/usr/bin/env bash
set -euo pipefail

# Load and test benchmarks against the MVP environment.
#
# Usage:
#   ./scripts/benchmark-mvp.sh                     # all benchmarks
#   ./scripts/benchmark-mvp.sh tpch ssb            # specific benchmarks
#   BENCH_SCALE=1 ./scripts/benchmark-mvp.sh tpch  # SF1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── MVP Configuration ─────────────────────────────────────
BENCH_SCALE="${BENCH_SCALE:-0.1}"
BENCH_DATA_DIR="${BENCH_DATA_DIR:-/tmp/sqe-bench-data}"
BENCH_HOST="localhost"
BENCH_PORT="50051"

# Auth: OIDC password grant against Keycloak
SQE_USERNAME="root"
SQE_PASSWORD="root123"

ALL_BENCHMARKS=(tpch tpcds ssb tpcc tpce clickbench)
if [ $# -gt 0 ]; then
    BENCHMARKS=("$@")
else
    BENCHMARKS=("${ALL_BENCHMARKS[@]}")
fi

cd "$ROOT_DIR"

# ── Build ─────────────────────────────────────────────────
echo "Building sqe-bench..."
cargo build -p sqe-bench --release 2>&1
BENCH_BIN="$ROOT_DIR/target/release/sqe-bench"
echo ""

# ── Run benchmarks ────────────────────────────────────────
for BENCH in "${BENCHMARKS[@]}"; do
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $(echo "$BENCH" | tr '[:lower:]' '[:upper:]') (SF${BENCH_SCALE})"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # ── Step 1: Generate ──────────────────────────────────
    echo ""
    echo "  [1/3] Generating data..."
    if ! "$BENCH_BIN" generate "$BENCH" \
        --scale "$BENCH_SCALE" \
        --output "$BENCH_DATA_DIR" 2>&1; then
        echo "  ✗ Generate FAILED"
        continue
    fi
    echo "  ✓ Generated"

    # ── Step 2: Load ──────────────────────────────────────
    echo ""
    echo "  [2/3] Loading into SQE..."
    if ! "$BENCH_BIN" load "$BENCH" \
        --scale "$BENCH_SCALE" \
        --data "$BENCH_DATA_DIR" \
        --host "$BENCH_HOST" \
        --port "$BENCH_PORT" \
        --username "$SQE_USERNAME" \
        --password "$SQE_PASSWORD" \
        --clean 2>&1; then
        echo "  ✗ Load FAILED"
        continue
    fi
    echo "  ✓ Loaded"

    # ── Step 3: Test ──────────────────────────────────────
    echo ""
    echo "  [3/3] Running queries..."
    "$BENCH_BIN" test "$BENCH" \
        --scale "$BENCH_SCALE" \
        --host "$BENCH_HOST" \
        --port "$BENCH_PORT" \
        --username "$SQE_USERNAME" \
        --password "$SQE_PASSWORD" 2>&1 || true

    # Clean generated data
    rm -rf "${BENCH_DATA_DIR:?}/$BENCH"
done

echo ""
echo "Done. Reports in benchmarks/results/"
