#!/usr/bin/env bash
set -euo pipefail

# Run all benchmarks against the lightweight test stack.
#
# For each benchmark: generate SF1 data → load into SQE → run queries.
# Uses the same Polaris + RustFS stack as integration-test.sh.
#
# Usage:
#   ./scripts/benchmark-test.sh                    # run all benchmarks
#   ./scripts/benchmark-test.sh tpch               # run only TPC-H
#   ./scripts/benchmark-test.sh tpch ssb           # run TPC-H and SSB
#   BENCH_SCALE=0.01 ./scripts/benchmark-test.sh   # use SF0.01 (faster)
#   BENCH_PROTOCOL=trino ./scripts/benchmark-test.sh  # use Trino HTTP

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.test.yml"

# ── Configuration ─────────────────────────────────────────────
BENCH_SCALE="${BENCH_SCALE:-1}"
BENCH_PROTOCOL="${BENCH_PROTOCOL:-flight}"
BENCH_DATA_DIR="${BENCH_DATA_DIR:-/tmp/sqe-bench-data}"
BENCH_HOST="localhost"
BENCH_PORT_FLIGHT="50051"
BENCH_PORT_TRINO="8080"

# S3 credentials (match test stack)
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
S3_REGION="${S3_REGION:-us-east-1}"

# Auth credentials (match test stack)
SQE_USERNAME="${SQE_USERNAME:-root}"
SQE_PASSWORD="${SQE_PASSWORD:-s3cr3t}"

# Benchmarks to run (default: all)
ALL_BENCHMARKS=(tpch ssb tpcds tpcc tpce tpcbb clickbench)
if [ $# -gt 0 ]; then
    BENCHMARKS=("$@")
else
    BENCHMARKS=("${ALL_BENCHMARKS[@]}")
fi

# Select port based on protocol
if [ "$BENCH_PROTOCOL" = "trino" ]; then
    BENCH_PORT="$BENCH_PORT_TRINO"
else
    BENCH_PORT="$BENCH_PORT_FLIGHT"
fi

cd "$ROOT_DIR"

# ── Build sqe-bench ───────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Building sqe-bench..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cargo build -p sqe-bench --release 2>&1
BENCH_BIN="$ROOT_DIR/target/release/sqe-bench"

if [ ! -x "$BENCH_BIN" ]; then
    echo "ERROR: sqe-bench binary not found at $BENCH_BIN"
    exit 1
fi

echo "Binary: $BENCH_BIN"
echo ""

# ── Start test stack ──────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting test stack..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" up -d

# Bootstrap (creates bucket, warehouse, grants)
"$SCRIPT_DIR/bootstrap-test.sh"
echo ""

# ── Start SQE coordinator in background ───────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting SQE coordinator..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SQE_LOG_FILE="$(mktemp /tmp/sqe-bench-coord-XXXXXX.log)"
SQE_CONFIG="$ROOT_DIR/tests/sqe-test.toml"

RUST_LOG="${RUST_LOG:-sqe=info,warn}" \
    cargo run -p sqe-coordinator --release -- --config "$SQE_CONFIG" \
    > "$SQE_LOG_FILE" 2>&1 &
SQE_PID=$!

# Wait for coordinator to be ready
echo -n "Waiting for SQE coordinator..."
for i in $(seq 1 30); do
    if curl -so /dev/null "http://localhost:8080/v1/info" 2>/dev/null; then
        echo " ready (PID $SQE_PID)"
        break
    fi
    if ! kill -0 "$SQE_PID" 2>/dev/null; then
        echo " FAILED (coordinator exited)"
        echo "Last 20 lines of coordinator log:"
        tail -20 "$SQE_LOG_FILE"
        exit 1
    fi
    if [ "$i" -eq 30 ]; then
        echo " TIMEOUT"
        kill "$SQE_PID" 2>/dev/null || true
        exit 1
    fi
    echo -n "."
    sleep 1
done
echo ""

# ── Cleanup handler ───────────────────────────────────────────
cleanup() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Cleaning up..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    kill "$SQE_PID" 2>/dev/null || true
    wait "$SQE_PID" 2>/dev/null || true
    rm -f "$SQE_LOG_FILE"
    # Don't tear down docker — leave it for subsequent runs
    echo "Done."
}
trap cleanup EXIT

# ── Run benchmarks ────────────────────────────────────────────
TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_SKIP=0
TOTAL_ERROR=0
RESULTS=()

for BENCH in "${BENCHMARKS[@]}"; do
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Benchmark: $(echo "$BENCH" | tr '[:lower:]' '[:upper:]') (SF${BENCH_SCALE})"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # ── Step 1: Generate ──────────────────────────────────────
    echo ""
    echo "  [1/3] Generating data..."
    GEN_START=$(date +%s)
    if ! "$BENCH_BIN" generate "$BENCH" \
        --scale "$BENCH_SCALE" \
        --output "$BENCH_DATA_DIR" 2>&1; then
        echo "  ✗ Generation FAILED for $BENCH"
        RESULTS+=("$BENCH: GENERATE_FAILED")
        TOTAL_ERROR=$((TOTAL_ERROR + 1))
        continue
    fi
    GEN_END=$(date +%s)
    echo "  ✓ Generated in $((GEN_END - GEN_START))s"

    # ── Step 2: Load ──────────────────────────────────────────
    echo ""
    echo "  [2/3] Loading into SQE..."
    LOAD_START=$(date +%s)
    if ! "$BENCH_BIN" load "$BENCH" \
        --scale "$BENCH_SCALE" \
        --data "$BENCH_DATA_DIR" \
        --protocol "$BENCH_PROTOCOL" \
        --host "$BENCH_HOST" \
        --port "$BENCH_PORT" \
        --username "$SQE_USERNAME" \
        --password "$SQE_PASSWORD" \
        --s3-access-key "$S3_ACCESS_KEY" \
        --s3-secret-key "$S3_SECRET_KEY" \
        --s3-endpoint "$S3_ENDPOINT" \
        --s3-region "$S3_REGION" \
        --clean 2>&1; then
        echo "  ✗ Load FAILED for $BENCH"
        RESULTS+=("$BENCH: LOAD_FAILED")
        TOTAL_ERROR=$((TOTAL_ERROR + 1))
        continue
    fi
    LOAD_END=$(date +%s)
    echo "  ✓ Loaded in $((LOAD_END - LOAD_START))s"

    # ── Step 3: Test ──────────────────────────────────────────
    echo ""
    echo "  [3/3] Running queries..."
    TEST_START=$(date +%s)
    if "$BENCH_BIN" test "$BENCH" \
        --scale "$BENCH_SCALE" \
        --protocol "$BENCH_PROTOCOL" \
        --host "$BENCH_HOST" \
        --port "$BENCH_PORT" \
        --username "$SQE_USERNAME" \
        --password "$SQE_PASSWORD" 2>&1; then
        TEST_EXIT=0
    else
        TEST_EXIT=$?
    fi
    TEST_END=$(date +%s)
    echo "  Completed in $((TEST_END - TEST_START))s"

    if [ $TEST_EXIT -eq 0 ]; then
        RESULTS+=("$BENCH: PASS")
        TOTAL_PASS=$((TOTAL_PASS + 1))
    else
        RESULTS+=("$BENCH: FAIL (exit $TEST_EXIT)")
        TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi

    # ── Clean up generated data for this benchmark ────────────
    rm -rf "${BENCH_DATA_DIR:?}/$BENCH"
done

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Benchmark Summary (SF${BENCH_SCALE}, ${BENCH_PROTOCOL})"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for R in "${RESULTS[@]}"; do
    echo "  $R"
done
echo ""
echo "  Total: $((TOTAL_PASS + TOTAL_FAIL + TOTAL_ERROR)) benchmarks"
echo "  Pass:  $TOTAL_PASS"
echo "  Fail:  $TOTAL_FAIL"
echo "  Error: $TOTAL_ERROR"
echo ""
echo "  Reports: benchmarks/results/"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Exit with failure if any benchmark failed
if [ $((TOTAL_FAIL + TOTAL_ERROR)) -gt 0 ]; then
    exit 1
fi
