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
BENCH_PORT_FLIGHT="60051"
BENCH_PORT_TRINO="18080"

# S3 credentials (match test stack)
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:19000}"
S3_REGION="${S3_REGION:-us-east-1}"

# Auth credentials (match test stack)
SQE_USERNAME="${SQE_USERNAME:-root}"
SQE_PASSWORD="${SQE_PASSWORD:-}"

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
cargo build -p sqe-bench -p sqe-coordinator --release 2>&1
BENCH_BIN="$ROOT_DIR/target/release/sqe-bench"
SQE_BIN="$ROOT_DIR/target/release/sqe-coordinator"

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
SQE_LOG_FILE="/tmp/sqe-bench-coord-$$.log"
SQE_CONFIG="$ROOT_DIR/tests/sqe-test.toml"

RUST_LOG="${RUST_LOG:-sqe=info,warn}" \
    "$SQE_BIN" "$SQE_CONFIG" \
    > "$SQE_LOG_FILE" 2>&1 &
SQE_PID=$!

# Wait for coordinator to be ready
echo -n "Waiting for SQE coordinator (may take a while on first build)..."
for i in $(seq 1 300); do
    if curl -so /dev/null "http://localhost:18080/v1/info" 2>/dev/null; then
        echo " ready (PID $SQE_PID)"
        break
    fi
    if ! kill -0 "$SQE_PID" 2>/dev/null; then
        echo " FAILED (coordinator exited)"
        echo "Last 20 lines of coordinator log:"
        tail -20 "$SQE_LOG_FILE"
        exit 1
    fi
    if [ "$i" -eq 300 ]; then
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
# Collect per-benchmark summary lines: "name:pass:fail:diff:skip:error:total:ms"
SUMMARIES=()

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
    TEST_LOG="/tmp/sqe-bench-test-$$.log"
    "$BENCH_BIN" test "$BENCH" \
        --scale "$BENCH_SCALE" \
        --protocol "$BENCH_PROTOCOL" \
        --host "$BENCH_HOST" \
        --port "$BENCH_PORT" \
        --username "$SQE_USERNAME" \
        --password "$SQE_PASSWORD" 2>&1 | tee "$TEST_LOG" || true
    TEST_END=$(date +%s)

    # Parse the BENCH_SUMMARY line: name:pass:fail:diff:skip:error:total:ms
    SUMMARY_LINE=$(grep "^BENCH_SUMMARY:" "$TEST_LOG" | tail -1)
    rm -f "$TEST_LOG"
    if [ -n "$SUMMARY_LINE" ]; then
        SUMMARIES+=("$SUMMARY_LINE")
        RESULTS+=("$BENCH: DONE")
        TOTAL_PASS=$((TOTAL_PASS + 1))
    else
        SUMMARIES+=("BENCH_SUMMARY:$BENCH:0:0:0:0:0:0:0")
        RESULTS+=("$BENCH: FAIL (no summary)")
        TOTAL_FAIL=$((TOTAL_FAIL + 1))
    fi

    # ── Clean up generated data for this benchmark ────────────
    rm -rf "${BENCH_DATA_DIR:?}/$BENCH"
done

# ── Summary table ─────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Benchmark Results (SF${BENCH_SCALE}, $(echo "$BENCH_PROTOCOL" | tr '[:lower:]' '[:upper:]'))"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "  %-14s %5s %5s %5s %5s %5s %5s %8s\n" "Benchmark" "Pass" "Fail" "Diff" "Skip" "Error" "Total" "Time"
echo "  ─────────────────────────────────────────────────────────────────"

SUM_PASS=0; SUM_FAIL=0; SUM_DIFF=0; SUM_SKIP=0; SUM_ERROR=0; SUM_TOTAL=0; SUM_MS=0

for S in "${SUMMARIES[@]}"; do
    # Parse BENCH_SUMMARY:name:pass:fail:diff:skip:error:total:ms
    IFS=':' read -r _ NAME PASS FAIL DIFF SKIP ERR TOTAL MS <<< "$S"
    TIME_S=$(echo "scale=1; $MS / 1000" | bc 2>/dev/null || echo "0.0")
    printf "  %-14s %5s %5s %5s %5s %5s %5s %7ss\n" "$NAME" "$PASS" "$FAIL" "$DIFF" "$SKIP" "$ERR" "$TOTAL" "$TIME_S"
    SUM_PASS=$((SUM_PASS + PASS))
    SUM_FAIL=$((SUM_FAIL + FAIL))
    SUM_DIFF=$((SUM_DIFF + DIFF))
    SUM_SKIP=$((SUM_SKIP + SKIP))
    SUM_ERROR=$((SUM_ERROR + ERR))
    SUM_TOTAL=$((SUM_TOTAL + TOTAL))
    SUM_MS=$((SUM_MS + MS))
done

echo "  ─────────────────────────────────────────────────────────────────"
SUM_TIME_S=$(echo "scale=1; $SUM_MS / 1000" | bc 2>/dev/null || echo "0.0")
printf "  %-14s %5s %5s %5s %5s %5s %5s %7ss\n" "TOTAL" "$SUM_PASS" "$SUM_FAIL" "$SUM_DIFF" "$SUM_SKIP" "$SUM_ERROR" "$SUM_TOTAL" "$SUM_TIME_S"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Reports: benchmarks/results/"

# Exit with failure if any query failed
if [ $((SUM_FAIL + SUM_ERROR)) -gt 0 ]; then
    exit 1
fi
