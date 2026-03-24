#!/usr/bin/env bash
set -euo pipefail

# Generate benchmark data and load it into SQE via the test stack.
#
# Uses the same Polaris + RustFS stack as integration-test.sh.
# After this script completes, the data is loaded and you can run queries
# manually or via: sqe-bench test <benchmark> ...
#
# Usage:
#   ./scripts/benchmark-load.sh                       # load all benchmarks at SF0.01
#   ./scripts/benchmark-load.sh tpch ssb              # load only TPC-H and SSB
#   BENCH_SCALE=1 ./scripts/benchmark-load.sh tpch    # load TPC-H at SF1
#   BENCH_KEEP_RUNNING=1 ./scripts/benchmark-load.sh  # leave SQE running after load

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.test.yml"

# ── Configuration ─────────────────────────────────────────────
BENCH_SCALE="${BENCH_SCALE:-0.01}"
BENCH_PROTOCOL="${BENCH_PROTOCOL:-flight}"
BENCH_DATA_DIR="${BENCH_DATA_DIR:-/tmp/sqe-bench-data}"
BENCH_KEEP_RUNNING="${BENCH_KEEP_RUNNING:-0}"
BENCH_HOST="localhost"
BENCH_PORT_FLIGHT="50051"
BENCH_PORT_TRINO="8080"

# Credentials (match test stack — same as integration-test.sh)
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
S3_REGION="${S3_REGION:-us-east-1}"
SQE_USERNAME="${SQE_USERNAME:-root}"
SQE_PASSWORD="${SQE_PASSWORD:-s3cr3t}"

ALL_BENCHMARKS=(tpch ssb tpcds tpcc tpce tpcbb clickbench)
if [ $# -gt 0 ]; then
    BENCHMARKS=("$@")
else
    BENCHMARKS=("${ALL_BENCHMARKS[@]}")
fi

if [ "$BENCH_PROTOCOL" = "trino" ]; then
    BENCH_PORT="$BENCH_PORT_TRINO"
else
    BENCH_PORT="$BENCH_PORT_FLIGHT"
fi

cd "$ROOT_DIR"

# ── Build ─────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Building sqe-bench + sqe-coordinator..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cargo build -p sqe-bench -p sqe-coordinator --release 2>&1
BENCH_BIN="$ROOT_DIR/target/release/sqe-bench"
echo ""

# ── Start test stack ──────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting test stack..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose -f "$COMPOSE_FILE" up -d

# Bootstrap (creates S3 bucket, Polaris warehouse, grants — idempotent)
"$SCRIPT_DIR/bootstrap-test.sh"
echo ""

# ── Start SQE coordinator ────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting SQE coordinator..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SQE_LOG_FILE="$(mktemp /tmp/sqe-bench-coord-XXXXXX.log)"
SQE_CONFIG="$ROOT_DIR/tests/sqe-test.toml"

RUST_LOG="${RUST_LOG:-sqe=info,warn}" \
    cargo run -p sqe-coordinator --release --bin sqe-coordinator -- --config "$SQE_CONFIG" \
    > "$SQE_LOG_FILE" 2>&1 &
SQE_PID=$!

echo -n "Waiting for SQE coordinator..."
for i in $(seq 1 30); do
    if curl -so /dev/null "http://localhost:8080/v1/info" 2>/dev/null; then
        echo " ready (PID $SQE_PID)"
        break
    fi
    if ! kill -0 "$SQE_PID" 2>/dev/null; then
        echo " FAILED (coordinator exited)"
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
    if [ "$BENCH_KEEP_RUNNING" = "1" ]; then
        echo ""
        echo "SQE coordinator still running (PID $SQE_PID)"
        echo "  Flight SQL: localhost:50051"
        echo "  Trino HTTP: localhost:8080"
        echo "  Logs: $SQE_LOG_FILE"
        echo ""
        echo "Run queries with:"
        for B in "${BENCHMARKS[@]}"; do
            echo "  $BENCH_BIN test $B --scale $BENCH_SCALE --protocol $BENCH_PROTOCOL --host $BENCH_HOST --username $SQE_USERNAME --password $SQE_PASSWORD"
        done
        echo ""
        echo "Stop with: kill $SQE_PID"
    else
        echo ""
        echo "Stopping SQE coordinator..."
        kill "$SQE_PID" 2>/dev/null || true
        wait "$SQE_PID" 2>/dev/null || true
        rm -f "$SQE_LOG_FILE"
    fi
}
trap cleanup EXIT

# ── Generate + Load each benchmark ────────────────────────────
TOTAL=0
PASS=0
FAIL=0

for BENCH in "${BENCHMARKS[@]}"; do
    TOTAL=$((TOTAL + 1))
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  $(echo "$BENCH" | tr '[:lower:]' '[:upper:]') (SF${BENCH_SCALE})"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # ── Generate ──────────────────────────────────────────────
    echo ""
    echo "  [1/2] Generating data..."
    GEN_START=$(date +%s)
    if ! "$BENCH_BIN" generate "$BENCH" \
        --scale "$BENCH_SCALE" \
        --output "$BENCH_DATA_DIR" 2>&1; then
        echo "  ✗ Generate FAILED"
        FAIL=$((FAIL + 1))
        continue
    fi
    GEN_END=$(date +%s)
    echo "  ✓ Generated in $((GEN_END - GEN_START))s"

    # ── Load ──────────────────────────────────────────────────
    echo ""
    echo "  [2/2] Loading into SQE..."
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
        echo "  ✗ Load FAILED"
        FAIL=$((FAIL + 1))
        continue
    fi
    LOAD_END=$(date +%s)
    echo "  ✓ Loaded in $((LOAD_END - LOAD_START))s"
    PASS=$((PASS + 1))

    # Clean generated files (data is now in Iceberg/S3)
    rm -rf "${BENCH_DATA_DIR:?}/$BENCH"
done

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Load Summary (SF${BENCH_SCALE})"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Loaded: $PASS/$TOTAL benchmarks"
if [ "$FAIL" -gt 0 ]; then
    echo "  Failed: $FAIL"
fi
echo ""
echo "  Namespaces created:"
for B in "${BENCHMARKS[@]}"; do
    echo "    ${B}_sf${BENCH_SCALE}"
done
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
