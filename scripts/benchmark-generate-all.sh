#!/usr/bin/env bash
set -euo pipefail

# Generate all benchmark datasets locally (no Docker or running SQE needed).
#
# Usage:
#   ./scripts/benchmark-generate-all.sh                     # SF0.01, all benchmarks
#   ./scripts/benchmark-generate-all.sh tpch ssb            # SF0.01, only TPC-H and SSB
#   BENCH_SCALE=1 ./scripts/benchmark-generate-all.sh       # SF1 (~1GB per benchmark)
#   BENCH_SCALE=10 ./scripts/benchmark-generate-all.sh tpch # SF10 (~10GB TPC-H)
#   BENCH_DATA_DIR=./data ./scripts/benchmark-generate-all.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BENCH_SCALE="${BENCH_SCALE:-0.01}"
BENCH_DATA_DIR="${BENCH_DATA_DIR:-/tmp/sqe-bench-data}"

ALL_BENCHMARKS=(tpch ssb tpcds tpcc tpce tpcbb clickbench)
if [ $# -gt 0 ]; then
    BENCHMARKS=("$@")
else
    BENCHMARKS=("${ALL_BENCHMARKS[@]}")
fi

cd "$ROOT_DIR"

# ── Build ─────────────────────────────────────────────────────
echo "Building sqe-bench..."
cargo build -p sqe-bench --release 2>&1
BENCH_BIN="$ROOT_DIR/target/release/sqe-bench"
echo ""

# ── Generate ──────────────────────────────────────────────────
TOTAL=0
PASS=0
FAIL=0

for BENCH in "${BENCHMARKS[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Generating: ${BENCH^^} (SF${BENCH_SCALE})"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    TOTAL=$((TOTAL + 1))
    START=$(date +%s)

    if "$BENCH_BIN" generate "$BENCH" \
        --scale "$BENCH_SCALE" \
        --output "$BENCH_DATA_DIR" 2>&1; then
        END=$(date +%s)
        # Count files and size
        DIR="$BENCH_DATA_DIR/$BENCH"
        if [ -d "$DIR" ]; then
            FILES=$(find "$DIR" -name "*.parquet" | wc -l | tr -d ' ')
            SIZE=$(du -sh "$DIR" 2>/dev/null | cut -f1)
        else
            FILES=0
            SIZE="0"
        fi
        echo "  ✓ ${BENCH^^}: ${FILES} files, ${SIZE}, $((END - START))s"
        PASS=$((PASS + 1))
    else
        echo "  ✗ ${BENCH^^}: FAILED"
        FAIL=$((FAIL + 1))
    fi
    echo ""
done

# ── Summary ───────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Generate Summary (SF${BENCH_SCALE})"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Total: $TOTAL"
echo "  Pass:  $PASS"
echo "  Fail:  $FAIL"
echo "  Data:  $BENCH_DATA_DIR"

if [ -d "$BENCH_DATA_DIR" ]; then
    TOTAL_SIZE=$(du -sh "$BENCH_DATA_DIR" 2>/dev/null | cut -f1)
    echo "  Size:  $TOTAL_SIZE"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
