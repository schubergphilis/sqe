#!/usr/bin/env bash
set -euo pipefail

# Generate benchmark datasets locally and publish them to an S3 bucket as
# immutable source data. Generate once, load many times: subsequent runs of
# benchmark-test.sh / benchmark-load.sh can skip the generate step entirely
# by pointing BENCH_DATA_SOURCE at the bucket.
#
# The bucket layout mirrors the local generator layout, which is exactly
# what `sqe-bench load --data <path>` expects:
#
#   s3://<bucket>/<benchmark>/sf<scale>/<table>/*.parquet
#
# Already-published datasets are skipped (the bucket prefix is checked
# before generating), so re-running for a new scale factor never
# regenerates or re-uploads existing ones. Use BENCH_FORCE=1 to overwrite.
#
# Usage:
#   BENCH_DATA_BUCKET=s3://sqe-benchmark \
#   BENCH_S3_PROFILE=storagegrid \
#   BENCH_S3_ENDPOINT=https://s3.example.com \
#   BENCH_SCALE=0.1 ./scripts/benchmark-publish-data.sh            # all benchmarks
#   BENCH_SCALE=1   ./scripts/benchmark-publish-data.sh tpch ssb   # only these
#
# Environment:
#   BENCH_DATA_BUCKET   target bucket URL, e.g. s3://sqe-benchmark (required)
#   BENCH_S3_ENDPOINT   S3 endpoint URL (required)
#   BENCH_S3_PROFILE    aws CLI profile holding the credentials (default: default)
#   BENCH_SCALE         scale factor (default: 0.1)
#   BENCH_DATA_DIR      local staging dir (default: /tmp/sqe-bench-data)
#   BENCH_KEEP_LOCAL=1  keep the local staging data after upload (default: delete)
#   BENCH_FORCE=1       regenerate and re-upload even if the prefix exists

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BENCH_SCALE="${BENCH_SCALE:-0.1}"
BENCH_DATA_DIR="${BENCH_DATA_DIR:-/tmp/sqe-bench-data}"
BENCH_DATA_BUCKET="${BENCH_DATA_BUCKET:-}"
BENCH_S3_ENDPOINT="${BENCH_S3_ENDPOINT:-}"
BENCH_S3_PROFILE="${BENCH_S3_PROFILE:-default}"
BENCH_KEEP_LOCAL="${BENCH_KEEP_LOCAL:-0}"
BENCH_FORCE="${BENCH_FORCE:-0}"

if [ -z "$BENCH_DATA_BUCKET" ] || [ -z "$BENCH_S3_ENDPOINT" ]; then
    echo "ERROR: BENCH_DATA_BUCKET and BENCH_S3_ENDPOINT must be set." >&2
    echo "  e.g. BENCH_DATA_BUCKET=s3://sqe-benchmark BENCH_S3_ENDPOINT=https://s3.example.com" >&2
    exit 1
fi
if ! command -v aws >/dev/null 2>&1; then
    echo "ERROR: the aws CLI is required for uploading." >&2
    exit 1
fi

AWS=(aws --profile "$BENCH_S3_PROFILE" --endpoint-url "$BENCH_S3_ENDPOINT")

ALL_BENCHMARKS=(tpch ssb tpcds tpcc tpce tpcbb clickbench)
if [ $# -gt 0 ]; then
    BENCHMARKS=("$@")
else
    BENCHMARKS=("${ALL_BENCHMARKS[@]}")
fi

cd "$ROOT_DIR"

echo "Building sqe-bench..."
cargo build -p sqe-bench --release 2>&1
BENCH_BIN="$ROOT_DIR/target/release/sqe-bench"
echo ""

TOTAL=0; PUBLISHED=0; SKIPPED=0; FAILED=0

for BENCH in "${BENCHMARKS[@]}"; do
    TOTAL=$((TOTAL + 1))
    PREFIX="$BENCH/sf$BENCH_SCALE"
    DEST="$BENCH_DATA_BUCKET/$PREFIX"
    LOCAL="$BENCH_DATA_DIR/$PREFIX"

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Publishing: $(echo "$BENCH" | tr '[:lower:]' '[:upper:]') (SF${BENCH_SCALE}) -> $DEST"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Skip if already published (any object under the prefix).
    if [ "$BENCH_FORCE" != "1" ]; then
        EXISTING=$("${AWS[@]}" s3 ls "$DEST/" --recursive 2>/dev/null | head -1 || true)
        if [ -n "$EXISTING" ]; then
            echo "  = already published, skipping (BENCH_FORCE=1 to overwrite)"
            SKIPPED=$((SKIPPED + 1))
            echo ""
            continue
        fi
    fi

    START=$(date +%s)
    if ! "$BENCH_BIN" generate "$BENCH" \
        --scale "$BENCH_SCALE" \
        --output "$BENCH_DATA_DIR" 2>&1; then
        echo "  x generate FAILED"
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi
    GEN_END=$(date +%s)
    SIZE=$(du -sh "$LOCAL" 2>/dev/null | cut -f1 || echo "?")
    echo "  generated $SIZE in $((GEN_END - START))s, uploading..."

    if ! "${AWS[@]}" s3 sync "$LOCAL" "$DEST" --only-show-errors; then
        echo "  x upload FAILED"
        FAILED=$((FAILED + 1))
        echo ""
        continue
    fi
    END=$(date +%s)
    FILES=$("${AWS[@]}" s3 ls "$DEST/" --recursive | wc -l | tr -d ' ')
    echo "  + published $FILES files ($SIZE) in $((END - START))s total"
    PUBLISHED=$((PUBLISHED + 1))

    if [ "$BENCH_KEEP_LOCAL" != "1" ]; then
        rm -rf "$LOCAL"
    fi
    echo ""
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Publish Summary (SF${BENCH_SCALE} -> $BENCH_DATA_BUCKET)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Total:     $TOTAL"
echo "  Published: $PUBLISHED"
echo "  Skipped:   $SKIPPED"
echo "  Failed:    $FAILED"

[ "$FAILED" -eq 0 ]
