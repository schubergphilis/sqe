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
BENCH_PORT_FLIGHT="60051"
BENCH_PORT_TRINO="18080"
# Bloom-filter A/B lever: any truthy value passes `--bloom-filter` to the
# load, writing Parquet blooms on TPC-H/SSB join-key columns. Off by default.
BENCH_BLOOM_FILTER="${BENCH_BLOOM_FILTER:-}"

# Credentials (match test stack — same as integration-test.sh)
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:19000}"
S3_REGION="${S3_REGION:-us-east-1}"
# Auth: Flight SQL handshake (username + password). In client_credentials mode
# the password is ignored by SQE — set it to empty string.
SQE_USERNAME="${SQE_USERNAME:-root}"
SQE_PASSWORD="${SQE_PASSWORD:-}"

# ── External data source (optional) ──────────────────────────
# BENCH_DATA_SOURCE=s3://<bucket> reads pre-published parquet from an S3
# bucket instead of generating locally (see benchmark-publish-data.sh).
BENCH_DATA_SOURCE="${BENCH_DATA_SOURCE:-generate}"
BENCH_S3_PROFILE="${BENCH_S3_PROFILE:-default}"
BENCH_S3_ENDPOINT="${BENCH_S3_ENDPOINT:-}"
BENCH_S3_REGION="${BENCH_S3_REGION:-us-east-1}"

EXTERNAL_DATA=""
case "$BENCH_DATA_SOURCE" in
    s3://*) EXTERNAL_DATA=1 ;;
    generate) ;;
    *)
        echo "ERROR: BENCH_DATA_SOURCE must be 'generate' or an s3:// URL, got: '$BENCH_DATA_SOURCE'" >&2
        exit 1
        ;;
esac

if [ -n "$EXTERNAL_DATA" ]; then
    if [ -z "$BENCH_S3_ENDPOINT" ]; then
        echo "ERROR: BENCH_S3_ENDPOINT is required with BENCH_DATA_SOURCE=$BENCH_DATA_SOURCE" >&2
        exit 1
    fi
    DATA_PATH="$BENCH_DATA_SOURCE"
    DATA_S3_ACCESS_KEY="${DATA_S3_ACCESS_KEY:-$(aws configure get aws_access_key_id --profile "$BENCH_S3_PROFILE")}"
    DATA_S3_SECRET_KEY="${DATA_S3_SECRET_KEY:-$(aws configure get aws_secret_access_key --profile "$BENCH_S3_PROFILE")}"
    DATA_S3_ENDPOINT="$BENCH_S3_ENDPOINT"
    DATA_S3_REGION="$BENCH_S3_REGION"
    if [ -z "$DATA_S3_ACCESS_KEY" ] || [ -z "$DATA_S3_SECRET_KEY" ]; then
        echo "ERROR: could not resolve S3 credentials from profile '$BENCH_S3_PROFILE'" >&2
        exit 1
    fi
else
    DATA_PATH="$BENCH_DATA_DIR"
    DATA_S3_ACCESS_KEY="$S3_ACCESS_KEY"
    DATA_S3_SECRET_KEY="$S3_SECRET_KEY"
    DATA_S3_ENDPOINT="$S3_ENDPOINT"
    DATA_S3_REGION="$S3_REGION"
fi

ALL_BENCHMARKS=(tpch ssb tpcds tpcc tpce tpcbb clickbench bank)
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
# PROFILE=release (default) | dev-release (same opt-level, no LTO,
# incremental — much faster rebuilds when iterating) | debug.
PROFILE="${PROFILE:-release}"
case "$PROFILE" in
    release|debug|dev-release) ;;
    *) echo "ERROR: PROFILE must be 'release', 'dev-release' or 'debug', got: '$PROFILE'" >&2; exit 1 ;;
esac
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Building sqe-bench + sqe-coordinator (profile: $PROFILE)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ "$PROFILE" = "release" ]; then
    cargo build -p sqe-bench -p sqe-coordinator --bin sqe-bench --bin sqe-coordinator --release 2>&1
elif [ "$PROFILE" = "dev-release" ]; then
    cargo build -p sqe-bench -p sqe-coordinator --bin sqe-bench --bin sqe-coordinator --profile dev-release 2>&1
else
    cargo build -p sqe-bench -p sqe-coordinator --bin sqe-bench --bin sqe-coordinator 2>&1
fi
BENCH_BIN="$ROOT_DIR/target/$PROFILE/sqe-bench"
SQE_BIN="$ROOT_DIR/target/$PROFILE/sqe-coordinator"
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
SQE_LOG_FILE="/tmp/sqe-bench-coord-$$.log"
SQE_CONFIG="$ROOT_DIR/tests/sqe-test.toml"

# With an external data source, read_parquet() carries an inline
# `endpoint =>` override, and the coordinator's SSRF gate (issue #46)
# rejects endpoint hosts that are not in `[storage.tvf] allowed_http_hosts`.
# Generate a temp config that allowlists exactly the data endpoint's host.
if [ -n "$EXTERNAL_DATA" ]; then
    ENDPOINT_HOST=$(python3 -c "from urllib.parse import urlparse; import sys; print(urlparse(sys.argv[1]).hostname or '')" "$BENCH_S3_ENDPOINT")
    if [ -z "$ENDPOINT_HOST" ]; then
        echo "ERROR: could not parse host from BENCH_S3_ENDPOINT=$BENCH_S3_ENDPOINT" >&2
        exit 1
    fi
    SQE_CONFIG_EXT="/tmp/sqe-bench-config-$$.toml"
    python3 - "$ROOT_DIR/tests/sqe-test.toml" "$SQE_CONFIG_EXT" "$ENDPOINT_HOST" <<'PYEOF'
import sys
src, dst, host = sys.argv[1:4]
text = open(src).read()
anchor = "[storage.tvf]\n"
assert anchor in text, "tests/sqe-test.toml is missing the [storage.tvf] section"
# Include the loopback hosts so local-endpoint TVF calls keep working:
# an explicit allowed_http_hosts REPLACES the (empty) default entirely.
text = text.replace(anchor, anchor + f'allowed_http_hosts = ["{host}", "localhost", "127.0.0.1"]\n', 1)
open(dst, "w").write(text)
PYEOF
    SQE_CONFIG="$SQE_CONFIG_EXT"
    echo "External data source: $BENCH_DATA_SOURCE (endpoint host '$ENDPOINT_HOST' allowlisted)"
fi

RUST_LOG="${RUST_LOG:-sqe=info,warn}" \
    "$SQE_BIN" "$SQE_CONFIG" \
    > "$SQE_LOG_FILE" 2>&1 &
SQE_PID=$!

echo -n "Waiting for SQE coordinator (may take a while on first build)..."
for i in $(seq 1 300); do
    if curl -so /dev/null "http://localhost:18080/v1/info" 2>/dev/null; then
        echo " ready (PID $SQE_PID)"
        break
    fi
    if ! kill -0 "$SQE_PID" 2>/dev/null; then
        echo " FAILED (coordinator exited)"
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
    if [ "$BENCH_KEEP_RUNNING" = "1" ]; then
        echo ""
        echo "SQE coordinator still running (PID $SQE_PID)"
        echo "  Flight SQL: localhost:60051"
        echo "  Trino HTTP: localhost:18080"
        echo "  Logs: $SQE_LOG_FILE"
        echo ""
        echo "Run queries with:"
        for B in "${BENCHMARKS[@]}"; do
            echo "  $BENCH_BIN test $B --scale $BENCH_SCALE --protocol $BENCH_PROTOCOL --host $BENCH_HOST --port $BENCH_PORT --username $SQE_USERNAME --password ''"
        done
        echo ""
        echo "Stop with: kill $SQE_PID"
    else
        echo ""
        echo "Stopping SQE coordinator..."
        kill "$SQE_PID" 2>/dev/null || true
        wait "$SQE_PID" 2>/dev/null || true
        rm -f "$SQE_LOG_FILE" "${SQE_CONFIG_EXT:-}"
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

    # ── bank: direct-to-Iceberg (no staging, no CTAS) ─────────
    # sqe-bench writes partition-aligned parquet straight to the warehouse
    # and commits one snapshot per trading day via the Iceberg REST API.
    if [ "$BENCH" = "bank" ]; then
        SCALE_FMT=$(python3 -c "s='$BENCH_SCALE'; print((s.rstrip('0').rstrip('.') if '.' in s else s).replace('.','_'))")
        BANK_ROWS_PER_DAY=$(python3 -c "print(max(1, int(2_000_000 * float('$BENCH_SCALE'))))")
        BANK_CUSTOMERS=$(python3 -c "print(max(100, int(100_000 * float('$BENCH_SCALE'))))")
        echo ""
        echo "  [1-2/2] Generating straight into Iceberg (12 days x ${BANK_ROWS_PER_DAY} rows/day)..."
        LOAD_START=$(date +%s)
        if ! "$BENCH_BIN" generate bank \
            --sink iceberg \
            --days 12 \
            --rows-per-day "$BANK_ROWS_PER_DAY" \
            --customers "$BANK_CUSTOMERS" \
            --namespace "bank_sf${SCALE_FMT}" \
            --catalog-uri "http://localhost:18181/api/catalog" \
            --warehouse test_warehouse \
            --client-id root \
            --client-secret s3cr3t \
            --scope 'PRINCIPAL_ROLE:ALL' \
            --s3-endpoint "$S3_ENDPOINT" \
            --s3-access-key "$S3_ACCESS_KEY" \
            --s3-secret-key "$S3_SECRET_KEY" \
            --s3-region "$S3_REGION" \
            --s3-path-style \
            --clean 2>&1; then
            echo "  ✗ Load FAILED"
            FAIL=$((FAIL + 1))
            continue
        fi
        LOAD_END=$(date +%s)
        echo "  ✓ Loaded in $((LOAD_END - LOAD_START))s"
        PASS=$((PASS + 1))
        continue
    fi

    # ── Generate ──────────────────────────────────────────────
    echo ""
    if [ -n "$EXTERNAL_DATA" ]; then
        echo "  [1/2] Using pre-published data: $DATA_PATH/$BENCH/sf$BENCH_SCALE"
    else
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
    fi

    # ── Load ──────────────────────────────────────────────────
    echo ""
    echo "  [2/2] Loading into SQE..."
    # Translate the BENCH_BLOOM_FILTER env toggle into the load flag.
    # Lowercase via tr for bash 3.2 (macOS).
    BLOOM_ARGS=()
    case "$(printf '%s' "$BENCH_BLOOM_FILTER" | tr '[:upper:]' '[:lower:]')" in
        ""|0|false|no|off) ;;
        *) BLOOM_ARGS=(--bloom-filter); echo "  (bloom filters ON for join-key columns)" ;;
    esac
    LOAD_START=$(date +%s)
    if ! "$BENCH_BIN" load "$BENCH" \
        --scale "$BENCH_SCALE" \
        --data "$DATA_PATH" \
        --protocol "$BENCH_PROTOCOL" \
        --host "$BENCH_HOST" \
        --port "$BENCH_PORT" \
        --username "$SQE_USERNAME" \
        --password "$SQE_PASSWORD" \
        --s3-access-key "$DATA_S3_ACCESS_KEY" \
        --s3-secret-key "$DATA_S3_SECRET_KEY" \
        --s3-endpoint "$DATA_S3_ENDPOINT" \
        --s3-region "$DATA_S3_REGION" \
        ${BLOOM_ARGS[@]+"${BLOOM_ARGS[@]}"} \
        --clean 2>&1; then
        echo "  ✗ Load FAILED"
        FAIL=$((FAIL + 1))
        continue
    fi
    LOAD_END=$(date +%s)
    echo "  ✓ Loaded in $((LOAD_END - LOAD_START))s"
    PASS=$((PASS + 1))

    # Clean generated files (data is now in Iceberg/S3)
    if [ -z "$EXTERNAL_DATA" ]; then
        rm -rf "${BENCH_DATA_DIR:?}/$BENCH"
    fi
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
