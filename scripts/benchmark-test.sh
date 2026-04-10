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
#   ./scripts/benchmark-test.sh --compare-trino tpch  # compare SQE vs Trino output
#   ./scripts/benchmark-test.sh --compare-trino       # compare all benchmarks

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

# Trino comparison mode: start a Trino container and validate results against it
COMPARE_TRINO="${COMPARE_TRINO:-}"
TRINO_PORT="38080"
TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:465}"

# S3 credentials (match test stack)
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:19000}"
S3_REGION="${S3_REGION:-us-east-1}"

# Auth credentials (match test stack)
SQE_USERNAME="${SQE_USERNAME:-root}"
SQE_PASSWORD="${SQE_PASSWORD:-}"

# Parse options
ALL_BENCHMARKS=(tpch ssb tpcds tpcc tpce tpcbb clickbench)
BENCHMARKS=()
for arg in "$@"; do
    case "$arg" in
        --compare-trino) COMPARE_TRINO=1 ;;
        *) BENCHMARKS+=("$arg") ;;
    esac
done
if [ ${#BENCHMARKS[@]} -eq 0 ]; then
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

# ── Start Trino container (if --compare-trino) ──────────────
TRINO_CONTAINER=""
if [ -n "$COMPARE_TRINO" ]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Starting Trino container for comparison..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Get a Polaris OAuth token for Trino
    POLARIS_TOKEN=$(curl -sf -X POST "http://localhost:18181/api/catalog/v1/oauth/tokens" \
        -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

    # Get Polaris and RustFS container IPs on the test stack network
    POLARIS_IP=$(docker inspect sqlengine-polaris-1 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
    RUSTFS_IP=$(docker inspect sqlengine-rustfs-1 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')

    # Create Trino catalog config
    mkdir -p /tmp/trino-bench/catalog
    cat > /tmp/trino-bench/catalog/iceberg.properties << TRINOEOF
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://${POLARIS_IP}:8181/api/catalog
iceberg.rest-catalog.warehouse=test_warehouse
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=${POLARIS_TOKEN}
fs.native-s3.enabled=true
s3.endpoint=http://${RUSTFS_IP}:9000
s3.region=${S3_REGION}
s3.path-style-access=true
s3.aws-access-key=${S3_ACCESS_KEY}
s3.aws-secret-key=${S3_SECRET_KEY}
TRINOEOF

    cat > /tmp/trino-bench/config.properties << 'TRINOEOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
TRINOEOF

    # Stop any existing Trino container
    docker stop trino-bench 2>/dev/null || true
    sleep 1

    TRINO_CONTAINER=$(docker run -d --rm \
        --name trino-bench \
        -p "${TRINO_PORT}:8080" \
        -v /tmp/trino-bench/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties:ro \
        -v /tmp/trino-bench/config.properties:/etc/trino/config.properties:ro \
        "$TRINO_IMAGE")

    echo -n "Waiting for Trino..."
    for i in $(seq 1 60); do
        if curl -sf "http://localhost:${TRINO_PORT}/v1/info" >/dev/null 2>&1; then
            echo " ready"
            break
        fi
        if [ "$i" -eq 60 ]; then echo " TIMEOUT"; COMPARE_TRINO=""; fi
        echo -n "."
        sleep 2
    done
    # Extra time for Trino to fully initialize its catalogs
    sleep 10
    echo "  Trino ${TRINO_IMAGE} on port ${TRINO_PORT}"
fi

# ── Cleanup handler ───────────────────────────────────────────
cleanup() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Cleaning up..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    kill "$SQE_PID" 2>/dev/null || true
    wait "$SQE_PID" 2>/dev/null || true
    rm -f "$SQE_LOG_FILE"
    if [ -n "$TRINO_CONTAINER" ]; then
        docker stop trino-bench 2>/dev/null || true
    fi
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

# Track whether TPC-DS tables have been loaded (needed by TPC-BB)
TPCDS_LOADED=""

for BENCH in "${BENCHMARKS[@]}"; do
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Benchmark: $(echo "$BENCH" | tr '[:lower:]' '[:upper:]') (SF${BENCH_SCALE})"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # TPC-BB reuses all TPC-DS tables.  Ensure they are generated and loaded
    # before the TPC-BB-specific tables are added.
    if [ "$BENCH" = "tpcbb" ] && [ -z "$TPCDS_LOADED" ]; then
        echo ""
        echo "  [pre] TPC-BB requires TPC-DS tables — generating and loading..."
        "$BENCH_BIN" generate tpcds \
            --scale "$BENCH_SCALE" \
            --output "$BENCH_DATA_DIR" 2>&1 || true
        "$BENCH_BIN" load tpcds \
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
            --clean 2>&1 || {
            echo "  ✗ Failed to load TPC-DS prerequisite tables for TPC-BB"
            RESULTS+=("$BENCH: PREREQ_FAILED")
            TOTAL_ERROR=$((TOTAL_ERROR + 1))
            continue
        }
        TPCDS_LOADED=1
        echo "  ✓ TPC-DS tables loaded for TPC-BB"
    fi

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
    if [ "$BENCH" = "tpcds" ]; then TPCDS_LOADED=1; fi

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

    # ── Step 4 (optional): Compare with Trino ────────────────
    if [ -n "$COMPARE_TRINO" ]; then
        echo ""
        echo "  [4/4] Comparing SQE vs Trino..."

        # Refresh the Trino Polaris token (JWT tokens expire quickly).
        # Restart the container with a fresh token to avoid auth failures.
        FRESH_TOKEN=$(curl -sf -X POST "http://localhost:18181/api/catalog/v1/oauth/tokens" \
            -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null)
        if [ -n "$FRESH_TOKEN" ]; then
            POLARIS_IP=$(docker inspect sqlengine-polaris-1 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
            RUSTFS_IP=$(docker inspect sqlengine-rustfs-1 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
            cat > /tmp/trino-bench/catalog/iceberg.properties << REFRESHEOF
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://${POLARIS_IP}:8181/api/catalog
iceberg.rest-catalog.warehouse=test_warehouse
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=${FRESH_TOKEN}
fs.native-s3.enabled=true
s3.endpoint=http://${RUSTFS_IP}:9000
s3.region=${S3_REGION}
s3.path-style-access=true
s3.aws-access-key=${S3_ACCESS_KEY}
s3.aws-secret-key=${S3_SECRET_KEY}
REFRESHEOF
            docker stop trino-bench 2>/dev/null; sleep 2
            docker run -d --rm --name trino-bench -p "${TRINO_PORT}:8080" \
                -v /tmp/trino-bench/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties:ro \
                -v /tmp/trino-bench/config.properties:/etc/trino/config.properties:ro \
                "$TRINO_IMAGE" >/dev/null
            echo -n "  Refreshing Trino token..."
            for i in $(seq 1 30); do
                if curl -sf "http://localhost:${TRINO_PORT}/v1/info" >/dev/null 2>&1; then echo " ready"; break; fi
                sleep 2
            done
            # Wait for Trino's Iceberg catalog connector to fully initialize.
            # The /v1/info endpoint returns "starting":false before the catalog is ready.
            echo -n "  Waiting for Trino catalog..."
            for i in $(seq 1 30); do
                RESULT=$(trino --server "http://localhost:${TRINO_PORT}" --user admin --catalog iceberg \
                    --execute "SHOW SCHEMAS" --output-format CSV_UNQUOTED 2>/dev/null | head -1)
                if [ -n "$RESULT" ]; then echo " catalog ready"; break; fi
                if [ "$i" -eq 30 ]; then echo " TIMEOUT (catalog may not be ready)"; fi
                sleep 2
            done
        fi

        "$BENCH_BIN" compare "$BENCH" \
            --scale "$BENCH_SCALE" \
            --sqe-host "$BENCH_HOST" \
            --sqe-port "$BENCH_PORT_FLIGHT" \
            --sqe-username "$SQE_USERNAME" \
            --sqe-password "${SQE_PASSWORD:-}" \
            --trino-url "http://localhost:${TRINO_PORT}" \
            --trino-user admin \
            --output "benchmarks/results" 2>&1 || {
            echo "  ⚠ Comparison had errors (some queries may differ)"
        }
    fi

    # ── Clean up generated data for this benchmark ────────────
    # Keep tpcds data if tpcbb still needs it (tpcbb loads into tpcds namespace)
    if [ "$BENCH" = "tpcds" ] && [[ " ${BENCHMARKS[*]} " =~ " tpcbb " ]]; then
        echo "  (keeping tpcds data — needed by tpcbb)"
    else
        rm -rf "${BENCH_DATA_DIR:?}/$BENCH"
    fi
done

# ── Summary table ─────────────────────────────────────────────
echo ""
COMPARE_LABEL=""
if [ -n "$COMPARE_TRINO" ]; then COMPARE_LABEL=" + Trino comparison"; fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Benchmark Results (SF${BENCH_SCALE}, $(echo "$BENCH_PROTOCOL" | tr '[:lower:]' '[:upper:]')${COMPARE_LABEL})"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "  %-14s %5s %5s %5s %5s %5s %5s %8s\n" "Benchmark" "Pass" "Fail" "Diff" "Skip" "Error" "Total" "Time"
echo "  ─────────────────────────────────────────────────────────────────"

SUM_PASS=0; SUM_FAIL=0; SUM_DIFF=0; SUM_SKIP=0; SUM_ERROR=0; SUM_TOTAL=0; SUM_MS=0

for S in "${SUMMARIES[@]+"${SUMMARIES[@]}"}"; do
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
