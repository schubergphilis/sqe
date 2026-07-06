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
#   PROFILE=debug ./scripts/benchmark-test.sh      # debug build + target/debug/ binaries
#                                                  # (default: release; skips --release
#                                                  # for faster incremental rebuilds)
#   ./scripts/benchmark-test.sh --compare-trino tpch  # compare SQE vs Trino output
#   ./scripts/benchmark-test.sh --compare-trino       # compare all benchmarks
#
# External data source (skip the generate step, load pre-published parquet
# straight from an S3 bucket — see benchmark-publish-data.sh):
#   BENCH_DATA_SOURCE=s3://sqe-benchmark \
#   BENCH_S3_ENDPOINT=https://s3.example.com \
#   BENCH_S3_PROFILE=storagegrid \
#   BENCH_SCALE=0.1 ./scripts/benchmark-test.sh tpch
#
# External warehouse (Iceberg tables on an external S3 endpoint instead of
# RustFS; with --compare-trino both engines then read the same endpoint).
# Convention: sqe-benchmark holds the generated source data, sqe-testlake
# is the Polaris-connected warehouse:
#   BENCH_WAREHOUSE=external \
#   BENCH_WAREHOUSE_LOCATION=s3://sqe-testlake \
#   BENCH_DATA_SOURCE=s3://sqe-benchmark \
#   BENCH_S3_ENDPOINT=https://s3.example.com \
#   BENCH_S3_PROFILE=storagegrid \
#   BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpch ssb

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
TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:481}"
# Optional container memory cap (e.g. TRINO_MEMORY=12g). The image sizes
# the JVM heap at 80% of container memory; without a cap that is 80% of
# the HOST's RAM — on a shared box that starves the coordinator and the
# comparison. Leave empty for the previous unbounded behavior.
TRINO_MEMORY="${TRINO_MEMORY:-}"

# S3 credentials (match test stack)
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:19000}"
S3_REGION="${S3_REGION:-us-east-1}"

# Auth credentials (match test stack)
SQE_USERNAME="${SQE_USERNAME:-root}"
SQE_PASSWORD="${SQE_PASSWORD:-}"

# ── External data source (optional) ──────────────────────────
# BENCH_DATA_SOURCE=s3://<bucket> reads pre-published parquet from an S3
# bucket instead of generating locally (see benchmark-publish-data.sh).
# The generate step is skipped and the load's read_parquet() call gets the
# bucket's credentials inline. The warehouse (where the Iceberg tables are
# written) is unaffected — it stays on the test stack's RustFS.
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

# ── External warehouse (optional) ────────────────────────────
# BENCH_WAREHOUSE=external puts the Iceberg WAREHOUSE (where the loaded
# tables live) on an external S3 endpoint instead of RustFS: Polaris is
# recreated with matching credentials, the catalog's storage config
# points at the endpoint, SQE's [storage] block is rewritten in the temp
# config, and the Trino comparison container reads the same endpoint —
# both engines then fetch over the identical network path.
#   BENCH_WAREHOUSE=external \
#   BENCH_WAREHOUSE_LOCATION=s3://sqe-testlake/warehouse \
#   BENCH_S3_ENDPOINT=https://s3.example.com BENCH_S3_PROFILE=... \
#   ./scripts/benchmark-test.sh --compare-trino tpch
BENCH_WAREHOUSE="${BENCH_WAREHOUSE:-local}"
BENCH_WAREHOUSE_LOCATION="${BENCH_WAREHOUSE_LOCATION:-}"

case "$BENCH_WAREHOUSE" in
    local|external) ;;
    *)
        echo "ERROR: BENCH_WAREHOUSE must be 'local' or 'external', got: '$BENCH_WAREHOUSE'" >&2
        exit 1
        ;;
esac

if [ "$BENCH_WAREHOUSE" = "external" ]; then
    if [ -z "$BENCH_S3_ENDPOINT" ] || [ -z "$BENCH_WAREHOUSE_LOCATION" ]; then
        echo "ERROR: BENCH_WAREHOUSE=external requires BENCH_S3_ENDPOINT and BENCH_WAREHOUSE_LOCATION" >&2
        exit 1
    fi
    WH_S3_ACCESS_KEY="${WH_S3_ACCESS_KEY:-$(aws configure get aws_access_key_id --profile "$BENCH_S3_PROFILE")}"
    WH_S3_SECRET_KEY="${WH_S3_SECRET_KEY:-$(aws configure get aws_secret_access_key --profile "$BENCH_S3_PROFILE")}"
    if [ -z "$WH_S3_ACCESS_KEY" ] || [ -z "$WH_S3_SECRET_KEY" ]; then
        echo "ERROR: could not resolve S3 credentials from profile '$BENCH_S3_PROFILE'" >&2
        exit 1
    fi
fi

# Build profile: `release` (default) uses `cargo build --release` and
# runs binaries out of `target/release/`. `debug` skips `--release` for
# faster incremental rebuilds and runs out of `target/debug/`. The debug
# profile is slower at query time but dramatically faster to compile, so
# it is the right default when iterating on the coordinator between
# benchmark runs. Scale factors beyond SF1 should still use release.
PROFILE="${PROFILE:-release}"
case "$PROFILE" in
    release|debug) ;;
    *)
        echo "ERROR: PROFILE must be 'release' or 'debug', got: '$PROFILE'" >&2
        exit 1
        ;;
esac

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
echo "  Building sqe-bench (profile: $PROFILE)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ "$PROFILE" = "release" ]; then
    cargo build -p sqe-bench -p sqe-coordinator --release 2>&1
else
    cargo build -p sqe-bench -p sqe-coordinator 2>&1
fi
BENCH_BIN="$ROOT_DIR/target/$PROFILE/sqe-bench"
SQE_BIN="$ROOT_DIR/target/$PROFILE/sqe-coordinator"

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
if [ "$BENCH_WAREHOUSE" = "external" ]; then
    # Polaris performs the warehouse's S3 operations itself (metadata
    # writes, drop-with-purge), so it must carry the external endpoint
    # and credentials. Recreate so the env change applies; Polaris here
    # is in-memory, the bootstrap below repopulates it.
    export POLARIS_S3_ENDPOINT="$BENCH_S3_ENDPOINT"
    export POLARIS_S3_ACCESS_KEY="$WH_S3_ACCESS_KEY"
    export POLARIS_S3_SECRET_KEY="$WH_S3_SECRET_KEY"
    export POLARIS_S3_REGION="$BENCH_S3_REGION"
    # No STS on S3-compatible endpoints like StorageGRID: skip Polaris'
    # credential subscoping or CREATE TABLE fails with STS 405.
    export POLARIS_SKIP_SUBSCOPING=true
    docker compose -f "$COMPOSE_FILE" up -d --force-recreate polaris
fi
docker compose -f "$COMPOSE_FILE" up -d

# Bootstrap (creates bucket, warehouse, grants)
if [ "$BENCH_WAREHOUSE" = "external" ]; then
    WAREHOUSE_MODE=external \
    EXT_S3_ENDPOINT="$BENCH_S3_ENDPOINT" \
    WAREHOUSE_LOCATION="$BENCH_WAREHOUSE_LOCATION" \
        "$SCRIPT_DIR/bootstrap-test.sh"
else
    "$SCRIPT_DIR/bootstrap-test.sh"
fi
echo ""

# ── Start SQE coordinator in background ───────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting SQE coordinator..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
SQE_LOG_FILE="/tmp/sqe-bench-coord-$$.log"
SQE_CONFIG="$ROOT_DIR/tests/sqe-test.toml"

# Generate a temp coordinator config when anything is external:
# - External data source: read_parquet() carries an inline `endpoint =>`
#   override, and the coordinator's SSRF gate (issue #46) rejects endpoint
#   hosts that are not in `[storage.tvf] allowed_http_hosts`.
# - External warehouse: SQE reads/writes the Iceberg tables with its
#   [storage] credentials, so that block must point at the external
#   endpoint instead of RustFS.
if [ -n "$EXTERNAL_DATA" ] || [ "$BENCH_WAREHOUSE" = "external" ]; then
    ENDPOINT_HOST=$(python3 -c "from urllib.parse import urlparse; import sys; print(urlparse(sys.argv[1]).hostname or '')" "$BENCH_S3_ENDPOINT")
    if [ -z "$ENDPOINT_HOST" ]; then
        echo "ERROR: could not parse host from BENCH_S3_ENDPOINT=$BENCH_S3_ENDPOINT" >&2
        exit 1
    fi
    SQE_CONFIG_EXT="/tmp/sqe-bench-config-$$.toml"
    touch "$SQE_CONFIG_EXT" && chmod 600 "$SQE_CONFIG_EXT"
    WH_MODE="$BENCH_WAREHOUSE" \
    WH_ENDPOINT="$BENCH_S3_ENDPOINT" \
    WH_ACCESS_KEY="${WH_S3_ACCESS_KEY:-}" \
    WH_SECRET_KEY="${WH_S3_SECRET_KEY:-}" \
    WH_REGION="$BENCH_S3_REGION" \
    python3 - "$ROOT_DIR/tests/sqe-test.toml" "$SQE_CONFIG_EXT" "$ENDPOINT_HOST" <<'PYEOF'
import os
import re
import sys
src, dst, host = sys.argv[1:4]
text = open(src).read()
anchor = "[storage.tvf]\n"
assert anchor in text, "tests/sqe-test.toml is missing the [storage.tvf] section"
# Include the loopback hosts so local-endpoint TVF calls keep working:
# an explicit allowed_http_hosts REPLACES the (empty) default entirely.
text = text.replace(anchor, anchor + f'allowed_http_hosts = ["{host}", "localhost", "127.0.0.1"]\n', 1)
if os.environ.get("WH_MODE") == "external":
    subs = {
        "s3_endpoint": os.environ["WH_ENDPOINT"],
        "s3_access_key": os.environ["WH_ACCESS_KEY"],
        "s3_secret_key": os.environ["WH_SECRET_KEY"],
        "s3_region": os.environ["WH_REGION"],
    }
    for key, value in subs.items():
        pattern = re.compile(rf'^{key} = ".*"$', re.MULTILINE)
        assert pattern.search(text), f"tests/sqe-test.toml is missing '{key}' under [storage]"
        text = pattern.sub(f'{key} = "{value}"', text)
open(dst, "w").write(text)
PYEOF
    SQE_CONFIG="$SQE_CONFIG_EXT"
    if [ -n "$EXTERNAL_DATA" ]; then
        echo "External data source: $BENCH_DATA_SOURCE (endpoint host '$ENDPOINT_HOST' allowlisted)"
    fi
    if [ "$BENCH_WAREHOUSE" = "external" ]; then
        echo "External warehouse: $BENCH_WAREHOUSE_LOCATION via $BENCH_S3_ENDPOINT"
    fi
fi

# Fail fast if any coordinator port is already bound. A stale coordinator
# from a previous run silently steals the health probe below and the bench
# then runs against the wrong binary -- something we burned hours debugging
# when a pre-streaming coordinator was still holding :18080.
for port in 60051 18080 19090; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo " FAILED: port $port is already bound. Kill the stale process and retry:"
        echo "   lsof -nP -iTCP:$port -sTCP:LISTEN"
        exit 1
    fi
done

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

    # Get the Polaris container IP on the test stack network
    POLARIS_IP=$(docker inspect sqlengine-polaris-1 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')

    # Trino's S3 target follows the warehouse: RustFS (via container IP)
    # in local mode, the external endpoint otherwise — then SQE and Trino
    # fetch table data over the identical network path.
    if [ "$BENCH_WAREHOUSE" = "external" ]; then
        TRINO_S3_ENDPOINT="$BENCH_S3_ENDPOINT"
        TRINO_S3_ACCESS_KEY="$WH_S3_ACCESS_KEY"
        TRINO_S3_SECRET_KEY="$WH_S3_SECRET_KEY"
        TRINO_S3_REGION="$BENCH_S3_REGION"
    else
        RUSTFS_IP=$(docker inspect sqlengine-rustfs-1 --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
        TRINO_S3_ENDPOINT="http://${RUSTFS_IP}:9000"
        TRINO_S3_ACCESS_KEY="$S3_ACCESS_KEY"
        TRINO_S3_SECRET_KEY="$S3_SECRET_KEY"
        TRINO_S3_REGION="$S3_REGION"
    fi

    # Create Trino catalog config (may carry external credentials)
    mkdir -p /tmp/trino-bench/catalog
    touch /tmp/trino-bench/catalog/iceberg.properties
    chmod 600 /tmp/trino-bench/catalog/iceberg.properties
    cat > /tmp/trino-bench/catalog/iceberg.properties << TRINOEOF
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://${POLARIS_IP}:8181/api/catalog
iceberg.rest-catalog.warehouse=test_warehouse
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=${POLARIS_TOKEN}
fs.native-s3.enabled=true
s3.endpoint=${TRINO_S3_ENDPOINT}
s3.region=${TRINO_S3_REGION}
s3.path-style-access=true
s3.aws-access-key=${TRINO_S3_ACCESS_KEY}
s3.aws-secret-key=${TRINO_S3_SECRET_KEY}
TRINOEOF

    cat > /tmp/trino-bench/config.properties << 'TRINOEOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
# The image sizes the JVM heap at 80% of container memory (~13GB on a
# 16GB Docker VM), but query memory defaults to 30% of heap (~4GB).
# Raise it so SF10 hash joins are not unfairly memory-starved vs SQE.
query.max-memory=8GB
query.max-memory-per-node=8GB
TRINOEOF

    # Stop any existing Trino container
    docker stop trino-bench 2>/dev/null || true
    sleep 1

    TRINO_MEMORY_ARGS=()
    if [ -n "$TRINO_MEMORY" ]; then
        TRINO_MEMORY_ARGS=(--memory "$TRINO_MEMORY")
    fi
    TRINO_CONTAINER=$(docker run -d --rm \
        --name trino-bench \
        -p "${TRINO_PORT}:8080" \
        ${TRINO_MEMORY_ARGS[@]+"${TRINO_MEMORY_ARGS[@]}"} \
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
    # Wait for Trino's Iceberg catalog to fully initialize (needs ~15-20s after HTTP ready)
    echo -n "  Waiting for Trino catalog (20s)..."
    sleep 20
    echo " done"
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
    # Preserve the coordinator log so a post-mortem is possible after a
    # crash or OOM kill. Old /tmp files are evicted by the OS in due course,
    # and disk cost is negligible versus the debugging value on large-scale
    # benchmark runs.
    if [ -f "$SQE_LOG_FILE" ]; then
        echo "Coordinator log preserved at: $SQE_LOG_FILE"
    fi
    if [ -n "$TRINO_CONTAINER" ]; then
        docker stop trino-bench 2>/dev/null || true
    fi
    rm -f "${SQE_CONFIG_EXT:-}"
    # Don't tear down docker -- leave it for subsequent runs
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
        if [ -z "$EXTERNAL_DATA" ]; then
            "$BENCH_BIN" generate tpcds \
                --scale "$BENCH_SCALE" \
                --output "$BENCH_DATA_DIR" 2>&1 || true
        fi
        "$BENCH_BIN" load tpcds \
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
    if [ -n "$EXTERNAL_DATA" ]; then
        echo "  [1/3] Using pre-published data: $DATA_PATH/$BENCH/sf$BENCH_SCALE"
    else
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
    fi

    # ── Step 2: Load ──────────────────────────────────────────
    #
    # We retry the load step once on failure. Polaris occasionally returns a
    # 403 "Access Key Id does not exist" from RustFS when writing the first
    # metadata file for a large CTAS — the auth state recovers within a few
    # seconds. The `--clean` flag makes the load idempotent (drop + reload),
    # so a second attempt is safe; if both fail it's a real failure, not a
    # transient blip. Tracked at ~3% rate in the May-2026 SF1 sweeps.
    echo ""
    echo "  [2/3] Loading into SQE..."
    LOAD_START=$(date +%s)
    LOAD_ATTEMPTS=2
    LOAD_OK=0
    for attempt in $(seq 1 "$LOAD_ATTEMPTS"); do
        if "$BENCH_BIN" load "$BENCH" \
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
            --clean 2>&1; then
            LOAD_OK=1
            break
        fi
        if [ "$attempt" -lt "$LOAD_ATTEMPTS" ]; then
            echo "  ⚠ Load attempt $attempt failed, retrying after 3s..."
            sleep 3
        fi
    done
    if [ "$LOAD_OK" -ne 1 ]; then
        echo "  ✗ Load FAILED for $BENCH after $LOAD_ATTEMPTS attempts"
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

        # Verify Trino is still running and responsive.
        if ! curl -sf "http://localhost:${TRINO_PORT}/v1/info" >/dev/null 2>&1; then
            echo "  ⚠ Trino not reachable, skipping comparison"
        else
            "$BENCH_BIN" compare "$BENCH" \
                --scale "$BENCH_SCALE" \
                --sqe-host "$BENCH_HOST" \
                --sqe-port "$BENCH_PORT_FLIGHT" \
                --sqe-username "${SQE_USERNAME:-root}" \
                --sqe-password "${SQE_PASSWORD:-s3cr3t}" \
                --trino-url "http://localhost:${TRINO_PORT}" \
                --trino-user admin \
                --output "benchmarks/results" 2>&1 || {
                echo "  ⚠ Comparison had errors (some queries may differ)"
            }
        fi
    fi

    # ── Clean up generated data for this benchmark ────────────
    # Keep tpcds data if tpcbb still needs it (tpcbb loads into tpcds namespace)
    if [ -n "$EXTERNAL_DATA" ]; then
        : # nothing staged locally
    elif [ "$BENCH" = "tpcds" ] && [[ " ${BENCHMARKS[*]} " =~ " tpcbb " ]]; then
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
