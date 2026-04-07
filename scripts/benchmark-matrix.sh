#!/usr/bin/env bash
set -euo pipefail

# ══════════════════════════════════════════════════════════════════════
#  SQE Benchmark Matrix
# ══════════════════════════════════════════════════════════════════════
#
#  Runs all benchmark suites across multiple deployment configurations:
#    1. single-512mb  — single-node, 512MB memory, spill stress test
#    2. single-8gb    — single-node, 8GB memory, baseline
#    3. distributed-2w — coordinator + 2 workers
#    4. distributed-4w — coordinator + 4 workers (optional, needs more RAM)
#
#  Data is generated and loaded ONCE, then reused across all configs.
#
#  Usage:
#    ./scripts/benchmark-matrix.sh                    # all configs, all suites
#    ./scripts/benchmark-matrix.sh --configs single   # single-node only
#    ./scripts/benchmark-matrix.sh --suites tpch,ssb  # specific suites
#    ./scripts/benchmark-matrix.sh --scale 10         # SF10 (default: 1)
#    ./scripts/benchmark-matrix.sh --skip-load        # skip data generation/loading
#    ./scripts/benchmark-matrix.sh --quick            # single-512mb + single-8gb, tpch only
#
# ══════════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ─────────────────────────────────────────────────────────
SCALE="${BENCH_SCALE:-1}"
DATA_DIR="${BENCH_DATA_DIR:-/tmp/sqe-bench-data}"
ALL_SUITES="tpch tpcds ssb tpcc tpce"
ALL_CONFIGS="single-512mb single-8gb distributed-2w distributed-4w"
SUITES="$ALL_SUITES"
CONFIGS="$ALL_CONFIGS"
SKIP_LOAD=false
QUICK=false
AUTO_YES=false

# Single-node connection (native coordinator, test stack ports)
SINGLE_HOST="localhost"
SINGLE_PORT="60051"
SINGLE_USER="root"
SINGLE_PASS="s3cr3t"
SINGLE_CATALOG="test_warehouse"

# Distributed connection (Docker coordinator, mapped ports)
DIST_HOST="localhost"
DIST_PORT="60051"
DIST_USER="root"
DIST_PASS="s3cr3t"
DIST_CATALOG="test_warehouse"
DIST_METRICS_PORT="29090"

# ── Parse args ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --configs)  CONFIGS="${2//,/ }"; shift 2 ;;
        --suites)   SUITES="${2//,/ }"; shift 2 ;;
        --scale)    SCALE="$2"; shift 2 ;;
        --skip-load) SKIP_LOAD=true; shift ;;
        --quick)    QUICK=true; shift ;;
        --yes|-y)   AUTO_YES=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--configs single-512mb,single-8gb,...] [--suites tpch,ssb,...] [--scale N] [--skip-load] [--quick]"
            echo ""
            echo "Configs: single-512mb, single-8gb, distributed-2w, distributed-4w"
            echo "Suites:  tpch, tpcds, ssb, tpcc, tpce"
            echo ""
            echo "Examples:"
            echo "  $0                              # full matrix"
            echo "  $0 --quick                      # single-node configs, TPC-H only"
            echo "  $0 --configs single --suites tpch,ssb --scale 10"
            echo "  $0 --yes                           # non-interactive (skip prompts)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if $QUICK; then
    CONFIGS="single-512mb single-8gb"
    SUITES="tpch"
fi

# Expand "single" shorthand
CONFIGS="${CONFIGS//single /single-512mb single-8gb }"
CONFIGS="${CONFIGS//distributed /distributed-2w distributed-4w }"

cd "$ROOT_DIR"

# ══════════════════════════════════════════════════════════════════════
#  Step 0: System checks
# ══════════════════════════════════════════════════════════════════════

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SQE Benchmark Matrix"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Scale factor: SF${SCALE}"
echo "  Suites:       $SUITES"
echo "  Configs:      $CONFIGS"
echo ""

# ── Memory check ─────────────────────────────────────────────────────
TOTAL_RAM_MB=$(sysctl -n hw.memsize 2>/dev/null | awk '{print int($1/1024/1024)}' || free -m 2>/dev/null | awk '/^Mem:/{print $2}' || echo "0")
TOTAL_RAM_GB=$((TOTAL_RAM_MB / 1024))

echo "System memory: ${TOTAL_RAM_GB}GB"

# Check Docker memory usage
DOCKER_MEM_MB=0
if command -v docker &>/dev/null; then
    DOCKER_MEM_MB=$(docker stats --no-stream --format "{{.MemUsage}}" 2>/dev/null \
        | awk -F'/' '{print $1}' \
        | awk '{
            val = $0
            if (index(val, "GiB")) { gsub(/[^0-9.]/, "", val); sum += val * 1024 }
            else if (index(val, "MiB")) { gsub(/[^0-9.]/, "", val); sum += val }
            else if (index(val, "KiB")) { gsub(/[^0-9.]/, "", val); sum += val / 1024 }
        } END { printf "%d", sum }' 2>/dev/null || echo "0")
    DOCKER_MEM_MB=${DOCKER_MEM_MB:-0}
fi
DOCKER_MEM_GB=$((DOCKER_MEM_MB / 1024))
AVAIL_GB=$((TOTAL_RAM_GB - DOCKER_MEM_GB - 4))  # 4GB OS overhead

echo "Docker memory: ~${DOCKER_MEM_GB}GB in use"
echo "Available:     ~${AVAIL_GB}GB"
echo ""

# Check if we need to warn about memory
NEEDS_DISTRIBUTED=false
for cfg in $CONFIGS; do
    if [[ "$cfg" == distributed-* ]]; then
        NEEDS_DISTRIBUTED=true
        break
    fi
done

if $NEEDS_DISTRIBUTED && [[ $AVAIL_GB -lt 12 ]]; then
    echo "WARNING: Distributed configs need ~12-16GB free."
    echo "Current Docker containers are using ~${DOCKER_MEM_GB}GB."
    echo ""

    # Find non-essential Docker stacks
    # Find containers NOT part of the SQE test/bench stack or Docker internals
    OTHER_STACKS=$(docker ps --format "{{.Names}}" 2>/dev/null \
        | grep -vE "^(sqlengine-|buildx_)" \
        | head -10 || true)

    if [[ -n "$OTHER_STACKS" ]]; then
        echo "Other Docker containers running:"
        echo "$OTHER_STACKS" | sed 's/^/  - /'
        echo ""
        if $AUTO_YES; then STOP_OTHERS="y"; else read -rp "Stop other Docker containers to free memory? [y/N] " STOP_OTHERS; fi
        if [[ "$STOP_OTHERS" =~ ^[Yy]$ ]]; then
            echo "Stopping non-SQE containers..."
            # Find compose projects that aren't ours
            # Collect unique compose projects to stop (never stop sqlengine project)
            PROJECTS_TO_STOP=""
            for container in $OTHER_STACKS; do
                project=$(docker inspect "$container" --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null || echo "")
                if [[ -n "$project" && "$project" != "sqlengine" ]]; then
                    PROJECTS_TO_STOP="$PROJECTS_TO_STOP $project"
                fi
            done
            # Deduplicate and stop each project
            for project in $(echo "$PROJECTS_TO_STOP" | tr ' ' '\n' | sort -u); do
                echo "  Stopping project: $project"
                docker compose -p "$project" stop 2>/dev/null || true
            done
            echo ""
            # Recheck
            sleep 2
            DOCKER_MEM_MB=$(docker stats --no-stream --format "{{.MemUsage}}" 2>/dev/null \
                | grep -oE '[0-9.]+GiB|[0-9.]+MiB' \
                | awk '{
                    if (index($0, "GiB")) { sum += $0 * 1024 }
                    else if (index($0, "MiB")) { sum += $0 }
                } END { print int(sum) }' || echo "0")
            DOCKER_MEM_GB=$((DOCKER_MEM_MB / 1024))
            AVAIL_GB=$((TOTAL_RAM_GB - DOCKER_MEM_GB - 4))
            echo "Updated available memory: ~${AVAIL_GB}GB"
        fi
    fi

    if [[ $AVAIL_GB -lt 12 ]]; then
        echo ""
        echo "WARNING: Still only ${AVAIL_GB}GB free. Distributed benchmarks may be slow or OOM."
        if $AUTO_YES; then CONTINUE="y"; else read -rp "Continue anyway? [y/N] " CONTINUE; fi
        if [[ ! "$CONTINUE" =~ ^[Yy]$ ]]; then
            echo "Aborting. Free memory or use --configs single to skip distributed tests."
            exit 1
        fi
    fi
fi

# ══════════════════════════════════════════════════════════════════════
#  Step 1: Build
# ══════════════════════════════════════════════════════════════════════

echo ""
echo "Building sqe-bench + sqe-coordinator (release)..."
cargo build --release -p sqe-bench -p sqe-coordinator 2>&1 | tail -3
BENCH_BIN="$ROOT_DIR/target/release/sqe-bench"
COORD_BIN="$ROOT_DIR/target/release/sqe-coordinator"
echo ""

# ══════════════════════════════════════════════════════════════════════
#  Step 2: Ensure test stack is running
# ══════════════════════════════════════════════════════════════════════

echo "Ensuring test stack (Polaris + RustFS) is running..."
docker compose -f docker-compose.test.yml up -d 2>&1 | tail -3

# Wait for Polaris
echo -n "Waiting for Polaris..."
for i in $(seq 1 60); do
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "http://localhost:18181/api/catalog/v1/oauth/tokens" \
        -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" 2>/dev/null || echo "000")
    if [ "$HTTP" = "200" ]; then
        echo " ready."
        break
    fi
    echo -n "."
    sleep 2
done

# Bootstrap (idempotent)
"$SCRIPT_DIR/bootstrap-test.sh" 2>&1 | tail -3
echo ""

# ══════════════════════════════════════════════════════════════════════
#  Step 3: Generate and load ALL benchmark data (once)
# ══════════════════════════════════════════════════════════════════════

if ! $SKIP_LOAD; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Loading benchmark data (SF${SCALE}) — all suites"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Start a temporary single-node coordinator for data loading
    # (uses 8GB config so large CTAS queries don't OOM)
    lsof -ti :60051 2>/dev/null | xargs kill -9 2>/dev/null || true
    sleep 1
    "$COORD_BIN" tests/benchmark-matrix/single-8gb.toml > /tmp/sqe-bench-load.log 2>&1 &
    LOAD_PID=$!
    echo "Started coordinator for loading (PID: $LOAD_PID)"

    # Wait for coordinator
    for i in $(seq 1 30); do
        if curl -s -o /dev/null http://localhost:19090/metrics 2>/dev/null; then
            break
        fi
        sleep 1
    done

    for SUITE in $SUITES; do
        echo ""
        echo "  ── Generating $SUITE SF${SCALE}..."
        "$BENCH_BIN" generate "$SUITE" --scale "$SCALE" --output "$DATA_DIR" 2>&1 | tail -3

        echo "  ── Loading $SUITE SF${SCALE}..."
        "$BENCH_BIN" load "$SUITE" \
            --scale "$SCALE" \
            --data "$DATA_DIR" \
            --host "$SINGLE_HOST" \
            --port "$SINGLE_PORT" \
            --username "$SINGLE_USER" \
            --password "$SINGLE_PASS" \
            --catalog "$SINGLE_CATALOG" \
            --clean 2>&1 | tail -5

        # Clean generated data to save disk
        rm -rf "${DATA_DIR:?}/$SUITE"
        echo "  Done: $SUITE"
    done

    # Stop the loading coordinator
    kill "$LOAD_PID" 2>/dev/null || true
    wait "$LOAD_PID" 2>/dev/null || true
    echo ""
    echo "All data loaded."
fi

# ══════════════════════════════════════════════════════════════════════
#  Step 4: Run benchmark matrix
# ══════════════════════════════════════════════════════════════════════

RESULTS_DIR="$ROOT_DIR/benchmarks/results"
METRICS_DIR="$ROOT_DIR/benchmarks/metrics"
mkdir -p "$RESULTS_DIR" "$METRICS_DIR"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")
SUMMARY_FILE="$RESULTS_DIR/matrix-${TIMESTAMP}.txt"

# ── Metrics snapshot helper ──────────────────────────────────────────
# Captures Prometheus metrics endpoint and saves to file.
# Usage: capture_metrics <port> <output_file>
capture_metrics() {
    local port="$1"
    local outfile="$2"
    curl -s "http://localhost:${port}/metrics" 2>/dev/null \
        | grep "^sqe_" \
        | sort > "$outfile" 2>/dev/null || true
}

# Computes delta between two metrics snapshots (before/after).
# Usage: metrics_delta <before_file> <after_file> <output_file>
metrics_delta() {
    local before="$1"
    local after="$2"
    local delta="$3"
    python3 -c "
import sys
before = {}
after = {}
for line in open('$before'):
    parts = line.strip().split(' ')
    if len(parts) == 2:
        before[parts[0]] = float(parts[1])
for line in open('$after'):
    parts = line.strip().split(' ')
    if len(parts) == 2:
        after[parts[0]] = float(parts[1])
with open('$delta', 'w') as f:
    for key in sorted(after.keys()):
        b = before.get(key, 0)
        a = after[key]
        d = a - b
        if d != 0:
            f.write(f'{key} {d:g}\n')
" 2>/dev/null || true
}

echo "" | tee "$SUMMARY_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$SUMMARY_FILE"
echo "  Benchmark Matrix Results — $(date)" | tee -a "$SUMMARY_FILE"
echo "  Scale factor: SF${SCALE}" | tee -a "$SUMMARY_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$SUMMARY_FILE"

# ── Start observability stack if available ────────────────────────────
if docker info &>/dev/null && [[ -f docker-compose.observability.yml ]]; then
    echo ""
    echo "Starting observability stack (VictoriaMetrics + Grafana)..."
    docker compose -f docker-compose.test.yml -f docker-compose.observability.yml up -d 2>&1 | tail -3
    sleep 3
    echo ""
    echo "  Grafana dashboard: http://localhost:13000/d/sqe-benchmark"
    echo "  VictoriaMetrics:   http://localhost:18428"
    echo "  (login: admin / admin)"
    echo ""
fi

run_single_config() {
    local CONFIG_NAME="$1"
    local CONFIG_FILE="tests/benchmark-matrix/${CONFIG_NAME}.toml"

    echo "" | tee -a "$SUMMARY_FILE"
    echo "── Config: $CONFIG_NAME ──────────────────────────────────────" | tee -a "$SUMMARY_FILE"

    # Kill any existing coordinator
    lsof -ti :60051 2>/dev/null | xargs kill -9 2>/dev/null || true
    sleep 1

    # Start coordinator
    "$COORD_BIN" "$CONFIG_FILE" > "/tmp/sqe-bench-${CONFIG_NAME}.log" 2>&1 &
    local COORD_PID=$!

    # Wait for ready
    for i in $(seq 1 30); do
        if curl -s -o /dev/null http://localhost:19090/metrics 2>/dev/null; then
            break
        fi
        sleep 1
    done

    # Verify coordinator is running
    if ! kill -0 "$COORD_PID" 2>/dev/null; then
        echo "  FAILED: coordinator did not start" | tee -a "$SUMMARY_FILE"
        return 1
    fi

    local MEM_LIMIT
    MEM_LIMIT=$(grep "memory_limit" "$CONFIG_FILE" | head -1 | sed 's/.*= *"//' | sed 's/".*//')
    echo "  Memory: $MEM_LIMIT, PID: $COORD_PID" | tee -a "$SUMMARY_FILE"

    for SUITE in $SUITES; do
        echo -n "  $SUITE SF${SCALE}: " | tee -a "$SUMMARY_FILE"

        # Capture metrics before
        local METRICS_BEFORE="${METRICS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-before.txt"
        local METRICS_AFTER="${METRICS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-after.txt"
        local METRICS_DELTA="${METRICS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-${TIMESTAMP}-delta.txt"
        capture_metrics 19090 "$METRICS_BEFORE"

        OUTPUT=$("$BENCH_BIN" test "$SUITE" \
            --scale "$SCALE" \
            --host "$SINGLE_HOST" \
            --port "$SINGLE_PORT" \
            --username "$SINGLE_USER" \
            --password "$SINGLE_PASS" \
            --catalog "$SINGLE_CATALOG" 2>&1 || true)

        # Capture metrics after and compute delta
        capture_metrics 19090 "$METRICS_AFTER"
        metrics_delta "$METRICS_BEFORE" "$METRICS_AFTER" "$METRICS_DELTA"

        # Extract summary line
        SUMMARY_LINE=$(echo "$OUTPUT" | grep "^BENCH_SUMMARY:" || echo "BENCH_SUMMARY:$SUITE:0:0:0:0:0:0:0")
        IFS=':' read -r _ _ PASS FAIL DIFF SKIP ERROR TOTAL TIME_MS <<< "$SUMMARY_LINE"
        TIME_S=$(echo "scale=1; ${TIME_MS:-0}/1000" | bc 2>/dev/null || echo "?")
        echo "${PASS:-0}/${TOTAL:-0} pass, ${ERROR:-0} error, ${TIME_S}s" | tee -a "$SUMMARY_FILE"

        # Show key metric deltas
        if [[ -s "$METRICS_DELTA" ]]; then
            grep -E "spill_count|spill_bytes|pruned|cache_hits|cache_misses|late_mat|memory_used" "$METRICS_DELTA" 2>/dev/null \
                | head -8 | sed 's/^/    /' | tee -a "$SUMMARY_FILE"
        fi

        # Rename result file with config tag
        LATEST_RESULT=$(ls -t "$RESULTS_DIR/${SUITE}-sf${SCALE}-"*.json 2>/dev/null | head -1 || true)
        if [[ -n "$LATEST_RESULT" ]]; then
            NEW_NAME="${RESULTS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-${TIMESTAMP}.json"
            cp "$LATEST_RESULT" "$NEW_NAME"
        fi
    done

    # Stop coordinator
    kill "$COORD_PID" 2>/dev/null || true
    wait "$COORD_PID" 2>/dev/null || true
}

run_distributed_config() {
    local CONFIG_NAME="$1"
    local NUM_WORKERS="${CONFIG_NAME##*-}"  # "2w" or "4w"
    NUM_WORKERS="${NUM_WORKERS%w}"          # "2" or "4"

    local COMPOSE_FILE
    if [[ "$NUM_WORKERS" == "2" ]]; then
        COMPOSE_FILE="docker-compose.bench-2w.yml"
    elif [[ "$NUM_WORKERS" == "4" ]]; then
        COMPOSE_FILE="docker-compose.bench-4w.yml"
    else
        echo "  UNSUPPORTED: ${NUM_WORKERS} workers" | tee -a "$SUMMARY_FILE"
        return 1
    fi

    echo "" | tee -a "$SUMMARY_FILE"
    echo "── Config: $CONFIG_NAME (${NUM_WORKERS} workers) ──────────────" | tee -a "$SUMMARY_FILE"

    # Stop any single-node coordinator
    lsof -ti :60051 2>/dev/null | xargs kill -9 2>/dev/null || true
    sleep 1

    # Check available memory before launching distributed stack
    if command -v sysctl &>/dev/null; then
        local SYS_RAM_GB=$(($(sysctl -n hw.memsize 2>/dev/null | awk '{print int($1/1024/1024/1024)}') ))
        local DOCKER_USED_GB=$(docker stats --no-stream --format "{{.MemUsage}}" 2>/dev/null \
            | awk -F'/' '{print $1}' \
            | awk '{val=$0; if(index(val,"GiB")){gsub(/[^0-9.]/,"",val);sum+=val}} END{printf "%d",sum}' 2>/dev/null || echo "0")
        local FREE_GB=$((SYS_RAM_GB - DOCKER_USED_GB - 8))
        local NEEDED_GB=$(( (NUM_WORKERS + 1) * 4 ))  # rough: 4GB per container
        if [[ $FREE_GB -lt $NEEDED_GB ]]; then
            echo "  WARNING: ~${FREE_GB}GB free, need ~${NEEDED_GB}GB for ${NUM_WORKERS} workers"
            echo "  Skipping $CONFIG_NAME to avoid Docker crash."
            echo "  $CONFIG_NAME: SKIPPED (insufficient memory)" | tee -a "$SUMMARY_FILE"
            return 0
        fi
    fi

    # Stop any previous distributed stacks to free memory
    echo "  Stopping any previous distributed stacks..."
    docker compose -f docker-compose.test.yml -f docker-compose.bench-2w.yml stop coordinator worker-1 worker-2 2>/dev/null || true
    docker compose -f docker-compose.test.yml -f docker-compose.bench-4w.yml stop coordinator worker-1 worker-2 worker-3 worker-4 2>/dev/null || true
    docker compose -f docker-compose.test.yml -f docker-compose.distributed.yml stop coordinator worker-1 worker-2 2>/dev/null || true
    sleep 2

    # Start distributed stack
    echo "  Starting distributed stack (${NUM_WORKERS} workers)..."
    docker compose -f docker-compose.test.yml -f "$COMPOSE_FILE" up -d --build 2>&1 | tail -3

    # Wait for coordinator
    echo -n "  Waiting for coordinator..."
    for i in $(seq 1 120); do
        if curl -s -o /dev/null "http://localhost:${DIST_METRICS_PORT}/metrics" 2>/dev/null; then
            echo " ready."
            break
        fi
        echo -n "."
        sleep 2
    done

    # Wait for workers to register
    sleep 10

    for SUITE in $SUITES; do
        echo -n "  $SUITE SF${SCALE}: " | tee -a "$SUMMARY_FILE"

        # Capture metrics before
        local METRICS_BEFORE="${METRICS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-before.txt"
        local METRICS_AFTER="${METRICS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-after.txt"
        local METRICS_DELTA="${METRICS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-${TIMESTAMP}-delta.txt"
        capture_metrics "$DIST_METRICS_PORT" "$METRICS_BEFORE"

        OUTPUT=$("$BENCH_BIN" test "$SUITE" \
            --scale "$SCALE" \
            --host "$DIST_HOST" \
            --port "$DIST_PORT" \
            --username "$DIST_USER" \
            --password "$DIST_PASS" \
            --catalog "$DIST_CATALOG" 2>&1 || true)

        # Capture metrics after and compute delta
        capture_metrics "$DIST_METRICS_PORT" "$METRICS_AFTER"
        metrics_delta "$METRICS_BEFORE" "$METRICS_AFTER" "$METRICS_DELTA"

        SUMMARY_LINE=$(echo "$OUTPUT" | grep "^BENCH_SUMMARY:" || echo "BENCH_SUMMARY:$SUITE:0:0:0:0:0:0:0")
        IFS=':' read -r _ _ PASS FAIL DIFF SKIP ERROR TOTAL TIME_MS <<< "$SUMMARY_LINE"
        TIME_S=$(echo "scale=1; ${TIME_MS:-0}/1000" | bc 2>/dev/null || echo "?")
        echo "${PASS:-0}/${TOTAL:-0} pass, ${ERROR:-0} error, ${TIME_S}s" | tee -a "$SUMMARY_FILE"

        # Show key metric deltas
        if [[ -s "$METRICS_DELTA" ]]; then
            grep -E "spill_count|spill_bytes|pruned|cache_hits|cache_misses|late_mat|memory_used" "$METRICS_DELTA" 2>/dev/null \
                | head -8 | sed 's/^/    /' | tee -a "$SUMMARY_FILE"
        fi

        LATEST_RESULT=$(ls -t "$RESULTS_DIR/${SUITE}-sf${SCALE}-"*.json 2>/dev/null | head -1 || true)
        if [[ -n "$LATEST_RESULT" ]]; then
            NEW_NAME="${RESULTS_DIR}/${SUITE}-sf${SCALE}-${CONFIG_NAME}-${TIMESTAMP}.json"
            cp "$LATEST_RESULT" "$NEW_NAME"
        fi
    done

    # Don't stop distributed stack — leave running for inspection
    echo "  Distributed stack left running for inspection."
    echo "  Stop manually: docker compose -f docker-compose.test.yml -f $COMPOSE_FILE down"
}

# ── Run each config ──────────────────────────────────────────────────

for CONFIG in $CONFIGS; do
    if [[ "$CONFIG" == single-* ]]; then
        run_single_config "$CONFIG"
    elif [[ "$CONFIG" == distributed-* ]]; then
        run_distributed_config "$CONFIG"
    else
        echo "Unknown config: $CONFIG" | tee -a "$SUMMARY_FILE"
    fi
done

# ══════════════════════════════════════════════════════════════════════
#  Step 5: Summary
# ══════════════════════════════════════════════════════════════════════

echo "" | tee -a "$SUMMARY_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$SUMMARY_FILE"
echo "  Matrix complete. Results in benchmarks/results/" | tee -a "$SUMMARY_FILE"
echo "  Metrics deltas in benchmarks/metrics/" | tee -a "$SUMMARY_FILE"
echo "  Summary:  $SUMMARY_FILE" | tee -a "$SUMMARY_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$SUMMARY_FILE"

# ── ASCII bar chart: total time per config ────────────────────────────
echo "" | tee -a "$SUMMARY_FILE"
echo "Total time per config:" | tee -a "$SUMMARY_FILE"

# Collect timing data from tagged result files
python3 -c "
import json, glob, sys, os

timestamp = '$TIMESTAMP'
results_dir = '$RESULTS_DIR'
configs = '$CONFIGS'.split()

# Collect total time per config
config_times = {}
for config in configs:
    total_ms = 0
    total_pass = 0
    total_queries = 0
    for f in sorted(glob.glob(os.path.join(results_dir, f'*-{config}-{timestamp}.json'))):
        try:
            with open(f) as fh:
                d = json.load(fh)
            queries = d.get('results', d.get('queries', []))
            for q in queries:
                ms = q.get('elapsed_ms', q.get('duration_ms', 0))
                total_ms += ms
                total_queries += 1
                if q.get('status') in ('PASS', 'pass', 'ok'):
                    total_pass += 1
        except:
            pass
    if total_queries > 0:
        config_times[config] = (total_ms, total_pass, total_queries)

if not config_times:
    print('  (no results found)')
    sys.exit(0)

max_ms = max(t[0] for t in config_times.values()) if config_times else 1
bar_width = 40

for config, (ms, passed, total) in config_times.items():
    secs = ms / 1000
    bar_len = int((ms / max_ms) * bar_width) if max_ms > 0 else 0
    bar = '#' * bar_len + '.' * (bar_width - bar_len)
    print(f'  {config:20s} [{bar}] {secs:6.1f}s  ({passed}/{total} pass)')
" 2>/dev/null | tee -a "$SUMMARY_FILE"

# ── ASCII bar chart: per-suite comparison across configs ──────────────
echo "" | tee -a "$SUMMARY_FILE"
echo "Per-suite timing comparison:" | tee -a "$SUMMARY_FILE"

python3 -c "
import json, glob, os

timestamp = '$TIMESTAMP'
results_dir = '$RESULTS_DIR'
configs = '$CONFIGS'.split()
suites = '$SUITES'.split()

for suite in suites:
    times = {}
    for config in configs:
        pattern = os.path.join(results_dir, f'{suite}-sf*-{config}-{timestamp}.json')
        for f in sorted(glob.glob(pattern)):
            try:
                with open(f) as fh:
                    d = json.load(fh)
                queries = d.get('results', d.get('queries', []))
                ms = sum(q.get('elapsed_ms', q.get('duration_ms', 0)) for q in queries)
                times[config] = ms / 1000
            except:
                pass
    if not times:
        continue
    max_s = max(times.values()) if times else 1
    print(f'  {suite}:')
    for config in configs:
        if config in times:
            s = times[config]
            bar_len = int((s / max_s) * 30) if max_s > 0 else 0
            bar = '#' * bar_len
            print(f'    {config:20s} {bar:30s} {s:6.1f}s')
    print()
" 2>/dev/null | tee -a "$SUMMARY_FILE"

# ── Metrics analytics report per config ───────────────────────────────
echo "" | tee -a "$SUMMARY_FILE"
echo "Metrics Analytics:" | tee -a "$SUMMARY_FILE"
echo "" | tee -a "$SUMMARY_FILE"

for CONFIG in $CONFIGS; do
    # Merge all suite deltas for this config into one
    MERGED=""
    for f in "$METRICS_DIR"/*-"${CONFIG}-${TIMESTAMP}"-delta.txt; do
        [[ -f "$f" ]] && MERGED="$MERGED $f"
    done
    [[ -z "$MERGED" ]] && continue

    python3 -c "
import sys, os

config = '$CONFIG'
files = '$MERGED'.strip().split()

# Sum all deltas across suites
totals = {}
for f in files:
    if not os.path.exists(f):
        continue
    for line in open(f):
        parts = line.strip().split(' ', 1)
        if len(parts) == 2:
            key, val = parts[0], float(parts[1])
            totals[key] = totals.get(key, 0) + val

if not totals:
    sys.exit(0)

def get(prefix):
    return {k: v for k, v in totals.items() if prefix in k}

def val(key, default=0):
    return totals.get(key, default)

def fmt_bytes(b):
    if b >= 1024**3: return f'{b/1024**3:.1f} GB'
    if b >= 1024**2: return f'{b/1024**2:.1f} MB'
    if b >= 1024: return f'{b/1024:.1f} KB'
    return f'{b:.0f} B'

def bar(value, max_val, width=20):
    if max_val <= 0: return '.' * width
    filled = int((value / max_val) * width)
    return '#' * min(filled, width) + '.' * max(width - filled, 0)

print(f'  {config}')
print(f'  {\"=\" * 60}')

# Memory
mem_used = val('sqe_coordinator_memory_used_bytes')
mem_limit = val('sqe_coordinator_memory_limit_bytes')
if mem_limit > 0:
    pct = (mem_used / mem_limit) * 100
    print(f'  Memory:  peak {fmt_bytes(mem_used)} / {fmt_bytes(mem_limit)} ({pct:.0f}%)')
    print(f'           [{bar(mem_used, mem_limit, 30)}]')

# Spill
sort_spills = val('sqe_sort_spill_count_total')
sort_bytes = val('sqe_sort_spill_bytes_total')
join_spills = val('sqe_join_spill_count_total')
join_bytes = val('sqe_join_spill_bytes_total')
if sort_spills > 0 or join_spills > 0:
    print(f'  Spill:   sort: {int(sort_spills)} events ({fmt_bytes(sort_bytes)})')
    print(f'           join: {int(join_spills)} events ({fmt_bytes(join_bytes)})')
else:
    print(f'  Spill:   none (all in-memory)')

# Cache
q_hits = val('sqe_cache_hits_total')
q_misses = val('sqe_cache_misses_total')
f_hits = val('sqe_footer_cache_hits')
f_misses = val('sqe_footer_cache_misses')
if q_hits + q_misses > 0:
    q_rate = q_hits / (q_hits + q_misses) * 100
    print(f'  Cache:   query: {int(q_hits)} hits / {int(q_misses)} misses ({q_rate:.0f}% hit rate)')
else:
    print(f'  Cache:   query: no activity')
if f_hits + f_misses > 0:
    f_rate = f_hits / (f_hits + f_misses) * 100
    print(f'           footer: {int(f_hits)} hits / {int(f_misses)} misses ({f_rate:.0f}% hit rate)')
    total_reads = f_hits + f_misses
    print(f'           [{bar(f_hits, total_reads, 30)}] footer cache')

# Pruning
minmax = val('sqe_files_pruned_minmax_total')
bloom = val('sqe_files_pruned_bloom_total')
pages = val('sqe_pages_pruned_index_total')
if minmax > 0 or bloom > 0 or pages > 0:
    print(f'  Pruning: {int(minmax)} files (min/max), {int(bloom)} files (bloom), {int(pages)} pages (index)')
else:
    print(f'  Pruning: none')

# Late materialization
pred_bytes = val('sqe_late_mat_bytes_predicate_total')
proj_bytes = val('sqe_late_mat_bytes_projection_total')
if pred_bytes > 0 or proj_bytes > 0:
    total_io = pred_bytes + proj_bytes
    savings = (1 - total_io / (total_io * 2)) * 100 if total_io > 0 else 0
    print(f'  Late mat: predicate I/O: {fmt_bytes(pred_bytes)}, projection I/O: {fmt_bytes(proj_bytes)}')

# Queries
queries = val('sqe_query_count_total{status=\"success\"}', val('sqe_query_count_total'))
rows = val('sqe_rows_returned_total')
if queries > 0:
    print(f'  Queries: {int(queries)} executed, {int(rows)} rows returned')

# Workers
frags = val('sqe_worker_fragments_executed_total')
w_bytes = val('sqe_worker_bytes_read_total')
if frags > 0:
    print(f'  Workers: {int(frags)} fragments, {fmt_bytes(w_bytes)} read from S3')

# Shuffle
shuf_sent = val('sqe_shuffle_bytes_sent_total')
shuf_recv = val('sqe_shuffle_bytes_received_total')
if shuf_sent > 0 or shuf_recv > 0:
    print(f'  Shuffle: {fmt_bytes(shuf_sent)} sent, {fmt_bytes(shuf_recv)} received')

print()
" 2>/dev/null | tee -a "$SUMMARY_FILE"
done

echo "" | tee -a "$SUMMARY_FILE"
echo "Tagged results (config in filename):"
ls -1 "$RESULTS_DIR"/*-"${TIMESTAMP}".json 2>/dev/null | sed 's|.*/||' | sed 's/^/  /'
echo ""
echo "Metrics deltas:"
ls -1 "$METRICS_DIR"/*-"${TIMESTAMP}"-delta.txt 2>/dev/null | sed 's|.*/||' | sed 's/^/  /'

echo ""
echo "To commit results for historical tracking:"
echo "  git add benchmarks/results/*-${TIMESTAMP}.json benchmarks/metrics/*-${TIMESTAMP}-delta.txt"
echo "  git commit -m 'bench: benchmark matrix SF${SCALE} $(date +%Y-%m-%d)'"
echo ""

# ── Grafana reminder ─────────────────────────────────────────────────
if docker info &>/dev/null && docker ps --format "{{.Names}}" 2>/dev/null | grep -q "grafana"; then
    echo "Grafana dashboard still running: http://localhost:13000/d/sqe-benchmark"
    echo ""
fi

# ── Cleanup prompt ───────────────────────────────────────────────────
echo "Services still running:"
docker ps --format "  {{.Names}}: {{.Status}}" 2>/dev/null | grep -E "sqlengine|victoriametrics|grafana" || echo "  (none)"
echo ""
if ! $AUTO_YES; then
    read -rp "Stop all benchmark services (SQE + observability)? [y/N] " STOP_ALL
    if [[ "$STOP_ALL" =~ ^[Yy]$ ]]; then
        echo "Stopping all benchmark services..."
        lsof -ti :60051 2>/dev/null | xargs kill -9 2>/dev/null || true
        docker compose -f docker-compose.test.yml -f docker-compose.observability.yml down 2>/dev/null || true
        docker compose -f docker-compose.test.yml -f docker-compose.bench-2w.yml down 2>/dev/null || true
        docker compose -f docker-compose.test.yml -f docker-compose.bench-4w.yml down 2>/dev/null || true
        echo "Done."
    else
        echo "Services left running. Stop manually when done."
    fi
fi
