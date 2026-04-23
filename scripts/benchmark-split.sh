#!/usr/bin/env bash
set -uo pipefail

# Split-phase benchmark runner. Same end result as benchmark-test.sh but
# structured so each phase (generate / load / test) can be retried
# independently and data is generated into tmpfs for speed.
#
# Key differences vs benchmark-test.sh:
#   - Generated data goes to /dev/shm by default (fast, no disk writes)
#   - One SQE coordinator process stays up for the whole run (warm caches)
#   - Per-benchmark log file under $LOG_DIR/<bench>.log
#   - Non-fatal failures: a load or test failure does not abort the loop;
#     subsequent benchmarks still run. Summary at the end shows which
#     benchmarks passed/failed/skipped.
#   - Configurable client-side timeout via BENCH_CLIENT_TIMEOUT_SECS for
#     long SF100+ loads that exceed the default 30-minute transport cap.
#
# Usage:
#   ./scripts/benchmark-split.sh                     # all benchmarks, SF from env
#   ./scripts/benchmark-split.sh tpch                # single benchmark
#   ./scripts/benchmark-split.sh tpch ssb            # subset
#   BENCH_SCALE=1000 ./scripts/benchmark-split.sh tpch
#   BENCH_GEN_THREADS=16 ./scripts/benchmark-split.sh
#   BENCH_DATA_DIR=/mnt/fast/sf100 ./scripts/benchmark-split.sh   # override tmpfs
#   BENCH_CLIENT_TIMEOUT_SECS=7200 ./scripts/benchmark-split.sh   # 2 hour cap
#   BENCH_SKIP_GENERATE=1 ./scripts/benchmark-split.sh            # reuse existing data
#   BENCH_SKIP_LOAD=1     ./scripts/benchmark-split.sh            # test-only after a prior load
#   BENCH_SKIP_TEST=1     ./scripts/benchmark-split.sh            # generate + load only

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.test.yml"

# ── Configuration ─────────────────────────────────────────────
BENCH_SCALE="${BENCH_SCALE:-100}"
BENCH_GEN_THREADS="${BENCH_GEN_THREADS:-$(nproc 2>/dev/null || echo 8)}"
BENCH_DATA_DIR="${BENCH_DATA_DIR:-/dev/shm/sqe-bench-split-data}"
BENCH_CLIENT_TIMEOUT_SECS="${BENCH_CLIENT_TIMEOUT_SECS:-1800}"
BENCH_SKIP_GENERATE="${BENCH_SKIP_GENERATE:-}"
BENCH_SKIP_LOAD="${BENCH_SKIP_LOAD:-}"
BENCH_SKIP_TEST="${BENCH_SKIP_TEST:-}"

PROFILE="${PROFILE:-release}"
case "$PROFILE" in
    release|debug) ;;
    *) echo "ERROR: PROFILE must be 'release' or 'debug', got: '$PROFILE'" >&2; exit 1 ;;
esac
BENCH_BIN="$ROOT_DIR/target/$PROFILE/sqe-bench"
SQE_BIN="$ROOT_DIR/target/$PROFILE/sqe-coordinator"

# S3 + auth (match docker-compose.test.yml)
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:19000}"
S3_REGION="${S3_REGION:-us-east-1}"
SQE_USERNAME="${SQE_USERNAME:-root}"
SQE_PASSWORD="${SQE_PASSWORD:-}"

# ── Parse benchmarks from args ────────────────────────────────
ALL_BENCHMARKS=(tpch ssb tpcds tpcc tpce tpcbb clickbench)
if [ "$#" -eq 0 ]; then
    BENCHMARKS=("${ALL_BENCHMARKS[@]}")
else
    BENCHMARKS=("$@")
fi

# ── Log directory ─────────────────────────────────────────────
LOG_DIR="/tmp/sqe-bench-split-$(date +%Y%m%dT%H%M%S)"
mkdir -p "$LOG_DIR" "$BENCH_DATA_DIR"

cd "$ROOT_DIR"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SQE split-phase benchmark runner"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Scale factor:    SF${BENCH_SCALE}"
echo "  Benchmarks:      ${BENCHMARKS[*]}"
echo "  Profile:         $PROFILE"
echo "  Data dir:        $BENCH_DATA_DIR"
echo "  Generate threads: $BENCH_GEN_THREADS"
echo "  Client timeout:  ${BENCH_CLIENT_TIMEOUT_SECS}s"
echo "  Log dir:         $LOG_DIR"
[ -n "$BENCH_SKIP_GENERATE" ] && echo "  ⚠ Skipping generate phase"
[ -n "$BENCH_SKIP_LOAD" ]     && echo "  ⚠ Skipping load phase"
[ -n "$BENCH_SKIP_TEST" ]     && echo "  ⚠ Skipping test phase"
echo ""

# ── Build if needed ───────────────────────────────────────────
if [ ! -x "$BENCH_BIN" ] || [ ! -x "$SQE_BIN" ]; then
    echo "Building sqe-bench + sqe-coordinator (profile: $PROFILE)..."
    if [ "$PROFILE" = "release" ]; then
        cargo build -p sqe-bench -p sqe-coordinator --release
    else
        cargo build -p sqe-bench -p sqe-coordinator
    fi
fi

# ── Start stack + coord (once for the whole run) ──────────────
echo "Starting test stack..."
docker compose -f "$COMPOSE_FILE" up -d >/dev/null
"$SCRIPT_DIR/bootstrap-test.sh"

# Kill any stale coord.
#
# Previous implementation used `pkill -f "$SQE_BIN"` where $SQE_BIN is
# the absolute path. That misses coords started with a relative path
# (e.g. `./target/release/sqe-coordinator` from an earlier shell). We
# saw this bite: the script spawned a "new" coord that tried to bind
# already-held ports (:18080, :19090) and partially failed, then
# queries landed inconsistently on the old vs new coord.
#
# Match by the binary name only, which catches both relative and
# absolute invocations. Escalate to SIGKILL if the first SIGTERM
# doesn't take effect within 3 s.
pkill -f 'sqe-coordinator($| )' 2>/dev/null || true
for _ in 1 2 3; do
    pgrep -f 'sqe-coordinator($| )' >/dev/null 2>&1 || break
    sleep 1
done
if pgrep -f 'sqe-coordinator($| )' >/dev/null 2>&1; then
    echo "WARN: sqe-coordinator did not exit on SIGTERM, sending SIGKILL" >&2
    pkill -9 -f 'sqe-coordinator($| )' 2>/dev/null || true
    sleep 2
fi

for port in 60051 18080 19090; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "ERROR: port $port already bound after coord cleanup. Check who owns it:" >&2
        echo "  lsof -nP -iTCP:$port -sTCP:LISTEN" >&2
        exit 1
    fi
done

SQE_LOG="$LOG_DIR/coord.log"
echo "Starting SQE coordinator (log: $SQE_LOG)..."
RUST_LOG="${RUST_LOG:-sqe=info,warn}" \
    "$SQE_BIN" "$ROOT_DIR/tests/sqe-test.toml" \
    > "$SQE_LOG" 2>&1 &
SQE_PID=$!

# Wait for coord ready
echo -n "Waiting for coord..."
for i in $(seq 1 300); do
    if curl -sf "http://localhost:18080/v1/info" >/dev/null 2>&1; then
        echo " ready (PID $SQE_PID)"
        break
    fi
    if ! kill -0 "$SQE_PID" 2>/dev/null; then
        echo " FAILED (coord exited)"
        tail -20 "$SQE_LOG"
        exit 1
    fi
    [ "$i" -eq 300 ] && { echo " TIMEOUT"; kill "$SQE_PID" 2>/dev/null; exit 1; }
    echo -n "."
    sleep 1
done

cleanup() {
    echo ""
    echo "Cleaning up..."
    kill "$SQE_PID" 2>/dev/null
    wait "$SQE_PID" 2>/dev/null
    echo "  Coord log:  $SQE_LOG"
    echo "  Bench logs: $LOG_DIR/<bench>.log"
}
trap cleanup EXIT INT TERM

# ── Phase runners ─────────────────────────────────────────────
# Each function returns 0 on success, non-zero on failure.

run_generate() {
    local bench=$1 log=$2
    # tpcbb reuses tpcds data; skip its generate step
    [ "$bench" = "tpcbb" ] && { echo "  [gen] skip (tpcbb reuses tpcds)"; return 0; }
    echo "  [gen] $bench (SF$BENCH_SCALE, threads=$BENCH_GEN_THREADS)"
    BENCH_GEN_THREADS=$BENCH_GEN_THREADS \
      "$BENCH_BIN" generate "$bench" \
        --scale "$BENCH_SCALE" \
        --output "$BENCH_DATA_DIR" \
        --threads "$BENCH_GEN_THREADS" >>"$log" 2>&1
}

run_load() {
    local bench=$1 log=$2
    echo "  [load] $bench"
    BENCH_CLIENT_TIMEOUT_SECS=$BENCH_CLIENT_TIMEOUT_SECS \
      "$BENCH_BIN" load "$bench" \
        --scale "$BENCH_SCALE" \
        --data "$BENCH_DATA_DIR" \
        --protocol flight \
        --host localhost --port 60051 \
        --username "$SQE_USERNAME" --password "$SQE_PASSWORD" \
        --s3-endpoint "$S3_ENDPOINT" \
        --s3-access-key "$S3_ACCESS_KEY" \
        --s3-secret-key "$S3_SECRET_KEY" \
        --s3-region "$S3_REGION" \
        --clean >>"$log" 2>&1
}

run_test() {
    local bench=$1 log=$2
    echo "  [test] $bench"
    BENCH_CLIENT_TIMEOUT_SECS=$BENCH_CLIENT_TIMEOUT_SECS \
      "$BENCH_BIN" test "$bench" \
        --scale "$BENCH_SCALE" \
        --protocol flight \
        --host localhost --port 60051 \
        --username "$SQE_USERNAME" --password "$SQE_PASSWORD" 2>&1 | tee -a "$log"
}

# ── Main loop ─────────────────────────────────────────────────
SUMMARIES=()
# Move tpcds ahead of tpcbb if both are requested
if [[ " ${BENCHMARKS[*]} " == *" tpcbb "* && " ${BENCHMARKS[*]} " != *" tpcds "* ]]; then
    echo "NOTE: tpcbb needs tpcds data; loading tpcds transparently."
    BENCHMARKS=("tpcds" "${BENCHMARKS[@]}")
fi

TPCDS_READY=""
for BENCH in "${BENCHMARKS[@]}"; do
    echo ""
    echo "━━ $BENCH (SF$BENCH_SCALE) ━━"
    BENCH_LOG="$LOG_DIR/$BENCH.log"
    STATUS="OK"
    T0=$(date +%s)

    if [ -z "$BENCH_SKIP_GENERATE" ]; then
        if ! run_generate "$BENCH" "$BENCH_LOG"; then
            STATUS="GEN_FAILED"
        fi
    fi

    if [ "$STATUS" = "OK" ] && [ -z "$BENCH_SKIP_LOAD" ]; then
        if ! run_load "$BENCH" "$BENCH_LOG"; then
            STATUS="LOAD_FAILED"
        fi
    fi

    if [ "$STATUS" = "OK" ] && [ -z "$BENCH_SKIP_TEST" ]; then
        if ! run_test "$BENCH" "$BENCH_LOG"; then
            STATUS="TEST_FAILED"
        fi
    fi

    T1=$(date +%s)
    ELAPSED=$((T1 - T0))
    SUMMARY_LINE=$(grep "^BENCH_SUMMARY:" "$BENCH_LOG" 2>/dev/null | tail -1 || true)
    if [ -n "$SUMMARY_LINE" ]; then
        SUMMARIES+=("$SUMMARY_LINE elapsed=${ELAPSED}s status=$STATUS")
    else
        SUMMARIES+=("BENCH_SUMMARY:$BENCH:0:0:0:0:0:0:0 elapsed=${ELAPSED}s status=$STATUS")
    fi

    # Keep tpcds data around if tpcbb is later in the list
    if [ "$BENCH" = "tpcds" ] && [[ " ${BENCHMARKS[*]} " == *" tpcbb "* ]]; then
        TPCDS_READY=1
        echo "  (keeping tpcds data for tpcbb)"
    else
        rm -rf "${BENCH_DATA_DIR:?}/$BENCH"
    fi
done

# ── Final summary ─────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Benchmark Results (SF${BENCH_SCALE})"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "  %-12s %5s %5s %5s %5s %5s %5s %7s %s\n" \
    "Benchmark" "Pass" "Fail" "Diff" "Skip" "Error" "Total" "Time" "Status"
echo "  ────────────────────────────────────────────────────────────"
SUM_PASS=0; SUM_FAIL=0; SUM_DIFF=0; SUM_SKIP=0; SUM_ERR=0; SUM_TOTAL=0; SUM_MS=0
for S in "${SUMMARIES[@]+"${SUMMARIES[@]}"}"; do
    # BENCH_SUMMARY:name:pass:fail:diff:skip:error:total:ms elapsed=Ns status=X
    LINE=${S#BENCH_SUMMARY:}
    META=${LINE#*[[:space:]]}
    CORE=${LINE%%[[:space:]]*}
    IFS=':' read -r NAME PASS FAIL DIFF SKIP ERR TOTAL MS <<< "$CORE"
    ELAPSED=$(echo "$META" | sed -n 's/.*elapsed=\([0-9]*\)s.*/\1/p')
    STATUS=$(echo "$META" | sed -n 's/.*status=\([A-Z_]*\).*/\1/p')
    TIME_S=$(echo "scale=1; ${MS:-0} / 1000" | bc 2>/dev/null || echo "0.0")
    printf "  %-12s %5s %5s %5s %5s %5s %5s %6ss %s\n" \
        "$NAME" "${PASS:-0}" "${FAIL:-0}" "${DIFF:-0}" "${SKIP:-0}" "${ERR:-0}" "${TOTAL:-0}" "$TIME_S" "${STATUS:-?}"
    SUM_PASS=$((SUM_PASS + ${PASS:-0}))
    SUM_FAIL=$((SUM_FAIL + ${FAIL:-0}))
    SUM_DIFF=$((SUM_DIFF + ${DIFF:-0}))
    SUM_SKIP=$((SUM_SKIP + ${SKIP:-0}))
    SUM_ERR=$((SUM_ERR + ${ERR:-0}))
    SUM_TOTAL=$((SUM_TOTAL + ${TOTAL:-0}))
    SUM_MS=$((SUM_MS + ${MS:-0}))
done
echo "  ────────────────────────────────────────────────────────────"
SUM_TIME_S=$(echo "scale=1; $SUM_MS / 1000" | bc 2>/dev/null || echo "0.0")
printf "  %-12s %5s %5s %5s %5s %5s %5s %6ss\n" \
    "TOTAL" "$SUM_PASS" "$SUM_FAIL" "$SUM_DIFF" "$SUM_SKIP" "$SUM_ERR" "$SUM_TOTAL" "$SUM_TIME_S"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Reports: benchmarks/results/"

# Cleanup generated data (tmpfs will free automatically, but be tidy)
rm -rf "$BENCH_DATA_DIR"

if [ $((SUM_FAIL + SUM_ERR)) -gt 0 ]; then
    exit 1
fi
