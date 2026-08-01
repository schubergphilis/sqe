#!/usr/bin/env bash
# Makefile helper: run one benchmark sweep at a fixed scale factor.
#
# Wraps scripts/benchmark-test.sh with the bits every `make benchmark_*`
# target wants:
#   * tee the whole sweep to a log under $BENCH_LOG_DIR
#   * cap Trino's container memory in comparison mode (unbounded means 80%
#     of the HOST's RAM, which starves the SQE coordinator on a shared box)
#   * fail when a Trino crash mid-sweep silently skipped comparisons -- the
#     underlying script prints a warning and still exits 0
#
# Usage:
#   scripts/benchmark-make-run.sh <scale> [--compare-trino] [suite...]
#
# Environment (all optional):
#   PROFILE          release (default) | dev-release | debug
#   BENCH_SUITES     passed through by the Makefile as positional suites
#   BENCH_LOG_DIR    log directory (default /tmp/sqe-bench-logs)
#   TRINO_MEMORY     Trino container memory cap (default 8g in compare mode;
#                    6g is too low for tpcbb at SF1)
# Anything else benchmark-test.sh understands (BENCH_DATA_SOURCE,
# BENCH_WAREHOUSE, BENCH_BLOOM_FILTER, ...) is inherited unchanged.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

if [ $# -lt 1 ]; then
    echo "usage: $0 <scale> [--compare-trino] [suite...]" >&2
    exit 2
fi

SCALE="$1"
shift

COMPARE=""
if [ "${1:-}" = "--compare-trino" ]; then
    COMPARE="--compare-trino"
    shift
fi

LOG_DIR="${BENCH_LOG_DIR:-/tmp/sqe-bench-logs}"
mkdir -p "$LOG_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SUFFIX=""
[ -n "$COMPARE" ] && SUFFIX="-trino"
LOG="$LOG_DIR/bench-sf${SCALE}${SUFFIX}-${STAMP}.log"

LABEL="SF${SCALE}"
[ -n "$COMPARE" ] && LABEL="$LABEL + Trino comparison"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Benchmarks: $LABEL (profile ${PROFILE:-release})"
echo "  Suites: ${*:-all}"
echo "  Log: $LOG"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -n "$COMPARE" ]; then
    export TRINO_MEMORY="${TRINO_MEMORY:-8g}"
fi

# $COMPARE is intentionally unquoted: quoting it would pass an empty argument,
# which benchmark-test.sh would take as a suite name.
# shellcheck disable=SC2086
BENCH_SCALE="$SCALE" ./scripts/benchmark-test.sh $COMPARE "$@" 2>&1 | tee "$LOG"
STATUS=$?

# A Trino crash mid-sweep skips the comparison for every suite that follows
# and still exits 0. Turn that into a real failure so a green run means the
# comparison actually happened.
if [ -n "$COMPARE" ] && grep -q "Trino not reachable" "$LOG"; then
    echo ""
    echo "ERROR: Trino was not reachable during the sweep -- comparisons were"
    echo "       skipped for at least one suite. Raise TRINO_MEMORY (currently"
    echo "       ${TRINO_MEMORY}) and re-run. Log: $LOG"
    exit 1
fi

exit "$STATUS"
