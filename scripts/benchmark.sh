#!/usr/bin/env bash
# Unified benchmark harness — infra layer. Owns the docker stack + coordinator
# lifecycle; delegates all benchmark logic to `sqe-bench run` (connect-only).
#
# Usage:
#   BENCH_PROFILE=local BENCH_SCALE=1 scripts/benchmark.sh tpch ssb tpcds clickbench
#
# Env:
#   BENCH_PROFILE   profile name/path (default: local)
#   BENCH_SCALE     scale factor (default: 1)
#   BENCH_COMPARE   set to 1 to add --compare-trino
#   BENCH_GOLDEN_TOKEN, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  passed through
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${BENCH_PROFILE:-local}"
SCALE="${BENCH_SCALE:-1}"
SUITES=("$@")
[ ${#SUITES[@]} -gt 0 ] || { echo "usage: benchmark.sh <suite...>" >&2; exit 1; }

PROFILE_FILE="benchmarks/profiles/${PROFILE}.toml"
[ -f "$PROFILE_FILE" ] || { echo "ERROR: no profile $PROFILE_FILE" >&2; exit 1; }
COMPOSE_FILE="$ROOT_DIR/docker-compose.test.yml"

# manage_stack decides whether we own the docker lifecycle.
if grep -E '^manage_stack' "$PROFILE_FILE" | grep -q true; then
    MANAGE_STACK=1
else
    MANAGE_STACK=0
fi

COORD_PID=""
STACK_UP=""
cleanup() {
    [ -n "$COORD_PID" ] && kill "$COORD_PID" 2>/dev/null || true
    if [ "$MANAGE_STACK" = 1 ] && [ -n "$STACK_UP" ]; then
        docker compose -f "$COMPOSE_FILE" down 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "==> Building sqe-bench + sqe-coordinator"
cargo build --release -p sqe-bench -p sqe-coordinator

if [ "$MANAGE_STACK" = 1 ]; then
    echo "==> Bringing up test stack (Polaris + RustFS)"
    docker compose -f "$COMPOSE_FILE" up -d
    scripts/bootstrap-test.sh   # bucket + warehouse + grants (no args)
    STACK_UP=1
fi

echo "==> Starting coordinator (attach config)"
SQE_CONFIG="$ROOT_DIR/tests/benchmark-attach/coordinator-attach.toml" \
    target/release/sqe-coordinator &
COORD_PID=$!

# Health-wait on the Flight SQL port.
for _ in $(seq 1 60); do
    if nc -z localhost 60051 2>/dev/null; then break; fi
    sleep 1
done

COMPARE_FLAG=()
[ "${BENCH_COMPARE:-0}" = 1 ] && COMPARE_FLAG=(--compare-trino)

echo "==> Running suites: ${SUITES[*]}"
target/release/sqe-bench run "${SUITES[@]}" \
    --profile "$PROFILE" --scale "$SCALE" "${COMPARE_FLAG[@]}"
