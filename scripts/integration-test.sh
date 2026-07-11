#!/usr/bin/env bash
set -euo pipefail

# Run integration tests against the lightweight test stack (Polaris in-memory + RustFS).
# Usage: ./scripts/integration-test.sh [filter]
# Example: ./scripts/integration-test.sh test_authentication      # single test by name
#          ./scripts/integration-test.sh test_sql_compat          # all SQL compat tests
#          ./scripts/integration-test.sh                          # run all

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Support distributed smoke via DISTRIBUTED=1 (wires Q-05 / audit distributed CI)
DISTRIBUTED="${DISTRIBUTED:-0}"
COMPOSE_ARGS=( -f "$ROOT_DIR/docker-compose.test.yml" )
BOOTSTRAP_SCRIPT="$SCRIPT_DIR/bootstrap-test.sh"
if [ "$DISTRIBUTED" = "1" ]; then
    COMPOSE_ARGS+=( -f "$ROOT_DIR/docker-compose.distributed.yml" )
    BOOTSTRAP_SCRIPT="$SCRIPT_DIR/bootstrap-distributed.sh"
fi

cd "$ROOT_DIR"

# ── Start test stack ──────────────────────────────────────────
echo "Starting test stack (DISTRIBUTED=$DISTRIBUTED)..."
docker compose "${COMPOSE_ARGS[@]}" up -d

# ── Bootstrap (idempotent) ────────────────────────────────────
"$BOOTSTRAP_SCRIPT"

# ── Run integration tests ─────────────────────────────────────
# Clean up any stale log files from aborted previous runs
rm -f /tmp/sqe-test-*.log
SQE_LOG_FILE="$(mktemp /tmp/sqe-test-XXXXXX.log)"

echo ""
echo "Running integration tests..."
# Runs all test binaries in sqe-coordinator (integration_test + sql_compat_test).
# Capture SQE coordinator tracing output (requires RUST_LOG to be set for structured logs).
# RUST_MIN_STACK=8 MiB matches the production coordinator runtime (see
# crates/sqe-coordinator/src/main.rs:96 WORKER_STACK_BYTES). The default
# 2 MiB tokio stack overflows in debug builds on the CTAS + policy-rewriter
# path that integration_test.rs::test_aggregation_basic + similar exercise.
# test_distributed_select intentionally fails when no worker is listening on
# :50052 (issue #122 — local-fallback masking distributed dispatch bugs).
# Default (DISTRIBUTED=0): this script targets docker-compose.test.yml (no worker);
# distributed coverage is exercised via DISTRIBUTED=1 or scripts/test.sh scenario distributed.
# When DISTRIBUTED=1 the distributed compose overlay is used and the skip is omitted.
# Skip the test here unless the caller passes
# their own filter args ($# > 0 implies an explicit test name was selected).
SKIP_ARGS=()
if [ "$#" -eq 0 ] && [ "$DISTRIBUTED" != "1" ]; then
    SKIP_ARGS=(--skip test_distributed_select)
    # The deep-OR stack-overflow guards (in_subquery_or_stack_overflow.rs
    # prod_stack_4k..32k) are release-only via #[cfg_attr(debug_assertions,
    # ignore)], but `--ignored` force-runs ignored tests, and in a debug
    # build the overflow is an OS-level SIGABRT that kills the whole test
    # binary. Skip them here (this script runs the debug profile); they run
    # via `cargo test --release -p sqe-coordinator --test
    # in_subquery_or_stack_overflow`.
    SKIP_ARGS+=(--skip prod_stack_4k --skip prod_stack_8k --skip prod_stack_16k --skip prod_stack_32k)
fi
RUST_LOG="${RUST_LOG:-sqe_coordinator=info,sqe_catalog=info,sqe_auth=info,warn}" \
RUST_MIN_STACK="${RUST_MIN_STACK:-8388608}" \
    cargo test -p sqe-coordinator -- --ignored --test-threads=1 --nocapture "${SKIP_ARGS[@]}" "$@" 2>&1 \
    | tee "$SQE_LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "=== All integration tests passed ==="
else
    echo "=== Integration tests FAILED (exit $EXIT_CODE) ==="
fi

# ── Show service logs before teardown ─────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SQE Engine logs (last 200 lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
#tail -200 "$SQE_LOG_FILE"
#rm -f "$SQE_LOG_FILE"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Polaris logs (last 100 lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
#docker compose -f "$COMPOSE_FILE" logs --no-log-prefix --tail=100 polaris

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  RustFS logs (last 50 lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
#docker compose -f "$COMPOSE_FILE" logs --no-log-prefix --tail=50 rustfs

# ── Tear down test stack ───────────────────────────────────────
echo ""
echo "Tearing down test stack..."
#docker compose -f "$COMPOSE_FILE" down

exit $EXIT_CODE
