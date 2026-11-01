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

# ── Native workers (DISTRIBUTED=1) ────────────────────────────
# Six tests dispatch to workers the TEST process dials directly:
# test_distributed_select, rewrite_data_files_distributed_parity (x3),
# compaction_distributed_benchmark and json/shredded_variant_column_scans_on_a_worker.
# They cannot use the overlay's worker containers: the test coordinator hands
# each worker an S3 connection built from tests/sqe-test.toml
# (s3_endpoint = http://localhost:19000), and `localhost` inside a container
# is the container. So the workers run natively on this host, on :50052 and
# :50053, the URLs those tests dial by default.
#
# Each worker gets its own metrics port (health is metrics+1), spill dir and
# advertise URL. The advertise URL must equal the URL the coordinator dials:
# a scan ticket's audience is checked against it verbatim. No coordinator_url,
# so no heartbeat: the tests build a static worker registry.
#
# Set SQE_IT_WORKER_URLS to run against workers you started yourself; the
# script then starts none.
WORKER_PIDS=()
stop_workers() {
    for pid in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"}; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in ${WORKER_PIDS[@]+"${WORKER_PIDS[@]}"}; do
        wait "$pid" 2>/dev/null || true
    done
}
trap stop_workers EXIT

if [ "$DISTRIBUTED" = "1" ] && [ -z "${SQE_IT_WORKER_URLS:-}" ]; then
    # The `cargo dev-server` build, so it shares the `make dev` cache.
    echo "Building sqe-server for the native workers..."
    cargo build --no-default-features --bin sqe-server
    WORKER_BIN="$ROOT_DIR/target/debug/sqe-server"
    WORKER_URLS=""
    i=0
    for port in 50052 50053; do
        metrics_port=$((19191 + 2 * i))
        log="/tmp/sqe-it-worker-$port.log"
        spill="/tmp/sqe-it-worker-$port-spill"
        rm -rf "$spill"
        echo "Starting native worker on :$port (log: $log)"
        RUST_LOG="${WORKER_RUST_LOG:-sqe=info,warn}" \
        SQE_WORKER__FLIGHT_PORT="$port" \
        SQE_WORKER__ADVERTISE_URL="http://localhost:$port" \
        SQE_WORKER__ALLOW_UNAUTHENTICATED=true \
        SQE_WORKER__SPILL_DIR="$spill" \
        SQE_METRICS__PROMETHEUS_PORT="$metrics_port" \
            "$WORKER_BIN" --mode worker --config "$ROOT_DIR/tests/sqe-test.toml" \
            > "$log" 2>&1 &
        WORKER_PIDS+=("$!")
        WORKER_URLS="${WORKER_URLS:+$WORKER_URLS,}http://localhost:$port"
        i=$((i + 1))
    done
    # Wait until every worker ACCEPTS on its Flight port: a test that dials a
    # worker still booting fails with "worker unreachable". Not /readyz: the
    # worker marks itself ready just before it binds Flight.
    i=0
    for port in 50052 50053; do
        pid="${WORKER_PIDS[$i]}"
        for _ in $(seq 1 60); do
            (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && break
            if ! kill -0 "$pid" 2>/dev/null; then
                echo "ERROR: native worker on :$port exited during startup:" >&2
                tail -30 "/tmp/sqe-it-worker-$port.log" >&2
                exit 1
            fi
            sleep 1
        done
        if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            echo "ERROR: native worker on :$port not accepting after 60s:" >&2
            tail -30 "/tmp/sqe-it-worker-$port.log" >&2
            exit 1
        fi
        i=$((i + 1))
    done
    echo "Native workers up: $WORKER_URLS"
    # compaction_distributed_benchmark defaults to four workers; two is enough
    # for its row-count guardrails.
    export SQE_IT_WORKER_URLS="$WORKER_URLS"
fi

# ── Run integration tests ─────────────────────────────────────
# Clean up any stale log files from aborted previous runs
rm -f /tmp/sqe-test-*.log
SQE_LOG_FILE="$(mktemp /tmp/sqe-test-XXXXXX.log)"

echo ""
echo "Running integration tests..."
# Q-04: Write-path CI validation (Polaris + RustFS).
# The write tests (test_ctas_roundtrip, test_insert_into, and related DELETE/MERGE)
# exercise the full write path against the test stack. They are included via --ignored.
# Use explicit filter e.g. ./scripts/integration-test.sh test_ctas_roundtrip to focus.
# For stronger validation a dedicated job or WRITE=1 mode can be added later.
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
# When DISTRIBUTED=1 the distributed compose overlay is used, the native workers
# above are started and the skip is omitted.
# Skip the test here unless the caller passes
# their own filter args ($# > 0 implies an explicit test name was selected).
SKIP_ARGS=()
if [ "$#" -eq 0 ]; then
    # The deep-OR stack-overflow guards (in_subquery_or_stack_overflow.rs
    # prod_stack_4k..32k) are release-only via #[cfg_attr(debug_assertions,
    # ignore)], but `--ignored` force-runs ignored tests, and in a debug
    # build the overflow is an OS-level SIGABRT that kills the whole test
    # binary. Skip them here (this script runs the debug profile), in the
    # distributed run too: there the abort ended the run before any
    # distributed test. They run via `cargo test --release -p sqe-coordinator
    # --test in_subquery_or_stack_overflow`.
    SKIP_ARGS+=(--skip prod_stack_4k --skip prod_stack_8k --skip prod_stack_16k --skip prod_stack_32k)
fi
if [ "$#" -eq 0 ] && [ "$DISTRIBUTED" != "1" ]; then
    SKIP_ARGS+=(--skip test_distributed_select)
    # Tests that need live sqe-worker processes, not just the test stack. They
    # were added after the test_distributed_select skip above was written, so a
    # default run reported four failures that no healthy machine could avoid.
    # rewrite_data_files_distributed_parity wants TWO workers on :50052 and
    # :50053 (see that file's header for the env overrides);
    # compaction_distributed_benchmark measures a distributed-vs-local ratio.
    SKIP_ARGS+=(--skip rewrite_data_files_distributed_parity)
    SKIP_ARGS+=(--skip compaction_distributed_benchmark)
    # One worker on :50052 (see variant_distributed.rs's header). The other
    # tests in that file run an in-process worker and stay in.
    SKIP_ARGS+=(--skip json_variant_column_scans_on_a_worker)
    SKIP_ARGS+=(--skip shredded_variant_column_scans_on_a_worker)
    # A timing study, not a correctness test: it prints a before/after ratio.
    SKIP_ARGS+=(--skip compaction_sort_benchmark)
fi
# GROUP=<stack> runs only the ignored tests that declare that stack
# (`#[ignore = "needs:<stack>"]`, finding 807), by exact name. The list comes
# from `cargo xtask test-groups --names`, so it cannot drift from the
# attributes. Without GROUP the run is what it always was: every ignored test
# in the crate minus the skips above.
GROUP_ARGS=()
if [ -n "${GROUP:-}" ]; then
    echo "Selecting ignored tests in group '$GROUP' (cargo xtask test-groups --names $GROUP)..."
    # `while read`, not `mapfile`: macOS ships bash 3.2 and `#!/usr/bin/env bash`
    # may resolve to it.
    GROUP_NAMES=()
    while IFS= read -r name; do
        [ -n "$name" ] && GROUP_NAMES+=("$name")
    done < <(cargo xtask test-groups --names "$GROUP")
    if [ "${#GROUP_NAMES[@]}" -eq 0 ]; then
        echo "no ignored tests declare needs:$GROUP" >&2
        exit 2
    fi
    echo "  ${#GROUP_NAMES[@]} tests"
    GROUP_ARGS=(--exact "${GROUP_NAMES[@]}")
fi
RUST_LOG="${RUST_LOG:-sqe_coordinator=info,sqe_catalog=info,sqe_auth=info,warn}" \
RUST_MIN_STACK="${RUST_MIN_STACK:-8388608}" \
    cargo test -p sqe-coordinator -- --ignored --test-threads=1 --nocapture \
        ${SKIP_ARGS[@]+"${SKIP_ARGS[@]}"} ${GROUP_ARGS[@]+"${GROUP_ARGS[@]}"} "$@" 2>&1 \
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
