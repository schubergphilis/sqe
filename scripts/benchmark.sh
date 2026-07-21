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
#   BENCH_QUERY_TIMEOUT_SECS  per-query timeout override. Unset -> a
#                   scale-aware default (300s below sf10, 900s at sf10+).
#   BENCH_MEMORY_LIMIT  coordinator memory_limit override (e.g. 24GB). Unset ->
#                   the committed coordinator-attach.toml value (64GB). Lower it
#                   below physical RAM at large scale so the engine SPILLS
#                   before the OS OOM-kills it (the memory-budget OOM trap).
#   BENCH_TRINO_MEMORY  Trino query.max-memory[-per-node] (default: 8GB).
#   BENCH_GOLDEN_TOKEN, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  passed through
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${BENCH_PROFILE:-local}"
SCALE="${BENCH_SCALE:-1}"
SUITES=("$@")
[ ${#SUITES[@]} -gt 0 ] || { echo "usage: benchmark.sh <suite...>" >&2; exit 1; }

# Scale-aware knobs. Heavy queries at sf10+ blow past the 300s per-query
# default; give them more headroom unless the caller pinned a value. awk does
# the float compare because SCALE may be fractional (0.1) or integral (10).
if [ -z "${BENCH_QUERY_TIMEOUT_SECS:-}" ]; then
    if awk "BEGIN{exit !(${SCALE} >= 10)}"; then
        BENCH_QUERY_TIMEOUT_SECS=900
    else
        BENCH_QUERY_TIMEOUT_SECS=300
    fi
    export BENCH_QUERY_TIMEOUT_SECS
fi
# Trino per-query memory (compare mode). SF10 joins/aggs need more than the
# 8GB baseline; override per box with BENCH_TRINO_MEMORY.
TRINO_MEMORY="${BENCH_TRINO_MEMORY:-8GB}"
echo "==> scale sf${SCALE}: per-query timeout ${BENCH_QUERY_TIMEOUT_SECS}s, coordinator memory_limit ${BENCH_MEMORY_LIMIT:-<config default>}, trino memory ${TRINO_MEMORY}"

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
TRINO_CONTAINER=""
COORD_CFG=""
COORD_TEMPLATE="$ROOT_DIR/tests/benchmark-attach/coordinator-attach.toml"
cleanup() {
    [ -n "$COORD_PID" ] && kill "$COORD_PID" 2>/dev/null || true
    [ -n "$TRINO_CONTAINER" ] && docker stop trino-bench >/dev/null 2>&1 || true
    # Drop the templated coordinator config only if we generated one.
    [ -n "$COORD_CFG" ] && [ "$COORD_CFG" != "$COORD_TEMPLATE" ] && rm -f "$COORD_CFG" || true
    # Never `docker compose down` here. Polaris runs POLARIS_PERSISTENCE_TYPE=
    # in-memory, so tearing the stack down wipes the pre-published golden
    # catalog that attach mode exists to reuse. Leave the stack up for
    # subsequent runs (mirrors benchmark-test.sh). Tear down explicitly with
    # `docker compose -f docker-compose.test.yml down` when you mean it.
    :
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

# Stale-coordinator guard. We health-wait on :60051 below; if a coordinator
# from a killed prior run is still bound there, that wait would pass against
# the STALE process and we would silently benchmark the wrong binary/config.
# Fail fast BEFORE spawning ours.
if nc -z localhost 60051 2>/dev/null; then
    echo "ERROR: something is already listening on :60051 (a stale coordinator from a killed run?)." >&2
    echo "       Kill it and retry:  lsof -ti tcp:60051 | xargs kill" >&2
    exit 1
fi

echo "==> Starting coordinator (attach config)"
# The coordinator takes its config as a positional arg (defaults to sqe.toml).
# Template the committed config when BENCH_MEMORY_LIMIT overrides memory_limit
# (the committed default stays authoritative when unset).
COORD_CFG="$COORD_TEMPLATE"
if [ -n "${BENCH_MEMORY_LIMIT:-}" ]; then
    COORD_CFG="/tmp/sqe-bench-coord-cfg-$$.toml"
    sed "s/^memory_limit = .*/memory_limit = \"${BENCH_MEMORY_LIMIT}\"/" \
        "$COORD_TEMPLATE" > "$COORD_CFG"
    echo "    memory_limit overridden -> ${BENCH_MEMORY_LIMIT} ($COORD_CFG)"
fi
COORD_LOG="/tmp/sqe-bench-coord-$$.log"
RUST_LOG="${RUST_LOG:-sqe=info,warn}" \
    target/release/sqe-coordinator \
    "$COORD_CFG" \
    > "$COORD_LOG" 2>&1 &
COORD_PID=$!
echo "    coordinator log: $COORD_LOG"

# Health-wait on the Flight SQL port; fail fast if the coordinator died.
COORD_READY=""
for _ in $(seq 1 60); do
    if ! kill -0 "$COORD_PID" 2>/dev/null; then
        echo "ERROR: coordinator exited during startup. Log tail:" >&2
        tail -20 "$COORD_LOG" >&2
        exit 1
    fi
    if nc -z localhost 60051 2>/dev/null; then COORD_READY=1; break; fi
    sleep 1
done
[ -n "$COORD_READY" ] || { echo "ERROR: coordinator not ready on :60051" >&2; tail -20 "$COORD_LOG" >&2; exit 1; }

# Credentials the `run` verb resolves at runtime (never from the profile TOML).
# Local RustFS quickstart defaults; override by exporting before the run.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-s3admin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-s3admin}"

# Bearer for the golden Polaris (forwarded by the coordinator's
# bearer_passthrough provider; also the ATTACH TOKEN). Fetch a fresh Polaris
# OAuth token unless one is already exported. ~1h TTL covers a full run.
if [ -z "${BENCH_GOLDEN_TOKEN:-}" ]; then
    echo "==> Fetching golden Polaris token"
    BENCH_GOLDEN_TOKEN=$(curl -sf -X POST \
        "http://localhost:18181/api/catalog/v1/oauth/tokens" \
        -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
    export BENCH_GOLDEN_TOKEN
fi

COMPARE_FLAG=()
if [ "${BENCH_COMPARE:-0}" = 1 ]; then
    COMPARE_FLAG=(--compare-trino)
    TRINO_PORT="${TRINO_PORT:-38080}"
    TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:481}"
    echo "==> Starting Trino ${TRINO_IMAGE} on :${TRINO_PORT} (compare)"

    # Trino reads the SAME golden catalog as SQE. Resolve Polaris/RustFS by
    # their compose-network IPs and join Trino to that network: on a
    # user-defined bridge, localhost is not the host, and the catalog URI
    # must be reachable from inside the container (macOS masks this; Linux
    # CI does not). Published -p still exposes localhost:${TRINO_PORT}.
    POLARIS_IP=$(docker inspect "$(docker compose -f "$COMPOSE_FILE" ps -q polaris)" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
    RUSTFS_IP=$(docker inspect "$(docker compose -f "$COMPOSE_FILE" ps -q rustfs)" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
    STACK_NETWORK=$(docker inspect "$(docker compose -f "$COMPOSE_FILE" ps -q polaris)" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')

    # Catalog file named `iceberg` -> Trino catalog `iceberg`; run.rs sets the
    # session default catalog to `iceberg` so bare `<ns>.<table>` resolves.
    mkdir -p /tmp/trino-bench/catalog
    touch /tmp/trino-bench/catalog/iceberg.properties
    chmod 600 /tmp/trino-bench/catalog/iceberg.properties
    cat > /tmp/trino-bench/catalog/iceberg.properties << TRINOEOF
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://${POLARIS_IP}:8181/api/catalog
iceberg.rest-catalog.warehouse=test_warehouse
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=${BENCH_GOLDEN_TOKEN}
fs.native-s3.enabled=true
s3.endpoint=http://${RUSTFS_IP}:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=${AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${AWS_SECRET_ACCESS_KEY}
TRINOEOF
    cat > /tmp/trino-bench/config.properties << TRINOEOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory=${TRINO_MEMORY}
query.max-memory-per-node=${TRINO_MEMORY}
TRINOEOF

    docker stop trino-bench >/dev/null 2>&1 || true
    docker pull "$TRINO_IMAGE" 2>&1 | tail -1
    NETWORK_ARGS=()
    [ -n "$STACK_NETWORK" ] && NETWORK_ARGS=(--network "$STACK_NETWORK")
    TRINO_CONTAINER=$(docker run -d --rm --name trino-bench \
        -p "${TRINO_PORT}:8080" \
        ${NETWORK_ARGS[@]+"${NETWORK_ARGS[@]}"} \
        -v /tmp/trino-bench/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties:ro \
        -v /tmp/trino-bench/config.properties:/etc/trino/config.properties:ro \
        "$TRINO_IMAGE")

    echo -n "==> Waiting for Trino"
    for _ in $(seq 1 180); do
        curl -sf "http://localhost:${TRINO_PORT}/v1/info" >/dev/null 2>&1 && { echo " ready"; break; }
        echo -n "."; sleep 2
    done
    echo -n "==> Waiting for Trino catalog (20s)..."; sleep 20; echo " done"
    # Full URL incl. scheme: the Trino client uses it verbatim as the base of
    # `<url>/v1/statement` (no scheme -> reqwest "builder error for url").
    export BENCH_TRINO_ENDPOINT="http://localhost:${TRINO_PORT}"
fi

QUERY_FLAG=()
[ -n "${BENCH_QUERY:-}" ] && QUERY_FLAG=(--query "$BENCH_QUERY")

echo "==> Running suites: ${SUITES[*]}"
target/release/sqe-bench run "${SUITES[@]}" \
    --profile "$PROFILE" --scale "$SCALE" "${COMPARE_FLAG[@]}" "${QUERY_FLAG[@]}"
