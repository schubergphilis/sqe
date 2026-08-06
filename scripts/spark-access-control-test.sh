#!/usr/bin/env bash
# Run the SPARK access-control e2e suite against the Polaris + Ranger + Keycloak
# quickstart stack.
#
#   scripts/spark-access-control-test.sh                        # whole suite
#   scripts/spark-access-control-test.sh spark_revoke_disables   # one by substring
#   scripts/spark-access-control-test.sh --down                  # tear down
#
# Same stack as scripts/access-control-test.sh, plus the `spark` service. Kept as
# a separate target because every assertion starts a JVM, so this suite is minutes
# slower and has no business slowing down `make test-access-control`.
#
# `data-seed` is deliberately NOT started. The suite creates its own tables in the
# `ac` namespace through SQE, the same way the SQE suite does, so the demo's
# seeded tables and policies are never touched.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STACK_DIR="$ROOT_DIR/quickstart/polaris-ranger-keycloak"

RANGER_TIMEOUT="${AC_RANGER_TIMEOUT:-300}"

if [ "${1:-}" = "--down" ]; then
    cd "$STACK_DIR" && docker compose down -v
    echo "torn down"
    exit 0
fi

cd "$STACK_DIR"
[ -f .env ] || { echo "creating .env from .env.example"; cp .env.example .env; }
set -a; . ./.env; set +a

# Resolve endpoints from the stack's OWN .env. Ports are not fixed: a developer
# whose 26080 is taken carries RANGER_PORT=46080, and a hardcoded config then
# talks to the WRONG Ranger, which fails in deeply confusing ways.
RANGER_URL="${AC_RANGER_URL:-http://localhost:${RANGER_PORT:-26080}}"
POLARIS_URL="${AC_POLARIS_URL:-http://localhost:${POLARIS_PORT:-28181}}"
KEYCLOAK_URL="${AC_KEYCLOAK_URL:-http://localhost:${KEYCLOAK_PORT:-38080}}"
RUSTFS_URL="http://localhost:${RUSTFS_PORT:-29000}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Spark access-control stack (Ranger first boot takes 2-4 min)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
# Two-phase bring-up. `--wait` treats an EXITED container as a failure, and the
# one-shot setup jobs exit 0 on success, so they are started without `--wait` and
# waited on by exit code below.
docker compose up -d keycloak-config bucket-init ranger-setup polaris-setup
docker compose up -d --wait keycloak rustfs ranger-db ranger-admin polaris sqe spark

wait_oneshot() { # service
    local svc=$1 deadline=$((SECONDS + RANGER_TIMEOUT)) cid state code
    while [ $SECONDS -lt $deadline ]; do
        cid="$(docker compose ps -aq "$svc" 2>/dev/null | head -1)"
        if [ -n "$cid" ]; then
            state="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || true)"
            code="$(docker inspect -f '{{.State.ExitCode}}' "$cid" 2>/dev/null || true)"
            if [ "$state" = "exited" ]; then
                if [ "$code" = "0" ]; then
                    echo "  $svc completed"
                    return 0
                fi
                echo "ERROR: $svc exited $code -- run 'docker compose logs $svc'" >&2
                return 1
            fi
        fi
        sleep 5
    done
    echo "ERROR: $svc did not complete within ${RANGER_TIMEOUT}s" >&2
    return 1
}

for svc in bucket-init keycloak-config ranger-setup polaris-setup; do
    wait_oneshot "$svc"
done

wait_for() { # name url
    local name=$1 url=$2 deadline=$((SECONDS + RANGER_TIMEOUT)) code=000
    while [ $SECONDS -lt $deadline ]; do
        code="$(curl -s -o /dev/null -w '%{http_code}' "$url" || true)"
        case "$code" in 2??|3??|401) echo "  $name ready ($code)"; return 0 ;; esac
        sleep 5
    done
    echo "ERROR: $name not ready after ${RANGER_TIMEOUT}s (last HTTP $code): $url" >&2
    return 1
}

# Ranger and Polaris answer 401 unauthenticated, which is a ready signal.
wait_for "ranger-admin" "$RANGER_URL/service/public/v2/api/servicedef"
wait_for "polaris"      "$POLARIS_URL/api/catalog/v1/config"
wait_for "keycloak"     "$KEYCLOAK_URL/realms/iceberg-ranger/.well-known/openid-configuration"

# Spark readiness is not container health: the image is up long before spark-sql
# can run. One trivial query proves the JVM, the jars, and the Ranger plugin conf
# all load, and fails here with a readable error rather than inside a test.
echo "  probing spark-sql (first JVM start can take ~30s)..."
if ! docker compose exec -T spark /opt/spark/bin/spark-sql -e "SELECT 1" >/tmp/spark-probe.log 2>&1; then
    echo "ERROR: spark-sql is not usable. Last lines:" >&2
    tail -20 /tmp/spark-probe.log >&2
    exit 1
fi
echo "  spark-sql ready"

# The Ranger plugin config is a BIND-MOUNTED FILE. Editing it on the host with
# sed replaces the inode, which breaks the mount and leaves the container with
# `FileNotFoundException: /opt/spark/conf/ranger-spark-security.xml`, after which
# Kyuubi enforces nothing and every denial test fails for the wrong reason.
# Check the file is visible and names the service the tests expect.
if ! docker compose exec -T spark test -f /opt/spark/conf/ranger-spark-security.xml; then
    echo "ERROR: ranger-spark-security.xml is not visible inside the container." >&2
    echo "       The bind mount broke (a host-side sed replaces the inode)." >&2
    echo "       Fix: docker compose up -d --force-recreate spark" >&2
    exit 1
fi

cd "$ROOT_DIR"

mkdir -p target
RESOLVED_CONFIG="$ROOT_DIR/target/sqe-ranger-test.resolved.toml"
sed -e "s|localhost:26080|localhost:${RANGER_PORT:-26080}|g" \
    -e "s|localhost:28181|localhost:${POLARIS_PORT:-28181}|g" \
    -e "s|localhost:38080|localhost:${KEYCLOAK_PORT:-38080}|g" \
    -e "s|localhost:29000|localhost:${RUSTFS_PORT:-29000}|g" \
    "$ROOT_DIR/tests/sqe-ranger-test.toml" > "$RESOLVED_CONFIG"
echo "  config: $RESOLVED_CONFIG"

# Scope the filter to this module. A bare substring under `--ignored` would match
# ignored tests in OTHER modules of the same `it` binary and force-run them
# against a stack they do not expect.
FILTER="spark_access_control_e2e"
if [ "$#" -gt 0 ]; then
    FILTER="spark_access_control_e2e::$1"
fi

echo ""
echo "Running Spark access-control e2e suite (filter: $FILTER)..."
echo "Each assertion starts a JVM; expect minutes, not seconds."
SQE_AC_E2E=1 \
SQE_AC_CONFIG="$RESOLVED_CONFIG" \
AC_RANGER_URL="$RANGER_URL" \
RUST_LOG="${RUST_LOG:-sqe_coordinator=info,sqe_policy=debug,sqe_catalog=info,sqe_auth=info,warn}" \
RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}" \
    cargo test -p sqe-coordinator --test it -- \
    --ignored --test-threads=1 --nocapture "$FILTER"
