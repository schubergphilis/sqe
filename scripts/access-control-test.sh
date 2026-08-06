#!/usr/bin/env bash
# Run the access-control e2e suite against the Polaris + Ranger + Keycloak
# quickstart stack.
#
#   scripts/access-control-test.sh                  # whole suite
#   scripts/access-control-test.sh tag_column_mask  # one test by substring
#   scripts/access-control-test.sh --down           # tear the stack down
#
# Only the services the suite needs are started. `sqe`, `data-seed`, and `spark`
# are NOT in any of those dependency chains, so no SQE image is built and the
# demo's seeded tables, grants, and hive policies are never created. That is
# what keeps this suite from disturbing quickstart test.sh / parity-test.sh.
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

# Resolve the endpoints from the stack's OWN .env rather than assuming the
# .env.example defaults. Ports are not fixed: when 26080 is already taken by
# another Ranger, a developer's .env carries RANGER_PORT=46080, and a hardcoded
# config then talks to the WRONG Ranger. That failure is deeply confusing
# (observed: "Role name: engineer does not exist in ranger admin", raised by an
# unrelated Ranger instance that happened to own the port).
RANGER_URL="${AC_RANGER_URL:-http://localhost:${RANGER_PORT:-26080}}"
POLARIS_URL="${AC_POLARIS_URL:-http://localhost:${POLARIS_PORT:-28181}}"
KEYCLOAK_URL="${AC_KEYCLOAK_URL:-http://localhost:${KEYCLOAK_PORT:-38080}}"
RUSTFS_URL="http://localhost:${RUSTFS_PORT:-29000}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Access-control stack (Ranger first boot takes 2-4 min)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
# Two-phase bring-up. `--wait` treats an EXITED container as a failure, and
# bucket-init / keycloak-config / ranger-setup / polaris-setup are one-shot jobs
# that exit 0 on success -- so they are started without `--wait` and waited on
# by exit code below, while `--wait` covers only the long-running services.
docker compose up -d keycloak-config bucket-init ranger-setup polaris-setup
docker compose up -d --wait keycloak rustfs ranger-db ranger-admin polaris

# Wait for a one-shot setup container to exit, and fail on a non-zero code.
# Their work (Ranger service-defs and roles, Keycloak realm, S3 bucket, Polaris
# warehouses and principals) is a precondition for every test.
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

# Container health is not application readiness. Poll the three endpoints the
# tests actually call, with a bounded deadline, and report the last status
# instead of hanging.
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
# Polaris's /q/health lives on its management port 8182, which the compose file
# does not publish, so probe the catalog endpoint on the published port instead.
wait_for "ranger-admin" "$RANGER_URL/service/public/v2/api/servicedef"
wait_for "polaris"      "$POLARIS_URL/api/catalog/v1/config"
wait_for "keycloak"     "$KEYCLOAK_URL/realms/iceberg-ranger/.well-known/openid-configuration"

cd "$ROOT_DIR"

# Write a config with the resolved endpoints and point the tests at it. The
# committed tests/sqe-ranger-test.toml keeps the .env.example ports, so it stays
# usable standalone; this copy adapts to whatever the stack actually published.
mkdir -p target
RESOLVED_CONFIG="$ROOT_DIR/target/sqe-ranger-test.resolved.toml"
sed -e "s|localhost:26080|localhost:${RANGER_PORT:-26080}|g" \
    -e "s|localhost:28181|localhost:${POLARIS_PORT:-28181}|g" \
    -e "s|localhost:38080|localhost:${KEYCLOAK_PORT:-38080}|g" \
    -e "s|localhost:29000|localhost:${RUSTFS_PORT:-29000}|g" \
    "$ROOT_DIR/tests/sqe-ranger-test.toml" > "$RESOLVED_CONFIG"
echo "  config: $RESOLVED_CONFIG (ranger=$RANGER_URL polaris=$POLARIS_URL keycloak=$KEYCLOAK_URL storage=$RUSTFS_URL)"

# Scope the filter to this module. A bare substring (e.g. `tag_`) under
# `--ignored` would match ignored tests in OTHER modules of the same `it` binary
# and force-run them against this stack, which is not the one they need.
FILTER="access_control_e2e"
if [ "$#" -gt 0 ]; then
    FILTER="access_control_e2e::$1"
fi

# `spark_access_control_e2e` CONTAINS `access_control_e2e`, so the filter above also
# matches the Spark modules and force-runs them. They need the `spark` service,
# which this script deliberately does NOT start, so on a stack without it every
# Spark case fails here for a reason that has nothing to do with the SQE suite.
# Measured: this ran 38 tests instead of 31 before the skip was added.
SKIP_SPARK="--skip spark_access_control_e2e --skip spark_mask_parity_e2e"

echo ""
echo "Running access-control e2e suite (filter: $FILTER)..."
SQE_AC_E2E=1 \
SQE_AC_CONFIG="$RESOLVED_CONFIG" \
AC_RANGER_URL="$RANGER_URL" \
RUST_LOG="${RUST_LOG:-sqe_coordinator=info,sqe_policy=debug,sqe_catalog=info,sqe_auth=info,warn}" \
RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}" \
    cargo test -p sqe-coordinator --test it -- \
    --ignored --test-threads=1 --nocapture $SKIP_SPARK "$FILTER"
