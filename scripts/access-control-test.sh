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
RANGER_URL="${AC_RANGER_URL:-http://localhost:26080}"
POLARIS_URL="${AC_POLARIS_URL:-http://localhost:28181}"
KEYCLOAK_URL="${AC_KEYCLOAK_URL:-http://localhost:38080}"

if [ "${1:-}" = "--down" ]; then
    cd "$STACK_DIR" && docker compose down -v
    echo "torn down"
    exit 0
fi

cd "$STACK_DIR"
[ -f .env ] || { echo "creating .env from .env.example"; cp .env.example .env; }
set -a; . ./.env; set +a

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

# Scope the filter to this module. A bare substring (e.g. `tag_`) under
# `--ignored` would match ignored tests in OTHER modules of the same `it` binary
# and force-run them against this stack, which is not the one they need.
FILTER="access_control_e2e"
if [ "$#" -gt 0 ]; then
    FILTER="access_control_e2e::$1"
fi

echo ""
echo "Running access-control e2e suite (filter: $FILTER)..."
SQE_AC_E2E=1 \
RUST_LOG="${RUST_LOG:-sqe_coordinator=info,sqe_policy=debug,sqe_catalog=info,sqe_auth=info,warn}" \
RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}" \
    cargo test -p sqe-coordinator --test it -- \
    --ignored --test-threads=1 --nocapture "$FILTER"
