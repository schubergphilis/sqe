#!/usr/bin/env bash
# Bring up Polaris + RustFS + SQE for the matrix suite and wait until the REST
# catalog and the SQE Trino HTTP endpoint answer from the host.
#
#   SQE_IMAGE=<image> tests/matrix/docker/start-sqe.sh
#
# Overrides: SQE_IMAGE (required), POLARIS_IMAGE, RUSTFS_VERSION, POLARIS_PORT,
# RUSTFS_PORT, SQE_PORT, POLARIS_CATALOG, BUCKET, S3_KEY_ID, S3_SECRET.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
: "${SQE_IMAGE:?set SQE_IMAGE to the SQE image under test}"
export SQE_IMAGE
POLARIS_PORT="${POLARIS_PORT:-48181}"; export POLARIS_PORT
RUSTFS_PORT="${RUSTFS_PORT:-49000}"; export RUSTFS_PORT
SQE_PORT="${SQE_PORT:-48080}"; export SQE_PORT
POLARIS_CATALOG="${POLARIS_CATALOG:-demo}"; export POLARIS_CATALOG
POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-s3cr3t}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
COMPOSE=(docker compose -f docker-compose.sqe.yml)

echo "[sqe-matrix] starting stack (SQE ${SQE_IMAGE}, Polaris ${POLARIS_IMAGE:-apache/polaris:1.7.0})"
"${COMPOSE[@]}" up -d

wait_for() { # description, attempts, command...
  local what="$1" n="$2"; shift 2
  for _ in $(seq 1 "$n"); do
    if "$@" >/dev/null 2>&1; then echo "[sqe-matrix] $what ready"; return 0; fi
    sleep 2
  done
  echo "[sqe-matrix] timed out waiting for $what" >&2
  "${COMPOSE[@]}" logs --tail 60 >&2 || true
  return 1
}

catalog_ready() {
  local token
  token="$(curl -fsS "http://127.0.0.1:${POLARIS_PORT}/api/catalog/v1/oauth/tokens" \
    --user "${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}" -H "Polaris-Realm: ${POLARIS_REALM}" \
    -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL \
    | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')"
  [[ -n "$token" ]] && curl -fsS -H "Authorization: Bearer $token" \
    "http://127.0.0.1:${POLARIS_PORT}/api/catalog/v1/config?warehouse=${POLARIS_CATALOG}"
}

wait_for "Polaris catalog '${POLARIS_CATALOG}'" 90 catalog_ready
wait_for "SQE Trino HTTP" 90 curl -fsS "http://127.0.0.1:${SQE_PORT}/v1/info"
echo "[sqe-matrix] up. source tests/matrix/docker/env.sh, then run tests/matrix/sqe_feature_tests.py"
