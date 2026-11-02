#!/usr/bin/env sh
# Polaris bootstrap for the engine-bench quickstart. Idempotent; runs in the
# curlimages/curl one-shot (POSIX sh + curl + sed).
#
#   1. root token (client_credentials, the principal from POLARIS_BOOTSTRAP_CREDENTIALS)
#   2. catalog `bench` on s3://warehouse/ (RustFS)
#   3. catalog role bench_admin with CATALOG_MANAGE_CONTENT, granted to the
#      service_admin principal role that root already holds
#
# The namespaces (bench_<size>) are created by `bench.py load` through SQE.
set -eu

POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
SECRET="${POLARIS_ROOT_SECRET:?POLARIS_ROOT_SECRET must be set}"
# Polaris hands `endpoint` to clients and uses `endpointInternal` itself:
# SQE runs on the host, Polaris inside the compose network.
S3_HOST_ENDPOINT="${S3_HOST_ENDPOINT:-http://localhost:39000}"
CAT="$POLARIS_URL/api/catalog/v1"
MGMT="$POLARIS_URL/api/management/v1"

TOKEN=$(curl -sf -X POST "$CAT/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=root&client_secret=$SECRET&scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
[ -n "$TOKEN" ] || { echo "[bootstrap] no Polaris token"; exit 1; }

api() { # method url body; 2xx and 409 (exists) are fine
  code=$(curl -s -o /tmp/resp -w "%{http_code}" -X "$1" "$2" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d "$3")
  case "$code" in
    2*|409) ;;
    *) echo "[bootstrap] $1 $2 -> HTTP $code: $(cat /tmp/resp)"; exit 1 ;;
  esac
}

api POST "$MGMT/catalogs" "{\"catalog\":{\"name\":\"bench\",\"type\":\"INTERNAL\",
  \"properties\":{\"default-base-location\":\"s3://warehouse/\",
                  \"polaris.config.drop-with-purge.enabled\":\"true\"},
  \"storageConfigInfo\":{\"storageType\":\"S3\",\"allowedLocations\":[\"s3://warehouse/\"],
    \"endpoint\":\"$S3_HOST_ENDPOINT\",\"endpointInternal\":\"http://rustfs:9000\",
    \"pathStyleAccess\":true,\"stsUnavailable\":true}}}"
api POST "$MGMT/catalogs/bench/catalog-roles" '{"catalogRole":{"name":"bench_admin"}}'
api PUT "$MGMT/catalogs/bench/catalog-roles/bench_admin/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
api PUT "$MGMT/principal-roles/service_admin/catalog-roles/bench" '{"catalogRole":{"name":"bench_admin"}}'
echo "[bootstrap] catalog bench ready"
