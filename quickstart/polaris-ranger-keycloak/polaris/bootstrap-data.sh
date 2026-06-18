#!/usr/bin/env sh
# Polaris bootstrap for the Ranger quickstart. Idempotent.
#
# Differs from the shared bootstrap: with the Ranger authorizer enabled, Polaris
# does NOT use its native catalog-role grants for enforcement (Ranger does), so
# this script does NOT build the catalog-role -> privilege chain. It only sets up
# the IDENTITY plumbing Ranger needs:
#   - catalogs + nested namespaces (the resources)
#   - Polaris principal-roles named exactly like the Keycloak realm roles
#   - principals matching the Keycloak users, with their principal-roles assigned
# Polaris sends principal.getName() (the username) and principal.getRoles() (the
# assigned principal-roles) to Ranger. Tables + data are created later by 'carol'
# through SQE (test.sh), which also exercises the write path under Ranger.
set -eu

POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
POLARIS_REALM="${POLARIS_REALM:-iceberg-ranger}"
S3_ENDPOINT="${S3_ENDPOINT:-http://rustfs:9000}"
S3_BUCKET="${S3_BUCKET:-warehouse}"
BOOTSTRAP_CLIENT_ID="${BOOTSTRAP_CLIENT_ID:-root}"
BOOTSTRAP_CLIENT_SECRET="${BOOTSTRAP_CLIENT_SECRET:?BOOTSTRAP_CLIENT_SECRET must be set}"

MGMT="$POLARIS_URL/api/management/v1"
CAT="$POLARIS_URL/api/catalog/v1"
REALM_HDR="Polaris-Realm: $POLARIS_REALM"

log() { echo "[polaris-data] $*"; }

log "waiting for Polaris at $POLARIS_URL ..."
i=0
while [ "$i" -lt 60 ]; do
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$CAT/oauth/tokens" \
    -d "grant_type=client_credentials&client_id=$BOOTSTRAP_CLIENT_ID&client_secret=$BOOTSTRAP_CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" || echo 000)
  [ "$code" = "200" ] && break
  i=$((i + 1)); [ "$i" -eq 60 ] && { log "TIMEOUT waiting for Polaris (last HTTP $code)"; exit 1; }
  sleep 2
done
log "Polaris is up"

TOKEN=$(curl -s -X POST "$CAT/oauth/tokens" \
  -d "grant_type=client_credentials&client_id=$BOOTSTRAP_CLIENT_ID&client_secret=$BOOTSTRAP_CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
[ -n "$TOKEN" ] || { log "ERROR: could not obtain Polaris admin token"; exit 1; }
AUTH="Authorization: Bearer $TOKEN"
CT="Content-Type: application/json"

api() { # method url body
  m=$1; u=$2; b=${3:-}
  code=$(curl -s -o /tmp/resp -w "%{http_code}" -X "$m" "$u" -H "$AUTH" -H "$REALM_HDR" -H "$CT" ${b:+-d "$b"} || echo 000)
  case "$code" in
    2*|409) return 0 ;;
    *) log "WARN $m $u -> HTTP $code: $(cat /tmp/resp 2>/dev/null)"; return 1 ;;
  esac
}

mk_catalog() { # name
  log "creating catalog '$1'"
  api POST "$MGMT/catalogs" "{
    \"catalog\": {
      \"name\": \"$1\",
      \"type\": \"INTERNAL\",
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"allowedLocations\": [\"s3://$S3_BUCKET/$1/\"],
        \"endpoint\": \"$S3_ENDPOINT\",
        \"endpointInternal\": \"$S3_ENDPOINT\",
        \"pathStyleAccess\": true,
        \"stsUnavailable\": true
      },
      \"properties\": {
        \"default-base-location\": \"s3://$S3_BUCKET/$1/\",
        \"polaris.config.drop-with-purge.enabled\": \"true\"
      }
    }
  }" || true
}

# ── Catalogs ──────────────────────────────────────────────────────────────
mk_catalog sales_wh
mk_catalog ops_wh

# ── Nested namespaces ───────────────────────────────────────────────────────
log "creating namespaces"
api POST "$CAT/sales_wh/namespaces" '{"namespace":["sales"]}' || true
api POST "$CAT/sales_wh/namespaces" '{"namespace":["sales","eu"]}' || true
api POST "$CAT/ops_wh/namespaces" '{"namespace":["ops"]}' || true

# ── Principal-roles (named like Keycloak realm roles) ───────────────────────
log "creating principal-roles"
for r in analyst engineer sqe_admin; do
  api POST "$MGMT/principal-roles" "{\"principalRole\":{\"name\":\"$r\"}}" || true
done

# ── Principals (match Keycloak preferred_username) + role assignment ────────
log "creating principals + assigning principal-roles"
mkprincipal() { api POST "$MGMT/principals" "{\"principal\":{\"name\":\"$1\",\"type\":\"USER\"}}" || true; }
assign() { api PUT "$MGMT/principals/$1/principal-roles" "{\"principalRole\":{\"name\":\"$2\"}}" || true; }

for u in alice bob carol dave; do mkprincipal "$u"; done

assign alice analyst
assign bob engineer
assign bob analyst
assign carol sqe_admin
assign carol engineer
assign carol analyst
# dave: no principal-roles (negative-test user)

log "bootstrap complete: catalogs=sales_wh,ops_wh"
