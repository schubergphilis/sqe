#!/usr/bin/env sh
# Polaris bootstrap for the spark-parity stack. Idempotent.
#
# Creates the catalog (sales_wh) + namespace (sales) that both SQE and Spark
# will read. Polaris uses native auth (no Ranger authorizer on Polaris itself).
#
# Access model (Polaris 1.5 native):
#   principal-role -> catalog-role -> grants (privileges on catalog/namespace/table)
#
# API paths (Polaris 1.5, hyphenated):
#   POST /api/management/v1/catalogs/{c}/catalog-roles        -- create catalog role
#   PUT  /api/management/v1/catalogs/{c}/catalog-roles/{r}/grants -- add privilege
#   POST /api/management/v1/principal-roles                   -- create principal role
#   PUT  /api/management/v1/principal-roles/{pr}/catalog-roles/{catalog} -- link pr->cr
#   PUT  /api/management/v1/principals/{p}/principal-roles    -- assign pr to principal
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

# ── Catalog ──────────────────────────────────────────────────────────────────
mk_catalog sales_wh

# ── Namespace ────────────────────────────────────────────────────────────────
log "creating namespace sales"
api POST "$CAT/sales_wh/namespaces" '{"namespace":["sales"]}' || true

# ── Principals (match Keycloak preferred_username) ──────────────────────────
log "creating SQE user principals"
mkprincipal() { api POST "$MGMT/principals" "{\"principal\":{\"name\":\"$1\",\"type\":\"USER\"}}" || true; }
for u in alice bob carol dave; do mkprincipal "$u"; done

# ── Catalog roles (Polaris 1.5: use hyphenated path /catalog-roles) ───────────
# Two catalog roles: admin_role (full control) and rw_role (namespace + table r/w).
log "creating catalog roles in sales_wh"
api POST "$MGMT/catalogs/sales_wh/catalog-roles" '{"catalogRole":{"name":"admin_role"}}' || true
api POST "$MGMT/catalogs/sales_wh/catalog-roles" '{"catalogRole":{"name":"rw_role"}}' || true

# ── Grants on catalog roles ───────────────────────────────────────────────────
# admin_role: CATALOG_MANAGE_CONTENT covers CREATE TABLE, INSERT, DROP, etc.
log "granting catalog privileges to admin_role"
api PUT "$MGMT/catalogs/sales_wh/catalog-roles/admin_role/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' || true
api PUT "$MGMT/catalogs/sales_wh/catalog-roles/admin_role/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_ACCESS"}}' || true

# rw_role: namespace + table read/write (table grants require the table to exist;
# namespace grants are sufficient for the bootstrap phase since tables are
# created later by data-seed).
log "granting namespace privileges to rw_role"
api PUT "$MGMT/catalogs/sales_wh/catalog-roles/rw_role/grants" \
  '{"grant":{"type":"namespace","privilege":"NAMESPACE_FULL_METADATA","namespace":["sales"]}}' || true

# ── Principal roles (named to match Keycloak realm_access.roles) ──────────────
# When Polaris receives an OIDC token, it activates principal-roles whose names
# match the token's realm roles (via principal-roles-mapper). So principal-role
# names MUST match the Keycloak realm roles: sqe_admin, engineer, analyst.
# Polaris does NOT use the native principal->principal-role assignment for OIDC
# users; it activates them by name from the token claims.
log "creating principal-roles matching Keycloak realm roles"
for role in sqe_admin engineer analyst; do
  api POST "$MGMT/principal-roles" "{\"principalRole\":{\"name\":\"$role\"}}" || true
done

# ── Link principal-roles to catalog-roles ─────────────────────────────────────
# sqe_admin and engineer get admin_role (full catalog control + CREATE TABLE / INSERT)
# engineer and analyst also get rw_role (namespace metadata read, table r/w)
log "linking principal-roles to catalog-roles"
api PUT "$MGMT/principal-roles/sqe_admin/catalog-roles/sales_wh" \
  '{"catalogRole":{"name":"admin_role"}}' || true
api PUT "$MGMT/principal-roles/engineer/catalog-roles/sales_wh" \
  '{"catalogRole":{"name":"admin_role"}}' || true
api PUT "$MGMT/principal-roles/engineer/catalog-roles/sales_wh" \
  '{"catalogRole":{"name":"rw_role"}}' || true
api PUT "$MGMT/principal-roles/analyst/catalog-roles/sales_wh" \
  '{"catalogRole":{"name":"rw_role"}}' || true

log "bootstrap complete: catalog=sales_wh, namespace=sales"
