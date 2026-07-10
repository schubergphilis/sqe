#!/usr/bin/env sh
# Polaris bootstrap for the SQE quickstarts.
#
# Idempotent. Safe to re-run. Runs inside a `curlimages/curl` container
# (POSIX sh + curl + sed only; no jq/python/aws-cli), so it works the same
# from a one-shot compose service or from a host shell.
#
# What it does, in order:
#   1. Mint a Polaris *internal* admin token (client_credentials, the root
#      principal created from POLARIS_BOOTSTRAP_CREDENTIALS). This is Polaris's
#      own OAuth, separate from Keycloak -- it is the only credential that
#      exists before any OIDC principal does.
#   2. Create the S3 bucket in the object store (RustFS) via a plain PUT.
#   3. Create the catalog (warehouse) pointing at that bucket.
#   4. Build the Polaris RBAC chain so Keycloak users get real access:
#        principal  ->  principal-role  ->  catalog-role  ->  privilege
#      Keycloak realm roles map 1:1 to Polaris principal-roles by name.
#   5. Create principals matching the realm users (preferred_username) and
#      assign their principal-roles. Polaris OIDC maps the token's
#      preferred_username to one of these principals; the principal's roles
#      (set here, in Polaris's own store) decide what it can do.
#   6. Create the demo namespace.
#
# Every value is overridable by env; defaults assume the in-network compose
# (polaris:8181, rustfs:9000).
set -eu

POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
POLARIS_REALM="${POLARIS_REALM:-iceberg}"
S3_ENDPOINT="${S3_ENDPOINT:-http://rustfs:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3adminpw}"
S3_BUCKET="${S3_BUCKET:-warehouse}"
BOOTSTRAP_CLIENT_ID="${BOOTSTRAP_CLIENT_ID:-root}"
BOOTSTRAP_CLIENT_SECRET="${BOOTSTRAP_CLIENT_SECRET:?BOOTSTRAP_CLIENT_SECRET (Polaris root secret) must be set}"
WAREHOUSE="${WAREHOUSE:-quickstart}"
NAMESPACE="${NAMESPACE:-demo}"

MGMT="$POLARIS_URL/api/management/v1"
CAT="$POLARIS_URL/api/catalog/v1"
REALM_HDR="Polaris-Realm: $POLARIS_REALM"

log() { echo "[bootstrap] $*"; }

# ── 1. Wait for Polaris, then mint the internal admin token ───────────────
log "waiting for Polaris at $POLARIS_URL ..."
i=0
while [ "$i" -lt 60 ]; do
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$CAT/oauth/tokens" \
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

# Helper: POST/PUT and treat 2xx and 409 (already exists) as success.
api() { # method url body
  m=$1; u=$2; b=${3:-}
  code=$(curl -s -o /tmp/resp -w "%{http_code}" -X "$m" "$u" -H "$AUTH" -H "$REALM_HDR" -H "$CT" ${b:+-d "$b"} || echo 000)
  case "$code" in
    2*|409) return 0 ;;
    *) log "WARN $m $u -> HTTP $code: $(cat /tmp/resp 2>/dev/null)"; return 1 ;;
  esac
}

# ── 2. (bucket creation is handled by the bucket-init service via aws-cli;
#       RustFS needs real S3 SigV4, which a plain curl PUT cannot provide) ──

# ── 3. Create the catalog (warehouse) ─────────────────────────────────────
log "creating catalog '$WAREHOUSE'"
api POST "$MGMT/catalogs" "{
  \"catalog\": {
    \"name\": \"$WAREHOUSE\",
    \"type\": \"INTERNAL\",
    \"storageConfigInfo\": {
      \"storageType\": \"S3\",
      \"allowedLocations\": [\"s3://$S3_BUCKET/\"],
      \"endpoint\": \"$S3_ENDPOINT\",
      \"endpointInternal\": \"$S3_ENDPOINT\",
      \"pathStyleAccess\": true,
      \"stsUnavailable\": true
    },
    \"properties\": {
      \"default-base-location\": \"s3://$S3_BUCKET/\",
      \"polaris.config.drop-with-purge.enabled\": \"true\"
    }
  }
}" || true

# ── 4. RBAC: catalog-roles + privileges ───────────────────────────────────
# sqe_admin: full content management; sqe_reader: read + list only.
log "creating catalog-roles + privileges"
api POST "$MGMT/catalogs/$WAREHOUSE/catalog-roles" '{"catalogRole":{"name":"sqe_admin"}}' || true
api POST "$MGMT/catalogs/$WAREHOUSE/catalog-roles" '{"catalogRole":{"name":"sqe_reader"}}' || true
api PUT "$MGMT/catalogs/$WAREHOUSE/catalog-roles/sqe_admin/grants" \
  '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' || true
for p in TABLE_READ_DATA TABLE_LIST NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_READ_PROPERTIES; do
  api PUT "$MGMT/catalogs/$WAREHOUSE/catalog-roles/sqe_reader/grants" \
    "{\"grant\":{\"type\":\"catalog\",\"privilege\":\"$p\"}}" || true
done

# principal-roles mirror the Keycloak realm roles, by name.
for r in service_admin catalog_admin data_writer table_reader; do
  api POST "$MGMT/principal-roles" "{\"principalRole\":{\"name\":\"$r\"}}" || true
done

# Bind principal-roles to catalog-roles (the catalog-level access grant).
for r in service_admin catalog_admin data_writer; do
  api PUT "$MGMT/principal-roles/$r/catalog-roles/$WAREHOUSE" '{"catalogRole":{"name":"sqe_admin"}}' || true
done
api PUT "$MGMT/principal-roles/table_reader/catalog-roles/$WAREHOUSE" '{"catalogRole":{"name":"sqe_reader"}}' || true

# ── 5. Principals matching the Keycloak users ─────────────────────────────
# Polaris OIDC resolves preferred_username -> a principal of the same name.
log "creating OIDC principals + assigning principal-roles"
mkprincipal() { api POST "$MGMT/principals" "{\"principal\":{\"name\":\"$1\",\"type\":\"USER\"}}" || true; }
assign() { api PUT "$MGMT/principals/$1/principal-roles" "{\"principalRole\":{\"name\":\"$2\"}}" || true; }

mkprincipal root
mkprincipal adminuser
mkprincipal testuser

for r in service_admin catalog_admin data_writer table_reader; do assign root "$r"; done
for r in catalog_admin data_writer table_reader; do assign adminuser "$r"; done
assign testuser table_reader

# ── 6. Demo namespace ─────────────────────────────────────────────────────
log "creating namespace '$NAMESPACE'"
api POST "$CAT/$WAREHOUSE/namespaces" "{\"namespace\":[\"$NAMESPACE\"]}" || true

log "bootstrap complete: warehouse=$WAREHOUSE namespace=$NAMESPACE"
