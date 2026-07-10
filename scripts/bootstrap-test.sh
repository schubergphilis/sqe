#!/usr/bin/env bash
set -euo pipefail

# Bootstrap lightweight test stack: create S3 bucket, Polaris warehouse, namespace.
# Idempotent — safe to re-run.
#
# Credentials come from POLARIS_BOOTSTRAP_CREDENTIALS env var in docker-compose.
# Format: realm,client_id,client_secret → "POLARIS,root,s3cr3t"

POLARIS_URL="${POLARIS_URL:-http://localhost:18181}"
S3_URL="${S3_URL:-http://localhost:19000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
WAREHOUSE="${WAREHOUSE:-test_warehouse}"
NAMESPACE="${NAMESPACE:-default}"

# Warehouse storage mode. `local` (default) targets the RustFS container
# and creates the buckets itself. `external` targets an existing bucket
# on an external S3 endpoint (the endpoint must be reachable from inside
# the Polaris container too — Polaris performs the metadata writes):
#   WAREHOUSE_MODE=external \
#   EXT_S3_ENDPOINT=https://s3.example.com \
#   WAREHOUSE_LOCATION=s3://my-bucket/warehouse ./scripts/bootstrap-test.sh
# Polaris must have been started with matching POLARIS_S3_* env
# (see docker-compose.test.yml).
WAREHOUSE_MODE="${WAREHOUSE_MODE:-local}"
EXT_S3_ENDPOINT="${EXT_S3_ENDPOINT:-}"
WAREHOUSE_LOCATION="${WAREHOUSE_LOCATION:-}"

if [ "$WAREHOUSE_MODE" = "external" ]; then
    if [ -z "$EXT_S3_ENDPOINT" ] || [ -z "$WAREHOUSE_LOCATION" ]; then
        echo "ERROR: WAREHOUSE_MODE=external requires EXT_S3_ENDPOINT and WAREHOUSE_LOCATION" >&2
        exit 1
    fi
    WAREHOUSE_BASE="${WAREHOUSE_LOCATION%/}/"
    STORAGE_ENDPOINT="$EXT_S3_ENDPOINT"
    STORAGE_ENDPOINT_INTERNAL="$EXT_S3_ENDPOINT"
    # With credential subscoping skipped (no STS on the endpoint), Polaris
    # builds its metadata-write S3 client without the storage config's
    # endpoint and falls through to real AWS. The Iceberg FileIO catalog
    # properties pin it to the external endpoint.
    EXTRA_CATALOG_PROPS=",
                \"s3.endpoint\": \"$EXT_S3_ENDPOINT\",
                \"s3.path-style-access\": \"true\""
else
    WAREHOUSE_BASE="s3://warehouse/"
    STORAGE_ENDPOINT="$S3_URL"
    STORAGE_ENDPOINT_INTERNAL="http://rustfs:9000"
    EXTRA_CATALOG_PROPS=""
fi

echo "=== SQE Test Stack Bootstrap ==="
echo "Polaris:   $POLARIS_URL"
echo "Mode:      $WAREHOUSE_MODE"
echo "S3:        $STORAGE_ENDPOINT"
echo "Warehouse: $WAREHOUSE ($WAREHOUSE_BASE)"
echo ""

# ── Wait for Polaris ───────────────────────────────────────────
echo -n "Waiting for Polaris..."
for i in $(seq 1 60); do
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
        -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" 2>/dev/null || echo "000")
    if [ "$HTTP" = "200" ]; then
        echo " ready"
        break
    fi
    if [ "$i" -eq 60 ]; then echo " TIMEOUT (last HTTP=$HTTP)"; exit 1; fi
    echo -n "."
    sleep 1
done

# ── Wait for RustFS (local mode only) ──────────────────────────
if [ "$WAREHOUSE_MODE" = "local" ]; then
echo -n "Waiting for RustFS..."
for i in $(seq 1 15); do
    if curl -so /dev/null "$S3_URL/" 2>/dev/null; then
        echo " ready"
        break
    fi
    if [ "$i" -eq 15 ]; then echo " TIMEOUT"; exit 1; fi
    echo -n "."
    sleep 1
done

# ── 1. Create S3 bucket ───────────────────────────────────────
# Create-or-verify: try head-bucket first, only attempt create if it's
# missing, and fail loud with the real error message if both fail. The
# old implementation masked stderr and always printed "done", which hid
# silent-creation failures until Polaris tried to write to the bucket
# and got back a NoSuchBucket 404 hours later.
# Two buckets: 'warehouse' for test_warehouse and 'warehouse-discovery'
# for discovery_test_wh. Polaris 1.5.0 rejects catalogs whose allowed
# locations overlap an existing catalog, so the discovery warehouse can
# no longer nest under s3://warehouse/.
for BUCKET in warehouse warehouse-discovery; do
echo -n "Creating S3 bucket '$BUCKET'... "
if command -v aws &> /dev/null; then
    if AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" \
       aws --endpoint-url "$S3_URL" --region us-east-1 \
       s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
        echo "already exists"
    else
        MB_OUT=$(AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" \
            aws --endpoint-url "$S3_URL" --region us-east-1 \
            s3 mb "s3://$BUCKET" 2>&1) || {
            echo "FAILED"
            echo "  aws s3 mb stderr: $MB_OUT" >&2
            echo "  Check RustFS is up and credentials match docker-compose.test.yml." >&2
            exit 1
        }
        echo "created"
    fi
else
    # curl fallback: HTTP PUT returns 200 on create, 409 on already-exists
    # (with RustFS), or 403/4xx on auth/config error. Capture status.
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$S3_URL/$BUCKET" \
        -u "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" 2>/dev/null || echo "000")
    case "$CODE" in
        200|204) echo "created" ;;
        409)     echo "already exists" ;;
        *)
            echo "FAILED (HTTP $CODE)"
            echo "  PUT $S3_URL/$BUCKET returned $CODE" >&2
            echo "  Install 'aws' CLI for a clearer error, or check RustFS auth." >&2
            exit 1
            ;;
    esac
fi
done
fi # WAREHOUSE_MODE=local

# ── 2. Get Polaris OAuth2 token ────────────────────────────────
echo -n "Getting Polaris token... "
TOKEN_RESPONSE=$(curl -sf -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL")
TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null \
    || echo "$TOKEN_RESPONSE" | jq -r '.access_token' 2>/dev/null)
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "FAILED"
    echo "Response: $TOKEN_RESPONSE"
    exit 1
fi
echo "done"

# ── 3. Create warehouse catalog ────────────────────────────────
echo -n "Creating warehouse '$WAREHOUSE'... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
        \"catalog\": {
            \"name\": \"$WAREHOUSE\",
            \"type\": \"INTERNAL\",
            \"storageConfigInfo\": {
                \"storageType\": \"S3\",
                \"allowedLocations\": [\"$WAREHOUSE_BASE\"],
                \"endpoint\": \"$STORAGE_ENDPOINT\",
                \"endpointInternal\": \"$STORAGE_ENDPOINT_INTERNAL\",
                \"pathStyleAccess\": true
            },
            \"properties\": {
                \"default-base-location\": \"$WAREHOUSE_BASE\",
                \"polaris.config.drop-with-purge.enabled\": \"true\"$EXTRA_CATALOG_PROPS
            }
        }
    }" 2>/dev/null)

case "$HTTP_CODE" in
    200|201) echo "done" ;;
    409) echo "already exists" ;;
    *) echo "FAILED (HTTP $HTTP_CODE)"; exit 1 ;;
esac

# ── 4. Grant catalog access ───────────────────────────────────
echo -n "Granting catalog admin... "
curl -s -o /dev/null -X POST "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}' 2>/dev/null || true

curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' 2>/dev/null || true

curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/principal-roles/service_admin/catalog-roles/$WAREHOUSE" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}' 2>/dev/null || true
echo "done"

# ── 5. Create default namespace ────────────────────────────────
echo -n "Creating namespace '$NAMESPACE'... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$POLARIS_URL/api/catalog/v1/$WAREHOUSE/namespaces" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\": [\"$NAMESPACE\"]}" 2>/dev/null)

case "$HTTP_CODE" in
    200) echo "done" ;;
    409) echo "already exists" ;;
    *) echo "FAILED (HTTP $HTTP_CODE)"; exit 1 ;;
esac

# ── 6. Create test_ns namespace (used by integration tests) ───
echo -n "Creating namespace 'test_ns'... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$POLARIS_URL/api/catalog/v1/$WAREHOUSE/namespaces" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"namespace": ["test_ns"]}' 2>/dev/null)

case "$HTTP_CODE" in
    200) echo "done" ;;
    409) echo "already exists" ;;
    *) echo "FAILED (HTTP $HTTP_CODE)"; exit 1 ;;
esac

# ── 7. Create discovery_test_wh (catalog_discovery_test.rs) ───
# The polaris-auto catalog-discovery integration tests need a SECOND
# warehouse that is NOT in [catalogs.*] of the test config. Polaris in
# this stack is in-memory, so it must be re-created on every fresh
# stack or those tests fail with "Unable to find warehouse".
# External mode is benchmark-only: the discovery warehouse stays on
# RustFS and is skipped.
if [ "$WAREHOUSE_MODE" = "external" ]; then
    echo ""
    echo "=== Bootstrap complete (external warehouse) ==="
    exit 0
fi
echo -n "Creating warehouse 'discovery_test_wh'... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
        \"catalog\": {
            \"name\": \"discovery_test_wh\",
            \"type\": \"INTERNAL\",
            \"storageConfigInfo\": {
                \"storageType\": \"S3\",
                \"allowedLocations\": [\"s3://warehouse-discovery/\"],
                \"endpoint\": \"$S3_URL\",
                \"endpointInternal\": \"http://rustfs:9000\",
                \"pathStyleAccess\": true
            },
            \"properties\": {
                \"default-base-location\": \"s3://warehouse-discovery/\",
                \"polaris.config.drop-with-purge.enabled\": \"true\"
            }
        }
    }" 2>/dev/null)

case "$HTTP_CODE" in
    200|201) echo "done" ;;
    409) echo "already exists" ;;
    *) echo "FAILED (HTTP $HTTP_CODE)"; exit 1 ;;
esac

curl -s -o /dev/null -X POST "$POLARIS_URL/api/management/v1/catalogs/discovery_test_wh/catalog-roles" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}' 2>/dev/null || true

curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/catalogs/discovery_test_wh/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' 2>/dev/null || true

curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/principal-roles/service_admin/catalog-roles/discovery_test_wh" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}' 2>/dev/null || true

echo -n "Creating namespace 'disc_ns' in discovery_test_wh... "
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$POLARIS_URL/api/catalog/v1/discovery_test_wh/namespaces" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"namespace": ["disc_ns"]}' 2>/dev/null)

case "$HTTP_CODE" in
    200) echo "done" ;;
    409) echo "already exists" ;;
    *) echo "FAILED (HTTP $HTTP_CODE)"; exit 1 ;;
esac

echo ""
echo "=== Bootstrap complete ==="
echo "SQE can connect with:"
echo "  token_endpoint = \"$POLARIS_URL/api/catalog/v1/oauth/tokens\""
echo "  client_id      = \"$CLIENT_ID\""
echo "  polaris_url     = \"$POLARIS_URL/api/catalog\""
echo "  warehouse       = \"$WAREHOUSE\""
