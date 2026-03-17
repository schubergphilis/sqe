#!/usr/bin/env bash
set -euo pipefail

# Bootstrap lightweight test stack: create S3 bucket, Polaris warehouse, namespace.
# Idempotent — safe to re-run.

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
S3_URL="${S3_URL:-http://localhost:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
WAREHOUSE="${WAREHOUSE:-test_warehouse}"
NAMESPACE="${NAMESPACE:-default}"

echo "=== SQE Test Stack Bootstrap ==="
echo "Polaris:   $POLARIS_URL"
echo "S3:        $S3_URL"
echo "Warehouse: $WAREHOUSE"
echo ""

# ── Wait for services ──────────────────────────────────────────
echo -n "Waiting for Polaris..."
for i in $(seq 1 30); do
    if curl -sf "$POLARIS_URL/q/health" > /dev/null 2>&1; then
        echo " ready"
        break
    fi
    if [ "$i" -eq 30 ]; then echo " TIMEOUT"; exit 1; fi
    echo -n "."
    sleep 1
done

echo -n "Waiting for RustFS..."
for i in $(seq 1 15); do
    if curl -sf "$S3_URL/minio/health/live" > /dev/null 2>&1; then
        echo " ready"
        break
    fi
    if [ "$i" -eq 15 ]; then echo " TIMEOUT"; exit 1; fi
    echo -n "."
    sleep 1
done

# ── 1. Create S3 bucket ───────────────────────────────────────
echo -n "Creating S3 bucket 'warehouse'... "
curl -sf -X PUT "http://${S3_ACCESS_KEY}:${S3_SECRET_KEY}@${S3_URL#http://}/warehouse" \
    > /dev/null 2>&1 || true
echo "done"

# ── 2. Get Polaris OAuth2 token ────────────────────────────────
echo -n "Getting Polaris token... "
TOKEN_RESPONSE=$(curl -sf -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL")
TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null \
    || echo "$TOKEN_RESPONSE" | jq -r '.access_token')
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "FAILED to get token"
    echo "$TOKEN_RESPONSE"
    exit 1
fi
echo "done"

# ── 3. Create warehouse catalog ────────────────────────────────
echo -n "Creating warehouse catalog '$WAREHOUSE'... "
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
                \"allowedLocations\": [\"s3://warehouse/\"],
                \"properties\": {
                    \"s3.endpoint\": \"http://rustfs:9000\",
                    \"s3.path-style-access\": \"true\",
                    \"s3.access-key-id\": \"$S3_ACCESS_KEY\",
                    \"s3.secret-access-key\": \"$S3_SECRET_KEY\",
                    \"region\": \"us-east-1\"
                }
            },
            \"properties\": {
                \"default-base-location\": \"s3://warehouse/\"
            }
        }
    }" 2>/dev/null)

if [ "$HTTP_CODE" = "409" ]; then
    echo "already exists"
elif [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
    echo "done"
else
    echo "FAILED (HTTP $HTTP_CODE)"
    exit 1
fi

# ── 4. Grant catalog access to root principal ──────────────────
echo -n "Granting catalog admin to root... "

# Create catalog admin role if needed
curl -s -o /dev/null -X POST "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "catalog_admin"}}' 2>/dev/null || true

# Grant CATALOG_MANAGE_CONTENT privilege
curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' 2>/dev/null || true

# Assign catalog role to service_admin principal role
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

if [ "$HTTP_CODE" = "409" ]; then
    echo "already exists"
elif [ "$HTTP_CODE" = "200" ]; then
    echo "done"
else
    echo "FAILED (HTTP $HTTP_CODE)"
    exit 1
fi

echo ""
echo "=== Bootstrap complete ==="
echo "SQE can connect with:"
echo "  token_endpoint = \"$POLARIS_URL/api/catalog/v1/oauth/tokens\""
echo "  polaris_url    = \"$POLARIS_URL/api/catalog\""
echo "  warehouse      = \"$WAREHOUSE\""
