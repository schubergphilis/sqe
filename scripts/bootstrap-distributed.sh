#!/usr/bin/env bash
set -euo pipefail

# Bootstrap the distributed test stack.
# Same as bootstrap-test.sh but uses the distributed compose ports.

POLARIS_URL="${POLARIS_URL:-http://localhost:18181}"
S3_URL="${S3_URL:-http://localhost:19000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-s3admin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-s3admin}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
WAREHOUSE="${WAREHOUSE:-test_warehouse}"

echo "=== SQE Distributed Stack Bootstrap ==="
echo "Polaris:      $POLARIS_URL"
echo "S3:           $S3_URL"
echo "Coordinator:  localhost:60051 (Flight SQL) / localhost:28080 (Trino HTTP)"
echo "Workers:      localhost:60061, localhost:60062"
echo ""

# ── Wait for Polaris ───────────────────────────────────────────
echo -n "Waiting for Polaris..."
for i in $(seq 1 60); do
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
        -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" 2>/dev/null || echo "000")
    if [ "$HTTP" = "200" ]; then echo " ready"; break; fi
    if [ "$i" -eq 60 ]; then echo " TIMEOUT"; exit 1; fi
    echo -n "."
    sleep 1
done

# ── Wait for RustFS ────────────────────────────────────────────
echo -n "Waiting for RustFS..."
for i in $(seq 1 30); do
    if curl -so /dev/null "http://localhost:19000/minio/health/live" 2>/dev/null; then
        echo " ready"; break
    fi
    if [ "$i" -eq 30 ]; then echo " TIMEOUT"; exit 1; fi
    echo -n "."
    sleep 1
done

# ── Create S3 bucket ───────────────────────────────────────────
echo -n "Creating S3 bucket 'warehouse'... "
mc alias set sqe "$S3_URL" "$S3_ACCESS_KEY" "$S3_SECRET_KEY" --api S3v4 >/dev/null 2>&1 || true
mc mb sqe/warehouse 2>/dev/null && echo "done" || echo "already exists"

# ── Get Polaris token ──────────────────────────────────────────
echo -n "Getting Polaris token... "
TOKEN=$(curl -s -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
    -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=PRINCIPAL_ROLE:ALL" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo "done"

# ── Create warehouse ──────────────────────────────────────────
echo -n "Creating warehouse '$WAREHOUSE'... "
HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
        \"catalog\": {
            \"name\": \"$WAREHOUSE\",
            \"type\": \"INTERNAL\",
            \"storageConfigInfo\": {
                \"storageType\": \"S3\",
                \"allowedLocations\": [\"s3://warehouse/\"],
                \"endpoint\": \"$S3_URL\",
                \"endpointInternal\": \"http://rustfs:9000\",
                \"pathStyleAccess\": true
            },
            \"properties\": {
                \"default-base-location\": \"s3://warehouse/\",
                \"polaris.config.drop-with-purge.enabled\": \"true\"
            }
        }
    }")
if [ "$HTTP" = "201" ]; then echo "done"; elif [ "$HTTP" = "409" ]; then echo "already exists"; else echo "HTTP $HTTP"; fi

# ── Grant catalog admin ───────────────────────────────────────
echo -n "Granting catalog admin... "
curl -s -o /dev/null -X PUT "$POLARIS_URL/api/management/v1/catalogs/$WAREHOUSE/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}' 2>/dev/null
echo "done"

# ── Create default namespace ──────────────────────────────────
echo -n "Creating namespace 'default'... "
HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POLARIS_URL/api/catalog/v1/$WAREHOUSE/namespaces" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"namespace":["default"]}')
if [ "$HTTP" = "200" ]; then echo "done"; elif [ "$HTTP" = "409" ]; then echo "already exists"; else echo "HTTP $HTTP"; fi

# ── Wait for coordinator ──────────────────────────────────────
echo ""
echo -n "Waiting for SQE coordinator..."
for i in $(seq 1 60); do
    if curl -so /dev/null "http://localhost:28080/v1/info" 2>/dev/null; then
        echo " ready"; break
    fi
    if [ "$i" -eq 60 ]; then echo " TIMEOUT"; exit 1; fi
    echo -n "."
    sleep 1
done

# ── Wait for workers ──────────────────────────────────────────
echo -n "Waiting for workers to register..."
sleep 10  # Give workers time to heartbeat
echo " done"

echo ""
echo "=== Distributed Stack Ready ==="
echo ""
echo "Query via Flight SQL:"
echo "  cargo run --bin sqe-cli -- --host localhost --port 60051 --username root --password ''"
echo ""
echo "Query via Trino HTTP:"
echo "  curl -X POST http://localhost:28080/v1/statement -u root: -d 'SELECT * FROM system.runtime.nodes'"
echo ""
echo "System tables to try:"
echo "  SELECT * FROM system.runtime.queries ORDER BY created DESC;"
echo "  SELECT * FROM system.runtime.nodes;"
echo "  SELECT * FROM system.runtime.tasks;"
echo "  SELECT * FROM system.metadata.catalogs;"
echo "  SELECT * FROM system.metadata.table_properties;"
