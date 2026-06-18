#!/usr/bin/env sh
# Register the polaris service-def and create the service instance in Ranger
# Admin, idempotently. Runs once before Polaris starts so the embedded Ranger
# authorizer has a service to poll.
set -eu

RANGER_URL="${RANGER_URL:-http://ranger-admin:6080}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASS="${RANGER_PASS:-rangerR0cks!}"
SERVICE_NAME="${SERVICE_NAME:-polaris}"
AUTH="-u ${RANGER_USER}:${RANGER_PASS}"

echo "Waiting for Ranger Admin at ${RANGER_URL} ..."
until curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef/count" >/dev/null 2>&1; do
  sleep 5
done
echo "Ranger Admin is up."

echo "Registering polaris service-def (idempotent) ..."
if curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef/name/polaris" >/dev/null 2>&1; then
  echo "  service-def 'polaris' already present, skipping."
else
  curl -fsS $AUTH -H 'Content-Type: application/json' \
    -X POST "${RANGER_URL}/service/public/v2/api/servicedef" \
    -d @/servicedef-polaris.json >/dev/null
  echo "  service-def 'polaris' created."
fi

echo "Creating service instance '${SERVICE_NAME}' (idempotent) ..."
if curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/name/${SERVICE_NAME}" >/dev/null 2>&1; then
  echo "  service '${SERVICE_NAME}' already present, skipping."
else
  curl -fsS $AUTH -H 'Content-Type: application/json' \
    -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d "{\"name\":\"${SERVICE_NAME}\",\"type\":\"polaris\",\"configs\":{},\"isEnabled\":true}" >/dev/null
  echo "  service '${SERVICE_NAME}' created."
fi

# Seed a broad admin grant so the Polaris bootstrap principal ('root') and the
# 'sqe_admin' role can create catalogs/namespaces/tables. With the Ranger
# authorizer enabled, Polaris delegates EVERY decision to Ranger, including the
# bootstrap's own catalog creation: without this seed, bootstrap is denied.
# Wildcard ('*') resources at each depth + recursive cover all operations. The
# `root` resource is left wildcard so the seed matches regardless of how Polaris
# prefixes the realm (resolved empirically; see OVERVIEW.md).
ADMIN_ACCESS='"catalog-create","catalog-drop","catalog-list","catalog-content-manage","catalog-metadata-full","namespace-create","namespace-drop","namespace-list","namespace-metadata-full","table-create","table-drop","table-list","table-data-read","table-data-write","table-metadata-full","view-create","view-drop","view-list","view-metadata-full","policy-create","policy-drop","policy-list","policy-read","policy-write"'

seed_grant() { # resource_json
  curl -fsS $AUTH -H 'Content-Type: application/json' \
    -X POST "${RANGER_URL}/service/plugins/services/grant/${SERVICE_NAME}" \
    -d "{
      \"grantor\": \"admin\",
      \"resource\": $1,
      \"users\": [\"root\"],
      \"roles\": [\"sqe_admin\"],
      \"accessTypes\": [${ADMIN_ACCESS}],
      \"delegateAdmin\": true,
      \"enableAudit\": true,
      \"replaceExistingPermissions\": false,
      \"isRecursive\": true
    }" >/dev/null && echo "  seeded admin grant: $1"
}

echo "Seeding admin grant for 'root' + role 'sqe_admin' (idempotent) ..."
seed_grant '{"root":"*"}'                              || echo "  (root-level seed skipped)"
seed_grant '{"catalog":"*"}'                           || echo "  (catalog-level seed skipped)"
seed_grant '{"catalog":"*","namespace":"*"}'           || echo "  (namespace-level seed skipped)"
seed_grant '{"catalog":"*","namespace":"*","table":"*"}' || echo "  (table-level seed skipped)"

echo "Ranger bootstrap complete."
