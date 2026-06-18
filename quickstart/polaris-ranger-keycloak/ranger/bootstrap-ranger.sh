#!/usr/bin/env sh
# Register the polaris service-def + service in Ranger Admin, create the grantee
# users/roles (with membership), and seed the policies the stack needs.
# Idempotent. Runs once before Polaris starts.
#
# Hard-won Ranger/Polaris facts encoded here:
#   1. State-changing requests need the `X-XSRF-HEADER` header (CSRF guard).
#   2. The grant API validates that grantee users/roles already EXIST in Ranger
#      (usersync does this in production; we create them explicitly).
#   3. Polaris IGNORES the bearer token's realm roles (they lack the
#      `PRINCIPAL_ROLE:` prefix). Role-based access therefore works through
#      Ranger ROLE MEMBERSHIP (user -> role, set here / by usersync), NOT through
#      Polaris principal-roles (whose management ops are unmapped in the 1.5.0
#      Ranger authorizer and always denied).
#   4. Every policy must include the `root` resource ("*") or Polaris never
#      matches it (Polaris sends root in every authorization request).
#   5. A read through SQE touches several Polaris ops, each needing a specific
#      access type: LIST_NAMESPACES->namespace-list,
#      LOAD_NAMESPACE_METADATA->namespace-properties-read,
#      LOAD_TABLE(+read delegation)->table-properties-read/table-data-read.
#      So roles get a "baseline" traverse grant here; table DATA access
#      (table-data-read / table-data-write) is left to SQE GRANT in test.sh.
set -eu

RANGER_URL="${RANGER_URL:-http://ranger-admin:6080}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASS="${RANGER_PASS:-rangerR0cks!}"
SERVICE_NAME="${SERVICE_NAME:-polaris}"
USER_PASSWORD="${RANGER_GRANTEE_PASSWORD:-SqeRanger123!}"

AUTH="-u ${RANGER_USER}:${RANGER_PASS}"
CSRF="-H X-XSRF-HEADER:x"
CT="-H Content-Type:application/json"

echo "Waiting for Ranger Admin at ${RANGER_URL} ..."
until curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef" >/dev/null 2>&1; do
  sleep 5
done
echo "Ranger Admin is up."

echo "Registering polaris service-def (idempotent) ..."
if curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef/name/polaris" >/dev/null 2>&1; then
  echo "  service-def 'polaris' already present, skipping."
else
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/servicedef" \
    -d @/servicedef-polaris.json >/dev/null
  echo "  service-def 'polaris' created."
fi

echo "Creating service instance '${SERVICE_NAME}' (idempotent) ..."
if curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/name/${SERVICE_NAME}" >/dev/null 2>&1; then
  echo "  service '${SERVICE_NAME}' already present, skipping."
else
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d "{\"name\":\"${SERVICE_NAME}\",\"type\":\"polaris\",\"configs\":{},\"isEnabled\":true}" >/dev/null
  echo "  service '${SERVICE_NAME}' created."
fi

# ── Grantee users (service principal + demo users) ──────────────────────────
# Exist purely as grantee references; login is via Keycloak, not Ranger.
echo "Creating Ranger users ..."
for u in root alice bob carol dave; do
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/xusers/secure/users" \
    -d "{\"name\":\"$u\",\"password\":\"${USER_PASSWORD}\",\"firstName\":\"$u\",\"lastName\":\"sqe\",\"emailAddress\":\"\",\"userRoleList\":[\"ROLE_USER\"]}" >/dev/null 2>&1 \
    && echo "  user '$u' created" || echo "  user '$u' exists"
done

# ── Roles WITH membership (the user -> role mapping Ranger resolves) ─────────
echo "Creating Ranger roles with membership ..."
mkrole() { # name  userlist-json
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/roles" \
    -d "{\"name\":\"$1\",\"description\":\"$1\",\"users\":$2}" >/dev/null 2>&1 \
    && echo "  role '$1' created" || echo "  role '$1' exists"
}
mkrole analyst   '[{"name":"alice","isAdmin":false},{"name":"bob","isAdmin":false},{"name":"carol","isAdmin":false}]'
mkrole engineer  '[{"name":"bob","isAdmin":false},{"name":"carol","isAdmin":false}]'
mkrole sqe_admin '[{"name":"carol","isAdmin":false}]'

# ── Seeds ───────────────────────────────────────────────────────────────────
# All policies carry root="*" (Polaris always sends root).
post_grant() { # resource_json  grantee_field  access_list
  curl -fsS $AUTH $CT -X POST "${RANGER_URL}/service/plugins/services/grant/${SERVICE_NAME}" \
    -d "{\"grantor\":\"admin\",\"resource\":$1,$2,\"accessTypes\":[$3],\"delegateAdmin\":true,\"enableAudit\":true,\"replaceExistingPermissions\":true,\"isRecursive\":true}" >/dev/null \
    && echo "  ok" || echo "  (skipped)"
}

# Full access-type set (all 69) for admin-plane principals (root, sqe_admin
# role). The embedded authorizer does not honor impliedGrants, so the
# fine-grained table-snapshot-*/table-schema-* commit types must be listed
# explicitly or even a full-access principal cannot INSERT.
ADMIN_ACCESS='"service-access-manage","catalog-create","catalog-drop","catalog-list","catalog-content-manage","catalog-metadata-full","catalog-metadata-manage","catalog-policy-attach","catalog-policy-detach","catalog-properties-read","catalog-properties-write","principal-create","principal-drop","principal-list","principal-credentials-reset","principal-credentials-rotate","principal-metadata-full","principal-properties-read","principal-properties-write","namespace-create","namespace-drop","namespace-list","namespace-metadata-full","namespace-policy-attach","namespace-policy-detach","namespace-properties-read","namespace-properties-write","table-create","table-drop","table-list","table-data-read","table-data-write","table-metadata-full","table-policy-attach","table-policy-detach","table-properties-read","table-properties-write","table-properties-set","table-properties-remove","table-uuid-assign","table-format-version-upgrade","table-schema-add","table-schema-set-current","table-partition-spec-add","table-partition-specs-remove","table-sort-order-add","table-sort-order-set-default","table-snapshot-add","table-snapshots-remove","table-snapshot-ref-set","table-snapshot-ref-remove","table-location-set","table-statistics-set","table-statistics-remove","table-structure-manage","view-create","view-drop","view-list","view-metadata-full","view-properties-read","view-properties-write","policy-create","policy-drop","policy-list","policy-read","policy-write","policy-attach","policy-detach","policy-metadata-full"'

# Baseline traverse set: lets a role member connect, list namespaces/tables, and
# read namespace metadata. It deliberately does NOT include table-properties-read
# (LOAD_TABLE): SQE reads parquet with its own configured S3 credentials, so the
# effective read gate is LOAD_TABLE, not credential vending. Leaving
# table-properties-read out of the baseline makes `GRANT SELECT` (which includes
# it) the thing that actually enables a member to read a table.
BASELINE='"catalog-list","catalog-properties-read","namespace-list","namespace-properties-read","table-list"'

echo "Seeding admin grants for user 'root' (bootstrap can manage everything) ..."
for res in '{"root":"*"}' '{"root":"*","catalog":"*"}' '{"root":"*","catalog":"*","namespace":"*"}' '{"root":"*","catalog":"*","namespace":"*","table":"*"}' '{"root":"*","principal":"*"}'; do
  printf "  root %s:" "$res"; post_grant "$res" '"users":["root"]' "$ADMIN_ACCESS"
done

echo "Seeding admin grants for role 'sqe_admin' (carol is a member) ..."
for res in '{"root":"*"}' '{"root":"*","catalog":"*"}' '{"root":"*","catalog":"*","namespace":"*"}' '{"root":"*","catalog":"*","namespace":"*","table":"*"}' '{"root":"*","principal":"*"}'; do
  printf "  sqe_admin %s:" "$res"; post_grant "$res" '"roles":["sqe_admin"]' "$ADMIN_ACCESS"
done

echo "Seeding baseline traverse grants for roles 'analyst' and 'engineer' ..."
for role in analyst engineer; do
  for res in '{"root":"*","catalog":"*"}' '{"root":"*","catalog":"*","namespace":"*"}' '{"root":"*","catalog":"*","namespace":"*","table":"*"}'; do
    printf "  %s %s:" "$role" "$res"; post_grant "$res" "\"roles\":[\"$role\"]" "$BASELINE"
  done
done

echo "Ranger bootstrap complete."
