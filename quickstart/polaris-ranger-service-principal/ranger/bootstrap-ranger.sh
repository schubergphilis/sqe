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

# ── Service principals (the client_credentials-passthrough demo) ─────────────
# Each SP authenticates to SQE with its own client_id/client_secret; SQE runs
# the client_credentials grant and forwards the token. Polaris maps the token's
# preferred_username (sp-admin/sp-reader/sp-denied) to the principal, then sends
# it to Ranger. So these are USER grants keyed on the SP name -- no Keycloak
# realm roles involved. This proves per-connection identity: the SAME query
# returns different results depending on which SP's credentials connected.
echo "Creating Ranger users for service principals ..."
for sp in sp-admin sp-reader sp-denied; do
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/xusers/secure/users" \
    -d "{\"name\":\"$sp\",\"password\":\"${USER_PASSWORD}\",\"firstName\":\"$sp\",\"lastName\":\"sp\",\"emailAddress\":\"\",\"userRoleList\":[\"ROLE_USER\"]}" >/dev/null 2>&1 \
    && echo "  user '$sp' created" || echo "  user '$sp' exists"
done

# Read access set: baseline traverse + LOAD_TABLE (table-properties-read) +
# table-data-read. This is exactly what `GRANT SELECT` would add; we pre-seed it
# so the demo needs no SQE-side GRANT (SQE runs as a single SP and cannot switch
# users to issue grants).
READ_ACCESS='"catalog-list","catalog-properties-read","namespace-list","namespace-properties-read","table-list","table-properties-read","table-data-read"'

echo "Seeding admin grants for service principal 'sp-admin' (creates + seeds data) ..."
for res in '{"root":"*"}' '{"root":"*","catalog":"*"}' '{"root":"*","catalog":"*","namespace":"*"}' '{"root":"*","catalog":"*","namespace":"*","table":"*"}' '{"root":"*","principal":"*"}'; do
  printf "  sp-admin %s:" "$res"; post_grant "$res" '"users":["sp-admin"]' "$ADMIN_ACCESS"
done

echo "Seeding read grants for service principal 'sp-reader' (SELECT sales_wh.sales.orders) ..."
# Traverse the catalog/namespace tree, then read data on orders specifically.
printf "  sp-reader catalog:"; post_grant '{"root":"*","catalog":"*"}' '"users":["sp-reader"]' "$READ_ACCESS"
printf "  sp-reader namespace:"; post_grant '{"root":"*","catalog":"*","namespace":"*"}' '"users":["sp-reader"]' "$READ_ACCESS"
printf "  sp-reader orders:"; post_grant '{"root":"*","catalog":"sales_wh","namespace":"sales","table":"orders"}' '"users":["sp-reader"]' "$READ_ACCESS"

# sp-denied gets NO grant: its SELECT on the same table must be denied. This is
# the negative half of the per-connection-identity proof.
echo "sp-denied: intentionally no grant (negative case)."

# ── Hive service instance for SQE fine-grained policy enforcement ────────────
# Apache Ranger 2.8 ships the `hive` service-def built in; no servicedef POST
# is needed. SQE reads this service via
# GET /service/plugins/policies/download/hive and rewrites the query plan
# (row filters above the scan, column masks). The `database` resource value
# SQE sends is the LAST dotted component of the namespace, so for
# `sales_wh.sales.orders` the resource is database=sales, table=orders.
echo "Creating hive service instance for fine-grained policies (idempotent) ..."
if curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/name/hive" >/dev/null 2>&1; then
  echo "  service 'hive' already present, skipping."
else
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d '{"name":"hive","type":"hive","configs":{"username":"admin","password":"none","jdbc.driverClassName":"org.apache.hive.jdbc.HiveDriver","jdbc.url":"none"},"isEnabled":true}' >/dev/null \
    && echo "  service 'hive' created." || echo "  service 'hive' creation FAILED (check ranger logs)."
fi

# ── Fine-grained policies on the hive service ───────────────────────────────
# All policies target role 'engineer' (bob + carol). Role 'analyst' (alice +
# bob + carol) serves as the unmasked baseline in test.sh: alice (analyst-only,
# not engineer) sees real amounts and raw ssn, while bob (engineer) gets amount
# masked to NULL and ssn masked to show-last-4.
#
# NO row-filter policy is seeded. The SQE<->Spark cross-compare (parity-test.sh)
# runs `SELECT id, ssn` as bob and asserts byte-identical masked ssn output in
# both engines. A row-filter on `region` would break that two ways:
#   1. Kyuubi Spark 3.5 throws MISSING_ATTRIBUTES (#6889) when the filter
#      references a column the query does not project (region).
#   2. The filter (region='EU') would drop the US row from bob's result, so the
#      two engines could not both return ssn for id=1 AND id=2.
# Mask parity is the cross-compare deliverable; row-filter parity is out of
# scope on Spark 3.5 + Kyuubi 1.11 until #6889 is resolved.
post_hive_policy() { # json_file
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/policy" \
    -d @"$1" >/dev/null 2>&1 \
    && echo "  policy created" || echo "  policy exists or failed (idempotent)"
}

echo "Creating fine-grained access + mask policies on hive service ..."

# Access policy (policyType 0): grant role engineer SELECT on sales.orders.*
# This is for the Spark/Kyuubi cross-compare ONLY. Kyuubi Authz gates table
# ACCESS on the hive service itself (it denies the query before applying any
# mask if no access policy grants select). SQE does NOT use hive access
# policies (its coarse gate is the Polaris `polaris` service); SQE reads only
# the mask (type 1) and row-filter (type 2) policies from hive and ignores
# type-0 access policies. So this policy is a no-op for SQE and a requirement
# for Spark -- one role (engineer) drives both Spark access and the ssn mask.
cat > /tmp/hive-access.json <<'EOF'
{
  "service": "hive",
  "name": "access-sales-orders-engineer",
  "policyType": 0,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["sales"]},
    "table":    {"values": ["orders"]},
    "column":   {"values": ["*"]}
  },
  "policyItems": [{
    "roles":   ["engineer"],
    "accesses": [{"type": "select", "isAllowed": true}]
  }]
}
EOF
post_hive_policy /tmp/hive-access.json

# Column-mask policy (policyType 1): mask orders.amount -> NULL for role engineer.
cat > /tmp/hive-mask.json <<'EOF'
{
  "service": "hive",
  "name": "mask-sales-orders-amount",
  "policyType": 1,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["sales"]},
    "table":    {"values": ["orders"]},
    "column":   {"values": ["amount"]}
  },
  "dataMaskPolicyItems": [{
    "roles":   ["engineer"],
    "accesses": [{"type": "select", "isAllowed": true}],
    "dataMaskInfo": {"dataMaskType": "MASK_NULL"}
  }]
}
EOF
post_hive_policy /tmp/hive-mask.json

# Column-mask policy (policyType 1): mask orders.ssn -> show last 4 for role
# engineer. Uses a CUSTOM transformer with a PORTABLE standard-SQL expression
# (concat + substr), NOT the named MASK_SHOW_LAST_4 type and NOT the Hive
# mask_show_last_n UDF. This is the only form that yields byte-exact parity
# across SQE and Spark/Kyuubi. WHY each alternative fails:
#   - Named MASK_SHOW_LAST_4: SQE honors the servicedef transformer and renders
#     xxx-xx-1111, but Kyuubi ignores it and applies its own mask chars
#     (digit->n, separator->U) -> nnnUnnU1111. NOT byte-equal.
#   - CUSTOM mask_show_last_n({col},4,...): Spark renders xxx-xx-1111 (once the
#     Hive UDF is registered), but SQE fails plan rewrite with a type_coercion /
#     "No field named ssn" error on that Hive-specific expression.
#   - CUSTOM concat('xxx-xx-', substr({col},8,4)): concat + substr are built-ins
#     in BOTH DataFusion (SQE) and Spark, so each engine injects the same
#     expression verbatim and both render xxx-xx-1111 / xxx-xx-2222. GREEN.
# 111-11-1111 -> substr(...,8,4)=1111 -> concat -> xxx-xx-1111.
cat > /tmp/hive-mask-ssn.json <<'EOF'
{
  "service": "hive",
  "name": "mask-sales-orders-ssn",
  "policyType": 1,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["sales"]},
    "table":    {"values": ["orders"]},
    "column":   {"values": ["ssn"]}
  },
  "dataMaskPolicyItems": [{
    "roles":   ["engineer"],
    "accesses": [{"type": "select", "isAllowed": true}],
    "dataMaskInfo": {
      "dataMaskType": "CUSTOM",
      "valueExpr": "concat('xxx-xx-', substr({col}, 8, 4))"
    }
  }]
}
EOF
post_hive_policy /tmp/hive-mask-ssn.json

# NOTE: no row-filter policy is seeded here (see the comment block above the
# mask policies). SQE supports Ranger row filters, but the SQE<->Spark mask
# cross-compare requires bob to see both rows of sales.orders, and Kyuubi Spark
# 3.5 cannot evaluate a row filter on an unprojected column (#6889).

echo "Ranger bootstrap complete."
