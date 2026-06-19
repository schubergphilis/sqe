#!/usr/bin/env sh
# Ranger bootstrap for the spark-parity stack.
#
# Sets up the Ranger `hive` service with ONE policy:
#   MASK_SHOW_LAST_4 on sales.orders.ssn for role `engineer`
#
# Both SQE and Spark/Kyuubi-Authz read this same hive service to apply the
# mask. ONLY this policy is seeded -- no polaris service, no row-filter,
# no MASK_NULL. This keeps the parity surface minimal and avoids the known
# Kyuubi Spark 3.5 row-filter MISSING_ATTRIBUTES bug (#6889).
#
# Identity: engineer role has bob + carol as members (set below).
# SQE sends database=sales, table=orders, column=ssn (last dotted namespace
# component). Kyuubi Authz sends the same resource path.
set -eu

RANGER_URL="${RANGER_URL:-http://ranger-admin:6080}"
RANGER_USER="${RANGER_USER:-admin}"
RANGER_PASS="${RANGER_PASS:-rangerR0cks!}"
USER_PASSWORD="${RANGER_GRANTEE_PASSWORD:-SqeRanger123!}"

AUTH="-u ${RANGER_USER}:${RANGER_PASS}"
CSRF="-H X-XSRF-HEADER:x"
CT="-H Content-Type:application/json"

echo "Waiting for Ranger Admin at ${RANGER_URL} ..."
until curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef" >/dev/null 2>&1; do
  sleep 5
done
echo "Ranger Admin is up."

# ── Grantee users (needed for role membership) ──────────────────────────
echo "Creating Ranger users ..."
for u in bob carol alice; do
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/xusers/secure/users" \
    -d "{\"name\":\"$u\",\"password\":\"${USER_PASSWORD}\",\"firstName\":\"$u\",\"lastName\":\"sqe\",\"emailAddress\":\"\",\"userRoleList\":[\"ROLE_USER\"]}" >/dev/null 2>&1 \
    && echo "  user '$u' created" || echo "  user '$u' exists"
done

# ── Role with membership ─────────────────────────────────────────────────
echo "Creating engineer role (bob + carol) ..."
curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/roles" \
  -d '{"name":"engineer","description":"engineer","users":[{"name":"bob","isAdmin":false},{"name":"carol","isAdmin":false}]}' >/dev/null 2>&1 \
  && echo "  role 'engineer' created" || echo "  role 'engineer' exists"

echo "Creating analyst role (alice + bob + carol) ..."
curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/roles" \
  -d '{"name":"analyst","description":"analyst","users":[{"name":"alice","isAdmin":false},{"name":"bob","isAdmin":false},{"name":"carol","isAdmin":false}]}' >/dev/null 2>&1 \
  && echo "  role 'analyst' created" || echo "  role 'analyst' exists"

# ── Hive service instance ─────────────────────────────────────────────────
# Ranger 2.8 ships the `hive` service-def built in; no servicedef POST needed.
# Both SQE and Kyuubi Authz use service name `hive`.
echo "Creating hive service instance (idempotent) ..."
if curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/name/hive" >/dev/null 2>&1; then
  echo "  service 'hive' already present, skipping."
else
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d '{"name":"hive","type":"hive","configs":{"username":"admin","password":"none","jdbc.driverClassName":"org.apache.hive.jdbc.HiveDriver","jdbc.url":"none"},"isEnabled":true}' >/dev/null \
    && echo "  service 'hive' created." || echo "  service 'hive' FAILED."
fi

# ── SSN column-mask policy ────────────────────────────────────────────────
# policyType 1 = column mask.
# MASK_SHOW_LAST_4: keeps last 4 digits, replaces preceding chars with 'x'.
# Input 111-11-1111 -> xxx-xx-1111  (dashes preserved by the transformer).
# Both SQE (PlanRewriter -> mask UDF) and Kyuubi Authz (RangerSparkExtension
# -> injects mask_show_last_n SQL expr) must produce the same result.
echo "Creating ssn MASK_SHOW_LAST_4 policy on hive service ..."
cat > /tmp/hive-mask-ssn.json <<'EOF'
{
  "service": "hive",
  "name": "parity-mask-sales-orders-ssn",
  "policyType": 1,
  "isEnabled": true,
  "resources": {
    "database": {"values": ["sales"], "isExcludes": false, "isRecursive": false},
    "table":    {"values": ["orders"], "isExcludes": false, "isRecursive": false},
    "column":   {"values": ["ssn"], "isExcludes": false, "isRecursive": false}
  },
  "dataMaskPolicyItems": [{
    "roles":   ["engineer"],
    "accesses": [{"type": "select", "isAllowed": true}],
    "dataMaskInfo": {"dataMaskType": "MASK_SHOW_LAST_4"}
  }]
}
EOF
curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/policy" \
  -d @/tmp/hive-mask-ssn.json >/dev/null 2>&1 \
  && echo "  ssn mask policy created." || echo "  ssn mask policy exists or failed (idempotent)."

echo "Ranger bootstrap complete."
