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
for u in root alice bob carol dave erin frank; do
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
# Two single-member roles used by scripts/access-control-parity-demo.sh. They
# exercise policy SHAPES the analyst/engineer pair cannot: a masked role with no
# row filter (fraud desk: every jurisdiction, no identity) and an unmasked role
# whose only restriction is a retention window on one of two tables (audit).
mkrole fraud_analyst '[{"name":"erin","isAdmin":false}]'
mkrole auditor       '[{"name":"frank","isAdmin":false}]'

# ── Seeds ───────────────────────────────────────────────────────────────────
# All policies carry root="*" (Polaris always sends root).
# Seed one policy through the AUTHENTICATED policy API.
#
# NOT `/service/plugins/services/grant/${SERVICE_NAME}`. Ranger declares that
# endpoint `security="none"`, so it accepts callers with no credentials at all,
# and Ranger 2.9.0 refuses it outright with HTTP 400 "Unauthenticated access not
# allowed" unless `ranger.admin.allow.unauthenticated.access` is enabled. On
# 2.9.0 the old form failed for all 23 seeds while curl's failure was swallowed
# and the script still exited 0, so the stack came up healthy with no
# authorization policies whatsoever.
#
# The plugin endpoint MERGED into whatever policy matched the resource. No merge
# is needed here: these seeds are a fixed set at distinct coordinates, so each is
# one whole policy. Idempotency is delete-then-create rather than reading an id
# back, because this image has no python3 or jq to parse JSON with.
#
# Pure sed on purpose. `curlimages/curl` is busybox; python3 is absent.
acc_json() { # bare access-type list -> [{type,isAllowed}] entries
  printf '%s' "$1" | sed 's/"\([^"]*\)"/{"type":"\1","isAllowed":true}/g'
}

seed_policy() { # name_suffix  resource_json  policy_items_json
  pname="sqe-seed-$1"
  # {"root":"*","catalog":"*"} -> {"root":{"values":["*"]},"catalog":{"values":["*"]}}
  #
  # NO `isRecursive`. The plugin endpoint took it once per REQUEST and resolved it
  # server-side; the policy API validates it per resource against the servicedef,
  # which declares no recursive resource, and answers "Recursive option not
  # supported: resource-name=[root]".
  res_json=$(printf '%s' "$2" \
    | sed 's/"\([a-zA-Z_]*\)":"\([^"]*\)"/"\1":{"values":["\2"]}/g')

  curl -fsS $AUTH $CSRF -X DELETE \
    "${RANGER_URL}/service/public/v2/api/policy?servicename=${SERVICE_NAME}&policyname=${pname}" \
    >/dev/null 2>&1 || true
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/policy" \
    -d "{\"service\":\"${SERVICE_NAME}\",\"name\":\"${pname}\",\"policyType\":0,\"isEnabled\":true,\"resources\":${res_json},\"policyItems\":[$3],\"denyPolicyItems\":[]}" \
    >/dev/null && echo "  ok" || echo "  FAILED"
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

# ONE POLICY PER RESOURCE COORDINATE, carrying every grantee that coordinate
# serves. Ranger allows a single policy per resource signature and answers
# "Another policy already exists for matching resource" otherwise. The plugin
# endpoint hid that by merging a grantee into the existing policy server-side;
# the policy API does not, so the merge is expressed here instead. Getting this
# wrong is not subtle: 18 of 23 seeds failed on the first attempt at one policy
# per grantee.
echo "Seeding grants (one policy per resource coordinate) ..."

# Clear every policy this script has ever seeded, whatever it named them, BEFORE
# creating the current set. Ranger allows one policy per resource signature, so a
# leftover seed occupying a coordinate makes the new POST fail with "Another
# policy already exists for matching resource" -- which is precisely what happened
# when the naming scheme changed under a stack that had already been seeded.
# Deleting by name from the listing keeps this idempotent across renames without
# needing a JSON parser, which this busybox image does not have.
for stale in $(curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/policy?serviceName=${SERVICE_NAME}" 2>/dev/null \
  | tr ',' '\n' | grep '"name":"sqe-seed-' | sed 's/.*"name":"\([^"]*\)".*/\1/'); do
  curl -fsS $AUTH $CSRF -X DELETE \
    "${RANGER_URL}/service/public/v2/api/policy?servicename=${SERVICE_NAME}&policyname=${stale}" \
    >/dev/null 2>&1 && echo "  cleared stale seed ${stale}" || true
done

ADMIN_ACC=$(acc_json "$ADMIN_ACCESS")
BASE_ACC=$(acc_json "$BASELINE")

# root (the bootstrap identity) and sqe_admin (carol) both get the full set.
ADMIN_ITEMS="{\"users\":[\"root\"],\"accesses\":[${ADMIN_ACC}],\"delegateAdmin\":true},{\"roles\":[\"sqe_admin\"],\"accesses\":[${ADMIN_ACC}],\"delegateAdmin\":true}"

# EVERY demo role needs the baseline, including the two parity-demo roles. A role
# without it cannot even list the namespace, and that failure is reported as a
# Polaris authorization error indistinguishable from the policy denials the demo
# asserts deliberately. Add new roles here at the same time as `mkrole` above.
# One item with all four: they share an identical access list, so separate items
# would only be noise.
ROLE_ITEM="{\"roles\":[\"analyst\",\"engineer\",\"fraud_analyst\",\"auditor\"],\"accesses\":[${BASE_ACC}],\"delegateAdmin\":true}"

printf "  root-level:      "; seed_policy root      '{"root":"*"}'                                           "$ADMIN_ITEMS"
printf "  catalog-level:   "; seed_policy catalog   '{"root":"*","catalog":"*"}'                              "${ADMIN_ITEMS},${ROLE_ITEM}"
printf "  namespace-level: "; seed_policy namespace '{"root":"*","catalog":"*","namespace":"*"}'              "${ADMIN_ITEMS},${ROLE_ITEM}"
printf "  table-level:     "; seed_policy table     '{"root":"*","catalog":"*","namespace":"*","table":"*"}'  "${ADMIN_ITEMS},${ROLE_ITEM}"
printf "  principal-level: "; seed_policy principal '{"root":"*","principal":"*"}'                            "$ADMIN_ITEMS"

# ── `query` service instance: the shared frontend-query plane ────────────────
# Read by BOTH engines: SQE's PlanRewriter and Spark's Kyuubi
# RangerSparkExtension. Row filters and column masks live here; object-level
# authorization lives in the `polaris` service and is enforced by Polaris.
#
# The servicedef TYPE stays `hive` (Ranger 2.8 ships it built in, no servicedef
# POST needed) because Kyuubi is hardwired to the hive resource shape:
# database / table / column. Only the INSTANCE is named `query`, because nothing
# here is a Hive metastore and the old name `hive` sent every reader looking for
# one. Both engines resolve policies by instance name, so the rename is free.
#
# The `database` resource value SQE sends is the whole dotted namespace, with a
# last-component fallback that still matches older policies (see
# `resolve_policy_key`). For the single-level namespaces here both forms are
# `sales`, which is exactly what Kyuubi sends for `sales_wh.sales.orders`
# (Kyuubi has no catalog level). Agreeing on that is what makes one shared
# service work for both engines.
echo "Creating 'query' service instance for fine-grained policies (idempotent) ..."
if curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/name/query" >/dev/null 2>&1; then
  echo "  service 'query' already present, skipping."
else
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d '{"name":"query","type":"hive","configs":{"username":"admin","password":"none","jdbc.driverClassName":"org.apache.hive.jdbc.HiveDriver","jdbc.url":"none"},"tagService":"tag","isEnabled":true}' >/dev/null \
    && echo "  service 'query' created." || echo "  service 'query' creation FAILED (check ranger logs)."
fi

# ── Fine-grained policies on the `query` service ─────────────────────────────
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
post_policy() { # json_file
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/policy" \
    -d @"$1" >/dev/null 2>&1 \
    && echo "  policy created" || echo "  policy exists or failed (idempotent)"
}

echo "Creating the defer policy + mask policies on the 'query' service ..."

# Object-level authorization belongs to POLARIS, not to this service. Polaris
# runs its own Ranger plugin against the `polaris` service and returns 403 at
# LOAD_TABLE for an unauthorized read.
#
# Kyuubi, however, checks its OWN privilege first and short-circuits before
# Polaris is ever consulted. Without a matching policyType-0 item it fails with
#   AccessControlException: Permission denied: user [bob] does not have
#   [select] privilege on [sales/orders/id]
# even when Polaris would have allowed the read. SQE ignores policyType-0
# entirely, so leaving this out makes the two engines disagree on every
# object-level grant, and the Spark failure reads like a Polaris bug.
#
# One blanket allow makes Kyuubi defer. Masks and row filters are UNAFFECTED:
# they are policy types 1 and 2 and are evaluated separately.
#
# DO NOT DELETE to "tighten security". It grants no data access; Polaris still
# decides, and there is a test (object_denial_survives_the_frontend_defer_policy)
# that proves it. Deleting it breaks Spark entirely.
#
# DO NOT copy it into a service whose engine does not ALSO authorize through
# Polaris. There it would be a wide-open hole.
#
# Every access type Kyuubi may check has to be listed: it checks `update` for
# INSERT and `create` for DDL, and a missing one short-circuits as above.
#
# WHY THE GRANT API AND NOT A NAMED POLICY: creating a hive-type service makes
# Ranger auto-generate `all - database, table, column` over exactly
# database=*/table=*/column=*, granted to `admin` and `{OWNER}` only, which does
# nothing for a query user. That policy OWNS the resource signature, so posting a
# self-documenting `defer-object-level-to-polaris` policy is refused:
#   Validation failure: error code[3010], reason[Another policy already exists
#   for matching resource: policy-name=[all - database, table, column]]
# Every other wildcard shape is taken by a sibling auto policy. The grant API
# MERGES a policy item into the existing match instead of colliding, so the defer
# item lands on `all - database, table, column` as a grant to group `public`.
# The intent therefore lives in this comment and in the docs rather than in a
# policy name. Look for the `public` group item on that policy.
echo "Granting the object-level DEFER item to group 'public' on 'query' ..."
# Through the AUTHENTICATED policy API, for the same reason as the seeds above:
# the plugin grant endpoint is `security="none"` and Ranger 2.9.0 refuses it.
#
# The grant API merged this item into the auto-generated
# `all - database, table, column` policy server-side. That merge happens here
# instead: fetch the policy by name, insert the `public` item at the head of
# policyItems, PUT it back. The auto-generated admin/{OWNER} items are preserved
# because the whole policy is written back.
#
# sed rather than a JSON edit because this image is busybox: no python3, no jq.
# `"policyItems":[` is an unambiguous anchor in Ranger's own serialisation.
QUERY_ALL_ENC=$(printf '%s' "all - database, table, column" | sed 's/ /%20/g; s/,/%2C/g')
QUERY_ALL=$(curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/query/policy/${QUERY_ALL_ENC}" 2>/dev/null || true)
DEFER_ITEM='{"groups":["public"],"accesses":[{"type":"select","isAllowed":true},{"type":"update","isAllowed":true},{"type":"create","isAllowed":true},{"type":"drop","isAllowed":true},{"type":"alter","isAllowed":true},{"type":"index","isAllowed":true},{"type":"lock","isAllowed":true},{"type":"read","isAllowed":true},{"type":"write","isAllowed":true}],"delegateAdmin":false}'
if [ -z "$QUERY_ALL" ]; then
  echo "  DEFER GRANT FAILED -- could not read 'all - database, table, column' on 'query'."
elif printf '%s' "$QUERY_ALL" | grep -q '"groups":\["public"\]'; then
  echo "  defer item already present for group 'public'."
else
  QUERY_ALL_ID=$(printf '%s' "$QUERY_ALL" | sed 's/^{"id":\([0-9]*\).*/\1/')
  printf '%s' "$QUERY_ALL" \
    | sed "s/\"policyItems\":\[/\"policyItems\":[${DEFER_ITEM},/" > /tmp/query-all-policy.json
  curl -fsS $AUTH $CSRF $CT -X PUT "${RANGER_URL}/service/public/v2/api/policy/${QUERY_ALL_ID}" \
    -d @/tmp/query-all-policy.json >/dev/null \
    && echo "  defer item granted to group 'public'." \
    || echo "  DEFER GRANT FAILED -- Spark will deny every read (check ranger logs)."
fi

# Column-mask policy (policyType 1): mask orders.amount -> NULL for role engineer.
cat > /tmp/query-mask.json <<'EOF'
{
  "service": "query",
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
post_policy /tmp/query-mask.json

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
cat > /tmp/query-mask-ssn.json <<'EOF'
{
  "service": "query",
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
post_policy /tmp/query-mask-ssn.json

# NOTE: no row-filter policy is seeded here (see the comment block above the
# mask policies). SQE supports Ranger row filters, but the SQE<->Spark mask
# cross-compare requires bob to see both rows of sales.orders, and Kyuubi Spark
# 3.5 cannot evaluate a row filter on an unprojected column (#6889).

echo "Ranger bootstrap complete."
