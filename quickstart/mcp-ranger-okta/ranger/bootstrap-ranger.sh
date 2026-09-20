#!/usr/bin/env sh
# Create the hive-type service `query` SQE reads policies from, the two Okta
# groups as Ranger groups, and the policies:
#   READONLY_GROUP + SENSITIVE_GROUP  -> select on every table
#   SENSITIVE_TABLES (root.db.table,…) -> select for SENSITIVE_GROUP,
#                                         DENY for READONLY_GROUP
# SQE evaluates these itself against the JWT `groups` claim, so Ranger never
# needs to know the users. Idempotent: delete-then-create. busybox sh, no jq.
set -eu
RANGER_URL="${RANGER_URL:-http://ranger-admin:6080}"
AUTH="-u ${RANGER_USER:-admin}:${RANGER_PASS:-rangerR0cks!}"
CSRF="-H X-XSRF-HEADER:x"
CT="-H Content-Type:application/json"
SERVICE=query
RO="${READONLY_GROUP:-datalake_readonly}"
SENS="${SENSITIVE_GROUP:-datalake_sensitive_ro}"

echo "Waiting for Ranger Admin at ${RANGER_URL} ..."
until curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/servicedef/name/hive" >/dev/null 2>&1; do sleep 5; done

if ! curl -fsS $AUTH "${RANGER_URL}/service/public/v2/api/service/name/${SERVICE}" >/dev/null 2>&1; then
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/service" \
    -d "{\"name\":\"${SERVICE}\",\"type\":\"hive\",\"configs\":{\"username\":\"admin\",\"password\":\"none\",\"jdbc.driverClassName\":\"org.apache.hive.jdbc.HiveDriver\",\"jdbc.url\":\"none\"},\"isEnabled\":true}" >/dev/null
  echo "service '${SERVICE}' created"
fi

for g in "$RO" "$SENS"; do
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/xusers/secure/groups" \
    -d "{\"name\":\"$g\",\"description\":\"Okta group\"}" >/dev/null 2>&1 && echo "group '$g' created" || echo "group '$g' exists"
done

# Ranger seeds a new hive service with an "all - database, table, column"
# policy for the service user, and refuses a second policy on the same
# resource. Ours replaces it.
curl -fsS $AUTH $CSRF -X DELETE "${RANGER_URL}/service/public/v2/api/policy?servicename=${SERVICE}&policyname=all%20-%20database,%20table,%20column" >/dev/null 2>&1 || true

# policy NAME DATABASE TABLE ALLOW_GROUPS_JSON [DENY_GROUPS_JSON]
policy() {
  deny=""
  [ -n "${5:-}" ] && deny=",\"denyPolicyItems\":[{\"groups\":$5,\"accesses\":[{\"type\":\"select\",\"isAllowed\":true}],\"delegateAdmin\":false}]"
  curl -fsS $AUTH $CSRF -X DELETE "${RANGER_URL}/service/public/v2/api/policy?servicename=${SERVICE}&policyname=$1" >/dev/null 2>&1 || true
  curl -fsS $AUTH $CSRF $CT -X POST "${RANGER_URL}/service/public/v2/api/policy" -d "{
    \"service\":\"${SERVICE}\",\"name\":\"$1\",\"policyType\":0,\"isEnabled\":true,
    \"resources\":{\"database\":{\"values\":[\"$2\"]},\"table\":{\"values\":[\"$3\"]},\"column\":{\"values\":[\"*\"]}},
    \"policyItems\":[{\"groups\":$4,\"accesses\":[{\"type\":\"select\",\"isAllowed\":true}],\"delegateAdmin\":false}]${deny}
  }" >/dev/null && echo "policy '$1' ok" || echo "policy '$1' FAILED"
}

policy sqe-readonly-all '*' '*' "[\"$RO\",\"$SENS\"]"

# root.db.table -> database "root.db", table "table" (SQE keys Hive tables that way)
for t in $(printf '%s' "${SENSITIVE_TABLES:-}" | tr ',' ' '); do
  tbl="${t##*.}"; db="${t%.*}"
  policy "sqe-sensitive-$(printf '%s' "$t" | tr '.' '-')" "$db" "$tbl" "[\"$SENS\"]" "[\"$RO\"]"
done
echo "Ranger bootstrap complete."
