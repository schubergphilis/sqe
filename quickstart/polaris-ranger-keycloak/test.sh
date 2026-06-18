#!/usr/bin/env bash
# End-to-end test for the Polaris + Ranger + Keycloak access-control quickstart.
#
# Proves SQE's `ranger` access-control backend: SQE translates GRANT/REVOKE into
# Apache Ranger policies, and Polaris 1.5's embedded Ranger authorizer enforces
# them. Demonstrates a GRANT ENABLING a query, a REVOKE disabling it, a Ranger
# DENY overriding an allow, user vs role grants, negative cases, and SHOW GRANTS.
#
# Identity model (see OVERVIEW.md):
#   - Polaris principals are federated from Keycloak preferred_username
#     (alice/bob/carol/dave), pre-created in Polaris.
#   - Roles are mapped to users in RANGER's role store (membership), since
#     Polaris ignores the token's realm roles. ranger-setup sets:
#       analyst -> alice,bob,carol   engineer -> bob,carol   sqe_admin -> carol
#   - Each role has a baseline traverse grant (list + read metadata). TABLE DATA
#     access (table-data-read/write) is granted below, via SQE GRANT.
set -uo pipefail
cd "$(dirname "$0")"

RANGER_PORT="${RANGER_PORT:-26080}"
RANGER_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
RANGER_HOST="http://localhost:${RANGER_PORT}"

PASS=0; FAIL=0
green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }

# Run SQL as a user via SQE (ROPC). sqe-cli prints "Error: ..." on failure and
# still exits 0, so classify by output text, not exit code.
sqe() { # user sql
  docker compose exec -T -e "SQE_PASSWORD=${1}123" sqe \
    sqe-cli --port 50051 --user "$1" -e "$2" 2>&1
}
is_error()  { echo "$1" | grep -qi 'Error:'; }
# A denied table is HIDDEN as "not found" (Polaris/SQE information-hiding: a user
# who cannot LOAD a table never learns it exists). Every table referenced here
# exists, so "not found" reliably means "access denied", same as an explicit 403.
is_denial() { echo "$1" | grep -qiE 'not authorized|forbidden|unauthorized|403|denied|permission|not found|does not exist'; }

assert_allow() { # desc user sql
  local out
  for _ in 1 2 3 4 5 6; do
    out="$(sqe "$2" "$3")"
    if ! is_error "$out"; then green "PASS  $1"; PASS=$((PASS+1)); return 0; fi
    sleep 5
  done
  red "FAIL  $1"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1))
}

assert_deny() { # desc user sql
  local out
  for _ in 1 2 3 4 5 6; do
    out="$(sqe "$2" "$3")"
    if is_denial "$out"; then green "PASS  $1 (denied)"; PASS=$((PASS+1)); return 0; fi
    [ -n "$(echo "$out" | grep -i 'Error:')" ] || sleep 5
    sleep 3
  done
  red "FAIL  $1 (expected denial)"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1))
}

admin() { # desc sql
  local out; out="$(sqe carol "$1")"
  if is_error "$out"; then red "ADMIN STEP FAILED: $1"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"
  else green "ok    $1"; fi
}

echo "== 1. Data setup (carol = sqe_admin) =="
admin "CREATE TABLE IF NOT EXISTS sales_wh.sales.orders (id BIGINT, region VARCHAR, amount DOUBLE)"
admin "INSERT INTO sales_wh.sales.orders VALUES (1,'eu',10.0),(2,'us',20.0)"
admin "CREATE TABLE IF NOT EXISTS sales_wh.ops.audit (id BIGINT, event VARCHAR)"
admin "INSERT INTO sales_wh.ops.audit VALUES (1,'login'),(2,'logout')"

echo
echo "== 2. Before any SQE grant: analyst has baseline metadata only, no data =="
assert_deny "alice SELECT orders BEFORE grant (no table-data-read yet)" alice "SELECT region FROM sales_wh.sales.orders LIMIT 1"

echo
echo "== 3. SQE grants (carol, one privilege per statement) =="
admin 'GRANT SELECT ON sales_wh.sales.orders TO ROLE "analyst"'
admin 'GRANT SELECT ON sales_wh.sales.orders TO ROLE "engineer"'
admin 'GRANT INSERT ON sales_wh.sales.orders TO ROLE "engineer"'
admin 'GRANT SELECT ON sales_wh.ops.audit TO USER "bob"'
admin 'GRANT SELECT ON sales_wh.ops.audit TO ROLE "analyst"'

echo
echo "== 4. Positive: the SQE grants enabled access =="
assert_allow "alice (analyst) SELECT orders AFTER grant" alice "SELECT region FROM sales_wh.sales.orders LIMIT 1"
assert_allow "bob (engineer) SELECT orders"             bob   "SELECT region FROM sales_wh.sales.orders LIMIT 1"
assert_allow "bob (engineer) INSERT orders"             bob   "INSERT INTO sales_wh.sales.orders VALUES (3,'eu',30.0)"
assert_allow "bob (user grant) SELECT audit"            bob   "SELECT event FROM sales_wh.ops.audit LIMIT 1"

echo
echo "== 5. Ranger DENY on sales_wh.ops.audit for role analyst (deny overrides allow) =="
# Ranger keeps one policy per resource, and SQE's GRANT SELECT already created
# the audit policy (with an allow for analyst). Deny precedence is expressed by
# ADDING a denyPolicyItem to that same policy, so add it via PUT.
if python3 - "$RANGER_HOST" "$RANGER_PASS" <<'PY'
import json,sys,urllib.request,base64
host,pw=sys.argv[1],sys.argv[2]
auth=base64.b64encode(f"admin:{pw}".encode()).decode()
def req(method,path,body=None):
    r=urllib.request.Request(host+path,method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization":"Basic "+auth,"X-XSRF-HEADER":"x","Content-Type":"application/json"})
    return json.load(urllib.request.urlopen(r))
pols=req("GET","/service/public/v2/api/policy?serviceName=polaris")
match=[p for p in pols if p.get("resources",{}).get("table",{}).get("values")==["audit"]
       and p.get("resources",{}).get("namespace",{}).get("values")==["ops"]]
if not match: print("no audit policy found",file=sys.stderr); sys.exit(1)
p=match[0]
p.setdefault("denyPolicyItems",[]).append(
    {"roles":["analyst"],"accesses":[{"type":"table-properties-read","isAllowed":True},
                                      {"type":"table-data-read","isAllowed":True}]})
req("PUT",f"/service/public/v2/api/policy/{p['id']}",p)
print("deny added to policy",p["id"])
PY
then green "ok    deny added to audit policy"; else red "could not add deny policy"; FAIL=$((FAIL+1)); fi
assert_deny "alice SELECT audit (deny overrides analyst allow)" alice "SELECT event FROM sales_wh.ops.audit LIMIT 1"

echo
echo "== 6. Negative tests =="
assert_deny "dave (no role) SELECT orders"        dave  "SELECT region FROM sales_wh.sales.orders LIMIT 1"
assert_deny "alice (read-only) INSERT orders"     alice "INSERT INTO sales_wh.sales.orders VALUES (9,'x',0.0)"
assert_deny "alice (read-only) DROP orders"       alice "DROP TABLE sales_wh.sales.orders"

echo
echo "== 7. SHOW GRANTS round-trip (carol) =="
SG="$(sqe carol 'SHOW GRANTS ON sales_wh.sales.orders')"
if echo "$SG" | grep -qi analyst && echo "$SG" | grep -qi engineer; then
  green "PASS  SHOW GRANTS lists analyst + engineer"; PASS=$((PASS+1))
else
  red "FAIL  SHOW GRANTS missing analyst/engineer"; echo "$SG" | head -8; FAIL=$((FAIL+1))
fi

echo
echo "== 8. Revoke, then re-check =="
admin 'REVOKE SELECT ON sales_wh.sales.orders FROM ROLE "analyst"'
assert_deny "alice SELECT orders AFTER revoke" alice "SELECT region FROM sales_wh.sales.orders LIMIT 1"

echo
echo "================ RESULT: ${PASS} passed, ${FAIL} failed ================"
[ "$FAIL" -eq 0 ] || exit 1
