#!/usr/bin/env bash
# End-to-end test for the Polaris + Ranger + Keycloak access-control quickstart.
#
# Proves the `ranger` access-control backend: SQE translates GRANT/REVOKE into
# Apache Ranger policies, and Polaris 1.5's embedded Ranger authorizer enforces
# them. Exercises multi-catalog/namespace resources, role + user grants, a Ranger
# DENY (deny-overrides-allow), negative tests, SHOW GRANTS, and REVOKE.
#
# Users (Keycloak realm iceberg-ranger), password = "<user>123":
#   carol -> sqe_admin,engineer,analyst   (runs GRANT/REVOKE)
#   bob   -> engineer,analyst
#   alice -> analyst
#   dave  -> (no roles)
#
# NOTE on resource shape (top correctness risk): SQE writes Ranger policies with
# the `root` realm value from sqe.toml `[access_control.ranger] realm`. It starts
# empty (root omitted). If a GRANT succeeds but enforcement does not match (an
# allowed user is still denied), the resolved value goes here and in OVERVIEW.md.
# Resolved realm value: "" (root omitted)   <-- update after first run if needed.
set -uo pipefail
cd "$(dirname "$0")"

RANGER_PORT="${RANGER_PORT:-26080}"
RANGER_PASS="${RANGER_ADMIN_PASSWORD:-rangerR0cks!}"
RANGER_HOST="http://localhost:${RANGER_PORT}"

PASS=0
FAIL=0
green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }

# Run a SQL statement as a user via SQE (ROPC). Echoes combined output; returns
# sqe-cli's exit code.
sqe() { # user sql
  local user="$1" sql="$2"
  docker compose exec -T -e "SQE_PASSWORD=${user}123" sqe \
    sqe-cli --port 50051 --user "$user" -e "$sql" 2>&1
}

# An output is a denial if sqe-cli failed AND the text looks like an authz error.
is_denial() { # output
  echo "$1" | grep -qiE 'denied|forbidden|not authorized|unauthorized|403|access|permission'
}

# assert_allow: retry up to ~25s (Polaris polls Ranger every 5s) for success.
assert_allow() { # desc user sql
  local desc="$1" user="$2" sql="$3" out
  for _ in $(seq 1 6); do
    out="$(sqe "$user" "$sql")"
    if [ $? -eq 0 ] && ! is_denial "$out"; then
      green "PASS  $desc"; PASS=$((PASS+1)); return 0
    fi
    sleep 5
  done
  red "FAIL  $desc"; echo "      last output: $(echo "$out" | tr '\n' ' ' | cut -c1-200)"
  FAIL=$((FAIL+1)); return 1
}

# assert_deny: retry up to ~25s for the denial to appear/propagate.
assert_deny() { # desc user sql
  local desc="$1" user="$2" sql="$3" out rc
  for _ in $(seq 1 6); do
    out="$(sqe "$user" "$sql")"; rc=$?
    if [ $rc -ne 0 ] && is_denial "$out"; then
      green "PASS  $desc (denied)"; PASS=$((PASS+1)); return 0
    fi
    sleep 5
  done
  red "FAIL  $desc (expected denial, got rc=$rc)"; echo "      last output: $(echo "$out" | tr '\n' ' ' | cut -c1-200)"
  FAIL=$((FAIL+1)); return 1
}

run_admin() { # desc sql   (carol; failure is fatal to setup)
  local desc="$1" sql="$2" out
  out="$(sqe carol "$sql")"
  if [ $? -eq 0 ]; then green "ok    $desc"; else red "ADMIN STEP FAILED: $desc"; echo "      $out"; fi
}

echo "== 1. Data setup (carol creates tables + rows under Ranger) =="
run_admin "create sales_wh.sales.orders"  "CREATE TABLE sales_wh.sales.orders (id BIGINT, region VARCHAR, amount DOUBLE)"
run_admin "insert orders"                 "INSERT INTO sales_wh.sales.orders VALUES (1,'eu',10.0),(2,'us',20.0)"
run_admin "create ops_wh.ops.audit"       "CREATE TABLE ops_wh.ops.audit (id BIGINT, event VARCHAR)"
run_admin "insert audit"                  "INSERT INTO ops_wh.ops.audit VALUES (1,'login'),(2,'logout')"

echo
echo "== 2. Grants (carol, one privilege per statement) =="
run_admin "grant analyst SELECT orders"   'GRANT SELECT ON sales_wh.sales.orders TO ROLE "analyst"'
run_admin "grant engineer SELECT orders"  'GRANT SELECT ON sales_wh.sales.orders TO ROLE "engineer"'
run_admin "grant engineer INSERT orders"  'GRANT INSERT ON sales_wh.sales.orders TO ROLE "engineer"'
run_admin "grant bob SELECT audit"        'GRANT SELECT ON ops_wh.ops.audit TO USER "bob"'
run_admin "grant analyst SELECT audit"    'GRANT SELECT ON ops_wh.ops.audit TO ROLE "analyst"'

echo
echo "== 3. Ranger DENY on ops_wh.ops.audit for role analyst (deny-overrides-allow) =="
# SQE has no DENY syntax; write the deny policy directly to Ranger Admin.
curl -fsS -u "admin:${RANGER_PASS}" -H 'Content-Type: application/json' \
  -X POST "${RANGER_HOST}/service/public/v2/api/policy" -d '{
    "service": "polaris",
    "name": "deny-analyst-ops-audit",
    "resources": {"catalog":{"values":["ops_wh"]},"namespace":{"values":["ops"]},"table":{"values":["audit"]}},
    "policyItems": [],
    "denyPolicyItems": [{"roles":["analyst"],"accesses":[{"type":"table-data-read","isAllowed":true}]}]
  }' >/dev/null 2>&1 && green "ok    deny policy created" || echo "      (deny policy may already exist)"

echo
echo "== 4. Positive assertions =="
assert_allow "alice (analyst) SELECT orders"        alice 'SELECT count(*) FROM sales_wh.sales.orders'
assert_allow "bob (engineer) SELECT orders"         bob   'SELECT count(*) FROM sales_wh.sales.orders'
assert_allow "bob (engineer) INSERT orders"         bob   "INSERT INTO sales_wh.sales.orders VALUES (3,'eu',30.0)"
assert_allow "bob (user grant) SELECT audit"        bob   'SELECT count(*) FROM ops_wh.ops.audit'

echo
echo "== 5. Deny precedence =="
assert_deny  "alice SELECT audit (deny overrides analyst allow)" alice 'SELECT count(*) FROM ops_wh.ops.audit'

echo
echo "== 6. Negative tests =="
assert_deny  "dave (no roles) SELECT orders"        dave  'SELECT count(*) FROM sales_wh.sales.orders'
assert_deny  "alice (read-only) INSERT orders"      alice "INSERT INTO sales_wh.sales.orders VALUES (9,'x',0.0)"
assert_deny  "alice (read-only) DROP orders"        alice 'DROP TABLE sales_wh.sales.orders'
assert_deny  "dave SELECT audit"                    dave  'SELECT count(*) FROM ops_wh.ops.audit'

echo
echo "== 7. SHOW GRANTS round-trip (carol) =="
SG="$(sqe carol 'SHOW GRANTS ON sales_wh.sales.orders')"
if echo "$SG" | grep -qi analyst && echo "$SG" | grep -qi engineer; then
  green "PASS  SHOW GRANTS lists analyst + engineer"; PASS=$((PASS+1))
else
  red "FAIL  SHOW GRANTS missing analyst/engineer"; echo "$SG" | head -10; FAIL=$((FAIL+1))
fi

echo
echo "== 8. Revoke then re-check =="
run_admin "revoke analyst SELECT orders" 'REVOKE SELECT ON sales_wh.sales.orders FROM ROLE "analyst"'
assert_deny "alice SELECT orders after revoke" alice 'SELECT count(*) FROM sales_wh.sales.orders'

echo
echo "================ RESULT: ${PASS} passed, ${FAIL} failed ================"
[ "$FAIL" -eq 0 ] || exit 1
