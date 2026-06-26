#!/usr/bin/env bash
# End-to-end test for the per-connection client_credentials passthrough quickstart.
#
# Proves: a client presents its OWN OAuth2 client_id/client_secret on the SQE
# connection (Flight Basic auth: username = client_id, password = client_secret);
# SQE runs the client_credentials grant per connection and forwards the token to
# Polaris; Apache Ranger authorizes the service principal at the Polaris boundary.
#
# The proof of per-connection identity: the SAME query returns a different result
# depending on which service principal's credentials connected.
#   sp-admin  -> ADMIN_ACCESS  -> creates + seeds sales_wh.sales.orders
#   sp-reader -> READ on orders -> SELECT allowed
#   sp-denied -> no grant       -> SELECT denied
set -uo pipefail
cd "$(dirname "$0")"

KEYCLOAK_PORT="${KEYCLOAK_PORT:-38080}"
KC="http://localhost:${KEYCLOAK_PORT}/realms/iceberg-ranger/protocol/openid-connect/token"

# SP credentials (must match keycloak/realm-ranger.json + .env).
SP_ADMIN_SECRET="${SP_ADMIN_SECRET:-sp-admin-secret}"
SP_READER_SECRET="${SP_READER_SECRET:-sp-reader-secret}"
SP_DENIED_SECRET="${SP_DENIED_SECRET:-sp-denied-secret}"

TRINO="http://localhost:${SQE_TRINO_PORT:-28080}"

PASS=0; FAIL=0
green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }

b64() { printf '%s' "$1" | base64 | tr -d '\n'; }

# Run a query over the Trino-compat HTTP path. $1 = full Authorization header
# value (e.g. "Basic ..." or "Bearer ..."), $2 = X-Trino-User, $3 = SQL.
# Prints DONE on success, or QUERYERROR/HTTPERROR/TIMEOUT with detail.
trino_run() { # auth_header user sql
  python3 - "$TRINO" "$1" "$2" "$3" <<'PY'
import sys, json, time, urllib.request, urllib.error
base, auth, user, sql = sys.argv[1:5]
def get(url, data=None, method="GET"):
    req = urllib.request.Request(url, data=(data.encode() if data else None), method=method,
        headers={"Authorization": auth, "X-Trino-User": user, "Content-Type": "text/plain"})
    return json.load(urllib.request.urlopen(req, timeout=15))
try:
    j = get(base + "/v1/statement", data=sql, method="POST")
    for _ in range(80):
        if j.get("error"):
            print("QUERYERROR", json.dumps(j["error"])[:300]); sys.exit(0)
        nu = j.get("nextUri")
        if not nu:
            print("DONE"); sys.exit(0)
        time.sleep(0.25)
        j = get(nu)
    print("TIMEOUT")
except urllib.error.HTTPError as e:
    print("HTTPERROR", e.code, e.read()[:200].decode(errors="replace"))
except Exception as e:
    print("HTTPERROR 0", str(e)[:200])
PY
}

assert_trino_allow() { # desc client_id secret sql
  local out; out="$(trino_run "Basic $(b64 "$2:$3")" "$2" "$4")"
  if echo "$out" | grep -q '^DONE'; then green "PASS  $1"; PASS=$((PASS+1));
  else red "FAIL  $1"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1)); fi
}
assert_trino_deny() { # desc client_id secret sql
  local out; out="$(trino_run "Basic $(b64 "$2:$3")" "$2" "$4")"
  if echo "$out" | grep -qE 'QUERYERROR|HTTPERROR'; then green "PASS  $1 (denied)"; PASS=$((PASS+1));
  else red "FAIL  $1 (expected denial)"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1)); fi
}

# Run SQL as a service principal: username = client_id, password = client_secret.
# sqe-cli prints "Error: ..." on failure and still exits 0, so classify by text.
sqe() { # client_id client_secret sql
  docker compose exec -T -e "SQE_PASSWORD=${2}" sqe \
    sqe-cli --port 50051 --user "$1" -e "$3" 2>&1
}
is_error()  { echo "$1" | grep -qi 'Error:'; }
# A denied table is HIDDEN as "not found" (Polaris/SQE information-hiding), so
# "not found" reliably means access denied for a table that exists.
is_denial() { echo "$1" | grep -qiE 'not authorized|forbidden|unauthorized|403|denied|permission|not found|does not exist'; }

assert_allow() { # desc client_id client_secret sql
  local out
  for _ in 1 2 3 4 5 6; do
    out="$(sqe "$2" "$3" "$4")"
    if ! is_error "$out"; then green "PASS  $1"; PASS=$((PASS+1)); return 0; fi
    sleep 5
  done
  red "FAIL  $1"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1))
}

assert_deny() { # desc client_id client_secret sql
  local out
  for _ in 1 2 3 4 5 6; do
    out="$(sqe "$2" "$3" "$4")"
    if is_denial "$out"; then green "PASS  $1 (denied)"; PASS=$((PASS+1)); return 0; fi
    sleep 4
  done
  red "FAIL  $1 (expected denial)"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1))
}

# Any failure (auth error OR access denial) counts as rejected.
assert_rejected() { # desc client_id client_secret sql
  local out; out="$(sqe "$2" "$3" "$4")"
  if is_error "$out" || is_denial "$out"; then green "PASS  $1 (rejected)"; PASS=$((PASS+1));
  else red "FAIL  $1 (expected rejection, got success)"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1)); fi
}

admin() { # desc sql
  local out; out="$(sqe sp-admin "$SP_ADMIN_SECRET" "$1")"
  if is_error "$out"; then red "ADMIN STEP FAILED: $1"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1))
  else green "ok    $1"; fi
}

# ── 1. Token-shape check: mint each SP token straight from Keycloak ──────────
# This is the make-or-break of the whole feature. Before any SQE query, confirm
# the client_credentials token carries the claims Polaris needs: preferred_username
# = the SP name (principal mapping) and aud = account (Polaris audience check).
echo "== 1. Service-principal token shape (minted directly from Keycloak) =="
check_token() { # sp secret
  local resp tok claims
  resp="$(curl -s -X POST "$KC" -d grant_type=client_credentials -d "client_id=$1" -d "client_secret=$2")"
  tok="$(echo "$resp" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null)"
  if [ -z "$tok" ]; then red "FAIL  $1 token mint"; echo "      $resp"; FAIL=$((FAIL+1)); return; fi
  claims="$(echo "$tok" | python3 -c '
import sys,base64,json
p=sys.stdin.read().strip().split(".")[1]; p+="="*(-len(p)%4)
c=json.loads(base64.urlsafe_b64decode(p))
aud=c.get("aud"); aud=aud if isinstance(aud,list) else [aud]
print(c.get("preferred_username",""), "account" in aud)
')"
  local pu acct; pu="$(echo "$claims" | awk '{print $1}')"; acct="$(echo "$claims" | awk '{print $2}')"
  if [ "$pu" = "$1" ] && [ "$acct" = "True" ]; then
    green "PASS  $1: preferred_username=$pu, aud contains account"; PASS=$((PASS+1))
  else
    red "FAIL  $1: preferred_username=$pu, aud-has-account=$acct"; FAIL=$((FAIL+1))
  fi
}
check_token sp-admin  "$SP_ADMIN_SECRET"
check_token sp-reader "$SP_READER_SECRET"
check_token sp-denied "$SP_DENIED_SECRET"

# ── 2. sp-admin seeds the table (write path under a forwarded SP token) ──────
echo
echo "== 2. sp-admin creates + seeds sales_wh.sales.orders =="
admin "CREATE TABLE IF NOT EXISTS sales_wh.sales.orders (id BIGINT, region VARCHAR, amount DOUBLE)"
admin "INSERT INTO sales_wh.sales.orders VALUES (1,'EU',10.0),(2,'US',20.0)"

# ── 3. The per-connection identity proof ─────────────────────────────────────
echo
echo "== 3. Same query, different connection credentials, different outcome =="
assert_allow "sp-reader SELECT orders (granted)"  sp-reader "$SP_READER_SECRET" "SELECT region FROM sales_wh.sales.orders LIMIT 1"
assert_deny  "sp-denied SELECT orders (no grant)" sp-denied "$SP_DENIED_SECRET" "SELECT region FROM sales_wh.sales.orders LIMIT 1"

# ── 4. sp-reader is read-only: writes are denied ─────────────────────────────
echo
echo "== 4. sp-reader is read-only =="
assert_deny "sp-reader INSERT orders (no write grant)" sp-reader "$SP_READER_SECRET" "INSERT INTO sales_wh.sales.orders VALUES (9,'x',0.0)"

# ── 5. Wrong secret is rejected ──────────────────────────────────────────────
echo
echo "== 5. A wrong client_secret is rejected at auth =="
assert_rejected "sp-reader with WRONG secret" sp-reader "totally-wrong-secret" "SELECT 1"

echo
echo "== 6. Same service principal over the Trino HTTP path (Basic auth) =="
# Proves Trino HTTP Basic auth now routes through the auth chain and reaches
# client_credentials_passthrough, exactly like Flight SQL.
assert_trino_allow "sp-reader SELECT orders over Trino HTTP"  sp-reader "$SP_READER_SECRET" "SELECT region FROM sales_wh.sales.orders LIMIT 1"
assert_trino_deny  "sp-denied SELECT orders over Trino HTTP"  sp-denied "$SP_DENIED_SECRET" "SELECT region FROM sales_wh.sales.orders LIMIT 1"

echo
echo "== 7. Client-fetched bearer token over Trino HTTP =="
# The client runs client_credentials against Keycloak itself, then presents the
# JWT as a Bearer. SQE validates it via JWKS (bearer_token provider) and forwards.
SP_READER_JWT="$(curl -s -X POST "$KC" -d grant_type=client_credentials -d client_id=sp-reader -d "client_secret=$SP_READER_SECRET" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null)"
if [ -z "$SP_READER_JWT" ]; then
  red "FAIL  could not mint sp-reader bearer token"; FAIL=$((FAIL+1))
else
  out="$(trino_run "Bearer $SP_READER_JWT" sp-reader "SELECT region FROM sales_wh.sales.orders LIMIT 1")"
  if echo "$out" | grep -q '^DONE'; then green "PASS  sp-reader SELECT orders via Bearer (Trino HTTP)"; PASS=$((PASS+1));
  else red "FAIL  sp-reader Bearer over Trino"; echo "      $(echo "$out" | tr '\n' ' ' | cut -c1-200)"; FAIL=$((FAIL+1)); fi
fi

echo
echo "================ RESULT: ${PASS} passed, ${FAIL} failed ================"
[ "$FAIL" -eq 0 ] || exit 1
