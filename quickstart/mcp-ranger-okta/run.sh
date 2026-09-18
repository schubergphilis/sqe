#!/usr/bin/env bash
# Polaris + Ranger + Keycloak stack, SQE MCP on top, Okta as the MCP identity provider.
#
#   ./run.sh            # up, protocol checks; identity + governed-read checks when OKTA_TOKEN is set
#   ./run.sh --writes   # expose execute_sql_change (Okta group engineer/sqe_admin may write)
#   ./run.sh --down     # tear everything down
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh

for a in "$@"; do
  case "$a" in
    --writes) export MCP_ALLOW_WRITES=true ;;
    --down) docker compose down -v; exit 0 ;;
    *) die "unknown arg: $a (use --writes or --down)" ;;
  esac
done

require docker curl jq
[ -f .env ] || { step "creating .env from .env.example"; cp .env.example .env; }
set -a; . ./.env; set +a
case "$OKTA_ISSUER" in *CHANGE-ME*) die "set OKTA_ISSUER in .env" ;; esac

step "starting Keycloak + Polaris + Ranger + RustFS + SQE (Ranger first boot takes 2-4 min)"
docker compose up -d --build --wait
ok "stack ready"

MCP_URL="http://localhost:${SQE_MCP_PORT:-19092}/mcp"
step "checking protected-resource metadata and bearer challenge"
metadata=$(curl -fsS "${MCP_URL%/mcp}/.well-known/oauth-protected-resource/mcp")
assert_eq "resource identifier" "$(jq -r .resource <<<"$metadata")" "$MCP_URL"
assert_eq "Okta advertised as authorization server" "$(jq -r '.authorization_servers[0]' <<<"$metadata")" "$OKTA_ISSUER"
code=$(curl -sS -o /dev/null -w '%{http_code}' "$MCP_URL" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"ping"}')
assert_eq "missing bearer returns 401" "$code" "401"

if [ -n "${OKTA_TOKEN:-}" ]; then
  rpc() {
    curl -fsS "$MCP_URL" -H "Authorization: Bearer $OKTA_TOKEN" -H 'Content-Type: application/json' \
      -H 'Accept: application/json' -H 'MCP-Protocol-Version: 2025-11-25' --data "$1"
  }
  step "checking Okta identity and a Ranger-governed read"
  who=$(rpc '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"whoami","arguments":{}}}')
  assert_not_empty "Okta user reaches MCP" "$(jq -r .result.structuredContent.user <<<"$who")"
  echo "  identity: $(jq -c .result.structuredContent <<<"$who")"
  rows=$(rpc '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"execute_sql_readonly","arguments":{"sql":"SELECT * FROM sales_wh.sales.orders ORDER BY 1 LIMIT 3"}}}')
  assert_eq "governed read succeeds" "$(jq -r .result.isError <<<"$rows")" "false"
  echo "  rows (masked per the caller's Ranger roles): $(jq -c .result.structuredContent.rows <<<"$rows")"
else
  warn "OKTA_TOKEN not set: skipped identity and governed-read checks"
  warn "get one with the Okta CLI / a device flow against $OKTA_ISSUER and re-run"
fi
check_summary
