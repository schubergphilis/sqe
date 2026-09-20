#!/usr/bin/env bash
# SQE + Ranger, Okta login, Glue + S3 data.
#   ./run.sh          start and check
#   ./run.sh --down   remove everything
# Needs: docker, curl, jq, python3; AWS credentials exported in this shell.
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh
[ "${1:-}" = --down ] && { docker compose down -v; exit 0; }

require docker curl jq python3
[ -f .env ] || cp .env.example .env
set -a; . ./.env; set +a
case "$OKTA_ISSUER$OKTA_CLIENT_ID" in *CHANGE-ME*) die "set OKTA_ISSUER and OKTA_CLIENT_ID in .env" ;; esac
[ -n "${AWS_ACCESS_KEY_ID:-}" ] || warn "no AWS credentials exported; Glue and S3 will fail"

# Claude Code reads this in the current directory; .claude/settings.json pre-approves it.
python3 - "$OKTA_CLIENT_ID" "${SQE_MCP_PORT:-19092}" <<'PY'
import json, sys
json.dump({"mcpServers": {"sqe": {"type": "http", "url": f"http://localhost:{sys.argv[2]}/mcp",
           "oauth": {"clientId": sys.argv[1], "callbackPort": 8400}}}}, open(".mcp.json", "w"), indent=2)
PY

step "starting Ranger + SQE (first boot 2-4 min)"
docker compose up -d --build --wait

MCP="http://localhost:${SQE_MCP_PORT:-19092}/mcp"
assert_eq "Okta advertised as authorization server" \
  "$(curl -fsS "${MCP%/mcp}/.well-known/oauth-protected-resource/mcp" | jq -r '.authorization_servers[0]')" "$OKTA_ISSUER"
assert_eq "missing bearer returns 401" \
  "$(curl -sS -o /dev/null -w '%{http_code}' "$MCP" -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"ping"}')" 401
check_summary
echo "Next: 'claude' here, then /mcp -> sqe -> Authenticate."
