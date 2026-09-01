#!/usr/bin/env bash
# Fast validation that does not start containers.
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh
require docker jq

step "checking shell scripts"
bash -n run.sh mint-token.sh test-mcp.sh setup-client.sh
ok "shell syntax"

step "checking JSON client and shared realm files"
jq -e '.mcpServers.sqe.type == "http"' clients/claude.mcp.json >/dev/null
jq -e '.clientScopes | map(.name) | contains(["sqe:mcp:read", "sqe:mcp:write"])' \
  ../_shared/keycloak/realm-iceberg.json >/dev/null
ok "JSON syntax and MCP scopes"

step "checking TOML files"
if command -v python3 >/dev/null 2>&1 && python3 -c 'import tomllib' 2>/dev/null; then
  python3 -c 'import tomllib; tomllib.load(open("sqe.toml", "rb")); tomllib.load(open("clients/codex-config.toml", "rb"))'
  ok "TOML syntax"
else
  warn "Python 3.11+ unavailable; runtime SQE and Codex perform the TOML checks"
fi

step "checking the reusable Compose overlay"
docker compose --env-file .env.example -p sqe-mcp-static \
  -f ../polaris-keycloak-user-token/docker-compose.yml \
  -f docker-compose.yml config --quiet
ok "Compose base + overlay"

ok "all static checks passed"
