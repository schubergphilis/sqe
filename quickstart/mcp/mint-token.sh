#!/usr/bin/env bash
# Print one short-lived Keycloak user token. Keep stdout token-only so callers
# can safely use: export SQE_MCP_TOKEN="$(./mint-token.sh)"
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh

require curl jq
[ -f .env ] && { set -a; . ./.env; set +a; }

USER_NAME=${1:-adminuser}
USER_PASSWORD=${2:-adminuser123}
REQUESTED_SCOPE=${3:-"openid sqe:mcp:write"}
KC_URL="http://localhost:${KEYCLOAK_PORT:-28080}"

curl -fsS -X POST "$KC_URL/realms/iceberg/protocol/openid-connect/token" \
  -d grant_type=password \
  -d client_id=polaris-frontend-client \
  -d "username=$USER_NAME" \
  -d "password=$USER_PASSWORD" \
  --data-urlencode "scope=$REQUESTED_SCOPE" \
  | jq -er .access_token
