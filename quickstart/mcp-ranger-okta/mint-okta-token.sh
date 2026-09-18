#!/usr/bin/env bash
# Mint an Okta access token with the device flow and print it (stdout is the
# token only). With --decode, print the claims SQE checks instead.
#   export OKTA_TOKEN="$(./mint-okta-token.sh)"
#   ./mint-okta-token.sh --decode
set -euo pipefail
cd "$(dirname "$0")"
. ../_shared/lib.sh
require curl jq
[ -f .env ] && { set -a; . ./.env; set +a; }
: "${OKTA_ISSUER:?set OKTA_ISSUER in .env}"
: "${OKTA_CLIENT_ID:?set OKTA_CLIENT_ID in .env (the sqemcp native app)}"
SCOPE=${OKTA_SCOPE:-"openid sqe:mcp:read"}

dev=$(curl -fsS -X POST "$OKTA_ISSUER/v1/device/authorize" \
  -d "client_id=$OKTA_CLIENT_ID" --data-urlencode "scope=$SCOPE")
echo "open: $(jq -r .verification_uri_complete <<<"$dev")" >&2
code=$(jq -r .device_code <<<"$dev"); interval=$(jq -r '.interval // 5' <<<"$dev")
while :; do
  sleep "$interval"
  resp=$(curl -sS -X POST "$OKTA_ISSUER/v1/token" -d "client_id=$OKTA_CLIENT_ID" \
    -d grant_type=urn:ietf:params:oauth:grant-type:device_code -d "device_code=$code")
  case "$(jq -r '.error // empty' <<<"$resp")" in
    "") break ;;
    authorization_pending|slow_down) ;;
    *) die "okta: $(jq -r .error_description <<<"$resp")" ;;
  esac
done
token=$(jq -er .access_token <<<"$resp")
if [ "${1:-}" = "--decode" ]; then
  payload=$(cut -d. -f2 <<<"$token" | tr '_-' '/+'); pad=$(( (4 - ${#payload} % 4) % 4 ))
  printf '%s%s' "$payload" "$(printf '=%.0s' $(seq 1 $pad))" | base64 -d | jq '{iss,aud,scp,groups,sub,exp}'
else
  printf '%s\n' "$token"
fi
