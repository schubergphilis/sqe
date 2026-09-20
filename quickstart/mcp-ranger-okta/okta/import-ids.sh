#!/usr/bin/env bash
# Print Terraform `import` blocks for Okta objects that already exist (created
# by hand in the console), so `terraform plan` adopts instead of duplicating.
#   OKTA_API_TOKEN=... ./import-ids.sh <org> <auth server id> <client id> > imports.tf
set -euo pipefail
ORG=${1:?org name}; AS=${2:?auth server id}; APP=${3:?app client id}
: "${OKTA_API_TOKEN:?export OKTA_API_TOKEN}"
api() { curl -fsS -H "Authorization: SSWS $OKTA_API_TOKEN" -H 'Accept: application/json' "https://$ORG.okta.com/api/v1$1"; }
imp() { printf 'import {\n  to = %s\n  id = "%s"\n}\n\n' "$1" "$2"; }

imp okta_auth_server.sqemcp "$AS"
imp okta_app_oauth.sqemcp "$APP"
imp okta_app_group_assignments.sqemcp "$APP"
for g in datalake_readonly:readonly datalake_sensitive_ro:sensitive; do
  id=$(api "/groups?q=${g%%:*}" | jq -r '.[0].id // empty'); [ -n "$id" ] && imp "okta_group.${g##*:}" "$id"
done
for s in read write; do
  id=$(api "/authorizationServers/$AS/scopes" | jq -r ".[] | select(.name==\"sqe:mcp:$s\") | .id"); [ -n "$id" ] && imp "okta_auth_server_scope.$s" "$AS/$id"
done
id=$(api "/authorizationServers/$AS/claims" | jq -r '.[] | select(.name=="groups") | .id'); [ -n "$id" ] && imp okta_auth_server_claim.groups "$AS/$id"
pol=$(api "/authorizationServers/$AS/policies" | jq -r '.[] | select(.conditions.clients.include | index("'"$APP"'")) | .id' | head -1)
if [ -n "$pol" ]; then
  imp okta_auth_server_policy.sqemcp "$AS/$pol"
  rule=$(api "/authorizationServers/$AS/policies/$pol/rules" | jq -r '.[0].id // empty'); [ -n "$rule" ] && imp okta_auth_server_policy_rule.sqemcp "$AS/$pol/$rule"
fi
