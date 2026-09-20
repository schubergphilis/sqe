# Okta objects for the SQE MCP quickstart, exactly what the README describes by
# hand: a custom authorization server with the two SQE scopes and a `groups`
# claim on the access token, an access policy for the native app, the app
# itself (Authorization Code + PKCE, refresh, device grant), and the two groups.
#
# Auth: export OKTA_API_TOKEN (Security > API > Tokens) or use an OAuth service
# app via OKTA_API_CLIENT_ID / OKTA_API_PRIVATE_KEY / OKTA_API_SCOPES.
terraform {
  required_version = ">= 1.5"
  required_providers {
    okta = {
      source  = "okta/okta"
      version = "~> 4.0"
    }
  }
}

provider "okta" {
  org_name = var.org_name
  base_url = var.base_url
}

resource "okta_group" "readonly" {
  name        = var.readonly_group
  description = "SQE: may read every table that is not sensitive"
}

resource "okta_group" "sensitive" {
  name        = var.sensitive_group
  description = "SQE: may read the sensitive tables"
}

resource "okta_auth_server" "sqemcp" {
  name        = var.auth_server_name
  description = "Issues access tokens for the SQE MCP endpoint"
  audiences   = [var.audience]
  issuer_mode = "ORG_URL"
}

resource "okta_auth_server_scope" "read" {
  auth_server_id   = okta_auth_server.sqemcp.id
  name             = "sqe:mcp:read"
  description      = "Read-only SQE MCP tools"
  default          = true
  metadata_publish = "ALL_CLIENTS"
}

resource "okta_auth_server_scope" "write" {
  auth_server_id   = okta_auth_server.sqemcp.id
  name             = "sqe:mcp:write"
  description      = "SQE MCP change tool (still gated by SQE role and source grants)"
  metadata_publish = "ALL_CLIENTS"
}

# The groups SQE maps to Ranger roles, on the ACCESS token.
resource "okta_auth_server_claim" "groups" {
  auth_server_id          = okta_auth_server.sqemcp.id
  name                    = "groups"
  claim_type              = "RESOURCE"
  value_type              = "GROUPS"
  group_filter_type       = "REGEX"
  value                   = var.groups_claim_regex
  always_include_in_token = true
}

resource "okta_app_oauth" "sqemcp" {
  label                      = var.app_label
  type                       = "native"
  grant_types                = ["authorization_code", "refresh_token", "urn:ietf:params:oauth:grant-type:device_code"]
  response_types             = ["code"]
  redirect_uris              = var.redirect_uris
  token_endpoint_auth_method = "none"
  pkce_required              = true
}

resource "okta_app_group_assignments" "sqemcp" {
  app_id = okta_app_oauth.sqemcp.id
  group {
    id = okta_group.readonly.id
  }
  group {
    id = okta_group.sensitive.id
  }
}

# Without this policy Okta answers `access_denied` after a successful login.
resource "okta_auth_server_policy" "sqemcp" {
  auth_server_id   = okta_auth_server.sqemcp.id
  name             = "sqemcp"
  description      = "MCP clients"
  priority         = 1
  client_whitelist = [okta_app_oauth.sqemcp.client_id]
}

resource "okta_auth_server_policy_rule" "sqemcp" {
  auth_server_id                 = okta_auth_server.sqemcp.id
  policy_id                      = okta_auth_server_policy.sqemcp.id
  name                           = "assigned users, all grants"
  priority                       = 1
  grant_type_whitelist           = ["authorization_code", "refresh_token", "urn:ietf:params:oauth:grant-type:device_code"]
  group_whitelist                = ["EVERYONE"]
  scope_whitelist                = ["*"]
  access_token_lifetime_minutes  = 60
  refresh_token_lifetime_minutes = 10080
  refresh_token_window_minutes   = 10080
}

output "okta_issuer" {
  value = okta_auth_server.sqemcp.issuer
}

output "okta_client_id" {
  value = okta_app_oauth.sqemcp.client_id
}
