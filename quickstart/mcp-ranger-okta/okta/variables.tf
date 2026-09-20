variable "org_name" {
  description = "Okta org, the part before .okta.com (acme for https://acme.okta.com)"
  type        = string
}

variable "base_url" {
  type    = string
  default = "okta.com"
}

variable "auth_server_name" {
  type    = string
  default = "sqemcp"
}

variable "app_label" {
  type    = string
  default = "sqemcp"
}

variable "audience" {
  description = "Must equal SQE's mcp.resource_uri"
  type        = string
  default     = "http://localhost:19092/mcp"
}

variable "redirect_uris" {
  description = "Loopback callbacks of the MCP clients (Okta needs exact ports)"
  type        = list(string)
  default     = ["http://localhost:8400/callback"]
}

variable "readonly_group" {
  type    = string
  default = "datalake_readonly"
}

variable "sensitive_group" {
  type    = string
  default = "datalake_sensitive_ro"
}

variable "groups_claim_regex" {
  type    = string
  default = "^datalake_.*"
}
