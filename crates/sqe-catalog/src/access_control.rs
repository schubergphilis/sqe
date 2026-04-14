//! HTTP client for the platform access control API.
//!
//! Forwards GRANT/REVOKE/SHOW GRANTS/CHECK ACCESS SQL to the configured
//! platform API URL. The user's bearer token is passed through for
//! authentication — no service account is used.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use sqe_core::SqeError;

/// HTTP client for the platform access control API.
///
/// Created once per `QueryHandler` (when `platform_api_url` is configured)
/// and reused across all sessions. The user's bearer token is passed per-call
/// so that authorization is always scoped to the authenticated user.
pub struct AccessControlClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Serialize)]
pub struct GrantRequest {
    pub privilege: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub grantee_type: String,
    pub grantee_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ShowGrantsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grantee_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grantee_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CheckAccessRequest {
    pub user: String,
    pub privilege: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GrantEntry {
    pub privilege: String,
    pub resource: String,
    pub grantee_type: String,
    pub grantee_name: String,
    pub effect: String,
    pub granted_by: Option<String>,
    pub granted_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CheckAccessResponse {
    pub allowed: bool,
    pub reason: Option<String>,
}

impl AccessControlClient {
    /// Create a new client targeting the given platform API base URL.
    ///
    /// The `base_url` should be something like
    /// `https://polaris.example.com/api/platform/v1/access` — the individual
    /// endpoint paths (`/grant`, `/revoke`, `/grants`, `/effective`, `/check`)
    /// are appended automatically.
    pub fn new(base_url: &str) -> sqe_core::Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| SqeError::Config(format!("Failed to build HTTP client: {e}")))?;

        // Strip trailing slash for consistent URL construction.
        let base_url = base_url.trim_end_matches('/').to_string();

        Ok(Self { client, base_url })
    }

    /// POST /grant — create or update a privilege grant.
    #[instrument(skip(self, token, req), fields(privilege = %req.privilege, grantee = %req.grantee_name))]
    pub async fn grant(&self, token: &str, req: &GrantRequest) -> sqe_core::Result<()> {
        let url = format!("{}/grant", self.base_url);
        debug!("POST {url}");

        let resp = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(req)
            .send()
            .await
            .map_err(|e| SqeError::Execution(format!("Access control API request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SqeError::Execution(format!(
                "GRANT failed (HTTP {status}): {text}"
            )));
        }

        Ok(())
    }

    /// POST /revoke — remove a privilege grant.
    #[instrument(skip(self, token, req), fields(privilege = %req.privilege, grantee = %req.grantee_name))]
    pub async fn revoke(&self, token: &str, req: &GrantRequest) -> sqe_core::Result<()> {
        let url = format!("{}/revoke", self.base_url);
        debug!("POST {url}");

        let resp = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(req)
            .send()
            .await
            .map_err(|e| SqeError::Execution(format!("Access control API request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SqeError::Execution(format!(
                "REVOKE failed (HTTP {status}): {text}"
            )));
        }

        Ok(())
    }

    /// GET /grants — list grants matching the given filter parameters.
    #[instrument(skip(self, token, params))]
    pub async fn show_grants(
        &self,
        token: &str,
        params: &ShowGrantsParams,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let url = format!("{}/grants", self.base_url);
        debug!("GET {url}");

        let resp = self
            .client
            .get(&url)
            .bearer_auth(token)
            .query(params)
            .send()
            .await
            .map_err(|e| SqeError::Execution(format!("Access control API request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SqeError::Execution(format!(
                "SHOW GRANTS failed (HTTP {status}): {text}"
            )));
        }

        let entries: Vec<GrantEntry> = resp
            .json()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to parse grants response: {e}")))?;

        Ok(entries)
    }

    /// GET /effective — list effective grants for a user.
    #[instrument(skip(self, token), fields(user = %user))]
    pub async fn show_effective(
        &self,
        token: &str,
        user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let url = format!("{}/effective", self.base_url);
        debug!("GET {url}?user={user}");

        let resp = self
            .client
            .get(&url)
            .bearer_auth(token)
            .query(&[("user", user)])
            .send()
            .await
            .map_err(|e| SqeError::Execution(format!("Access control API request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SqeError::Execution(format!(
                "SHOW EFFECTIVE GRANTS failed (HTTP {status}): {text}"
            )));
        }

        let entries: Vec<GrantEntry> = resp
            .json()
            .await
            .map_err(|e| {
                SqeError::Execution(format!("Failed to parse effective grants response: {e}"))
            })?;

        Ok(entries)
    }

    /// POST /check — check whether a user has a specific privilege.
    #[instrument(skip(self, token, req), fields(user = %req.user, privilege = %req.privilege))]
    pub async fn check_access(
        &self,
        token: &str,
        req: &CheckAccessRequest,
    ) -> sqe_core::Result<CheckAccessResponse> {
        let url = format!("{}/check", self.base_url);
        debug!("POST {url}");

        let resp = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(req)
            .send()
            .await
            .map_err(|e| SqeError::Execution(format!("Access control API request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(SqeError::Execution(format!(
                "CHECK ACCESS failed (HTTP {status}): {text}"
            )));
        }

        let response: CheckAccessResponse = resp
            .json()
            .await
            .map_err(|e| {
                SqeError::Execution(format!("Failed to parse check access response: {e}"))
            })?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── GrantRequest serialization ──────────────────────────────────

    #[test]
    fn grant_request_serializes_full() {
        let req = GrantRequest {
            privilege: "SELECT".to_string(),
            catalog: Some("my_catalog".to_string()),
            namespace: Some("my_schema".to_string()),
            table: Some("my_table".to_string()),
            grantee_type: "ROLE".to_string(),
            grantee_name: "analyst".to_string(),
            effect: Some("ALLOW".to_string()),
        };

        let json: serde_json::Value = serde_json::to_value(&req).unwrap();

        assert_eq!(json["privilege"], "SELECT");
        assert_eq!(json["catalog"], "my_catalog");
        assert_eq!(json["namespace"], "my_schema");
        assert_eq!(json["table"], "my_table");
        assert_eq!(json["grantee_type"], "ROLE");
        assert_eq!(json["grantee_name"], "analyst");
        assert_eq!(json["effect"], "ALLOW");
    }

    #[test]
    fn grant_request_omits_none_fields() {
        let req = GrantRequest {
            privilege: "INSERT".to_string(),
            catalog: None,
            namespace: None,
            table: None,
            grantee_type: "NONE".to_string(),
            grantee_name: "alice".to_string(),
            effect: None,
        };

        let json: serde_json::Value = serde_json::to_value(&req).unwrap();

        assert_eq!(json["privilege"], "INSERT");
        assert!(json.get("catalog").is_none(), "None catalog should be omitted");
        assert!(json.get("namespace").is_none(), "None namespace should be omitted");
        assert!(json.get("table").is_none(), "None table should be omitted");
        assert!(json.get("effect").is_none(), "None effect should be omitted");
        // grantee_type and grantee_name are always present
        assert_eq!(json["grantee_type"], "NONE");
        assert_eq!(json["grantee_name"], "alice");
    }

    #[test]
    fn grant_request_roundtrip_json_string() {
        let req = GrantRequest {
            privilege: "SELECT".to_string(),
            catalog: Some("prod".to_string()),
            namespace: Some("sales".to_string()),
            table: Some("orders".to_string()),
            grantee_type: "ROLE".to_string(),
            grantee_name: "data_eng".to_string(),
            effect: None,
        };

        let json_str = serde_json::to_string(&req).unwrap();
        // Verify it's valid JSON that can be re-parsed
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["privilege"], "SELECT");
        assert_eq!(parsed["table"], "orders");
        assert!(parsed.get("effect").is_none());
    }

    // ── GrantEntry deserialization ──────────────────────────────────

    #[test]
    fn grant_entry_deserializes_full_response() {
        let json = r#"{
            "privilege": "SELECT",
            "resource": "my_catalog.my_schema.my_table",
            "grantee_type": "ROLE",
            "grantee_name": "analyst",
            "effect": "ALLOW",
            "granted_by": "admin",
            "granted_at": "2026-04-14T10:00:00Z"
        }"#;

        let entry: GrantEntry = serde_json::from_str(json).unwrap();

        assert_eq!(entry.privilege, "SELECT");
        assert_eq!(entry.resource, "my_catalog.my_schema.my_table");
        assert_eq!(entry.grantee_type, "ROLE");
        assert_eq!(entry.grantee_name, "analyst");
        assert_eq!(entry.effect, "ALLOW");
        assert_eq!(entry.granted_by.as_deref(), Some("admin"));
        assert_eq!(entry.granted_at.as_deref(), Some("2026-04-14T10:00:00Z"));
    }

    #[test]
    fn grant_entry_deserializes_with_optional_fields_null() {
        let json = r#"{
            "privilege": "INSERT",
            "resource": "catalog.schema.table",
            "grantee_type": "USER",
            "grantee_name": "bob",
            "effect": "DENY",
            "granted_by": null,
            "granted_at": null
        }"#;

        let entry: GrantEntry = serde_json::from_str(json).unwrap();

        assert_eq!(entry.privilege, "INSERT");
        assert_eq!(entry.effect, "DENY");
        assert!(entry.granted_by.is_none());
        assert!(entry.granted_at.is_none());
    }

    #[test]
    fn grant_entry_deserializes_with_optional_fields_absent() {
        let json = r#"{
            "privilege": "DELETE",
            "resource": "prod.analytics.events",
            "grantee_type": "ROLE",
            "grantee_name": "etl",
            "effect": "ALLOW"
        }"#;

        let entry: GrantEntry = serde_json::from_str(json).unwrap();

        assert_eq!(entry.privilege, "DELETE");
        assert_eq!(entry.resource, "prod.analytics.events");
        assert!(entry.granted_by.is_none());
        assert!(entry.granted_at.is_none());
    }

    #[test]
    fn grant_entry_list_deserializes() {
        let json = r#"[
            {
                "privilege": "SELECT",
                "resource": "prod.public.users",
                "grantee_type": "ROLE",
                "grantee_name": "reader",
                "effect": "ALLOW"
            },
            {
                "privilege": "INSERT",
                "resource": "prod.public.events",
                "grantee_type": "USER",
                "grantee_name": "alice",
                "effect": "ALLOW",
                "granted_by": "admin",
                "granted_at": "2026-01-01T00:00:00Z"
            }
        ]"#;

        let entries: Vec<GrantEntry> = serde_json::from_str(json).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].privilege, "SELECT");
        assert_eq!(entries[0].grantee_name, "reader");
        assert!(entries[0].granted_by.is_none());
        assert_eq!(entries[1].privilege, "INSERT");
        assert_eq!(entries[1].granted_by.as_deref(), Some("admin"));
    }

    // ── CheckAccessResponse deserialization ─────────────────────────

    #[test]
    fn check_access_response_allowed() {
        let json = r#"{"allowed": true, "reason": null}"#;
        let resp: CheckAccessResponse = serde_json::from_str(json).unwrap();
        assert!(resp.allowed);
        assert!(resp.reason.is_none());
    }

    #[test]
    fn check_access_response_denied_with_reason() {
        let json = r#"{"allowed": false, "reason": "No matching grant found"}"#;
        let resp: CheckAccessResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.allowed);
        assert_eq!(resp.reason.as_deref(), Some("No matching grant found"));
    }

    #[test]
    fn check_access_response_allowed_without_reason_field() {
        let json = r#"{"allowed": true}"#;
        let resp: CheckAccessResponse = serde_json::from_str(json).unwrap();
        assert!(resp.allowed);
        assert!(resp.reason.is_none());
    }

    // ── CheckAccessRequest serialization ────────────────────────────

    #[test]
    fn check_access_request_serializes_full() {
        let req = CheckAccessRequest {
            user: "alice".to_string(),
            privilege: "SELECT".to_string(),
            catalog: Some("prod".to_string()),
            namespace: Some("public".to_string()),
            table: Some("users".to_string()),
        };

        let json: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(json["user"], "alice");
        assert_eq!(json["privilege"], "SELECT");
        assert_eq!(json["catalog"], "prod");
        assert_eq!(json["namespace"], "public");
        assert_eq!(json["table"], "users");
    }

    #[test]
    fn check_access_request_omits_none_fields() {
        let req = CheckAccessRequest {
            user: "bob".to_string(),
            privilege: "INSERT".to_string(),
            catalog: None,
            namespace: None,
            table: None,
        };

        let json: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(json["user"], "bob");
        assert_eq!(json["privilege"], "INSERT");
        assert!(json.get("catalog").is_none());
        assert!(json.get("namespace").is_none());
        assert!(json.get("table").is_none());
    }

    // ── ShowGrantsParams serialization ──────────────────────────────

    #[test]
    fn show_grants_params_serializes_resource_filter() {
        let params = ShowGrantsParams {
            catalog: Some("prod".to_string()),
            namespace: Some("analytics".to_string()),
            table: Some("events".to_string()),
            grantee_type: None,
            grantee_name: None,
        };

        let json: serde_json::Value = serde_json::to_value(&params).unwrap();
        assert_eq!(json["catalog"], "prod");
        assert_eq!(json["namespace"], "analytics");
        assert_eq!(json["table"], "events");
        assert!(json.get("grantee_type").is_none());
        assert!(json.get("grantee_name").is_none());
    }

    #[test]
    fn show_grants_params_serializes_grantee_filter() {
        let params = ShowGrantsParams {
            catalog: None,
            namespace: None,
            table: None,
            grantee_type: Some("ROLE".to_string()),
            grantee_name: Some("admin".to_string()),
        };

        let json: serde_json::Value = serde_json::to_value(&params).unwrap();
        assert!(json.get("catalog").is_none());
        assert!(json.get("namespace").is_none());
        assert!(json.get("table").is_none());
        assert_eq!(json["grantee_type"], "ROLE");
        assert_eq!(json["grantee_name"], "admin");
    }

    #[test]
    fn show_grants_params_empty_serializes_to_empty_object() {
        let params = ShowGrantsParams {
            catalog: None,
            namespace: None,
            table: None,
            grantee_type: None,
            grantee_name: None,
        };

        let json: serde_json::Value = serde_json::to_value(&params).unwrap();
        assert_eq!(json, serde_json::json!({}));
    }

    // ── AccessControlClient construction ────────────────────────────

    #[test]
    fn client_strips_trailing_slash() {
        let client = AccessControlClient::new("https://api.example.com/v1/access/").unwrap();
        assert_eq!(client.base_url, "https://api.example.com/v1/access");
    }

    #[test]
    fn client_preserves_url_without_trailing_slash() {
        let client = AccessControlClient::new("https://api.example.com/v1/access").unwrap();
        assert_eq!(client.base_url, "https://api.example.com/v1/access");
    }
}
