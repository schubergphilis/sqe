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
