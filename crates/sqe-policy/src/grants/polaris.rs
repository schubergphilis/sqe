//! PolarisGrantBackend — calls the Polaris Management REST API.
//!
//! Implements the three-step grant chain:
//! 1. Ensure catalog role exists (`POST /catalogs/{c}/catalog-roles`)
//! 2. Grant privilege to catalog role (`PUT /catalogs/{c}/catalog-roles/{r}/grants`)
//! 3. Assign catalog role to principal role (`PUT /principal-roles/{pr}/catalog-roles/{c}`)

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    Grantee, RevokeStatement,
};

// ── Privilege mapping (SQL -> Polaris) ────────────────────────────────────────
//
// NOTE: this helper was moved here from grants/mod.rs to keep Polaris-specific
// code out of the backend-neutral types file.

/// Map a SQL privilege string to the Polaris privilege name and resource type.
///
/// Returns `(polaris_privilege, resource_type)`. If the input already looks
/// like a Polaris native name (all-caps with underscores), pass it through.
pub fn map_sql_to_polaris_privilege(sql_priv: &str) -> (String, &'static str) {
    match sql_priv.to_uppercase().as_str() {
        "SELECT" => ("TABLE_READ_DATA".into(), "table"),
        "INSERT" => ("TABLE_WRITE_DATA".into(), "table"),
        "CREATE TABLE" => ("TABLE_CREATE".into(), "namespace"),
        "DROP" => ("TABLE_DROP".into(), "table"),
        "ALL" | "ALL PRIVILEGES" => ("CATALOG_MANAGE_CONTENT".into(), "catalog"),
        "USAGE" => ("NAMESPACE_LIST".into(), "namespace"),
        "CREATE SCHEMA" | "CREATE" => ("NAMESPACE_CREATE".into(), "catalog"),
        "DROP SCHEMA" => ("NAMESPACE_DROP".into(), "namespace"),
        _ => {
            // Pass-through: send unrecognized privileges verbatim.
            // Polaris native names (ALL_CAPS_WITH_UNDERSCORES) go through as-is.
            (sql_priv.to_uppercase(), "table")
        }
    }
}

/// Polaris Management API grant backend.
pub struct PolarisGrantBackend {
    client: Client,
    /// Base URL for the Polaris Management API, e.g.
    /// `http://polaris:8181/api/management/v1`
    management_url: String,
    /// Optional service token source. When present, the backend fetches
    /// a service token instead of using the user's passthrough token.
    service_token: Option<ServiceTokenSource>,
}

/// OAuth2 client_credentials token source for Polaris management API.
struct ServiceTokenSource {
    token_url: String,
    client_id: String,
    client_secret: String,
    cache: moka::future::Cache<String, String>,
}

/// OAuth2 token response from Polaris.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default = "default_expires_in")]
    #[allow(dead_code)]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    3600
}

impl PolarisGrantBackend {
    /// Create a new Polaris grant backend.
    ///
    /// `management_url` is the base URL for the Management API
    /// (e.g. `http://polaris:8181/api/management/v1`).
    ///
    /// When `client_id` and `client_secret` are both `Some`, the backend
    /// uses OAuth2 client_credentials to fetch a service token. Otherwise
    /// it uses the user's passthrough token.
    pub fn new(
        management_url: &str,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> sqe_core::Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                sqe_core::SqeError::Config(format!("Failed to build HTTP client: {e}"))
            })?;

        let management_url = management_url.trim_end_matches('/').to_string();

        let service_token = match (client_id, client_secret) {
            (Some(id), Some(secret)) => {
                // Derive token URL from management URL:
                // http://polaris:8181/api/management/v1 -> http://polaris:8181/api/catalog/v1/oauth/tokens
                let token_url = management_url
                    .replace("/api/management/v1", "/api/catalog/v1/oauth/tokens");
                Some(ServiceTokenSource {
                    token_url,
                    client_id: id,
                    client_secret: secret,
                    cache: moka::future::Cache::builder()
                        .max_capacity(1)
                        .time_to_live(std::time::Duration::from_secs(3570))
                        .build(),
                })
            }
            _ => None,
        };

        Ok(Self {
            client,
            management_url,
            service_token,
        })
    }

    /// Resolve the token to use for a Management API call.
    ///
    /// If service credentials are configured, fetch (or return cached)
    /// service token. Otherwise return the user's passthrough token.
    async fn resolve_token(&self, user_token: &str) -> sqe_core::Result<String> {
        match &self.service_token {
            None => Ok(user_token.to_string()),
            Some(source) => {
                let key = "polaris_service_token".to_string();
                if let Some(cached) = source.cache.get(&key).await {
                    return Ok(cached);
                }
                debug!(token_url = %source.token_url, "Fetching Polaris service token");
                let resp = self
                    .client
                    .post(&source.token_url)
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("client_id", &source.client_id),
                        ("client_secret", &source.client_secret),
                        ("scope", "PRINCIPAL_ROLE:ALL"),
                    ])
                    .send()
                    .await
                    .map_err(|e| {
                        sqe_core::SqeError::Auth(format!(
                            "Polaris token fetch failed: {e}"
                        ))
                    })?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(sqe_core::SqeError::Auth(format!(
                        "Polaris token fetch failed (HTTP {status}): {text}"
                    )));
                }

                let token_resp: TokenResponse = resp.json().await.map_err(|e| {
                    sqe_core::SqeError::Auth(format!(
                        "Polaris token response parse failed: {e}"
                    ))
                })?;

                source
                    .cache
                    .insert(key, token_resp.access_token.clone())
                    .await;
                Ok(token_resp.access_token)
            }
        }
    }
}

/// Build the SQE-managed catalog role name for a principal role.
/// Convention: `sqe_{principal_role_name}` to avoid collisions.
pub fn catalog_role_name(principal_role: &str) -> String {
    format!("sqe_{principal_role}")
}

// ── Polaris Management API request/response types ────────────────────────────

#[derive(Debug, Serialize)]
struct CreateCatalogRoleRequest {
    #[serde(rename = "catalogRole")]
    catalog_role: CatalogRoleName,
}

#[derive(Debug, Serialize, Deserialize)]
struct CatalogRoleName {
    name: String,
}

#[derive(Debug, Serialize)]
struct GrantPrivilegeRequest {
    grant: PolarisGrant,
}

#[derive(Debug, Serialize, Deserialize)]
struct PolarisGrant {
    #[serde(rename = "type")]
    resource_type: String,
    privilege: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "tableName")]
    table_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct AssignCatalogRoleRequest {
    #[serde(rename = "catalogRole")]
    catalog_role: CatalogRoleName,
}

#[derive(Debug, Deserialize)]
struct ListCatalogRolesResponse {
    #[serde(default)]
    roles: Vec<CatalogRoleName>,
}

#[derive(Debug, Deserialize)]
struct ListGrantsResponse {
    #[serde(default)]
    grants: Vec<PolarisGrant>,
}

#[derive(Debug, Deserialize)]
struct ListPrincipalRolesResponse {
    #[serde(default)]
    roles: Vec<CatalogRoleName>,
}

#[async_trait]
impl GrantBackend for PolarisGrantBackend {
    async fn grant(&self, token: &str, stmt: &GrantStatement) -> sqe_core::Result<()> {
        let token = self.resolve_token(token).await?;
        let catalog = stmt.catalog.as_deref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Polaris GRANT requires a catalog name (use catalog.namespace.table)".into(),
            )
        })?;

        let principal_role = match &stmt.grantee {
            Grantee::Role(name) | Grantee::Group(name) => name.clone(),
            Grantee::User(_) => {
                return Err(sqe_core::SqeError::NotImplemented(
                    "Polaris does not support USER-level grants. Use ROLE instead.".into(),
                ));
            }
        };

        let cr_name = catalog_role_name(&principal_role);
        let (polaris_priv, resource_type) = map_sql_to_polaris_privilege(&stmt.privilege);

        // Step 1: Ensure catalog role exists (409 = already exists, OK)
        let url = format!(
            "{}/catalogs/{}/catalog-roles",
            self.management_url, catalog
        );
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&CreateCatalogRoleRequest {
                catalog_role: CatalogRoleName {
                    name: cr_name.clone(),
                },
            })
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() && resp.status().as_u16() != 409 {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to create catalog role '{cr_name}' (HTTP {status}): {text}"
            )));
        }

        // Step 2: Grant privilege to catalog role
        let url = format!(
            "{}/catalogs/{}/catalog-roles/{}/grants",
            self.management_url, catalog, cr_name
        );
        let grant_body = GrantPrivilegeRequest {
            grant: PolarisGrant {
                resource_type: resource_type.to_string(),
                privilege: polaris_priv.to_string(),
                namespace: stmt.namespace.as_ref().map(|ns| vec![ns.clone()]),
                table_name: stmt.table.clone(),
            },
        };
        let resp = self
            .client
            .put(&url)
            .bearer_auth(&token)
            .json(&grant_body)
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to grant privilege to catalog role '{cr_name}' (HTTP {status}): {text}"
            )));
        }

        // Step 3: Assign catalog role to principal role
        let url = format!(
            "{}/principal-roles/{}/catalog-roles/{}",
            self.management_url, principal_role, catalog
        );
        let resp = self
            .client
            .put(&url)
            .bearer_auth(&token)
            .json(&AssignCatalogRoleRequest {
                catalog_role: CatalogRoleName {
                    name: cr_name.clone(),
                },
            })
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to assign catalog role '{cr_name}' to principal role '{principal_role}' (HTTP {status}): {text}"
            )));
        }

        debug!(
            catalog = catalog,
            principal_role = principal_role,
            catalog_role = cr_name,
            privilege = polaris_priv,
            "Polaris grant completed (3-step chain)"
        );

        Ok(())
    }

    async fn revoke(&self, token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let token = self.resolve_token(token).await?;
        let catalog = stmt.catalog.as_deref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Polaris REVOKE requires a catalog name (use catalog.namespace.table)".into(),
            )
        })?;

        let principal_role = match &stmt.grantee {
            Grantee::Role(name) | Grantee::Group(name) => name.clone(),
            Grantee::User(_) => {
                return Err(sqe_core::SqeError::NotImplemented(
                    "Polaris does not support USER-level grants. Use ROLE instead.".into(),
                ));
            }
        };

        let cr_name = catalog_role_name(&principal_role);
        let (polaris_priv, resource_type) = map_sql_to_polaris_privilege(&stmt.privilege);

        // Remove the specific privilege from the catalog role.
        // Does NOT delete the catalog role or unassign it.
        let url = format!(
            "{}/catalogs/{}/catalog-roles/{}/grants",
            self.management_url, catalog, cr_name
        );
        let resp = self
            .client
            .request(reqwest::Method::DELETE, &url)
            .bearer_auth(&token)
            .json(&GrantPrivilegeRequest {
                grant: PolarisGrant {
                    resource_type: resource_type.to_string(),
                    privilege: polaris_priv.to_string(),
                    namespace: stmt.namespace.as_ref().map(|ns| vec![ns.clone()]),
                    table_name: stmt.table.clone(),
                },
            })
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to revoke privilege from catalog role '{cr_name}' (HTTP {status}): {text}"
            )));
        }

        debug!(
            catalog = catalog,
            principal_role = principal_role,
            catalog_role = cr_name,
            privilege = polaris_priv,
            "Polaris revoke completed"
        );

        Ok(())
    }

    async fn show_grants(
        &self,
        token: &str,
        filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let token = self.resolve_token(token).await?;

        match filter {
            GrantFilter::OnResource {
                catalog,
                namespace,
                table,
            } => {
                let catalog = catalog.as_deref().ok_or_else(|| {
                    sqe_core::SqeError::Execution(
                        "Polaris SHOW GRANTS ON requires a catalog name".into(),
                    )
                })?;

                // List all catalog roles, then get grants for each
                let url = format!(
                    "{}/catalogs/{}/catalog-roles",
                    self.management_url, catalog
                );
                let resp = self
                    .client
                    .get(&url)
                    .bearer_auth(&token)
                    .send()
                    .await
                    .map_err(|e| {
                        sqe_core::SqeError::Execution(format!(
                            "Polaris management API request failed: {e}"
                        ))
                    })?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(sqe_core::SqeError::Execution(format!(
                        "Failed to list catalog roles (HTTP {status}): {text}"
                    )));
                }

                let roles: ListCatalogRolesResponse = resp.json().await.map_err(|e| {
                    sqe_core::SqeError::Execution(format!(
                        "Failed to parse catalog roles response: {e}"
                    ))
                })?;

                let mut entries = Vec::new();
                for role in &roles.roles {
                    let grants_url = format!(
                        "{}/catalogs/{}/catalog-roles/{}/grants",
                        self.management_url, catalog, role.name
                    );
                    let resp = self
                        .client
                        .get(&grants_url)
                        .bearer_auth(&token)
                        .send()
                        .await
                        .map_err(|e| {
                            sqe_core::SqeError::Execution(format!(
                                "Polaris management API request failed: {e}"
                            ))
                        })?;

                    if !resp.status().is_success() {
                        warn!(
                            catalog_role = role.name,
                            status = %resp.status(),
                            "Skipping catalog role grants (non-200)"
                        );
                        continue;
                    }

                    let grants: ListGrantsResponse =
                        resp.json().await.unwrap_or(ListGrantsResponse {
                            grants: Vec::new(),
                        });

                    for grant in &grants.grants {
                        // Filter by namespace/table if provided
                        if let Some(ns) = namespace {
                            if let Some(ref grant_ns) = grant.namespace {
                                if !grant_ns.contains(ns) {
                                    continue;
                                }
                            }
                        }
                        if let Some(tbl) = table {
                            if let Some(ref grant_tbl) = grant.table_name {
                                if grant_tbl != tbl {
                                    continue;
                                }
                            }
                        }

                        let resource = format_polaris_resource(
                            catalog,
                            grant.namespace.as_deref(),
                            grant.table_name.as_deref(),
                        );

                        entries.push(GrantEntry {
                            privilege: grant.privilege.clone(),
                            resource,
                            grantee_type: "CATALOG_ROLE".into(),
                            grantee_name: role.name.clone(),
                            effect: "ALLOW".into(),
                            granted_by: None,
                            granted_at: None,
                        });
                    }
                }

                Ok(entries)
            }
            GrantFilter::ToGrantee(grantee) => {
                // Find the SQE-managed catalog role for this grantee
                let role_name = grantee.name();
                let cr_name = catalog_role_name(role_name);

                // We need a catalog to query. Without one we can't list grants.
                // Return empty for now — the coordinator should provide catalog context.
                warn!(
                    principal_role = role_name,
                    catalog_role = cr_name,
                    "SHOW GRANTS TO ROLE requires catalog context for Polaris; returning empty"
                );
                Ok(vec![])
            }
        }
    }

    async fn show_effective(
        &self,
        token: &str,
        user: &str,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let token = self.resolve_token(token).await?;

        // Step 1: Get principal's principal-roles
        let url = format!(
            "{}/principals/{}/principal-roles",
            self.management_url, user
        );
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Polaris management API request failed: {e}"
                ))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            if status.as_u16() == 403 {
                return Err(sqe_core::SqeError::Auth(
                    "Insufficient privileges to manage grants".into(),
                ));
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::Execution(format!(
                "Failed to list principal roles for '{user}' (HTTP {status}): {text}"
            )));
        }

        let principal_roles: ListPrincipalRolesResponse =
            resp.json().await.map_err(|e| {
                sqe_core::SqeError::Execution(format!(
                    "Failed to parse principal roles response: {e}"
                ))
            })?;

        // Step 2+3: For each principal-role, get catalog-role assignments
        // and for each catalog-role get grants. Flatten into GrantEntry list.
        let mut entries = Vec::new();

        for pr in &principal_roles.roles {
            // We need to iterate catalogs to find catalog-role assignments.
            // For now, use the management API to list catalogs.
            let catalogs_url = format!("{}/catalogs", self.management_url);
            let resp = self
                .client
                .get(&catalogs_url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| {
                    sqe_core::SqeError::Execution(format!(
                        "Polaris management API request failed: {e}"
                    ))
                })?;

            if !resp.status().is_success() {
                continue;
            }

            #[derive(Deserialize)]
            struct CatalogList {
                #[serde(default)]
                catalogs: Vec<CatalogInfo>,
            }
            #[derive(Deserialize)]
            struct CatalogInfo {
                name: String,
            }

            let catalog_list: CatalogList =
                resp.json().await.unwrap_or(CatalogList { catalogs: vec![] });

            for cat in &catalog_list.catalogs {
                let assign_url = format!(
                    "{}/principal-roles/{}/catalog-roles/{}",
                    self.management_url, pr.name, cat.name
                );
                let resp = self
                    .client
                    .get(&assign_url)
                    .bearer_auth(&token)
                    .send()
                    .await;

                let resp = match resp {
                    Ok(r) if r.status().is_success() => r,
                    _ => continue,
                };

                let assigned: ListCatalogRolesResponse =
                    resp.json().await.unwrap_or(ListCatalogRolesResponse {
                        roles: vec![],
                    });

                for cr in &assigned.roles {
                    let grants_url = format!(
                        "{}/catalogs/{}/catalog-roles/{}/grants",
                        self.management_url, cat.name, cr.name
                    );
                    let resp = self
                        .client
                        .get(&grants_url)
                        .bearer_auth(&token)
                        .send()
                        .await;

                    let resp = match resp {
                        Ok(r) if r.status().is_success() => r,
                        _ => continue,
                    };

                    let grants: ListGrantsResponse =
                        resp.json().await.unwrap_or(ListGrantsResponse {
                            grants: vec![],
                        });

                    for grant in &grants.grants {
                        let resource = format_polaris_resource(
                            &cat.name,
                            grant.namespace.as_deref(),
                            grant.table_name.as_deref(),
                        );

                        entries.push(GrantEntry {
                            privilege: grant.privilege.clone(),
                            resource,
                            grantee_type: "PRINCIPAL_ROLE".into(),
                            grantee_name: pr.name.clone(),
                            effect: "ALLOW".into(),
                            granted_by: None,
                            granted_at: None,
                        });
                    }
                }
            }
        }

        Ok(entries)
    }

    async fn check_access(
        &self,
        token: &str,
        check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        // Walk the effective grants chain and check for a matching privilege
        let effective = self.show_effective(token, &check.user).await?;

        let (polaris_priv, _) = map_sql_to_polaris_privilege(&check.privilege);

        for entry in &effective {
            if entry.privilege == polaris_priv {
                // Check resource match
                let ns_vec = check.namespace.as_ref().map(|s| vec![s.clone()]);
                let target_resource = format_polaris_resource(
                    check.catalog.as_deref().unwrap_or(""),
                    ns_vec.as_deref(),
                    check.table.as_deref(),
                );
                if entry.resource == target_resource || check.catalog.is_none() {
                    return Ok(AccessCheckResult {
                        allowed: true,
                        reason: Some(format!(
                            "Granted via principal role '{}'",
                            entry.grantee_name
                        )),
                    });
                }
            }
        }

        Ok(AccessCheckResult {
            allowed: false,
            reason: Some(format!(
                "No matching grant for {} {} on {}",
                check.user,
                check.privilege,
                check.catalog.as_deref().unwrap_or("(no catalog)")
            )),
        })
    }

    fn backend_name(&self) -> &str {
        "polaris"
    }
}

/// Format a Polaris resource as a dotted string for GrantEntry.
fn format_polaris_resource(
    catalog: &str,
    namespace: Option<&[String]>,
    table: Option<&str>,
) -> String {
    let mut parts = vec![catalog.to_string()];
    if let Some(ns) = namespace {
        for n in ns {
            parts.push(n.clone());
        }
    }
    if let Some(t) = table {
        parts.push(t.to_string());
    }
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Existing tests (do not remove) ───────────────────────────────

    #[test]
    fn map_select_to_table_read_data() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("SELECT");
        assert_eq!(priv_name, "TABLE_READ_DATA");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_insert_to_table_write_data() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("INSERT");
        assert_eq!(priv_name, "TABLE_WRITE_DATA");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_all_to_catalog_manage_content() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("ALL");
        assert_eq!(priv_name, "CATALOG_MANAGE_CONTENT");
        assert_eq!(res_type, "catalog");
    }

    #[test]
    fn map_usage_to_namespace_list() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("USAGE");
        assert_eq!(priv_name, "NAMESPACE_LIST");
        assert_eq!(res_type, "namespace");
    }

    #[test]
    fn map_create_schema_to_namespace_create() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("CREATE SCHEMA");
        assert_eq!(priv_name, "NAMESPACE_CREATE");
        assert_eq!(res_type, "catalog");
    }

    #[test]
    fn passthrough_polaris_native_privilege() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("TABLE_WRITE_PROPERTIES");
        assert_eq!(priv_name, "TABLE_WRITE_PROPERTIES");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_is_case_insensitive() {
        let (priv_name, _) = map_sql_to_polaris_privilege("select");
        assert_eq!(priv_name.as_str(), "TABLE_READ_DATA");
    }

    // ── New Task 4 tests ─────────────────────────────────────────────

    #[test]
    fn catalog_role_name_adds_prefix() {
        assert_eq!(catalog_role_name("analysts"), "sqe_analysts");
        assert_eq!(catalog_role_name("data_eng"), "sqe_data_eng");
    }

    #[test]
    fn format_polaris_resource_full() {
        let result = format_polaris_resource(
            "warehouse",
            Some(&["ns".to_string()]),
            Some("orders"),
        );
        assert_eq!(result, "warehouse.ns.orders");
    }

    #[test]
    fn format_polaris_resource_catalog_only() {
        let result = format_polaris_resource("warehouse", None, None);
        assert_eq!(result, "warehouse");
    }

    #[test]
    fn format_polaris_resource_namespace_only() {
        let result = format_polaris_resource(
            "warehouse",
            Some(&["ns".to_string()]),
            None,
        );
        assert_eq!(result, "warehouse.ns");
    }

    #[test]
    fn constructor_passthrough_mode() {
        let backend = PolarisGrantBackend::new(
            "http://polaris:8181/api/management/v1",
            None,
            None,
        )
        .unwrap();
        assert!(backend.service_token.is_none());
        assert_eq!(backend.management_url, "http://polaris:8181/api/management/v1");
        assert_eq!(backend.backend_name(), "polaris");
    }

    #[test]
    fn constructor_service_credential_mode() {
        let backend = PolarisGrantBackend::new(
            "http://polaris:8181/api/management/v1/",
            Some("client-id".into()),
            Some("client-secret".into()),
        )
        .unwrap();
        assert!(backend.service_token.is_some());
        // URL should be trimmed
        assert_eq!(backend.management_url, "http://polaris:8181/api/management/v1");
        let source = backend.service_token.as_ref().unwrap();
        assert_eq!(source.token_url, "http://polaris:8181/api/catalog/v1/oauth/tokens");
        assert_eq!(source.client_id, "client-id");
    }

    #[tokio::test]
    async fn resolve_token_passthrough_returns_user_token() {
        let backend = PolarisGrantBackend::new(
            "http://polaris:8181/api/management/v1",
            None,
            None,
        )
        .unwrap();
        let token = backend.resolve_token("user-jwt-token").await.unwrap();
        assert_eq!(token, "user-jwt-token");
    }

    #[test]
    fn create_catalog_role_request_serializes() {
        let req = CreateCatalogRoleRequest {
            catalog_role: CatalogRoleName {
                name: "sqe_analysts".into(),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["catalogRole"]["name"], "sqe_analysts");
    }

    #[test]
    fn grant_privilege_request_serializes() {
        let req = GrantPrivilegeRequest {
            grant: PolarisGrant {
                resource_type: "table".into(),
                privilege: "TABLE_READ_DATA".into(),
                namespace: Some(vec!["ns".into()]),
                table_name: Some("orders".into()),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["grant"]["type"], "table");
        assert_eq!(json["grant"]["privilege"], "TABLE_READ_DATA");
        assert_eq!(json["grant"]["namespace"], serde_json::json!(["ns"]));
        assert_eq!(json["grant"]["tableName"], "orders");
    }

    #[test]
    fn grant_privilege_request_omits_none_fields() {
        let req = GrantPrivilegeRequest {
            grant: PolarisGrant {
                resource_type: "catalog".into(),
                privilege: "CATALOG_MANAGE_CONTENT".into(),
                namespace: None,
                table_name: None,
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert!(json["grant"].get("namespace").is_none());
        assert!(json["grant"].get("tableName").is_none());
    }

    #[test]
    fn assign_catalog_role_request_serializes() {
        let req = AssignCatalogRoleRequest {
            catalog_role: CatalogRoleName {
                name: "sqe_analysts".into(),
            },
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["catalogRole"]["name"], "sqe_analysts");
    }
}
