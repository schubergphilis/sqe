//! OPA (Open Policy Agent) backend for the PolicyStore trait.
//!
//! Evaluates Rego policies by calling OPA's REST API with the user identity
//! and table context. Returns a ResolvedPolicy with row filters, column masks,
//! and column restrictions.
//!
//! OPA query format:
//!   POST /v1/data/sqe/policy/evaluate
//!   {
//!     "input": {
//!       "user": { "username": "alice", "roles": ["analyst"] },
//!       "table": { "name": "employees", "namespace": "hr" }
//!     }
//!   }
//!
//! Expected response:
//!   {
//!     "result": {
//!       "allow": true,
//!       "row_filters": ["clearance >= 3"],
//!       "column_masks": { "ssn": "hash", "salary": "redact:***" },
//!       "restricted_columns": ["internal_notes"]
//!     }
//!   }

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use sqe_core::SessionUser;

use crate::{MaskType, PolicyStore, ResolvedPolicy};

/// OPA policy store that evaluates policies via the OPA REST API.
pub struct OpaStore {
    client: Client,
    opa_url: String,
    policy_path: String,
    cache: Cache<String, ResolvedPolicy>,
}

impl OpaStore {
    pub fn new(opa_url: &str, policy_path: &str, cache_ttl_secs: u64) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?,
            opa_url: opa_url.trim_end_matches('/').to_string(),
            policy_path: policy_path.to_string(),
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cache_ttl_secs))
                .max_capacity(10_000)
                .build(),
        })
    }

    fn cache_key(user: &SessionUser, table: &str, namespace: &str) -> String {
        {
        let mut roles_sorted = user.roles.clone();
        roles_sorted.sort();
        let roles_hash = {
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(roles_sorted.join(",").as_bytes());
            format!("{:x}", digest).chars().take(16).collect::<String>()
        };
        format!("{}:{}:{}:{}", user.username, namespace, table, roles_hash)
    }
    }
}

#[derive(Serialize)]
struct OpaRequest {
    input: OpaInput,
}

#[derive(Serialize)]
struct OpaInput {
    user: OpaUser,
    table: OpaTable,
}

#[derive(Serialize)]
struct OpaUser {
    username: String,
    roles: Vec<String>,
}

#[derive(Serialize)]
struct OpaTable {
    name: String,
    namespace: String,
}

#[derive(Deserialize, Default)]
struct OpaResponse {
    result: Option<OpaResult>,
}

#[derive(Deserialize, Default)]
struct OpaResult {
    #[serde(default)]
    allow: bool,
    #[serde(default)]
    row_filters: Vec<String>,
    #[serde(default)]
    column_masks: HashMap<String, String>,
    #[serde(default)]
    restricted_columns: Vec<String>,
}

#[async_trait]
impl PolicyStore for OpaStore {
    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        let key = Self::cache_key(user, table_name, namespace);

        // Check cache first
        if let Some(cached) = self.cache.get(&key).await {
            debug!(user = %user.username, table = %table_name, "Policy cache hit");
            return Ok(cached);
        }

        let url = format!("{}/v1/data/{}", self.opa_url, self.policy_path);
        let request = OpaRequest {
            input: OpaInput {
                user: OpaUser {
                    username: user.username.clone(),
                    roles: user.roles.clone(),
                },
                table: OpaTable {
                    name: table_name.to_string(),
                    namespace: namespace.to_string(),
                },
            },
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                sqe_core::error::SqeError::Execution(format!("OPA request failed: {e}"))
            })?;

        if !response.status().is_success() {
            return Err(sqe_core::error::SqeError::Execution(format!(
                "OPA returned status {}",
                response.status()
            )));
        }

        let opa_response: OpaResponse = response.json().await.map_err(|e| {
            sqe_core::error::SqeError::Execution(format!("Failed to parse OPA response: {e}"))
        })?;

        // Fail-closed when OPA returns `{ "result": null }` — typical when the
        // queried policy package or rule does not exist (typo in path, mis-
        // deployed bundle, partial reload). Without this guard a degraded OPA
        // silently lifts every row filter and column mask.
        let result = opa_response.result.ok_or_else(|| {
            sqe_core::error::SqeError::Execution(format!(
                "OPA policy package missing for query path: {}",
                self.policy_path
            ))
        })?;

        if !result.allow {
            // Denied — inject FALSE filter (returns zero rows, no error)
            let policy = ResolvedPolicy {
                row_filters: vec![datafusion::logical_expr::lit(false)],
                ..Default::default()
            };
            self.cache.insert(key, policy.clone()).await;
            return Ok(policy);
        }

        // Parse row filters from string expressions.
        // Fail closed: if any filter the policy returned cannot be parsed the resolve
        // errors out, so the planner does not silently run with weaker policy than OPA
        // intended. The supported shape is documented at the parse_filter_expr docstring.
        let mut row_filters: Vec<datafusion::logical_expr::Expr> =
            Vec::with_capacity(result.row_filters.len());
        for filter_str in &result.row_filters {
            match parse_filter_expr(filter_str) {
                Some(expr) => row_filters.push(expr),
                None => {
                    warn!(
                        filter = %filter_str,
                        "Unparseable OPA row filter; rejecting policy (fail-closed)"
                    );
                    return Err(sqe_core::error::SqeError::Execution(format!(
                        "OPA returned an unsupported row filter expression: '{}'. \
                         Only single comparisons (col <op> literal) with =, !=, >, <, >=, <= are accepted.",
                        filter_str
                    )));
                }
            }
        }

        // Parse column masks
        let column_masks: HashMap<String, MaskType> = result
            .column_masks
            .into_iter()
            .map(|(col, mask_str)| {
                let mask = parse_mask_type(&mask_str);
                (col, mask)
            })
            .collect();

        let policy = ResolvedPolicy {
            row_filters,
            column_masks,
            restricted_columns: result.restricted_columns,
        };

        self.cache.insert(key, policy.clone()).await;
        Ok(policy)
    }
}

/// Parse a simple filter expression string (e.g., "clearance >= 3").
/// Supports basic comparison operators: =, !=, >, <, >=, <=.
fn parse_filter_expr(expr_str: &str) -> Option<datafusion::logical_expr::Expr> {
    use datafusion::logical_expr::{col, lit, Expr, Operator};

    let operators = [">=", "<=", "!=", ">", "<", "="];
    for op_str in &operators {
        if let Some(pos) = expr_str.find(op_str) {
            let left = expr_str[..pos].trim();
            let right = expr_str[pos + op_str.len()..].trim();

            let op = match *op_str {
                ">=" => Operator::GtEq,
                "<=" => Operator::LtEq,
                "!=" => Operator::NotEq,
                ">" => Operator::Gt,
                "<" => Operator::Lt,
                "=" => Operator::Eq,
                _ => return None,
            };

            let right_expr = if let Ok(n) = right.parse::<i64>() {
                lit(n)
            } else if let Ok(f) = right.parse::<f64>() {
                lit(f)
            } else {
                lit(right.trim_matches('\''))
            };

            return Some(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                left: Box::new(col(left)),
                op,
                right: Box::new(right_expr),
            }));
        }
    }
    None
}

/// Parse a mask type string from OPA (e.g., "hash", "redact:***", "null").
fn parse_mask_type(mask_str: &str) -> MaskType {
    if mask_str == "hash" {
        MaskType::Hash
    } else if mask_str == "null" || mask_str == "nullify" {
        MaskType::Nullify
    } else if let Some(value) = mask_str.strip_prefix("redact:") {
        MaskType::Redact(value.to_string())
    } else {
        // Default: redact with the raw string
        MaskType::Redact(mask_str.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filter_expr() {
        let expr = parse_filter_expr("clearance >= 3").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_filter_expr_string() {
        let expr = parse_filter_expr("department = 'engineering'").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_mask_type_hash() {
        assert!(matches!(parse_mask_type("hash"), MaskType::Hash));
    }

    #[test]
    fn test_parse_mask_type_redact() {
        assert!(matches!(parse_mask_type("redact:***"), MaskType::Redact(_)));
    }

    #[test]
    fn test_parse_mask_type_null() {
        assert!(matches!(parse_mask_type("null"), MaskType::Nullify));
    }

    #[test]
    fn test_parse_mask_type_nullify_alias() {
        // Both "null" and "nullify" map to MaskType::Nullify
        assert!(matches!(parse_mask_type("nullify"), MaskType::Nullify));
    }

    #[test]
    fn test_parse_mask_type_unknown_defaults_to_redact() {
        // Unrecognised mask strings should be treated as a literal redact value
        let mask = parse_mask_type("CUSTOM_VALUE");
        assert!(matches!(mask, MaskType::Redact(_)));
        if let MaskType::Redact(val) = mask {
            assert_eq!(val, "CUSTOM_VALUE");
        }
    }

    #[test]
    fn test_parse_filter_expr_eq() {
        let expr = parse_filter_expr("status = 'active'").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_filter_expr_not_eq() {
        let expr = parse_filter_expr("deleted != 1").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_filter_expr_lt() {
        let expr = parse_filter_expr("risk_score < 5").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_filter_expr_lte() {
        let expr = parse_filter_expr("tier <= 3").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_filter_expr_gt() {
        let expr = parse_filter_expr("age > 18").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_filter_expr_float_literal() {
        let expr = parse_filter_expr("score >= 0.5").unwrap();
        assert!(matches!(expr, datafusion::logical_expr::Expr::BinaryExpr(_)));
    }

    #[test]
    fn test_parse_filter_expr_no_operator_returns_none() {
        // A string without a recognised operator should return None
        let result = parse_filter_expr("just_a_column_name");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_mask_type_redact_with_custom_string() {
        let mask = parse_mask_type("redact:[HIDDEN]");
        if let MaskType::Redact(val) = mask {
            assert_eq!(val, "[HIDDEN]");
        } else {
            panic!("Expected Redact variant");
        }
    }

    // --- Fail-closed regression tests (issue #5) ---

    #[test]
    fn opa_result_allow_defaults_to_false_when_field_missing() {
        // Regression: prior `default_true` made a missing `allow` field
        // silently permit. Default must be false (deny) so a degraded OPA
        // cannot lift restrictions by omission.
        let result: OpaResult = serde_json::from_str("{}").unwrap();
        assert!(!result.allow, "allow must default to false (fail-closed)");
    }

    #[test]
    fn opa_response_with_null_result_yields_none() {
        // OPA returns `{ "result": null }` when the queried policy package or
        // rule does not exist. The resolver must NOT treat this as
        // "permit everything" — it must surface an error so operators notice
        // a missing bundle / typo in policy_path.
        let response: OpaResponse =
            serde_json::from_str(r#"{"result": null}"#).unwrap();
        assert!(response.result.is_none());

        // And the absent-field form behaves the same way.
        let response: OpaResponse = serde_json::from_str("{}").unwrap();
        assert!(response.result.is_none());
    }

}
