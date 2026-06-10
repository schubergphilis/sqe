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
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use moka::future::Cache;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use sqe_core::config::OpaConfig;
use sqe_core::SessionUser;
use sqe_metrics::MetricsRegistry;

use crate::{MaskType, PolicyStore, ResolvedPolicy};

const STATE_CLOSED: u32 = 0;
const STATE_OPEN: u32 = 1;
const STATE_HALF_OPEN: u32 = 2;

/// Lightweight three-state circuit breaker around the OPA call.
///
/// Mirrors `sqe_catalog::CircuitBreaker`. The OPA crate cannot depend
/// on sqe-catalog (the dependency direction is the other way around),
/// so the smaller implementation lives here.
struct OpaCircuitBreaker {
    failure_count: AtomicU32,
    failure_threshold: u32,
    recovery_timeout: Duration,
    last_failure_ms: AtomicU64,
    state: AtomicU32,
}

impl OpaCircuitBreaker {
    fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            failure_count: AtomicU32::new(0),
            failure_threshold,
            recovery_timeout,
            last_failure_ms: AtomicU64::new(0),
            state: AtomicU32::new(STATE_CLOSED),
        }
    }

    fn check(&self) -> Result<(), String> {
        let state = self.state.load(Ordering::Acquire);
        match state {
            STATE_CLOSED => Ok(()),
            STATE_OPEN => {
                let elapsed_ms = now_millis()
                    .saturating_sub(self.last_failure_ms.load(Ordering::Relaxed));
                if elapsed_ms >= self.recovery_timeout.as_millis() as u64
                    && self
                        .state
                        .compare_exchange(
                            STATE_OPEN,
                            STATE_HALF_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    info!("OPA circuit breaker moving to half_open (probe allowed)");
                    return Ok(());
                }
                Err("OPA circuit breaker is open".to_string())
            }
            STATE_HALF_OPEN => Ok(()),
            _ => Ok(()),
        }
    }

    fn record_success(&self) {
        if self.state.load(Ordering::Acquire) != STATE_CLOSED {
            self.state.store(STATE_CLOSED, Ordering::Release);
            self.failure_count.store(0, Ordering::Release);
            info!("OPA circuit breaker closed after successful probe");
        } else {
            self.failure_count.store(0, Ordering::Relaxed);
        }
    }

    fn record_failure(&self) {
        self.last_failure_ms.store(now_millis(), Ordering::Relaxed);
        let count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.failure_threshold
            && self
                .state
                .compare_exchange(
                    STATE_CLOSED,
                    STATE_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            warn!(
                failures = count,
                threshold = self.failure_threshold,
                "OPA circuit breaker opened"
            );
        } else if self.state.load(Ordering::Acquire) == STATE_HALF_OPEN {
            self.state.store(STATE_OPEN, Ordering::Release);
        }
    }

    fn state_code(&self) -> u8 {
        match self.state.load(Ordering::Relaxed) {
            STATE_OPEN => 2,
            STATE_HALF_OPEN => 1,
            _ => 0,
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// OPA policy store that evaluates policies via the OPA REST API.
pub struct OpaStore {
    client: Client,
    opa_url: String,
    policy_path: String,
    cache: Cache<String, ResolvedPolicy>,
    breaker: Arc<OpaCircuitBreaker>,
    metrics: Option<Arc<MetricsRegistry>>,
}

impl OpaStore {
    pub fn new(opa_url: &str, policy_path: &str, cache_ttl_secs: u64) -> Result<Self, reqwest::Error> {
        let cfg = OpaConfig {
            cache_ttl_secs,
            ..OpaConfig::default()
        };
        Self::with_config(opa_url, policy_path, &cfg)
    }

    /// Build an `OpaStore` from a typed `OpaConfig`. Use this in production;
    /// the legacy `new()` is kept for tests and existing call sites.
    pub fn with_config(
        opa_url: &str,
        policy_path: &str,
        cfg: &OpaConfig,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(cfg.timeout_secs))
                .build()?,
            opa_url: opa_url.trim_end_matches('/').to_string(),
            policy_path: policy_path.to_string(),
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cfg.cache_ttl_secs))
                .max_capacity(cfg.cache_max_entries)
                .build(),
            breaker: Arc::new(OpaCircuitBreaker::new(
                cfg.breaker_failure_threshold,
                Duration::from_secs(cfg.breaker_recovery_secs),
            )),
            metrics: None,
        })
    }

    /// Attach a metrics registry. Resolve latency and breaker state are
    /// recorded under `sqe_policy_*` series.
    #[must_use = "with_metrics consumes self; bind the returned store"]
    pub fn with_metrics(mut self, metrics: Arc<MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn record_metric(&self, started: Instant, status: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .policy_resolve_duration_seconds
                .with_label_values(&["opa", status])
                .observe(started.elapsed().as_secs_f64());
            metrics
                .policy_circuit_breaker_state
                .with_label_values(&["opa"])
                .set(self.breaker.state_code() as f64);
        }
    }

    fn record_cache_hit(&self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .policy_cache_hits_total
                .with_label_values(&["opa"])
                .inc();
        }
    }

    fn record_cache_miss(&self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .policy_cache_misses_total
                .with_label_values(&["opa"])
                .inc();
        }
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
            self.record_cache_hit();
            return Ok(cached);
        }
        self.record_cache_miss();

        // Fail closed when the breaker is open. The earlier code went
        // straight to the HTTP call and ate up to `timeout_secs` per
        // query when OPA was degraded.
        self.breaker.check().map_err(|e| {
            sqe_core::error::SqeError::Execution(format!(
                "OPA unavailable: {e}"
            ))
        })?;

        let started = Instant::now();
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
                self.breaker.record_failure();
                self.record_metric(started, "err");
                sqe_core::error::SqeError::Execution(format!("OPA request failed: {e}"))
            })?;

        if !response.status().is_success() {
            self.breaker.record_failure();
            self.record_metric(started, "err");
            return Err(sqe_core::error::SqeError::Execution(format!(
                "OPA returned status {}",
                response.status()
            )));
        }

        let opa_response: OpaResponse = response.json().await.map_err(|e| {
            self.breaker.record_failure();
            self.record_metric(started, "err");
            sqe_core::error::SqeError::Execution(format!("Failed to parse OPA response: {e}"))
        })?;
        self.breaker.record_success();
        self.record_metric(started, "ok");

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

    /// Flush every cached OPA decision. Called after a GRANT/REVOKE so a
    /// tightened policy or revoked grant takes effect on the next query
    /// instead of lingering until the cache TTL expires (issue #207).
    /// moka's `invalidate_all()` is synchronous: it marks all current
    /// entries stale, so subsequent `resolve()` calls miss the cache and
    /// re-query OPA.
    fn invalidate_all(&self) {
        self.cache.invalidate_all();
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
