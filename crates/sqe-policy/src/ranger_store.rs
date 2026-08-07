//! Apache Ranger fine-grained PolicyStore. Reads row-filter (policyType 2) and
//! data-mask (policyType 1) policies from a `hive`-type Ranger service and
//! returns a `ResolvedPolicy` for the PlanRewriter. Shares the policy set with
//! Apache Spark / Kyuubi. See docs/ranger-fine-grained-service-type.md.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::logical_expr::{lit, Expr};
use moka::future::Cache;
use reqwest::Client;
use serde::Deserialize;
use sqe_core::config::RangerPolicyConfig;
use sqe_core::{SecretString, SessionUser};
use sqe_metrics::MetricsRegistry;
use tracing::{debug, warn};

use crate::policy_breaker::PolicyCircuitBreaker;
use crate::policy_expr::parse_sql_predicate;
use crate::session_udf::SessionIdentity;
use crate::{MaskType, PolicyStore, ResolvedPolicy, TagMaskSpec};

// --- Ranger policy bundle model (ServicePolicies) ---

/// Nested tag-service policy bundle. Present when Ranger has at least one
/// tag-based policy. Structure mirrors the top-level `ServicePolicies` but
/// with `tag` resources instead of database/table/column.
///
/// Shape VERIFIED against a live Ranger 2.8 bundle, captured by
/// `access_control_e2e::capture_live_tag_bundle` into
/// `src/testdata/tag_bundle_live_sample.json` and asserted by
/// `resolve_tag_policies_against_live_sample`. The capture also showed that live
/// tag policies name their mask types with the owning component's prefix
/// (`hive:MASK_SHOW_LAST_4`), which `normalize_mask_type` handles.
#[derive(Debug, Deserialize, Default)]
pub(crate) struct TagPolicies {
    /// Same `RangerPolicy` type as resource policies; `resources` map carries
    /// a `tag` key with the tag values (e.g. `["PII"]`).
    #[serde(default)]
    pub(crate) policies: Vec<RangerPolicy>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ServicePolicies {
    #[serde(rename = "policyVersion", default)]
    #[allow(dead_code)] // read only in #[cfg(test)]; used by serde and test assertions
    pub(crate) policy_version: Option<i64>,
    #[serde(default)]
    pub(crate) policies: Vec<RangerPolicy>,
    /// Nested tag-service policies. Present when the Ranger bundle includes
    /// tag-based policies. Absent in pure-resource bundles (default = None).
    #[serde(rename = "tagPolicies", default)]
    pub(crate) tag_policies: Option<TagPolicies>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RangerPolicy {
    #[serde(default)]
    #[allow(dead_code)] // read only in #[cfg(test)]; present in Ranger JSON for traceability
    pub(crate) id: i64,
    /// The policy's name in Ranger. Not used for enforcement (matching is on
    /// resources and grantees), but it is how an operator refers to a policy in the
    /// console, so `SHOW MASKING POLICIES` surfaces it.
    #[serde(default)]
    pub(crate) name: String,
    /// 0 = access, 1 = DATAMASK, 2 = ROWFILTER.
    #[serde(rename = "policyType", default)]
    pub(crate) policy_type: i32,
    #[serde(rename = "isEnabled", default)]
    pub(crate) is_enabled: bool,
    /// Resource map: keys are "database", "table", "column".
    #[serde(default)]
    pub(crate) resources: HashMap<String, RangerResource>,
    #[serde(rename = "dataMaskPolicyItems", default)]
    pub(crate) data_mask_policy_items: Vec<DataMaskPolicyItem>,
    #[serde(rename = "rowFilterPolicyItems", default)]
    pub(crate) row_filter_policy_items: Vec<RowFilterPolicyItem>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RangerResource {
    #[serde(default)]
    pub(crate) values: Vec<String>,
    #[serde(rename = "isExcludes", default)]
    pub(crate) is_excludes: bool,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct DataMaskPolicyItem {
    #[serde(default)]
    pub(crate) users: Vec<String>,
    #[serde(default)]
    pub(crate) roles: Vec<String>,
    // groups-based binding is NOT enforced (SQE matches token roles only); see Phase 2.
    #[serde(default)]
    pub(crate) groups: Vec<String>,
    #[serde(rename = "dataMaskInfo", default)]
    pub(crate) data_mask_info: DataMaskInfo,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct DataMaskInfo {
    #[serde(rename = "dataMaskType", default)]
    pub(crate) data_mask_type: String,
    #[serde(rename = "valueExpr", default)]
    pub(crate) value_expr: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RowFilterPolicyItem {
    #[serde(default)]
    pub(crate) users: Vec<String>,
    #[serde(default)]
    pub(crate) roles: Vec<String>,
    // groups-based binding is NOT enforced (SQE matches token roles only); see Phase 2.
    #[serde(default)]
    pub(crate) groups: Vec<String>,
    #[serde(rename = "rowFilterInfo", default)]
    pub(crate) row_filter_info: RowFilterInfo,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RowFilterInfo {
    #[serde(rename = "filterExpr", default)]
    pub(crate) filter_expr: Option<String>,
}

// --- RangerStore struct, constructor, and download fetch ---

/// PolicyType discriminators in the Ranger bundle JSON (`policyType` field).
/// 0 = access (not enforced here), 1 = DATAMASK, 2 = ROWFILTER.
const POLICY_TYPE_DATAMASK: i32 = 1;
const POLICY_TYPE_ROWFILTER: i32 = 2;

/// Ranger `dataMaskType` discriminator for an operator-authored masking
/// expression. Shared by `map_mask` and `resolve_tag_policies` so the resource
/// path and the tag path agree on the CUSTOM special-case.
const MASK_TYPE_CUSTOM: &str = "CUSTOM";

/// Single, constant key for the user-independent ServicePolicies bundle cache.
const BUNDLE_KEY: &str = "__bundle__";

/// Fine-grained policy store backed by a `hive`-type Ranger service.
pub struct RangerStore {
    client: Client,
    /// Base download URL, e.g. ".../service/plugins/policies/download/hive".
    download_url: String,
    admin_user: String,
    admin_password: SecretString,
    cache: Cache<String, ResolvedPolicy>,
    /// Cache of the raw, user-independent ServicePolicies bundle under a single
    /// `BUNDLE_KEY`. The bundle is the same for every user, so caching it here
    /// (with the same TTL as the per-user `cache`) avoids re-downloading and
    /// re-parsing it on every query and every tagged table. `Arc` keeps the
    /// moka `get`/`insert` clones cheap.
    bundle_cache: Cache<&'static str, Arc<ServicePolicies>>,
    breaker: Arc<PolicyCircuitBreaker>,
    metrics: Option<Arc<MetricsRegistry>>,
}

impl RangerStore {
    pub fn from_config(cfg: &RangerPolicyConfig) -> sqe_core::Result<Self> {
        let base = cfg.url.trim_end_matches('/');
        let download_url = format!(
            "{base}/service/plugins/policies/download/{}",
            cfg.service_name
        );
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(cfg.timeout_secs))
                .danger_accept_invalid_certs(cfg.accept_invalid_certs)
                .build()
                .map_err(|e| {
                    sqe_core::error::SqeError::Config(format!(
                        "Failed to build Ranger HTTP client: {e}"
                    ))
                })?,
            download_url,
            admin_user: cfg.admin_user.clone(),
            admin_password: cfg.admin_password.clone(),
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cfg.cache_ttl_secs))
                .max_capacity(cfg.cache_max_entries)
                .build(),
            bundle_cache: Cache::builder()
                .time_to_live(Duration::from_secs(cfg.cache_ttl_secs))
                .max_capacity(1)
                .build(),
            breaker: Arc::new(PolicyCircuitBreaker::new(
                "Ranger",
                cfg.breaker_failure_threshold,
                Duration::from_secs(cfg.breaker_recovery_secs),
            )),
            metrics: None,
        })
    }

    /// Attach a metrics registry. Mirrors `OpaStore::with_metrics`: resolve
    /// latency, cache hit/miss, and circuit-breaker state are recorded under the
    /// shared `sqe_policy_*` series, all labelled `backend="ranger"`.
    #[must_use = "with_metrics consumes self; bind the returned store"]
    pub fn with_metrics(mut self, metrics: Arc<MetricsRegistry>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Record fetch latency + current breaker state. Mirrors
    /// `OpaStore::record_metric`. `status` is `"ok"` or `"err"`; the breaker
    /// state gauge always reflects the current `state_code()` so a breaker-open
    /// deny-all is visible even when every query fails closed.
    fn record_metric(&self, started: Instant, status: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .policy_resolve_duration_seconds
                .with_label_values(&["ranger", status])
                .observe(started.elapsed().as_secs_f64());
            metrics
                .policy_circuit_breaker_state
                .with_label_values(&["ranger"])
                .set(self.breaker.state_code() as f64);
        }
    }

    fn record_cache_hit(&self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .policy_cache_hits_total
                .with_label_values(&["ranger"])
                .inc();
        }
    }

    fn record_cache_miss(&self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .policy_cache_misses_total
                .with_label_values(&["ranger"])
                .inc();
        }
    }

    /// Fetch the full policy bundle. Fail-closed: any transport/parse error
    /// trips the breaker and returns Err so the caller denies.
    // TODO(phase2): lastKnownVersion + HTTP 304 incremental refresh.
    async fn fetch_bundle(&self) -> sqe_core::Result<ServicePolicies> {
        // Breaker-open path: mirror OpaStore, which records NOTHING here. The
        // call that trips the breaker (a failed fetch below) already sets the
        // state gauge via record_metric, so the gauge reads `open` without a
        // recording point here. Adding one would push a 0-second sample into the
        // resolve-duration histogram on every query during an outage, skewing
        // p50/p99 toward zero exactly when the metric is being read.
        self.breaker.check().map_err(|e| {
            sqe_core::error::SqeError::Execution(format!("Ranger unavailable: {e}"))
        })?;

        let started = Instant::now();
        let resp = self
            .client
            .get(&self.download_url)
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .send()
            .await
            .map_err(|e| {
                self.breaker.record_failure();
                self.record_metric(started, "err");
                sqe_core::error::SqeError::Execution(format!("Ranger download failed: {e}"))
            })?;

        if !resp.status().is_success() {
            self.breaker.record_failure();
            self.record_metric(started, "err");
            return Err(sqe_core::error::SqeError::Execution(format!(
                "Ranger download returned status {}",
                resp.status()
            )));
        }

        let bundle: ServicePolicies = resp.json().await.map_err(|e| {
            self.breaker.record_failure();
            self.record_metric(started, "err");
            sqe_core::error::SqeError::Execution(format!("Failed to parse Ranger bundle: {e}"))
        })?;
        self.breaker.record_success();
        self.record_metric(started, "ok");
        Ok(bundle)
    }

    /// Return the ServicePolicies bundle, served from the bundle cache when
    /// fresh. On a cache miss, downloads + parses via `fetch_bundle` and stores
    /// the result. The bundle is user-independent, so a single cached copy is
    /// shared across every `resolve` / `resolve_tags` call within the TTL.
    ///
    /// Fail-closed is preserved: a fetch error propagates (`?`) and is NEVER
    /// cached, so the next caller retries (subject to the circuit breaker).
    async fn cached_bundle(&self) -> sqe_core::Result<Arc<ServicePolicies>> {
        if let Some(bundle) = self.bundle_cache.get(BUNDLE_KEY).await {
            return Ok(bundle);
        }
        let bundle = Arc::new(self.fetch_bundle().await?);
        self.bundle_cache.insert(BUNDLE_KEY, bundle.clone()).await;
        Ok(bundle)
    }
}

// --- Pure resolution helpers ---

/// Flatten an Iceberg namespace to a hive `database` name. SQE namespaces are
/// already dotted multi-level strings and Kyuubi uses the same dotted
/// convention, so this is identity for now. Catalog is intentionally dropped
/// (hive has no catalog level); cross-engine policies must be written without a
/// catalog prefix. See docs/ranger-fine-grained-service-type.md.
///
/// NOTE: `plan_rewriter.rs::resolve_policy_key` passes the **last** dotted
/// component of the schema as `namespace` (e.g. schema `"sales_wh.sales"` ->
/// `"sales"`). Ranger `database` resource values must match that last component
/// for policies to fire. See project tracking for the namespace convention
/// alignment task.
fn hive_database(namespace: &str) -> String {
    namespace.to_string()
}

/// Ranger wildcard match: `*` matches any run of characters (including none),
/// `?` matches exactly one.
///
/// Semantics taken from the live `hive` service definition, whose `database`,
/// `table` and `column` resources all declare
/// `matcherOptions: {wildCard: "true", ignoreCase: "true"}`. Ranger implements
/// that with commons-io `FilenameUtils.wildcardMatch` and `IOCase.INSENSITIVE`,
/// where no character other than `*` and `?` is special.
///
/// Case folding is part of the same contract, not a separate liberty: with
/// `ignoreCase: true`, a policy written on `Orders` fires for `orders` in Ranger,
/// and SQE comparing case-sensitively would silently not mask.
///
/// Iterative with a single backtrack point, so a pattern like `*a*a*a*a*` cannot
/// blow up: policies are operator input reaching a per-query path.
fn wildcard_match(pattern: &str, target: &str) -> bool {
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let t: Vec<char> = target.to_lowercase().chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut resume = 0usize;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            pi += 1;
            resume = ti;
        } else if let Some(s) = star {
            // Backtrack: let the last `*` swallow one more character.
            pi = s + 1;
            resume += 1;
            ti = resume;
        } else {
            return false;
        }
    }
    // Trailing `*`s may match nothing.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// True if a Ranger resource value list matches `target`.
///
/// Wildcards and case-insensitivity per the service definition (see
/// `wildcard_match`); `isExcludes` inverts the result. An empty values list
/// matches nothing.
///
/// Previously only exact match and a bare `*` were honoured, so a policy written
/// `orders*` or `*_pii` silently never fired. For a masking or row-filtering
/// policy that means the protection is simply ABSENT while the console shows it
/// as configured, which is the worst failure mode a governance tool has.
fn resource_matches(res: &RangerResource, target: &str) -> bool {
    let hit = res.values.iter().any(|v| wildcard_match(v, target));
    hit ^ res.is_excludes
}

/// True if a policy's database + table resources match the target table.
///
/// `database` is the full dotted Iceberg namespace. A policy naming only its LAST
/// component also matches, and says so in the log.
///
/// That fallback is a migration path, not the intended convention.
/// `resolve_policy_key` used to pass only the last component, so `sales` and
/// `a.b.sales` collided on the Ranger database `sales` and a policy written for one
/// fired on the other. Now that the full namespace is passed, a policy authored
/// against the old key would stop matching -- and for a mask or row filter, a
/// policy that stops matching is protection silently disappearing. Over-matching an
/// old policy is the safe direction while operators migrate; under-matching is a
/// leak.
fn policy_matches_table(p: &RangerPolicy, database: &str, table: &str) -> bool {
    let db_ok = p
        .resources
        .get("database")
        .map(|r| {
            if resource_matches(r, database) {
                return true;
            }
            let last = database.rsplit('.').next().unwrap_or(database);
            if last != database && resource_matches(r, last) {
                warn!(
                    policy_id = p.id,
                    namespace = %database,
                    matched_as = %last,
                    "Ranger policy matched only by the LAST namespace component. This is a \
                     compatibility path: rewrite the policy's `database` value as the full \
                     dotted namespace, which is also what Kyuubi uses. Two namespaces sharing \
                     a last component are indistinguishable to a policy written this way."
                );
                return true;
            }
            false
        })
        .unwrap_or(false);
    let tbl_ok = p
        .resources
        .get("table")
        .map(|r| resource_matches(r, table))
        .unwrap_or(false);
    db_ok && tbl_ok
}

/// True if a policy-item applies to this user/roles (token roles, matched directly).
///
/// `groups` is accepted but NOT enforced (SQE has no group info; token roles
/// only, by design — Phase 2). A policy item bound ONLY via `groups` is skipped
/// with a warning so operators see the gap instead of a silent drop.
fn item_matches(
    users: &[String],
    roles: &[String],
    groups: &[String],
    user: &SessionUser,
) -> bool {
    let matched = users.iter().any(|u| u == &user.username)
        || roles.iter().any(|r| user.roles.contains(r))
        || groups.iter().any(|g| user.groups.contains(g));
    if !matched && !groups.is_empty() && user.groups.is_empty() {
        // The item is group-bound and the session carries NO groups, which in
        // practice means `[[auth.providers]] groups_claim` is unset so the token's
        // group memberships were never read. That is the one case where a
        // group-bound Ranger policy still cannot apply, and it is a configuration
        // gap rather than a design limit, so say which knob fixes it.
        //
        // debug! not warn!: this runs per item, per user, on every cache miss, so
        // a bundle with many group-bound items would produce a WARN burst.
        debug!(
            ?groups,
            user = %user.username,
            "Ranger policy item is group-bound but the session carries no groups; \
             set `groups_claim` on the auth provider so group bindings resolve"
        );
    }
    matched
}

/// Normalize a Ranger data-mask type name.
///
/// TAG-service policies qualify the mask type with the owning component
/// (`hive:MASK_SHOW_LAST_4`), because the `tag` servicedef aggregates the mask
/// types of every component it can decorate (hive, trino, presto,
/// nestedstructure). Resource-service policies use the bare name
/// (`MASK_SHOW_LAST_4`). Verified against a live Ranger 2.8:
/// `GET /service/public/v2/api/servicedef/name/tag` lists only prefixed forms.
///
/// SQE downloads a hive-type service, so it accepts the bare form and the
/// `hive:` form. Another component's prefix is deliberately left unmatched:
/// a `trino:`-scoped mask was authored for a different engine, and leaving it
/// unmatched makes the caller fail closed (the tagged column is restricted)
/// rather than silently applying another engine's policy.
fn normalize_mask_type(mask_type: &str) -> &str {
    match mask_type.split_once(':') {
        Some(("hive", rest)) => rest,
        Some(_) => mask_type,
        None => mask_type,
    }
}

/// Map a Ranger hive data-mask type to an SQE `MaskType`.
///  - `Ok(Some(mask))` supported,
///  - `Ok(None)` for MASK_NONE (explicit exemption: no mask, not restricted),
///  - `Err(())` for not-yet-supported types (caller restricts the column, fail-closed).
fn map_mask(info: &DataMaskInfo, column: &str, identity: &SessionIdentity) -> Result<Option<MaskType>, ()> {
    match normalize_mask_type(info.data_mask_type.as_str()) {
        "MASK_NULL" => Ok(Some(MaskType::Nullify)),
        "MASK_NONE" => Ok(None),
        "MASK_HASH" => Ok(Some(MaskType::Hash)),
        MASK_TYPE_CUSTOM => {
            let expr_str = info.value_expr.as_deref().ok_or(())?;
            // Ranger CUSTOM masks use `{col}` as the column placeholder.
            // Substitute with the real column name so the parsed Expr references
            // the actual column. The rewriter splices the Expr as-is via
            // `MaskType::Custom(expr) => expr.clone()` (plan_rewriter.rs:323),
            // so the column name must be correct at parse time.
            // If parsing fails -> Err(()) -> column restricted (fail-closed).
            let substituted = expr_str.replace("{col}", column);
            parse_sql_predicate(&substituted, identity)
                .map(|e| Some(MaskType::Custom(e)))
                .map_err(|_| ())
        }
        "MASK" => Ok(Some(MaskType::PartialMask {
            show_first: 0,
            show_last: 0,
            upper: 'X',
            lower: 'x',
            digit: 'n',
        })),
        "MASK_SHOW_LAST_4" => Ok(Some(MaskType::PartialMask {
            show_first: 0,
            show_last: 4,
            upper: 'x',
            lower: 'x',
            digit: 'x',
        })),
        "MASK_SHOW_FIRST_4" => Ok(Some(MaskType::PartialMask {
            show_first: 4,
            show_last: 0,
            upper: 'x',
            lower: 'x',
            digit: 'x',
        })),
        "MASK_DATE_SHOW_YEAR" => Ok(Some(MaskType::DateShowYear)),
        // Genuinely unknown / unsupported types still fail closed (restrict).
        _ => Err(()),
    }
}

/// Build a `ResolvedPolicy` from an already-fetched bundle. Pure (no I/O), so it
/// is unit-tested directly and reused by `resolve()` after a cache miss.
fn resolve_from_bundle(
    bundle: &ServicePolicies,
    user: &SessionUser,
    table: &str,
    namespace: &str,
) -> ResolvedPolicy {
    let database = hive_database(namespace);
    let mut policy = ResolvedPolicy::default();

    // Build the identity once for the whole resolution pass. database/schema
    // are None here -- RangerStore doesn't hold the session warehouse; UDFs
    // referencing current_database()/current_schema() fold to NULL (MVP).
    let identity = SessionIdentity {
        username: user.username.clone(),
        roles: user.roles.clone(),
        database: None,
        schema: None,
    };

    for p in &bundle.policies {
        if !p.is_enabled || !policy_matches_table(p, &database, table) {
            continue;
        }

        // Data-mask policy (policyType 1). A datamask policy's `column`
        // resource can list several columns that all receive the same mask;
        // iterate ALL of them so multi-column policies don't leak.
        if p.policy_type == POLICY_TYPE_DATAMASK {
            let Some(col_res) = p.resources.get("column") else { continue };
            if col_res.is_excludes {
                // "mask all columns EXCEPT these" cannot be honored on the
                // resource path: the column complement needs the table schema,
                // which is not available here. Treating excludes as includes
                // (the previous behavior) left every intended-masked column raw.
                // Fail closed: deny the table.
                warn!(
                    table = %table,
                    policy_id = p.id,
                    "datamask policy uses column isExcludes (unsupported); denying table (fail-closed)"
                );
                policy.row_filters.push(lit(false));
                continue;
            }
            for column in &col_res.values {
                for item in &p.data_mask_policy_items {
                    if !item_matches(&item.users, &item.roles, &item.groups, user) {
                        continue;
                    }
                    match map_mask(&item.data_mask_info, column, &identity) {
                        Ok(Some(mask)) => {
                            policy.column_masks.insert(column.clone(), mask);
                        }
                        Ok(None) => { /* MASK_NONE exemption: leave column visible */ }
                        Err(()) => {
                            warn!(
                                column = %column,
                                mask_type = %item.data_mask_info.data_mask_type,
                                "unsupported Ranger mask type; restricting column (fail-closed)"
                            );
                            if !policy.restricted_columns.contains(column) {
                                policy.restricted_columns.push(column.clone());
                            }
                        }
                    }
                }
            }
        }

        // Row-filter policy (policyType 2)
        if p.policy_type == POLICY_TYPE_ROWFILTER {
            for item in &p.row_filter_policy_items {
                if !item_matches(&item.users, &item.roles, &item.groups, user) {
                    continue;
                }
                if let Some(expr_str) = &item.row_filter_info.filter_expr {
                    match parse_sql_predicate(expr_str, &identity) {
                        Ok(expr) => policy.row_filters.push(expr),
                        Err(e) => {
                            // Do not log `expr_str`: a row-filter body routinely
                            // embeds sensitive literals (e.g. region = 'EU'). The
                            // expression lives in Ranger; log only that it failed.
                            warn!(
                                error = %e,
                                "unparseable Ranger row filter; denying (fail-closed)"
                            );
                            policy.row_filters.push(lit(false));
                        }
                    }
                }
            }
        }
    }

    debug!(
        user = %user.username,
        table = %table,
        db = %database,
        masks = policy.column_masks.len(),
        filters = policy.row_filters.len(),
        restricted = policy.restricted_columns.len(),
        "resolved Ranger policy"
    );
    policy
}

/// Resolve tag-based mask and row-filter policies from the bundle for a given
/// user identity and a set of column tags (Iceberg column-level tags).
///
/// Returns:
/// - `HashMap<tag, TagMaskSpec>` -- mask specs keyed by **tag name** (not column name).
///   `TagMaskSpec::Ready` holds a fully-resolved mask. `TagMaskSpec::Custom` holds the
///   raw `{col}`-template string; the rewriter substitutes the real column name at merge
///   time. The caller (Task 4 rewriter) maps tag -> column using the Iceberg schema's
///   column->tags map.
/// - `Vec<(tag, Expr)>` -- row filters keyed by the tag that triggered them.
/// - `HashSet<tag>` -- tags that matched the user but whose mask could NOT be
///   mapped to any supported spec (genuinely unsupported type). The caller MUST
///   restrict any column bearing one of these tags (fail-closed), mirroring the
///   resource path's `Err(())` -> `restricted_columns` behaviour. CUSTOM tags are
///   no longer in this set; they appear in the masks map as `TagMaskSpec::Custom`.
///
/// This function is pure (no I/O) and unit-tested directly. It is wired into
/// the plan rewriter in Task 4.
// The 3-tuple return (masks-by-tag, row-filters, unmappable-tags) is the
// documented contract; factoring it into a named type would obscure rather
// than clarify the shape at the single call site.
#[allow(clippy::type_complexity)]
pub(crate) fn resolve_tag_policies(
    bundle: &ServicePolicies,
    identity: &SessionIdentity,
    tags: &HashSet<String>,
) -> (HashMap<String, TagMaskSpec>, Vec<(String, Expr)>, HashSet<String>) {
    let mut masks: HashMap<String, TagMaskSpec> = HashMap::new();
    let mut filters: Vec<(String, Expr)> = Vec::new();
    let mut unmappable: HashSet<String> = HashSet::new();

    let tag_bundle = match &bundle.tag_policies {
        Some(tp) => tp,
        None => return (masks, filters, unmappable),
    };

    // Bridge SessionIdentity -> SessionUser for item_matches.
    use sqe_core::SessionUser;
    let user = SessionUser {
        username: identity.username.clone(),
        roles: identity.roles.clone(),
        subject: None,
        email: None,
        groups: Vec::new(),
    };

    for p in &tag_bundle.policies {
        if !p.is_enabled {
            continue;
        }

        // Read tag resource values for this policy.
        let tag_res = match p.resources.get("tag") {
            Some(r) => r,
            None => continue,
        };

        // Only process tags that the caller's column set carries.
        for tag_value in &tag_res.values {
            if !tags.contains(tag_value.as_str()) {
                continue;
            }

            // policyType 1: datamask
            if p.policy_type == POLICY_TYPE_DATAMASK {
                for item in &p.data_mask_policy_items {
                    if !item_matches(&item.users, &item.roles, &item.groups, &user) {
                        continue;
                    }
                    // CUSTOM masks carry a `{col}` placeholder that only the
                    // rewriter can substitute (the column name is not known here).
                    // Store the raw template as TagMaskSpec::Custom; merge_tag_masks
                    // performs the substitution and parses the expression per column.
                    // On parse failure the rewriter restricts the column (fail-closed).
                    if normalize_mask_type(&item.data_mask_info.data_mask_type) == MASK_TYPE_CUSTOM
                    {
                        if let Some(template) = &item.data_mask_info.value_expr {
                            masks.insert(tag_value.clone(), TagMaskSpec::Custom(template.clone()));
                        } else {
                            // CUSTOM with no value_expr: nothing to substitute -> restrict.
                            warn!(
                                tag = %tag_value,
                                "CUSTOM tag mask has no value_expr; marking tag \
                                 unmappable (caller restricts columns bearing this \
                                 tag, fail-closed)"
                            );
                            unmappable.insert(tag_value.clone());
                        }
                        continue;
                    }
                    // column placeholder is empty for non-CUSTOM types.
                    match map_mask(&item.data_mask_info, "", identity) {
                        Ok(Some(mask)) => {
                            masks.insert(tag_value.clone(), TagMaskSpec::Ready(mask));
                        }
                        Ok(None) => { /* MASK_NONE exemption: tag has no mask */ }
                        Err(()) => {
                            warn!(
                                tag = %tag_value,
                                mask_type = %item.data_mask_info.data_mask_type,
                                "unsupported Ranger tag mask type; marking tag \
                                 unmappable (caller restricts columns bearing this \
                                 tag, fail-closed)"
                            );
                            unmappable.insert(tag_value.clone());
                        }
                    }
                }
            }

            // policyType 2: rowfilter
            if p.policy_type == POLICY_TYPE_ROWFILTER {
                for item in &p.row_filter_policy_items {
                    if !item_matches(&item.users, &item.roles, &item.groups, &user) {
                        continue;
                    }
                    if let Some(expr_str) = &item.row_filter_info.filter_expr {
                        match parse_sql_predicate(expr_str, identity) {
                            Ok(expr) => filters.push((tag_value.clone(), expr)),
                            Err(e) => {
                                // Do not log the filter body (may carry literals).
                                warn!(
                                    tag = %tag_value,
                                    error = %e,
                                    "unparseable Ranger tag row filter; denying (fail-closed)"
                                );
                                filters.push((tag_value.clone(), lit(false)));
                            }
                        }
                    }
                }
            }
        }
    }

    (masks, filters, unmappable)
}

// --- Cache key + PolicyStore impl ---

fn cache_key(user: &SessionUser, table: &str, namespace: &str) -> String {
    let mut roles = user.roles.clone();
    roles.sort();
    format!("{}:{}:{}:{}", user.username, namespace, table, roles.join(","))
}

#[async_trait]
impl PolicyStore for RangerStore {
    async fn list_mask_policies(&self) -> sqe_core::Result<Vec<crate::MaskPolicyInfo>> {
        self.list_mask_policies_impl().await
    }

    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        let key = cache_key(user, table_name, namespace);
        if let Some(cached) = self.cache.get(&key).await {
            self.record_cache_hit();
            return Ok(cached);
        }
        self.record_cache_miss();
        let bundle = self.cached_bundle().await?;
        let policy = resolve_from_bundle(&bundle, user, table_name, namespace);
        self.cache.insert(key, policy.clone()).await;
        Ok(policy)
    }

    /// Resolve tag-based policies from the Ranger bundle for a given user and
    /// set of tag names present on a table's columns.
    ///
    /// Fetches the bundle (or re-uses the in-flight breaker state). On any
    /// fetch failure the method returns `(empty, [lit(false)])` — the
    /// `lit(false)` row filter denies all rows (fail-closed), consistent with
    /// how `resolve()` handles bundle errors.
    ///
    /// Masks are returned keyed by TAG NAME as `TagMaskSpec`. The plan rewriter
    /// maps tag -> column using the `TagSource` column->tags map. The third
    /// value is the set of tags whose mask could not be mapped; the rewriter
    /// restricts any column bearing one of those tags (fail-closed).
    async fn resolve_tags(
        &self,
        user: &SessionUser,
        tags: &std::collections::HashSet<String>,
    ) -> (
        std::collections::HashMap<String, TagMaskSpec>,
        Vec<Expr>,
        std::collections::HashSet<String>,
    ) {
        if tags.is_empty() {
            return (
                std::collections::HashMap::<String, TagMaskSpec>::new(),
                vec![],
                std::collections::HashSet::new(),
            );
        }

        let bundle = match self.cached_bundle().await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    user = %user.username,
                    error = %e,
                    "resolve_tags: failed to fetch Ranger bundle; \
                     denying all rows (fail-closed)"
                );
                return (
                    std::collections::HashMap::<String, TagMaskSpec>::new(),
                    vec![lit(false)],
                    std::collections::HashSet::new(),
                );
            }
        };

        let identity = SessionIdentity {
            username: user.username.clone(),
            roles: user.roles.clone(),
            database: None,
            schema: None,
        };

        let (masks, tag_filters, unmappable) = resolve_tag_policies(&bundle, &identity, tags);
        // Discard the tag keys from row filters — the rewriter only needs Exprs.
        let filter_exprs: Vec<Expr> = tag_filters.into_iter().map(|(_, e)| e).collect();
        (masks, filter_exprs, unmappable)
    }

    fn invalidate_all(&self) {
        self.cache.invalidate_all();
        // Also drop the shared bundle so a manual refresh does not serve a stale
        // bundle until its TTL elapses.
        self.bundle_cache.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tag-service mask types carry the owning component's prefix. Verified
    /// against a live Ranger 2.8 tag servicedef, which lists ONLY prefixed
    /// forms (`hive:MASK_SHOW_LAST_4`, `trino:MASK_NULL`, ...). Before this
    /// normalization every tag-based mask fell through to the unsupported arm
    /// and the tagged column was restricted instead of masked.
    #[test]
    fn mask_type_component_prefix_is_normalized() {
        assert_eq!(normalize_mask_type("MASK_SHOW_LAST_4"), "MASK_SHOW_LAST_4");
        assert_eq!(
            normalize_mask_type("hive:MASK_SHOW_LAST_4"),
            "MASK_SHOW_LAST_4"
        );
        assert_eq!(normalize_mask_type("hive:CUSTOM"), "CUSTOM");
        // Another engine's mask stays unmatched, so the caller fails closed
        // rather than applying a policy authored for Trino.
        assert_eq!(
            normalize_mask_type("trino:MASK_NULL"),
            "trino:MASK_NULL"
        );
    }

    const BUNDLE: &str = r#"{
      "policyVersion": 7,
      "policies": [
        {
          "id": 1, "policyType": 1, "isEnabled": true,
          "resources": {
            "database": {"values": ["sales"]},
            "table": {"values": ["orders"]},
            "column": {"values": ["amount"]}
          },
          "dataMaskPolicyItems": [
            {"users": [], "roles": ["analyst"],
             "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}
          ]
        },
        {
          "id": 2, "policyType": 2, "isEnabled": true,
          "resources": {
            "database": {"values": ["sales"]},
            "table": {"values": ["orders"]}
          },
          "rowFilterPolicyItems": [
            {"users": [], "roles": ["analyst"],
             "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
          ]
        }
      ]
    }"#;

    #[test]
    fn parses_bundle() {
        let sp: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        assert_eq!(sp.policy_version, Some(7));
        assert_eq!(sp.policies.len(), 2);
        assert_eq!(sp.policies[0].policy_type, 1);
        assert!(sp.policies[0].is_enabled);
        assert_eq!(
            sp.policies[0].data_mask_policy_items[0].data_mask_info.data_mask_type,
            "MASK_NULL"
        );
        assert_eq!(
            sp.policies[1].row_filter_policy_items[0]
                .row_filter_info
                .filter_expr
                .as_deref(),
            Some("region = 'EU'")
        );
    }

    #[test]
    fn empty_bundle_is_default() {
        let sp: ServicePolicies = serde_json::from_str("{}").unwrap();
        assert!(sp.policies.is_empty());
        assert_eq!(sp.policy_version, None);
    }

    #[test]
    fn from_config_builds_store() {
        let cfg = RangerPolicyConfig::default();
        // from_config must succeed even with an empty URL (no network call).
        let store = RangerStore::from_config(&cfg);
        assert!(store.is_ok(), "from_config failed: {:?}", store.err());
    }

    /// Fix 1: a fetch failure trips the breaker and the breaker-state gauge must
    /// read `open` (2). With no metrics wired the OPA-style record calls are
    /// no-ops; here we attach a registry and assert the gauge after a forced
    /// failure. `breaker_failure_threshold = 1` so a single failed download
    /// opens the breaker. The download URL is an unroutable address so the HTTP
    /// send fails fast (records a failure + records the "err" metric).
    #[tokio::test]
    async fn breaker_open_sets_state_gauge_for_ranger() {
        let cfg = RangerPolicyConfig {
            // RFC 5737 TEST-NET-1, unroutable; with a short timeout the send fails fast.
            url: "http://192.0.2.1:6080".to_string(),
            timeout_secs: 1,
            breaker_failure_threshold: 1,
            ..RangerPolicyConfig::default()
        };
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        let store = RangerStore::from_config(&cfg).unwrap().with_metrics(metrics.clone());

        // First fetch fails (transport error) -> record_failure opens the
        // breaker (threshold 1) and record_metric writes the gauge.
        let err = store.fetch_bundle().await;
        assert!(err.is_err(), "fetch against unroutable URL must fail");

        let gauge = metrics
            .policy_circuit_breaker_state
            .with_label_values(&["ranger"])
            .get();
        assert_eq!(
            gauge, 2.0,
            "breaker must be open (gauge=2) after a fetch failure tripped it"
        );

        // The duration histogram must have at least one observation labelled err.
        let observed = metrics
            .policy_resolve_duration_seconds
            .with_label_values(&["ranger", "err"])
            .get_sample_count();
        assert!(observed >= 1, "fetch failure must record a resolve-duration sample");
    }

    /// Fix 1: per-user cache hit/miss counters increment via `resolve`. We seed
    /// the per-user cache directly so the hit path is taken without any network
    /// call (the miss path would hit `cached_bundle`/`fetch_bundle`).
    #[tokio::test]
    async fn cache_hit_counter_increments_on_ranger() {
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        let store = RangerStore::from_config(&RangerPolicyConfig::default())
            .unwrap()
            .with_metrics(metrics.clone());

        let u = user("alice", &["analyst"]);
        let key = cache_key(&u, "orders", "sales");
        store.cache.insert(key, ResolvedPolicy::default()).await;

        let _ = store.resolve(&u, "orders", "sales").await.unwrap();
        let hits = metrics
            .policy_cache_hits_total
            .with_label_values(&["ranger"])
            .get();
        assert_eq!(hits, 1, "a warm per-user cache entry must record one hit");
    }

    /// `cached_bundle` serves a warm bundle from the cache without re-fetching.
    /// We seed the bundle cache directly (no HTTP), then assert the returned Arc
    /// is pointer-identical to the seeded one -- proving the cache hit path is
    /// taken and `fetch_bundle` (which would fail on the empty URL) is skipped.
    #[tokio::test]
    async fn cached_bundle_serves_warm_copy_without_fetch() {
        let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
        let seeded: Arc<ServicePolicies> = Arc::new(serde_json::from_str(BUNDLE).unwrap());
        store
            .bundle_cache
            .insert(BUNDLE_KEY, seeded.clone())
            .await;

        let got = store
            .cached_bundle()
            .await
            .expect("warm bundle must be served from cache");
        assert!(
            Arc::ptr_eq(&seeded, &got),
            "cached_bundle must return the cached Arc, not a re-fetched copy"
        );
    }

    fn user(name: &str, roles: &[&str]) -> SessionUser {
        SessionUser {
            username: name.to_string(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
            subject: None,
            email: None,
            groups: vec![],
        }
    }

    /// Ranger wildcard and case-insensitive matching, per the `hive` service
    /// definition's `matcherOptions: {wildCard: "true", ignoreCase: "true"}`.
    ///
    /// This replaces a characterization test that pinned the OPPOSITE behaviour
    /// (`ranger_glob_patterns_are_not_matched`) and said in its own comment that it
    /// SHOULD fail and be rewritten when glob support landed. It has.
    ///
    /// Both halves matter for the same reason: a policy that does not fire is
    /// masking or filtering that is silently ABSENT while the Ranger console shows
    /// it as configured.
    #[test]
    fn ranger_wildcards_and_case_folding_match_the_servicedef() {
        let r = |v: &str| RangerResource {
            values: vec![v.to_string()],
            is_excludes: false,
        };

        // Prefix, suffix and interior globs.
        assert!(resource_matches(&r("orders*"), "orders"), "* may match nothing");
        assert!(resource_matches(&r("orders*"), "orders_2024"));
        assert!(!resource_matches(&r("orders*"), "sales_orders"));
        assert!(resource_matches(&r("*_pii"), "customer_pii"));
        assert!(!resource_matches(&r("*_pii"), "pii_customer"));
        assert!(resource_matches(&r("a*b*c"), "axxbyyc"));

        // `?` is exactly one character.
        assert!(resource_matches(&r("orders_20??"), "orders_2024"));
        assert!(!resource_matches(&r("orders_20??"), "orders_204"));

        // ignoreCase, both directions.
        assert!(resource_matches(&r("Orders"), "orders"));
        assert!(resource_matches(&r("orders"), "ORDERS"));
        assert!(resource_matches(&r("ORD*"), "orders_2024"));

        // Nothing else is special: a Ranger pattern is not a regex, so these are
        // literals and must NOT match arbitrary text.
        assert!(!resource_matches(&r("orders."), "ordersX"));
        assert!(!resource_matches(&r("or.*"), "orders"));

        // Bare `*` and exact match still behave.
        assert!(resource_matches(&r("*"), "anything"));
        assert!(resource_matches(&r("orders"), "orders"));
        assert!(!resource_matches(&r("orders"), "invoices"));

        // isExcludes inverts, and now inverts a GLOB too.
        let excl = RangerResource {
            values: vec!["tmp_*".to_string()],
            is_excludes: true,
        };
        assert!(!resource_matches(&excl, "tmp_scratch"), "excluded by the glob");
        assert!(resource_matches(&excl, "orders"), "outside the exclusion");

        // An empty values list matches nothing.
        let empty = RangerResource { values: vec![], is_excludes: false };
        assert!(!resource_matches(&empty, "orders"));
    }

    fn user_with_groups(name: &str, roles: &[&str], groups: &[&str]) -> SessionUser {
        SessionUser {
            username: name.to_string(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
            subject: None,
            email: None,
            groups: groups.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// A pathological pattern must not blow up: policies are operator input on a
    /// per-query path, so backtracking has to stay bounded.
    #[test]
    fn wildcard_matching_terminates_on_a_pathological_pattern() {
        let start = std::time::Instant::now();
        assert!(!wildcard_match("*a*a*a*a*a*a*a*b", &"a".repeat(64)));
        assert!(wildcard_match(&"*".repeat(32), "anything"));
        assert!(
            start.elapsed() < std::time::Duration::from_millis(500),
            "matching took {:?}; the backtracking is not bounded",
            start.elapsed()
        );
    }

    #[test]
    fn group_bound_items_match_the_session_groups() {
        let g = ["data-platform".to_string()];
        let empty: [String; 0] = [];

        assert!(
            item_matches(&empty, &empty, &g, &user_with_groups("bob", &[], &["data-platform"])),
            "a group the session carries must match"
        );
        assert!(
            !item_matches(&empty, &empty, &g, &user_with_groups("bob", &[], &["other-group"])),
            "a group the session does not carry must not match"
        );
        assert!(
            !item_matches(&empty, &empty, &g, &user_with_groups("bob", &["analyst"], &[])),
            "no groups on the session cannot match a group-bound item"
        );
        // Roles and users still match on their own, and the three are OR-ed.
        assert!(item_matches(
            &["bob".to_string()],
            &empty,
            &empty,
            &user_with_groups("bob", &[], &[])
        ));
        assert!(item_matches(
            &empty,
            &["analyst".to_string()],
            &g,
            &user_with_groups("bob", &["analyst"], &[])
        ));
    }

    #[test]
    fn flattens_iceberg_to_hive_database() {
        assert_eq!(hive_database("sales"), "sales");
        assert_eq!(hive_database("sales.eu"), "sales.eu");
    }

    #[test]
    fn mask_null_maps_to_nullify() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(matches!(
            policy.column_masks.get("amount"),
            Some(MaskType::Nullify)
        ));
    }

    #[test]
    fn row_filter_applied_for_matching_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert_eq!(policy.row_filters.len(), 1);
    }

    #[test]
    fn no_match_for_other_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("bob", &["engineer"]),
            "orders",
            "sales",
        );
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    #[test]
    fn user_match_works_too() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0].roles.clear();
        bundle.policies[0].data_mask_policy_items[0].users = vec!["alice".to_string()];
        let policy =
            resolve_from_bundle(&bundle, &user("alice", &[]), "orders", "sales");
        assert!(policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn unsupported_mask_restricts_column_failclosed() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "MASK_FUTURE_UNSUPPORTED".to_string();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(policy.restricted_columns.contains(&"amount".to_string()));
        assert!(!policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn custom_mask_unparseable_expr_restricts_column_failclosed() {
        // A resource CUSTOM mask whose valueExpr cannot be parsed (here a
        // qualified reference, which parse_sql_predicate rejects) must fail
        // closed: the column is restricted and carries no mask, never returned
        // raw. This pins the documented behavior of the resource CUSTOM path.
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "CUSTOM".to_string();
        bundle.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .value_expr = Some("t.department".to_string());
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(
            policy.restricted_columns.contains(&"amount".to_string()),
            "unparseable CUSTOM mask must restrict the column (fail-closed)"
        );
        assert!(
            !policy.column_masks.contains_key("amount"),
            "restricted column must not also carry a mask"
        );
    }

    #[test]
    fn datamask_column_isexcludes_denies_failclosed() {
        // A datamask policy authored as "mask all columns EXCEPT these"
        // (column.isExcludes = true) cannot be honored on the resource path
        // (no schema for the complement). The previous code read the values as
        // an include list, leaving every intended-masked column raw. It must
        // now fail closed: deny the table.
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].resources.get_mut("column").unwrap().is_excludes = true;
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(
            policy.row_filters.contains(&lit(false)),
            "column isExcludes datamask must inject a deny (lit(false)) row filter"
        );
        assert!(
            policy.column_masks.is_empty(),
            "excludes datamask must not be treated as an include list"
        );
    }

    #[test]
    fn mask_none_is_exemption() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "MASK_NONE".to_string();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(!policy.column_masks.contains_key("amount"));
        assert!(!policy.restricted_columns.contains(&"amount".to_string()));
    }

    #[test]
    fn disabled_policy_is_skipped() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].is_enabled = false; // the datamask policy
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(policy.column_masks.is_empty());
    }

    #[test]
    fn wrong_table_does_not_match() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "customers",
            "sales",
        );
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    #[test]
    fn unparseable_row_filter_fails_closed() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[1].row_filter_policy_items[0]
            .row_filter_info
            .filter_expr = Some("this is not sql !!!".to_string());
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        // Fail-closed: a broken filter must NOT result in zero filters (which
        // would expose all rows). Expect a lit(false) deny filter instead.
        assert_eq!(policy.row_filters.len(), 1);
        // The single filter should be the literal-false deny, not a parsed predicate.
        let s = format!("{:?}", policy.row_filters[0]).to_lowercase();
        assert!(
            s.contains("false") || s.contains("boolean(false)"),
            "expected deny filter, got {s}"
        );
    }

    #[test]
    fn masks_all_columns_in_multi_column_policy() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].resources.get_mut("column").unwrap().values =
            vec!["amount".to_string(), "discount".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        assert!(policy.column_masks.contains_key("amount"));
        assert!(policy.column_masks.contains_key("discount"));
    }

    #[test]
    fn wildcard_table_matches() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].resources.get_mut("table").unwrap().values = vec!["*".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "anything", "sales");
        assert!(policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn excludes_inverts_match() {
        // is_excludes on table should make "orders" NOT match a values=["orders"] exclude.
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let tr = bundle.policies[0].resources.get_mut("table").unwrap();
        tr.is_excludes = true; // exclude "orders"
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        assert!(policy.column_masks.is_empty());
    }

    #[test]
    fn custom_mask_substitutes_column() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let mi = &mut bundle.policies[0].data_mask_policy_items[0].data_mask_info;
        mi.data_mask_type = "CUSTOM".to_string();
        mi.value_expr = Some("concat('x', {col})".to_string());
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        match policy.column_masks.get("amount") {
            Some(crate::MaskType::Custom(e)) => {
                let s = datafusion::sql::unparser::expr_to_sql(e)
                    .unwrap()
                    .to_string()
                    .to_lowercase();
                assert!(s.contains("amount"), "custom expr must reference the real column: {s}");
            }
            other => panic!("expected Custom mask, got {other:?}"),
        }
    }

    #[test]
    fn group_bound_item_is_skipped() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let item = &mut bundle.policies[0].data_mask_policy_items[0];
        item.roles.clear();
        item.users.clear();
        item.groups = vec!["analysts_group".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        assert!(policy.column_masks.is_empty(), "group-bound item must not be enforced in MVP");
    }

    // --- map_mask arm tests ---

    #[test]
    fn maps_show_last_4() {
        let info = DataMaskInfo { data_mask_type: "MASK_SHOW_LAST_4".into(), ..Default::default() };
        match map_mask(&info, "ssn", &SessionIdentity::default()) {
            Ok(Some(MaskType::PartialMask { show_last: 4, show_first: 0, .. })) => {}
            other => panic!("expected show-last-4 PartialMask, got {other:?}"),
        }
    }

    #[test]
    fn maps_show_first_4() {
        let info = DataMaskInfo { data_mask_type: "MASK_SHOW_FIRST_4".into(), ..Default::default() };
        assert!(matches!(
            map_mask(&info, "ssn", &SessionIdentity::default()),
            Ok(Some(MaskType::PartialMask { show_first: 4, show_last: 0, .. }))
        ));
    }

    #[test]
    fn maps_full_mask_uses_hive_default_chars() {
        let info = DataMaskInfo { data_mask_type: "MASK".into(), ..Default::default() };
        match map_mask(&info, "name", &SessionIdentity::default()) {
            Ok(Some(MaskType::PartialMask {
                upper: 'X',
                lower: 'x',
                digit: 'n',
                show_first: 0,
                show_last: 0,
            })) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn maps_date_show_year() {
        let info = DataMaskInfo { data_mask_type: "MASK_DATE_SHOW_YEAR".into(), ..Default::default() };
        assert!(matches!(map_mask(&info, "hired_at", &SessionIdentity::default()), Ok(Some(MaskType::DateShowYear))));
    }

    #[test]
    fn truly_unknown_mask_is_err() {
        let info = DataMaskInfo { data_mask_type: "MASK_FUTURE_UNSUPPORTED".into(), ..Default::default() };
        assert!(map_mask(&info, "x", &SessionIdentity::default()).is_err());
    }

    // --- tagPolicies tests ---

    /// A ServicePolicies bundle that includes a top-level `tagPolicies` block
    /// with a datamask policy for tag "PII" (role=engineer) and a row-filter
    /// policy for tag "RESTRICTED" (role=analyst).
    ///
    /// NOTE: the exact live shape of tagPolicies must be verified against a
    /// real Ranger bundle that has tag-linked policies before this is used in
    /// production. See the Phase 3 prerequisite task.
    const TAG_BUNDLE: &str = r#"{
      "policyVersion": 1,
      "policies": [],
      "tagPolicies": {
        "serviceName": "tag",
        "policies": [
          {
            "id": 1, "policyType": 1, "isEnabled": true,
            "resources": { "tag": { "values": ["PII"] } },
            "dataMaskPolicyItems": [
              { "users": [], "roles": ["engineer"],
                "dataMaskInfo": { "dataMaskType": "MASK_SHOW_LAST_4" } }
            ]
          },
          {
            "id": 2, "policyType": 2, "isEnabled": true,
            "resources": { "tag": { "values": ["RESTRICTED"] } },
            "rowFilterPolicyItems": [
              { "users": [], "roles": ["analyst"],
                "rowFilterInfo": { "filterExpr": "region = 'EU'" } }
            ]
          }
        ]
      }
    }"#;

    #[test]
    fn tag_mask_resolved_for_matching_role() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "bob".into(), roles: vec!["engineer".into()], ..Default::default() };
        let (masks, filters, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        // tag PII -> a PartialMask (MASK_SHOW_LAST_4) for engineer, wrapped in TagMaskSpec::Ready
        assert!(masks.contains_key("PII"));
        assert!(
            matches!(
                masks.get("PII"),
                Some(TagMaskSpec::Ready(crate::MaskType::PartialMask { show_last: 4, .. }))
            ),
            "supported mask must be wrapped in TagMaskSpec::Ready"
        );
        assert!(unmappable.is_empty(), "supported mask must not be unmappable");
        let _ = filters; // not the focus of this test
    }

    #[test]
    fn tag_mask_not_resolved_for_other_role() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "x".into(), roles: vec!["other".into()], ..Default::default() };
        let (masks, _f, _u) = resolve_tag_policies(&sp, &id, &tags);
        assert!(masks.is_empty());
    }

    #[test]
    fn tag_row_filter_resolved() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["RESTRICTED".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "a".into(), roles: vec!["analyst".into()], ..Default::default() };
        let (_m, filters, _u) = resolve_tag_policies(&sp, &id, &tags);
        assert_eq!(filters.len(), 1); // one (tag, Expr) row filter
    }

    #[test]
    fn untagged_yields_nothing() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = HashSet::new();
        let id = SessionIdentity::default();
        let (m, f, u) = resolve_tag_policies(&sp, &id, &tags);
        assert!(m.is_empty() && f.is_empty() && u.is_empty());
    }

    /// An unsupported tag mask type must be reported as unmappable (not silently
    /// skipped) so the rewriter restricts the column (fail-closed).
    #[test]
    fn unsupported_tag_mask_is_unmappable() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        sp.tag_policies.as_mut().unwrap().policies[0]
            .data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "MASK_FUTURE_UNSUPPORTED".to_string();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "bob".into(), roles: vec!["engineer".into()], ..Default::default() };
        let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        assert!(!masks.contains_key("PII"), "unsupported mask must not produce a mask");
        assert!(unmappable.contains("PII"), "unsupported mask must mark tag unmappable");
    }

    /// A CUSTOM tag mask with a value_expr must be stored as `TagMaskSpec::Custom`
    /// (not unmappable). The rewriter performs `{col}` substitution at merge time.
    #[test]
    fn custom_tag_mask_stored_as_custom_spec() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let mi = &mut sp.tag_policies.as_mut().unwrap().policies[0]
            .data_mask_policy_items[0]
            .data_mask_info;
        mi.data_mask_type = "CUSTOM".to_string();
        mi.value_expr = Some("concat('x', {col})".to_string());
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "bob".into(), roles: vec!["engineer".into()], ..Default::default() };
        let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        assert!(
            !unmappable.contains("PII"),
            "CUSTOM tag with value_expr must NOT be unmappable"
        );
        match masks.get("PII") {
            Some(TagMaskSpec::Custom(template)) => {
                assert_eq!(template, "concat('x', {col})", "template must be stored verbatim");
            }
            other => panic!("expected TagMaskSpec::Custom for PII, got {:?}", other),
        }
    }

    /// A CUSTOM tag mask with no value_expr must remain unmappable (fail-closed):
    /// there is nothing to substitute, so the column must be restricted.
    #[test]
    fn custom_tag_mask_no_value_expr_is_unmappable() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let mi = &mut sp.tag_policies.as_mut().unwrap().policies[0]
            .data_mask_policy_items[0]
            .data_mask_info;
        mi.data_mask_type = "CUSTOM".to_string();
        mi.value_expr = None; // no template
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "bob".into(), roles: vec!["engineer".into()], ..Default::default() };
        let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        assert!(
            !masks.contains_key("PII"),
            "CUSTOM mask with no value_expr must not produce a spec"
        );
        assert!(
            unmappable.contains("PII"),
            "CUSTOM mask with no value_expr must be unmappable (fail-closed)"
        );
    }

    // --- tagPolicies live-sample validation (HIGH-tagpolicies-shape-unvalidated) ---

    /// Placeholder bundle for the live-capture validation. Ships as an empty
    /// `tagPolicies` block (valid JSON) so this file compiles before the real
    /// capture exists. During the Ranger-backend validation run, replace
    /// `src/testdata/tag_bundle_live_sample.json` with a bundle captured from a
    /// real Ranger (`GET /service/plugins/policies/download/<service>`) that
    /// carries at least one tag-linked datamask and one tag-linked rowfilter,
    /// then remove the `#[ignore]` below and adjust the constants to match the
    /// capture.
    const TAG_BUNDLE_LIVE_SAMPLE: &str = include_str!("testdata/tag_bundle_live_sample.json");

    /// Expected identity/tag the captured sample must resolve a non-empty result
    /// for. Whoever drops in the real capture aligns these with the policies in
    /// it (role bound to the datamask/rowfilter items, tag on the resources).
    const LIVE_SAMPLE_ROLE: &str = "engineer";
    const LIVE_SAMPLE_TAG: &str = "PII";
    /// Tag carrying the row-filter policy in the capture.
    const LIVE_SAMPLE_FILTER_TAG: &str = "RESTRICTED";

    /// HIGH-tagpolicies-shape-unvalidated: deserialize a bundle captured from a
    /// LIVE Ranger and assert `resolve_tag_policies` returns a non-empty result
    /// (at least one mask OR one row filter). The whole tag-masking path is
    /// currently validated only against a hand-authored `TAG_BUNDLE` fixture
    /// whose shape is flagged unconfirmed (`TODO(phase3)` at the top of this
    /// file). If the live `tagPolicies` JSON differs, `bundle.tag_policies`
    /// deserializes to `None` and this test fails, surfacing the shape drift
    /// instead of silently returning raw PII columns.
    ///
    /// ACTIVE as of the Ranger-backend validation run: the capture in
    /// `testdata/tag_bundle_live_sample.json` came from a live Ranger 2.8 via
    /// `access_control_e2e::capture_live_tag_bundle` (re-capture with
    /// `SQE_AC_CAPTURE=1 scripts/access-control-test.sh capture_live_tag_bundle`).
    ///
    /// It carries BOTH tag policy types, so this asserts both branches against
    /// real data: a `hive:MASK_SHOW_LAST_4` datamask on tag `PII` and a row
    /// filter `region = 'EU'` on tag `RESTRICTED`, both bound to role
    /// `engineer`. The row filter is only present because the e2e bootstrap
    /// gives the tag servicedef a `rowFilterDef` -- Ranger propagates
    /// `dataMaskDef` into the tag servicedef unconditionally but `rowFilterDef`
    /// only when Ranger Admin sets
    /// `ranger.servicedef.autopropagate.rowfilterdef.to.tag=true` (default
    /// false).
    #[test]
    fn resolve_tag_policies_against_live_sample() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE_LIVE_SAMPLE)
            .expect("captured live sample must be valid ServicePolicies JSON");
        assert!(
            sp.tag_policies.is_some(),
            "live bundle must deserialize a tagPolicies block (None means the \
             shape drifted and tag masking would silently no-op)"
        );

        let tags: HashSet<String> = [
            LIVE_SAMPLE_TAG.to_string(),
            LIVE_SAMPLE_FILTER_TAG.to_string(),
        ]
        .into_iter()
        .collect();
        let id = SessionIdentity {
            username: "live-sample-user".into(),
            roles: vec![LIVE_SAMPLE_ROLE.into()],
            ..Default::default()
        };
        let (masks, filters, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        assert!(
            masks.contains_key(LIVE_SAMPLE_TAG),
            "live capture must resolve a mask for tag {LIVE_SAMPLE_TAG}; got masks {:?}, \
             unmappable {unmappable:?} (a shape drift or an unhandled component-prefixed \
             mask type lands here)",
            masks.keys().collect::<Vec<_>>()
        );
        assert!(
            filters.iter().any(|(tag, _)| tag == LIVE_SAMPLE_FILTER_TAG),
            "live capture must resolve a row filter for tag {LIVE_SAMPLE_FILTER_TAG}; got {:?}",
            filters.iter().map(|(t, _)| t).collect::<Vec<_>>()
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Introspection: listing the mask policies as WRITTEN
// ─────────────────────────────────────────────────────────────────────────────

impl RangerStore {
    /// Every data-mask policy (`policyType` 1) in the configured service, as
    /// written in Ranger.
    ///
    /// Deliberately NOT filtered by user or resolved against precedence. What
    /// applies to a given user is a different question, answered by
    /// `SHOW EFFECTIVE POLICY`, and conflating the two is how an audit reaches the
    /// wrong conclusion: a policy listed here may be overridden by a deny, beaten
    /// by a tag mask, or simply not match the caller's roles.
    ///
    /// Reads the same download bundle enforcement reads, so it cannot drift from
    /// what the engine actually sees. It bypasses the resolve cache on purpose:
    /// an operator asking what is configured wants the current answer, not one up
    /// to `cache_ttl_secs` old.
    pub async fn list_mask_policies_impl(
        &self,
    ) -> sqe_core::Result<Vec<crate::MaskPolicyInfo>> {
        let bundle = self.fetch_bundle().await?;
        Ok(mask_policies_from_bundle(&bundle))
    }
}

/// Extract the data-mask policies from a bundle.
///
/// Split out from the fetch so the mapping can be tested against real Ranger JSON
/// without a live Ranger. The mapping is where the mistakes are: a policy can name
/// several columns, carry several grantee kinds, and be disabled.
fn mask_policies_from_bundle(bundle: &ServicePolicies) -> Vec<crate::MaskPolicyInfo> {
    {
        let mut out = Vec::new();
        for p in &bundle.policies {
            if p.policy_type != POLICY_TYPE_DATAMASK {
                continue;
            }
            let values = |key: &str| -> Vec<String> {
                p.resources
                    .get(key)
                    .map(|r| r.values.clone())
                    .unwrap_or_default()
            };
            for item in &p.data_mask_policy_items {
                let mut grantees: Vec<String> = Vec::new();
                grantees.extend(item.users.iter().map(|u| format!("USER {u}")));
                grantees.extend(item.roles.iter().map(|r| format!("ROLE {r}")));
                grantees.extend(item.groups.iter().map(|g| format!("GROUP {g}")));
                grantees.sort();
                out.push(crate::MaskPolicyInfo {
                    name: p.name.clone(),
                    enabled: p.is_enabled,
                    database: values("database").join(", "),
                    table: values("table").join(", "),
                    column: values("column").join(", "),
                    mask_type: item.data_mask_info.data_mask_type.clone(),
                    expression: item.data_mask_info.value_expr.clone().unwrap_or_default(),
                    grantees: grantees.join(", "),
                });
            }
        }
        // Stable order: by name, then by the resource it covers, so two runs of
        // SHOW MASKING POLICIES on an unchanged service produce identical output.
        out.sort_by(|a, b| {
            (&a.name, &a.database, &a.table, &a.column, &a.grantees).cmp(&(
                &b.name,
                &b.database,
                &b.table,
                &b.column,
                &b.grantees,
            ))
        });
        out
    }
}

#[cfg(test)]
mod mask_policy_listing_tests {
    use super::*;

    /// Real Ranger bundle shape, including a row-filter policy that must NOT appear
    /// and a disabled policy that MUST appear (listed, doing nothing).
    const BUNDLE: &str = r#"{
      "policyVersion": 7,
      "policies": [
        {"id":1,"name":"mask-orders-ssn","policyType":1,"isEnabled":true,
         "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]},
                      "column":{"values":["ssn"]}},
         "dataMaskPolicyItems":[{"roles":["engineer"],"users":["dave"],
           "dataMaskInfo":{"dataMaskType":"CUSTOM","valueExpr":"concat('x', {col})"}}]},
        {"id":2,"name":"mask-multi","policyType":1,"isEnabled":false,
         "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]},
                      "column":{"values":["email","phone"]}},
         "dataMaskPolicyItems":[{"groups":["public"],
           "dataMaskInfo":{"dataMaskType":"MASK_NULL"}}]},
        {"id":3,"name":"filter-orders-eu","policyType":2,"isEnabled":true,
         "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]}},
         "rowFilterPolicyItems":[{"roles":["engineer"],
           "rowFilterInfo":{"filterExpr":"region = 'EU'"}}]},
        {"id":4,"name":"access-orders","policyType":0,"isEnabled":true,
         "resources":{"database":{"values":["sales"]}}},
        {"id":5,"name":"rowfilter-carrying-a-mask-item","policyType":2,"isEnabled":true,
         "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]},
                      "column":{"values":["ssn"]}},
         "dataMaskPolicyItems":[{"roles":["engineer"],
           "dataMaskInfo":{"dataMaskType":"MASK_NULL"}}]}
      ]
    }"#;

    fn listed() -> Vec<crate::MaskPolicyInfo> {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).expect("parse bundle");
        mask_policies_from_bundle(&bundle)
    }

    /// Only policyType 1. A row filter or an access policy showing up under
    /// SHOW MASKING POLICIES would misreport what protects a column.
    ///
    /// The fixture includes a policyType-2 policy that ALSO carries
    /// `dataMaskPolicyItems`, which is the only shape where the type check is the
    /// thing doing the work. Without that entry this assertion was vacuous: a plain
    /// row-filter policy has no mask items, so the loop emitted nothing for it
    /// whether the type was checked or not, and removing the check changed no output.
    /// Mutation-verified after adding it: dropping the guard makes
    /// `rowfilter-carrying-a-mask-item` appear.
    #[test]
    fn only_data_mask_policies_are_listed() {
        let all = listed();
        let names: Vec<&str> = all.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["mask-multi", "mask-orders-ssn"]);
    }

    #[test]
    fn the_resource_and_expression_survive_the_mapping() {
        let p = listed()
            .into_iter()
            .find(|p| p.name == "mask-orders-ssn")
            .expect("the ssn policy");
        assert_eq!(p.database, "sales");
        assert_eq!(p.table, "orders");
        assert_eq!(p.column, "ssn");
        assert_eq!(p.mask_type, "CUSTOM");
        assert_eq!(p.expression, "concat('x', {col})");
        assert!(p.enabled);
    }

    /// Every grantee kind is shown, and labelled. "engineer" alone would not say
    /// whether it is a user or a role, and Ranger allows a user and a role of the
    /// same name.
    #[test]
    fn grantees_are_labelled_by_kind() {
        let p = listed()
            .into_iter()
            .find(|p| p.name == "mask-orders-ssn")
            .expect("the ssn policy");
        assert_eq!(p.grantees, "ROLE engineer, USER dave");
    }

    /// A policy disabled in the console is LISTED, with enabled = false. Hiding it
    /// would make a mask that someone can re-enable with one click invisible to an
    /// audit.
    #[test]
    fn a_disabled_policy_is_listed_and_flagged() {
        let p = listed()
            .into_iter()
            .find(|p| p.name == "mask-multi")
            .expect("the disabled policy");
        assert!(!p.enabled);
        assert_eq!(p.column, "email, phone", "several columns are joined");
        assert_eq!(p.grantees, "GROUP public");
    }

    /// Deterministic order, so two runs on an unchanged service produce identical
    /// output and a diff means something changed.
    #[test]
    fn the_order_is_stable() {
        assert_eq!(listed(), listed());
    }
}
