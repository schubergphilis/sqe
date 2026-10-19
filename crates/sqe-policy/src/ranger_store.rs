//! Apache Ranger fine-grained PolicyStore. Reads row-filter (policyType 2) and
//! data-mask (policyType 1) policies from a `hive`-type Ranger service and
//! returns a `ResolvedPolicy` for the PlanRewriter. Shares the policy set with
//! Apache Spark / Kyuubi. See docs/ranger-fine-grained-service-type.md.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::logical_expr::{lit, Expr};
use moka::future::Cache;
use reqwest::Client;
use serde::Deserialize;
use sqe_core::config::RangerPolicyConfig;
use sqe_core::{SecretString, SessionUser};
use sqe_metrics::MetricsRegistry;
use tracing::{debug, warn};

use sqe_core::CircuitBreaker;
use crate::policy_expr::parse_sql_predicate;
use crate::session_udf::SessionIdentity;
use crate::{MaskType, PolicyStore, ResolvedPolicy, TagMaskSpec};
use sqe_policy_api::{wildcard_match, HiveCatalogs};

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
    /// Ranger `policyPriority`: 0 normal, 1 override. Ranger evaluates
    /// higher priorities first, and for a mask the first policy that decides a
    /// column is final, so an override mask beats a normal one.
    #[serde(rename = "policyPriority", default)]
    pub(crate) policy_priority: i32,
    #[serde(rename = "isEnabled", default)]
    pub(crate) is_enabled: bool,
    /// Resource map: keys are "database", "table", "column".
    #[serde(default)]
    pub(crate) resources: HashMap<String, RangerResource>,
    #[serde(rename = "dataMaskPolicyItems", default)]
    pub(crate) data_mask_policy_items: Vec<DataMaskPolicyItem>,
    #[serde(rename = "rowFilterPolicyItems", default)]
    pub(crate) row_filter_policy_items: Vec<RowFilterPolicyItem>,
    /// Access policy (policyType 0) items: the grants. Read only for
    /// Hive-root tables, see `access_grants_table`.
    #[serde(rename = "policyItems", default)]
    pub(crate) policy_items: Vec<AccessPolicyItem>,
    /// Access policy deny items. A matching deny wins over every allow, as in
    /// Ranger's own evaluator.
    #[serde(rename = "denyPolicyItems", default)]
    pub(crate) deny_policy_items: Vec<AccessPolicyItem>,
    /// Grantees carved out of `policyItems`.
    #[serde(rename = "allowExceptions", default)]
    pub(crate) allow_exceptions: Vec<AccessPolicyItem>,
    /// Grantees carved out of `denyPolicyItems`.
    #[serde(rename = "denyExceptions", default)]
    pub(crate) deny_exceptions: Vec<AccessPolicyItem>,
    /// Time windows the policy is in force. Empty means always. Read only for
    /// access policies, see `policy_schedule_active`.
    #[serde(rename = "validitySchedules", default)]
    pub(crate) validity_schedules: Vec<ValiditySchedule>,
}

/// One Ranger `validitySchedules` entry. Times use Ranger's
/// `yyyy/MM/dd HH:mm:ss` format in `timeZone` (UTC when unset).
#[derive(Debug, Deserialize, Default)]
pub(crate) struct ValiditySchedule {
    #[serde(rename = "startTime", default)]
    pub(crate) start_time: Option<String>,
    #[serde(rename = "endTime", default)]
    pub(crate) end_time: Option<String>,
    #[serde(rename = "timeZone", default)]
    pub(crate) time_zone: Option<String>,
    /// Recurring windows. Not modelled: a schedule carrying any is treated as
    /// unevaluable.
    #[serde(default)]
    pub(crate) recurrences: Vec<serde_json::Value>,
}

/// One grantee list with the access types it carries, the shape shared by the
/// four access-policy item lists (`policyItems`, `denyPolicyItems`,
/// `allowExceptions`, `denyExceptions`).
#[derive(Debug, Deserialize, Default)]
pub(crate) struct AccessPolicyItem {
    #[serde(default)]
    pub(crate) users: Vec<String>,
    #[serde(default)]
    pub(crate) roles: Vec<String>,
    #[serde(default)]
    pub(crate) groups: Vec<String>,
    #[serde(default)]
    pub(crate) accesses: Vec<RangerAccess>,
    /// Ranger policy conditions (IP range, expressions, ...). SQE evaluates
    /// none of them, so an allow item carrying any never grants.
    #[serde(default)]
    pub(crate) conditions: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RangerAccess {
    #[serde(rename = "type", default)]
    pub(crate) access_type: String,
    #[serde(rename = "isAllowed", default = "default_true")]
    pub(crate) is_allowed: bool,
}

fn default_true() -> bool {
    true
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
    /// Matched against `SessionUser.groups` (JWT `groups_claim`). A group-only
    /// item never fires when that claim is unset; see `note_group_only_items`.
    #[serde(default)]
    pub(crate) groups: Vec<String>,
    #[serde(rename = "dataMaskInfo", default)]
    pub(crate) data_mask_info: DataMaskInfo,
    /// Ranger item conditions. SQE evaluates none of them, and the first
    /// matching item decides the column, so a conditional item restricts the
    /// column rather than letting a later (possibly `MASK_NONE`) item decide.
    #[serde(default)]
    pub(crate) conditions: Vec<serde_json::Value>,
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
    /// Matched against `SessionUser.groups` (JWT `groups_claim`). A group-only
    /// item never fires when that claim is unset; see `note_group_only_items`.
    #[serde(default)]
    pub(crate) groups: Vec<String>,
    #[serde(rename = "rowFilterInfo", default)]
    pub(crate) row_filter_info: RowFilterInfo,
    /// Ranger item conditions. SQE evaluates none of them; see
    /// `deciding_row_filter_items` for how a conditional item is applied.
    #[serde(default)]
    pub(crate) conditions: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RowFilterInfo {
    #[serde(rename = "filterExpr", default)]
    pub(crate) filter_expr: Option<String>,
}

// --- RangerStore struct, constructor, and download fetch ---

/// PolicyType discriminators in the Ranger bundle JSON (`policyType` field).
/// 0 = access (enforced for Hive-root tables only, see `access_grants_table`),
/// 1 = DATAMASK, 2 = ROWFILTER.
const POLICY_TYPE_ACCESS: i32 = 0;
const POLICY_TYPE_DATAMASK: i32 = 1;
const POLICY_TYPE_ROWFILTER: i32 = 2;

/// Ranger's built-in group that every user belongs to. The `hive` servicedef
/// evaluator matches a `public` item for any authenticated user, whatever the
/// session's own group list says, so the blanket "everyone may select" policy
/// operators write against it has to fire here too.
const PUBLIC_GROUP: &str = "public";

/// Ranger `dataMaskType` discriminator for an operator-authored masking
/// expression. Shared by `map_mask` and `resolve_tag_policies` so the resource
/// path and the tag path agree on the CUSTOM special-case.
const MASK_TYPE_CUSTOM: &str = "CUSTOM";

/// Single, constant key for the user-independent ServicePolicies bundle cache.
const BUNDLE_KEY: &str = "__bundle__";

/// Fraction of the bundle TTL after which a reader refreshes the bundle
/// ahead of its expiry (#816). One reader downloads; the rest keep reading the
/// cached copy, so expiry never sends every concurrent query to Ranger.
const BUNDLE_REFRESH_AHEAD: f64 = 0.8;

/// The cached bundle and when it was downloaded.
#[derive(Clone)]
struct CachedBundle {
    bundle: Arc<ServicePolicies>,
    fetched_at: Instant,
}

/// A cached `ResolvedPolicy` behind a trait object: a compile firewall, keep it.
///
/// `ResolvedPolicy` holds row-filter `Expr`s. Stored concretely in the moka
/// cache, every `Send`/`Sync` proof of `RangerStore` (one per `async_trait`
/// method; the solver cache does not span them) walks DataFusion's whole
/// expression and plan type graph. `dyn PolicyValue` stops the walk at its
/// declared bounds. Same device as the coordinator's `StoredPlan`.
#[derive(Clone)]
struct CachedPolicy(Arc<dyn PolicyValue>);

trait PolicyValue: Send + Sync + 'static {
    fn policy(&self) -> &ResolvedPolicy;
}

impl PolicyValue for ResolvedPolicy {
    fn policy(&self) -> &ResolvedPolicy {
        self
    }
}

/// Fine-grained policy store backed by a `hive`-type Ranger service.
pub struct RangerStore {
    client: Client,
    /// Ranger Admin base URL used by SQL policy DDL.
    admin_url: String,
    /// Component service instance (for example `query`).
    service_name: String,
    /// Base download URL, e.g.
    /// ".../service/plugins/secure/policies/download/hive".
    download_url: String,
    admin_user: String,
    admin_password: SecretString,
    cache: Cache<String, CachedPolicy>,
    /// Cache of the raw, user-independent ServicePolicies bundle under a single
    /// `BUNDLE_KEY`. The bundle is the same for every user, so caching it here
    /// (with the same TTL as the per-user `cache`) avoids re-downloading and
    /// re-parsing it on every query and every tagged table. `Arc` keeps the
    /// moka `get`/`insert` clones cheap.
    bundle_cache: Cache<&'static str, CachedBundle>,
    /// Age at which a reader refreshes the bundle ahead of its TTL.
    bundle_refresh_after: Duration,
    /// Serialises bundle downloads: a cold miss waits on it, a refresh-ahead
    /// only `try_lock`s it, so at most one download is in flight.
    bundle_fetch: tokio::sync::Mutex<()>,
    /// Bumped by `invalidate_all`. A download that started before an
    /// invalidation is not cached, so it cannot put a pre-GRANT bundle back.
    bundle_generation: AtomicU64,
    /// Last `policyVersion` observed on a download. 0 means none yet.
    /// When a later download reports a different version, the per-user
    /// ResolvedPolicy cache is dropped so Ranger Admin edits take effect
    /// on the next resolve instead of waiting out a second TTL (#425).
    last_policy_version: AtomicI64,
    breaker: Arc<CircuitBreaker>,
    metrics: Option<Arc<MetricsRegistry>>,
    /// The live Hive-root registry (`ATTACH` adds to it at runtime). A policy
    /// namespace whose first component is a root name is a Hive table, and
    /// those are the tables whose access SQE decides itself.
    hive_catalogs: Arc<HiveCatalogs>,
    /// `[policy.ranger] hive_access_policies`: gate Hive-root tables on a
    /// Ranger access grant (default true).
    hive_access_policies: bool,
    /// `[policy.ranger] catalog_scoped`: the Ranger `database` of an Iceberg
    /// table is `{catalog}.{namespace}`, and the last-component fallback is off.
    catalog_scoped: bool,
}

impl RangerStore {
    pub fn from_config(cfg: &RangerPolicyConfig) -> sqe_core::Result<Self> {
        let base = cfg.url.trim_end_matches('/');
        // The SECURE download path, not `/service/plugins/policies/download/`.
        //
        // Both return the identical `ServicePolicies` bundle, but Ranger declares
        // the non-secure one `security="none"`: measured on 2.8.0, it serves the
        // complete policy set to a caller with NO credentials (HTTP 200), while the
        // secure path answers 401. Handing the whole authorization model to any
        // unauthenticated caller is a disclosure this store does not need, since it
        // already sends basic auth on this request; the credentials were simply
        // being ignored.
        //
        // Ranger 2.9.0 also stops serving the non-secure path unless
        // `ranger.admin.allow.unauthenticated.download.access` is enabled
        // (RANGER-5635 made that property honored regardless of Kerberos), so this
        // is what lets the read path work there at all.
        let download_url = format!(
            "{base}/service/plugins/secure/policies/download/{}",
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
            admin_url: base.to_string(),
            service_name: cfg.service_name.clone(),
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
            bundle_refresh_after: Duration::from_secs(cfg.cache_ttl_secs)
                .mul_f64(BUNDLE_REFRESH_AHEAD),
            bundle_fetch: tokio::sync::Mutex::new(()),
            bundle_generation: AtomicU64::new(0),
            last_policy_version: AtomicI64::new(0),
            breaker: Arc::new(CircuitBreaker::new(
                "Ranger",
                cfg.breaker_failure_threshold,
                Duration::from_secs(cfg.breaker_recovery_secs),
            )),
            metrics: None,
            hive_catalogs: Arc::new(HiveCatalogs::new()),
            hive_access_policies: cfg.hive_access_policies,
            catalog_scoped: cfg.catalog_scoped,
        })
    }

    /// Attach the coordinator's Hive-root registry, so `resolve` can tell a
    /// Hive table (`<root>.<database>` namespace, access decided here) from an
    /// Iceberg one (access decided by the catalog). Without it every table
    /// resolves as Iceberg and no access gate applies, so the coordinator
    /// wires it unconditionally.
    #[must_use = "with_hive_catalogs consumes self; bind the returned store"]
    pub fn with_hive_catalogs(mut self, hive_catalogs: Arc<HiveCatalogs>) -> Self {
        self.hive_catalogs = hive_catalogs;
        self
    }

    /// Whether `[policy.ranger] hive_access_policies` is on for this store.
    pub fn enforces_hive_access(&self) -> bool {
        self.hive_access_policies
    }

    /// A Hive-root table needs an access grant when the gate is on and the
    /// policy namespace starts with a registered root name.
    fn requires_access_grant(&self, namespace: &str) -> bool {
        self.hive_access_policies
            && self
                .hive_catalogs
                .contains_name(namespace.split('.').next())
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
            sqe_core::error::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Ranger unavailable: {e}"))
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
                sqe_core::error::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Ranger download failed: {e}"))
            })?;

        if !resp.status().is_success() {
            self.breaker.record_failure();
            self.record_metric(started, "err");
            return Err(sqe_core::error::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                "Ranger download returned status {}",
                resp.status()
            )));
        }

        let bundle: ServicePolicies = resp.json().await.map_err(|e| {
            self.breaker.record_failure();
            self.record_metric(started, "err");
            sqe_core::error::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Failed to parse Ranger bundle: {e}"))
        })?;
        // Once per download, not per query: group-only items are invisible
        // unless `groups_claim` is set, which is a config gap not a per-row miss.
        note_group_only_items(&bundle, self.metrics.as_deref());
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
    ///
    /// Concurrent misses download once (#816): the first takes `bundle_fetch`,
    /// the rest wait on it and then find the bundle cached. Past
    /// `BUNDLE_REFRESH_AHEAD` of the TTL, the one reader that wins `try_lock`
    /// downloads a fresh copy while every other reader is served the cached
    /// one; if that download fails, the cached bundle stays until its TTL.
    async fn cached_bundle(&self) -> sqe_core::Result<Arc<ServicePolicies>> {
        if let Some(cached) = self.bundle_cache.get(BUNDLE_KEY).await {
            if cached.fetched_at.elapsed() < self.bundle_refresh_after {
                return Ok(cached.bundle);
            }
            let Ok(_guard) = self.bundle_fetch.try_lock() else {
                return Ok(cached.bundle);
            };
            return match self.download_and_cache().await {
                Ok(bundle) => Ok(bundle),
                Err(e) => {
                    warn!(error = %e, "Ranger bundle refresh-ahead failed; serving the cached bundle");
                    Ok(cached.bundle)
                }
            };
        }
        let _guard = self.bundle_fetch.lock().await;
        if let Some(cached) = self.bundle_cache.get(BUNDLE_KEY).await {
            return Ok(cached.bundle);
        }
        self.download_and_cache().await
    }

    /// Download the bundle and cache it, unless `invalidate_all` ran while it
    /// was in flight. Callers hold `bundle_fetch`.
    async fn download_and_cache(&self) -> sqe_core::Result<Arc<ServicePolicies>> {
        let generation = self.bundle_generation.load(Ordering::Acquire);
        let fetched_at = Instant::now();
        let bundle = Arc::new(self.fetch_bundle().await?);
        self.drop_resolved_if_bundle_changed(&bundle);
        if self.bundle_generation.load(Ordering::Acquire) == generation {
            self.bundle_cache
                .insert(BUNDLE_KEY, CachedBundle { bundle: bundle.clone(), fetched_at })
                .await;
        }
        Ok(bundle)
    }

    /// Drop per-user ResolvedPolicy entries when Ranger's `policyVersion`
    /// moved (or is absent, so we cannot tell). GRANT through SQE already
    /// calls `invalidate_all`. This is the Admin-behind-our-back path (#425).
    fn drop_resolved_if_bundle_changed(&self, bundle: &ServicePolicies) {
        match bundle.policy_version {
            Some(version) => {
                let previous = self.last_policy_version.swap(version, Ordering::AcqRel);
                if previous != 0 && previous != version {
                    self.cache.invalidate_all();
                    warn!(
                        previous,
                        version,
                        "Ranger bundle policyVersion changed; dropped resolved policy cache"
                    );
                }
            }
            None => {
                self.cache.invalidate_all();
            }
        }
    }
}

// --- Pure resolution helpers ---

/// Flatten an Iceberg namespace to a hive `database` name. SQE namespaces are
/// already dotted multi-level strings and Kyuubi uses the same dotted
/// convention, so this is identity for now. By default the catalog is dropped
/// before this point (hive has no catalog level) and cross-engine policies are
/// written without a catalog prefix. Under `[policy.ranger] catalog_scoped` the
/// caller's key already carries it (`{catalog}.{namespace}`, see
/// `plan_rewriter::resolve_policy_key_scoped`). See
/// docs/ranger-fine-grained-service-type.md.
///
/// `plan_rewriter.rs::resolve_policy_key` passes the **whole** dotted
/// namespace as `namespace` (schema `"sales_wh.sales"` stays `"sales_wh.sales"`),
/// on every path: SELECT, DML, `information_schema`. Ranger `database`
/// resource values must be written against that full dotted name.
fn hive_database(namespace: &str) -> String {
    namespace.to_string()
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
#[cfg(test)]
fn policy_matches_table(p: &RangerPolicy, database: &str, table: &str) -> bool {
    policy_matches_table_with(p, database, table, true)
}

/// `policy_matches_table` with the last-component fallback switchable. Access
/// allow items pass `false`: over-matching a grant is the leak direction, so
/// the fallback is only safe where it can restrict (masks, filters, denies).
///
/// It is off everywhere under `catalog_scoped`: the key there is
/// `{catalog}.{namespace}`, so the last component of `ws_a.finance` is
/// `finance`, and a policy written for `finance` would fire in every catalog --
/// the cross-catalog match the setting exists to stop. A catalog-scoped
/// deployment has no old-style policies to migrate.
fn policy_matches_table_with(p: &RangerPolicy, database: &str, table: &str, last_component_fallback: bool) -> bool {
    let db_ok = p
        .resources
        .get("database")
        .map(|r| {
            if resource_matches(r, database) {
                return true;
            }
            let last = database.rsplit('.').next().unwrap_or(database);
            if last_component_fallback && last != database && resource_matches(r, last) {
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

/// True if a policy-item applies to this user, any of their roles, or any of
/// their groups. The three lists are OR-ed.
///
/// Groups come from the JWT `groups_claim` on the auth provider. A group-only
/// item (no users, no roles) cannot match when the session carries no groups,
/// which is what happens when `groups_claim` is unset. That miss is logged at
/// debug here (per item, per cache miss) and as a single warn on bundle
/// download via `note_group_only_items`.
fn item_matches(users: &[String], roles: &[String], groups: &[String], user: &SessionUser) -> bool {
    let matched = users.iter().any(|u| u == &user.username)
        || roles.iter().any(|r| user.roles.contains(r))
        || groups
            .iter()
            .any(|g| g == PUBLIC_GROUP || user.groups.contains(g));
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

/// True when an item binds only via `groups` (no users, no roles). Those items
/// never fire unless the session carries groups, which requires `groups_claim`.
fn is_group_only(users: &[String], roles: &[String], groups: &[String]) -> bool {
    users.is_empty() && roles.is_empty() && !groups.is_empty()
}

/// Count group-only mask and row-filter items across resource and tag policies.
///
/// Split out so the bundle-download warn is unit-testable without HTTP.
fn count_group_only_items(bundle: &ServicePolicies) -> usize {
    let in_policies = |policies: &[RangerPolicy]| -> usize {
        policies
            .iter()
            .map(|p| {
                // `public` is Ranger's everyone-group and matches without a
                // groups claim, so an item bound only to it is not at risk.
                let group_only = |users: &[String], roles: &[String], groups: &[String]| {
                    is_group_only(users, roles, groups) && !groups.iter().any(|g| g == PUBLIC_GROUP)
                };
                p.data_mask_policy_items
                    .iter()
                    .filter(|i| group_only(&i.users, &i.roles, &i.groups))
                    .count()
                    + p.row_filter_policy_items
                        .iter()
                        .filter(|i| group_only(&i.users, &i.roles, &i.groups))
                        .count()
                    // Access grants too: with the Hive access gate on, a
                    // group-only allow that never matches denies everyone.
                    + p.policy_items
                        .iter()
                        .chain(&p.deny_policy_items)
                        .filter(|i| group_only(&i.users, &i.roles, &i.groups))
                        .count()
            })
            .sum()
    };
    let tagged = bundle
        .tag_policies
        .as_ref()
        .map(|t| in_policies(&t.policies))
        .unwrap_or(0);
    in_policies(&bundle.policies) + tagged
}

/// Log one warn per downloaded bundle that contains group-only items, and bump
/// `sqe_ranger_group_only_items` when a registry is attached.
///
/// Returns the count so tests can assert the path without capturing logs.
fn note_group_only_items(bundle: &ServicePolicies, metrics: Option<&MetricsRegistry>) -> usize {
    let n = count_group_only_items(bundle);
    if n == 0 {
        return 0;
    }
    warn!(
        group_only_items = n,
        "Ranger bundle has {n} group-only policy item(s) (groups set, no users or roles). \
         Those items never apply unless `groups_claim` is set on the auth provider \
         so session groups are populated from the token."
    );
    if let Some(metrics) = metrics {
        metrics.ranger_group_only_items.inc_by(n as u64);
    }
    n
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
fn map_mask(
    info: &DataMaskInfo,
    column: &str,
    identity: &SessionIdentity,
) -> Result<Option<MaskType>, ()> {
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

/// True when an access-policy item carries `access_type` (or `all`) as an
/// allowed access type. Ranger writes `isAllowed: true` on deny items too
/// (the item list, not the flag, makes it a deny), so the flag only rules out
/// an item an operator toggled off.
fn item_grants_access(item: &AccessPolicyItem, access_type: &str) -> bool {
    item.accesses.iter().any(|a| {
        a.is_allowed
            && (a.access_type.eq_ignore_ascii_case(access_type)
                || a.access_type.eq_ignore_ascii_case("all"))
    })
}

/// True when an access policy's `column` resource covers the whole table.
///
/// Whole-table access is what a listing, `SELECT *` and `SHOW CREATE TABLE`
/// need, and it is what Ranger's own evaluator requires for them: a policy
/// granting `select` on `orders.amount` alone lets Hive read that one column
/// and denies `SELECT *`. SQE has no column-level access decision on this
/// path (it does not hold the schema here), so a column-scoped grant does not
/// make the table visible. Grant `*` for table access, mask the columns that
/// need it.
fn column_resource_covers_table(p: &RangerPolicy) -> bool {
    match p.resources.get("column") {
        None => true,
        Some(res) => !res.is_excludes && res.values.iter().any(|v| v == "*"),
    }
}

/// Ranger access-policy evaluation for one table: does any enabled
/// `policyType` 0 policy on this database and table grant `select` to the
/// user, with no matching deny item? See `access_grants_table_type`.
fn access_grants_table(
    bundle: &ServicePolicies,
    user: &SessionUser,
    database: &str,
    table: &str,
    last_component_fallback: bool,
) -> bool {
    access_grants_table_type(bundle, user, database, table, "select", last_component_fallback)
}

/// Ranger access-policy evaluation for one table and one access type
/// (`select`, `alter`, `drop`; `all` carries each): does any enabled
/// `policyType` 0 policy on this database and table grant it to the user,
/// with no matching deny item for it?
///
/// Order follows the `hive` servicedef evaluator: a deny item that matches
/// the user (and is not carved out by a `denyExceptions` item) decides
/// immediately, across every matching policy; only then do the allow items
/// (minus `allowExceptions`) count. Nothing matching means denied. Policy
/// priority (`policyPriority` override policies) is not modelled: an
/// override deny still denies, an override allow does not beat a normal deny
/// here, which errs on the closed side.
///
/// Deny items match with the last-namespace-component fallback (fail closed);
/// allow items need the full dotted namespace, so an allow on `sales` does not
/// grant `<root>.sales`. `last_component_fallback` is false under
/// `catalog_scoped`, which turns the fallback off for deny items too.
fn access_grants_table_type(
    bundle: &ServicePolicies,
    user: &SessionUser,
    database: &str,
    table: &str,
    access_type: &str,
    last_component_fallback: bool,
) -> bool {
    let matching = |fallback: bool| {
        bundle
            .policies
            .iter()
            .filter(|p| p.is_enabled && p.policy_type == POLICY_TYPE_ACCESS)
            .filter(|p| policy_matches_table_with(p, database, table, fallback) && column_resource_covers_table(p))
            .collect::<Vec<_>>()
    };

    let applies = |items: &[AccessPolicyItem]| {
        items.iter().any(|i| {
            item_grants_access(i, access_type) && item_matches(&i.users, &i.roles, &i.groups, user)
        })
    };

    // Items that widen access (allow items, deny exceptions) do not count when
    // they carry conditions SQE cannot evaluate. Deny items and allow
    // exceptions keep ignoring conditions, which only ever narrows access.
    let widens = |p: &RangerPolicy, items: &[AccessPolicyItem]| {
        items.iter().any(|i| {
            if !(item_grants_access(i, access_type) && item_matches(&i.users, &i.roles, &i.groups, user)) {
                return false;
            }
            if !i.conditions.is_empty() {
                warn_unevaluated_conditions(p);
                return false;
            }
            true
        })
    };

    // A policy outside its validity schedule is skipped. A schedule SQE cannot
    // evaluate (`None`) still lets a deny decide but never lets an allow grant.
    let now = Utc::now();
    for p in matching(last_component_fallback) {
        if policy_schedule_active(p, now) == Some(false) {
            continue;
        }
        if applies(&p.deny_policy_items) && !widens(p, &p.deny_exceptions) {
            debug!(policy_id = p.id, policy = %p.name, user = %user.username, table, database, "Ranger deny item matched");
            return false;
        }
    }
    for p in matching(false) {
        if policy_schedule_active(p, now) != Some(true) {
            continue;
        }
        if widens(p, &p.policy_items) && !applies(&p.allow_exceptions) {
            return true;
        }
    }
    false
}

/// Warn once per policy id (per process) that an allow item was not honoured
/// because it carries Ranger conditions.
fn warn_unevaluated_conditions(p: &RangerPolicy) {
    static WARNED: OnceLock<Mutex<HashSet<i64>>> = OnceLock::new();
    let first = WARNED
        .get_or_init(Default::default)
        .lock()
        .map_or(true, |mut seen| seen.insert(p.id));
    if first {
        warn!(
            policy_id = p.id,
            policy = %p.name,
            "Ranger allow item has conditions SQE cannot evaluate; it grants nothing (fail-closed)"
        );
    }
}

/// Whether `now` falls in the policy's `validitySchedules`: `Some(true)` when
/// there are none or one covers `now`, `Some(false)` when every schedule was
/// evaluated and none covers it, `None` when no schedule covers `now` and at
/// least one could not be evaluated (bad timestamp, unknown time zone,
/// recurrences).
fn policy_schedule_active(p: &RangerPolicy, now: DateTime<Utc>) -> Option<bool> {
    let mut unknown = false;
    for s in &p.validity_schedules {
        match schedule_covers(s, now) {
            Some(true) => return Some(true),
            Some(false) => {}
            None => unknown = true,
        }
    }
    if p.validity_schedules.is_empty() || !unknown {
        Some(p.validity_schedules.is_empty())
    } else {
        None
    }
}

fn schedule_covers(s: &ValiditySchedule, now: DateTime<Utc>) -> Option<bool> {
    if !s.recurrences.is_empty() {
        return None;
    }
    let tz: Tz = match s.time_zone.as_deref().filter(|z| !z.is_empty()) {
        Some(z) => z.parse().ok()?,
        None => Tz::UTC,
    };
    // Outer None: unparseable. Inner None: open-ended bound.
    let at = |t: &Option<String>| -> Option<Option<DateTime<Utc>>> {
        match t.as_deref().filter(|t| !t.is_empty()) {
            None => Some(None),
            Some(t) => {
                let local = NaiveDateTime::parse_from_str(t, "%Y/%m/%d %H:%M:%S").ok()?;
                Some(Some(tz.from_local_datetime(&local).earliest()?.with_timezone(&Utc)))
            }
        }
    };
    let (start, end) = (at(&s.start_time)?, at(&s.end_time)?);
    Some(start.is_none_or(|t| t <= now) && end.is_none_or(|t| now < t))
}

/// Build a `ResolvedPolicy` from an already-fetched bundle. Pure (no I/O), so it
/// is unit-tested directly and reused by `resolve()` after a cache miss.
///
/// `require_access_grant` is true for a table whose visibility SQE alone
/// decides (a Hive-root table with `hive_access_policies` on): when no access
/// policy grants `select`, the result carries the deny-all row filter that
/// every consumer already treats as "table not found". Iceberg tables pass
/// `false`, because their object-level decision belongs to the catalog.
#[cfg(test)]
fn resolve_from_bundle(
    bundle: &ServicePolicies,
    user: &SessionUser,
    table: &str,
    namespace: &str,
    require_access_grant: bool,
) -> ResolvedPolicy {
    resolve_from_bundle_with(bundle, user, table, namespace, require_access_grant, false)
}

/// The row-filter expressions one policy applies to `user`, ANDed by the
/// caller. Ranger takes the first matching item, and an empty or missing
/// `filterExpr` filters nothing (that is how an exemption item is written).
///
/// A first matching item carrying conditions SQE cannot evaluate does not
/// decide alone: the condition might be false in Ranger, handing the decision
/// to a later item. So that item and every later matching one apply, ANDed,
/// which is never less restrictive than either outcome.
fn deciding_row_filter_items<'a>(p: &'a RangerPolicy, user: &SessionUser) -> Vec<&'a str> {
    let mut exprs = Vec::new();
    for item in p
        .row_filter_policy_items
        .iter()
        .filter(|item| item_matches(&item.users, &item.roles, &item.groups, user))
    {
        if let Some(e) = item
            .row_filter_info
            .filter_expr
            .as_deref()
            .map(str::trim)
            .filter(|e| !e.is_empty())
        {
            exprs.push(e);
        }
        if item.conditions.is_empty() {
            break;
        }
        warn!(
            policy_id = p.id,
            "Ranger row-filter item has conditions SQE cannot evaluate; \
             applying it and the later matching items (fail-closed)"
        );
    }
    exprs
}

/// The enabled mask and row-filter policies that cover `database.table`, in
/// Ranger's evaluation order as far as SQE models it: `policyPriority`
/// descending (override before normal), then bundle order. The sort is stable.
///
/// Ranger breaks a priority tie with a resource-specificity heuristic
/// (`RangerOptimizedPolicyEvaluator.computeEvalOrder`: fewer wildcards first)
/// and then the policy name. SQE does not reproduce that heuristic, so two
/// same-priority mask policies naming the same column for the same user can
/// pick a different winner here than in Ranger. Do not author overlapping
/// same-priority masks; give the one that must win override priority.
fn matching_fine_grained_policies<'a>(
    bundle: &'a ServicePolicies,
    database: &str,
    table: &str,
    last_component_fallback: bool,
) -> Vec<&'a RangerPolicy> {
    let mut matching: Vec<&RangerPolicy> = bundle
        .policies
        .iter()
        .filter(|p| p.is_enabled)
        .filter(|p| p.policy_type == POLICY_TYPE_DATAMASK || p.policy_type == POLICY_TYPE_ROWFILTER)
        .filter(|p| policy_matches_table_with(p, database, table, last_component_fallback))
        .collect();
    matching.sort_by_key(|p| std::cmp::Reverse(p.policy_priority));
    matching
}

/// `resolve_from_bundle` with the `catalog_scoped` flag. Under it, `namespace`
/// is already `{catalog}.{namespace}` (see `resolve_policy_key_scoped`) and the
/// last-component fallback is off, so no policy can match across catalogs.
///
/// # Precedence (Ranger semantics, verified against Apache Ranger source)
///
/// - **Within one policy the FIRST matching item wins**, for masks and row
///   filters alike. Ranger keeps `dataMaskPolicyItems` and
///   `rowFilterPolicyItems` in the order written (`RangerDefaultPolicyEvaluator`:
///   "must be evaluated in the order given in the policy; hence no sort") and
///   takes the first item whose users, groups or roles match. So the usual
///   "everyone except group E" policy is written `[{groups:[E], MASK_NONE},
///   {groups:[public], MASK}]`, and a member of E sees the raw value.
/// - **Across mask policies the first decision for a column is final**,
///   including a `MASK_NONE` decision ("decision made earlier by a policy with
///   same priority or higher will be final", `RangerPolicyEngineImpl`). Order is
///   `matching_fine_grained_policies`.
/// - **Across row-filter policies SQE ANDs** the filter of every matching
///   policy, where Ranger applies only the first policy's. That is a deliberate,
///   more restrictive deviation: a user covered by two filter policies sees the
///   intersection, never more than Ranger would show.
/// - A matched row-filter item with no or an empty `filterExpr` filters
///   nothing, as in Ranger (`isRowFilterEnabled` is `isNotEmpty(filterExpr)`):
///   that is how an exemption item is written.
/// - A first matching item that carries conditions SQE cannot evaluate decides
///   too: its columns are restricted (fail-closed), never left to a later item.
fn resolve_from_bundle_with(
    bundle: &ServicePolicies,
    user: &SessionUser,
    table: &str,
    namespace: &str,
    require_access_grant: bool,
    catalog_scoped: bool,
) -> ResolvedPolicy {
    let database = hive_database(namespace);
    let last_component_fallback = !catalog_scoped;
    let mut policy = ResolvedPolicy::default();

    if require_access_grant
        && !access_grants_table(bundle, user, &database, table, last_component_fallback)
    {
        debug!(
            user = %user.username,
            table,
            database = %database,
            "no Ranger access policy grants select on this Hive table; denying (default deny)"
        );
        policy.row_filters.push(lit(false));
        return policy;
    }

    // Build the identity once for the whole resolution pass. database/schema
    // are None here -- RangerStore doesn't hold the session warehouse; UDFs
    // referencing current_database()/current_schema() fold to NULL (MVP).
    let identity = SessionIdentity {
        username: user.username.clone(),
        roles: user.roles.clone(),
        database: None,
        schema: None,
    };

    // Columns some earlier policy already decided for this user: masked,
    // exempted (MASK_NONE) or restricted. A later policy does not reopen them.
    let mut decided_columns: HashSet<&str> = HashSet::new();

    for p in matching_fine_grained_policies(bundle, &database, table, last_component_fallback) {
        // Data-mask policy (policyType 1). A datamask policy's `column`
        // resource can list several columns that all receive the same mask;
        // iterate ALL of them so multi-column policies don't leak.
        if p.policy_type == POLICY_TYPE_DATAMASK {
            let Some(col_res) = p.resources.get("column") else {
                continue;
            };
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
            // Ranger semantics: the first matching item of a policy decides
            // the column, and the first policy to decide a column is final.
            let Some(item) = p
                .data_mask_policy_items
                .iter()
                .find(|item| item_matches(&item.users, &item.roles, &item.groups, user))
            else {
                continue;
            };
            for column in &col_res.values {
                if !decided_columns.insert(column.as_str()) {
                    continue;
                }
                if !item.conditions.is_empty() {
                    warn!(
                        column = %column,
                        policy_id = p.id,
                        "Ranger datamask item has conditions SQE cannot evaluate; \
                         restricting column (fail-closed)"
                    );
                    if !policy.restricted_columns.contains(column) {
                        policy.restricted_columns.push(column.clone());
                    }
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

        // Row-filter policy (policyType 2): first matching item wins.
        if p.policy_type == POLICY_TYPE_ROWFILTER {
            for expr_str in deciding_row_filter_items(p, user) {
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
    user: &SessionUser,
    tags: &HashSet<String>,
) -> (
    HashMap<String, TagMaskSpec>,
    Vec<(String, Expr)>,
    HashSet<String>,
) {
    let mut masks: HashMap<String, TagMaskSpec> = HashMap::new();
    let mut filters: Vec<(String, Expr)> = Vec::new();
    let mut unmappable: HashSet<String> = HashSet::new();

    let tag_bundle = match &bundle.tag_policies {
        Some(tp) => tp,
        None => return (masks, filters, unmappable),
    };

    // Items match on the full SessionUser (groups included); the identity is
    // only the UDF-folding view used when parsing filter and mask SQL.
    let identity = &SessionIdentity {
        username: user.username.clone(),
        roles: user.roles.clone(),
        database: None,
        schema: None,
    };

    // Same precedence as the resource path (`resolve_from_bundle_with`):
    // priority first, the first matching item of a policy wins, and the first
    // policy to decide a tag's mask (MASK_NONE included) is final.
    let mut ordered: Vec<&RangerPolicy> = tag_bundle.policies.iter().filter(|p| p.is_enabled).collect();
    ordered.sort_by_key(|p| std::cmp::Reverse(p.policy_priority));
    let mut decided_tags: HashSet<&str> = HashSet::new();

    for p in ordered {
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
                    if !item_matches(&item.users, &item.roles, &item.groups, user) {
                        continue;
                    }
                    // An earlier policy already decided this tag.
                    if !decided_tags.insert(tag_value.as_str()) {
                        break;
                    }
                    if !item.conditions.is_empty() {
                        warn!(
                            tag = %tag_value,
                            policy_id = p.id,
                            "Ranger tag datamask item has conditions SQE cannot \
                             evaluate; marking tag unmappable (fail-closed)"
                        );
                        unmappable.insert(tag_value.clone());
                        break;
                    }
                    // CUSTOM masks carry a `{col}` placeholder that only the
                    // rewriter can substitute (the column name is not known here).
                    // Store the raw template as TagMaskSpec::Custom; merge_tag_masks
                    // performs the substitution and parses the expression per column.
                    // On parse failure the rewriter restricts the column (fail-closed).
                    if normalize_mask_type(&item.data_mask_info.data_mask_type) == MASK_TYPE_CUSTOM
                    {
                        if let Some(template) = &item.data_mask_info.value_expr {
                            masks
                                .entry(tag_value.clone())
                                .or_insert_with(|| TagMaskSpec::Custom(template.clone()));
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
                        break;
                    }
                    // column placeholder is empty for non-CUSTOM types.
                    match map_mask(&item.data_mask_info, "", identity) {
                        Ok(Some(mask)) => {
                            masks.entry(tag_value.clone()).or_insert(TagMaskSpec::Ready(mask));
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
                    // First matching item decides, as on the resource path.
                    break;
                }
            }

            // policyType 2: rowfilter. The first matching item decides and an
            // empty expression filters nothing; filters of several policies
            // are ANDed.
            if p.policy_type == POLICY_TYPE_ROWFILTER {
                for expr_str in deciding_row_filter_items(p, user) {
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

    (masks, filters, unmappable)
}

// --- Cache key + PolicyStore impl ---

fn cache_key(user: &SessionUser, table: &str, namespace: &str) -> String {
    let mut roles = user.roles.clone();
    roles.sort();
    roles.dedup();
    let mut groups = user.groups.clone();
    groups.sort();
    groups.dedup();
    // Debug tuple encoding preserves field/list boundaries, unlike comma joins.
    format!("{:?}", (&user.username, namespace, table, roles, groups))
}

#[derive(Debug)]
struct RangerServiceLink {
    component_type: String,
    tag_service: Option<String>,
}

impl RangerStore {
    async fn service_link(&self) -> sqe_core::Result<RangerServiceLink> {
        let url = format!(
            "{}/service/public/v2/api/service/name/{}",
            self.admin_url, self.service_name
        );
        let response = self
            .client
            .get(url)
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Ranger service lookup failed: {e}"))
            })?;
        if !response.status().is_success() {
            return Err(sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                "Ranger service lookup failed (HTTP {})",
                response.status()
            )));
        }
        let body: serde_json::Value = response.json().await.map_err(|e| {
            sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Ranger service lookup parse failed: {e}"))
        })?;
        let component_type = body
            .get("type")
            .and_then(serde_json::Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed,
                    "Ranger component service response omitted its type".to_string(),
                )
            })?
            .to_string();
        let tag_service = body
            .get("tagService")
            .and_then(serde_json::Value::as_str)
            .filter(|v| !v.is_empty())
            .map(str::to_string);
        Ok(RangerServiceLink {
            component_type,
            tag_service,
        })
    }

    async fn policies_for_service(
        &self,
        service: &str,
    ) -> sqe_core::Result<Vec<serde_json::Value>> {
        let url = format!("{}/service/public/v2/api/policy", self.admin_url);
        let response = self
            .client
            .get(url)
            .query(&[("serviceName", service)])
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Ranger policy list failed: {e}"))
            })?;
        if !response.status().is_success() {
            return Err(sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                "Ranger policy list failed (HTTP {})",
                response.status()
            )));
        }
        response.json().await.map_err(|e| {
            sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Ranger policy list parse failed: {e}"))
        })
    }

    async fn send_policy_ddl(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> sqe_core::Result<()> {
        let url = format!("{}{}", self.admin_url, path);
        let mut request = self
            .client
            .request(method, url)
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .header("X-XSRF-HEADER", "x");
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|e| {
            sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Ranger policy mutation failed: {e}"))
        })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                "Ranger policy mutation failed (HTTP {status}){}",
                if body.trim().is_empty() {
                    String::new()
                } else {
                    format!(". Ranger said: {}", body.trim())
                }
            )));
        }
        Ok(())
    }

    async fn create_policy_ddl(
        &self,
        stmt: &sqe_sql::policy_ddl::CreatePolicyStatement,
    ) -> sqe_core::Result<()> {
        let link = self.service_link().await?;
        let service = match &stmt.target {
            sqe_sql::policy_ddl::PolicyTarget::Table(_) => self.service_name.clone(),
            sqe_sql::policy_ddl::PolicyTarget::Tag(_) => link.tag_service.clone().ok_or_else(|| {
                sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                    "CREATE POLICY ON TAG requires Ranger service '{}' to have a linked tag service",
                    self.service_name
                ))
            })?,
        };
        let body = ranger_policy_body(stmt, &service, &link.component_type, self.catalog_scoped)?;
        let existing = self
            .policies_for_service(&service)
            .await?
            .into_iter()
            .find(|p| p.get("name").and_then(serde_json::Value::as_str) == Some(&stmt.name));
        match existing {
            Some(_) if !stmt.or_replace => Err(sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                "Policy '{}' is present; use CREATE OR REPLACE POLICY",
                stmt.name
            ))),
            Some(policy) => {
                let id = policy
                    .get("id")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| {
                        sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                            "Ranger policy '{}' has no numeric id",
                            stmt.name
                        ))
                    })?;
                self.send_policy_ddl(
                    reqwest::Method::PUT,
                    &format!("/service/public/v2/api/policy/{id}"),
                    Some(&body),
                )
                .await
            }
            None => {
                self.send_policy_ddl(
                    reqwest::Method::POST,
                    "/service/public/v2/api/policy",
                    Some(&body),
                )
                .await
            }
        }
    }

    async fn drop_policy_ddl(
        &self,
        stmt: &sqe_sql::policy_ddl::DropPolicyStatement,
    ) -> sqe_core::Result<()> {
        let link = self.service_link().await?;
        let mut services = vec![self.service_name.clone()];
        if let Some(tag_service) = link.tag_service {
            services.push(tag_service);
        }
        let mut ids = Vec::new();
        for service in services {
            ids.extend(
                self.policies_for_service(&service)
                    .await?
                    .into_iter()
                    .filter(|p| {
                        p.get("name").and_then(serde_json::Value::as_str) == Some(&stmt.name)
                    })
                    .filter_map(|p| p.get("id").and_then(serde_json::Value::as_i64)),
            );
        }
        if ids.is_empty() && !stmt.if_exists {
            return Err(sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!(
                "Policy '{}' not found",
                stmt.name
            )));
        }
        for id in ids {
            self.send_policy_ddl(
                reqwest::Method::DELETE,
                &format!("/service/public/v2/api/policy/{id}"),
                None,
            )
            .await?;
        }
        Ok(())
    }

    /// `GRANT ... MASKED WITH / ROWS WHERE`. Ranger holds one mask policy per
    /// column and one row-filter policy per table (a second is error 3010),
    /// so the grantee gets an item in that policy rather than a policy of
    /// its own.
    async fn set_grant_policies_impl(
        &self,
        spec: &sqe_sql::grant_policy::GrantPolicySpec,
    ) -> sqe_core::Result<()> {
        let (database, table) = ranger_table_resource(
            &spec.table,
            self.catalog_scoped,
            "GRANT ... MASKED WITH / ROWS WHERE",
        )?;
        let service = self.service_name.clone();
        let policies = self.policies_for_service(&service).await?;
        let mut names: HashSet<String> = policies
            .iter()
            .filter_map(|p| p.get("name").and_then(serde_json::Value::as_str))
            .map(str::to_ascii_lowercase)
            .collect();
        for mask in &spec.masks {
            let item = grant_item(
                &spec.principal,
                "dataMaskInfo",
                serde_json::json!({"dataMaskType": MASK_TYPE_CUSTOM, "valueExpr": mask.expression}),
            );
            self.upsert_grant_item(
                &service,
                &policies,
                &mut names,
                (&database, &table, Some(&mask.column)),
                &spec.principal,
                item,
            )
            .await?;
        }
        if let Some(filter) = &spec.row_filter {
            let item = grant_item(
                &spec.principal,
                "rowFilterInfo",
                serde_json::json!({"filterExpr": filter}),
            );
            self.upsert_grant_item(
                &service,
                &policies,
                &mut names,
                (&database, &table, None),
                &spec.principal,
                item,
            )
            .await?;
        }
        Ok(())
    }

    /// Put `item` first in the policy on this exact resource, after taking
    /// the principal out of every item there; create the policy when there
    /// is none. First is deliberate: Ranger applies the first item that
    /// matches, so a user holding this role and a broader group must get the
    /// mask written for the role. The existing policy is edited in place
    /// (read-modify-write), so whatever an admin set on it by hand survives.
    async fn upsert_grant_item(
        &self,
        service: &str,
        policies: &[serde_json::Value],
        names: &mut HashSet<String>,
        (database, table, column): (&str, &str, Option<&str>),
        principal: &sqe_sql::policy_ddl::PolicyPrincipal,
        item: serde_json::Value,
    ) -> sqe_core::Result<()> {
        let policy_type = if column.is_some() { 1 } else { 2 };
        let key = grant_items_key(policy_type);
        let existing = policies
            .iter()
            .find(|p| is_grant_signature(p, policy_type, database, table, column));
        if let Some(policy) = existing {
            let id = policy_id(policy)?;
            let mut policy = policy.clone();
            let mut items: Vec<serde_json::Value> = policy
                .get(key)
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            remove_principal_from_items(&mut items, principal);
            items.insert(0, item);
            policy[key] = serde_json::Value::Array(items);
            return self
                .send_policy_ddl(
                    reqwest::Method::PUT,
                    &format!("/service/public/v2/api/policy/{id}"),
                    Some(&policy),
                )
                .await;
        }

        let base = match column {
            Some(column) => format!("sqe-grant-mask-{database}.{table}.{column}"),
            None => format!("sqe-grant-filter-{database}.{table}"),
        };
        let mut name = base.clone();
        let mut n = 2;
        while names.contains(&name.to_ascii_lowercase()) {
            name = format!("{base}-{n}");
            n += 1;
        }
        names.insert(name.to_ascii_lowercase());
        let mut resources = serde_json::json!({
            "database": {"values": [database]},
            "table": {"values": [table]},
        });
        if let Some(column) = column {
            resources["column"] = serde_json::json!({"values": [column]});
        }
        let mut body = serde_json::json!({
            "service": service,
            "name": name,
            "isEnabled": true,
            "policyType": policy_type,
            "resources": resources,
        });
        body[key] = serde_json::json!([item]);
        self.send_policy_ddl(
            reqwest::Method::POST,
            "/service/public/v2/api/policy",
            Some(&body),
        )
        .await
    }
}

fn policy_id(policy: &serde_json::Value) -> sqe_core::Result<i64> {
    policy
        .get("id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                "Ranger policy has no numeric id".to_string(),
            )
        })
}

/// The Ranger `(database, table)` values for a dotted table name.
///
/// `catalog_scoped` keeps the catalog of `cat.ns.t` in the Ranger `database`
/// (`cat.ns`), which is the key the read path resolves under
/// `[policy.ranger] catalog_scoped`; without it the catalog is dropped, the
/// Hive/Kyuubi convention. Scoped mode refuses a two-part name: the store does
/// not know the session's catalog, and a policy written without one would
/// never match. `what` names the statement in the error.
fn ranger_table_resource(
    table: &str,
    catalog_scoped: bool,
    what: &str,
) -> sqe_core::Result<(String, String)> {
    let parts: Vec<&str> = table.split('.').map(|v| v.trim_matches('"')).collect();
    if parts.len() < 2 {
        return Err(sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::ExecutionFailed,
            format!("{what} requires at least <schema>.<table>"),
        ));
    }
    if catalog_scoped && parts.len() < 3 {
        return Err(sqe_core::SqeError::with_code(
            sqe_core::SqeErrorCode::ExecutionFailed,
            format!(
                "{what} needs <catalog>.<schema>.<table> when \
                 [policy.ranger] catalog_scoped is on"
            ),
        ));
    }
    let table_name = parts[parts.len() - 1];
    let namespace_start = usize::from(parts.len() >= 3 && !catalog_scoped);
    let namespace = parts[namespace_start..parts.len() - 1].join(".");
    Ok((namespace, table_name.to_string()))
}

/// The JSON key of a policy type's item list.
fn grant_items_key(policy_type: i64) -> &'static str {
    if policy_type == 1 {
        "dataMaskPolicyItems"
    } else {
        "rowFilterPolicyItems"
    }
}

/// Whether a resource entry of a Ranger policy is exactly one value, `want`
/// (any case, as Ranger matches), with no exclusion. A wildcard or a list is a
/// different resource signature, and not one a grant edits.
fn resource_is_exactly(policy: &serde_json::Value, key: &str, want: &str) -> bool {
    let Some(res) = policy.get("resources").and_then(|r| r.get(key)) else {
        return false;
    };
    if res.get("isExcludes").and_then(serde_json::Value::as_bool) == Some(true) {
        return false;
    }
    matches!(
        res.get("values").and_then(serde_json::Value::as_array).map(Vec::as_slice),
        Some([v]) if v.as_str().is_some_and(|v| v.eq_ignore_ascii_case(want))
    )
}

/// Whether `policy` is a row-filter (`column == None`) or column-mask policy
/// on exactly this resource: the signature Ranger allows one policy for
/// (error 3010 on a second).
fn is_grant_signature(
    policy: &serde_json::Value,
    policy_type: i64,
    database: &str,
    table: &str,
    column: Option<&str>,
) -> bool {
    policy.get("policyType").and_then(serde_json::Value::as_i64) == Some(policy_type)
        && resource_is_exactly(policy, "database", database)
        && resource_is_exactly(policy, "table", table)
        && match column {
            Some(column) => resource_is_exactly(policy, "column", column),
            None => policy
                .get("resources")
                .and_then(|r| r.get("column"))
                .is_none(),
        }
}

/// The principal-list key of a policy item for a principal kind.
fn principal_key(kind: &sqe_sql::policy_ddl::PolicyPrincipalKind) -> &'static str {
    use sqe_sql::policy_ddl::PolicyPrincipalKind;
    match kind {
        PolicyPrincipalKind::User => "users",
        PolicyPrincipalKind::Role => "roles",
        PolicyPrincipalKind::Group => "groups",
    }
}

/// Take `principal` out of every item in `items`, splitting it out of items it
/// shares, and drop the items it leaves with no principal. Returns whether it
/// was present. Items that named no principal before are kept as they are.
fn remove_principal_from_items(
    items: &mut Vec<serde_json::Value>,
    principal: &sqe_sql::policy_ddl::PolicyPrincipal,
) -> bool {
    let key = principal_key(&principal.kind);
    let mut found = false;
    items.retain_mut(|item| {
        let Some(list) = item.get_mut(key).and_then(serde_json::Value::as_array_mut) else {
            return true;
        };
        let before = list.len();
        list.retain(|v| v.as_str() != Some(principal.name.as_str()));
        if list.len() == before {
            return true;
        }
        found = true;
        ["users", "roles", "groups"].iter().any(|k| {
            item.get(*k)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|l| !l.is_empty())
        })
    });
    found
}

/// One mask or row-filter item for a single principal.
fn grant_item(
    principal: &sqe_sql::policy_ddl::PolicyPrincipal,
    info_key: &str,
    info: serde_json::Value,
) -> serde_json::Value {
    let mut item = serde_json::json!({
        "users": [],
        "roles": [],
        "groups": [],
        "accesses": [{"type": "select", "isAllowed": true}],
    });
    item[principal_key(&principal.kind)] = serde_json::json!([principal.name]);
    item[info_key] = info;
    item
}

fn ranger_policy_body(
    stmt: &sqe_sql::policy_ddl::CreatePolicyStatement,
    service: &str,
    component_type: &str,
    catalog_scoped: bool,
) -> sqe_core::Result<serde_json::Value> {
    use sqe_sql::policy_ddl::{FineGrainedPolicyKind, PolicyPrincipalKind, PolicyTarget};

    let is_tag = matches!(stmt.target, PolicyTarget::Tag(_));
    let access_type = if is_tag {
        format!("{component_type}:select")
    } else {
        "select".to_string()
    };
    let mut users = Vec::new();
    let mut roles = Vec::new();
    let mut groups = Vec::new();
    match stmt.principal.kind {
        PolicyPrincipalKind::User => users.push(stmt.principal.name.clone()),
        PolicyPrincipalKind::Role => roles.push(stmt.principal.name.clone()),
        PolicyPrincipalKind::Group => groups.push(stmt.principal.name.clone()),
    }
    let mut resources = serde_json::Map::new();
    match &stmt.target {
        PolicyTarget::Tag(tag) => {
            resources.insert("tag".into(), serde_json::json!({"values": [tag]}));
        }
        PolicyTarget::Table(table) => {
            let (namespace, table_name) =
                ranger_table_resource(table, catalog_scoped, "CREATE POLICY ON TABLE")?;
            resources.insert(
                "database".into(),
                serde_json::json!({"values": [namespace]}),
            );
            resources.insert("table".into(), serde_json::json!({"values": [table_name]}));
        }
    }
    let accesses = serde_json::json!([{"type": access_type, "isAllowed": true}]);
    let mut body = serde_json::json!({
        "service": service,
        "name": stmt.name,
        "isEnabled": true,
        "resources": resources,
    });
    match &stmt.kind {
        FineGrainedPolicyKind::ColumnMask {
            mask_type,
            column,
            expression,
        } => {
            if let Some(column) = column {
                body["resources"]["column"] = serde_json::json!({"values": [column]});
            }
            let mask_type = if is_tag && !mask_type.contains(':') {
                format!("{component_type}:{mask_type}")
            } else {
                mask_type.clone()
            };
            body["policyType"] = serde_json::json!(1);
            body["dataMaskPolicyItems"] = serde_json::json!([{
                "users": users,
                "roles": roles,
                "groups": groups,
                "accesses": accesses,
                "dataMaskInfo": {
                    "dataMaskType": mask_type,
                    "valueExpr": expression,
                }
            }]);
        }
        FineGrainedPolicyKind::RowFilter { expression } => {
            body["policyType"] = serde_json::json!(2);
            body["rowFilterPolicyItems"] = serde_json::json!([{
                "users": users,
                "roles": roles,
                "groups": groups,
                "accesses": accesses,
                "rowFilterInfo": {"filterExpr": expression},
            }]);
        }
    }
    Ok(body)
}

#[async_trait]
impl PolicyStore for RangerStore {
    /// `[policy.ranger] catalog_scoped`: callers key Iceberg tables as
    /// `{catalog}.{namespace}`, the Ranger `database` policies must name.
    fn catalog_scoped(&self) -> bool {
        self.catalog_scoped
    }

    /// Ranger publishes a monotonically increasing `policyVersion` with every
    /// bundle; the resolved-policy cache is already dropped when it changes,
    /// so it is the natural fingerprint. Before the first download nothing is
    /// known and the result cache stays off.
    fn policy_fingerprint(&self) -> Option<String> {
        match self.last_policy_version.load(Ordering::Acquire) {
            0 => None,
            v => Some(format!("ranger:{v}")),
        }
    }

    async fn create_policy(
        &self,
        stmt: &sqe_sql::policy_ddl::CreatePolicyStatement,
    ) -> sqe_core::Result<()> {
        self.create_policy_ddl(stmt).await
    }

    async fn drop_policy(
        &self,
        stmt: &sqe_sql::policy_ddl::DropPolicyStatement,
    ) -> sqe_core::Result<()> {
        self.drop_policy_ddl(stmt).await
    }

    async fn set_grant_policies(
        &self,
        spec: &sqe_sql::grant_policy::GrantPolicySpec,
    ) -> sqe_core::Result<()> {
        self.set_grant_policies_impl(spec).await
    }

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
            return Ok(cached.0.policy().clone());
        }
        self.record_cache_miss();
        let bundle = self.cached_bundle().await?;
        let policy = resolve_from_bundle_with(
            &bundle,
            user,
            table_name,
            namespace,
            self.requires_access_grant(namespace),
            self.catalog_scoped,
        );
        self.cache
            .insert(key, CachedPolicy(Arc::new(policy.clone())))
            .await;
        Ok(policy)
    }

    /// The same access-policy evaluation that gates a Hive-root SELECT
    /// (`access_grants_table`), asked for the DDL access type instead of
    /// `select`: deny items, exceptions, the full-namespace rule for allows
    /// and the whole-table column rule all apply unchanged.
    ///
    /// Not tied to `hive_access_policies`. That flag decides whether a
    /// missing `select` grant hides a table; this only ever adds DDL
    /// authority beside admin and owner, so reading the access policies
    /// whatever the flag says cannot take anything away. A bundle that will
    /// not load is an error, which the caller treats as no grant.
    async fn grants_table_privilege(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
        privilege: crate::TablePrivilege,
    ) -> sqe_core::Result<bool> {
        let bundle = self.cached_bundle().await?;
        Ok(access_grants_table_type(
            &bundle,
            user,
            &hive_database(namespace),
            table_name,
            privilege.access_type(),
            !self.catalog_scoped,
        ))
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

        let (masks, tag_filters, unmappable) = resolve_tag_policies(&bundle, user, tags);
        // Discard the tag keys from row filters — the rewriter only needs Exprs.
        let filter_exprs: Vec<Expr> = tag_filters.into_iter().map(|(_, e)| e).collect();
        (masks, filter_exprs, unmappable)
    }

    fn invalidate_all(&self) {
        self.cache.invalidate_all();
        // Also drop the shared bundle so a manual refresh does not serve a stale
        // bundle until its TTL elapses.
        self.bundle_generation.fetch_add(1, Ordering::AcqRel);
        self.bundle_cache.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn create(sql: &str) -> sqe_sql::policy_ddl::CreatePolicyStatement {
        let parsed = sqe_sql::policy_ddl::try_parse_policy_ddl(sql)
            .expect("valid policy SQL")
            .expect("policy statement");
        let sqe_sql::policy_ddl::PolicyDdlStatement::Create(stmt) = parsed else {
            panic!("expected create")
        };
        stmt
    }

    #[test]
    fn sql_resource_mask_maps_to_ranger_policy_json() {
        let stmt = create(
            "CREATE POLICY p ON TABLE sales_wh.ac.orders COLUMN MASK MASK_NULL \
             TO ROLE engineer ON COLUMN amount",
        );
        let body = ranger_policy_body(&stmt, "query", "hive", false).unwrap();
        assert_eq!(body["service"], "query");
        assert_eq!(body["policyType"], 1);
        assert_eq!(body["resources"]["database"]["values"][0], "ac");
        assert_eq!(body["resources"]["table"]["values"][0], "orders");
        assert_eq!(body["resources"]["column"]["values"][0], "amount");
        assert_eq!(body["dataMaskPolicyItems"][0]["roles"][0], "engineer");
        assert_eq!(
            body["dataMaskPolicyItems"][0]["dataMaskInfo"]["dataMaskType"],
            "MASK_NULL"
        );
    }

    #[test]
    fn sql_tag_mask_uses_component_qualified_ranger_vocabulary() {
        let stmt = create(
            "CREATE POLICY p ON TAG pii COLUMN MASK CUSTOM TO ROLE engineer \
             USING (concat('x', {col}))",
        );
        let body = ranger_policy_body(&stmt, "query_tag", "hive", false).unwrap();
        assert_eq!(body["service"], "query_tag");
        assert_eq!(body["resources"]["tag"]["values"][0], "pii");
        assert_eq!(
            body["dataMaskPolicyItems"][0]["accesses"][0]["type"],
            "hive:select"
        );
        assert_eq!(
            body["dataMaskPolicyItems"][0]["dataMaskInfo"]["dataMaskType"],
            "hive:CUSTOM"
        );
    }

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
        assert_eq!(normalize_mask_type("trino:MASK_NULL"), "trino:MASK_NULL");
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
            sp.policies[0].data_mask_policy_items[0]
                .data_mask_info
                .data_mask_type,
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
        let store = RangerStore::from_config(&cfg)
            .unwrap()
            .with_metrics(metrics.clone());

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
        assert!(
            observed >= 1,
            "fetch failure must record a resolve-duration sample"
        );
    }

    #[test]
    fn policy_cache_distinguishes_groups_and_list_boundaries() {
        let mut alice = user("alice", &["analyst"]);
        let original = cache_key(&alice, "orders", "sales");
        alice.groups = vec!["restricted".into()];
        assert_ne!(original, cache_key(&alice, "orders", "sales"));
        alice.groups = vec!["a,b".into()];
        let combined = cache_key(&alice, "orders", "sales");
        alice.groups = vec!["a".into(), "b".into()];
        assert_ne!(combined, cache_key(&alice, "orders", "sales"));
        let ordered = cache_key(&alice, "orders", "sales");
        alice.groups.reverse();
        assert_eq!(ordered, cache_key(&alice, "orders", "sales"));
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
        store
            .cache
            .insert(key, CachedPolicy(Arc::new(ResolvedPolicy::default())))
            .await;

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
            .insert(BUNDLE_KEY, CachedBundle { bundle: seeded.clone(), fetched_at: Instant::now() })
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

    /// A store whose bundle download is served by `server`, with a 100 s TTL.
    async fn store_against(server: &wiremock::MockServer) -> Arc<RangerStore> {
        Arc::new(
            RangerStore::from_config(&RangerPolicyConfig {
                url: server.uri(),
                cache_ttl_secs: 100,
                ..RangerPolicyConfig::default()
            })
            .unwrap(),
        )
    }

    async fn mount_bundle(server: &wiremock::MockServer, status: u16, delay: Duration) {
        use wiremock::matchers::{method, path_regex};
        wiremock::Mock::given(method("GET"))
            .and(path_regex("/policies/download/"))
            .respond_with(
                wiremock::ResponseTemplate::new(status)
                    .set_body_string(BUNDLE)
                    .insert_header("content-type", "application/json")
                    .set_delay(delay),
            )
            .mount(server)
            .await;
    }

    async fn downloads(server: &wiremock::MockServer) -> usize {
        server.received_requests().await.unwrap_or_default().len()
    }

    async fn concurrent_bundles(store: &Arc<RangerStore>, n: usize) -> Vec<Arc<ServicePolicies>> {
        let tasks: Vec<_> = (0..n)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move { store.cached_bundle().await.expect("bundle") })
            })
            .collect();
        let mut out = Vec::new();
        for t in tasks {
            out.push(t.await.unwrap());
        }
        out
    }

    /// #816: 50 queries arriving with no bundle cached download it once, not
    /// 50 times.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_cold_misses_download_the_bundle_once() {
        let server = wiremock::MockServer::start().await;
        mount_bundle(&server, 200, Duration::from_millis(100)).await;
        let store = store_against(&server).await;

        concurrent_bundles(&store, 50).await;
        assert_eq!(downloads(&server).await, 1);
    }

    /// #816: past 80% of the TTL one reader refreshes, every other reader is
    /// served the cached bundle without waiting, and the refresh resets the age.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_ageing_bundle_is_refreshed_once_ahead_of_expiry() {
        let server = wiremock::MockServer::start().await;
        mount_bundle(&server, 200, Duration::from_millis(100)).await;
        let store = store_against(&server).await;
        let seeded: Arc<ServicePolicies> = Arc::new(serde_json::from_str(BUNDLE).unwrap());
        let aged = Instant::now() - Duration::from_secs(90);
        store
            .bundle_cache
            .insert(BUNDLE_KEY, CachedBundle { bundle: seeded.clone(), fetched_at: aged })
            .await;

        let got = concurrent_bundles(&store, 50).await;
        assert_eq!(downloads(&server).await, 1);
        assert!(
            got.iter().filter(|b| Arc::ptr_eq(b, &seeded)).count() >= 49,
            "readers that did not refresh are served the cached bundle"
        );
        let cached = store.bundle_cache.get(BUNDLE_KEY).await.expect("refreshed bundle");
        assert!(cached.fetched_at > aged, "the refresh restarted the TTL");

        // Fresh again: no further download.
        concurrent_bundles(&store, 10).await;
        assert_eq!(downloads(&server).await, 1);
    }

    /// #816: a refresh-ahead that fails keeps serving the cached bundle; it is
    /// still within its TTL, so this is not a fail-open.
    #[tokio::test]
    async fn a_failed_refresh_ahead_serves_the_cached_bundle() {
        let server = wiremock::MockServer::start().await;
        mount_bundle(&server, 503, Duration::ZERO).await;
        let store = store_against(&server).await;
        let seeded: Arc<ServicePolicies> = Arc::new(serde_json::from_str(BUNDLE).unwrap());
        store
            .bundle_cache
            .insert(
                BUNDLE_KEY,
                CachedBundle { bundle: seeded.clone(), fetched_at: Instant::now() - Duration::from_secs(90) },
            )
            .await;

        let got = store.cached_bundle().await.expect("cached bundle served");
        assert!(Arc::ptr_eq(&got, &seeded));
        assert_eq!(downloads(&server).await, 1);
    }

    /// A download in flight when `invalidate_all` runs (a GRANT through SQE)
    /// is returned to its caller but not cached, so the next query downloads
    /// the post-GRANT bundle.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_download_racing_invalidate_all_is_not_cached() {
        let server = wiremock::MockServer::start().await;
        mount_bundle(&server, 200, Duration::from_millis(300)).await;
        let store = store_against(&server).await;

        let in_flight = {
            let store = store.clone();
            tokio::spawn(async move { store.cached_bundle().await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        PolicyStore::invalidate_all(store.as_ref());
        in_flight.await.unwrap().expect("bundle");

        assert!(store.bundle_cache.get(BUNDLE_KEY).await.is_none());
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
        assert!(
            resource_matches(&r("orders*"), "orders"),
            "* may match nothing"
        );
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
        assert!(
            !resource_matches(&excl, "tmp_scratch"),
            "excluded by the glob"
        );
        assert!(resource_matches(&excl, "orders"), "outside the exclusion");

        // An empty values list matches nothing.
        let empty = RangerResource {
            values: vec![],
            is_excludes: false,
        };
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
            item_matches(
                &empty,
                &empty,
                &g,
                &user_with_groups("bob", &[], &["data-platform"])
            ),
            "a group the session carries must match"
        );
        assert!(
            !item_matches(
                &empty,
                &empty,
                &g,
                &user_with_groups("bob", &[], &["other-group"])
            ),
            "a group the session does not carry must not match"
        );
        assert!(
            !item_matches(
                &empty,
                &empty,
                &g,
                &user_with_groups("bob", &["analyst"], &[])
            ),
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

    fn db_table_policy(database: &str, table: &str) -> RangerPolicy {
        let mut p = RangerPolicy::default();
        p.resources.insert(
            "database".to_string(),
            RangerResource {
                values: vec![database.to_string()],
                is_excludes: false,
            },
        );
        p.resources.insert(
            "table".to_string(),
            RangerResource {
                values: vec![table.to_string()],
                is_excludes: false,
            },
        );
        p
    }

    #[test]
    fn last_component_fallback_matches_and_collides_across_tenants() {
        let policy = db_table_policy("finance", "orders");
        assert!(
            policy_matches_table(&policy, "finance", "orders"),
            "exact last-component key still matches"
        );
        assert!(
            policy_matches_table(&policy, "tenant_a.finance", "orders"),
            "full namespace must still match a last-component policy"
        );
        assert!(
            policy_matches_table(&policy, "tenant_b.finance", "orders"),
            "the same last-component policy also hits the other tenant (collision)"
        );
        assert!(
            !policy_matches_table(&policy, "tenant_a.other", "orders"),
            "a different last component must not match"
        );
    }

    /// 902 follow-up: no fingerprint before the first bundle, then the
    /// Ranger policyVersion.
    #[test]
    fn policy_fingerprint_follows_bundle_version() {
        let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
        assert!(store.policy_fingerprint().is_none());
        store.last_policy_version.store(7, Ordering::Relaxed);
        assert_eq!(store.policy_fingerprint().as_deref(), Some("ranger:7"));
    }

    #[tokio::test]
    async fn new_bundle_version_drops_resolved_policy_cache() {
        let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
        let u = user("alice", &["analyst"]);
        let key = cache_key(&u, "orders", "sales");
        store
            .cache
            .insert(key.clone(), CachedPolicy(Arc::new(ResolvedPolicy::default())))
            .await;
        store.last_policy_version.store(1, Ordering::Relaxed);

        store.drop_resolved_if_bundle_changed(&ServicePolicies {
            policy_version: Some(2),
            ..ServicePolicies::default()
        });
        store.cache.run_pending_tasks().await;
        assert!(
            store.cache.get(&key).await.is_none(),
            "a newer policyVersion must drop the resolved cache"
        );
    }

    #[tokio::test]
    async fn same_bundle_version_keeps_resolved_policy_cache() {
        let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
        let u = user("alice", &["analyst"]);
        let key = cache_key(&u, "orders", "sales");
        store
            .cache
            .insert(key.clone(), CachedPolicy(Arc::new(ResolvedPolicy::default())))
            .await;
        store.last_policy_version.store(7, Ordering::Relaxed);

        store.drop_resolved_if_bundle_changed(&ServicePolicies {
            policy_version: Some(7),
            ..ServicePolicies::default()
        });
        store.cache.run_pending_tasks().await;
        assert!(
            store.cache.get(&key).await.is_some(),
            "an unchanged policyVersion must keep the resolved cache"
        );
    }

    #[test]
    fn mask_null_maps_to_nullify() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(matches!(
            policy.column_masks.get("amount"),
            Some(MaskType::Nullify)
        ));
    }

    #[test]
    fn first_matching_mask_item_wins() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        // A second item also matching `analyst`, after the MASK_NULL one.
        let later: DataMaskPolicyItem = serde_json::from_str(
            r#"{"roles": ["analyst"], "dataMaskInfo": {"dataMaskType": "MASK_SHOW_LAST_4"}}"#,
        )
        .unwrap();
        bundle.policies[0].data_mask_policy_items.push(later);
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(
            matches!(policy.column_masks.get("amount"), Some(MaskType::Nullify)),
            "the first matching item decides, a later one must not override it"
        );
    }

    #[test]
    fn row_filter_applied_for_matching_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert_eq!(policy.row_filters.len(), 1);
    }

    #[test]
    fn no_match_for_other_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(&bundle, &user("bob", &["engineer"]), "orders", "sales", false);
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    #[test]
    fn user_match_works_too() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0].roles.clear();
        bundle.policies[0].data_mask_policy_items[0].users = vec!["alice".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &[]), "orders", "sales", false);
        assert!(policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn unsupported_mask_restricts_column_failclosed() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "MASK_FUTURE_UNSUPPORTED".to_string();
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
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
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
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
        bundle.policies[0]
            .resources
            .get_mut("column")
            .unwrap()
            .is_excludes = true;
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
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
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(!policy.column_masks.contains_key("amount"));
        assert!(!policy.restricted_columns.contains(&"amount".to_string()));
    }

    #[test]
    fn disabled_policy_is_skipped() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].is_enabled = false; // the datamask policy
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(policy.column_masks.is_empty());
    }

    #[test]
    fn wrong_table_does_not_match() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy =
            resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "customers", "sales", false);
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    #[test]
    fn unparseable_row_filter_fails_closed() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[1].row_filter_policy_items[0]
            .row_filter_info
            .filter_expr = Some("this is not sql !!!".to_string());
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
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
        bundle.policies[0]
            .resources
            .get_mut("column")
            .unwrap()
            .values = vec!["amount".to_string(), "discount".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(policy.column_masks.contains_key("amount"));
        assert!(policy.column_masks.contains_key("discount"));
    }

    #[test]
    fn wildcard_table_matches() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0]
            .resources
            .get_mut("table")
            .unwrap()
            .values = vec!["*".to_string()];
        let policy =
            resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "anything", "sales", false);
        assert!(policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn excludes_inverts_match() {
        // is_excludes on table should make "orders" NOT match a values=["orders"] exclude.
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let tr = bundle.policies[0].resources.get_mut("table").unwrap();
        tr.is_excludes = true; // exclude "orders"
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(policy.column_masks.is_empty());
    }

    #[test]
    fn custom_mask_substitutes_column() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let mi = &mut bundle.policies[0].data_mask_policy_items[0].data_mask_info;
        mi.data_mask_type = "CUSTOM".to_string();
        mi.value_expr = Some("concat('x', {col})".to_string());
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        match policy.column_masks.get("amount") {
            Some(crate::MaskType::Custom(e)) => {
                let s = datafusion::sql::unparser::expr_to_sql(e)
                    .unwrap()
                    .to_string()
                    .to_lowercase();
                assert!(
                    s.contains("amount"),
                    "custom expr must reference the real column: {s}"
                );
            }
            other => panic!("expected Custom mask, got {other:?}"),
        }
    }

    #[test]
    fn group_only_item_does_not_match_empty_session_groups() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let item = &mut bundle.policies[0].data_mask_policy_items[0];
        item.roles.clear();
        item.users.clear();
        item.groups = vec!["analysts_group".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(
            policy.column_masks.is_empty(),
            "group-only item must not match a session with empty groups \
             (groups_claim unset)"
        );
    }

    #[test]
    fn group_only_item_matches_when_session_has_groups() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let item = &mut bundle.policies[0].data_mask_policy_items[0];
        item.roles.clear();
        item.users.clear();
        item.groups = vec!["analysts_group".to_string()];
        let policy = resolve_from_bundle(
            &bundle,
            &user_with_groups("alice", &[], &["analysts_group"]),
            "orders",
            "sales",
            false,
        );
        assert!(
            policy.column_masks.contains_key("amount"),
            "group-only item must fire when the session carries that group"
        );
    }

    #[test]
    fn count_group_only_items_is_the_bundle_warn_hook() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        assert_eq!(count_group_only_items(&bundle), 0);
        assert_eq!(note_group_only_items(&bundle, None), 0);

        bundle.policies[0].data_mask_policy_items[0].roles.clear();
        bundle.policies[0].data_mask_policy_items[0].users.clear();
        bundle.policies[0].data_mask_policy_items[0].groups = vec!["analysts_group".into()];
        assert_eq!(count_group_only_items(&bundle), 1);

        // An item that also has roles is not group-only.
        bundle.policies[1].row_filter_policy_items[0].groups = vec!["analysts_group".into()];
        assert_eq!(count_group_only_items(&bundle), 1);

        let metrics = MetricsRegistry::new().unwrap();
        assert_eq!(note_group_only_items(&bundle, Some(&metrics)), 1);
        assert_eq!(metrics.ranger_group_only_items.get(), 1);
        // A second note (another download of the same bundle) bumps again.
        assert_eq!(note_group_only_items(&bundle, Some(&metrics)), 1);
        assert_eq!(metrics.ranger_group_only_items.get(), 2);
    }

    // --- Hive-root access gate (policyType 0) ---

    /// Access policies on a Glue root `glue`, database `bi`. Table names are
    /// fixture names, not anything real.
    const ACCESS_BUNDLE: &str = r#"{
      "policyVersion": 3,
      "policies": [
        {"id":1,"name":"bi-read","policyType":0,"isEnabled":true,
         "resources":{"database":{"values":["glue.bi"]},"table":{"values":["*"]},"column":{"values":["*"]}},
         "policyItems":[{"groups":["all-employees"],"accesses":[{"type":"select","isAllowed":true}]}]},
        {"id":2,"name":"bi-secret-deny","policyType":0,"isEnabled":true,
         "resources":{"database":{"values":["glue.bi"]},"table":{"values":["secret_*"]},"column":{"values":["*"]}},
         "denyPolicyItems":[{"groups":["all-employees"],"accesses":[{"type":"select","isAllowed":true}]}],
         "denyExceptions":[{"groups":["sensitive-readers"],"accesses":[{"type":"select","isAllowed":true}]}]},
        {"id":3,"name":"bi-partial-columns","policyType":0,"isEnabled":true,
         "resources":{"database":{"values":["glue.bi"]},"table":{"values":["partial"]},"column":{"values":["amount"]}},
         "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
        {"id":4,"name":"bi-public","policyType":0,"isEnabled":true,
         "resources":{"database":{"values":["glue.bi"]},"table":{"values":["open_t"]}},
         "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
        {"id":5,"name":"bi-disabled","policyType":0,"isEnabled":false,
         "resources":{"database":{"values":["glue.bi"]},"table":{"values":["hidden"]}},
         "policyItems":[{"groups":["public"],"accesses":[{"type":"all","isAllowed":true}]}]},
        {"id":6,"name":"bi-admin-except-eve","policyType":0,"isEnabled":true,
         "resources":{"database":{"values":["glue.bi"]},"table":{"values":["*"]}},
         "policyItems":[{"roles":["admin"],"accesses":[{"type":"all","isAllowed":true}]}],
         "allowExceptions":[{"users":["eve"],"accesses":[{"type":"all","isAllowed":true}]}]},
        {"id":7,"name":"bi-mask-ssn","policyType":1,"isEnabled":true,
         "resources":{"database":{"values":["glue.bi"]},"table":{"values":["*"]},"column":{"values":["ssn"]}},
         "dataMaskPolicyItems":[{"groups":["all-employees"],"dataMaskInfo":{"dataMaskType":"MASK_NULL"}}]}
      ]
    }"#;

    fn access_bundle() -> ServicePolicies {
        serde_json::from_str(ACCESS_BUNDLE).unwrap()
    }

    fn gated(user: &SessionUser, table: &str, namespace: &str) -> ResolvedPolicy {
        resolve_from_bundle(&access_bundle(), user, table, namespace, true)
    }

    #[test]
    fn a_hive_table_with_no_grant_is_denied() {
        let policy = gated(&user_with_groups("alice", &[], &["all-employees"]), "orders", "glue.other");
        assert!(crate::plan_rewriter::policy_denies_all(&policy), "no policy names glue.other");
        // And a user with no groups at all gets nothing from a group-bound grant.
        let policy = gated(&user("nobody", &[]), "orders", "glue.bi");
        assert!(crate::plan_rewriter::policy_denies_all(&policy));
    }

    #[test]
    fn a_group_grant_opens_the_table_and_masks_still_apply() {
        let policy = gated(&user_with_groups("alice", &[], &["all-employees"]), "orders", "glue.bi");
        assert!(!crate::plan_rewriter::policy_denies_all(&policy));
        assert!(policy.row_filters.is_empty());
        assert!(matches!(policy.column_masks.get("ssn"), Some(MaskType::Nullify)), "the mask policy still lands");
    }

    #[test]
    fn a_deny_item_beats_the_allow_unless_excepted() {
        let general = user_with_groups("alice", &[], &["all-employees"]);
        assert!(crate::plan_rewriter::policy_denies_all(&gated(&general, "secret_forecast", "glue.bi")));
        // Same person, also in the exception group: the deny is carved out and
        // policy 1 grants.
        let sensitive = user_with_groups("alice", &[], &["all-employees", "sensitive-readers"]);
        assert!(!crate::plan_rewriter::policy_denies_all(&gated(&sensitive, "secret_forecast", "glue.bi")));
    }

    #[test]
    fn a_column_scoped_grant_does_not_open_the_table() {
        let policy = gated(&user("carol", &[]), "partial", "glue.bi");
        assert!(crate::plan_rewriter::policy_denies_all(&policy), "select on one column is not table access");
    }

    #[test]
    fn public_grants_every_authenticated_user() {
        let policy = gated(&user("carol", &[]), "open_t", "glue.bi");
        assert!(!crate::plan_rewriter::policy_denies_all(&policy));
    }

    #[test]
    fn a_disabled_policy_grants_nothing() {
        assert!(crate::plan_rewriter::policy_denies_all(&gated(&user("carol", &[]), "hidden", "glue.bi")));
    }

    #[test]
    fn allow_exceptions_carve_a_user_out_of_a_role_grant() {
        assert!(!crate::plan_rewriter::policy_denies_all(&gated(&user("dan", &["admin"]), "anything", "glue.bi")));
        assert!(crate::plan_rewriter::policy_denies_all(&gated(&user("eve", &["admin"]), "anything", "glue.bi")));
    }

    /// DDL authority comes from the same access policies as `select`, asked
    /// for `alter`/`drop`: `all` carries both, a `select`-only grant carries
    /// neither, and a deny on `all` or an allow exception still wins.
    #[tokio::test]
    async fn ddl_privileges_come_from_the_access_policies() {
        use crate::TablePrivilege::{Alter, Drop};
        let ddl: ServicePolicies = serde_json::from_str(
            r#"{"policies":[
             {"id":1,"name":"ops-alter","policyType":0,"isEnabled":true,
              "resources":{"database":{"values":["lake.sales"]},"table":{"values":["orders"]}},
              "policyItems":[{"roles":["ops"],"accesses":[{"type":"alter","isAllowed":true}]}]},
             {"id":2,"name":"owners-all","policyType":0,"isEnabled":true,
              "resources":{"database":{"values":["lake.sales"]},"table":{"values":["*"]}},
              "policyItems":[{"roles":["stewards"],"accesses":[{"type":"all","isAllowed":true}]}],
              "allowExceptions":[{"users":["eve"],"accesses":[{"type":"all","isAllowed":true}]}]},
             {"id":3,"name":"readers","policyType":0,"isEnabled":true,
              "resources":{"database":{"values":["lake.sales"]},"table":{"values":["*"]}},
              "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
             {"id":4,"name":"no-drop-locked","policyType":0,"isEnabled":true,
              "resources":{"database":{"values":["lake.sales"]},"table":{"values":["locked"]}},
              "denyPolicyItems":[{"roles":["stewards"],"accesses":[{"type":"drop","isAllowed":true}]}]}]}"#,
        )
        .unwrap();
        let store = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
        store
            .bundle_cache
            .insert(BUNDLE_KEY, CachedBundle { bundle: Arc::new(ddl), fetched_at: Instant::now() })
            .await;
        let can = |u: SessionUser, t: &'static str, p| {
            let store = &store;
            async move {
                store
                    .grants_table_privilege(&u, t, "lake.sales", p)
                    .await
                    .unwrap()
            }
        };

        assert!(can(user("olga", &["ops"]), "orders", Alter).await);
        assert!(
            !can(user("olga", &["ops"]), "orders", Drop).await,
            "alter does not carry drop"
        );
        assert!(
            can(user("sam", &["stewards"]), "orders", Drop).await,
            "all carries drop"
        );
        assert!(
            !can(user("eve", &["stewards"]), "orders", Drop).await,
            "allow exception"
        );
        assert!(
            !can(user("carol", &[]), "orders", Alter).await,
            "select is not DDL"
        );
        assert!(
            !can(user("sam", &["stewards"]), "locked", Drop).await,
            "deny wins"
        );
        assert!(
            can(user("sam", &["stewards"]), "locked", Alter).await,
            "the deny names drop only"
        );
    }

    #[test]
    fn without_the_gate_the_table_is_open() {
        let policy = resolve_from_bundle(&access_bundle(), &user("nobody", &[]), "orders", "glue.other", false);
        assert!(!crate::plan_rewriter::policy_denies_all(&policy));
    }

    #[test]
    fn the_gate_applies_to_registered_hive_roots_only() {
        let roots = Arc::new(HiveCatalogs::new());
        roots.insert("glue");
        let store = RangerStore::from_config(&RangerPolicyConfig {
            url: "http://ranger:6080".into(),
            ..RangerPolicyConfig::default()
        })
        .unwrap()
        .with_hive_catalogs(roots.clone());
        assert!(store.enforces_hive_access(), "on by default");
        assert!(store.requires_access_grant("glue.bi"), "Hive root namespace");
        assert!(!store.requires_access_grant("sales"), "Iceberg namespace: the catalog decides");
        assert!(!store.requires_access_grant("a.b.sales"));

        let off = RangerStore::from_config(&RangerPolicyConfig {
            url: "http://ranger:6080".into(),
            hive_access_policies: false,
            ..RangerPolicyConfig::default()
        })
        .unwrap()
        .with_hive_catalogs(roots);
        assert!(!off.requires_access_grant("glue.bi"));
    }

    #[test]
    fn last_component_fallback_restricts_but_never_grants_access() {
        let allow_short: ServicePolicies = serde_json::from_str(
            r#"{"policies":[{"id":1,"name":"sales-allow","policyType":0,"isEnabled":true,
             "resources":{"database":{"values":["sales"]},"table":{"values":["*"]}},
             "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]}]}"#,
        )
        .unwrap();
        let carol = user("carol", &[]);
        assert!(
            crate::plan_rewriter::policy_denies_all(&resolve_from_bundle(&allow_short, &carol, "t", "root.sales", true)),
            "an allow on `sales` must not grant `root.sales`"
        );
        assert!(!crate::plan_rewriter::policy_denies_all(&resolve_from_bundle(&allow_short, &carol, "t", "sales", true)));

        let deny_short: ServicePolicies = serde_json::from_str(
            r#"{"policies":[
             {"id":1,"name":"root-sales-allow","policyType":0,"isEnabled":true,
              "resources":{"database":{"values":["root.sales"]},"table":{"values":["*"]}},
              "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
             {"id":2,"name":"sales-deny","policyType":0,"isEnabled":true,
              "resources":{"database":{"values":["sales"]},"table":{"values":["*"]}},
              "denyPolicyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]}]}"#,
        )
        .unwrap();
        assert!(
            crate::plan_rewriter::policy_denies_all(&resolve_from_bundle(&deny_short, &carol, "t", "root.sales", true)),
            "a deny on `sales` still denies `root.sales`"
        );
    }

    #[test]
    fn group_only_access_items_are_counted_but_public_is_not() {
        // Policy 1's allow and policy 2's deny bind to a real group with no
        // users or roles, as does policy 7's mask item; the public-bound
        // items (3, 4, 5), the exceptions and the role/user items (6) are not
        // at risk.
        assert_eq!(count_group_only_items(&access_bundle()), 3);
    }

    /// One public `select` access policy on `glue.bi.t`, with `extra` JSON
    /// spliced into the policy object and `item_extra` into its allow item.
    fn one_policy_bundle(extra: &str, item_extra: &str) -> ServicePolicies {
        serde_json::from_str(&format!(
            r#"{{"policies":[{{"id":42,"name":"p","policyType":0,"isEnabled":true{extra},
              "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
              "policyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]{item_extra}}}]}}]}}"#
        ))
        .unwrap()
    }

    fn denied(bundle: &ServicePolicies) -> bool {
        crate::plan_rewriter::policy_denies_all(&resolve_from_bundle(bundle, &user("carol", &[]), "t", "glue.bi", true))
    }

    #[test]
    fn an_allow_item_with_conditions_grants_nothing() {
        assert!(!denied(&one_policy_bundle("", "")), "control: the plain item grants");
        let conditional = one_policy_bundle("", r#","conditions":[{"type":"ip-range","values":["10.0.0.0/8"]}]"#);
        assert!(denied(&conditional), "an unevaluated condition must not grant");
        assert!(!denied(&one_policy_bundle("", r#","conditions":[]"#)), "an empty list is no condition");
    }

    /// A deny exception widens access, so one carrying conditions SQE cannot
    /// evaluate does not carve the user out of the deny.
    #[test]
    fn a_deny_exception_with_conditions_does_not_exempt() {
        let bundle = |exception_extra: &str| -> ServicePolicies {
            serde_json::from_str(&format!(
                r#"{{"policies":[{{"id":1,"policyType":0,"isEnabled":true,
                  "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
                  "policyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}],
                  "denyPolicyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}],
                  "denyExceptions":[{{"users":["carol"],"accesses":[{{"type":"select","isAllowed":true}}]{exception_extra}}}]}}]}}"#
            ))
            .unwrap()
        };
        assert!(!denied(&bundle("")), "control: the plain exception exempts carol");
        assert!(
            denied(&bundle(r#","conditions":[{"type":"ip-range","values":["10.0.0.0/8"]}]"#)),
            "an unevaluated condition must not exempt from a deny"
        );
    }

    /// The first matching datamask item decides; when it carries conditions
    /// SQE cannot evaluate, the column is restricted instead of falling
    /// through to a later item (here a `MASK_NONE` that would show it raw).
    #[test]
    fn a_conditional_mask_item_restricts_the_column() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let conditional: DataMaskPolicyItem = serde_json::from_str(
            r#"{"roles": ["analyst"], "conditions": [{"type": "ip-range", "values": ["10.0.0.0/8"]}],
                "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}"#,
        )
        .unwrap();
        let unmask: DataMaskPolicyItem = serde_json::from_str(
            r#"{"roles": ["analyst"], "dataMaskInfo": {"dataMaskType": "MASK_NONE"}}"#,
        )
        .unwrap();
        bundle.policies[0].data_mask_policy_items = vec![conditional, unmask];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales", false);
        assert!(policy.restricted_columns.contains(&"amount".to_string()));
        assert!(!policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn a_conditional_tag_mask_item_makes_the_tag_unmappable() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0].conditions =
            vec![serde_json::json!({"type": "ip-range", "values": ["10.0.0.0/8"]})];
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let (masks, _, unmappable) = resolve_tag_policies(&sp, &user("bob", &["engineer"]), &tags);
        assert!(!masks.contains_key("PII"));
        assert!(unmappable.contains("PII"));
    }

    #[test]
    fn validity_schedules_gate_the_allow() {
        let sched = |s: &str| one_policy_bundle(&format!(r#","validitySchedules":[{s}]"#), "");
        assert!(denied(&sched(r#"{"startTime":"2000/01/01 00:00:00","endTime":"2001/01/01 00:00:00","timeZone":"UTC"}"#)), "expired");
        assert!(denied(&sched(r#"{"startTime":"2999/01/01 00:00:00"}"#)), "not started");
        assert!(!denied(&sched(r#"{"startTime":"2000/01/01 00:00:00","endTime":"2999/01/01 00:00:00","timeZone":"Europe/Amsterdam"}"#)), "in force");
        assert!(denied(&sched(r#"{"endTime":"not a date"}"#)), "unparseable schedule fails closed");
        assert!(denied(&sched(r#"{"endTime":"2999/01/01 00:00:00","timeZone":"Mars/Olympus"}"#)), "unknown zone fails closed");
        assert!(denied(&sched(r#"{"recurrences":[{"interval":{"hours":1}}]}"#)), "recurrences are not modelled");
        // Any one in-force schedule is enough.
        assert!(!denied(&sched(r#"{"endTime":"bad"},{"endTime":"2999/01/01 00:00:00"}"#)));
    }

    #[test]
    fn an_expired_deny_no_longer_denies_but_an_unparseable_one_still_does() {
        let bundle = |sched: &str| -> ServicePolicies {
            serde_json::from_str(&format!(
                r#"{{"policies":[
                  {{"id":1,"policyType":0,"isEnabled":true,
                    "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
                    "policyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}]}},
                  {{"id":2,"policyType":0,"isEnabled":true,"validitySchedules":[{sched}],
                    "resources":{{"database":{{"values":["glue.bi"]}},"table":{{"values":["t"]}}}},
                    "denyPolicyItems":[{{"groups":["public"],"accesses":[{{"type":"select","isAllowed":true}}]}}]}}]}}"#
            ))
            .unwrap()
        };
        assert!(!denied(&bundle(r#"{"endTime":"2001/01/01 00:00:00"}"#)));
        assert!(denied(&bundle(r#"{"endTime":"garbage"}"#)));
        assert!(denied(&bundle(r#"{"endTime":"2999/01/01 00:00:00"}"#)));
    }

    #[test]
    fn schedule_bounds_are_read_in_the_schedule_time_zone() {
        // 2026-06-01 10:00 UTC is 12:00 in Amsterdam (CEST, UTC+2).
        let now = Utc.with_ymd_and_hms(2026, 6, 1, 10, 0, 0).unwrap();
        let s = |start: &str, tz: &str| ValiditySchedule {
            start_time: Some(start.into()),
            time_zone: Some(tz.into()),
            ..Default::default()
        };
        assert_eq!(schedule_covers(&s("2026/06/01 11:00:00", "UTC"), now), Some(false));
        assert_eq!(schedule_covers(&s("2026/06/01 11:00:00", "Europe/Amsterdam"), now), Some(true));
        assert_eq!(schedule_covers(&s("2026-06-01 11:00:00", "UTC"), now), None);
    }

    // --- map_mask arm tests ---

    #[test]
    fn maps_show_last_4() {
        let info = DataMaskInfo {
            data_mask_type: "MASK_SHOW_LAST_4".into(),
            ..Default::default()
        };
        match map_mask(&info, "ssn", &SessionIdentity::default()) {
            Ok(Some(MaskType::PartialMask {
                show_last: 4,
                show_first: 0,
                ..
            })) => {}
            other => panic!("expected show-last-4 PartialMask, got {other:?}"),
        }
    }

    #[test]
    fn maps_show_first_4() {
        let info = DataMaskInfo {
            data_mask_type: "MASK_SHOW_FIRST_4".into(),
            ..Default::default()
        };
        assert!(matches!(
            map_mask(&info, "ssn", &SessionIdentity::default()),
            Ok(Some(MaskType::PartialMask {
                show_first: 4,
                show_last: 0,
                ..
            }))
        ));
    }

    #[test]
    fn maps_full_mask_uses_hive_default_chars() {
        let info = DataMaskInfo {
            data_mask_type: "MASK".into(),
            ..Default::default()
        };
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
        let info = DataMaskInfo {
            data_mask_type: "MASK_DATE_SHOW_YEAR".into(),
            ..Default::default()
        };
        assert!(matches!(
            map_mask(&info, "hired_at", &SessionIdentity::default()),
            Ok(Some(MaskType::DateShowYear))
        ));
    }

    #[test]
    fn truly_unknown_mask_is_err() {
        let info = DataMaskInfo {
            data_mask_type: "MASK_FUTURE_UNSUPPORTED".into(),
            ..Default::default()
        };
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
        let id = user("bob", &["engineer"]);
        let (masks, filters, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        // tag PII -> a PartialMask (MASK_SHOW_LAST_4) for engineer, wrapped in TagMaskSpec::Ready
        assert!(masks.contains_key("PII"));
        assert!(
            matches!(
                masks.get("PII"),
                Some(TagMaskSpec::Ready(crate::MaskType::PartialMask {
                    show_last: 4,
                    ..
                }))
            ),
            "supported mask must be wrapped in TagMaskSpec::Ready"
        );
        assert!(
            unmappable.is_empty(),
            "supported mask must not be unmappable"
        );
        let _ = filters; // not the focus of this test
    }

    #[test]
    fn first_matching_tag_mask_item_wins() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let later: DataMaskPolicyItem = serde_json::from_str(
            r#"{"roles": ["engineer"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}"#,
        )
        .unwrap();
        sp.tag_policies.as_mut().unwrap().policies[0]
            .data_mask_policy_items
            .push(later);
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = user("bob", &["engineer"]);
        let (masks, _, _) = resolve_tag_policies(&sp, &id, &tags);
        assert!(
            matches!(
                masks.get("PII"),
                Some(TagMaskSpec::Ready(crate::MaskType::PartialMask { show_last: 4, .. }))
            ),
            "the first matching item (MASK_SHOW_LAST_4) decides, not the later MASK_NULL"
        );
    }

    #[test]
    fn tag_mask_not_resolved_for_other_role() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = user("x", &["other"]);
        let (masks, _f, _u) = resolve_tag_policies(&sp, &id, &tags);
        assert!(masks.is_empty());
    }

    #[test]
    fn tag_row_filter_resolved() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["RESTRICTED".to_string()].into_iter().collect();
        let id = user("a", &["analyst"]);
        let (_m, filters, _u) = resolve_tag_policies(&sp, &id, &tags);
        assert_eq!(filters.len(), 1); // one (tag, Expr) row filter
    }

    /// Group-bound tag items must match on the session's groups: dropping
    /// them would leave a group-scoped tag mask or row filter unapplied.
    #[test]
    fn tag_policies_match_on_groups() {
        let sp: ServicePolicies = serde_json::from_str(
            r#"{
              "policyVersion": 1, "policies": [],
              "tagPolicies": { "serviceName": "tag", "policies": [
                { "id": 1, "policyType": 1, "isEnabled": true,
                  "resources": { "tag": { "values": ["PII"] } },
                  "dataMaskPolicyItems": [
                    { "groups": ["contractors"],
                      "dataMaskInfo": { "dataMaskType": "MASK_NULL" } } ] },
                { "id": 2, "policyType": 2, "isEnabled": true,
                  "resources": { "tag": { "values": ["PII"] } },
                  "rowFilterPolicyItems": [
                    { "groups": ["contractors"],
                      "rowFilterInfo": { "filterExpr": "region = 'EU'" } } ] }
              ] }
            }"#,
        )
        .unwrap();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = user_with_groups("carol", &[], &["contractors"]);
        let (masks, filters, _u) = resolve_tag_policies(&sp, &id, &tags);
        assert!(matches!(masks.get("PII"), Some(TagMaskSpec::Ready(crate::MaskType::Nullify))));
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn untagged_yields_nothing() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = HashSet::new();
        let id = user("", &[]);
        let (m, f, u) = resolve_tag_policies(&sp, &id, &tags);
        assert!(m.is_empty() && f.is_empty() && u.is_empty());
    }

    /// An unsupported tag mask type must be reported as unmappable (not silently
    /// skipped) so the rewriter restricts the column (fail-closed).
    #[test]
    fn unsupported_tag_mask_is_unmappable() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "MASK_FUTURE_UNSUPPORTED".to_string();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = user("bob", &["engineer"]);
        let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        assert!(
            !masks.contains_key("PII"),
            "unsupported mask must not produce a mask"
        );
        assert!(
            unmappable.contains("PII"),
            "unsupported mask must mark tag unmappable"
        );
    }

    /// A CUSTOM tag mask with a value_expr must be stored as `TagMaskSpec::Custom`
    /// (not unmappable). The rewriter performs `{col}` substitution at merge time.
    #[test]
    fn custom_tag_mask_stored_as_custom_spec() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let mi = &mut sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0]
            .data_mask_info;
        mi.data_mask_type = "CUSTOM".to_string();
        mi.value_expr = Some("concat('x', {col})".to_string());
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = user("bob", &["engineer"]);
        let (masks, _f, unmappable) = resolve_tag_policies(&sp, &id, &tags);
        assert!(
            !unmappable.contains("PII"),
            "CUSTOM tag with value_expr must NOT be unmappable"
        );
        match masks.get("PII") {
            Some(TagMaskSpec::Custom(template)) => {
                assert_eq!(
                    template, "concat('x', {col})",
                    "template must be stored verbatim"
                );
            }
            other => panic!("expected TagMaskSpec::Custom for PII, got {:?}", other),
        }
    }

    /// A CUSTOM tag mask with no value_expr must remain unmappable (fail-closed):
    /// there is nothing to substitute, so the column must be restricted.
    #[test]
    fn custom_tag_mask_no_value_expr_is_unmappable() {
        let mut sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let mi = &mut sp.tag_policies.as_mut().unwrap().policies[0].data_mask_policy_items[0]
            .data_mask_info;
        mi.data_mask_type = "CUSTOM".to_string();
        mi.value_expr = None; // no template
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = user("bob", &["engineer"]);
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
        let id = user("live-sample-user", &[LIVE_SAMPLE_ROLE]);
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
    pub async fn list_mask_policies_impl(&self) -> sqe_core::Result<Vec<crate::MaskPolicyInfo>> {
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
        // BOTH stores. A deployment can mask by naming the column directly
        // (resource policies, in `bundle.policies`) or by tagging it (tag policies,
        // in the linked tag service, which Ranger ships inside the same download as
        // `bundle.tag_policies`).
        //
        // Walking only the first list is what this originally did, and it made
        // `SHOW MASKING POLICIES` answer "nothing" on a deployment that masks
        // entirely by tag. An empty listing reads as "nothing is masked here",
        // which is the same failure the refusing trait default exists to avoid --
        // just arrived at from the other direction.
        let tag_policies = bundle
            .tag_policies
            .as_ref()
            .map(|tp| tp.policies.as_slice())
            .unwrap_or_default();
        for p in bundle.policies.iter().chain(tag_policies) {
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
                    // A tag policy names a tag and no table, so these are empty and
                    // `tag` carries the binding. Which kind a row is is therefore
                    // readable from the output rather than implied.
                    tag: values("tag").join(", "),
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
            (
                &a.name,
                &a.tag,
                &a.database,
                &a.table,
                &a.column,
                &a.grantees,
            )
                .cmp(&(
                    &b.name,
                    &b.tag,
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
      ],
      "tagPolicies": {
        "serviceName": "sqe_tag",
        "policies": [
          {"id":10,"name":"mask-pii-tag","policyType":1,"isEnabled":true,
           "resources":{"tag":{"values":["PII"]}},
           "dataMaskPolicyItems":[{"roles":["engineer"],
             "dataMaskInfo":{"dataMaskType":"hive:CUSTOM",
                             "valueExpr":"concat('t', {col})"}}]},
          {"id":11,"name":"tag-rowfilter","policyType":2,"isEnabled":true,
           "resources":{"tag":{"values":["PII"]}},
           "rowFilterPolicyItems":[{"roles":["engineer"],
             "rowFilterInfo":{"filterExpr":"region = 'EU'"}}],
           "dataMaskPolicyItems":[{"roles":["engineer"],
             "dataMaskInfo":{"dataMaskType":"hive:MASK_NULL"}}]}
        ]
      }
    }"#;

    fn listed() -> Vec<crate::MaskPolicyInfo> {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).expect("parse bundle");
        mask_policies_from_bundle(&bundle)
    }

    /// TAG-based masks must be listed too.
    ///
    /// This is the one that was WRONG. `mask_policies_from_bundle` walked only
    /// `bundle.policies`, so on a deployment that masks entirely by tag --
    /// which is the pattern the tag projector exists to support --
    /// `SHOW MASKING POLICIES` returned an empty listing. An operator reads that
    /// as "nothing is masked here", which is exactly the failure the refusing
    /// trait default was written to prevent, reached from the other side.
    ///
    /// The tag row carries the TAG and no table; a resource row the reverse. So
    /// the two kinds are distinguishable in the output instead of a reader having
    /// to infer which store a row came from.
    #[test]
    fn tag_based_masks_are_listed_with_their_tag() {
        let all = listed();
        let tagged: Vec<_> = all.iter().filter(|p| p.name == "mask-pii-tag").collect();
        assert_eq!(
            tagged.len(),
            1,
            "the tag-service mask policy is missing from the listing, so a \
             tag-masked deployment would report nothing masked. Got: {:?}",
            all.iter().map(|p| &p.name).collect::<Vec<_>>()
        );
        let row = tagged[0];
        assert_eq!(row.tag, "PII");
        assert_eq!(row.mask_type, "hive:CUSTOM");
        assert_eq!(row.expression, "concat('t', {col})");
        assert_eq!(row.grantees, "ROLE engineer");
        // A tag policy names no table, and saying so is the point: a reader can
        // tell it applies wherever the tag is, not to one column.
        assert_eq!(
            (
                row.database.as_str(),
                row.table.as_str(),
                row.column.as_str()
            ),
            ("", "", "")
        );
    }

    /// A tag ROW FILTER is not a mask, in the tag store either.
    ///
    /// The resource-side equivalent is covered by `only_data_mask_policies_are_listed`.
    /// The tag store needs its own case because the policyType guard runs over a
    /// chained iterator, and a guard that is correct for one arm is not
    /// automatically correct for the other.
    ///
    /// `tag-rowfilter` carries `dataMaskPolicyItems` as well as its row filter, and
    /// that detail is the whole test. My first version left it with only a row
    /// filter, and the mutation survived: with no mask items the loop emits nothing
    /// for it whether the type is checked or not, so the assertion held for a reason
    /// that had nothing to do with the guard. The comment on
    /// `only_data_mask_policies_are_listed` warns about exactly this shape, in this
    /// file, and I reproduced it anyway. Mutation-verified after the fixture change:
    /// dropping the guard makes `tag-rowfilter` appear.
    #[test]
    fn a_tag_row_filter_is_not_listed_as_a_mask() {
        assert!(
            !listed().iter().any(|p| p.name == "tag-rowfilter"),
            "a policyType-2 tag policy is a row filter, not a mask"
        );
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
    /// `mask-pii-tag` is here because it IS a policyType-1 mask, just one living in
    /// the tag store. `tag-rowfilter` is absent for the same reason
    /// `filter-orders-eu` is.
    #[test]
    fn only_data_mask_policies_are_listed() {
        let all = listed();
        let names: Vec<&str> = all.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["mask-multi", "mask-orders-ssn", "mask-pii-tag"]);
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

/// `[policy.ranger] catalog_scoped` and Ranger's first-match item precedence.
#[cfg(test)]
mod catalog_scope_and_precedence_tests {
    use super::*;
    use serde_json::json;

    fn bundle(policies: serde_json::Value) -> ServicePolicies {
        serde_json::from_value(json!({ "policyVersion": 1, "policies": policies })).unwrap()
    }

    fn member(name: &str, groups: &[&str]) -> SessionUser {
        SessionUser {
            username: name.to_string(),
            roles: Vec::new(),
            subject: None,
            email: None,
            groups: groups.iter().map(|g| g.to_string()).collect(),
        }
    }

    fn mask_policy(id: i64, database: &str, column: &str, items: serde_json::Value) -> serde_json::Value {
        json!({
            "id": id, "name": format!("mask-{id}"), "policyType": 1, "isEnabled": true,
            "resources": {
                "database": {"values": [database]},
                "table": {"values": ["orders"]},
                "column": {"values": [column]}
            },
            "dataMaskPolicyItems": items
        })
    }

    fn item(groups: &[&str], mask: &str) -> serde_json::Value {
        json!({"groups": groups, "accesses": [{"type": "select", "isAllowed": true}],
               "dataMaskInfo": {"dataMaskType": mask}})
    }

    /// The portal's shape: "mask for everyone except group E".
    fn everyone_but_e() -> serde_json::Value {
        json!([item(&["SG-Exempt"], "MASK_NONE"), item(&["public"], "MASK_NULL")])
    }

    fn resolve(b: &ServicePolicies, u: &SessionUser, namespace: &str, scoped: bool) -> ResolvedPolicy {
        resolve_from_bundle_with(b, u, "orders", namespace, false, scoped)
    }

    // ── catalog_scoped ─────────────────────────────────────────────────

    #[test]
    fn scoped_mask_applies_only_to_the_named_catalog() {
        let b = bundle(json!([mask_policy(1, "ws_a.finance", "ssn", json!([item(&["public"], "MASK_NULL")]))]));
        let u = member("ann", &[]);
        assert!(resolve(&b, &u, "ws_a.finance", true).column_masks.contains_key("ssn"));
        assert!(
            resolve(&b, &u, "ws_b.finance", true).column_masks.is_empty(),
            "same namespace.table in another catalog is not covered"
        );
    }

    #[test]
    fn unscoped_a_catalogless_policy_covers_every_catalog() {
        // Today's behaviour, kept when the setting is off: both catalogs key on
        // `finance`, so one policy fires in both.
        let b = bundle(json!([mask_policy(1, "finance", "ssn", json!([item(&["public"], "MASK_NULL")]))]));
        let u = member("ann", &[]);
        assert!(resolve(&b, &u, "finance", false).column_masks.contains_key("ssn"));
    }

    #[test]
    fn scoped_disables_the_last_component_fallback() {
        let b = bundle(json!([mask_policy(1, "finance", "ssn", json!([item(&["public"], "MASK_NULL")]))]));
        let u = member("ann", &[]);
        assert!(
            resolve(&b, &u, "ws_a.finance", false).column_masks.contains_key("ssn"),
            "unscoped: the migration fallback matches on the last component"
        );
        assert!(
            resolve(&b, &u, "ws_a.finance", true).column_masks.is_empty(),
            "scoped: `finance` must not match `ws_a.finance` (or `ws_b.finance`)"
        );
        // A policy naming another catalog never matches through the fallback.
        let other = bundle(json!([mask_policy(1, "ws_b.finance", "ssn", json!([item(&["public"], "MASK_NULL")]))]));
        assert!(resolve(&other, &u, "ws_a.finance", true).column_masks.is_empty());
    }

    /// The deny-item fallback is off under `catalog_scoped` too: a deny on
    /// `finance` is another catalog's (or no catalog's) policy.
    #[test]
    fn scoped_access_deny_does_not_match_by_last_component() {
        let b = bundle(json!([
            {"id": 1, "name": "allow", "policyType": 0, "isEnabled": true,
             "resources": {"database": {"values": ["ws_a.finance"]}, "table": {"values": ["*"]}},
             "policyItems": [{"groups": ["public"], "accesses": [{"type": "select", "isAllowed": true}]}]},
            {"id": 2, "name": "deny", "policyType": 0, "isEnabled": true,
             "resources": {"database": {"values": ["finance"]}, "table": {"values": ["*"]}},
             "denyPolicyItems": [{"groups": ["public"], "accesses": [{"type": "select", "isAllowed": true}]}]}
        ]));
        let u = member("ann", &[]);
        let denied = |scoped| {
            crate::plan_rewriter::policy_denies_all(&resolve_from_bundle_with(
                &b, &u, "orders", "ws_a.finance", true, scoped,
            ))
        };
        assert!(denied(false), "unscoped: the deny on `finance` matches by last component");
        assert!(!denied(true), "scoped: it does not");
    }

    #[test]
    fn scoped_store_reports_the_flag_and_scoped_ddl_keeps_the_catalog() {
        let on = RangerStore::from_config(&RangerPolicyConfig {
            catalog_scoped: true,
            ..RangerPolicyConfig::default()
        })
        .unwrap();
        assert!(on.catalog_scoped());
        let off = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
        assert!(!off.catalog_scoped());

        let parsed = sqe_sql::policy_ddl::try_parse_policy_ddl(
            "CREATE POLICY m ON TABLE ws_a.finance.orders COLUMN MASK MASK_NULL \
             TO GROUP analysts ON COLUMN ssn",
        )
        .expect("valid policy SQL")
        .expect("policy statement");
        let sqe_sql::policy_ddl::PolicyDdlStatement::Create(stmt) = parsed else {
            panic!("expected create")
        };
        let scoped = ranger_policy_body(&stmt, "query", "hive", true).unwrap();
        assert_eq!(scoped["resources"]["database"]["values"], json!(["ws_a.finance"]));
        let unscoped = ranger_policy_body(&stmt, "query", "hive", false).unwrap();
        assert_eq!(unscoped["resources"]["database"]["values"], json!(["finance"]));

        let two_part = sqe_sql::policy_ddl::try_parse_policy_ddl(
            "CREATE POLICY m ON TABLE finance.orders COLUMN MASK MASK_NULL TO GROUP analysts ON COLUMN ssn",
        )
        .unwrap()
        .unwrap();
        let sqe_sql::policy_ddl::PolicyDdlStatement::Create(two_part) = two_part else {
            panic!("expected create")
        };
        assert!(
            ranger_policy_body(&two_part, "query", "hive", true).is_err(),
            "scoped DDL without a catalog would write a policy that never matches"
        );
    }

    // ── first-match precedence ─────────────────────────────────────────

    #[test]
    fn first_matching_mask_item_wins_so_an_exemption_item_exempts() {
        let b = bundle(json!([mask_policy(1, "finance", "ssn", everyone_but_e())]));
        let exempt = member("eve", &["SG-Exempt"]);
        let other = member("ann", &["SG-DataAnalysts"]);
        let p = resolve(&b, &exempt, "finance", false);
        assert!(p.column_masks.is_empty() && p.restricted_columns.is_empty(), "E sees the raw value");
        assert!(resolve(&b, &other, "finance", false).column_masks.contains_key("ssn"));
    }

    #[test]
    fn mask_none_for_a_user_leaves_the_column_unmasked() {
        let items = json!([
            {"users": ["owner"], "dataMaskInfo": {"dataMaskType": "MASK_NONE"}},
            item(&["public"], "MASK_NULL")
        ]);
        let b = bundle(json!([mask_policy(1, "finance", "ssn", items)]));
        let p = resolve(&b, &member("owner", &[]), "finance", false);
        assert!(p.column_masks.is_empty() && p.restricted_columns.is_empty());
        assert!(resolve(&b, &member("ann", &[]), "finance", false).column_masks.contains_key("ssn"));
    }

    #[test]
    fn item_order_decides_so_public_first_masks_everyone() {
        let items = json!([item(&["public"], "MASK_NULL"), item(&["SG-Exempt"], "MASK_NONE")]);
        let b = bundle(json!([mask_policy(1, "finance", "ssn", items)]));
        assert!(
            resolve(&b, &member("eve", &["SG-Exempt"]), "finance", false)
                .column_masks
                .contains_key("ssn"),
            "Ranger takes the first matching item: public, so E is masked too"
        );
    }

    #[test]
    fn with_two_matching_mask_items_the_first_wins() {
        let items = json!([item(&["public"], "MASK_HASH"), item(&["public"], "MASK_NULL")]);
        let b = bundle(json!([mask_policy(1, "finance", "ssn", items)]));
        assert!(matches!(
            resolve(&b, &member("ann", &[]), "finance", false).column_masks.get("ssn"),
            Some(MaskType::Hash)
        ));
    }

    #[test]
    fn first_policy_to_decide_a_column_is_final_and_override_priority_goes_first() {
        let exempt = member("eve", &["SG-Exempt"]);
        // Policy 1 exempts E; policy 2 masks everyone. Same priority: bundle
        // order, policy 1 decides for E and policy 2 cannot re-mask.
        let b = bundle(json!([
            mask_policy(1, "finance", "ssn", json!([item(&["SG-Exempt"], "MASK_NONE")])),
            mask_policy(2, "finance", "ssn", json!([item(&["public"], "MASK_HASH")])),
        ]));
        assert!(resolve(&b, &exempt, "finance", false).column_masks.is_empty());
        assert!(matches!(
            resolve(&b, &member("ann", &[]), "finance", false).column_masks.get("ssn"),
            Some(MaskType::Hash)
        ));

        // Override priority on the masking policy puts it first.
        let mut over = mask_policy(2, "finance", "ssn", json!([item(&["public"], "MASK_HASH")]));
        over["policyPriority"] = json!(1);
        let b = bundle(json!([
            mask_policy(1, "finance", "ssn", json!([item(&["SG-Exempt"], "MASK_NONE")])),
            over
        ]));
        assert!(matches!(
            resolve(&b, &exempt, "finance", false).column_masks.get("ssn"),
            Some(MaskType::Hash)
        ));
    }

    /// A conditional first item decides its columns (restricted) for the
    /// whole bundle: a later policy's `MASK_NONE` does not reopen them.
    #[test]
    fn a_conditional_first_item_is_a_final_decision() {
        let conditional = json!([{"groups": ["public"], "conditions": [{"type": "ip-range", "values": ["10.0.0.0/8"]}],
                                  "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]);
        let b = bundle(json!([
            mask_policy(1, "finance", "ssn", conditional),
            mask_policy(2, "finance", "ssn", json!([item(&["public"], "MASK_NONE")])),
        ]));
        let p = resolve(&b, &member("ann", &[]), "finance", false);
        assert!(p.restricted_columns.contains(&"ssn".to_string()));
    }

    fn filter_policy(id: i64, items: serde_json::Value) -> serde_json::Value {
        json!({
            "id": id, "name": format!("rf-{id}"), "policyType": 2, "isEnabled": true,
            "resources": {"database": {"values": ["finance"]}, "table": {"values": ["orders"]}},
            "rowFilterPolicyItems": items
        })
    }

    #[test]
    fn first_matching_row_filter_item_wins_and_an_empty_filter_exempts() {
        let b = bundle(json!([filter_policy(
            1,
            json!([
                {"groups": ["SG-Exempt"], "rowFilterInfo": {"filterExpr": ""}},
                {"groups": ["public"], "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
            ])
        )]));
        assert!(resolve(&b, &member("eve", &["SG-Exempt"]), "finance", false).row_filters.is_empty());
        assert_eq!(resolve(&b, &member("ann", &[]), "finance", false).row_filters.len(), 1);
    }

    /// A conditional exemption item cannot exempt: SQE cannot tell whether
    /// its condition holds, so the later matching item applies as well.
    #[test]
    fn a_conditional_row_filter_item_does_not_decide_alone() {
        let b = bundle(json!([filter_policy(
            1,
            json!([
                {"groups": ["SG-Exempt"], "conditions": [{"type": "ip-range", "values": ["10.0.0.0/8"]}],
                 "rowFilterInfo": {"filterExpr": ""}},
                {"groups": ["public"], "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
            ])
        )]));
        assert_eq!(resolve(&b, &member("eve", &["SG-Exempt"]), "finance", false).row_filters.len(), 1);
    }

    #[test]
    fn row_filters_of_separate_policies_are_anded() {
        let b = bundle(json!([
            filter_policy(1, json!([{"groups": ["public"], "rowFilterInfo": {"filterExpr": "region = 'EU'"}}])),
            filter_policy(2, json!([{"groups": ["public"], "rowFilterInfo": {"filterExpr": "year > 2020"}}])),
        ]));
        assert_eq!(resolve(&b, &member("ann", &[]), "finance", false).row_filters.len(), 2);
    }

    // ── tag path ───────────────────────────────────────────────────────

    #[test]
    fn tag_policies_take_the_first_item_and_the_first_deciding_policy() {
        let mut b = bundle(json!([]));
        b.tag_policies = Some(
            serde_json::from_value(json!({"policies": [
                {"id": 9, "name": "pii-exempt", "policyType": 1, "isEnabled": true,
                 "resources": {"tag": {"values": ["PII"]}},
                 "dataMaskPolicyItems": [
                     {"groups": ["SG-Exempt"], "dataMaskInfo": {"dataMaskType": "hive:MASK_NONE"}},
                     {"groups": ["SG-DataAnalysts"], "dataMaskInfo": {"dataMaskType": "hive:MASK_NULL"}}
                 ]},
                {"id": 10, "name": "pii-all", "policyType": 1, "isEnabled": true,
                 "resources": {"tag": {"values": ["PII"]}},
                 "dataMaskPolicyItems": [
                     {"groups": ["public"], "dataMaskInfo": {"dataMaskType": "hive:MASK_HASH"}}
                 ]}
            ]}))
            .unwrap(),
        );
        let tags: HashSet<String> = ["PII".to_string()].into();
        let (masks, _, _) = resolve_tag_policies(&b, &member("ann", &["SG-DataAnalysts"]), &tags);
        assert!(matches!(masks.get("PII"), Some(TagMaskSpec::Ready(MaskType::Nullify))));
        let (masks, _, _) = resolve_tag_policies(&b, &member("eve", &["SG-Exempt", "SG-DataAnalysts"]), &tags);
        assert!(masks.is_empty(), "the exemption item comes first and its decision is final");
        let (masks, _, _) = resolve_tag_policies(&b, &member("bob", &[]), &tags);
        assert!(matches!(masks.get("PII"), Some(TagMaskSpec::Ready(MaskType::Hash))));
    }
}
