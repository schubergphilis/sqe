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
    /// Catalogs read with the engine's keys (`[catalogs.*] credentials`,
    /// #934). Tables under them need a Ranger `select` grant: without one the
    /// person gets no rows, and the engine fetches no keys.
    engine_catalogs: HashSet<String>,
    /// Tag definition names from `GET /service/tags/tagdefs`, for
    /// `[access_control] tag_names = "ranger-tagdefs"`. Same TTL as the bundle;
    /// a fetch error is never cached.
    tagdef_cache: Cache<&'static str, Arc<HashSet<String>>>,
    /// Serialises tagdef downloads, so concurrent misses download once.
    tagdef_fetch: tokio::sync::Mutex<()>,
    /// Completed tagdef downloads: a `fresh` caller that waited on
    /// `tagdef_fetch` while one finished takes that one instead of its own.
    tagdef_downloads: AtomicU64,
}

/// The one key of `tagdef_cache`.
const TAGDEF_KEY: &str = "__tagdefs__";

/// The part of a Ranger `RangerTagDef` the name check reads.
#[derive(Debug, Deserialize)]
struct RangerTagDefName {
    name: String,
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
            engine_catalogs: HashSet::new(),
            tagdef_cache: Cache::builder()
                .time_to_live(Duration::from_secs(cfg.cache_ttl_secs))
                .max_capacity(1)
                .build(),
            tagdef_fetch: tokio::sync::Mutex::new(()),
            tagdef_downloads: AtomicU64::new(0),
        })
    }

    /// Download every tag definition name. `GET /service/tags/tagdefs` answers
    /// a bare JSON array of `RangerTagDef` (measured on the compose Ranger),
    /// global to the Ranger instance rather than per tag service.
    ///
    /// Not behind the policy breaker: this is a DDL-path lookup like
    /// `service_link`, and a misconfigured endpoint (401, 404) failing on every
    /// tag write must not open the breaker the read path depends on.
    async fn fetch_tag_definitions(&self) -> sqe_core::Result<HashSet<String>> {
        let unreadable = |why: String| {
            sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                format!("could not read the Ranger tag definitions: {why}"),
            )
        };
        let response = self
            .client
            .get(format!("{}/service/tags/tagdefs", self.admin_url))
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| unreadable(e.to_string()))?;
        if !response.status().is_success() {
            return Err(unreadable(format!("HTTP {}", response.status())));
        }
        let defs: Vec<RangerTagDefName> = response
            .json()
            .await
            .map_err(|e| unreadable(format!("unexpected response ({e})")))?;
        Ok(defs.into_iter().map(|d| d.name).collect())
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

    /// A table needs an access grant when it sits under an engine-keyed
    /// catalog (always, see `engine_catalogs`), or when the gate is on and
    /// the policy namespace starts with a registered Hive root name.
    fn requires_access_grant(&self, namespace: &str) -> bool {
        let first = namespace.split('.').next();
        self.under_engine_catalog(first)
            || (self.hive_access_policies && self.hive_catalogs.contains_name(first))
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

/// True when an access-policy item carries `access_type` (or, with
/// `all_counts`, `all`) as an allowed access type. Ranger writes
/// `isAllowed: true` on deny items too (the item list, not the flag, makes it
/// a deny), so the flag only rules out an item an operator toggled off.
fn item_grants_access(item: &AccessPolicyItem, access_type: &str, all_counts: bool) -> bool {
    item.accesses.iter().any(|a| {
        a.is_allowed
            && (a.access_type.eq_ignore_ascii_case(access_type)
                || (all_counts && a.access_type.eq_ignore_ascii_case("all")))
    })
}

/// Whether an allow item's `all` grants `access_type`. `all` carries the
/// stock `hive` access types it lists as implied grants. `apply_tag` and
/// `remove_tag` (#927) are custom types added to the servicedef with no place
/// in `all`'s implied grants, so only an item naming one grants it: a blanket
/// `all` (the usual owner or `public` item) does not make its holders taggers.
/// A deny item's `all` still denies it; narrowing never needs the exact type.
fn all_grants(access_type: &str) -> bool {
    use crate::TablePrivilege::{ApplyTag, RemoveTag};
    ![ApplyTag, RemoveTag]
        .iter()
        .any(|p| access_type.eq_ignore_ascii_case(p.access_type()))
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
/// (`select`, `alter`, `drop`, `apply_tag`; an allow item's `all` carries
/// each but `apply_tag`, see [`all_grants`]): does any enabled
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
    access_grants_type_with(
        bundle,
        user,
        database,
        table,
        access_type,
        last_component_fallback,
        column_resource_covers_table,
    )
}

/// True when an access policy's `column` resource matches `column`: absent
/// matches every column, otherwise `resource_matches` (wildcards, case
/// folding, `isExcludes`).
fn column_resource_matches(p: &RangerPolicy, column: &str) -> bool {
    p.resources
        .get("column")
        .is_none_or(|res| resource_matches(res, column))
}

/// `access_grants_table_type` for one column: same evaluation order, but a
/// policy counts when its `column` resource matches `column` instead of
/// covering the whole table. A deny on this column or on `*` decides; a deny
/// on another column does not.
fn access_grants_column_type(
    bundle: &ServicePolicies,
    user: &SessionUser,
    database: &str,
    table: &str,
    column: &str,
    access_type: &str,
    last_component_fallback: bool,
) -> bool {
    access_grants_type_with(
        bundle,
        user,
        database,
        table,
        access_type,
        last_component_fallback,
        |p| column_resource_matches(p, column),
    )
}

/// Shared body of the table and column access checks; `column_ok` is the only
/// difference between them.
fn access_grants_type_with(
    bundle: &ServicePolicies,
    user: &SessionUser,
    database: &str,
    table: &str,
    access_type: &str,
    last_component_fallback: bool,
    column_ok: impl Fn(&RangerPolicy) -> bool,
) -> bool {
    let matching = |fallback: bool| {
        bundle
            .policies
            .iter()
            .filter(|p| p.is_enabled && p.policy_type == POLICY_TYPE_ACCESS)
            .filter(|p| policy_matches_table_with(p, database, table, fallback) && column_ok(p))
            .collect::<Vec<_>>()
    };

    // Deny items and allow exceptions narrow access, so their `all` always
    // counts; allow items and deny exceptions widen it, so their `all` counts
    // only for the access types `all` implies (`all_grants`).
    let applies = |items: &[AccessPolicyItem]| {
        items.iter().any(|i| {
            item_grants_access(i, access_type, true)
                && item_matches(&i.users, &i.roles, &i.groups, user)
        })
    };
    let all_widens = all_grants(access_type);

    // Items that widen access (allow items, deny exceptions) do not count when
    // they carry conditions SQE cannot evaluate. Deny items and allow
    // exceptions keep ignoring conditions, which only ever narrows access.
    let widens = |p: &RangerPolicy, items: &[AccessPolicyItem]| {
        items.iter().any(|i| {
            if !(item_grants_access(i, access_type, all_widens)
                && item_matches(&i.users, &i.roles, &i.groups, user))
            {
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
            Some(_) if !stmt.or_replace => Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                format!(
                    "Policy '{}' is present; use CREATE OR REPLACE POLICY",
                    stmt.name
                ),
            )),
            Some(policy) => {
                if let Some((from, to)) = tag_policy_move(&policy, &body) {
                    return Err(sqe_core::SqeError::with_code(
                        sqe_core::SqeErrorCode::ExecutionFailed,
                        format!(
                            "CREATE OR REPLACE POLICY '{}' would move it from tag '{from}' to \
                             tag '{to}', which changes the catalogs it governs; DROP POLICY \
                             '{}' first",
                            stmt.name, stmt.name
                        ),
                    ));
                }
                let id = policy
                    .get("id")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| {
                        sqe_core::SqeError::with_code(
                            sqe_core::SqeErrorCode::ExecutionFailed,
                            format!("Ranger policy '{}' has no numeric id", stmt.name),
                        )
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

    /// `DROP POLICY ON TABLE <t> FROM <principal>`: take the principal out of
    /// the mask and row-filter policies SQE created on exactly this table
    /// (`sqe-grant-*`), and delete one left with no items. A policy written
    /// by hand is never edited, even when it names the principal and GRANT
    /// added an item to it: lifting a mask a security team set in Ranger
    /// would show real values, so that stays a Ranger edit. Policies with
    /// wildcards or value lists are not matched at all.
    async fn drop_principal_policies_impl(
        &self,
        stmt: &sqe_sql::policy_ddl::DropPrincipalPolicyStatement,
    ) -> sqe_core::Result<usize> {
        let (database, table) =
            ranger_table_resource(&stmt.table, self.catalog_scoped, "DROP POLICY ON TABLE")?;
        let mut changed = 0;
        let mut hand_written = Vec::new();
        for policy in self.policies_for_service(&self.service_name).await? {
            let Some(policy_type) = policy
                .get("policyType")
                .and_then(serde_json::Value::as_i64)
                .filter(|t| *t == 1 || *t == 2)
            else {
                continue;
            };
            if !resource_is_exactly(&policy, "database", &database)
                || !resource_is_exactly(&policy, "table", &table)
            {
                continue;
            }
            let key = grant_items_key(policy_type);
            let mut items: Vec<serde_json::Value> = policy
                .get(key)
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            if !remove_principal_from_items(&mut items, &stmt.principal) {
                continue;
            }
            let name = policy
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if !name.starts_with("sqe-grant-") {
                hand_written.push(name.to_string());
                continue;
            }
            changed += 1;
            let id = policy_id(&policy)?;
            let path = format!("/service/public/v2/api/policy/{id}");
            if items.is_empty() {
                self.send_policy_ddl(reqwest::Method::DELETE, &path, None)
                    .await?;
            } else {
                let mut policy = policy;
                policy[key] = serde_json::Value::Array(items);
                self.send_policy_ddl(reqwest::Method::PUT, &path, Some(&policy))
                    .await?;
            }
        }
        if changed == 0 && !stmt.if_exists {
            let mut message = format!(
                "{} has no column mask or row filter on {} written by GRANT",
                stmt.principal.name, stmt.table
            );
            if !hand_written.is_empty() {
                message.push_str(&format!(
                    "; it is named in Ranger policies written by hand ({}), which \
                     DROP POLICY ... FROM does not edit",
                    hand_written.join(", ")
                ));
            }
            return Err(sqe_core::SqeError::with_code(
                sqe_core::SqeErrorCode::ExecutionFailed,
                message,
            ));
        }
        Ok(changed)
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

/// The tags a stored tag policy and its replacement body name, when they
/// differ. Under `catalog_scoped` a tag key carries its catalog (#919), so
/// `CREATE OR REPLACE` onto a policy with another tag would silently move it to
/// other catalogs; the caller refuses that instead. A resource policy (no `tag`
/// resource on either side) yields `None`.
fn tag_policy_move(
    existing: &serde_json::Value,
    body: &serde_json::Value,
) -> Option<(String, String)> {
    let tags = |policy: &serde_json::Value| -> Option<Vec<String>> {
        let values = policy.pointer("/resources/tag/values")?.as_array()?;
        let mut tags: Vec<String> = values
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        tags.sort();
        Some(tags)
    };
    let (from, to) = (tags(existing), tags(body));
    if from == to {
        return None;
    }
    let show = |t: Option<Vec<String>>| t.map(|t| t.join(", ")).unwrap_or_default();
    Some((show(from), show(to)))
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

impl RangerStore {
    /// `PolicyStore::resolve`, boxed in a non-generic function. The future
    /// holds a `ResolvedPolicy` (masks and row filters, so `Expr`) across its
    /// awaits; inside the `#[async_trait]` method the `Send` proof over
    /// `Expr` ran in that method's own parameter environment (2.6 s).
    fn resolve_boxed<'a>(
        &'a self,
        user: &'a SessionUser,
        table_name: &'a str,
        namespace: &'a str,
    ) -> crate::BoxedFuture<'a, sqe_core::Result<ResolvedPolicy>> {
        Box::pin(self.resolve_policy(user, table_name, namespace))
    }

    async fn resolve_policy(
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

    async fn drop_principal_policies(
        &self,
        stmt: &sqe_sql::policy_ddl::DropPrincipalPolicyStatement,
    ) -> sqe_core::Result<usize> {
        self.drop_principal_policies_impl(stmt).await
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
        self.resolve_boxed(user, table_name, namespace).await
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

    /// Column-level sibling of `grants_table_privilege`: a policy whose
    /// `column` resource names (or wildcard-matches) `column` counts, so a
    /// column-scoped steward is recognised. Deny-first, like the table check.
    async fn grants_column_privilege(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
        column: &str,
        privilege: crate::TablePrivilege,
    ) -> sqe_core::Result<bool> {
        let bundle = self.cached_bundle().await?;
        Ok(access_grants_column_type(
            &bundle,
            user,
            &hive_database(namespace),
            table_name,
            column,
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

    async fn conflict_principals(
        &self,
        table_name: &str,
        namespace: &str,
        tags: &std::collections::HashSet<String>,
    ) -> sqe_core::Result<Option<Vec<SessionUser>>> {
        let bundle = self.cached_bundle().await?;
        Ok(Some(conflict_principals::conflict_principals_from_bundle(
            &bundle,
            &hive_database(namespace),
            table_name,
            !self.catalog_scoped,
            tags,
        )))
    }

    fn invalidate_all(&self) {
        self.cache.invalidate_all();
        // Also drop the shared bundle so a manual refresh does not serve a stale
        // bundle until its TTL elapses.
        self.bundle_generation.fetch_add(1, Ordering::AcqRel);
        self.bundle_cache.invalidate_all();
        self.tagdef_cache.invalidate_all();
    }

    /// Tag definition names, cached for the policy TTL. `fresh` skips the
    /// cache (and refills it); concurrent callers download once.
    async fn tag_definitions(&self, fresh: bool) -> sqe_core::Result<Arc<HashSet<String>>> {
        if !fresh {
            if let Some(cached) = self.tagdef_cache.get(TAGDEF_KEY).await {
                return Ok(cached);
            }
        }
        let downloads_before = self.tagdef_downloads.load(Ordering::Acquire);
        let _guard = self.tagdef_fetch.lock().await;
        // A download that finished while this caller waited is as fresh as
        // one it would start now (and, for a cache miss, fills the cache).
        if !fresh || self.tagdef_downloads.load(Ordering::Acquire) != downloads_before {
            if let Some(cached) = self.tagdef_cache.get(TAGDEF_KEY).await {
                return Ok(cached);
            }
        }
        let defs = Arc::new(self.fetch_tag_definitions().await?);
        self.tagdef_cache.insert(TAGDEF_KEY, defs.clone()).await;
        self.tagdef_downloads.fetch_add(1, Ordering::AcqRel);
        Ok(defs)
    }
}

mod conflict_principals;
mod engine_catalogs;

#[cfg(test)]
mod tests;

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
mod mask_policy_listing_tests;

/// `[policy.ranger] catalog_scoped` and Ranger's first-match item precedence.
#[cfg(test)]
mod catalog_scope_and_precedence_tests;
