//! V10 httpfs: a [`ObjectStoreRegistry`] that builds an HTTP / HTTPS object
//! store on first request.
//!
//! DataFusion's default registry is a `scheme://host` -> store map: queries
//! against URLs whose scheme + host pair was never explicitly registered fail
//! with `"No suitable object store found"`. For S3 this is fine because the
//! coordinator pre-registers every bucket it knows about. For arbitrary
//! HTTPS / HTTP URLs (raw GitHub, HuggingFace, public dataset mirrors) the
//! pre-registration list is unbounded.
//!
//! This wrapper layers lazy construction on top of the default registry: if
//! the inner registry has no store for a URL whose scheme is `https` or
//! `http`, build one with [`object_store::http::HttpBuilder`] using the URL's
//! `scheme://host[:port]` as the root, register it, and return it. Subsequent
//! lookups hit the cache.
//!
//! When constructed with [`LazyHttpObjectStoreRegistry::with_s3_fallback`] the
//! same lazy treatment is extended to `s3://` / `s3a://` URLs: a registry miss
//! builds an [`object_store::aws::AmazonS3`] from the coordinator-wide
//! [`StorageConfig`]. This is what lets the file-reader TVFs (`read_csv` /
//! `read_parquet`) resolve ad-hoc S3 buckets that were never pre-registered as
//! Iceberg catalogs — without it, `read_csv('s3://…')` fails at physical-plan
//! creation with `"No suitable object store found"`. The plain [`new`]
//! constructor keeps `s3` / `file` flowing through the inner registry
//! unchanged (embedded CLI, tests).
//!
//! The wrapper is used by both [`build_embedded_context`] and
//! `coordinator::session_context`, so the cluster engine and the embedded CLI
//! share one HTTP fetch path.
//!
//! [`new`]: LazyHttpObjectStoreRegistry::new

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::http::HttpBuilder;
use object_store::ObjectStore;
use sqe_core::config::{StorageConfig, TvfPolicy};
use url::Url;

/// Wrap any inner [`ObjectStoreRegistry`] (typically
/// [`DefaultObjectStoreRegistry`]) so that HTTP / HTTPS URLs without a
/// pre-registered store get one constructed on demand.
///
/// The constructed store is cached in the inner registry, so the build
/// happens at most once per `scheme://host[:port]` per session.
#[derive(Debug)]
pub struct LazyHttpObjectStoreRegistry<R: ObjectStoreRegistry> {
    inner: R,
    /// When `Some`, `s3://` / `s3a://` URLs that miss the inner registry are
    /// built on demand from this coordinator-wide storage config (endpoint,
    /// credentials, path-style, allow_http). `None` keeps the http-only
    /// behaviour for callers that never read S3 via TVFs (embedded CLI, tests).
    s3_storage: Option<StorageConfig>,
    /// CAT-01 defense-in-depth: when `Some`, a lazily-built `http(s)://` store
    /// is only constructed when the URL's host clears `TvfPolicy::check`
    /// (`allow_http` or `allowed_http_hosts`). `None` keeps the legacy
    /// permissive behaviour for the embedded CLI / tests, where the
    /// single-tenant trust model already allows arbitrary hosts.
    tvf_policy: Option<TvfPolicy>,
}

impl<R: ObjectStoreRegistry> LazyHttpObjectStoreRegistry<R> {
    /// Wrap the given inner registry (lazy http/https build only; `s3` and
    /// `file` flow through the inner registry unchanged).
    pub fn new(inner: R) -> Self {
        Self { inner, s3_storage: None, tvf_policy: None }
    }

    /// Wrap the inner registry and additionally build `s3://` / `s3a://`
    /// stores on demand from `storage`. Used by the coordinator runtime so the
    /// file-reader TVFs (`read_csv` / `read_parquet`) resolve ad-hoc S3 buckets
    /// that were never pre-registered as Iceberg catalogs.
    ///
    /// The `[storage.tvf]` policy carried on `storage` is also applied to lazy
    /// `http(s)://` builds (CAT-01): a registry miss for an http host outside
    /// the allowlist is rejected, so a server-side resolution path that escaped
    /// the per-call TVF check cannot reach arbitrary hosts.
    pub fn with_s3_fallback(inner: R, storage: StorageConfig) -> Self {
        let tvf_policy = Some(storage.tvf.clone());
        Self { inner, s3_storage: Some(storage), tvf_policy }
    }

    /// Wrap the inner registry with an explicit `TvfPolicy` gating lazy
    /// `http(s)://` builds, without the `s3://` fallback. Used where the
    /// caller wants the http allowlist enforced but pre-registers its own
    /// S3 stores.
    pub fn with_tvf_policy(inner: R, tvf_policy: TvfPolicy) -> Self {
        Self { inner, s3_storage: None, tvf_policy: Some(tvf_policy) }
    }
}

impl<R: ObjectStoreRegistry> ObjectStoreRegistry for LazyHttpObjectStoreRegistry<R> {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn deregister_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        self.inner.deregister_store(url)
    }

    fn get_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        match self.inner.get_store(url) {
            Ok(store) => Ok(store),
            Err(_) if matches!(url.scheme(), "http" | "https") => {
                build_and_register_http_store(&self.inner, url, self.tvf_policy.as_ref())
            }
            // S3 lazy build only when a storage config was supplied
            // (`with_s3_fallback`); otherwise fall through to the inner error.
            Err(_)
                if matches!(url.scheme(), "s3" | "s3a") && self.s3_storage.is_some() =>
            {
                build_and_register_s3_store(
                    &self.inner,
                    url,
                    self.s3_storage.as_ref().expect("guarded by is_some above"),
                )
            }
            Err(e) => Err(e),
        }
    }
}

fn build_and_register_http_store(
    inner: &dyn ObjectStoreRegistry,
    url: &Url,
    tvf_policy: Option<&TvfPolicy>,
) -> DFResult<Arc<dyn ObjectStore>> {
    // CAT-01 defense-in-depth: reject hosts the TVF policy would deny before a
    // store is ever built. `TvfPolicy::check` allows the host when `allow_http`
    // is set or the host is in `allowed_http_hosts`; otherwise it errors. When
    // no policy is supplied (embedded CLI / tests) the legacy permissive
    // behaviour is preserved.
    if let Some(policy) = tvf_policy {
        policy
            .check(url.as_str())
            .map_err(DataFusionError::Plan)?;
    }

    let host = url.host_str().ok_or_else(|| {
        DataFusionError::Plan(format!("URL '{url}' is missing a host"))
    })?;
    let scheme = url.scheme();
    let base = match url.port() {
        Some(p) => format!("{scheme}://{host}:{p}"),
        None => format!("{scheme}://{host}"),
    };

    let store = HttpBuilder::new()
        .with_url(&base)
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let store: Arc<dyn ObjectStore> = Arc::new(store);

    let key = Url::parse(&base).map_err(|e| {
        DataFusionError::Plan(format!("failed to build object-store URL '{base}': {e}"))
    })?;

    inner.register_store(&key, Arc::clone(&store));
    Ok(store)
}

fn build_and_register_s3_store(
    inner: &dyn ObjectStoreRegistry,
    url: &Url,
    storage: &StorageConfig,
) -> DFResult<Arc<dyn ObjectStore>> {
    let bucket = url.host_str().ok_or_else(|| {
        DataFusionError::Plan(format!("S3 URL '{url}' is missing a bucket (host)"))
    })?;

    let store = crate::file_tvf_common::build_s3_store_for_bucket(bucket, storage)?;
    let store: Arc<dyn ObjectStore> = Arc::new(store);

    // DataFusion keys the registry by `scheme://host`; register under the
    // bucket-scoped URL so the constructed store is reused for every object in
    // the bucket and the build happens at most once per session.
    let scheme = url.scheme();
    let key = Url::parse(&format!("{scheme}://{bucket}")).map_err(|e| {
        DataFusionError::Plan(format!("failed to build object-store URL '{scheme}://{bucket}': {e}"))
    })?;

    inner.register_store(&key, Arc::clone(&store));
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;
    use object_store::memory::InMemory;

    /// Minimal storage config that lets `AmazonS3Builder::build()` succeed
    /// (http endpoint + allow_http) without contacting any network.
    fn http_s3_storage() -> StorageConfig {
        StorageConfig {
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_path_style: true,
            s3_allow_http: true,
            ..Default::default()
        }
    }

    #[test]
    fn falls_back_to_inner_for_known_scheme() {
        let inner = DefaultObjectStoreRegistry::new();
        let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let url = Url::parse("memory://my-store").unwrap();
        inner.register_store(&url, Arc::clone(&memory));

        let lazy = LazyHttpObjectStoreRegistry::new(inner);
        let resolved = lazy.get_store(&url).expect("memory store still resolvable");
        assert!(Arc::ptr_eq(&resolved, &memory));
    }

    #[test]
    fn lazy_builds_https_store_on_demand() {
        let lazy = LazyHttpObjectStoreRegistry::new(DefaultObjectStoreRegistry::new());
        // No prior registration. Asking for an arbitrary HTTPS URL must succeed.
        let url = Url::parse("https://huggingface.co/datasets/squad/resolve/main/x").unwrap();
        let store = lazy.get_store(&url).expect("lazy build for https succeeds");
        // Second call returns the cached instance.
        let again = lazy.get_store(&url).unwrap();
        assert!(Arc::ptr_eq(&store, &again));
    }

    #[test]
    fn lazy_builds_http_store_on_demand() {
        let lazy = LazyHttpObjectStoreRegistry::new(DefaultObjectStoreRegistry::new());
        let url = Url::parse("http://internal.example.com/data.csv").unwrap();
        assert!(lazy.get_store(&url).is_ok());
    }

    #[test]
    fn unknown_scheme_propagates_inner_error() {
        let lazy = LazyHttpObjectStoreRegistry::new(DefaultObjectStoreRegistry::new());
        let url = Url::parse("ftp://example.com/file").unwrap();
        let err = lazy
            .get_store(&url)
            .expect_err("ftp must not be auto-built by the http-only lazy registry");
        let msg = err.to_string();
        assert!(
            msg.contains("No suitable object store"),
            "expected the standard registry error, got: {msg}"
        );
    }

    #[test]
    fn https_with_explicit_port() {
        let lazy = LazyHttpObjectStoreRegistry::new(DefaultObjectStoreRegistry::new());
        let url = Url::parse("https://internal:8443/data.parquet").unwrap();
        assert!(lazy.get_store(&url).is_ok());
    }

    #[test]
    fn s3_without_storage_propagates_inner_error() {
        // Plain `new()` keeps the http-only behaviour: an unregistered s3
        // bucket must surface the standard registry error, not be auto-built.
        let lazy = LazyHttpObjectStoreRegistry::new(DefaultObjectStoreRegistry::new());
        let url = Url::parse("s3://my-bucket/data.csv").unwrap();
        let err = lazy
            .get_store(&url)
            .expect_err("s3 must not be auto-built without a storage config");
        assert!(
            err.to_string().contains("No suitable object store"),
            "expected the standard registry error, got: {err}"
        );
    }

    #[test]
    fn s3_builds_store_on_demand_with_storage() {
        // `with_s3_fallback` builds + caches the bucket store on first miss.
        let lazy = LazyHttpObjectStoreRegistry::with_s3_fallback(
            DefaultObjectStoreRegistry::new(),
            http_s3_storage(),
        );
        let url = Url::parse("s3://my-bucket/data.csv").unwrap();
        let store = lazy.get_store(&url).expect("lazy s3 build succeeds");
        // A second object in the same bucket returns the cached instance.
        let other = Url::parse("s3://my-bucket/other.parquet").unwrap();
        let again = lazy.get_store(&other).unwrap();
        assert!(Arc::ptr_eq(&store, &again));
    }

    #[test]
    fn s3a_scheme_also_builds_with_storage() {
        let lazy = LazyHttpObjectStoreRegistry::with_s3_fallback(
            DefaultObjectStoreRegistry::new(),
            http_s3_storage(),
        );
        let url = Url::parse("s3a://hadoop-bucket/data.csv").unwrap();
        assert!(lazy.get_store(&url).is_ok());
    }

    // ── CAT-01: TvfPolicy gating of lazy http(s) builds ──────────────────

    #[test]
    fn tvf_policy_blocks_non_allowlisted_http_host() {
        // Default fail-closed policy: no allow_http, empty allowlist.
        let lazy = LazyHttpObjectStoreRegistry::with_tvf_policy(
            DefaultObjectStoreRegistry::new(),
            TvfPolicy::default(),
        );
        let url = Url::parse("http://169.254.169.254/latest/meta-data/").unwrap();
        let err = lazy
            .get_store(&url)
            .expect_err("non-allowlisted http host must be rejected");
        assert!(
            err.to_string().contains("not in")
                || err.to_string().contains("allowed_http_hosts"),
            "expected a TVF policy rejection, got: {err}"
        );
    }

    #[test]
    fn tvf_policy_allows_allowlisted_http_host() {
        let policy = TvfPolicy {
            allow_local_paths: false,
            allow_http: false,
            allowed_http_hosts: vec!["data.example.com".to_string()],
        };
        let lazy = LazyHttpObjectStoreRegistry::with_tvf_policy(
            DefaultObjectStoreRegistry::new(),
            policy,
        );
        let url = Url::parse("https://data.example.com/x.parquet").unwrap();
        assert!(
            lazy.get_store(&url).is_ok(),
            "an allowlisted host must still build its store"
        );
    }

    #[test]
    fn tvf_policy_allow_http_permits_any_host() {
        let policy = TvfPolicy {
            allow_local_paths: false,
            allow_http: true,
            allowed_http_hosts: vec![],
        };
        let lazy = LazyHttpObjectStoreRegistry::with_tvf_policy(
            DefaultObjectStoreRegistry::new(),
            policy,
        );
        let url = Url::parse("https://anything.internal/x.csv").unwrap();
        assert!(lazy.get_store(&url).is_ok());
    }

    #[test]
    fn no_policy_keeps_legacy_permissive_http() {
        // `new()` carries no TvfPolicy: embedded CLI / test behaviour is
        // unchanged — arbitrary hosts still build on demand.
        let lazy = LazyHttpObjectStoreRegistry::new(DefaultObjectStoreRegistry::new());
        let url = Url::parse("http://anything.internal/data.csv").unwrap();
        assert!(lazy.get_store(&url).is_ok());
    }
}
