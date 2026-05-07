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
//! lookups hit the cache. All other schemes (`s3`, `file`, ...) flow through
//! the inner registry unchanged.
//!
//! The wrapper is used by both [`build_embedded_context`] and
//! `coordinator::session_context`, so the cluster engine and the embedded CLI
//! share one HTTP fetch path.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreRegistry;
use object_store::http::HttpBuilder;
use object_store::ObjectStore;
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
}

impl<R: ObjectStoreRegistry> LazyHttpObjectStoreRegistry<R> {
    /// Wrap the given inner registry.
    pub fn new(inner: R) -> Self {
        Self { inner }
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
                build_and_register_http_store(&self.inner, url)
            }
            Err(e) => Err(e),
        }
    }
}

fn build_and_register_http_store(
    inner: &dyn ObjectStoreRegistry,
    url: &Url,
) -> DFResult<Arc<dyn ObjectStore>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;
    use object_store::memory::InMemory;

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
}
