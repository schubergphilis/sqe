//! Shared helpers for integration test binaries.
//! Each file in tests/ is its own binary; include this via `mod common;`.
#![allow(dead_code)]

use std::sync::Arc;

/// Initialize the tracing subscriber once for the entire test binary.
pub fn init_tracing() {
    static TRACING_INIT: std::sync::Once = std::sync::Once::new();
    TRACING_INIT.call_once(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "sqe_coordinator=info,sqe_catalog=info,sqe_auth=info,warn",
                )
            });
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    });
}

/// Resolve the test config path relative to the workspace root.
/// CARGO_MANIFEST_DIR points to the crate dir (crates/sqe-coordinator),
/// so we go up two levels to reach the workspace root.
pub fn test_config_path() -> String {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent() // crates/
        .and_then(|p| p.parent()) // workspace root
        .unwrap_or(std::path::Path::new("."));
    workspace_root
        .join("tests")
        .join("sqe-test.toml")
        .to_string_lossy()
        .to_string()
}

/// Authenticate as root and return (session, handler).
pub async fn setup_handler() -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    init_tracing();
    let config =
        sqe_core::SqeConfig::load(&test_config_path()).expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker = Arc::new(
        sqe_coordinator::query_tracker::QueryTracker::new(&config.query_history),
    );
    let query_cache = if config.query_cache.enabled {
        Some(Arc::new(sqe_coordinator::query_cache::ResultCache::new(&config.query_cache, None)))
    } else {
        None
    };
    let handler = sqe_coordinator::QueryHandler::new(
        policy, None, config, None, None, None, None, query_tracker, query_cache,
        None, // grant_backend
        None, // lineage observer
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    ).expect("Failed to create QueryHandler");
    (session, handler)
}

/// Resolve a `host:port` string to its candidate socket addresses.
///
/// `SocketAddr::from_str` is purely syntactic and does not resolve hostnames,
/// so it fails on `"localhost:50052"` even though that is the exact default
/// used throughout the distributed test docs. `ToSocketAddrs` performs actual
/// hostname resolution (via the system resolver / `/etc/hosts`), so
/// `"localhost:50052"` resolves correctly here.
fn resolve_host_port(host_port: &str) -> std::io::Result<Vec<std::net::SocketAddr>> {
    use std::net::ToSocketAddrs;
    let addrs: Vec<std::net::SocketAddr> = host_port.to_socket_addrs()?.collect();
    if addrs.is_empty() {
        return Err(std::io::Error::other(format!(
            "'{host_port}' resolved to no addresses"
        )));
    }
    Ok(addrs)
}

/// Try every resolved candidate address and succeed if any of them accepts a
/// TCP connection within `timeout`. `localhost` can resolve `::1` before
/// `127.0.0.1` (or vice versa) depending on the host's resolver config, and
/// only one of the two may actually be listening, so all candidates must be
/// tried rather than just the first.
///
/// Uses `tokio::net::TcpStream::connect` (not the blocking
/// `std::net::TcpStream::connect_timeout`) so the check does not block the
/// async worker thread it runs on inside `#[tokio::test(flavor =
/// "multi_thread")]`.
async fn is_any_addr_reachable(
    addrs: &[std::net::SocketAddr],
    timeout: std::time::Duration,
) -> bool {
    for addr in addrs {
        let connected = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr))
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false);
        if connected {
            return true;
        }
    }
    false
}

/// Authenticate as root and return (session, handler) wired to a live worker
/// fleet, mirroring `integration_test.rs::test_distributed_select`'s
/// construction. Used by distributed-dispatch tests (query-path dispatch and,
/// as of Phase 4c Task 6, `CALL system.rewrite_data_files(..., distributed =>
/// 'require')` compaction dispatch) that need a real `WorkerRegistry` with
/// reachable worker URLs instead of `setup_handler`'s no-fleet handler.
///
/// Fails loudly (not silently) when a URL is unreachable -- issue #122's
/// lesson applies here too: a distributed test that falls back to local
/// execution and still passes hides real dispatch regressions.
///
/// `worker_urls` entries must be `http://host:port`. Each is checked for TCP
/// reachability before being marked healthy in the registry.
#[allow(dead_code)]
pub async fn setup_handler_with_workers(
    worker_urls: &[String],
) -> (sqe_core::Session, sqe_coordinator::QueryHandler) {
    init_tracing();
    let config =
        sqe_core::SqeConfig::load(&test_config_path()).expect("Failed to load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("Failed to create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Auth failed for root");
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);

    for url in worker_urls {
        let host_port = url
            .strip_prefix("http://")
            .unwrap_or_else(|| panic!("worker URL must be http://host:port, got '{url}'"));
        let addrs = resolve_host_port(host_port)
            .unwrap_or_else(|e| panic!("invalid worker URL '{url}': {e}"));
        let port = addrs
            .first()
            .map(|a| a.port())
            .unwrap_or_else(|| panic!("worker URL '{url}' resolved to no addresses"));
        let reachable = is_any_addr_reachable(&addrs, std::time::Duration::from_secs(2)).await;
        assert!(
            reachable,
            "worker unreachable at {url}: a distributed test must fail loudly, not fall \
             back to local execution. Start a worker there, e.g.:\n  \
             SQE_WORKER__FLIGHT_PORT={} cargo run -p sqe-worker -- {}\n\
             or pass --ignored to skip.",
            port,
            test_config_path(),
        );
    }

    let registry = Arc::new(sqe_coordinator::worker_registry::WorkerRegistry::new(
        worker_urls.to_vec(),
    ));
    for url in worker_urls {
        registry.mark_healthy(url).await;
    }

    let query_tracker = Arc::new(
        sqe_coordinator::query_tracker::QueryTracker::new(&config.query_history),
    );
    let query_cache = if config.query_cache.enabled {
        Some(Arc::new(sqe_coordinator::query_cache::ResultCache::new(&config.query_cache, None)))
    } else {
        None
    };
    let handler = sqe_coordinator::QueryHandler::new(
        policy, None, config, Some(registry), None, None, None, query_tracker, query_cache,
        None, // grant_backend
        None, // lineage observer
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    ).expect("Failed to create QueryHandler");
    (session, handler)
}

/// Format a single cell value from an Arrow column for display / comparison.
// Used by sql_compat_test.rs; integration_test.rs uses print_results instead.
#[allow(dead_code)]
pub fn fmt_val(col: &dyn arrow_array::Array, row: usize) -> String {
    #[allow(unused_imports)]
    use arrow_array::Array as _;
    if col.is_null(row) || col.as_any().downcast_ref::<arrow_array::NullArray>().is_some() {
        return "NULL".to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::UInt32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Float64Array>() {
        return format!("{:.2}", a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Float32Array>() {
        return format!("{:.2}", a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::StringViewArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::BooleanArray>() {
        return a.value(row).to_string();
    }
    // Fallback: show the Arrow type name so unknown types are diagnosable
    format!("?({:?})", col.data_type())
}

/// Pretty-print RecordBatches for test diagnostics using Arrow's built-in formatter.
#[allow(dead_code)] // used by integration_test.rs; dead in binaries that don't call it
pub fn print_results(label: &str, sql: &str, batches: &[arrow_array::RecordBatch]) {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("\n-- {label} ({total_rows} rows)");
    println!("-- {sql}");
    match arrow::util::pretty::pretty_format_batches(batches) {
        Ok(table) => println!("{table}"),
        Err(e) => println!("(could not format: {e})"),
    }
}

#[cfg(test)]
mod host_port_resolution_tests {
    use super::resolve_host_port;

    /// Regression test for the bug fixed in `setup_handler_with_workers`:
    /// `SocketAddr::from_str` cannot resolve hostnames, so
    /// `"localhost:50052".parse::<SocketAddr>()` returns `Err` even though
    /// `localhost` is the exact default host documented for distributed
    /// tests. `resolve_host_port` must accept it.
    #[test]
    fn resolves_localhost_host_port() {
        let addrs = resolve_host_port("localhost:50052")
            .expect("resolve_host_port must resolve 'localhost:50052'");
        assert!(!addrs.is_empty(), "expected at least one resolved address");
        assert!(
            addrs.iter().all(|a| a.port() == 50052),
            "all resolved addresses must keep the requested port: {addrs:?}"
        );
    }

    #[test]
    fn resolves_literal_ipv4_host_port() {
        let addrs = resolve_host_port("127.0.0.1:50052")
            .expect("resolve_host_port must resolve a literal IPv4 address");
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].port(), 50052);
        assert!(addrs[0].ip().is_loopback());
    }
}
