//! Shared helpers for integration test binaries.
//! Each file in tests/ is its own binary; include this via `mod common;`.

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
    let handler = sqe_coordinator::QueryHandler::new(policy, config, None, None, None);
    (session, handler)
}

/// Format a single cell value from an Arrow column for display / comparison.
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
pub fn print_results(label: &str, sql: &str, batches: &[arrow_array::RecordBatch]) {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("\n-- {label} ({total_rows} rows)");
    println!("-- {sql}");
    match arrow::util::pretty::pretty_format_batches(batches) {
        Ok(table) => println!("{table}"),
        Err(e) => println!("(could not format: {e})"),
    }
}
