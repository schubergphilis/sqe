//! Hive-style external tables with definitions in a sidecar manifest.
//!
//! Spec: `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`
//!
//! Layout: `<root>/<database>/<table>/_sqe/table.json`. The manifest mirrors
//! Glue's `TableInput`, so storing definitions in Glue or HMS later is a field
//! mapping rather than a redesign.

pub mod athena_view;
pub mod ddl_render;
#[cfg(feature = "glue")]
pub mod glue_source;
pub mod manifest;
pub mod partition;
pub mod projection;
pub mod prune;
pub mod provider;
pub mod serde_map;
pub mod source;
pub mod stats;
pub mod store;
pub mod scan_exec;
pub mod scan_mode;
pub mod worker_scan;

pub use ddl_render::{hive_spec_from_manifest, render_hive_create_table_ddl};

/// Errors from the Hive external-table path.
///
/// `Forbidden` is separate from `Validation` because a location that fails the
/// `[storage.tvf]` allowlist is a permission answer, not a malformed input, and
/// the two map to different `SqeError` variants at the boundary.
#[derive(Debug, thiserror::Error)]
pub enum HiveError {
    #[error("hive_external: {0}")]
    Validation(String),
    #[error("hive_external: table not found: {0}")]
    NotFound(String),
    #[error("hive_external: object store error: {0}")]
    Store(String),
    #[error("hive_external: forbidden: {0}")]
    Forbidden(String),
    /// A metastore call exceeded `[hive] metadata_timeout_secs` (issue
    /// #666). Distinct from `Store` so the provider can serve a stale cache
    /// entry for this case and this case only.
    #[error("hive_external: catalog metadata timeout: {0}")]
    Timeout(String),
    /// The metastore refused or could not sign the call because the engine's
    /// AWS credentials are missing, expired or invalid (SSO session over,
    /// `AWS_SESSION_TOKEN` stale, no provider in the chain). Distinct from
    /// `Store` so the message names the cause and the region instead of the
    /// SDK's bare "dispatch failure", and so the MCP layer can report the
    /// catalog as `auth_error` rather than `unreachable`.
    #[error("hive_external: AWS credentials missing or expired for the Glue catalog ({0})")]
    Credentials(String),
}
