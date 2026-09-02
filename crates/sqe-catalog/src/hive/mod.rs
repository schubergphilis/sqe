//! Hive-style external tables with definitions in a sidecar manifest.
//!
//! Spec: `docs/superpowers/specs/2026-08-19-hive-external-tables-design.md`
//!
//! Layout: `<root>/<database>/<table>/_sqe/table.json`. The manifest mirrors
//! Glue's `TableInput`, so storing definitions in Glue or HMS later is a field
//! mapping rather than a redesign.

pub mod ddl_render;
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
}
