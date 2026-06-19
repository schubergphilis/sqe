//! Tag source: reads column -> tags associations for a table.
//!
//! Dependency inversion: `sqe-policy` cannot depend on `sqe-catalog`, so the
//! Iceberg/Polaris-backed implementation lives in the coordinator and is
//! injected as `Arc<dyn TagSource>`. Tags are stored as the Iceberg table
//! property `sqe.column-tags` (see docs/ranger-tag-storage-decision.md).
//!
//! Takes the FULL table identity (catalog + full namespace path + table). The
//! `TableMetadataCache` is keyed by the full identity; a reduced
//! last-component namespace cannot reconstruct it for multi-level namespaces
//! (the recurring identity bug this design avoids).

use std::collections::HashMap;

/// Resolves column -> tags for a table, from its metadata.
pub trait TagSource: Send + Sync {
    /// Return a map of column name -> tags for the given (fully-qualified)
    /// table. Implementations MUST fail safe: any miss / unparseable metadata
    /// returns an EMPTY map (no tags => no extra masking, which is correct
    /// because tags only ADD restrictions). Never error.
    fn column_tags(
        &self,
        catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> HashMap<String, Vec<String>>;
}

/// A TagSource that always returns no tags. Used when tag-based masking is
/// disabled or no catalog/cache is wired.
#[derive(Debug, Default)]
pub struct NoopTagSource;

impl TagSource for NoopTagSource {
    fn column_tags(
        &self,
        _c: Option<&str>,
        _n: &[String],
        _t: &str,
    ) -> HashMap<String, Vec<String>> {
        HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_returns_empty() {
        let s = NoopTagSource;
        assert!(s
            .column_tags(Some("cat"), &["ns1".into(), "ns2".into()], "t")
            .is_empty());
    }

    #[test]
    fn noop_with_none_catalog_returns_empty() {
        let s = NoopTagSource;
        assert!(s.column_tags(None, &["ns".into()], "tbl").is_empty());
    }

    #[test]
    fn noop_with_empty_namespace_returns_empty() {
        let s = NoopTagSource;
        assert!(s.column_tags(Some("cat"), &[], "tbl").is_empty());
    }
}
