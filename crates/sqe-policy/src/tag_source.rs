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
    /// Return the column name -> tags map for the given (fully-qualified)
    /// table, distinguishing "known" from "unknown".
    ///
    /// - `Some(map)` means the table's tag state is KNOWN. `Some(empty)` is a
    ///   valid known state ("this table carries no tags"); the caller does no
    ///   tag work but does NOT deny.
    /// - `None` means the tag state is UNKNOWN: a cache miss, a disabled cache,
    ///   or unreadable metadata. The caller MUST fail closed (deny) for that
    ///   scan, because a tag mask or tag row-filter may exist that we cannot
    ///   see. Treating unknown as "no tags" would silently skip a security
    ///   control.
    ///
    /// Implementations MUST NOT error: an I/O or parse failure is reported as
    /// `None` (unknown), not propagated.
    fn column_tags(
        &self,
        catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> Option<HashMap<String, Vec<String>>>;
}

/// A TagSource that always reports "known: no tags". Used when no tag SOURCE is
/// configured (passthrough / no-tag deployments). Returning `Some(empty)`
/// rather than `None` is deliberate: the absence of a tag source means tags are
/// definitively not in play, so it must NOT cause fail-closed denials.
#[derive(Debug, Default)]
pub struct NoopTagSource;

impl TagSource for NoopTagSource {
    fn column_tags(
        &self,
        _c: Option<&str>,
        _n: &[String],
        _t: &str,
    ) -> Option<HashMap<String, Vec<String>>> {
        Some(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_returns_known_empty() {
        let s = NoopTagSource;
        // Some(empty) = "known: no tags", NOT None (unknown). A missing tag
        // source must never fail closed.
        let got = s.column_tags(Some("cat"), &["ns1".into(), "ns2".into()], "t");
        assert_eq!(got, Some(HashMap::new()));
    }

    #[test]
    fn noop_with_none_catalog_returns_known_empty() {
        let s = NoopTagSource;
        assert_eq!(s.column_tags(None, &["ns".into()], "tbl"), Some(HashMap::new()));
    }

    #[test]
    fn noop_with_empty_namespace_returns_known_empty() {
        let s = NoopTagSource;
        assert_eq!(s.column_tags(Some("cat"), &[], "tbl"), Some(HashMap::new()));
    }
}
