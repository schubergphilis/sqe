//! Adapter that bridges `sqe_policy::tag_source::TagSource` to
//! `sqe_metrics::audit::TagLookup`.
//!
//! `sqe-metrics` cannot depend on `sqe-policy` (it would create a circular
//! dependency chain), so `TagLookup` is a mirror trait defined in
//! `sqe-metrics`. This adapter lives in `sqe-coordinator`, which depends on
//! both crates, and delegates every call to the underlying `TagSource`.
//!
//! The coordinator creates one `CacheTagSource` for the policy pipeline.
//! Wrapping the same `Arc<dyn TagSource>` in an `AuditTagAdapter` gives the
//! audit writer access to the same tag resolution with zero extra network
//! calls or cache entries.

use std::collections::HashMap;
use std::sync::Arc;

use sqe_metrics::audit::TagLookup;
use sqe_policy::tag_source::TagSource;

/// Wraps an `Arc<dyn TagSource>` and implements `TagLookup` by delegation.
///
/// Both traits have identical signatures so the delegation is mechanical.
/// The `None` semantics are identical: unknown tag state -> fail closed.
pub struct AuditTagAdapter(pub Arc<dyn TagSource>);

impl TagLookup for AuditTagAdapter {
    fn column_tags(
        &self,
        catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> Option<HashMap<String, Vec<String>>> {
        self.0.column_tags(catalog, namespace, table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_policy::tag_source::NoopTagSource;

    #[test]
    fn delegates_to_inner_tag_source() {
        let inner = Arc::new(NoopTagSource);
        let adapter = AuditTagAdapter(inner);
        // NoopTagSource returns Some(empty) for all inputs.
        let result = adapter.column_tags(Some("cat"), &["ns".into()], "tbl");
        assert_eq!(result, Some(HashMap::new()));
    }
}
