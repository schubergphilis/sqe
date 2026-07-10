//! [`TagLookup`] trait for resolving column tags referenced in audit context,
//! with a [`NoopTagLookup`] default.

/// Mirror of `sqe_policy::tag_source::TagSource`, reproduced here so
/// `sqe-metrics` does not gain a dependency on `sqe-policy`.
///
/// Mirrors the exact signature of `TagSource::column_tags`:
/// - `Some(map)` means the tag state for the table is KNOWN. An empty map
///   is a valid "known: no tags" answer and MUST NOT cause fail-closed
///   behaviour.
/// - `None` means the tag state is UNKNOWN (cache miss, disabled cache,
///   parse error). The caller MUST fail closed: apply conservative literal
///   stripping rather than assuming no tags.
///
/// Implementations MUST be `Send + Sync`. Do NOT block on I/O inside the
/// method; it is called synchronously from the audit writer thread.
pub trait TagLookup: Send + Sync {
    /// Return the column name -> tags map for the given (fully-qualified)
    /// table, distinguishing "known" from "unknown".
    fn column_tags(
        &self,
        catalog: Option<&str>,
        namespace: &[String],
        table: &str,
    ) -> Option<std::collections::HashMap<String, Vec<String>>>;
}

/// A `TagLookup` that always reports "known: no tags" (`Some(empty)`).
///
/// Used when GDPR masking is not configured or when no external tag source is
/// available. Returning `Some(empty)` rather than `None` is intentional: the
/// absence of a configured tag source means tags are definitively not in play,
/// so it MUST NOT trigger fail-closed literal stripping.
#[derive(Debug, Default)]
pub struct NoopTagLookup;

impl TagLookup for NoopTagLookup {
    fn column_tags(
        &self,
        _catalog: Option<&str>,
        _namespace: &[String],
        _table: &str,
    ) -> Option<std::collections::HashMap<String, Vec<String>>> {
        Some(std::collections::HashMap::new())
    }
}
